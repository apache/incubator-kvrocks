/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package set

import (
	"context"
	"sort"
	"strconv"
	"testing"

	"github.com/apache/kvrocks/tests/gocase/util"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func CreateSet(t *testing.T, rdb *redis.Client, ctx context.Context, key string, entries []interface{}) {
	require.NoError(t, rdb.Del(ctx, key).Err())
	for _, entry := range entries {
		switch entry := entry.(type) {
		default:
			require.NoError(t, rdb.SAdd(ctx, key, entry).Err())
		}
	}
}

func GetArrayUnion(arrays ...[]string) []string {
	result := []string{}
	var vis = make(map[string]bool)
	for _, array := range arrays {
		for _, value := range array {
			_, ok := vis[value]
			if ok {
				continue
			}
			vis[value] = true
			result = append(result, value)
		}
	}
	return result
}

func TestSet(t *testing.T) {
	configOptions := []util.ConfigOptions{
		{
			Name:       "txn-context-enabled",
			Options:    []string{"yes", "no"},
			ConfigType: util.YesNo,
		},
		{
			Name:       "resp3-enabled",
			Options:    []string{"yes", "no"},
			ConfigType: util.YesNo,
		},
	}

	configsMatrix, err := util.GenerateConfigsMatrix(configOptions)
	require.NoError(t, err)

	for _, configs := range configsMatrix {
		setTests(t, configs)
	}
}

var setTests = func(t *testing.T, configs util.KvrocksServerConfigs) {
	srv := util.StartServer(t, configs)
	defer srv.Close()
	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	t.Run("SADD, SCARD, SISMEMBER, SMISMEMBER, SMEMBERS basics - regular set", func(t *testing.T) {
		CreateSet(t, rdb, ctx, "myset", []interface{}{"foo"})
		require.EqualValues(t, 1, rdb.SAdd(ctx, "myset", "bar").Val())
		require.EqualValues(t, 0, rdb.SAdd(ctx, "myset", "bar").Val())
		require.EqualValues(t, 2, rdb.SCard(ctx, "myset").Val())
		require.EqualValues(t, true, rdb.SIsMember(ctx, "myset", "foo").Val())
		require.EqualValues(t, true, rdb.SIsMember(ctx, "myset", "bar").Val())
		require.EqualValues(t, false, rdb.SIsMember(ctx, "myset", "bla").Val())
		require.EqualValues(t, []bool{true}, rdb.SMIsMember(ctx, "myset", "foo").Val())
		require.EqualValues(t, []bool{true, true}, rdb.SMIsMember(ctx, "myset", "foo", "bar").Val())
		require.EqualValues(t, []bool{true, false}, rdb.SMIsMember(ctx, "myset", "foo", "bla").Val())
		require.EqualValues(t, []bool{false, true}, rdb.SMIsMember(ctx, "myset", "bla", "foo").Val())
		require.EqualValues(t, []bool{false}, rdb.SMIsMember(ctx, "myset", "bla").Val())
		cmd := rdb.SMembers(ctx, "myset")
		require.NoError(t, cmd.Err())
		sort.Strings(cmd.Val())
		require.EqualValues(t, []string{"bar", "foo"}, cmd.Val())
	})

	t.Run("SADD, SCARD, SISMEMBER, SMISMEMBER, SMEMBERS basics - intset", func(t *testing.T) {
		CreateSet(t, rdb, ctx, "myset", []interface{}{17})
		require.EqualValues(t, 1, rdb.SAdd(ctx, "myset", 16).Val())
		require.EqualValues(t, 0, rdb.SAdd(ctx, "myset", 16).Val())
		require.EqualValues(t, 2, rdb.SCard(ctx, "myset").Val())
		require.EqualValues(t, true, rdb.SIsMember(ctx, "myset", 16).Val())
		require.EqualValues(t, true, rdb.SIsMember(ctx, "myset", 17).Val())
		require.EqualValues(t, false, rdb.SIsMember(ctx, "myset", 18).Val())
		require.EqualValues(t, []bool{true}, rdb.SMIsMember(ctx, "myset", 16).Val())
		require.EqualValues(t, []bool{true, true}, rdb.SMIsMember(ctx, "myset", 16, 17).Val())
		require.EqualValues(t, []bool{true, false}, rdb.SMIsMember(ctx, "myset", 16, 18).Val())
		require.EqualValues(t, []bool{false, true}, rdb.SMIsMember(ctx, "myset", 18, 16).Val())
		require.EqualValues(t, []bool{false}, rdb.SMIsMember(ctx, "myset", 18).Val())
		cmd := rdb.SMembers(ctx, "myset")
		require.NoError(t, cmd.Err())
		sort.Strings(cmd.Val())
		require.EqualValues(t, []string{"16", "17"}, cmd.Val())
	})

	t.Run("SMISMEMBER against non set", func(t *testing.T) {
		require.ErrorContains(t, rdb.LPush(ctx, "myset", "foo").Err(), "WRONGTYPE")
	})

	t.Run("SMISMEMBER non existing key", func(t *testing.T) {
		require.EqualValues(t, []bool{false}, rdb.SMIsMember(ctx, "myset1", "foo").Val())
		require.EqualValues(t, []bool{false, false}, rdb.SMIsMember(ctx, "myset1", "foo", "bar").Val())
	})

	t.Run("SMISMEMBER requires one or more members", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "zmscoretest").Err())
		require.NoError(t, rdb.ZAdd(ctx, "zmscoretest", redis.Z{Score: 10, Member: "x"}).Err())
		require.NoError(t, rdb.ZAdd(ctx, "zmscoretest", redis.Z{Score: 20, Member: "y"}).Err())
		util.ErrorRegexp(t, rdb.Do(ctx, "smismember", "zmscoretest").Err(), ".*ERR.*wrong.*number.*arg.*")
	})

	t.Run("SADD against non set", func(t *testing.T) {
		require.NoError(t, rdb.LPush(ctx, "mylist", "foo").Err())
		util.ErrorRegexp(t, rdb.SAdd(ctx, "mylist", "bar").Err(), ".*WRONGTYPE.*")
	})

	t.Run("SADD a non-integer against an intset", func(t *testing.T) {
		CreateSet(t, rdb, ctx, "myset", []interface{}{1, 2, 3})
		require.EqualValues(t, 1, rdb.SAdd(ctx, "myset", "a").Val())
	})

	t.Run("SADD an integer larger than 64 bits", func(t *testing.T) {
		CreateSet(t, rdb, ctx, "myset", []interface{}{"213244124402402314402033402"})
		require.EqualValues(t, true, rdb.SIsMember(ctx, "myset", "213244124402402314402033402").Val())
		require.EqualValues(t, []bool{true}, rdb.SMIsMember(ctx, "myset", "213244124402402314402033402").Val())
	})

	t.Run("SADD overflows the maximum allowed integers in an intset", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "myset").Err())
		for i := 0; i < 512; i++ {
			require.NoError(t, rdb.SAdd(ctx, "myset", i).Err())
		}
		require.EqualValues(t, 1, rdb.SAdd(ctx, "myset", 512).Val())
	})

	t.Run("Variadic SADD", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "myset").Err())
		require.EqualValues(t, 3, rdb.SAdd(ctx, "myset", "a", "b", "c").Val())
		require.EqualValues(t, 2, rdb.SAdd(ctx, "myset", "A", "a", "b", "c", "B").Val())
		cmd := rdb.SMembers(ctx, "myset")
		require.NoError(t, cmd.Err())
		sort.Strings(cmd.Val())
		require.EqualValues(t, []string{"A", "B", "a", "b", "c"}, cmd.Val())
	})

	t.Run("SREM basics - regular set", func(t *testing.T) {
		CreateSet(t, rdb, ctx, "myset", []interface{}{"foo", "bar", "ciao"})
		require.EqualValues(t, 0, rdb.SRem(ctx, "myset", "qux").Val())
		require.EqualValues(t, 1, rdb.SRem(ctx, "myset", "foo").Val())
		cmd := rdb.SMembers(ctx, "myset")
		require.NoError(t, cmd.Err())
		sort.Strings(cmd.Val())
		require.EqualValues(t, []string{"bar", "ciao"}, cmd.Val())
	})

	t.Run("SREM basics - intset", func(t *testing.T) {
		CreateSet(t, rdb, ctx, "myset", []interface{}{3, 4, 5})
		require.EqualValues(t, 0, rdb.SRem(ctx, "myset", 6).Val())
		require.EqualValues(t, 1, rdb.SRem(ctx, "myset", 4).Val())
		cmd := rdb.SMembers(ctx, "myset")
		require.NoError(t, cmd.Err())
		sort.Strings(cmd.Val())
		require.EqualValues(t, []string{"3", "5"}, cmd.Val())
	})

	t.Run("SREM with multiple arguments", func(t *testing.T) {
		CreateSet(t, rdb, ctx, "myset", []interface{}{"a", "b", "c", "d"})
		require.EqualValues(t, 0, rdb.SRem(ctx, "myset", "k", "k", "k").Val())
		require.EqualValues(t, 2, rdb.SRem(ctx, "myset", "b", "d", "x", "y").Val())
		cmd := rdb.SMembers(ctx, "myset")
		require.NoError(t, cmd.Err())
		sort.Strings(cmd.Val())
		require.EqualValues(t, []string{"a", "c"}, cmd.Val())
	})

	t.Run("SREM variadic version with more args needed to destroy the key", func(t *testing.T) {
		CreateSet(t, rdb, ctx, "myset", []interface{}{1, 2, 3})
		require.EqualValues(t, 3, rdb.SRem(ctx, "myset", 1, 2, 3, 4, 5, 6, 7, 8).Val())
	})

	t.Run("SREM variadic version with more args needed to destroy the key", func(t *testing.T) {
		CreateSet(t, rdb, ctx, "myset", []interface{}{1, 2, 3})
		require.EqualValues(t, 3, rdb.SRem(ctx, "myset", 1, 2, 3, 4, 5, 6, 7, 8).Val())
	})

	for _, dstype := range [2]string{"hashtable", "intset"} {
		for i := 1; i <= 5; i++ {
			require.NoError(t, rdb.Del(ctx, "set"+strconv.Itoa(i)).Err())
		}
		for i := 0; i < 200; i++ {
			require.NoError(t, rdb.SAdd(ctx, "set1", i).Err())
			require.NoError(t, rdb.SAdd(ctx, "set2", i+195).Err())
		}
		for _, i := range []int{199, 195, 100, 2000} {
			require.NoError(t, rdb.SAdd(ctx, "set3", i).Err())
		}
		for i := 5; i < 200; i++ {
			require.NoError(t, rdb.SAdd(ctx, "set4", i).Err())
		}
		var larger interface{}
		larger = 200
		require.NoError(t, rdb.SAdd(ctx, "set5", 0).Err())
		if dstype == "hashtable" {
			larger = "foo"
		}
		for i := 1; i <= 5; i++ {
			switch larger := larger.(type) {
			default:
				require.NoError(t, rdb.SAdd(ctx, "set"+strconv.Itoa(i), larger).Err())
			}
		}
		t.Run("SINTER with two sets - "+dstype, func(t *testing.T) {
			cmd := rdb.SInter(ctx, "set1", "set2")
			require.NoError(t, cmd.Err())
			sort.Strings(cmd.Val())
			switch larger := larger.(type) {
			case int:
				require.EqualValues(t, []string{"195", "196", "197", "198", "199", strconv.Itoa(larger)}, cmd.Val())
			case string:
				require.EqualValues(t, []string{"195", "196", "197", "198", "199", larger}, cmd.Val())
			}
		})

		t.Run("SINTERCARD with a single set - "+dstype, func(t *testing.T) {
			cmd1 := rdb.SInter(ctx, "set1")
			require.NoError(t, cmd1.Err())
			for j := 1; j < 20; j++ {
				cmd2 := rdb.SInterCard(ctx, int64(j), "set1")
				require.NoError(t, cmd2.Err())
				if j > len(cmd1.Val()) {
					require.EqualValues(t, cmd2.Val(), len(cmd1.Val()))
				} else {
					require.EqualValues(t, cmd2.Val(), j)
				}
			}
		})

		t.Run("SINTERCARD with two sets - "+dstype, func(t *testing.T) {
			cmd1 := rdb.SInter(ctx, "set1", "set2")
			require.NoError(t, cmd1.Err())
			for j := 1; j < 20; j++ {
				cmd2 := rdb.SInterCard(ctx, int64(j), "set1", "set2")
				require.NoError(t, cmd2.Err())
				if j > len(cmd1.Val()) {
					require.EqualValues(t, cmd2.Val(), len(cmd1.Val()))
				} else {
					require.EqualValues(t, cmd2.Val(), j)
				}
			}
		})

		t.Run("SINTERSTORE with two sets - "+dstype, func(t *testing.T) {
			require.NoError(t, rdb.SInterStore(ctx, "setres", "set1", "set2").Err())
			cmd := rdb.SMembers(ctx, "setres")
			require.NoError(t, cmd.Err())
			sort.Strings(cmd.Val())

			switch larger := larger.(type) {
			case int:
				require.EqualValues(t, []string{"195", "196", "197", "198", "199", strconv.Itoa(larger)}, cmd.Val())
			case string:
				require.EqualValues(t, []string{"195", "196", "197", "198", "199", larger}, cmd.Val())
			}
		})

		t.Run("SUNION with two sets - "+dstype, func(t *testing.T) {
			set1 := rdb.SMembers(ctx, "set1")
			require.NoError(t, set1.Err())
			set2 := rdb.SMembers(ctx, "set2")
			require.NoError(t, set2.Err())
			expect := GetArrayUnion(set1.Val(), set2.Val())
			sort.Strings(expect)
			cmd := rdb.SUnion(ctx, "set1", "set2")
			require.NoError(t, cmd.Err())
			sort.Strings(cmd.Val())
			require.EqualValues(t, expect, cmd.Val())
		})

		t.Run("SUNIONSTORE with two sets - "+dstype, func(t *testing.T) {
			set1 := rdb.SMembers(ctx, "set1")
			require.NoError(t, set1.Err())
			set2 := rdb.SMembers(ctx, "set2")
			require.NoError(t, set2.Err())
			expect := GetArrayUnion(set1.Val(), set2.Val())
			sort.Strings(expect)
			require.NoError(t, rdb.SUnionStore(ctx, "setres", "set1", "set2").Err())
			cmd := rdb.SMembers(ctx, "setres")
			require.NoError(t, cmd.Err())
			sort.Strings(cmd.Val())
			require.EqualValues(t, expect, cmd.Val())
		})

		t.Run("SINTER against three sets - "+dstype, func(t *testing.T) {
			cmd := rdb.SInter(ctx, "set1", "set2", "set3")
			require.NoError(t, cmd.Err())
			sort.Strings(cmd.Val())
			switch larger := larger.(type) {
			case int:
				require.EqualValues(t, []string{"195", "199", strconv.Itoa(larger)}, cmd.Val())
			case string:
				require.EqualValues(t, []string{"195", "199", larger}, cmd.Val())
			}
		})

		t.Run("SINTERCARD with three sets - "+dstype, func(t *testing.T) {
			cmd1 := rdb.SInter(ctx, "set1", "set2", "set3")
			require.NoError(t, cmd1.Err())
			for j := 1; j < 20; j++ {
				cmd2 := rdb.SInterCard(ctx, int64(j), "set1", "set2", "set3")
				require.NoError(t, cmd2.Err())
				if j > len(cmd1.Val()) {
					require.EqualValues(t, cmd2.Val(), len(cmd1.Val()))
				} else {
					require.EqualValues(t, cmd2.Val(), j)
				}
			}
		})

		t.Run("SINTERSTORE with three sets - "+dstype, func(t *testing.T) {
			require.NoError(t, rdb.SInterStore(ctx, "setres", "set1", "set2", "set3").Err())
			cmd := rdb.SMembers(ctx, "setres")
			require.NoError(t, cmd.Err())
			sort.Strings(cmd.Val())
			switch larger := larger.(type) {
			case int:
				require.EqualValues(t, []string{"195", "199", strconv.Itoa(larger)}, cmd.Val())
			case string:
				require.EqualValues(t, []string{"195", "199", larger}, cmd.Val())
			}
		})

		t.Run("SUNION with non existing keys - "+dstype, func(t *testing.T) {
			set1 := rdb.SMembers(ctx, "set1")
			require.NoError(t, set1.Err())
			set2 := rdb.SMembers(ctx, "set2")
			require.NoError(t, set2.Err())
			expect := GetArrayUnion(set1.Val(), set2.Val())
			sort.Strings(expect)
			cmd := rdb.SUnion(ctx, "nokey1", "set1", "set2", "nokey2")
			require.NoError(t, cmd.Err())
			sort.Strings(cmd.Val())
			require.EqualValues(t, expect, cmd.Val())
		})

		t.Run("SDIFF with two sets - "+dstype, func(t *testing.T) {
			cmd := rdb.SDiff(ctx, "set1", "set4")
			require.NoError(t, cmd.Err())
			sort.Strings(cmd.Val())
			require.EqualValues(t, []string{"0", "1", "2", "3", "4"}, cmd.Val())
		})

		t.Run("SDIFF with three sets - "+dstype, func(t *testing.T) {
			cmd := rdb.SDiff(ctx, "set1", "set4", "set5")
			require.NoError(t, cmd.Err())
			sort.Strings(cmd.Val())
			require.EqualValues(t, []string{"1", "2", "3", "4"}, cmd.Val())
		})

		t.Run("SDIFFSTORE with three sets - "+dstype, func(t *testing.T) {
			require.NoError(t, rdb.SDiffStore(ctx, "setres", "set1", "set4", "set5").Err())
			cmd := rdb.SMembers(ctx, "setres")
			require.NoError(t, cmd.Err())
			sort.Strings(cmd.Val())
			require.EqualValues(t, []string{"1", "2", "3", "4"}, cmd.Val())
		})
	}

	t.Run("SDIFF with first set empty", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "set1", "set2", "set3").Err())
		require.NoError(t, rdb.SAdd(ctx, "set2", 1, 2, 3, 4).Err())
		require.NoError(t, rdb.SAdd(ctx, "set3", "a", "b", "c", "d").Err())
		cmd := rdb.SDiff(ctx, "set1", "set2", "set3")
		require.NoError(t, cmd.Err())
		sort.Strings(cmd.Val())
		require.EqualValues(t, []string{}, cmd.Val())
	})

	t.Run("SDIFF with same set two times", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "set1").Err())
		require.NoError(t, rdb.SAdd(ctx, "set1", "a", "b", "c", 1, 2, 3, 4, 5, 6).Err())
		cmd := rdb.SDiff(ctx, "set1", "set1")
		require.NoError(t, cmd.Err())
		sort.Strings(cmd.Val())
		require.EqualValues(t, []string{}, cmd.Val())
	})

	t.Run("SDIFF fuzzing", func(t *testing.T) {
		for j := 0; j < 100; j++ {
			s := make(map[string]bool)
			var args []string
			numSets := util.RandomInt(10) + 1
			for i := 0; i < int(numSets); i++ {
				numElements := util.RandomInt(100)
				require.NoError(t, rdb.Del(ctx, "set_"+strconv.Itoa(i)).Err())
				args = append(args, "set_"+strconv.Itoa(i))
				for numElements > 0 {
					ele := util.RandomValue()
					require.NoError(t, rdb.SAdd(ctx, "set_"+strconv.Itoa(i), ele).Err())
					if i == 0 {
						s[ele] = true
					} else {
						delete(s, ele)
					}
					numElements -= 1
				}
			}
			cmd := rdb.SDiff(ctx, args...)
			require.NoError(t, cmd.Err())
			sort.Strings(cmd.Val())
			expect := make([]string, 0, 12)
			for key := range s {
				expect = append(expect, key)
			}
			sort.Strings(expect)
			require.EqualValues(t, expect, cmd.Val())
		}
	})

	t.Run("SINTER against non-set should throw error", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "key1", "x", 0).Err())
		util.ErrorRegexp(t, rdb.SInter(ctx, "key1", "noset").Err(), ".*WRONGTYPE.*")
	})

	t.Run("SUNION against non-set should throw error", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "key1", "x", 0).Err())
		util.ErrorRegexp(t, rdb.SUnion(ctx, "key1", "noset").Err(), ".*WRONGTYPE.*")
	})

	t.Run("SINTER should handle non existing key as empty", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "set1", "set2", "set3").Err())
		require.NoError(t, rdb.SAdd(ctx, "set1", "a", "b", "c").Err())
		require.NoError(t, rdb.SAdd(ctx, "set2", "b", "c", "d").Err())
		require.EqualValues(t, []string{}, rdb.SInter(ctx, "set1", "set2", "key3").Val())
	})

	t.Run("SINTERCARD with wrong args", func(t *testing.T) {
		CreateSet(t, rdb, ctx, "set1", []interface{}{"foo", "2"})
		CreateSet(t, rdb, ctx, "set2", []interface{}{"a", "b", "2"})

		// numkey should be a positive integer.
		require.ErrorContains(t, rdb.Do(ctx, "sintercard", "3.5", "set1").Err(), "is not an integer")
		require.ErrorContains(t, rdb.Do(ctx, "sintercard", "-1", "set1").Err(), "must be positive")
		require.ErrorContains(t, rdb.Do(ctx, "sintercard", "0", "set1").Err(), "must be positive")

		require.ErrorContains(t, rdb.Do(ctx, "sintercard", "1", "set1", "set2").Err(), "wrong number of arguments")
		require.ErrorContains(t, rdb.Do(ctx, "sintercard", "1", "set1", "set2", "LIMIT", "2").Err(), "wrong number of arguments")

		// The limit argument should be a positive integer.
		require.ErrorContains(t, rdb.Do(ctx, "sintercard", "2", "set1", "set2", "LIMIT", "-1").Err(), "can't be negative")
		require.ErrorContains(t, rdb.Do(ctx, "sintercard", "2", "set1", "set2", "LIMIT", "1.5").Err(), "is not an integer")
	})

	t.Run("SINTERCARD should return zero with empty sets", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "set1", "set2", "set3").Err())
		require.NoError(t, rdb.SAdd(ctx, "set1", "a", "b", "c").Err())
		require.NoError(t, rdb.SAdd(ctx, "set2", "b", "c", "d").Err())
		for j := 1; j < 20; j++ {
			require.EqualValues(t, 0, rdb.SInterCard(ctx, int64(j), "set1", "set2", "key3").Val())
		}
		require.EqualValues(t, 0, rdb.SInterCard(ctx, 0, "set3").Val())
	})

	t.Run("SINTER with same integer elements but different encoding", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "set1", "set2").Err())
		require.NoError(t, rdb.SAdd(ctx, "set1", 1, 2, 3).Err())
		require.NoError(t, rdb.SAdd(ctx, "set2", 1, 2, 3, "a").Err())
		require.NoError(t, rdb.SRem(ctx, "set2", "a").Err())
		require.EqualValues(t, []string{"1", "2", "3"}, rdb.SInter(ctx, "set1", "set2").Val())
	})

	t.Run("SINTERSTORE against non existing keys should delete dstkey", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "setres", "xxx", 0).Err())
		require.EqualValues(t, 0, rdb.SInterStore(ctx, "setres", "foo111", "bar222").Val())
		require.EqualValues(t, 0, rdb.Exists(ctx, "setres").Val())
	})

	t.Run("SUNIONSTORE against non existing keys should delete dstkey", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "setres", "xxx", 0).Err())
		require.EqualValues(t, 0, rdb.SUnionStore(ctx, "setres", "foo111", "bar222").Val())
		require.EqualValues(t, 0, rdb.Exists(ctx, "setres").Val())
	})

	t.Run("SPOP basics - hashtable", func(t *testing.T) {
		CreateSet(t, rdb, ctx, "myset", []interface{}{"a", "b", "c"})
		var array []string
		for i := 0; i < 3; i++ {
			cmd := rdb.SPop(ctx, "myset")
			require.NoError(t, cmd.Err())
			array = append(array, cmd.Val())
		}
		sort.Strings(array)
		require.EqualValues(t, []string{"a", "b", "c"}, array)
	})

	t.Run("SPOP with <count>=1 - hashtable", func(t *testing.T) {
		CreateSet(t, rdb, ctx, "myset", []interface{}{"a", "b", "c"})
		var array []string
		for i := 0; i < 3; i++ {
			cmd := rdb.SPopN(ctx, "myset", 1)
			require.NoError(t, cmd.Err())
			array = append(array, cmd.Val()...)
		}
		sort.Strings(array)
		require.EqualValues(t, []string{"a", "b", "c"}, array)
		require.EqualValues(t, 0, rdb.SCard(ctx, "myset").Val())
	})

	t.Run("SPOP basics - intset", func(t *testing.T) {
		CreateSet(t, rdb, ctx, "myset", []interface{}{1, 2, 3})
		var array []string
		for i := 0; i < 3; i++ {
			cmd := rdb.SPop(ctx, "myset")
			require.NoError(t, cmd.Err())
			array = append(array, cmd.Val())
		}
		sort.Strings(array)
		require.EqualValues(t, []string{"1", "2", "3"}, array)
	})

	t.Run("SPOP with <count>=1 - intset", func(t *testing.T) {
		CreateSet(t, rdb, ctx, "myset", []interface{}{1, 2, 3})
		var array []string
		for i := 0; i < 3; i++ {
			cmd := rdb.SPopN(ctx, "myset", 1)
			require.NoError(t, cmd.Err())
			array = append(array, cmd.Val()...)
		}
		sort.Strings(array)
		require.EqualValues(t, []string{"1", "2", "3"}, array)
		require.EqualValues(t, 0, rdb.SCard(ctx, "myset").Val())
	})

	t.Run("SPOP with <count> hashtable", func(t *testing.T) {
		CreateSet(t, rdb, ctx, "myset", []interface{}{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z"})
		var array []string
		var popNum = []int64{11, 9, 0, 4, 1, 0, 1, 9}
		for _, i := range popNum {
			cmd := rdb.SPopN(ctx, "myset", i)
			require.NoError(t, cmd.Err())
			array = append(array, cmd.Val()...)
		}
		sort.Strings(array)
		require.EqualValues(t, []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z"}, array)
		require.EqualValues(t, 0, rdb.SCard(ctx, "myset").Val())
	})

	t.Run("SPOP with <count> intset", func(t *testing.T) {
		CreateSet(t, rdb, ctx, "myset", []interface{}{1, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 2, 20, 21, 22, 23, 24, 25, 26, 3, 4, 5, 6, 7, 8, 9})
		var array []string
		var popNum = []int64{11, 9, 0, 4, 1, 0, 1, 9}
		for _, i := range popNum {
			cmd := rdb.SPopN(ctx, "myset", i)
			require.NoError(t, cmd.Err())
			array = append(array, cmd.Val()...)
		}
		sort.Strings(array)
		require.EqualValues(t, []string{"1", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "2", "20", "21", "22", "23", "24", "25", "26", "3", "4", "5", "6", "7", "8", "9"}, array)
		require.EqualValues(t, 0, rdb.SCard(ctx, "myset").Val())
	})

	t.Run("SPOP using integers, testing Knuth's and Floyd's algorithm", func(t *testing.T) {
		CreateSet(t, rdb, ctx, "myset", []interface{}{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20})
		var popNum = []int64{1, 2, 3, 4, 10, 10}
		var nowsize = 20
		for _, i := range popNum {
			require.EqualValues(t, nowsize, rdb.SCard(ctx, "myset").Val())
			cmd := rdb.SPopN(ctx, "myset", i)
			require.NoError(t, cmd.Err())
			nowsize -= len(cmd.Val())
		}
		require.EqualValues(t, nowsize, rdb.SCard(ctx, "myset").Val())
	})

	t.Run("SPOP using integers with Knuth's algorithm", func(t *testing.T) {
		require.EqualValues(t, []string{}, rdb.SPopN(ctx, "nonexisting_key", 100).Val())
	})

	t.Run("SPOP new implementation: code path #1", func(t *testing.T) {
		CreateSet(t, rdb, ctx, "myset", []interface{}{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20})
		cmd := rdb.SPopN(ctx, "myset", 30)
		require.NoError(t, cmd.Err())
		sort.Strings(cmd.Val())
		require.EqualValues(t, []string{"1", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "2", "20", "3", "4", "5", "6", "7", "8", "9"}, cmd.Val())
	})

	t.Run("SPOP new implementation: code path #2", func(t *testing.T) {
		CreateSet(t, rdb, ctx, "myset", []interface{}{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20})
		cmd := rdb.SPopN(ctx, "myset", 2)
		require.NoError(t, cmd.Err())
		require.EqualValues(t, 2, len(cmd.Val()))
		require.EqualValues(t, 18, rdb.SCard(ctx, "myset").Val())
		var array = cmd.Val()
		cmd = rdb.SMembers(ctx, "myset")
		require.NoError(t, cmd.Err())
		array = append(array, cmd.Val()...)
		sort.Strings(array)
		require.EqualValues(t, []string{"1", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "2", "20", "3", "4", "5", "6", "7", "8", "9"}, array)
	})

	t.Run("SPOP new implementation: code path #3", func(t *testing.T) {
		CreateSet(t, rdb, ctx, "myset", []interface{}{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20})
		cmd := rdb.SPopN(ctx, "myset", 18)
		require.NoError(t, cmd.Err())
		require.EqualValues(t, 18, len(cmd.Val()))
		require.EqualValues(t, 2, rdb.SCard(ctx, "myset").Val())
		var array = cmd.Val()
		cmd = rdb.SMembers(ctx, "myset")
		require.NoError(t, cmd.Err())
		array = append(array, cmd.Val()...)
		sort.Strings(array)
		require.EqualValues(t, []string{"1", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "2", "20", "3", "4", "5", "6", "7", "8", "9"}, array)
	})

	t.Run("SPOP with <count> not integer", func(t *testing.T) {
		CreateSet(t, rdb, ctx, "myset", []interface{}{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20})
		require.ErrorContains(t, rdb.Do(ctx, "spop", "myset", 3.5).Err(), "is not an integer")
		require.ErrorContains(t, rdb.Do(ctx, "spop", "myset", -5.5).Err(), "is not an integer")
	})

	t.Run("SPOP with <count> not positive", func(t *testing.T) {
		CreateSet(t, rdb, ctx, "myset", []interface{}{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20})
		require.ErrorContains(t, rdb.Do(ctx, "spop", "myset", -5.5).Err(), "is not an integer")
		require.ErrorContains(t, rdb.SPopN(ctx, "myset", -2).Err(), "must be positive")

		// 0 is an exception, although it is not a positive number, it will not return error, just like redis
		cmd := rdb.SPopN(ctx, "myset", 0)
		require.NoError(t, cmd.Err())
		require.EqualValues(t, []string{}, cmd.Val())
	})

	t.Run("SPOP the num of arguments is not 2 or 3", func(t *testing.T) {
		CreateSet(t, rdb, ctx, "myset", []interface{}{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20})
		require.ErrorContains(t, rdb.Do(ctx, "spop").Err(), "wrong number of arguments")
		require.NoError(t, rdb.Do(ctx, "spop", "myset").Err())
		require.NoError(t, rdb.Do(ctx, "spop", "myset", 1).Err())
		require.ErrorContains(t, rdb.Do(ctx, "spop", "myset", 1, 1).Err(), "wrong number of arguments")
	})

	t.Run("SRANDMEMBER with <count> against non existing key", func(t *testing.T) {
		require.EqualValues(t, "", rdb.SRandMember(ctx, "nonexisting_key").Val())
	})

	t.Run("SRANDMEMBER the num of arguments is not 2 or 3", func(t *testing.T) {
		CreateSet(t, rdb, ctx, "myset", []interface{}{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20})
		require.ErrorContains(t, rdb.Do(ctx, "srandmember").Err(), "wrong number of arguments")
		require.NoError(t, rdb.Do(ctx, "srandmember", "myset").Err())
		require.NoError(t, rdb.Do(ctx, "srandmember", "myset", 1).Err())
		require.ErrorContains(t, rdb.Do(ctx, "srandmember", "myset", 1, 1).Err(), "wrong number of arguments")
	})

	SetupMove := func() {
		require.NoError(t, rdb.Del(ctx, "myset3", "myset4").Err())
		CreateSet(t, rdb, ctx, "myset1", []interface{}{1, "a", "b"})
		CreateSet(t, rdb, ctx, "myset2", []interface{}{2, 3, 4})
	}

	t.Run("SMOVE basics - from regular set to intset", func(t *testing.T) {
		// move a non-integer element to an intset should convert encoding
		SetupMove()
		require.EqualValues(t, true, rdb.SMove(ctx, "myset1", "myset2", "a").Val())
		cmd := rdb.SMembers(ctx, "myset1")
		require.NoError(t, cmd.Err())
		sort.Strings(cmd.Val())
		require.EqualValues(t, []string{"1", "b"}, cmd.Val())
		cmd = rdb.SMembers(ctx, "myset2")
		require.NoError(t, cmd.Err())
		sort.Strings(cmd.Val())
		require.EqualValues(t, []string{"2", "3", "4", "a"}, cmd.Val())
		SetupMove()
		// move an integer element should not convert the encoding
		require.EqualValues(t, true, rdb.SMove(ctx, "myset1", "myset2", 1).Val())
		cmd = rdb.SMembers(ctx, "myset1")
		require.NoError(t, cmd.Err())
		sort.Strings(cmd.Val())
		require.EqualValues(t, []string{"a", "b"}, cmd.Val())
		cmd = rdb.SMembers(ctx, "myset2")
		require.NoError(t, cmd.Err())
		sort.Strings(cmd.Val())
		require.EqualValues(t, []string{"1", "2", "3", "4"}, cmd.Val())
		SetupMove()
	})

	t.Run("SMOVE basics - from intset to regular set", func(t *testing.T) {
		SetupMove()
		require.EqualValues(t, true, rdb.SMove(ctx, "myset2", "myset1", 2).Val())
		cmd := rdb.SMembers(ctx, "myset1")
		require.NoError(t, cmd.Err())
		sort.Strings(cmd.Val())
		require.EqualValues(t, []string{"1", "2", "a", "b"}, cmd.Val())
		cmd = rdb.SMembers(ctx, "myset2")
		require.NoError(t, cmd.Err())
		sort.Strings(cmd.Val())
		require.EqualValues(t, []string{"3", "4"}, cmd.Val())
	})

	t.Run("SMOVE non existing key", func(t *testing.T) {
		SetupMove()
		require.EqualValues(t, false, rdb.SMove(ctx, "myset1", "myset2", "foo").Val())
		require.EqualValues(t, false, rdb.SMove(ctx, "myset1", "myset1", "foo").Val())
		cmd := rdb.SMembers(ctx, "myset1")
		require.NoError(t, cmd.Err())
		sort.Strings(cmd.Val())
		require.EqualValues(t, []string{"1", "a", "b"}, cmd.Val())
		cmd = rdb.SMembers(ctx, "myset2")
		require.NoError(t, cmd.Err())
		sort.Strings(cmd.Val())
		require.EqualValues(t, []string{"2", "3", "4"}, cmd.Val())
	})

	t.Run("SMOVE non existing src set", func(t *testing.T) {
		SetupMove()
		require.EqualValues(t, false, rdb.SMove(ctx, "noset", "myset2", "foo").Val())
		cmd := rdb.SMembers(ctx, "myset2")
		require.NoError(t, cmd.Err())
		sort.Strings(cmd.Val())
		require.EqualValues(t, []string{"2", "3", "4"}, cmd.Val())
	})

	t.Run("SMOVE from regular set to non existing destination set", func(t *testing.T) {
		SetupMove()
		require.EqualValues(t, true, rdb.SMove(ctx, "myset1", "myset3", "a").Val())
		cmd := rdb.SMembers(ctx, "myset1")
		require.NoError(t, cmd.Err())
		sort.Strings(cmd.Val())
		require.EqualValues(t, []string{"1", "b"}, cmd.Val())
		cmd = rdb.SMembers(ctx, "myset3")
		require.NoError(t, cmd.Err())
		sort.Strings(cmd.Val())
		require.EqualValues(t, []string{"a"}, cmd.Val())
	})

	t.Run("SMOVE from intset to non existing destination set", func(t *testing.T) {
		SetupMove()
		require.EqualValues(t, true, rdb.SMove(ctx, "myset2", "myset3", 2).Val())
		cmd := rdb.SMembers(ctx, "myset2")
		require.NoError(t, cmd.Err())
		sort.Strings(cmd.Val())
		require.EqualValues(t, []string{"3", "4"}, cmd.Val())
		cmd = rdb.SMembers(ctx, "myset3")
		require.NoError(t, cmd.Err())
		sort.Strings(cmd.Val())
		require.EqualValues(t, []string{"2"}, cmd.Val())
	})

	t.Run("SMOVE wrong src key type", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "x", 10, 0).Err())
		util.ErrorRegexp(t, rdb.SMove(ctx, "x", "myset2", "foo").Err(), ".*WRONGTYPE.*")
	})

	t.Run("SMOVE wrong dst key type", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "str", 10, 0).Err())
		util.ErrorRegexp(t, rdb.SMove(ctx, "myset2", "str", "foo").Err(), ".*WRONGTYPE.*")
	})

	t.Run("SMOVE with identical source and destination", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "set").Err())
		require.NoError(t, rdb.SAdd(ctx, "set", "a", "b", "c").Err())
		require.NoError(t, rdb.SMove(ctx, "set", "set", "b").Err())
		cmd := rdb.SMembers(ctx, "set")
		require.NoError(t, cmd.Err())
		sort.Strings(cmd.Val())
		require.EqualValues(t, []string{"a", "b", "c"}, cmd.Val())
	})

	t.Run("intsets implementation stress testing", func(t *testing.T) {
		for j := 0; j < 20; j++ {
			s := make(map[string]bool)
			require.NoError(t, rdb.Del(ctx, "s").Err())
			opNum := util.RandomInt(1024)
			for i := 0; i < int(opNum); i++ {
				data := util.RandomValue()
				s[data] = true
				require.NoError(t, rdb.SAdd(ctx, "s", data).Err())
			}
			cmd := rdb.SMembers(ctx, "s")
			require.NoError(t, cmd.Err())
			sort.Strings(cmd.Val())
			expect := make([]string, 0, 1025)
			for key := range s {
				expect = append(expect, key)
			}
			sort.Strings(expect)
			require.EqualValues(t, expect, cmd.Val())

			opNum = int64(len(expect))
			for i := 0; i < int(opNum); i++ {
				cmd := rdb.SPop(ctx, "s")
				require.NoError(t, cmd.Err())
				if _, ok := s[cmd.Val()]; !ok {
					t.FailNow()
				}
				delete(s, cmd.Val())
			}
			require.EqualValues(t, 0, rdb.SCard(ctx, "s").Val())
			require.EqualValues(t, 0, len(s))
		}
	})
}
