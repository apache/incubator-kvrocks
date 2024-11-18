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

package scan

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/apache/kvrocks/tests/gocase/util"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/slices"
)

func TestScanEmptyKey(t *testing.T) {
	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	require.NoError(t, rdb.Set(ctx, "", "empty", 0).Err())
	require.NoError(t, rdb.Set(ctx, "foo", "bar", 0).Err())
	require.Equal(t, []string{"", "foo"}, scanAll(t, rdb))

	require.NoError(t, rdb.SAdd(ctx, "sadd_key", "", "fab", "fiz", "foobar").Err())
	keys, _, err := rdb.SScan(ctx, "sadd_key", 0, "*", 10000).Result()
	require.NoError(t, err)
	slices.Sort(keys)
	keys = slices.Compact(keys)
	require.Equal(t, []string{"", "fab", "fiz", "foobar"}, keys)
}

func TestScanWithNumberCursor(t *testing.T) {
	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()
	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()
	require.NoError(t, rdb.ConfigSet(ctx, "redis-cursor-compatible", "yes").Err())
	ScanTest(t, rdb, ctx)
}

func TestScanWithStringCursor(t *testing.T) {
	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()
	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()
	ScanTest(t, rdb, ctx)
}

func ScanTest(t *testing.T, rdb *redis.Client, ctx context.Context) {

	t.Run("SCAN Basic", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		util.Populate(t, rdb, "", 1000, 10)
		keys := scanAll(t, rdb)
		require.Len(t, keys, 1000)
	})

	t.Run("SCAN COUNT", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		util.Populate(t, rdb, "", 1000, 10)
		keys := scanAll(t, rdb, "count", 5)
		require.Len(t, keys, 1000)
	})

	t.Run("SCAN MATCH", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		util.Populate(t, rdb, "key:", 1000, 10)
		keys := scanAll(t, rdb, "match", "key:*")
		require.Len(t, keys, 1000)
	})

	t.Run("SCAN MATCH non-trivial pattern", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())

		for _, key := range []string{"aa", "aab", "aabb", "ab", "abb", "ba"} {
			require.NoError(t, rdb.Set(ctx, key, "hello", 0).Err())
		}

		keys := scanAll(t, rdb, "match", "a*")
		require.Equal(t, []string{"aa", "aab", "aabb", "ab", "abb"}, keys)

		keys = scanAll(t, rdb, "match", "aa")
		require.Equal(t, []string{"aa"}, keys)

		keys = scanAll(t, rdb, "match", "aa*")
		require.Equal(t, []string{"aa", "aab", "aabb"}, keys)

		keys = scanAll(t, rdb, "match", "a?")
		require.Equal(t, []string{"aa", "ab"}, keys)

		keys = scanAll(t, rdb, "match", "a*?")
		require.Equal(t, []string{"aa", "aab", "aabb", "ab", "abb"}, keys)

		keys = scanAll(t, rdb, "match", "ab*")
		require.Equal(t, []string{"ab", "abb"}, keys)

		keys = scanAll(t, rdb, "match", "*ab")
		require.Equal(t, []string{"aab", "ab"}, keys)

		keys = scanAll(t, rdb, "match", "*ab*")
		require.Equal(t, []string{"aab", "aabb", "ab", "abb"}, keys)

		// Special case: using [b]* instead of b* forces the a full scan of the keyspace,
		// matching every result with the pattern. We ask for exactly one key, but the
		// first 5 keys don't match the pattern. This tests that SCAN returns a valid
		// cursor even when the first [limit] keys don't satisfy the pattern.
		keys = scanAll(t, rdb, "match", "[b]*", "count", "1")
		require.Equal(t, []string{"ba"}, keys)
	})

	t.Run("SCAN guarantees check under write load", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		util.Populate(t, rdb, "", 100, 10)

		// We start scanning here, so keys from 0 to 99 should all be reported at the end of the iteration.
		var keys []string
		c := "0"
		for {
			cursor, keyList := scan(t, rdb, c)

			c = cursor
			keys = append(keys, keyList...)

			if c == "0" {
				break
			}

			// Write 10 random keys at every SCAN iteration.
			for i := 0; i < 10; i++ {
				require.NoError(t, rdb.Set(ctx, fmt.Sprintf("addedkey:%d", util.RandomInt(1000)), "foo", 0).Err())
			}
		}

		var originKeys []string
		for _, key := range keys {
			if strings.Contains(key, "addedkey:") {
				continue
			}
			originKeys = append(originKeys, key)
		}
		originKeys = slices.Compact(originKeys)
		require.Len(t, originKeys, 100)
	})

	t.Run("SCAN with multi namespace", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		require.NoError(t, rdb.ConfigSet(ctx, "requirepass", "foobared").Err())

		tokens := []string{"test_ns_token1", "test_ns_token2"}
		keyPrefixes := []string{"key1*", "key2*"}
		namespaces := []string{"test_ns1", "test_ns2"}

		// Add namespaces and write key
		for i := 0; i < 2; i++ {
			require.NoError(t, rdb.Do(ctx, "AUTH", "foobared").Err())
			require.NoError(t, rdb.Do(ctx, "NAMESPACE", "ADD", namespaces[i], tokens[i]).Err())
			require.NoError(t, rdb.Do(ctx, "AUTH", tokens[i]).Err())

			for k := 0; k < 1000; k++ {
				require.NoError(t, rdb.Set(ctx, fmt.Sprintf("%s:%d", keyPrefixes[i], k), "hello", 0).Err())
			}
			for k := 0; k < 100; k++ {
				require.NoError(t, rdb.Set(ctx, strconv.Itoa(k), "hello", 0).Err())
			}
		}

		// Check SCAN and SCAN MATCH in different namespace
		for i := 0; i < 2; i++ {
			require.NoError(t, rdb.Do(ctx, "AUTH", tokens[i]).Err())

			// SCAN to get all keys
			keys := scanAll(t, rdb)
			require.Len(t, keys, 1100)

			// SCAN MATCH
			keys = scanAll(t, rdb, "match", keyPrefixes[i])
			require.Len(t, keys, 1000)
		}
	})

	t.Run("SSCAN with PATTERN", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "mykey").Err())
		require.NoError(t, rdb.SAdd(ctx, "mykey", "foo", "fab", "fiz", "foobar", 1, 2, 3, 4).Err())
		keys, _, err := rdb.SScan(ctx, "mykey", 0, "foo*", 10000).Result()
		require.NoError(t, err)
		slices.Sort(keys)
		keys = slices.Compact(keys)
		require.Equal(t, []string{"foo", "foobar"}, keys)
	})

	t.Run("HSCAN with PATTERN", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "mykey").Err())
		require.NoError(t, rdb.HMSet(ctx, "mykey", "foo", 1, "fab", 2, "fiz", 3, "foobar", 10, 1, "a", 2, "b", 3, "c", 4, "d").Err())
		keys, _, err := rdb.HScan(ctx, "mykey", 0, "foo*", 10000).Result()
		require.NoError(t, err)
		slices.Sort(keys)
		keys = slices.Compact(keys)
		require.Equal(t, []string{"1", "10", "foo", "foobar"}, keys)
	})

	t.Run("ZSCAN with PATTERN", func(t *testing.T) {
		members := []redis.Z{
			{Score: 1, Member: "foo"},
			{Score: 2, Member: "fab"},
			{Score: 3, Member: "fiz"},
			{Score: 10, Member: "foobar"},
		}
		require.NoError(t, rdb.Del(ctx, "mykey").Err())
		require.NoError(t, rdb.ZAdd(ctx, "mykey", members...).Err())
		keys, _, err := rdb.ZScan(ctx, "mykey", 0, "foo*", 10000).Result()
		require.NoError(t, err)
		slices.Sort(keys)
		keys = slices.Compact(keys)
		require.Equal(t, []string{"1", "10", "foo", "foobar"}, keys)
	})

	for _, test := range []struct {
		name   string
		keyGen func(int) interface{}
	}{
		{"SSCAN with encoding intset", func(i int) interface{} { return i }},
		{"SSCAN with encoding hashtable", func(i int) interface{} { return fmt.Sprintf("ele:%d", i) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			require.NoError(t, rdb.Del(ctx, "set").Err())
			var elements []interface{}
			for i := 0; i < 100; i++ {
				elements = append(elements, test.keyGen(i))
			}
			require.NoError(t, rdb.SAdd(ctx, "set", elements...).Err())
			keys, _, err := rdb.SScan(ctx, "set", 0, "", 10000).Result()
			require.NoError(t, err)
			slices.Sort(keys)
			keys = slices.Compact(keys)
			require.Len(t, keys, 100)
		})
	}

	for _, test := range []struct {
		name  string
		count int
	}{
		{"HSCAN with encoding ziplist", 30},
		{"HSCAN with encoding hashtable", 1000},
	} {
		t.Run(test.name, func(t *testing.T) {
			require.NoError(t, rdb.Del(ctx, "hash").Err())
			var elements []interface{}
			for i := 0; i < test.count; i++ {
				elements = append(elements, fmt.Sprintf("key:%d", i), i)
			}
			require.NoError(t, rdb.HMSet(ctx, "hash", elements...).Err())
			keys, _, err := rdb.HScan(ctx, "hash", 0, "", 10000).Result()
			require.NoError(t, err)
			var hashKeys []string

			var hashKey string
			for _, key := range keys {
				if hashKey != "" {
					require.Equal(t, fmt.Sprintf("key:%s", key), hashKey)
					hashKeys = append(hashKeys, hashKey)
					hashKey = ""
				} else {
					hashKey = key
				}
			}
			require.Len(t, hashKeys, test.count)
		})
	}

	for _, test := range []struct {
		name  string
		count int
	}{
		{"ZSCAN with encoding ziplist", 30},
		{"ZSCAN with encoding skiplist", 1000},
	} {
		t.Run(test.name, func(t *testing.T) {
			require.NoError(t, rdb.Del(ctx, "zset").Err())
			var elements []redis.Z
			for i := 0; i < test.count; i++ {
				elements = append(elements, redis.Z{
					Score:  float64(i),
					Member: fmt.Sprintf("key:%d", i),
				})
			}
			require.NoError(t, rdb.ZAdd(ctx, "zset", elements...).Err())
			keys, _, err := rdb.ZScan(ctx, "zset", 0, "", 10000).Result()
			require.NoError(t, err)
			var zsetKeys []string

			var zsetKey string
			for _, key := range keys {
				if zsetKey != "" {
					require.Equal(t, fmt.Sprintf("key:%s", key), zsetKey)
					zsetKeys = append(zsetKeys, zsetKey)
					zsetKey = ""
				} else {
					zsetKey = key
				}
			}
			require.Len(t, zsetKeys, test.count)
		})
	}

	t.Run("SCAN reject invalid input", func(t *testing.T) {
		util.ErrorRegexp(t, rdb.Do(ctx, "SCAN", "0", "hello").Err(), ".*syntax error.*")
		util.ErrorRegexp(t, rdb.Do(ctx, "SCAN", "0", "hello", "hi").Err(), ".*syntax error.*")
		util.ErrorRegexp(t, rdb.Do(ctx, "SCAN", "0", "count", "1", "hello", "hi").Err(), ".*syntax error.*")
		util.ErrorRegexp(t, rdb.Do(ctx, "SCAN", "0", "hello", "hi", "count", "1").Err(), ".*syntax error.*")
		require.NoError(t, rdb.Do(ctx, "SCAN", "0", "count", "1", "match", "a*").Err())
		require.NoError(t, rdb.Do(ctx, "SCAN", "0", "match", "a*", "count", "1").Err())
		util.ErrorRegexp(t, rdb.Do(ctx, "SCAN", "0", "count", "1", "match", "a*", "hello").Err(), ".*syntax error.*")
		util.ErrorRegexp(t, rdb.Do(ctx, "SCAN", "0", "count", "1", "match", "a*", "hello", "hi").Err(), ".*syntax error.*")

		util.ErrorRegexp(t, rdb.Do(ctx, "SCAN", "0", "match", "[").Err(), ".*Invalid glob pattern.*")
		util.ErrorRegexp(t, rdb.Do(ctx, "SCAN", "0", "match", "\\").Err(), ".*Invalid glob pattern.*")
		util.ErrorRegexp(t, rdb.Do(ctx, "SCAN", "0", "match", "[a").Err(), ".*Invalid glob pattern.*")
		util.ErrorRegexp(t, rdb.Do(ctx, "SCAN", "0", "match", "[a-]").Err(), ".*Invalid glob pattern.*")
	})

	t.Run("SCAN with type args ", func(t *testing.T) {
		//string type
		require.NoError(t, rdb.Set(ctx, "stringtype1", "fee1", 0).Err())
		require.NoError(t, rdb.Set(ctx, "stringtype2", "fee1", 0).Err())
		require.NoError(t, rdb.Set(ctx, "stringtype3", "fee1", 0).Err())
		require.Equal(t, []string{"stringtype1", "stringtype2", "stringtype3"}, scanAll(t, rdb, "match", "stringtype*", "type", "string"))
		require.Equal(t, []string{"stringtype1", "stringtype2", "stringtype3"}, scanAll(t, rdb, "match", "stringtype*", "count", "3", "type", "string"))
		//hash type
		require.NoError(t, rdb.HSet(ctx, "hashtype1", "key1", "val1", "key2", "val2").Err())
		require.NoError(t, rdb.HSet(ctx, "hashtype2", "key1", "val1", "key2", "val2").Err())
		require.NoError(t, rdb.HSet(ctx, "hashtype3", "key1", "val1", "key2", "val2").Err())
		require.Equal(t, []string{"hashtype1", "hashtype2", "hashtype3"}, scanAll(t, rdb, "match", "hashtype*", "type", "hash"))
		require.Equal(t, []string{"hashtype1", "hashtype2", "hashtype3"}, scanAll(t, rdb, "match", "hashtype*", "count", "3", "type", "hash"))
		//list type
		require.NoError(t, rdb.RPush(ctx, "listtype1", "1").Err())
		require.NoError(t, rdb.RPush(ctx, "listtype2", "2").Err())
		require.NoError(t, rdb.RPush(ctx, "listtype3", "3").Err())
		require.Equal(t, []string{"listtype1", "listtype2", "listtype3"}, scanAll(t, rdb, "match", "listtype*", "type", "list"))
		require.Equal(t, []string{"listtype1", "listtype2", "listtype3"}, scanAll(t, rdb, "match", "listtype*", "count", "3", "type", "list"))
		//set type
		require.NoError(t, rdb.SAdd(ctx, "settype1", "1").Err())
		require.NoError(t, rdb.SAdd(ctx, "settype2", "1").Err())
		require.NoError(t, rdb.SAdd(ctx, "settype3", "1").Err())
		require.Equal(t, []string{"settype1", "settype2", "settype3"}, scanAll(t, rdb, "match", "settype*", "type", "set"))
		require.Equal(t, []string{"settype1", "settype2", "settype3"}, scanAll(t, rdb, "match", "settype*", "count", "3", "type", "set"))
		//zet type
		members := []redis.Z{
			{Score: 1, Member: "1"},
			{Score: 2, Member: "2"},
			{Score: 3, Member: "3"},
			{Score: 10, Member: "4"},
		}
		require.NoError(t, rdb.ZAdd(ctx, "zsettype1", members...).Err())
		require.NoError(t, rdb.ZAdd(ctx, "zsettype2", members...).Err())
		require.NoError(t, rdb.ZAdd(ctx, "zsettype3", members...).Err())
		require.Equal(t, []string{"zsettype1", "zsettype2", "zsettype3"}, scanAll(t, rdb, "match", "zsettype*", "type", "zset"))
		require.Equal(t, []string{"zsettype1", "zsettype2", "zsettype3"}, scanAll(t, rdb, "match", "zsettype*", "count", "3", "type", "zset"))
		//bitmap type
		require.NoError(t, rdb.SetBit(ctx, "bitmaptype1", 0, 0).Err())
		require.NoError(t, rdb.SetBit(ctx, "bitmaptype2", 0, 0).Err())
		require.NoError(t, rdb.SetBit(ctx, "bitmaptype3", 0, 0).Err())
		require.Equal(t, []string{"bitmaptype1", "bitmaptype2", "bitmaptype3"}, scanAll(t, rdb, "match", "bitmaptype*", "type", "bitmap"))
		require.Equal(t, []string{"bitmaptype1", "bitmaptype2", "bitmaptype3"}, scanAll(t, rdb, "match", "bitmaptype*", "count", "3", "type", "bitmap"))
		//stream type
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{Stream: "streamtype1", Values: []string{"item", "1", "value", "a"}}).Err())
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{Stream: "streamtype2", Values: []string{"item", "1", "value", "a"}}).Err())
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{Stream: "streamtype3", Values: []string{"item", "1", "value", "a"}}).Err())
		require.Equal(t, []string{"streamtype1", "streamtype2", "streamtype3"}, scanAll(t, rdb, "match", "streamtype*", "type", "stream"))
		require.Equal(t, []string{"streamtype1", "streamtype2", "streamtype3"}, scanAll(t, rdb, "match", "streamtype*", "count", "3", "type", "stream"))
		//MBbloom type
		require.NoError(t, rdb.Do(ctx, "bf.reserve", "MBbloomtype1", "0.02", "1000").Err())
		require.NoError(t, rdb.Do(ctx, "bf.reserve", "MBbloomtype2", "0.02", "1000").Err())
		require.NoError(t, rdb.Do(ctx, "bf.reserve", "MBbloomtype3", "0.02", "1000").Err())
		require.Equal(t, []string{"MBbloomtype1", "MBbloomtype2", "MBbloomtype3"}, scanAll(t, rdb, "match", "MBbloomtype*", "type", "MBbloom--"))
		require.Equal(t, []string{"MBbloomtype1", "MBbloomtype2", "MBbloomtype3"}, scanAll(t, rdb, "match", "MBbloomtype*", "count", "3", "type", "MBbloom--"))
		//ReJSON-RL type
		require.NoError(t, rdb.Do(ctx, "JSON.SET", "ReJSONtype1", "$", ` {"x":1, "y":2} `).Err())
		require.NoError(t, rdb.Do(ctx, "JSON.SET", "ReJSONtype2", "$", ` {"x":1, "y":2} `).Err())
		require.NoError(t, rdb.Do(ctx, "JSON.SET", "ReJSONtype3", "$", ` {"x":1, "y":2} `).Err())
		require.Equal(t, []string{"ReJSONtype1", "ReJSONtype2", "ReJSONtype3"}, scanAll(t, rdb, "match", "ReJSONtype*", "type", "ReJSON-RL"))
		require.Equal(t, []string{"ReJSONtype1", "ReJSONtype2", "ReJSONtype3"}, scanAll(t, rdb, "match", "ReJSONtype*", "count", "3", "type", "ReJSON-RL"))
		//invalid type
		util.ErrorRegexp(t, rdb.Do(ctx, "SCAN", "0", "count", "1", "match", "a*", "type", "hi").Err(), "Invalid type")

	})

}

// SCAN of Kvrocks returns _cursor instead of cursor. Thus, redis.Client Scan can fail with
// `cursor, err := rd.ReadInt()' returns error.
//
// This method provides an alternative to workaround it.
func scan(t testing.TB, rdb *redis.Client, c string, args ...interface{}) (cursor string, keys []string) {
	args = append([]interface{}{"SCAN", c}, args...)
	r := rdb.Do(context.Background(), args...)
	require.NoError(t, r.Err())
	require.Len(t, r.Val(), 2)

	rs := r.Val().([]interface{})
	cursor = rs[0].(string)

	for _, key := range rs[1].([]interface{}) {
		keys = append(keys, key.(string))
	}

	return
}

func scanAll(t testing.TB, rdb *redis.Client, args ...interface{}) (keys []string) {
	c := "0"
	for {
		cursor, keyList := scan(t, rdb, c, args...)

		c = cursor
		keys = append(keys, keyList...)

		if c == "0" {
			slices.Sort(keys)
			keys = slices.Compact(keys)
			return
		}
	}
}
