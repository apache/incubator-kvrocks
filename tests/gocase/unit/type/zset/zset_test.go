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

package zset

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/apache/kvrocks/tests/gocase/util"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/slices"
)

func createZset(rdb *redis.Client, ctx context.Context, key string, items []redis.Z) {
	rdb.Del(ctx, key)
	for _, it := range items {
		rdb.ZAdd(ctx, key, it)
	}
}

func createDefaultZset(rdb *redis.Client, ctx context.Context) {
	createZset(rdb, ctx, "zset", []redis.Z{
		{math.Inf(-1), "a"},
		{1, "b"},
		{2, "c"},
		{3, "d"},
		{4, "e"},
		{5, "f"},
		{math.Inf(1), "g"}})
}

func createDefaultLexZset(rdb *redis.Client, ctx context.Context) {
	createZset(rdb, ctx, "zset", []redis.Z{
		{0, "alpha"},
		{0, "bar"},
		{0, "cool"},
		{0, "down"},
		{0, "elephant"},
		{0, "foo"},
		{0, "great"},
		{0, "hill"},
		{0, "omega"}})
}

func basicTests(t *testing.T, rdb *redis.Client, ctx context.Context, enabledRESP3, encoding string, srv *util.KvrocksServer) {
	isRESP3 := enabledRESP3 == "yes"
	t.Run(fmt.Sprintf("Check encoding - %s", encoding), func(t *testing.T) {
		rdb.Del(ctx, "ztmp")
		rdb.ZAdd(ctx, "ztmp", redis.Z{Score: 10, Member: "x"})
	})

	t.Run(fmt.Sprintf("ZSET basic ZADD and score update - %s", encoding), func(t *testing.T) {
		rdb.Del(ctx, "ztmp")
		rdb.ZAdd(ctx, "ztmp", redis.Z{Score: 10, Member: "x"})
		rdb.ZAdd(ctx, "ztmp", redis.Z{Score: 20, Member: "y"})
		rdb.ZAdd(ctx, "ztmp", redis.Z{Score: 30, Member: "z"})
		require.Equal(t, []string{"x", "y", "z"}, rdb.ZRange(ctx, "ztmp", 0, -1).Val())
		rdb.ZAdd(ctx, "ztmp", redis.Z{Score: 1, Member: "y"})
		require.Equal(t, []string{"y", "x", "z"}, rdb.ZRange(ctx, "ztmp", 0, -1).Val())
	})

	t.Run(fmt.Sprintf("ZSET basic ZADD the same member with different scores - %s", encoding), func(t *testing.T) {
		rdb.Del(ctx, "ztmp")
		require.Equal(t, int64(1), rdb.ZAdd(ctx, "ztmp", redis.Z{Score: 10, Member: "x"}, redis.Z{Score: 20, Member: "x"}).Val())
		require.Equal(t, []string{"x"}, rdb.ZRange(ctx, "ztmp", 0, -1).Val())
		require.Equal(t, float64(20), rdb.ZScore(ctx, "ztmp", "x").Val())

		require.Equal(t, int64(2), rdb.ZAdd(ctx, "ztmp", redis.Z{Score: 30, Member: "x"}, redis.Z{Score: 40, Member: "y"}, redis.Z{Score: 50, Member: "z"}).Val())
		require.Equal(t, []string{"x", "y", "z"}, rdb.ZRange(ctx, "ztmp", 0, -1).Val())
		require.Equal(t, float64(30), rdb.ZScore(ctx, "ztmp", "x").Val())
	})

	t.Run(fmt.Sprintf("ZSET ZADD INCR option supports a single pair - %s", encoding), func(t *testing.T) {
		rdb.Del(ctx, "ztmp")
		require.Equal(t, 1.5, rdb.ZAddArgsIncr(ctx, "ztmp", redis.ZAddArgs{Members: []redis.Z{{Member: "abc", Score: 1.5}}}).Val())
		require.Contains(t, rdb.ZAddArgsIncr(ctx, "ztmp", redis.ZAddArgs{Members: []redis.Z{{Member: "abc", Score: 1.5}, {Member: "adc"}}}).Err(),
			"INCR option supports a single increment-element pair")
	})

	t.Run(fmt.Sprintf("ZSET ZADD IncrMixedOtherOptions - %s", encoding), func(t *testing.T) {
		rdb.Del(ctx, "ztmp")
		if isRESP3 {
			require.Equal(t, 1.5, rdb.Do(ctx, "zadd", "ztmp", "nx", "nx", "nx", "nx", "incr", "1.5", "abc").Val())
			require.Equal(t, redis.Nil, rdb.Do(ctx, "zadd", "ztmp", "nx", "nx", "nx", "nx", "incr", "1.5", "abc").Err())
			require.EqualValues(t, 3, rdb.Do(ctx, "zadd", "ztmp", "xx", "xx", "xx", "xx", "incr", "1.5", "abc").Val())
		} else {
			require.Equal(t, "1.5", rdb.Do(ctx, "zadd", "ztmp", "nx", "nx", "nx", "nx", "incr", "1.5", "abc").Val())
			require.Equal(t, redis.Nil, rdb.Do(ctx, "zadd", "ztmp", "nx", "nx", "nx", "nx", "incr", "1.5", "abc").Err())
			require.Equal(t, "3", rdb.Do(ctx, "zadd", "ztmp", "xx", "xx", "xx", "xx", "incr", "1.5", "abc").Val())
		}

		rdb.Del(ctx, "ztmp")
		require.Equal(t, 1.5, rdb.ZAddArgsIncr(ctx, "ztmp", redis.ZAddArgs{NX: true, Members: []redis.Z{{Member: "abc", Score: 1.5}}}).Val())
		require.Equal(t, redis.Nil, rdb.ZAddArgsIncr(ctx, "ztmp", redis.ZAddArgs{NX: true, Members: []redis.Z{{Member: "abc", Score: 1.5}}}).Err())

		rdb.Del(ctx, "ztmp")
		require.Equal(t, redis.Nil, rdb.ZAddArgsIncr(ctx, "ztmp", redis.ZAddArgs{XX: true, Members: []redis.Z{{Member: "abc", Score: 1.5}}}).Err())
		require.Equal(t, 1.5, rdb.ZAddArgsIncr(ctx, "ztmp", redis.ZAddArgs{NX: true, Members: []redis.Z{{Member: "abc", Score: 1.5}}}).Val())

		rdb.Del(ctx, "ztmp")
		require.Equal(t, 1.5, rdb.ZAddArgsIncr(ctx, "ztmp", redis.ZAddArgs{NX: true, Members: []redis.Z{{Member: "abc", Score: 1.5}}}).Val())
		require.Equal(t, 3.0, rdb.ZAddArgsIncr(ctx, "ztmp", redis.ZAddArgs{GT: true, Members: []redis.Z{{Member: "abc", Score: 1.5}}}).Val())
		require.Equal(t, 0.0, rdb.ZAddArgsIncr(ctx, "ztmp", redis.ZAddArgs{GT: true, Members: []redis.Z{{Member: "abc", Score: -1.5}}}).Val())
		require.Equal(t, redis.Nil, rdb.ZAddArgsIncr(ctx, "ztmp", redis.ZAddArgs{GT: true, Members: []redis.Z{{Member: "abc", Score: -1.5}}}).Err())

		rdb.Del(ctx, "ztmp")
		require.Equal(t, 1.5, rdb.ZAddArgsIncr(ctx, "ztmp", redis.ZAddArgs{NX: true, Members: []redis.Z{{Member: "abc", Score: 1.5}}}).Val())
		require.Equal(t, 0.0, rdb.ZAddArgsIncr(ctx, "ztmp", redis.ZAddArgs{LT: true, Members: []redis.Z{{Member: "abc", Score: -1.5}}}).Val())
		require.Equal(t, 0.0, rdb.ZAddArgsIncr(ctx, "ztmp", redis.ZAddArgs{LT: true, Members: []redis.Z{{Member: "abc", Score: 1.5}}}).Val())
		require.Equal(t, redis.Nil, rdb.ZAddArgsIncr(ctx, "ztmp", redis.ZAddArgs{LT: true, Members: []redis.Z{{Member: "abc", Score: 1.5}}}).Err())
	})

	t.Run(fmt.Sprintf("ZSET ZADD LT/GT with other options - %s", encoding), func(t *testing.T) {
		rdb.Del(ctx, "ztmp")
		require.EqualValues(t, 1, rdb.ZAddArgs(ctx, "ztmp", redis.ZAddArgs{Members: []redis.Z{{Member: "abc", Score: 1.5}}}).Val())
		require.EqualValues(t, 1, rdb.ZAddArgs(ctx, "ztmp", redis.ZAddArgs{GT: true, Ch: true, Members: []redis.Z{{Member: "abc", Score: 2.5}}}).Val())
		require.EqualValues(t, 0, rdb.ZAddArgs(ctx, "ztmp", redis.ZAddArgs{GT: true, Ch: true, Members: []redis.Z{{Member: "abc", Score: 2.5}}}).Val())
		require.EqualValues(t, 0, rdb.ZAddArgs(ctx, "ztmp", redis.ZAddArgs{GT: true, Ch: false, Members: []redis.Z{{Member: "abc", Score: 2.5}}}).Val())
		require.EqualValues(t, 0, rdb.ZAddArgs(ctx, "ztmp", redis.ZAddArgs{GT: true, Ch: false, Members: []redis.Z{{Member: "abc", Score: 100}}}).Val())
		require.Contains(t, rdb.Do(ctx, "zadd", "ztmp", "lt", "gt", "1", "m1", "2", "m2").Err(),
			"GT, LT, and/or NX options at the same time are not compatible")

		rdb.Del(ctx, "ztmp")
		require.EqualValues(t, 1, rdb.ZAddArgs(ctx, "ztmp", redis.ZAddArgs{LT: true, Ch: true, Members: []redis.Z{{Member: "abc", Score: 1.5}}}).Val())
		require.EqualValues(t, 1, rdb.ZAddArgs(ctx, "ztmp", redis.ZAddArgs{LT: true, Ch: true, Members: []redis.Z{{Member: "abc", Score: 1.2}}}).Val())
		require.EqualValues(t, 0, rdb.ZAddArgs(ctx, "ztmp", redis.ZAddArgs{LT: true, Ch: false, Members: []redis.Z{{Member: "abc", Score: 0.5}}}).Val())

		rdb.Del(ctx, "newAbc1", "newAbc2")
		require.EqualValues(t, 2, rdb.ZAddArgs(ctx, "ztmp", redis.ZAddArgs{Ch: true, Members: []redis.Z{{Member: "abc", Score: 0.5}, {Member: "newAbc1", Score: 10}, {Member: "newAbc2"}}}).Val())
	})

	t.Run(fmt.Sprintf("ZSET ZADD NX/XX option supports a single pair - %s", encoding), func(t *testing.T) {
		rdb.Del(ctx, "ztmp")
		require.EqualValues(t, 2, rdb.ZAddArgs(ctx, "ztmp", redis.ZAddArgs{NX: true, Members: []redis.Z{{Member: "a", Score: 1}, {Member: "b", Score: 2}}}).Val())
		require.EqualValues(t, 1, rdb.ZAddArgs(ctx, "ztmp", redis.ZAddArgs{NX: true, Members: []redis.Z{{Member: "c", Score: 3}}}).Val())

		rdb.Del(ctx, "ztmp")
		require.EqualValues(t, 1, rdb.ZAddArgs(ctx, "ztmp", redis.ZAddArgs{NX: true, Members: []redis.Z{{Member: "abc", Score: 1.5}}}).Val())
		require.EqualValues(t, 0, rdb.ZAddArgs(ctx, "ztmp", redis.ZAddArgs{NX: true, Members: []redis.Z{{Member: "abc", Score: 1.5}}}).Val())
		require.EqualValues(t, 1, rdb.ZAddArgs(ctx, "ztmp", redis.ZAddArgs{XX: true, Ch: true, Members: []redis.Z{{Member: "abc", Score: 2.5}}}).Val())
		require.EqualValues(t, 0, rdb.ZAddArgs(ctx, "ztmp", redis.ZAddArgs{XX: true, Ch: true, Members: []redis.Z{{Member: "abc", Score: 2.5}}}).Val())
		require.Contains(t, rdb.Do(ctx, "zadd", "ztmp", "nx", "xx", "1", "m1", "2", "m2").Err(),
			"XX and NX options at the same time are not compatible")

		require.Contains(t, rdb.Do(ctx, "zadd", "ztmp", "lt", "nx", "1", "m1", "2", "m2").Err(),
			"GT, LT, and/or NX options at the same time are not compatible")
		require.Contains(t, rdb.Do(ctx, "zadd", "ztmp", "gt", "nx", "1", "m1", "2", "m2").Err(),
			"GT, LT, and/or NX options at the same time are not compatible")

		rdb.Del(ctx, "ztmp")
		require.EqualValues(t, 1, rdb.ZAddArgs(ctx, "ztmp", redis.ZAddArgs{NX: true, Ch: true, Members: []redis.Z{{Member: "abc", Score: 1.5}}}).Val())
		require.EqualValues(t, 0, rdb.ZAddArgs(ctx, "ztmp", redis.ZAddArgs{NX: true, Members: []redis.Z{{Member: "abc", Score: 1.5}}}).Val())
	})

	t.Run(fmt.Sprintf("ZSET element can't be set to NaN with ZADD - %s", encoding), func(t *testing.T) {
		require.Contains(t, rdb.ZAdd(ctx, "myzset", redis.Z{Score: math.NaN(), Member: "abc"}).Err(), "float")
	})

	t.Run("ZSET element can't be set to NaN with ZINCRBY", func(t *testing.T) {
		require.Contains(t, rdb.ZAdd(ctx, "myzset", redis.Z{Score: math.NaN(), Member: "abc"}).Err(), "float")
	})

	t.Run("ZINCRBY calls leading to NaN result in error", func(t *testing.T) {
		rdb.ZIncrBy(ctx, "myzset", math.Inf(1), "abc")
		util.ErrorRegexp(t, rdb.ZIncrBy(ctx, "myzset", math.Inf(-1), "abc").Err(), ".*NaN.*")
	})

	t.Run("ZADD - Variadic version base case", func(t *testing.T) {
		rdb.Del(ctx, "myzset")
		require.Equal(t, int64(3), rdb.ZAdd(ctx, "myzset", redis.Z{Score: 10, Member: "a"}, redis.Z{Score: 20, Member: "b"}, redis.Z{Score: 30, Member: "c"}).Val())
		require.Equal(t, []redis.Z{{10, "a"}, {20, "b"}, {30, "c"}}, rdb.ZRangeWithScores(ctx, "myzset", 0, -1).Val())
	})

	t.Run("ZADD - Return value is the number of actually added items", func(t *testing.T) {
		require.Equal(t, int64(1), rdb.ZAdd(ctx, "myzset", redis.Z{Score: 5, Member: "x"}, redis.Z{Score: 20, Member: "b"}, redis.Z{Score: 30, Member: "c"}).Val())
		require.Equal(t, []redis.Z{{5, "x"}, {10, "a"}, {20, "b"}, {30, "c"}}, rdb.ZRangeWithScores(ctx, "myzset", 0, -1).Val())
	})

	t.Run("ZADD - Variadic version will raise error on missing arg", func(t *testing.T) {
		rdb.Del(ctx, "myzset")
		util.ErrorRegexp(t, rdb.Do(ctx, "zadd", "myzset", 10, "a", 20, "b", 30, "c", 40).Err(), ".*syntax.*")
	})

	t.Run("ZADD - invalid score will raise error", func(t *testing.T) {
		rdb.Del(ctx, "myzet")
		require.ErrorContains(t, rdb.Do(ctx, "zadd", "myzet", "one", "one").Err(), "is not a valid float")
		require.ErrorContains(t, rdb.Do(ctx, "zadd", "myzet", "3.3.3", "one").Err(), "is not a valid float")
	})

	t.Run("ZINCRBY does not work variadic even if shares ZADD implementation", func(t *testing.T) {
		rdb.Del(ctx, "myzset")
		util.ErrorRegexp(t, rdb.Do(ctx, "zincrby", "myzset", 10, "a", 20, "b", 30, "c").Err(), ".*ERR.*wrong.*number.*arg.*")
	})

	t.Run("ZINCRBY - invalid increment will raise error", func(t *testing.T) {
		rdb.Del(ctx, "myzet")
		require.NoError(t, rdb.ZIncrBy(ctx, "myzet", 1, "one").Err())
		require.ErrorContains(t, rdb.Do(ctx, "zincrby", "myzet", "one", "one").Err(), "is not a valid float")
		require.ErrorContains(t, rdb.Do(ctx, "zincrby", "myzet", "3.3.3", "one").Err(), "is not a valid float")
	})

	t.Run("ZLEXCOUNT - invalid range will raise error", func(t *testing.T) {
		rdb.Del(ctx, "myzet")
		require.ErrorContains(t, rdb.ZLexCount(ctx, "myzet", "+", "-").Err(), "min > max")
		require.ErrorContains(t, rdb.ZLexCount(ctx, "myzet", "x", "[y").Err(), "the min is illegal")
		require.ErrorContains(t, rdb.ZLexCount(ctx, "myzet", "[x", "y").Err(), "the max is illegal")
	})

	t.Run("ZPOP - the num of arguments is not 2 or 3", func(t *testing.T) {
		rdb.Del(ctx, "myzet")
		require.ErrorContains(t, rdb.Do(ctx, "zpopmin", "myzet", "1", "1").Err(), "wrong number of arguments")
		require.ErrorContains(t, rdb.Do(ctx, "zpopmax", "myzet", "1", "1").Err(), "wrong number of arguments")
	})

	t.Run("ZUNIONSTORE - invalid numkeys will raise error", func(t *testing.T) {
		rdb.Del(ctx, "out")
		rdb.Del(ctx, "myzet")
		require.ErrorContains(t, rdb.Do(ctx, "zunionstore", "out", "one", "myzet").Err(), "value is not an integer or out of range")
		require.ErrorContains(t, rdb.Do(ctx, "zunionstore", "out", "3.3", "myzet").Err(), "value is not an integer or out of range")
	})

	t.Run("ZUNIONSTORE - invalid weights will raise error", func(t *testing.T) {
		rdb.Del(ctx, "out")
		rdb.Del(ctx, "myzet")
		require.ErrorContains(t, rdb.Do(ctx, "zunionstore", "out", "1", "myzet", "weights", "one").Err(), "weight is not a double or out of range")
		require.ErrorContains(t, rdb.Do(ctx, "zunionstore", "out", "1", "myzet", "weights", "3.3.3").Err(), "weight is not a double or out of range")
	})

	t.Run("ZUNIONSTORE - invalid aggregate will raise error", func(t *testing.T) {
		rdb.Del(ctx, "out")
		rdb.Del(ctx, "myzet")
		require.ErrorContains(t, rdb.Do(ctx, "zunionstore", "out", "1", "myzet", "aggregate", "xxx").Err(), "aggregate param error")
	})

	t.Run(fmt.Sprintf("ZCARD basics - %s", encoding), func(t *testing.T) {
		rdb.Del(ctx, "ztmp")
		rdb.ZAdd(ctx, "ztmp", redis.Z{Score: 10, Member: "a"}, redis.Z{Score: 20, Member: "b"}, redis.Z{Score: 30, Member: "c"})
		require.Equal(t, int64(3), rdb.ZCard(ctx, "ztmp").Val())
		require.Equal(t, int64(0), rdb.ZCard(ctx, "zdoesntexist").Val())
	})

	t.Run("ZREM removes key after last element is removed", func(t *testing.T) {
		rdb.Del(ctx, "ztmp")
		rdb.ZAdd(ctx, "ztmp", redis.Z{Score: 10, Member: "x"}, redis.Z{Score: 20, Member: "y"})
		require.Equal(t, int64(1), rdb.Exists(ctx, "ztmp").Val())
		require.Equal(t, int64(0), rdb.ZRem(ctx, "ztmp", "z").Val())
		require.Equal(t, int64(1), rdb.ZRem(ctx, "ztmp", "y").Val())
		require.Equal(t, int64(1), rdb.ZRem(ctx, "ztmp", "x").Val())
		require.Equal(t, int64(0), rdb.Exists(ctx, "ztmp").Val())
	})

	t.Run("ZREM variadic version", func(t *testing.T) {
		rdb.Del(ctx, "ztmp")
		rdb.ZAdd(ctx, "ztmp", redis.Z{Score: 10, Member: "a"}, redis.Z{Score: 20, Member: "b"}, redis.Z{Score: 30, Member: "c"})
		require.Equal(t, int64(2), rdb.ZRem(ctx, "ztmp", []string{"x", "y", "a", "b", "k"}).Val())
		require.Equal(t, int64(0), rdb.ZRem(ctx, "ztmp", []string{"foo", "bar"}).Val())
		require.Equal(t, int64(1), rdb.ZRem(ctx, "ztmp", []string{"c"}).Val())
		require.Equal(t, int64(0), rdb.Exists(ctx, "ztmp").Val())
	})

	t.Run("ZREM variadic version -- remove elements after key deletion", func(t *testing.T) {
		rdb.Del(ctx, "ztmp")
		rdb.ZAdd(ctx, "ztmp", redis.Z{Score: 10, Member: "a"}, redis.Z{Score: 20, Member: "b"}, redis.Z{Score: 30, Member: "c"})
		require.Equal(t, int64(3), rdb.ZRem(ctx, "ztmp", []string{"a", "b", "c", "d", "e", "f", "g"}).Val())
	})

	t.Run(fmt.Sprintf("ZPOPMIN basics - %s", encoding), func(t *testing.T) {
		rdb.Del(ctx, "ztmp")
		rdb.ZAdd(ctx, "ztmp", redis.Z{Score: 10, Member: "a"}, redis.Z{Score: 20, Member: "b"}, redis.Z{Score: 30, Member: "c"})
		require.EqualValues(t, 3, rdb.ZCard(ctx, "ztmp").Val())
		require.Equal(t, []redis.Z{{Score: 10, Member: "a"}}, rdb.ZPopMin(ctx, "ztmp").Val())
		require.EqualValues(t, 2, rdb.ZCard(ctx, "ztmp").Val())
		require.Equal(t, []redis.Z{{Score: 20, Member: "b"}}, rdb.ZPopMin(ctx, "ztmp").Val())
		require.EqualValues(t, 1, rdb.ZCard(ctx, "ztmp").Val())
		require.Equal(t, []redis.Z{{Score: 30, Member: "c"}}, rdb.ZPopMin(ctx, "ztmp").Val())
		require.EqualValues(t, 0, rdb.ZCard(ctx, "ztmp").Val())
		require.Equal(t, []redis.Z{}, rdb.ZPopMin(ctx, "ztmp").Val())
		rdb.ZAdd(ctx, "ztmp", redis.Z{Score: 10, Member: "a"}, redis.Z{Score: 20, Member: "b"}, redis.Z{Score: 30, Member: "c"})
		require.EqualValues(t, 3, rdb.ZCard(ctx, "ztmp").Val())
		require.Equal(t, []redis.Z{{Score: 10, Member: "a"}, {Score: 20, Member: "b"}}, rdb.ZPopMin(ctx, "ztmp", 2).Val())
		require.EqualValues(t, 1, rdb.ZCard(ctx, "ztmp").Val())
		require.Equal(t, []redis.Z{{Score: 30, Member: "c"}}, rdb.ZPopMin(ctx, "ztmp", 3).Val())
	})

	t.Run(fmt.Sprintf("ZPOPMAX basics - %s", encoding), func(t *testing.T) {
		rdb.Del(ctx, "ztmp")
		rdb.ZAdd(ctx, "ztmp", redis.Z{Score: 10, Member: "a"}, redis.Z{Score: 20, Member: "b"}, redis.Z{Score: 30, Member: "c"})
		require.EqualValues(t, 3, rdb.ZCard(ctx, "ztmp").Val())
		require.Equal(t, []redis.Z{{Score: 30, Member: "c"}}, rdb.ZPopMax(ctx, "ztmp").Val())
		require.EqualValues(t, 2, rdb.ZCard(ctx, "ztmp").Val())
		require.Equal(t, []redis.Z{{Score: 20, Member: "b"}}, rdb.ZPopMax(ctx, "ztmp").Val())
		require.EqualValues(t, 1, rdb.ZCard(ctx, "ztmp").Val())
		require.Equal(t, []redis.Z{{Score: 10, Member: "a"}}, rdb.ZPopMax(ctx, "ztmp").Val())
		require.EqualValues(t, 0, rdb.ZCard(ctx, "ztmp").Val())
		require.Equal(t, []redis.Z{}, rdb.ZPopMax(ctx, "ztmp").Val())
		rdb.ZAdd(ctx, "ztmp", redis.Z{Score: 10, Member: "a"}, redis.Z{Score: 20, Member: "b"}, redis.Z{Score: 30, Member: "c"})
		require.EqualValues(t, 3, rdb.ZCard(ctx, "ztmp").Val())
		require.Equal(t, []redis.Z{{Score: 30, Member: "c"}, {Score: 20, Member: "b"}}, rdb.ZPopMax(ctx, "ztmp", 2).Val())
		require.EqualValues(t, 1, rdb.ZCard(ctx, "ztmp").Val())
		require.Equal(t, []redis.Z{{Score: 10, Member: "a"}}, rdb.ZPopMax(ctx, "ztmp", 3).Val())
	})

	t.Run(fmt.Sprintf("BZPOPMIN basics - %s", encoding), func(t *testing.T) {
		rdb.Del(ctx, "zseta")
		rdb.Del(ctx, "zsetb")
		rdb.ZAdd(ctx, "zseta", redis.Z{Score: 1, Member: "a"}, redis.Z{Score: 2, Member: "b"}, redis.Z{Score: 3, Member: "c"})
		rdb.ZAdd(ctx, "zsetb", redis.Z{Score: 1, Member: "d"}, redis.Z{Score: 2, Member: "e"})
		require.EqualValues(t, 3, rdb.ZCard(ctx, "zseta").Val())
		require.EqualValues(t, 2, rdb.ZCard(ctx, "zsetb").Val())
		// TODO: Remove time.Sleep after fix issue #2473
		time.Sleep(time.Millisecond * 100)
		resultz := rdb.BZPopMin(ctx, 0, "zseta", "zsetb").Val().Z
		require.Equal(t, redis.Z{Score: 1, Member: "a"}, resultz)
		resultz = rdb.BZPopMin(ctx, 0, "zseta", "zsetb").Val().Z
		require.Equal(t, redis.Z{Score: 2, Member: "b"}, resultz)
		resultz = rdb.BZPopMin(ctx, 0, "zsetb", "zseta").Val().Z
		require.Equal(t, redis.Z{Score: 1, Member: "d"}, resultz)
		resultz = rdb.BZPopMin(ctx, 0, "zsetb", "zseta").Val().Z
		require.Equal(t, redis.Z{Score: 2, Member: "e"}, resultz)
		resultz = rdb.BZPopMin(ctx, 0, "zseta", "zsetb").Val().Z
		require.Equal(t, redis.Z{Score: 3, Member: "c"}, resultz)
		var err = rdb.BZPopMin(ctx, time.Millisecond*1000, "zseta", "zsetb").Err()
		require.Equal(t, redis.Nil, err)

		rd := srv.NewTCPClient()
		defer func() { require.NoError(t, rd.Close()) }()
		time.Sleep(time.Millisecond * 100)
		require.NoError(t, rd.WriteArgs("bzpopmin", "zseta", "0"))
		time.Sleep(time.Millisecond * 100)
		rdb.ZAdd(ctx, "zseta", redis.Z{Score: 1, Member: "a"})
		rd.MustReadStrings(t, []string{"zseta", "a", "1"})
	})

	t.Run(fmt.Sprintf("BZPOPMAX basics - %s", encoding), func(t *testing.T) {
		rdb.Del(ctx, "zseta")
		rdb.Del(ctx, "zsetb")
		rdb.ZAdd(ctx, "zseta", redis.Z{Score: 1, Member: "a"}, redis.Z{Score: 2, Member: "b"}, redis.Z{Score: 3, Member: "c"})
		rdb.ZAdd(ctx, "zsetb", redis.Z{Score: 1, Member: "d"}, redis.Z{Score: 2, Member: "e"})
		require.EqualValues(t, 3, rdb.ZCard(ctx, "zseta").Val())
		require.EqualValues(t, 2, rdb.ZCard(ctx, "zsetb").Val())
		time.Sleep(time.Millisecond * 100)
		resultz := rdb.BZPopMax(ctx, 0, "zseta", "zsetb").Val().Z
		require.Equal(t, redis.Z{Score: 3, Member: "c"}, resultz)
		resultz = rdb.BZPopMax(ctx, 0, "zseta", "zsetb").Val().Z
		require.Equal(t, redis.Z{Score: 2, Member: "b"}, resultz)
		resultz = rdb.BZPopMax(ctx, 0, "zsetb", "zseta").Val().Z
		require.Equal(t, redis.Z{Score: 2, Member: "e"}, resultz)
		resultz = rdb.BZPopMax(ctx, 0, "zsetb", "zseta").Val().Z
		require.Equal(t, redis.Z{Score: 1, Member: "d"}, resultz)
		resultz = rdb.BZPopMax(ctx, 0, "zseta", "zsetb").Val().Z
		require.Equal(t, redis.Z{Score: 1, Member: "a"}, resultz)
		var err = rdb.BZPopMin(ctx, time.Millisecond*1000, "zseta", "zsetb").Err()
		require.Equal(t, redis.Nil, err)

		rd := srv.NewTCPClient()
		defer func() { require.NoError(t, rd.Close()) }()
		time.Sleep(time.Millisecond * 100)
		require.NoError(t, rd.WriteArgs("bzpopmax", "zseta", "0"))
		time.Sleep(time.Millisecond * 100)
		rdb.ZAdd(ctx, "zseta", redis.Z{Score: 1, Member: "a"})
		rd.MustReadStrings(t, []string{"zseta", "a", "1"})
	})

	t.Run(fmt.Sprintf("ZMPOP basics - %s", encoding), func(t *testing.T) {
		rdb.Del(ctx, "zseta")
		rdb.Del(ctx, "zsetb")
		require.Equal(t, redis.Nil, rdb.ZMPop(ctx, "min", 1, "nosuchkey").Err())
		require.EqualValues(t, 3, rdb.ZAdd(ctx, "zseta", redis.Z{Score: 10, Member: "a"}, redis.Z{Score: 20, Member: "b"}, redis.Z{Score: 30, Member: "c"}).Val())
		var key, zset = rdb.ZMPop(ctx, "min", 1, "zseta").Val()
		require.Equal(t, "zseta", key)
		require.Equal(t, []redis.Z{{Score: 10, Member: "a"}}, zset)
		require.Equal(t, []redis.Z{{Score: 20, Member: "b"}, {Score: 30, Member: "c"}}, rdb.ZRangeWithScores(ctx, "zseta", 0, -1).Val())
		key, zset = rdb.ZMPop(ctx, "max", 10, "zseta").Val()
		require.Equal(t, "zseta", key)
		require.Equal(t, []redis.Z{{Score: 30, Member: "c"}, {Score: 20, Member: "b"}}, zset)
		require.EqualValues(t, 3, rdb.ZAdd(ctx, "zsetb", redis.Z{Score: 40, Member: "d"}, redis.Z{Score: 50, Member: "e"}, redis.Z{Score: 60, Member: "f"}).Val())
		key, zset = rdb.ZMPop(ctx, "min", 10, "zseta", "zsetb").Val()
		require.Equal(t, "zsetb", key)
		require.Equal(t, []redis.Z{{Score: 40, Member: "d"}, {Score: 50, Member: "e"}, {Score: 60, Member: "f"}}, zset)
		require.Equal(t, redis.Nil, rdb.ZMPop(ctx, "max", 10, "zseta", "zsetb").Err())
		require.EqualValues(t, 0, rdb.Exists(ctx, "zseta", "zsetb").Val())
	})

	t.Run(fmt.Sprintf("ZMPOP error - %s", encoding), func(t *testing.T) {
		rdb.Del(ctx, "zseta")
		rdb.Del(ctx, "zsetb")

		util.ErrorRegexp(t, rdb.Do(ctx, "zmpop", 1, "zseta").Err(), ".*wrong number of arguments.*")
		util.ErrorRegexp(t, rdb.Do(ctx, "zmpop", "wrong_numkeys", "zseta", "zsetb").Err(), ".*not started as an integer.*")
		util.ErrorRegexp(t, rdb.Do(ctx, "zmpop", 2, "zseta", "min").Err(), ".*syntax error.*")
		util.ErrorRegexp(t, rdb.Do(ctx, "zmpop", 2, "zseta", "zsetb", "min", "min").Err(), ".*syntax error.*")
		util.ErrorRegexp(t, rdb.Do(ctx, "zmpop", 1, "zseta", "min", "max").Err(), ".*syntax error.*")
		util.ErrorRegexp(t, rdb.Do(ctx, "zmpop", 1, "zseta", "min", "count", "wrong_count").Err(), ".*not started as an integer.*")
		util.ErrorRegexp(t, rdb.Do(ctx, "zmpop", 1, "zseta", "min", "count", 1, "count", 10).Err(), ".*syntax error.*")
	})

	t.Run(fmt.Sprintf("BZMPOP basics - %s", encoding), func(t *testing.T) {
		rdb.Del(ctx, "zseta")
		rdb.Del(ctx, "zsetb")
		rdb.ZAdd(ctx, "zseta", redis.Z{Score: 1, Member: "a"}, redis.Z{Score: 2, Member: "b"}, redis.Z{Score: 3, Member: "c"})
		rdb.ZAdd(ctx, "zsetb", redis.Z{Score: 1, Member: "d"}, redis.Z{Score: 2, Member: "e"})
		require.EqualValues(t, 3, rdb.ZCard(ctx, "zseta").Val())
		require.EqualValues(t, 2, rdb.ZCard(ctx, "zsetb").Val())
		var key, zset = rdb.BZMPop(ctx, 0, "min", 1, "zseta").Val()
		require.Equal(t, "zseta", key)
		require.Equal(t, []redis.Z{{Score: 1, Member: "a"}}, zset)
		key, zset = rdb.BZMPop(ctx, 0, "max", 2, "zsetb").Val()
		require.Equal(t, "zsetb", key)
		require.Equal(t, []redis.Z{{Score: 2, Member: "e"}, {Score: 1, Member: "d"}}, zset)
		key, zset = rdb.BZMPop(ctx, 0, "min", 3, "zseta").Val()
		require.Equal(t, "zseta", key)
		require.Equal(t, []redis.Z{{Score: 2, Member: "b"}, {Score: 3, Member: "c"}}, zset)
		require.Equal(t, redis.Nil, rdb.BZMPop(ctx, time.Millisecond*1000, "max", 10, "zseta", "zsetb").Err())

		rd := srv.NewClient()
		defer func() { require.NoError(t, rd.Close()) }()
		ch := make(chan *redis.ZSliceWithKeyCmd)
		go func() {
			ch <- rd.BZMPop(ctx, 0, "min", 10, "zseta")
		}()
		require.Eventually(t, func() bool {
			cnt, _ := strconv.Atoi(util.FindInfoEntry(rdb, "blocked_clients"))
			return cnt == 1
		}, 5*time.Second, 100*time.Millisecond)
		rdb.ZAdd(ctx, "zseta", redis.Z{Score: 1, Member: "a"}, redis.Z{Score: 2, Member: "b"})
		r := <-ch
		key, zset = r.Val()
		require.Equal(t, "zseta", key)
		require.Equal(t, []redis.Z{{Score: 1, Member: "a"}, {Score: 2, Member: "b"}}, zset)
	})

	t.Run(fmt.Sprintf("BZMPOP error - %s", encoding), func(t *testing.T) {
		rdb.Del(ctx, "zseta")
		rdb.Del(ctx, "zsetb")

		util.ErrorRegexp(t, rdb.Do(ctx, "bzmpop", 0.1, 1, "zseta").Err(), ".*wrong number of arguments.*")
		util.ErrorRegexp(t, rdb.Do(ctx, "bzmpop", "wrong_timeout", 1, "zseta", "min").Err(), ".*not started as a number.*")
		util.ErrorRegexp(t, rdb.Do(ctx, "bzmpop", 0.1, "wrong_numkeys", "zseta", "min").Err(), ".*not started as an integer.*")
		util.ErrorRegexp(t, rdb.Do(ctx, "bzmpop", 0.1, 2, "zseta", "min").Err(), ".*syntax error.*")
		util.ErrorRegexp(t, rdb.Do(ctx, "bzmpop", 0.1, 1, "zseta", "min", "max").Err(), ".*syntax error.*")
		util.ErrorRegexp(t, rdb.Do(ctx, "bzmpop", 0.1, 2, "zseta", "zsetb", "min", "min").Err(), ".*syntax error.*")
		util.ErrorRegexp(t, rdb.Do(ctx, "bzmpop", 0.1, 1, "zseta", "min", "count", "wrong_count").Err(), ".*not started as an integer.*")
		util.ErrorRegexp(t, rdb.Do(ctx, "bzmpop", 0.1, 1, "zseta", "min", "count", 1, "count", 10).Err(), ".*syntax error.*")
	})

	t.Run(fmt.Sprintf("ZRANGESTORE arity check - %s", encoding), func(t *testing.T) {
		rdb.Del(ctx, "zsrc")
		rdb.Del(ctx, "zdst")

		util.ErrorRegexp(t, rdb.Do(ctx, "zrangestore", "zdst", "zsrc").Err(), ".*wrong number of arguments.*")
		util.ErrorRegexp(t, rdb.Do(ctx, "zrangestore", "zdst", "zsrc", 0).Err(), ".*wrong number of arguments.*")
	})

	t.Run(fmt.Sprintf("ZRANGESTORE basics - %s", encoding), func(t *testing.T) {
		rdb.Del(ctx, "zsrc")
		rdb.Del(ctx, "zdst")

		rdb.ZAdd(ctx, "zsrc", redis.Z{Score: 1, Member: "a"})
		rdb.ZAdd(ctx, "zsrc", redis.Z{Score: 3, Member: "b"})
		rdb.ZAdd(ctx, "zsrc", redis.Z{Score: 4, Member: "c"})
		rdb.ZAdd(ctx, "zsrc", redis.Z{Score: 6, Member: "d"})
		rdb.ZAdd(ctx, "zsrc", redis.Z{Score: 9, Member: "g"})
		rdb.ZAdd(ctx, "zsrc", redis.Z{Score: 7, Member: "f"})

		rdb.ZRangeStore(ctx, "zdst", redis.ZRangeArgs{Key: "zsrc", Start: 1, Stop: 3})
		require.Equal(t, []string{"b", "c", "d"}, rdb.ZRange(ctx, "zdst", 0, -1).Val())

		rdb.ZRangeStore(ctx, "zdst", redis.ZRangeArgs{Key: "zsrc", Start: 0, Stop: 2})
		require.Equal(t, []string{"a", "b", "c"}, rdb.ZRange(ctx, "zdst", 0, -1).Val())

		rdb.Del(ctx, "zdst")
		rdb.ZRangeStore(ctx, "zdst", redis.ZRangeArgs{Key: "zsrc", Start: 0, Stop: 0})
		require.Equal(t, []string{"a"}, rdb.ZRange(ctx, "zdst", 0, -1).Val())

		//add none
		rdb.Del(ctx, "zdst")
		rdb.ZRangeStore(ctx, "zdst", redis.ZRangeArgs{Key: "zsrc", Start: 99, Stop: 99})
		require.Equal(t, []string{}, rdb.ZRange(ctx, "zdst", 0, -1).Val())

		// rev
		rdb.Del(ctx, "zdst")
		rdb.ZRangeStore(ctx, "zdst", redis.ZRangeArgs{Key: "zsrc", Start: 1, Stop: 3, Rev: true})
		require.Equal(t, []string{"c", "d", "f"}, rdb.ZRange(ctx, "zdst", 0, -1).Val())

		// byScore
		rdb.Del(ctx, "zdst")
		rdb.ZRangeStore(ctx, "zdst", redis.ZRangeArgs{Key: "zsrc", Start: 2, Stop: 5, ByScore: true})
		require.Equal(t, []string{"b", "c"}, rdb.ZRange(ctx, "zdst", 0, -1).Val())

		// byScore limit offset count
		rdb.Del(ctx, "zdst")
		rdb.ZRangeStore(ctx, "zdst", redis.ZRangeArgs{Key: "zsrc", Start: "1", Stop: "7", ByScore: true, Offset: 2, Count: 3})
		require.Equal(t, []string{"c", "d", "f"}, rdb.ZRange(ctx, "zdst", 0, -1).Val())

		// byLex
		rdb.Del(ctx, "zdst")
		rdb.ZRangeStore(ctx, "zdst", redis.ZRangeArgs{Key: "zsrc", Start: "[c", Stop: "[f", ByLex: true})
		require.Equal(t, []string{"c", "d", "f"}, rdb.ZRange(ctx, "zdst", 0, -1).Val())

		// byLex limit offset count
		rdb.Del(ctx, "zdst")
		rdb.ZRangeStore(ctx, "zdst", redis.ZRangeArgs{Key: "zsrc", Start: "[a", Stop: "[g", ByLex: true, Offset: 2, Count: 3})
		require.Equal(t, []string{"c", "d", "f"}, rdb.ZRange(ctx, "zdst", 0, -1).Val())
	})

	t.Run(fmt.Sprintf("ZRANGESTORE will overwrite dst - %s", encoding), func(t *testing.T) {
		rdb.Del(ctx, "zsrc")
		rdb.Del(ctx, "zdst")

		// non-existing src key delete the dst key
		rdb.ZAdd(ctx, "zdst", redis.Z{Score: 1, Member: "dst_a"})
		require.Equal(t, int64(0), rdb.Do(ctx, "zrangestore", "zdst", "zsrc", 0, -1).Val())
		require.Equal(t, int64(0), rdb.Exists(ctx, "zdst").Val())

		// non-existing src key delete the dst key (different type)
		require.Equal(t, "OK", rdb.Set(ctx, "zdst", 100, 0).Val())
		require.Equal(t, int64(0), rdb.Do(ctx, "zrangestore", "zdst", "zsrc", 0, -1).Val())
		require.Equal(t, int64(0), rdb.Exists(ctx, "zdst").Val())

		// overwrite the dst key with the result zset instead of adding
		rdb.ZAdd(ctx, "zsrc", redis.Z{Score: 1, Member: "src_a"})
		rdb.ZAdd(ctx, "zsrc", redis.Z{Score: 2, Member: "src_b"})
		rdb.ZAdd(ctx, "zdst", redis.Z{Score: 3, Member: "dst_c"})
		require.Equal(t, int64(2), rdb.Do(ctx, "zrangestore", "zdst", "zsrc", 0, -1).Val())
		require.Equal(t, []redis.Z{
			{1, "src_a"},
			{2, "src_b"},
		}, rdb.ZRangeWithScores(ctx, "zdst", 0, -1).Val())
	})

	t.Run(fmt.Sprintf("ZRANGESTORE error - %s", encoding), func(t *testing.T) {
		rdb.Del(ctx, "zsrc")
		rdb.Del(ctx, "zdst")

		rdb.ZAdd(ctx, "zsrc", redis.Z{Score: 1, Member: "a"})
		rdb.ZAdd(ctx, "zsrc", redis.Z{Score: 3, Member: "b"})
		rdb.ZAdd(ctx, "zsrc", redis.Z{Score: 4, Member: "c"})
		rdb.ZAdd(ctx, "zsrc", redis.Z{Score: 6, Member: "d"})
		rdb.ZAdd(ctx, "zsrc", redis.Z{Score: 9, Member: "g"})
		rdb.ZAdd(ctx, "zsrc", redis.Z{Score: 7, Member: "f"})

		util.ErrorRegexp(t, rdb.ZRangeStore(ctx, "zdst", redis.ZRangeArgs{Key: "zsrc", Start: "xx", Stop: "ww"}).Err(), ".*not an integer.*")
		util.ErrorRegexp(t, rdb.ZRangeStore(ctx, "zdst", redis.ZRangeArgs{Key: "zsrc", Start: 1}).Err(), ".*not an integer.*")
		util.ErrorRegexp(t, rdb.ZRangeStore(ctx, "zdst", redis.ZRangeArgs{Key: "zsrc", Start: 1, Stop: 3, Count: 1, Offset: 1}).Err(), ".*error.*")
	})

	t.Run(fmt.Sprintf("ZRANGE basics - %s", encoding), func(t *testing.T) {
		rdb.Del(ctx, "ztmp")
		rdb.ZAdd(ctx, "ztmp", redis.Z{Score: 1, Member: "a"})
		rdb.ZAdd(ctx, "ztmp", redis.Z{Score: 2, Member: "b"})
		rdb.ZAdd(ctx, "ztmp", redis.Z{Score: 3, Member: "c"})
		rdb.ZAdd(ctx, "ztmp", redis.Z{Score: 4, Member: "d"})

		require.Equal(t, []string{"a", "b", "c", "d"}, rdb.ZRange(ctx, "ztmp", 0, -1).Val())
		require.Equal(t, []string{"a", "b", "c"}, rdb.ZRange(ctx, "ztmp", 0, -2).Val())
		require.Equal(t, []string{"b", "c", "d"}, rdb.ZRange(ctx, "ztmp", 1, -1).Val())
		require.Equal(t, []string{"b", "c"}, rdb.ZRange(ctx, "ztmp", 1, -2).Val())
		require.Equal(t, []string{"c", "d"}, rdb.ZRange(ctx, "ztmp", -2, -1).Val())
		require.Equal(t, []string{"c"}, rdb.ZRange(ctx, "ztmp", -2, -2).Val())

		// out of range start index
		require.Equal(t, []string{"a", "b", "c"}, rdb.ZRange(ctx, "ztmp", -5, 2).Val())
		require.Equal(t, []string{"a", "b"}, rdb.ZRange(ctx, "ztmp", -5, 1).Val())
		require.Equal(t, []string{}, rdb.ZRange(ctx, "ztmp", 5, -1).Val())
		require.Equal(t, []string{}, rdb.ZRange(ctx, "ztmp", 5, -2).Val())

		// out of range end index
		require.Equal(t, []string{"a", "b", "c", "d"}, rdb.ZRange(ctx, "ztmp", 0, 5).Val())
		require.Equal(t, []string{"b", "c", "d"}, rdb.ZRange(ctx, "ztmp", 1, 5).Val())
		require.Equal(t, []string{}, rdb.ZRange(ctx, "ztmp", 0, -5).Val())
		require.Equal(t, []string{}, rdb.ZRange(ctx, "ztmp", 1, -5).Val())

		// withscores
		require.Equal(t, []redis.Z{
			{1, "a"},
			{2, "b"},
			{3, "c"},
			{4, "d"},
		}, rdb.ZRangeWithScores(ctx, "ztmp", 0, -1).Val())

		for i := 1; i < 10; i++ {
			cmd := rdb.ZRangeArgs(ctx, redis.ZRangeArgs{Key: "ztmp", Count: 0, ByScore: true, Start: 0, Stop: -1, Offset: int64(i)})
			require.NoError(t, cmd.Err())
			require.Equal(t, []string{}, cmd.Val())
		}

		// go-redis removes the limit condition when (offset, count) == (0, 0)
		// so we use (offset, count) = (0, -1)
		cmd1 := rdb.Do(ctx, "zrange", "ztmp", 0, -1, "byscore", "limit", 0, 0)
		require.NoError(t, cmd1.Err())
		require.Equal(t, []interface{}{}, cmd1.Val())

		// limit with zero count
		for i := 0; i < 20; i++ {
			var args [3]int64
			for j := 0; j < 3; j++ {
				rand := rand.New(rand.NewSource(time.Now().UnixNano()))
				args[j] = rand.Int63n(20) - 10
			}
			if args[2] == 0 {
				continue
			}
			cmd := rdb.ZRangeArgs(ctx, redis.ZRangeArgs{Key: "ztmp", Count: 0, ByScore: true, Start: args[0], Stop: args[1], Offset: args[2]})
			require.NoError(t, cmd.Err())
			require.Equal(t, []string{}, cmd.Val())
		}

		// extend zrange commands
		require.Equal(t, []string{"a", "b", "c", "d"}, rdb.ZRangeArgs(ctx, redis.ZRangeArgs{Key: "ztmp", Start: 0, Stop: -1, Offset: 0, Count: -1}).Val())
		require.Equal(t, []string{"d", "c", "b", "a"}, rdb.ZRangeArgs(ctx, redis.ZRangeArgs{Key: "ztmp", Start: 0, Stop: -1, Offset: 0, Count: -1, Rev: true}).Val())
		require.Equal(t, []redis.Z{
			{1, "a"},
			{2, "b"},
			{3, "c"},
			{4, "d"},
		}, rdb.ZRangeArgsWithScores(ctx, redis.ZRangeArgs{Key: "ztmp", Start: 0, Stop: -1, Offset: 0, Count: -1}).Val())
		require.Equal(t, []redis.Z{
			{4, "d"},
			{3, "c"},
			{2, "b"},
			{1, "a"},
		}, rdb.ZRangeArgsWithScores(ctx, redis.ZRangeArgs{Key: "ztmp", Start: 0, Stop: -1, Offset: 0, Count: -1, Rev: true}).Val())

	})

	t.Run(fmt.Sprintf("ZREVRANGE basics - %s", encoding), func(t *testing.T) {
		rdb.Del(ctx, "ztmp")
		rdb.ZAdd(ctx, "ztmp", redis.Z{Score: 1, Member: "a"})
		rdb.ZAdd(ctx, "ztmp", redis.Z{Score: 2, Member: "b"})
		rdb.ZAdd(ctx, "ztmp", redis.Z{Score: 3, Member: "c"})
		rdb.ZAdd(ctx, "ztmp", redis.Z{Score: 4, Member: "d"})

		require.Equal(t, []string{"d", "c", "b", "a"}, rdb.ZRevRange(ctx, "ztmp", 0, -1).Val())
		require.Equal(t, []string{"d", "c", "b"}, rdb.ZRevRange(ctx, "ztmp", 0, -2).Val())
		require.Equal(t, []string{"c", "b", "a"}, rdb.ZRevRange(ctx, "ztmp", 1, -1).Val())
		require.Equal(t, []string{"c", "b"}, rdb.ZRevRange(ctx, "ztmp", 1, -2).Val())
		require.Equal(t, []string{"b", "a"}, rdb.ZRevRange(ctx, "ztmp", -2, -1).Val())
		require.Equal(t, []string{"b"}, rdb.ZRevRange(ctx, "ztmp", -2, -2).Val())

		// out of range start index
		require.Equal(t, []string{"d", "c", "b"}, rdb.ZRevRange(ctx, "ztmp", -5, 2).Val())
		require.Equal(t, []string{"d", "c"}, rdb.ZRevRange(ctx, "ztmp", -5, 1).Val())
		require.Equal(t, []string{}, rdb.ZRevRange(ctx, "ztmp", 5, -1).Val())
		require.Equal(t, []string{}, rdb.ZRevRange(ctx, "ztmp", 5, -2).Val())

		// out of range end index
		require.Equal(t, []string{"d", "c", "b", "a"}, rdb.ZRevRange(ctx, "ztmp", 0, 5).Val())
		require.Equal(t, []string{"c", "b", "a"}, rdb.ZRevRange(ctx, "ztmp", 1, 5).Val())
		require.Equal(t, []string{}, rdb.ZRevRange(ctx, "ztmp", 0, -5).Val())
		require.Equal(t, []string{}, rdb.ZRevRange(ctx, "ztmp", 1, -5).Val())

		// withscores
		require.Equal(t, []redis.Z{
			{4, "d"},
			{3, "c"},
			{2, "b"},
			{1, "a"},
		}, rdb.ZRevRangeWithScores(ctx, "ztmp", 0, -1).Val())
	})

	t.Run(fmt.Sprintf("ZRANK/ZREVRANK basics - %s", encoding), func(t *testing.T) {
		rdb.Del(ctx, "zranktmp")
		rdb.ZAdd(ctx, "zranktmp", redis.Z{Score: 10, Member: "x"})
		rdb.ZAdd(ctx, "zranktmp", redis.Z{Score: 20, Member: "y"})
		rdb.ZAdd(ctx, "zranktmp", redis.Z{Score: 30, Member: "z"})

		require.Equal(t, int64(0), rdb.ZRank(ctx, "zranktmp", "x").Val())
		require.Equal(t, int64(1), rdb.ZRank(ctx, "zranktmp", "y").Val())
		require.Equal(t, int64(2), rdb.ZRank(ctx, "zranktmp", "z").Val())
		require.Equal(t, redis.Nil, rdb.ZRank(ctx, "zranktmp", "foo").Err())
		require.Equal(t, int64(2), rdb.ZRevRank(ctx, "zranktmp", "x").Val())
		require.Equal(t, int64(1), rdb.ZRevRank(ctx, "zranktmp", "y").Val())
		require.Equal(t, int64(0), rdb.ZRevRank(ctx, "zranktmp", "z").Val())
		require.Equal(t, redis.Nil, rdb.ZRevRank(ctx, "zranktmp", "foo").Err())

		require.Equal(t, redis.RankScore{Rank: 0, Score: 10}, rdb.ZRankWithScore(ctx, "zranktmp", "x").Val())
		require.Equal(t, redis.RankScore{Rank: 1, Score: 20}, rdb.ZRankWithScore(ctx, "zranktmp", "y").Val())
		require.Equal(t, redis.RankScore{Rank: 2, Score: 30}, rdb.ZRankWithScore(ctx, "zranktmp", "z").Val())
		require.Equal(t, redis.Nil, rdb.ZRankWithScore(ctx, "zranktmp", "foo").Err())
		require.Equal(t, redis.RankScore{Rank: 2, Score: 10}, rdb.ZRevRankWithScore(ctx, "zranktmp", "x").Val())
		require.Equal(t, redis.RankScore{Rank: 1, Score: 20}, rdb.ZRevRankWithScore(ctx, "zranktmp", "y").Val())
		require.Equal(t, redis.RankScore{Rank: 0, Score: 30}, rdb.ZRevRankWithScore(ctx, "zranktmp", "z").Val())
		require.Equal(t, redis.Nil, rdb.ZRevRankWithScore(ctx, "zranktmp", "foo").Err())
	})

	t.Run(fmt.Sprintf("ZRANK/ZREVRANK - after deletion -%s", encoding), func(t *testing.T) {
		rdb.ZRem(ctx, "zranktmp", "y")

		require.Equal(t, int64(0), rdb.ZRank(ctx, "zranktmp", "x").Val())
		require.Equal(t, int64(1), rdb.ZRank(ctx, "zranktmp", "z").Val())
		require.Equal(t, redis.Nil, rdb.ZRank(ctx, "zranktmp", "foo").Err())
		require.Equal(t, int64(1), rdb.ZRevRank(ctx, "zranktmp", "x").Val())
		require.Equal(t, int64(0), rdb.ZRevRank(ctx, "zranktmp", "z").Val())
		require.Equal(t, redis.Nil, rdb.ZRevRank(ctx, "zranktmp", "foo").Err())

		require.Equal(t, redis.RankScore{Rank: 0, Score: 10}, rdb.ZRankWithScore(ctx, "zranktmp", "x").Val())
		require.Equal(t, redis.RankScore{Rank: 1, Score: 30}, rdb.ZRankWithScore(ctx, "zranktmp", "z").Val())
		require.Equal(t, redis.Nil, rdb.ZRankWithScore(ctx, "zranktmp", "foo").Err())
		require.Equal(t, redis.RankScore{Rank: 1, Score: 10}, rdb.ZRevRankWithScore(ctx, "zranktmp", "x").Val())
		require.Equal(t, redis.RankScore{Rank: 0, Score: 30}, rdb.ZRevRankWithScore(ctx, "zranktmp", "z").Val())
		require.Equal(t, redis.Nil, rdb.ZRevRankWithScore(ctx, "zranktmp", "foo").Err())
	})

	t.Run(fmt.Sprintf("ZINCRBY - can create a new sorted set - %s", encoding), func(t *testing.T) {
		rdb.Del(ctx, "zset")
		rdb.ZIncrBy(ctx, "zset", 1, "foo")
		require.Equal(t, []string{"foo"}, rdb.ZRange(ctx, "zset", 0, -1).Val())
		require.Equal(t, float64(1), rdb.ZScore(ctx, "zset", "foo").Val())
	})

	t.Run(fmt.Sprintf("ZINCRBY - increment and decrement - %s", encoding), func(t *testing.T) {
		rdb.ZIncrBy(ctx, "zset", 2, "foo")
		rdb.ZIncrBy(ctx, "zset", 1, "bar")
		require.Equal(t, []string{"bar", "foo"}, rdb.ZRange(ctx, "zset", 0, -1).Val())
		rdb.ZIncrBy(ctx, "zset", 10, "bar")
		rdb.ZIncrBy(ctx, "zset", -5, "foo")
		rdb.ZIncrBy(ctx, "zset", -5, "bar")
		require.Equal(t, []string{"foo", "bar"}, rdb.ZRange(ctx, "zset", 0, -1).Val())
		require.Equal(t, float64(-2), rdb.ZScore(ctx, "zset", "foo").Val())
		require.Equal(t, float64(6), rdb.ZScore(ctx, "zset", "bar").Val())
	})

	t.Run("ZINCRBY return value", func(t *testing.T) {
		rdb.Del(ctx, "ztmp")
		require.Equal(t, float64(1), rdb.ZIncrBy(ctx, "ztmp", 1.0, "x").Val())
	})

	t.Run("ZRANGEBYSCORE/ZREVRANGEBYSCORE/ZCOUNT basics", func(t *testing.T) {
		createDefaultZset(rdb, ctx)

		// inclusive range
		require.Equal(t, []string{"a", "b", "c"}, rdb.ZRangeByScore(ctx, "zset", &redis.ZRangeBy{Min: "-inf", Max: "2"}).Val())
		require.Equal(t, []string{"b", "c", "d"}, rdb.ZRangeByScore(ctx, "zset", &redis.ZRangeBy{Min: "0", Max: "3"}).Val())
		require.Equal(t, []string{"d", "e", "f"}, rdb.ZRangeByScore(ctx, "zset", &redis.ZRangeBy{Min: "3", Max: "6"}).Val())
		require.Equal(t, []string{"e", "f", "g"}, rdb.ZRangeByScore(ctx, "zset", &redis.ZRangeBy{Min: "4", Max: "+inf"}).Val())
		require.Equal(t, []string{"c", "b", "a"}, rdb.ZRevRangeByScore(ctx, "zset", &redis.ZRangeBy{Max: "2", Min: "-inf"}).Val())
		require.Equal(t, []string{"d", "c", "b"}, rdb.ZRevRangeByScore(ctx, "zset", &redis.ZRangeBy{Max: "3", Min: "0"}).Val())
		require.Equal(t, []string{"f", "e", "d"}, rdb.ZRevRangeByScore(ctx, "zset", &redis.ZRangeBy{Max: "6", Min: "3"}).Val())
		require.Equal(t, []string{"g", "f", "e"}, rdb.ZRevRangeByScore(ctx, "zset", &redis.ZRangeBy{Max: "+inf", Min: "4"}).Val())
		require.Equal(t, int64(3), rdb.ZCount(ctx, "zset", "0", "3").Val())
		// .. in zrange extension syntax
		require.Equal(t, []string{"a", "b", "c"}, rdb.ZRangeArgs(ctx, redis.ZRangeArgs{Key: "zset", Start: "-inf", Stop: "2", ByScore: true}).Val())
		require.Equal(t, []string{"b", "c", "d"}, rdb.ZRangeArgs(ctx, redis.ZRangeArgs{Key: "zset", Start: "0", Stop: "3", ByScore: true}).Val())
		require.Equal(t, []string{"d", "e", "f"}, rdb.ZRangeArgs(ctx, redis.ZRangeArgs{Key: "zset", Start: "3", Stop: "6", ByScore: true}).Val())
		require.Equal(t, []string{"e", "f", "g"}, rdb.ZRangeArgs(ctx, redis.ZRangeArgs{Key: "zset", Start: "4", Stop: "+inf", ByScore: true}).Val())
		require.Equal(t, []string{"c", "b", "a"}, rdb.ZRangeArgs(ctx, redis.ZRangeArgs{Key: "zset", Stop: "2", Start: "-inf", ByScore: true, Rev: true}).Val())
		require.Equal(t, []string{"d", "c", "b"}, rdb.ZRangeArgs(ctx, redis.ZRangeArgs{Key: "zset", Stop: "3", Start: "0", ByScore: true, Rev: true}).Val())
		require.Equal(t, []string{"f", "e", "d"}, rdb.ZRangeArgs(ctx, redis.ZRangeArgs{Key: "zset", Stop: "6", Start: "3", ByScore: true, Rev: true}).Val())
		require.Equal(t, []string{"g", "f", "e"}, rdb.ZRangeArgs(ctx, redis.ZRangeArgs{Key: "zset", Stop: "+inf", Start: "4", ByScore: true, Rev: true}).Val())

		// exclusive range
		require.Equal(t, []string{"b"}, rdb.ZRangeByScore(ctx, "zset", &redis.ZRangeBy{Min: "(-inf", Max: "(2"}).Val())
		require.Equal(t, []string{"b", "c"}, rdb.ZRangeByScore(ctx, "zset", &redis.ZRangeBy{Min: "(0", Max: "(3"}).Val())
		require.Equal(t, []string{"e", "f"}, rdb.ZRangeByScore(ctx, "zset", &redis.ZRangeBy{Min: "(3", Max: "(6"}).Val())
		require.Equal(t, []string{"f"}, rdb.ZRangeByScore(ctx, "zset", &redis.ZRangeBy{Min: "(4", Max: "(+inf"}).Val())
		require.Equal(t, []string{"b"}, rdb.ZRevRangeByScore(ctx, "zset", &redis.ZRangeBy{Max: "(2", Min: "(-inf"}).Val())
		require.Equal(t, []string{"c", "b"}, rdb.ZRevRangeByScore(ctx, "zset", &redis.ZRangeBy{Max: "(3", Min: "(0"}).Val())
		require.Equal(t, []string{"f", "e"}, rdb.ZRevRangeByScore(ctx, "zset", &redis.ZRangeBy{Max: "(6", Min: "(3"}).Val())
		require.Equal(t, []string{"f"}, rdb.ZRevRangeByScore(ctx, "zset", &redis.ZRangeBy{Max: "(+inf", Min: "(4"}).Val())
		require.Equal(t, int64(2), rdb.ZCount(ctx, "zset", "(0", "(3").Val())
		// .. in zrange extension syntax
		require.Equal(t, []string{"b"}, rdb.ZRangeArgs(ctx, redis.ZRangeArgs{Key: "zset", Start: "(-inf", Stop: "(2", ByScore: true}).Val())
		require.Equal(t, []string{"b", "c"}, rdb.ZRangeArgs(ctx, redis.ZRangeArgs{Key: "zset", Start: "(0", Stop: "(3", ByScore: true}).Val())
		require.Equal(t, []string{"e", "f"}, rdb.ZRangeArgs(ctx, redis.ZRangeArgs{Key: "zset", Start: "(3", Stop: "(6", ByScore: true}).Val())
		require.Equal(t, []string{"f"}, rdb.ZRangeArgs(ctx, redis.ZRangeArgs{Key: "zset", Start: "(4", Stop: "(+inf", ByScore: true}).Val())
		require.Equal(t, []string{"b"}, rdb.ZRangeArgs(ctx, redis.ZRangeArgs{Key: "zset", Stop: "(2", Start: "(-inf", ByScore: true, Rev: true}).Val())
		require.Equal(t, []string{"c", "b"}, rdb.ZRangeArgs(ctx, redis.ZRangeArgs{Key: "zset", Stop: "(3", Start: "(0", ByScore: true, Rev: true}).Val())
		require.Equal(t, []string{"f", "e"}, rdb.ZRangeArgs(ctx, redis.ZRangeArgs{Key: "zset", Stop: "(6", Start: "(3", ByScore: true, Rev: true}).Val())
		require.Equal(t, []string{"f"}, rdb.ZRangeArgs(ctx, redis.ZRangeArgs{Key: "zset", Stop: "(+inf", Start: "(4", ByScore: true, Rev: true}).Val())

		// test empty ranges
		rdb.ZRem(ctx, "zset", "a")
		rdb.ZRem(ctx, "zset", "g")

		// inclusive range
		require.Equal(t, []string{}, rdb.ZRangeByScore(ctx, "zset", &redis.ZRangeBy{Min: "4", Max: "2"}).Val())
		require.Equal(t, []string{}, rdb.ZRangeByScore(ctx, "zset", &redis.ZRangeBy{Min: "6", Max: "+inf"}).Val())
		require.Equal(t, []string{}, rdb.ZRangeByScore(ctx, "zset", &redis.ZRangeBy{Min: "-inf", Max: "-6"}).Val())
		require.Equal(t, []string{}, rdb.ZRevRangeByScore(ctx, "zset", &redis.ZRangeBy{Max: "+inf", Min: "6"}).Val())
		require.Equal(t, []string{}, rdb.ZRevRangeByScore(ctx, "zset", &redis.ZRangeBy{Max: "-6", Min: "-inf"}).Val())
		// .. in zrange extension syntax
		require.Equal(t, []string{}, rdb.ZRangeArgs(ctx, redis.ZRangeArgs{Key: "zset", Start: "4", Stop: "2", ByScore: true}).Val())
		require.Equal(t, []string{}, rdb.ZRangeArgs(ctx, redis.ZRangeArgs{Key: "zset", Start: "6", Stop: "+inf", ByScore: true}).Val())
		require.Equal(t, []string{}, rdb.ZRangeArgs(ctx, redis.ZRangeArgs{Key: "zset", Start: "-inf", Stop: "-6", ByScore: true}).Val())
		require.Equal(t, []string{}, rdb.ZRangeArgs(ctx, redis.ZRangeArgs{Key: "zset", Stop: "+inf", Start: "6", ByScore: true, Rev: true}).Val())
		require.Equal(t, []string{}, rdb.ZRangeArgs(ctx, redis.ZRangeArgs{Key: "zset", Stop: "-6", Start: "-inf", ByScore: true, Rev: true}).Val())

		// exclusive range
		require.Equal(t, []string{}, rdb.ZRangeByScore(ctx, "zset", &redis.ZRangeBy{Min: "(4", Max: "(2"}).Val())
		require.Equal(t, []string{}, rdb.ZRangeByScore(ctx, "zset", &redis.ZRangeBy{Min: "2", Max: "(2"}).Val())
		require.Equal(t, []string{}, rdb.ZRangeByScore(ctx, "zset", &redis.ZRangeBy{Min: "(2", Max: "2"}).Val())
		require.Equal(t, []string{}, rdb.ZRangeByScore(ctx, "zset", &redis.ZRangeBy{Min: "(6", Max: "(+inf"}).Val())
		require.Equal(t, []string{}, rdb.ZRangeByScore(ctx, "zset", &redis.ZRangeBy{Min: "(-inf", Max: "(-6"}).Val())
		require.Equal(t, []string{}, rdb.ZRevRangeByScore(ctx, "zset", &redis.ZRangeBy{Max: "(+inf", Min: "(6"}).Val())
		require.Equal(t, []string{}, rdb.ZRevRangeByScore(ctx, "zset", &redis.ZRangeBy{Max: "(-6", Min: "(-inf"}).Val())
		// .. in zrange extension syntax
		require.Equal(t, []string{}, rdb.ZRangeArgs(ctx, redis.ZRangeArgs{Key: "zset", Start: "(4", Stop: "(2", ByScore: true}).Val())
		require.Equal(t, []string{}, rdb.ZRangeArgs(ctx, redis.ZRangeArgs{Key: "zset", Start: "2", Stop: "(2", ByScore: true}).Val())
		require.Equal(t, []string{}, rdb.ZRangeArgs(ctx, redis.ZRangeArgs{Key: "zset", Start: "(2", Stop: "2", ByScore: true}).Val())
		require.Equal(t, []string{}, rdb.ZRangeArgs(ctx, redis.ZRangeArgs{Key: "zset", Start: "(6", Stop: "(+inf", ByScore: true}).Val())
		require.Equal(t, []string{}, rdb.ZRangeArgs(ctx, redis.ZRangeArgs{Key: "zset", Start: "(-inf", Stop: "(-6", ByScore: true}).Val())
		require.Equal(t, []string{}, rdb.ZRangeArgs(ctx, redis.ZRangeArgs{Key: "zset", Stop: "(+inf", Start: "(6", ByScore: true, Rev: true}).Val())
		require.Equal(t, []string{}, rdb.ZRangeArgs(ctx, redis.ZRangeArgs{Key: "zset", Stop: "(-6", Start: "(-inf", ByScore: true, Rev: true}).Val())

		// empty inner range
		require.Equal(t, []string{}, rdb.ZRangeByScore(ctx, "zset", &redis.ZRangeBy{Min: "2.4", Max: "2.6"}).Val())
		require.Equal(t, []string{}, rdb.ZRangeByScore(ctx, "zset", &redis.ZRangeBy{Min: "(2.4", Max: "2.6"}).Val())
		require.Equal(t, []string{}, rdb.ZRangeByScore(ctx, "zset", &redis.ZRangeBy{Min: "2.4", Max: "(2.6"}).Val())
		require.Equal(t, []string{}, rdb.ZRangeByScore(ctx, "zset", &redis.ZRangeBy{Min: "(2.4", Max: "(2.6"}).Val())
		// .. in zrange extension syntax
		require.Equal(t, []string{}, rdb.ZRangeArgs(ctx, redis.ZRangeArgs{Key: "zset", Start: "2.4", Stop: "2.6", ByScore: true}).Val())
		require.Equal(t, []string{}, rdb.ZRangeArgs(ctx, redis.ZRangeArgs{Key: "zset", Start: "(2.4", Stop: "2.6", ByScore: true}).Val())
		require.Equal(t, []string{}, rdb.ZRangeArgs(ctx, redis.ZRangeArgs{Key: "zset", Start: "2.4", Stop: "(2.6", ByScore: true}).Val())
		require.Equal(t, []string{}, rdb.ZRangeArgs(ctx, redis.ZRangeArgs{Key: "zset", Start: "(2.4", Stop: "(2.6", ByScore: true}).Val())
	})

	t.Run("ZRANGEBYSCORE with WITHSCORES", func(t *testing.T) {
		createDefaultZset(rdb, ctx)
		require.Equal(t, []redis.Z{{1, "b"}, {2, "c"}, {3, "d"}}, rdb.ZRangeByScoreWithScores(ctx, "zset", &redis.ZRangeBy{Min: "0", Max: "3"}).Val())
		require.Equal(t, []redis.Z{{3, "d"}, {2, "c"}, {1, "b"}}, rdb.ZRevRangeByScoreWithScores(ctx, "zset", &redis.ZRangeBy{Min: "0", Max: "3"}).Val())
		// .. in zrange extension syntax
		require.Equal(t, []redis.Z{{1, "b"}, {2, "c"}, {3, "d"}}, rdb.ZRangeArgsWithScores(ctx, redis.ZRangeArgs{Key: "zset", Start: "0", Stop: "3", ByScore: true}).Val())
		require.Equal(t, []redis.Z{{3, "d"}, {2, "c"}, {1, "b"}}, rdb.ZRangeArgsWithScores(ctx, redis.ZRangeArgs{Key: "zset", Start: "0", Stop: "3", ByScore: true, Rev: true}).Val())
	})

	t.Run("ZRANGEBYSCORE with LIMIT", func(t *testing.T) {
		createDefaultZset(rdb, ctx)
		require.Equal(t, []string{"b", "c"}, rdb.ZRangeByScore(ctx, "zset", &redis.ZRangeBy{Min: "0", Max: "10", Offset: 0, Count: 2}).Val())
		require.Equal(t, []string{"d", "e", "f"}, rdb.ZRangeByScore(ctx, "zset", &redis.ZRangeBy{Min: "0", Max: "10", Offset: 2, Count: 3}).Val())
		require.Equal(t, []string{"d", "e", "f"}, rdb.ZRangeByScore(ctx, "zset", &redis.ZRangeBy{Min: "0", Max: "10", Offset: 2, Count: 10}).Val())
		require.Equal(t, []string{}, rdb.ZRangeByScore(ctx, "zset", &redis.ZRangeBy{Min: "0", Max: "10", Offset: 20, Count: 10}).Val())
		require.Equal(t, []string{"f", "e"}, rdb.ZRevRangeByScore(ctx, "zset", &redis.ZRangeBy{Min: "0", Max: "10", Offset: 0, Count: 2}).Val())
		require.Equal(t, []string{"d", "c", "b"}, rdb.ZRevRangeByScore(ctx, "zset", &redis.ZRangeBy{Min: "0", Max: "10", Offset: 2, Count: 3}).Val())
		require.Equal(t, []string{"d", "c", "b"}, rdb.ZRevRangeByScore(ctx, "zset", &redis.ZRangeBy{Min: "0", Max: "10", Offset: 2, Count: 10}).Val())
		require.Equal(t, []string{}, rdb.ZRevRangeByScore(ctx, "zset", &redis.ZRangeBy{Min: "0", Max: "10", Offset: 20, Count: 10}).Val())
		// .. in zrange extension syntax
		require.Equal(t, []string{"b", "c"}, rdb.ZRangeArgs(ctx, redis.ZRangeArgs{Key: "zset", Start: "0", Stop: "10", Offset: 0, Count: 2, ByScore: true}).Val())
		require.Equal(t, []string{"d", "e", "f"}, rdb.ZRangeArgs(ctx, redis.ZRangeArgs{Key: "zset", Start: "0", Stop: "10", Offset: 2, Count: 3, ByScore: true}).Val())
		require.Equal(t, []string{"d", "e", "f"}, rdb.ZRangeArgs(ctx, redis.ZRangeArgs{Key: "zset", Start: "0", Stop: "10", Offset: 2, Count: 10, ByScore: true}).Val())
		require.Equal(t, []string{}, rdb.ZRangeArgs(ctx, redis.ZRangeArgs{Key: "zset", Start: "0", Stop: "10", Offset: 20, Count: 10, ByScore: true}).Val())
		require.Equal(t, []string{"f", "e"}, rdb.ZRangeArgs(ctx, redis.ZRangeArgs{Key: "zset", Start: "0", Stop: "10", Offset: 0, Count: 2, ByScore: true, Rev: true}).Val())
		require.Equal(t, []string{"d", "c", "b"}, rdb.ZRangeArgs(ctx, redis.ZRangeArgs{Key: "zset", Start: "0", Stop: "10", Offset: 2, Count: 3, ByScore: true, Rev: true}).Val())
		require.Equal(t, []string{"d", "c", "b"}, rdb.ZRangeArgs(ctx, redis.ZRangeArgs{Key: "zset", Start: "0", Stop: "10", Offset: 2, Count: 10, ByScore: true, Rev: true}).Val())
		require.Equal(t, []string{}, rdb.ZRangeArgs(ctx, redis.ZRangeArgs{Key: "zset", Start: "0", Stop: "10", Offset: 20, Count: 10, ByScore: true, Rev: true}).Val())
	})

	t.Run("ZRANGEBYSCORE with LIMIT and WITHSCORES", func(t *testing.T) {
		createDefaultZset(rdb, ctx)
		require.Equal(t, []redis.Z{{4, "e"}, {5, "f"}}, rdb.ZRangeByScoreWithScores(ctx, "zset", &redis.ZRangeBy{Min: "2", Max: "5", Offset: 2, Count: 3}).Val())
		require.Equal(t, []redis.Z{{3, "d"}, {2, "c"}}, rdb.ZRevRangeByScoreWithScores(ctx, "zset", &redis.ZRangeBy{Min: "2", Max: "5", Offset: 2, Count: 3}).Val())
		// .. in zrange extension syntax
		require.Equal(t, []redis.Z{{4, "e"}, {5, "f"}}, rdb.ZRangeArgsWithScores(ctx, redis.ZRangeArgs{Key: "zset", Start: "2", Stop: "5", Offset: 2, Count: 3, ByScore: true}).Val())
		require.Equal(t, []redis.Z{{3, "d"}, {2, "c"}}, rdb.ZRangeArgsWithScores(ctx, redis.ZRangeArgs{Key: "zset", Start: "2", Stop: "5", Offset: 2, Count: 3, ByScore: true, Rev: true}).Val())
	})

	t.Run("ZRANGEBYSCORE with non-value min or max", func(t *testing.T) {
		util.ErrorRegexp(t, rdb.ZRangeByScore(ctx, "fooz", &redis.ZRangeBy{Min: "str", Max: "1"}).Err(), ".*double.*")
		util.ErrorRegexp(t, rdb.ZRangeByScore(ctx, "fooz", &redis.ZRangeBy{Min: "1", Max: "str"}).Err(), ".*double.*")
		util.ErrorRegexp(t, rdb.ZRangeByScore(ctx, "fooz", &redis.ZRangeBy{Min: "1", Max: "NaN"}).Err(), ".*double.*")
		// .. in zrange extension syntax
		util.ErrorRegexp(t, rdb.ZRangeArgs(ctx, redis.ZRangeArgs{Key: "fooz", Start: "str", Stop: "1", ByScore: true}).Err(), ".*double.*")
		util.ErrorRegexp(t, rdb.ZRangeArgs(ctx, redis.ZRangeArgs{Key: "fooz", Start: "1", Stop: "str", ByScore: true}).Err(), ".*double.*")
		util.ErrorRegexp(t, rdb.ZRangeArgs(ctx, redis.ZRangeArgs{Key: "fooz", Start: "1", Stop: "NaN", ByScore: true}).Err(), ".*double.*")
	})

	t.Run("ZRANGEBYSCORE for min/max score with multi member", func(t *testing.T) {
		zsetInt := []redis.Z{
			{math.Inf(-1), "a"},
			{math.Inf(-1), "b"},
			{-1, "c"},
			{2, "d"},
			{3, "e"},
			{math.Inf(1), "f"},
			{math.Inf(1), "g"}}
		createZset(rdb, ctx, "mzset", zsetInt)
		require.Equal(t, zsetInt, rdb.ZRangeByScoreWithScores(ctx, "mzset", &redis.ZRangeBy{Min: "-inf", Max: "+inf"}).Val())
		require.Equal(t, zsetInt, rdb.ZRangeArgsWithScores(ctx, redis.ZRangeArgs{Key: "mzset", Start: "-inf", Stop: "+inf", ByScore: true}).Val())
		util.ReverseSlice(zsetInt)
		require.Equal(t, zsetInt, rdb.ZRevRangeByScoreWithScores(ctx, "mzset", &redis.ZRangeBy{Min: "-inf", Max: "+inf"}).Val())
		require.Equal(t, zsetInt, rdb.ZRangeArgsWithScores(ctx, redis.ZRangeArgs{Key: "mzset", Start: "-inf", Stop: "+inf", ByScore: true, Rev: true}).Val())

		zsetDouble := []redis.Z{
			{-1.004, "a"},
			{-1.004, "b"},
			{-1.002, "c"},
			{1.002, "d"},
			{1.004, "e"},
			{1.004, "f"}}
		createZset(rdb, ctx, "mzset", zsetDouble)
		require.Equal(t, zsetDouble, rdb.ZRangeByScoreWithScores(ctx, "mzset", &redis.ZRangeBy{Min: "-inf", Max: "+inf"}).Val())
		require.Equal(t, zsetDouble, rdb.ZRangeArgsWithScores(ctx, redis.ZRangeArgs{Key: "mzset", Start: "-inf", Stop: "+inf", ByScore: true}).Val())
		util.ReverseSlice(zsetDouble)
		require.Equal(t, zsetDouble, rdb.ZRevRangeByScoreWithScores(ctx, "mzset", &redis.ZRangeBy{Min: "-inf", Max: "+inf"}).Val())
		require.Equal(t, zsetDouble, rdb.ZRangeArgsWithScores(ctx, redis.ZRangeArgs{Key: "mzset", Start: "-inf", Stop: "+inf", ByScore: true, Rev: true}).Val())
	})

	t.Run("ZRANGEBYLEX/ZREVRANGEBYLEX/ZLEXCOUNT basics", func(t *testing.T) {
		createDefaultLexZset(rdb, ctx)

		// inclusive range
		require.Equal(t, []string{"alpha", "bar", "cool"}, rdb.ZRangeByLex(ctx, "zset", &redis.ZRangeBy{Min: "-", Max: "[cool"}).Val())
		require.Equal(t, []string{"bar", "cool", "down"}, rdb.ZRangeByLex(ctx, "zset", &redis.ZRangeBy{Min: "[bar", Max: "[down"}).Val())
		require.Equal(t, []string{"great", "hill", "omega"}, rdb.ZRangeByLex(ctx, "zset", &redis.ZRangeBy{Min: "[g", Max: "+"}).Val())
		require.Equal(t, []string{"cool", "bar", "alpha"}, rdb.ZRevRangeByLex(ctx, "zset", &redis.ZRangeBy{Min: "-", Max: "[cool"}).Val())
		require.Equal(t, []string{"down", "cool", "bar"}, rdb.ZRevRangeByLex(ctx, "zset", &redis.ZRangeBy{Min: "[bar", Max: "[down"}).Val())
		require.Equal(t, []string{"omega", "hill", "great", "foo", "elephant", "down"}, rdb.ZRevRangeByLex(ctx, "zset", &redis.ZRangeBy{Min: "[d", Max: "+"}).Val())
		// .. in zrange extension syntax
		require.Equal(t, []string{"alpha", "bar", "cool"}, rdb.ZRangeArgs(ctx, redis.ZRangeArgs{Key: "zset", Start: "-", Stop: "[cool", ByLex: true}).Val())
		require.Equal(t, []string{"bar", "cool", "down"}, rdb.ZRangeArgs(ctx, redis.ZRangeArgs{Key: "zset", Start: "[bar", Stop: "[down", ByLex: true}).Val())
		require.Equal(t, []string{"great", "hill", "omega"}, rdb.ZRangeArgs(ctx, redis.ZRangeArgs{Key: "zset", Start: "[g", Stop: "+", ByLex: true}).Val())
		require.Equal(t, []string{"cool", "bar", "alpha"}, rdb.ZRangeArgs(ctx, redis.ZRangeArgs{Key: "zset", Start: "-", Stop: "[cool", ByLex: true, Rev: true}).Val())
		require.Equal(t, []string{"down", "cool", "bar"}, rdb.ZRangeArgs(ctx, redis.ZRangeArgs{Key: "zset", Start: "[bar", Stop: "[down", ByLex: true, Rev: true}).Val())
		require.Equal(t, []string{"omega", "hill", "great", "foo", "elephant", "down"}, rdb.ZRangeArgs(ctx, redis.ZRangeArgs{Key: "zset", Start: "[d", Stop: "+", ByLex: true, Rev: true}).Val())

		// exclusive range
		require.Equal(t, []string{"alpha", "bar"}, rdb.ZRangeByLex(ctx, "zset", &redis.ZRangeBy{Min: "-", Max: "(cool"}).Val())
		require.Equal(t, []string{"cool"}, rdb.ZRangeByLex(ctx, "zset", &redis.ZRangeBy{Min: "(bar", Max: "(down"}).Val())
		require.Equal(t, []string{"hill", "omega"}, rdb.ZRangeByLex(ctx, "zset", &redis.ZRangeBy{Min: "(great", Max: "+"}).Val())
		require.Equal(t, []string{"bar", "alpha"}, rdb.ZRevRangeByLex(ctx, "zset", &redis.ZRangeBy{Min: "-", Max: "(cool"}).Val())
		require.Equal(t, []string{"cool"}, rdb.ZRevRangeByLex(ctx, "zset", &redis.ZRangeBy{Min: "(bar", Max: "(down"}).Val())
		require.Equal(t, []string{"omega", "hill"}, rdb.ZRevRangeByLex(ctx, "zset", &redis.ZRangeBy{Min: "(great", Max: "+"}).Val())
		// .. in zrange extension syntax
		require.Equal(t, []string{"alpha", "bar"}, rdb.ZRangeArgs(ctx, redis.ZRangeArgs{Key: "zset", Start: "-", Stop: "(cool", ByLex: true}).Val())
		require.Equal(t, []string{"cool"}, rdb.ZRangeArgs(ctx, redis.ZRangeArgs{Key: "zset", Start: "(bar", Stop: "(down", ByLex: true}).Val())
		require.Equal(t, []string{"hill", "omega"}, rdb.ZRangeArgs(ctx, redis.ZRangeArgs{Key: "zset", Start: "(great", Stop: "+", ByLex: true}).Val())
		require.Equal(t, []string{"bar", "alpha"}, rdb.ZRangeArgs(ctx, redis.ZRangeArgs{Key: "zset", Start: "-", Stop: "(cool", ByLex: true, Rev: true}).Val())
		require.Equal(t, []string{"cool"}, rdb.ZRangeArgs(ctx, redis.ZRangeArgs{Key: "zset", Start: "(bar", Stop: "(down", ByLex: true, Rev: true}).Val())
		require.Equal(t, []string{"omega", "hill"}, rdb.ZRangeArgs(ctx, redis.ZRangeArgs{Key: "zset", Start: "(great", Stop: "+", ByLex: true, Rev: true}).Val())

		// inclusive and exclusive
		require.Equal(t, []string{}, rdb.ZRangeByLex(ctx, "zset", &redis.ZRangeBy{Min: "(az", Max: "(b"}).Val())
		require.Equal(t, []string{}, rdb.ZRangeByLex(ctx, "zset", &redis.ZRangeBy{Min: "(z", Max: "+"}).Val())
		require.Equal(t, []string{}, rdb.ZRangeByLex(ctx, "zset", &redis.ZRangeBy{Min: "-", Max: "[aaaa"}).Val())
		require.Equal(t, []string{}, rdb.ZRevRangeByLex(ctx, "zset", &redis.ZRangeBy{Min: "[elez", Max: "[elex"}).Val())
		require.Equal(t, []string{}, rdb.ZRangeByLex(ctx, "zset", &redis.ZRangeBy{Min: "(hill", Max: "(omega"}).Val())
		// .. in zrange extension syntax
		require.Equal(t, []string{}, rdb.ZRangeArgs(ctx, redis.ZRangeArgs{Key: "zset", Start: "(az", Stop: "(b", ByLex: true}).Val())
		require.Equal(t, []string{}, rdb.ZRangeArgs(ctx, redis.ZRangeArgs{Key: "zset", Start: "(z", Stop: "+", ByLex: true}).Val())
		require.Equal(t, []string{}, rdb.ZRangeArgs(ctx, redis.ZRangeArgs{Key: "zset", Start: "-", Stop: "[aaaa", ByLex: true}).Val())
		require.Equal(t, []string{}, rdb.ZRangeArgs(ctx, redis.ZRangeArgs{Key: "zset", Start: "[elez", Stop: "[elex", ByLex: true, Rev: true}).Val())
		require.Equal(t, []string{}, rdb.ZRangeArgs(ctx, redis.ZRangeArgs{Key: "zset", Start: "(hill", Stop: "(omega", ByLex: true}).Val())
	})

	t.Run("ZRANGEBYLEX with LIMIT", func(t *testing.T) {
		createDefaultLexZset(rdb, ctx)
		require.Equal(t, []string{"alpha", "bar"}, rdb.ZRangeByLex(ctx, "zset", &redis.ZRangeBy{Min: "-", Max: "[cool", Offset: 0, Count: 2}).Val())
		require.Equal(t, []string{"bar", "cool"}, rdb.ZRangeByLex(ctx, "zset", &redis.ZRangeBy{Min: "-", Max: "[cool", Offset: 1, Count: 2}).Val())
		require.Equal(t, []interface{}{}, rdb.Do(ctx, "zrangebylex", "zset", "[bar", "[down", "limit", "0", "0").Val())
		require.Equal(t, []string{}, rdb.ZRangeByLex(ctx, "zset", &redis.ZRangeBy{Min: "[bar", Max: "[down", Offset: 2, Count: 0}).Val())
		require.Equal(t, []string{"bar"}, rdb.ZRangeByLex(ctx, "zset", &redis.ZRangeBy{Min: "[bar", Max: "[down", Offset: 0, Count: 1}).Val())
		require.Equal(t, []string{"cool"}, rdb.ZRangeByLex(ctx, "zset", &redis.ZRangeBy{Min: "[bar", Max: "[down", Offset: 1, Count: 1}).Val())
		require.Equal(t, []string{"bar", "cool", "down"}, rdb.ZRangeByLex(ctx, "zset", &redis.ZRangeBy{Min: "[bar", Max: "[down", Offset: 0, Count: 100}).Val())
		require.Equal(t, []string{"omega", "hill", "great", "foo", "elephant"}, rdb.ZRevRangeByLex(ctx, "zset", &redis.ZRangeBy{Min: "[d", Max: "+", Offset: 0, Count: 5}).Val())
		require.Equal(t, []string{"omega", "hill", "great", "foo"}, rdb.ZRevRangeByLex(ctx, "zset", &redis.ZRangeBy{Min: "[d", Max: "+", Offset: 0, Count: 4}).Val())
		// .. in zrange extension syntax
		require.Equal(t, []string{"alpha", "bar"}, rdb.ZRangeArgs(ctx, redis.ZRangeArgs{Key: "zset", Start: "-", Stop: "[cool", Offset: 0, Count: 2, ByLex: true}).Val())
		require.Equal(t, []string{"bar", "cool"}, rdb.ZRangeArgs(ctx, redis.ZRangeArgs{Key: "zset", Start: "-", Stop: "[cool", Offset: 1, Count: 2, ByLex: true}).Val())
		require.Equal(t, []interface{}{}, rdb.Do(ctx, "zrange", "zset", "[bar", "[down", "bylex", "limit", "0", "0").Val())
		require.Equal(t, []string{}, rdb.ZRangeArgs(ctx, redis.ZRangeArgs{Key: "zset", Start: "[bar", Stop: "[down", Offset: 2, Count: 0, ByLex: true}).Val())
		require.Equal(t, []string{"bar"}, rdb.ZRangeArgs(ctx, redis.ZRangeArgs{Key: "zset", Start: "[bar", Stop: "[down", Offset: 0, Count: 1, ByLex: true}).Val())
		require.Equal(t, []string{"cool"}, rdb.ZRangeArgs(ctx, redis.ZRangeArgs{Key: "zset", Start: "[bar", Stop: "[down", Offset: 1, Count: 1, ByLex: true}).Val())
		require.Equal(t, []string{"bar", "cool", "down"}, rdb.ZRangeArgs(ctx, redis.ZRangeArgs{Key: "zset", Start: "[bar", Stop: "[down", Offset: 0, Count: 100, ByLex: true}).Val())
		require.Equal(t, []string{"omega", "hill", "great", "foo", "elephant"}, rdb.ZRangeArgs(ctx, redis.ZRangeArgs{Key: "zset", Start: "[d", Stop: "+", Offset: 0, Count: 5, ByLex: true, Rev: true}).Val())
		require.Equal(t, []string{"omega", "hill", "great", "foo"}, rdb.ZRangeArgs(ctx, redis.ZRangeArgs{Key: "zset", Start: "[d", Stop: "+", Offset: 0, Count: 4, ByLex: true, Rev: true}).Val())
	})

	t.Run("ZRANGEBYLEX with invalid lex range specifiers", func(t *testing.T) {
		util.ErrorRegexp(t, rdb.ZRangeByLex(ctx, "fooz", &redis.ZRangeBy{Min: "foo", Max: "bar"}).Err(), ".*illegal.*")
		util.ErrorRegexp(t, rdb.ZRangeByLex(ctx, "fooz", &redis.ZRangeBy{Min: "[foo", Max: "bar"}).Err(), ".*illegal.*")
		util.ErrorRegexp(t, rdb.ZRangeByLex(ctx, "fooz", &redis.ZRangeBy{Min: "foo", Max: "[bar"}).Err(), ".*illegal.*")
		util.ErrorRegexp(t, rdb.ZRangeByLex(ctx, "fooz", &redis.ZRangeBy{Min: "+x", Max: "[bar"}).Err(), ".*illegal.*")
		util.ErrorRegexp(t, rdb.ZRangeByLex(ctx, "fooz", &redis.ZRangeBy{Min: "-x", Max: "[bar"}).Err(), ".*illegal.*")
		// .. in zrange extension syntax
		util.ErrorRegexp(t, rdb.ZRangeArgs(ctx, redis.ZRangeArgs{Key: "fooz", Start: "foo", Stop: "bar", ByLex: true}).Err(), ".*illegal.*")
		util.ErrorRegexp(t, rdb.ZRangeArgs(ctx, redis.ZRangeArgs{Key: "fooz", Start: "[foo", Stop: "bar", ByLex: true}).Err(), ".*illegal.*")
		util.ErrorRegexp(t, rdb.ZRangeArgs(ctx, redis.ZRangeArgs{Key: "fooz", Start: "foo", Stop: "[bar", ByLex: true}).Err(), ".*illegal.*")
		util.ErrorRegexp(t, rdb.ZRangeArgs(ctx, redis.ZRangeArgs{Key: "fooz", Start: "+x", Stop: "[bar", ByLex: true}).Err(), ".*illegal.*")
		util.ErrorRegexp(t, rdb.ZRangeArgs(ctx, redis.ZRangeArgs{Key: "fooz", Start: "-x", Stop: "[bar", ByLex: true}).Err(), ".*illegal.*")
	})

	t.Run("ZREMRANGEBYSCORE basics", func(t *testing.T) {
		remrangebyscore := func(min, max string) int64 {
			createZset(rdb, ctx, "zset", []redis.Z{{1, "a"}, {2, "b"}, {3, "c"},
				{4, "d"}, {5, "e"}})
			require.Equal(t, int64(1), rdb.Exists(ctx, "zset").Val())
			return rdb.ZRemRangeByScore(ctx, "zset", min, max).Val()
		}

		// inner range
		require.Equal(t, int64(3), remrangebyscore("2", "4"))
		require.Equal(t, []string{"a", "e"}, rdb.ZRange(ctx, "zset", 0, -1).Val())

		// start underflow
		require.Equal(t, int64(1), remrangebyscore("-10", "1"))
		require.Equal(t, []string{"b", "c", "d", "e"}, rdb.ZRange(ctx, "zset", 0, -1).Val())

		// end overflow
		require.Equal(t, int64(1), remrangebyscore("5", "10"))
		require.Equal(t, []string{"a", "b", "c", "d"}, rdb.ZRange(ctx, "zset", 0, -1).Val())

		// switch min and max
		require.Equal(t, int64(0), remrangebyscore("4", "2"))
		require.Equal(t, []string{"a", "b", "c", "d", "e"}, rdb.ZRange(ctx, "zset", 0, -1).Val())

		// -inf to mid
		require.Equal(t, int64(3), remrangebyscore("-inf", "3"))
		require.Equal(t, []string{"d", "e"}, rdb.ZRange(ctx, "zset", 0, -1).Val())

		// mid to +inf
		require.Equal(t, int64(3), remrangebyscore("3", "+inf"))
		require.Equal(t, []string{"a", "b"}, rdb.ZRange(ctx, "zset", 0, -1).Val())

		// -inf to +inf
		require.Equal(t, int64(5), remrangebyscore("-inf", "+inf"))
		require.Equal(t, []string{}, rdb.ZRange(ctx, "zset", 0, -1).Val())

		// exclusive min
		require.Equal(t, int64(4), remrangebyscore("(1", "5"))
		require.Equal(t, []string{"a"}, rdb.ZRange(ctx, "zset", 0, -1).Val())
		require.Equal(t, int64(3), remrangebyscore("(2", "5"))
		require.Equal(t, []string{"a", "b"}, rdb.ZRange(ctx, "zset", 0, -1).Val())

		// exclusive max
		require.Equal(t, int64(4), remrangebyscore("1", "(5"))
		require.Equal(t, []string{"e"}, rdb.ZRange(ctx, "zset", 0, -1).Val())
		require.Equal(t, int64(3), remrangebyscore("1", "(4"))
		require.Equal(t, []string{"d", "e"}, rdb.ZRange(ctx, "zset", 0, -1).Val())

		// exclusive min and max
		require.Equal(t, int64(3), remrangebyscore("(1", "(5"))
		require.Equal(t, []string{"a", "e"}, rdb.ZRange(ctx, "zset", 0, -1).Val())

		// destroy when empty
		require.Equal(t, int64(5), remrangebyscore("1", "5"))
		require.Equal(t, int64(0), rdb.Exists(ctx, "zset").Val())
	})

	t.Run("ZREMRANGEBYSCORE with non-value min or max", func(t *testing.T) {
		util.ErrorRegexp(t, rdb.ZRemRangeByScore(ctx, "fooz", "str", "1").Err(), ".*double.*")
		util.ErrorRegexp(t, rdb.ZRemRangeByScore(ctx, "fooz", "1", "str").Err(), ".*double.*")
		util.ErrorRegexp(t, rdb.ZRemRangeByScore(ctx, "fooz", "1", "NaN").Err(), ".*double.*")
	})

	t.Run("ZREMRANGEBYRANK basics", func(t *testing.T) {
		remrangebyrank := func(min, max int64) int64 {
			createZset(rdb, ctx, "zset", []redis.Z{{1, "a"}, {2, "b"}, {3, "c"},
				{4, "d"}, {5, "e"}})
			require.Equal(t, int64(1), rdb.Exists(ctx, "zset").Val())
			return rdb.ZRemRangeByRank(ctx, "zset", min, max).Val()
		}

		// inner range
		require.Equal(t, int64(3), remrangebyrank(1, 3))
		require.Equal(t, []string{"a", "e"}, rdb.ZRange(ctx, "zset", 0, -1).Val())

		// start underflow
		require.Equal(t, int64(1), remrangebyrank(-10, 0))
		require.Equal(t, []string{"b", "c", "d", "e"}, rdb.ZRange(ctx, "zset", 0, -1).Val())

		// start overflow
		require.Equal(t, int64(0), remrangebyrank(10, -1))
		require.Equal(t, []string{"a", "b", "c", "d", "e"}, rdb.ZRange(ctx, "zset", 0, -1).Val())

		// end underflow
		require.Equal(t, int64(0), remrangebyrank(0, -10))
		require.Equal(t, []string{"a", "b", "c", "d", "e"}, rdb.ZRange(ctx, "zset", 0, -1).Val())

		// end overflow
		require.Equal(t, int64(5), remrangebyrank(0, 10))
		require.Equal(t, []string{}, rdb.ZRange(ctx, "zset", 0, -1).Val())

		// destroy when empty
		require.Equal(t, int64(5), remrangebyrank(0, 4))
		require.Equal(t, int64(0), rdb.Exists(ctx, "zset").Val())
	})

	t.Run(fmt.Sprintf("ZUNIONSTORE against non-existing key doesn't set destination - %s", encoding), func(t *testing.T) {
		rdb.Del(ctx, "zseta")
		require.Equal(t, int64(0), rdb.ZUnionStore(ctx, "dst_key", &redis.ZStore{Keys: []string{"zseta"}}).Val())
		require.Equal(t, int64(0), rdb.Exists(ctx, "dst_key").Val())
	})

	t.Run(fmt.Sprintf("ZUNIONSTORE with empty set - %s", encoding), func(t *testing.T) {
		rdb.Del(ctx, "zseta", "zsetb")
		rdb.ZAdd(ctx, "zseta", redis.Z{Score: 1, Member: "a"})
		rdb.ZAdd(ctx, "zsetb", redis.Z{Score: 2, Member: "b"})
		rdb.ZUnionStore(ctx, "zsetc", &redis.ZStore{Keys: []string{"zseta", "zsetb"}})
		require.Equal(t, []redis.Z{{1, "a"}, {2, "b"}}, rdb.ZRangeWithScores(ctx, "zsetc", 0, -1).Val())
	})

	t.Run(fmt.Sprintf("ZUNIONSTORE basics - %s", encoding), func(t *testing.T) {
		rdb.Del(ctx, "zseta", "zsetb", "zsetc")
		rdb.ZAdd(ctx, "zseta", redis.Z{Score: 1, Member: "a"})
		rdb.ZAdd(ctx, "zseta", redis.Z{Score: 2, Member: "b"})
		rdb.ZAdd(ctx, "zseta", redis.Z{Score: 3, Member: "c"})
		rdb.ZAdd(ctx, "zsetb", redis.Z{Score: 1, Member: "b"})
		rdb.ZAdd(ctx, "zsetb", redis.Z{Score: 2, Member: "c"})
		rdb.ZAdd(ctx, "zsetb", redis.Z{Score: 3, Member: "d"})
		require.Equal(t, int64(4), rdb.ZUnionStore(ctx, "zsetc", &redis.ZStore{Keys: []string{"zseta", "zsetb"}}).Val())
		require.Equal(t, []redis.Z{{1, "a"}, {3, "b"}, {3, "d"}, {5, "c"}}, rdb.ZRangeWithScores(ctx, "zsetc", 0, -1).Val())
	})

	t.Run(fmt.Sprintf("ZUNIONSTORE with weights - %s", encoding), func(t *testing.T) {
		require.Equal(t, int64(4), rdb.ZUnionStore(ctx, "zsetc", &redis.ZStore{Keys: []string{"zseta", "zsetb"}, Weights: []float64{2, 3}}).Val())
		require.Equal(t, []redis.Z{{2, "a"}, {7, "b"}, {9, "d"}, {12, "c"}}, rdb.ZRangeWithScores(ctx, "zsetc", 0, -1).Val())
	})

	t.Run(fmt.Sprintf("ZUNIONSTORE with AGGREGATE MIN - %s", encoding), func(t *testing.T) {
		require.Equal(t, int64(4), rdb.ZUnionStore(ctx, "zsetc", &redis.ZStore{Keys: []string{"zseta", "zsetb"}, Aggregate: "min"}).Val())
		require.Equal(t, []redis.Z{{1, "a"}, {1, "b"}, {2, "c"}, {3, "d"}}, rdb.ZRangeWithScores(ctx, "zsetc", 0, -1).Val())

	})

	t.Run(fmt.Sprintf("ZUNIONSTORE with AGGREGATE MAX - %s", encoding), func(t *testing.T) {
		require.Equal(t, int64(4), rdb.ZUnionStore(ctx, "zsetc", &redis.ZStore{Keys: []string{"zseta", "zsetb"}, Aggregate: "max"}).Val())
		require.Equal(t, []redis.Z{{1, "a"}, {2, "b"}, {3, "c"}, {3, "d"}}, rdb.ZRangeWithScores(ctx, "zsetc", 0, -1).Val())
	})

	t.Run(fmt.Sprintf("ZUNION error - %s", encoding), func(t *testing.T) {
		rdb.Del(ctx, "zseta")

		util.ErrorRegexp(t, rdb.Do(ctx, "zunion", 1, "zseta", "wrong_arg").Err(), ".*syntax error.*")
	})

	t.Run(fmt.Sprintf("ZUNION basics - %s", encoding), func(t *testing.T) {
		rdb.Del(ctx, "zseta")
		require.NoError(t, rdb.Do(ctx, "zunion", 1, "zseta").Err())

		createZset(rdb, ctx, "zseta", []redis.Z{
			{Score: 1, Member: "a"},
			{Score: 2, Member: "b"},
			{Score: 3, Member: "c"},
		})
		createZset(rdb, ctx, "zsetb", []redis.Z{
			{Score: 1, Member: "b"},
			{Score: 2, Member: "c"},
			{Score: 3, Member: "d"},
		})

		zsetInt := []redis.Z{
			{1, "a"},
			{3, "b"},
			{3, "d"},
			{5, "c"}}
		require.Equal(t, zsetInt, rdb.ZUnionWithScores(ctx, redis.ZStore{Keys: []string{"zseta", "zsetb"}}).Val())
	})

	t.Run(fmt.Sprintf("ZUNION with weights - %s", encoding), func(t *testing.T) {
		createZset(rdb, ctx, "zseta", []redis.Z{
			{Score: 1, Member: "a"},
			{Score: 2, Member: "b"},
			{Score: 3, Member: "c"},
		})
		createZset(rdb, ctx, "zsetb", []redis.Z{
			{Score: 1, Member: "b"},
			{Score: 2, Member: "c"},
			{Score: 3, Member: "d"},
		})

		zsetInt := []redis.Z{
			{2, "a"},
			{7, "b"},
			{9, "d"},
			{12, "c"}}
		require.Equal(t, zsetInt, rdb.ZUnionWithScores(ctx, redis.ZStore{Keys: []string{"zseta", "zsetb"}, Weights: []float64{2, 3}}).Val())
	})

	t.Run(fmt.Sprintf("ZUNION with AGGREGATE MIN - %s", encoding), func(t *testing.T) {
		createZset(rdb, ctx, "zseta", []redis.Z{
			{Score: 1, Member: "a"},
			{Score: 2, Member: "b"},
			{Score: 3, Member: "c"},
		})
		createZset(rdb, ctx, "zsetb", []redis.Z{
			{Score: 1, Member: "b"},
			{Score: 2, Member: "c"},
			{Score: 3, Member: "d"},
		})

		zsetInt := []redis.Z{
			{1, "a"},
			{1, "b"},
			{2, "c"},
			{3, "d"}}
		require.Equal(t, zsetInt, rdb.ZUnionWithScores(ctx, redis.ZStore{Keys: []string{"zseta", "zsetb"}, Aggregate: "min"}).Val())
	})

	t.Run(fmt.Sprintf("ZUNIONSTORE with AGGREGATE MAX - %s", encoding), func(t *testing.T) {
		createZset(rdb, ctx, "zseta", []redis.Z{
			{Score: 1, Member: "a"},
			{Score: 2, Member: "b"},
			{Score: 3, Member: "c"},
		})
		createZset(rdb, ctx, "zsetb", []redis.Z{
			{Score: 1, Member: "b"},
			{Score: 2, Member: "c"},
			{Score: 3, Member: "d"},
		})

		zsetInt := []redis.Z{
			{1, "a"},
			{2, "b"},
			{3, "c"},
			{3, "d"}}
		require.Equal(t, zsetInt, rdb.ZUnionWithScores(ctx, redis.ZStore{Keys: []string{"zseta", "zsetb"}, Aggregate: "max"}).Val())
	})

	t.Run(fmt.Sprintf("ZINTERSTORE basics - %s", encoding), func(t *testing.T) {
		require.Equal(t, int64(2), rdb.ZInterStore(ctx, "zsetc", &redis.ZStore{Keys: []string{"zseta", "zsetb"}}).Val())
		require.Equal(t, []redis.Z{{3, "b"}, {5, "c"}}, rdb.ZRangeWithScores(ctx, "zsetc", 0, -1).Val())
	})

	t.Run(fmt.Sprintf("ZINTERSTORE with weights - %s", encoding), func(t *testing.T) {
		require.Equal(t, int64(2), rdb.ZInterStore(ctx, "zsetc", &redis.ZStore{Keys: []string{"zseta", "zsetb"}, Weights: []float64{2, 3}}).Val())
		require.Equal(t, []redis.Z{{7, "b"}, {12, "c"}}, rdb.ZRangeWithScores(ctx, "zsetc", 0, -1).Val())
	})

	t.Run(fmt.Sprintf("ZINTERSTORE with AGGREGATE MIN - %s", encoding), func(t *testing.T) {
		require.Equal(t, int64(2), rdb.ZInterStore(ctx, "zsetc", &redis.ZStore{Keys: []string{"zseta", "zsetb"}, Aggregate: "min"}).Val())
		require.Equal(t, []redis.Z{{1, "b"}, {2, "c"}}, rdb.ZRangeWithScores(ctx, "zsetc", 0, -1).Val())
	})

	t.Run(fmt.Sprintf("ZINTERSTORE with AGGREGATE MAX - %s", encoding), func(t *testing.T) {
		require.Equal(t, int64(2), rdb.ZInterStore(ctx, "zsetc", &redis.ZStore{Keys: []string{"zseta", "zsetb"}, Aggregate: "max"}).Val())
		require.Equal(t, []redis.Z{{2, "b"}, {3, "c"}}, rdb.ZRangeWithScores(ctx, "zsetc", 0, -1).Val())
	})

	t.Run(fmt.Sprintf("ZINTER with AGGREGATE and WEIGHTS - %s", encoding), func(t *testing.T) {
		createZset(rdb, ctx, "zseta", []redis.Z{
			{Score: 1, Member: "a"},
			{Score: 2, Member: "b"},
			{Score: 3, Member: "c"},
		})
		createZset(rdb, ctx, "zsetb", []redis.Z{
			{Score: 1, Member: "b"},
			{Score: 2, Member: "c"},
			{Score: 3, Member: "d"},
		})

		require.Equal(t, []string{"b", "c"}, rdb.ZInter(ctx, &redis.ZStore{Keys: []string{"zseta", "zsetb"}}).Val())
		require.Equal(t, []string{"b", "c"}, rdb.ZInter(ctx, &redis.ZStore{Keys: []string{"zseta", "zsetb"}, Aggregate: "max"}).Val())
		require.Equal(t, []string{"b", "c"}, rdb.ZInter(ctx, &redis.ZStore{Keys: []string{"zseta", "zsetb"}, Aggregate: "min"}).Val())
		require.Equal(t, []string{"b", "c"}, rdb.ZInter(ctx, &redis.ZStore{Keys: []string{"zseta", "zsetb"}, Aggregate: "sum"}).Val())

		require.Equal(t, []string{"b", "c"}, rdb.ZInter(ctx, &redis.ZStore{Keys: []string{"zseta", "zsetb"}, Weights: []float64{2, 3}, Aggregate: "sum"}).Val())
		require.Equal(t, []string{"b", "c"}, rdb.ZInter(ctx, &redis.ZStore{Keys: []string{"zseta", "zsetb"}, Weights: []float64{2, 3}, Aggregate: "max"}).Val())
		require.Equal(t, []string{"b", "c"}, rdb.ZInter(ctx, &redis.ZStore{Keys: []string{"zseta", "zsetb"}, Weights: []float64{2, 3}, Aggregate: "min"}).Val())

		require.Equal(t, []redis.Z{{3, "b"}, {5, "c"}}, rdb.ZInterWithScores(ctx, &redis.ZStore{Keys: []string{"zseta", "zsetb"}}).Val())
		require.Equal(t, []redis.Z{{2, "b"}, {3, "c"}}, rdb.ZInterWithScores(ctx, &redis.ZStore{Keys: []string{"zseta", "zsetb"}, Aggregate: "max"}).Val())
		require.Equal(t, []redis.Z{{1, "b"}, {2, "c"}}, rdb.ZInterWithScores(ctx, &redis.ZStore{Keys: []string{"zseta", "zsetb"}, Aggregate: "min"}).Val())
		require.Equal(t, []redis.Z{{3, "b"}, {5, "c"}}, rdb.ZInterWithScores(ctx, &redis.ZStore{Keys: []string{"zseta", "zsetb"}, Aggregate: "sum"}).Val())

		require.Equal(t, []redis.Z{{7, "b"}, {12, "c"}}, rdb.ZInterWithScores(ctx, &redis.ZStore{Keys: []string{"zseta", "zsetb"}, Weights: []float64{2, 3}, Aggregate: "sum"}).Val())
		require.Equal(t, []redis.Z{{4, "b"}, {6, "c"}}, rdb.ZInterWithScores(ctx, &redis.ZStore{Keys: []string{"zseta", "zsetb"}, Weights: []float64{2, 3}, Aggregate: "max"}).Val())
		require.Equal(t, []redis.Z{{3, "b"}, {6, "c"}}, rdb.ZInterWithScores(ctx, &redis.ZStore{Keys: []string{"zseta", "zsetb"}, Weights: []float64{2, 3}, Aggregate: "min"}).Val())

		require.Equal(t, 0, len(rdb.ZInter(ctx, &redis.ZStore{Keys: []string{"zseta", "zsetb", "zset_noexists"}}).Val()))
		require.Equal(t, 0, len(rdb.ZInterWithScores(ctx, &redis.ZStore{Keys: []string{"zseta", "zsetb", "zset_noexists"}, Weights: []float64{2, 3}, Aggregate: "sum"}).Val()))
	})

	t.Run(fmt.Sprintf("ZINTERCARD with LIMIT - %s", encoding), func(t *testing.T) {
		createZset(rdb, ctx, "zseta", []redis.Z{
			{Score: 1, Member: "a"},
			{Score: 2, Member: "b"},
			{Score: 3, Member: "c"},
		})
		createZset(rdb, ctx, "zsetb", []redis.Z{
			{Score: 1, Member: "b"},
			{Score: 2, Member: "c"},
			{Score: 3, Member: "d"},
		})
		require.Equal(t, int64(2), rdb.ZInterCard(ctx, 0, "zseta", "zsetb").Val())
		require.Equal(t, int64(1), rdb.ZInterCard(ctx, 1, "zseta", "zsetb").Val())

		require.Error(t, rdb.ZInterCard(ctx, -1, "zseta", "zsetb").Err())

	})

	t.Run(fmt.Sprintf("ZRANDMEMBER without scores - %s", encoding), func(t *testing.T) {
		// create a zset with 6 elements
		members := []string{"a", "b", "c", "d", "e", "f"}
		scores := []float64{1, 2, 3, 4, 5, 6}
		sort.Strings(members)
		sort.Float64s(scores)

		z := make([]redis.Z, len(members))
		for i := range members {
			z[i] = redis.Z{Score: scores[i], Member: members[i]}
		}
		createZset(rdb, ctx, "zset", z)

		// ZRANDMEMBER zset len(members)
		res := rdb.ZRandMember(ctx, "zset", len(members)).Val()
		sort.Strings(res)
		require.Equal(t, members, res)

		// ZRANDMEMBER zset len(members)+10
		res = rdb.ZRandMember(ctx, "zset", len(members)+10).Val()
		sort.Strings(res)
		require.Equal(t, members, res)

		// ZRANDMEMBER zset -len(members)
		res = rdb.ZRandMember(ctx, "zset", -len(members)).Val()
		sort.Strings(res)
		for _, v := range res {
			require.Contains(t, members, v)
		}

		// ZRANDMEMBER zset -len(members) - 10
		res = rdb.ZRandMember(ctx, "zset", -len(members)-10).Val()
		sort.Strings(res)
		require.Equal(t, len(res), len(members)+10)
		for _, v := range res {
			require.Contains(t, members, v)
		}

		// ZRANDMEMBER zset 0
		require.Equal(t, []string{}, rdb.ZRandMember(ctx, "zset", 0).Val())
		// ZRANDMEMBER zset 1
		res = rdb.ZRandMember(ctx, "zset", 1).Val()
		require.Len(t, res, 1)
		require.Contains(t, members, res[0])

		// ZRANDMEMBER zset 3
		res = rdb.ZRandMember(ctx, "zset", 3).Val()
		require.Len(t, res, 3)
		memberMap := make(map[string]struct{})
		for _, v := range res {
			require.Contains(t, members, v)
			memberMap[v] = struct{}{}
		}
		require.Equal(t, len(res), len(memberMap))

		// ZRANDMEMBER zset -3
		res = rdb.ZRandMember(ctx, "zset", -3).Val()
		require.Len(t, res, 3)
		for _, v := range res {
			require.Contains(t, members, v)
		}
	})

	t.Run(fmt.Sprintf("ZRANDMEMBER with scores - %s", encoding), func(t *testing.T) {
		// create a zset with 6 elements
		members := []string{"a", "b", "c", "d", "e", "f"}
		scores := []float64{1, 2, 3, 4, 5, 6}
		sort.Strings(members)
		sort.Float64s(scores)

		z := make([]redis.Z, len(members))
		for i := range members {
			z[i] = redis.Z{Score: scores[i], Member: members[i]}
		}
		createZset(rdb, ctx, "zset", z)

		// ZRANDMEMBER zset len(members) WITHSCORES
		res := rdb.ZRandMemberWithScores(ctx, "zset", len(members)).Val()
		sort.Slice(res, func(i, j int) bool {
			return res[i].Member.(string) < res[j].Member.(string)
		})
		require.Equal(t, z, res)

		// ZRANDMEMBER zset len(members)+10 WITHSCORES
		res = rdb.ZRandMemberWithScores(ctx, "zset", len(members)+10).Val()
		sort.Slice(res, func(i, j int) bool {
			return res[i].Member.(string) < res[j].Member.(string)
		})
		require.Equal(t, z, res)

		// ZRANDMEMBER zset -len(members) WITHSCORES
		res = rdb.ZRandMemberWithScores(ctx, "zset", -len(members)).Val()
		sort.Slice(res, func(i, j int) bool {
			return res[i].Member.(string) < res[j].Member.(string)
		})
		for _, v := range res {
			require.Contains(t, z, v)
		}

		// ZRANDMEMBER zset -len(members)-10 WITHSCORES
		res = rdb.ZRandMemberWithScores(ctx, "zset", -len(members)-10).Val()
		require.Equal(t, len(members)+10, len(res))
		for _, v := range res {
			require.Contains(t, z, v)
		}

		// ZRANDMEMBER zset 0 WITHSCORES
		require.Equal(t, []redis.Z{}, rdb.ZRandMemberWithScores(ctx, "zset", 0).Val())

		// ZRANDMEMBER zset 1 WITHSCORES
		res = rdb.ZRandMemberWithScores(ctx, "zset", 1).Val()
		require.Len(t, res, 1)
		require.Contains(t, z, res[0])

		// ZRANDMEMBER zset 3 WITHSCORES
		res = rdb.ZRandMemberWithScores(ctx, "zset", 3).Val()
		require.Len(t, res, 3)
		memberMap := make(map[string]struct{})
		for _, v := range res {
			require.Contains(t, z, v)
			memberMap[v.Member.(string)] = struct{}{}
		}
		require.Equal(t, len(res), len(memberMap))

		// ZRANDMEMBER zset -3 WITHSCORES
		res = rdb.ZRandMemberWithScores(ctx, "zset", -3).Val()
		require.Len(t, res, 3)
		for _, v := range res {
			require.Contains(t, z, v)
		}
	})

	for i, cmd := range []func(ctx context.Context, dest string, store *redis.ZStore) *redis.IntCmd{rdb.ZInterStore, rdb.ZUnionStore} {
		var funcName string
		switch i {
		case 0:
			funcName = "ZINTERSTORE"
		case 1:
			funcName = "ZUNIONSTORE"
		}

		t.Run(fmt.Sprintf("%s with +inf/-inf scores - %s", funcName, encoding), func(t *testing.T) {
			rdb.Del(ctx, "zsetinf1", "zsetinf2")

			rdb.ZAdd(ctx, "zsetinf1", redis.Z{Score: math.Inf(1), Member: "key"})
			rdb.ZAdd(ctx, "zsetinf2", redis.Z{Score: math.Inf(1), Member: "key"})
			cmd(ctx, "zsetinf3", &redis.ZStore{Keys: []string{"zsetinf1", "zsetinf2"}})
			require.Equal(t, math.Inf(1), rdb.ZScore(ctx, "zsetinf3", "key").Val())

			rdb.ZAdd(ctx, "zsetinf1", redis.Z{Score: math.Inf(-1), Member: "key"})
			rdb.ZAdd(ctx, "zsetinf2", redis.Z{Score: math.Inf(1), Member: "key"})
			cmd(ctx, "zsetinf3", &redis.ZStore{Keys: []string{"zsetinf1", "zsetinf2"}})
			require.Equal(t, float64(0), rdb.ZScore(ctx, "zsetinf3", "key").Val())

			rdb.ZAdd(ctx, "zsetinf1", redis.Z{Score: math.Inf(1), Member: "key"})
			rdb.ZAdd(ctx, "zsetinf2", redis.Z{Score: math.Inf(-1), Member: "key"})
			cmd(ctx, "zsetinf3", &redis.ZStore{Keys: []string{"zsetinf1", "zsetinf2"}})
			require.Equal(t, float64(0), rdb.ZScore(ctx, "zsetinf3", "key").Val())

			rdb.ZAdd(ctx, "zsetinf1", redis.Z{Score: math.Inf(-1), Member: "key"})
			rdb.ZAdd(ctx, "zsetinf2", redis.Z{Score: math.Inf(-1), Member: "key"})
			cmd(ctx, "zsetinf3", &redis.ZStore{Keys: []string{"zsetinf1", "zsetinf2"}})
			require.Equal(t, math.Inf(-1), rdb.ZScore(ctx, "zsetinf3", "key").Val())
		})

		t.Run(fmt.Sprintf("%s with NaN weights - %s", funcName, encoding), func(t *testing.T) {
			rdb.Del(ctx, "zsetinf1", "zsetinf2")
			rdb.ZAdd(ctx, "zsetinf1", redis.Z{Score: 1.0, Member: "key"})
			rdb.ZAdd(ctx, "zsetinf2", redis.Z{Score: 1.0, Member: "key"})
			util.ErrorRegexp(t, cmd(ctx, "zsetinf3", &redis.ZStore{
				Keys:    []string{"zsetinf1", "zsetinf2"},
				Weights: []float64{math.NaN(), math.NaN()}},
			).Err(), ".*weight.*not.*double.*")
		})
	}

	t.Run(fmt.Sprintf("ZDIFF with two sets - %s", encoding), func(t *testing.T) {
		createZset(rdb, ctx, "zseta", []redis.Z{
			{Score: 1, Member: "a"},
			{Score: 2, Member: "b"},
			{Score: 3, Member: "c"},
			{Score: 3, Member: "d"},
			{Score: 4, Member: "e"},
		})
		createZset(rdb, ctx, "zsetb", []redis.Z{
			{Score: 1, Member: "b"},
			{Score: 2, Member: "c"},
			{Score: 4, Member: "f"},
		})
		cmd := rdb.ZDiff(ctx, "zseta", "zsetb")
		require.NoError(t, cmd.Err())
		sort.Strings(cmd.Val())
		require.EqualValues(t, []string{"a", "d", "e"}, cmd.Val())
	})

	t.Run(fmt.Sprintf("ZDIFF with three sets - %s", encoding), func(t *testing.T) {
		createZset(rdb, ctx, "zseta", []redis.Z{
			{Score: 1, Member: "a"},
			{Score: 2, Member: "b"},
			{Score: 3, Member: "c"},
			{Score: 3, Member: "d"},
			{Score: 4, Member: "e"},
		})
		createZset(rdb, ctx, "zsetb", []redis.Z{
			{Score: 1, Member: "b"},
			{Score: 2, Member: "c"},
			{Score: 4, Member: "f"},
		})
		createZset(rdb, ctx, "zsetc", []redis.Z{
			{Score: 3, Member: "c"},
			{Score: 3, Member: "d"},
			{Score: 5, Member: "e"},
		})
		cmd := rdb.ZDiff(ctx, "zseta", "zsetb", "zsetc")
		require.NoError(t, cmd.Err())
		sort.Strings(cmd.Val())
		require.EqualValues(t, []string{"a"}, cmd.Val())
	})

	t.Run(fmt.Sprintf("ZDIFF with three sets with scores - %s", encoding), func(t *testing.T) {
		createZset(rdb, ctx, "zseta", []redis.Z{
			{Score: 1, Member: "a"},
			{Score: 2, Member: "b"},
			{Score: 3, Member: "c"},
			{Score: 3, Member: "d"},
			{Score: 4, Member: "e"},
		})
		createZset(rdb, ctx, "zsetb", []redis.Z{
			{Score: 1, Member: "b"},
			{Score: 2, Member: "c"},
			{Score: 4, Member: "f"},
		})
		createZset(rdb, ctx, "zsetc", []redis.Z{
			{Score: 4, Member: "c"},
			{Score: 5, Member: "e"},
		})
		cmd := rdb.ZDiffWithScores(ctx, "zseta", "zsetb", "zsetc")
		require.NoError(t, cmd.Err())
		require.EqualValues(t, []redis.Z([]redis.Z{{Score: 1, Member: "a"}, {Score: 3, Member: "d"}}), cmd.Val())
	})

	t.Run(fmt.Sprintf("ZDIFF with empty sets - %s", encoding), func(t *testing.T) {
		createZset(rdb, ctx, "zseta", []redis.Z{})
		createZset(rdb, ctx, "zsetb", []redis.Z{})
		cmd := rdb.ZDiff(ctx, "zseta", "zsetb")
		require.NoError(t, cmd.Err())
		require.EqualValues(t, []string([]string{}), cmd.Val())
	})

	t.Run(fmt.Sprintf("ZDIFF with non existing sets - %s", encoding), func(t *testing.T) {
		rdb.Del(ctx, "zseta")
		rdb.Del(ctx, "zsetb")
		cmd := rdb.ZDiff(ctx, "zseta", "zsetb")
		require.NoError(t, cmd.Err())
		require.EqualValues(t, []string([]string{}), cmd.Val())
	})

	t.Run(fmt.Sprintf("ZDIFF with missing set with scores - %s", encoding), func(t *testing.T) {
		createZset(rdb, ctx, "zseta", []redis.Z{
			{Score: 1, Member: "a"},
			{Score: 2, Member: "b"},
			{Score: 3, Member: "c"},
			{Score: 3, Member: "d"},
		})
		createZset(rdb, ctx, "zsetb", []redis.Z{
			{Score: 1, Member: "b"},
			{Score: 2, Member: "c"},
			{Score: 4, Member: "f"},
		})
		rdb.Del(ctx, "zsetc")
		cmd := rdb.ZDiffWithScores(ctx, "zseta", "zsetb", "zsetc")
		require.NoError(t, cmd.Err())
		require.EqualValues(t, []redis.Z([]redis.Z{{Score: 1, Member: "a"}, {Score: 3, Member: "d"}}), cmd.Val())
	})

	t.Run(fmt.Sprintf("ZDIFF with empty sets with scores - %s", encoding), func(t *testing.T) {
		createZset(rdb, ctx, "zseta", []redis.Z{})
		createZset(rdb, ctx, "zsetb", []redis.Z{})
		cmd := rdb.ZDiffWithScores(ctx, "zseta", "zsetb")
		require.NoError(t, cmd.Err())
		require.EqualValues(t, []redis.Z([]redis.Z{}), cmd.Val())
	})

	t.Run("ZDIFFSTORE with three sets - ", func(t *testing.T) {
		createZset(rdb, ctx, "zseta", []redis.Z{
			{Score: 1, Member: "a"},
			{Score: 2, Member: "b"},
			{Score: 3, Member: "c"},
			{Score: 3, Member: "d"},
			{Score: 4, Member: "e"},
		})
		createZset(rdb, ctx, "zsetb", []redis.Z{
			{Score: 1, Member: "b"},
			{Score: 2, Member: "c"},
			{Score: 4, Member: "f"},
		})
		createZset(rdb, ctx, "zsetc", []redis.Z{
			{Score: 4, Member: "c"},
			{Score: 5, Member: "e"},
		})
		cmd := rdb.ZDiffStore(ctx, "setres", "zseta", "zsetb", "zsetc")
		require.NoError(t, cmd.Err())
		require.EqualValues(t, int64(2), cmd.Val())
		require.Equal(t, []redis.Z([]redis.Z{{Score: 1, Member: "a"}, {Score: 3, Member: "d"}}), rdb.ZRangeWithScores(ctx, "setres", 0, -1).Val())
	})

	t.Run("ZDIFFSTORE with missing sets - ", func(t *testing.T) {
		createZset(rdb, ctx, "zseta", []redis.Z{
			{Score: 1, Member: "a"},
			{Score: 2, Member: "b"},
			{Score: 3, Member: "c"},
			{Score: 3, Member: "d"},
			{Score: 4, Member: "e"},
		})
		createZset(rdb, ctx, "zsetb", []redis.Z{
			{Score: 1, Member: "b"},
			{Score: 2, Member: "c"},
			{Score: 4, Member: "f"},
			{Score: 4, Member: "e"},
		})
		rdb.Del(ctx, "zsetc")
		cmd := rdb.ZDiffStore(ctx, "setres", "zseta", "zsetb", "zsetc")
		require.NoError(t, cmd.Err())
		require.EqualValues(t, int64(2), cmd.Val())
		require.Equal(t, []redis.Z([]redis.Z{{Score: 1, Member: "a"}, {Score: 3, Member: "d"}}), rdb.ZRangeWithScores(ctx, "setres", 0, -1).Val())
	})

	t.Run("ZDIFFSTORE with missing sets - ", func(t *testing.T) {
		rdb.Del(ctx, "zseta")
		rdb.Del(ctx, "zsetb")
		rdb.Del(ctx, "zsetc")
		cmd := rdb.ZDiffStore(ctx, "setres", "zseta", "zsetb", "zsetc")
		require.NoError(t, cmd.Err())
		require.EqualValues(t, int64(0), cmd.Val())
		require.Equal(t, []redis.Z([]redis.Z{}), rdb.ZRangeWithScores(ctx, "setres", 0, -1).Val())
	})
}

func stressTests(t *testing.T, rdb *redis.Client, ctx context.Context, encoding string) {
	var elements int
	if encoding == "ziplist" {
		elements = 128
	} else if encoding == "skiplist" {
		elements = 100
	} else {
		fmt.Println("Unknown sorted set encoding")
		return
	}
	t.Run(fmt.Sprintf("ZSCORE - %s", encoding), func(t *testing.T) {
		rdb.Del(ctx, "zscoretest")
		aux := make([]float64, 0)
		for i := 0; i < elements; i++ {
			score := rand.Float64()
			aux = append(aux, score)
			rdb.ZAdd(ctx, "zscoretest", redis.Z{Score: score, Member: strconv.Itoa(i)})
		}
		for i := 0; i < elements; i++ {
			require.Equal(t, aux[i], rdb.ZScore(ctx, "zscoretest", strconv.Itoa(i)).Val())
		}
	})

	t.Run(fmt.Sprintf("ZSET sorting stresser - %s", encoding), func(t *testing.T) {
		delta := 0
		for test := 0; test < 2; test++ {
			auxArray := make(map[string]float64)
			auxList := make([]redis.Z, 0)
			rdb.Del(ctx, "myzset")
			var score float64
			for i := 0; i < elements; i++ {
				if test == 0 {
					score = rand.Float64()
				} else {
					score = float64(rand.Intn(10))
				}
				auxArray[strconv.Itoa(i)] = score
				rdb.ZAdd(ctx, "myzset", redis.Z{Score: score, Member: strconv.Itoa(i)})
				if rand.Float64() < 0.2 {
					j := rand.Intn(1000)
					if test == 0 {
						score = rand.Float64()
					} else {
						score = float64(rand.Intn(10))

					}
					auxArray[strconv.Itoa(j)] = score
					rdb.ZAdd(ctx, "myzset", redis.Z{Score: score, Member: strconv.Itoa(j)})
				}
			}
			for i, s := range auxArray {
				auxList = append(auxList, redis.Z{Score: s, Member: i})
			}
			sort.Slice(auxList, func(i, j int) bool {
				if auxList[i].Score < auxList[j].Score {
					return true
				} else if auxList[i].Score > auxList[j].Score {
					return false
				} else {
					if strings.Compare(auxList[i].Member.(string), auxList[j].Member.(string)) == 1 {
						return false
					} else {
						return true
					}
				}
			})
			var aux []string
			for _, z := range auxList {
				aux = append(aux, z.Member.(string))
			}
			fromRedis := rdb.ZRange(ctx, "myzset", 0, -1).Val()
			for i := 0; i < len(fromRedis); i++ {
				if aux[i] != fromRedis[i] {
					delta++
				}
			}
			require.Equal(t, 0, delta)
		}
	})

	t.Run(fmt.Sprintf("ZRANGEBYSCORE fuzzy test, 100 ranges in %d element sorted set - %s", elements, encoding), func(t *testing.T) {
		rdb.Del(ctx, "zset")
		for i := 0; i < elements; i++ {
			rdb.ZAdd(ctx, "zset", redis.Z{Score: rand.Float64(), Member: strconv.Itoa(i)})
		}

		for i := 0; i < 100; i++ {
			min, max := rand.Float64(), rand.Float64()
			min, max = math.Min(min, max), math.Max(min, max)
			low := rdb.ZRangeByScore(ctx, "zset", &redis.ZRangeBy{Min: "-inf", Max: fmt.Sprintf("%v", min)}).Val()
			ok := rdb.ZRangeByScore(ctx, "zset", &redis.ZRangeBy{Min: fmt.Sprintf("%v", min), Max: fmt.Sprintf("%v", max)}).Val()
			high := rdb.ZRangeByScore(ctx, "zset", &redis.ZRangeBy{Min: fmt.Sprintf("%v", max), Max: "+inf"}).Val()
			lowEx := rdb.ZRangeByScore(ctx, "zset", &redis.ZRangeBy{Min: "-inf", Max: fmt.Sprintf("(%v", min)}).Val()
			okEx := rdb.ZRangeByScore(ctx, "zset", &redis.ZRangeBy{Min: fmt.Sprintf("(%v", min), Max: fmt.Sprintf("(%v", max)}).Val()
			highEx := rdb.ZRangeByScore(ctx, "zset", &redis.ZRangeBy{Min: fmt.Sprintf("(%v", max), Max: "+inf"}).Val()

			require.Len(t, low, int(rdb.ZCount(ctx, "zset", "-inf", fmt.Sprintf("%v", min)).Val()))
			require.Len(t, ok, int(rdb.ZCount(ctx, "zset", fmt.Sprintf("%v", min), fmt.Sprintf("%v", max)).Val()))
			require.Len(t, high, int(rdb.ZCount(ctx, "zset", fmt.Sprintf("%v", max), "+inf").Val()))
			require.Len(t, lowEx, int(rdb.ZCount(ctx, "zset", "-inf", fmt.Sprintf("(%v", min)).Val()))
			require.Len(t, okEx, int(rdb.ZCount(ctx, "zset", fmt.Sprintf("(%v", min), fmt.Sprintf("(%v", max)).Val()))
			require.Len(t, highEx, int(rdb.ZCount(ctx, "zset", fmt.Sprintf("(%v", max), "+inf").Val()))

			for _, x := range low {
				require.LessOrEqual(t, rdb.ZScore(ctx, "zset", x).Val(), min)
			}
			for _, x := range lowEx {
				require.Less(t, rdb.ZScore(ctx, "zset", x).Val(), min)
			}
			for _, x := range ok {
				util.BetweenValues(t, rdb.ZScore(ctx, "zset", x).Val(), min, max)
			}
			for _, x := range okEx {
				util.BetweenValuesEx(t, rdb.ZScore(ctx, "zset", x).Val(), min, max)
			}
			for _, x := range high {
				require.GreaterOrEqual(t, rdb.ZScore(ctx, "zset", x).Val(), min)
			}
			for _, x := range highEx {
				require.Greater(t, rdb.ZScore(ctx, "zset", x).Val(), min)
			}
		}
	})

	t.Run(fmt.Sprintf("ZRANGEBYLEX fuzzy test, 100 ranges in %d element sorted set - %s", elements, encoding), func(t *testing.T) {
		rdb.Del(ctx, "zset")

		var lexSet []string
		for i := 0; i < elements; i++ {
			e := util.RandString(0, 30, util.Alpha)
			lexSet = append(lexSet, e)
			rdb.ZAdd(ctx, "zset", redis.Z{Member: e})
		}
		sort.Strings(lexSet)
		lexSet = slices.Compact(lexSet)

		for i := 0; i < 100; i++ {
			min, max := util.RandString(0, 30, util.Alpha), util.RandString(0, 30, util.Alpha)
			minInc, maxInc := util.RandomBool(), util.RandomBool()
			cMin, cMax := "("+min, "("+max
			if minInc {
				cMin = "[" + min
			}
			if maxInc {
				cMax = "[" + max
			}
			rev := util.RandomBool()

			// make sure data is the same in both sides
			require.Equal(t, lexSet, rdb.ZRange(ctx, "zset", 0, -1).Val())

			var output []string
			var outLen int64
			if rev {
				output = rdb.ZRevRangeByLex(ctx, "zset", &redis.ZRangeBy{Min: cMax, Max: cMin}).Val()
				outLen = rdb.ZLexCount(ctx, "zset", cMax, cMin).Val()
			} else {
				output = rdb.ZRangeByLex(ctx, "zset", &redis.ZRangeBy{Min: cMin, Max: cMax}).Val()
				outLen = rdb.ZLexCount(ctx, "zset", cMin, cMax).Val()
			}

			// compute the same output by programming
			o := make([]string, 0)
			c := lexSet
			if (!rev && min > max) || (rev && max > min) {
				// empty output when ranges are inverted
			} else {
				if rev {
					c = rdb.ZRevRange(ctx, "zset", 0, -1).Val()
					min, max, minInc, maxInc = max, min, maxInc, minInc
				}

				for _, e := range c {
					if (minInc && e >= min || !minInc && e > min) && (maxInc && e <= max || !maxInc && e < max) {
						o = append(o, e)
					}
				}
			}
			require.Equal(t, o, output)
			require.Len(t, output, int(outLen))
		}
	})

	t.Run(fmt.Sprintf("ZREMRANGEBYLEX fuzzy test, 100 ranges in %d element sorted set - %s", elements, encoding), func(t *testing.T) {
		var lexSet []string
		rdb.Del(ctx, "zset", "zsetcopy")
		for i := 0; i < elements; i++ {
			e := util.RandString(0, 30, util.Alpha)
			lexSet = append(lexSet, e)
			rdb.ZAdd(ctx, "zset", redis.Z{Member: e})
		}
		sort.Strings(lexSet)
		lexSet = slices.Compact(lexSet)
		for i := 0; i < 100; i++ {
			rdb.ZUnionStore(ctx, "zsetcopy", &redis.ZStore{Keys: []string{"zset"}})
			var lexSetCopy []string
			lexSetCopy = append(lexSetCopy, lexSet...)
			min, max := util.RandString(0, 30, util.Alpha), util.RandString(0, 30, util.Alpha)
			minInc, maxInc := util.RandomBool(), util.RandomBool()
			cMin, cMax := "("+min, "("+max
			if minInc {
				cMin = "[" + min
			}
			if maxInc {
				cMax = "[" + max
			}
			require.Equal(t, lexSet, rdb.ZRange(ctx, "zset", 0, -1).Val())
			toRem := rdb.ZRangeByLex(ctx, "zset", &redis.ZRangeBy{Min: cMin, Max: cMax}).Val()
			toRemLen := rdb.ZLexCount(ctx, "zset", cMin, cMax).Val()
			rdb.ZRemRangeByLex(ctx, "zsetcopy", cMin, cMax)
			output := rdb.ZRange(ctx, "zsetcopy", 0, -1).Val()
			if toRemLen > 0 {
				var first, last int64
				for idx, v := range lexSetCopy {
					if v == toRem[0] {
						first = int64(idx)
					}
				}
				last = first + toRemLen - 1
				lexSetCopy = append(lexSetCopy[:first], lexSetCopy[last+1:]...)
			}
			require.Equal(t, lexSetCopy, output)
		}
	})

	t.Run(fmt.Sprintf("ZSETs skiplist implementation backlink consistency test - %s", encoding), func(t *testing.T) {
		diff := 0
		for i := 0; i < elements; i++ {
			rdb.ZAdd(ctx, "zset", redis.Z{Score: rand.Float64(), Member: fmt.Sprintf("Element-%d", i)})
			rdb.ZRem(ctx, "myzset", fmt.Sprintf("Element-%d", rand.Intn(elements)))
		}
		l1 := rdb.ZRange(ctx, "myzset", 0, -1).Val()
		l2 := rdb.ZRevRange(ctx, "myzset", 0, -1).Val()
		for j := 0; j < len(l1); j++ {
			if l1[j] != l2[len(l1)-j-1] {
				diff++
			}
		}
		require.Equal(t, 0, diff)
	})

	t.Run(fmt.Sprintf("ZSETs ZRANK augmented skip list stress testing - %s", encoding), func(t *testing.T) {
		var err error
		rdb.Del(ctx, "myzset")
		for k := 0; k < 2000; k++ {
			i := k % elements
			if rand.Float64() < 0.2 {
				rdb.ZRem(ctx, "myzset", strconv.Itoa(i))
			} else {
				score := rand.Float64()
				rdb.ZAdd(ctx, "myzset", redis.Z{Score: score, Member: strconv.Itoa(i)})
			}
			card := rdb.ZCard(ctx, "myzset").Val()
			if card > 0 {
				index := util.RandomInt(card)
				ele := rdb.ZRange(ctx, "myzset", index, index).Val()[0]
				rank := rdb.ZRank(ctx, "myzset", ele).Val()
				if rank != index {
					err = fmt.Errorf("%s RANK is wrong! (%d != %d)", ele, rank, index)
					break
				}
			}
		}
		require.NoError(t, err)
	})
}

func TestZSet(t *testing.T) {
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
		testZSet(t, configs)
	}
}

var testZSet = func(t *testing.T, configs util.KvrocksServerConfigs) {
	srv := util.StartServer(t, configs)
	defer srv.Close()
	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	basicTests(t, rdb, ctx, configs["resp3-enabled"], "skiplist", srv)

	t.Run("ZUNIONSTORE regression, should not create NaN in scores", func(t *testing.T) {
		rdb.ZAdd(ctx, "z", redis.Z{Score: math.Inf(-1), Member: "neginf"})
		rdb.ZUnionStore(ctx, "out", &redis.ZStore{Keys: []string{"z"}, Weights: []float64{0}})
		require.Equal(t, []redis.Z{{0, "neginf"}}, rdb.ZRangeWithScores(ctx, "out", 0, -1).Val())
	})

	t.Run("ZUNIONSTORE result is sorted", func(t *testing.T) {
		rdb.Del(ctx, "one", "two", "dest")
		var zset1 []redis.Z
		var zset2 []redis.Z
		for j := 0; j < 1000; j++ {
			zset1 = append(zset1, redis.Z{Score: float64(util.RandomInt(1000)), Member: util.RandomValue()})
			zset2 = append(zset2, redis.Z{Score: float64(util.RandomInt(1000)), Member: util.RandomValue()})
		}
		rdb.ZAdd(ctx, "one", zset1...)
		rdb.ZAdd(ctx, "two", zset2...)
		require.Greater(t, rdb.ZCard(ctx, "one").Val(), int64(100))
		require.Greater(t, rdb.ZCard(ctx, "two").Val(), int64(100))
		rdb.ZUnionStore(ctx, "dest", &redis.ZStore{Keys: []string{"one", "two"}})
		oldScore := float64(0)
		for _, z := range rdb.ZRangeWithScores(ctx, "dest", 0, -1).Val() {
			require.GreaterOrEqual(t, z.Score, oldScore)
			oldScore = z.Score
		}
	})

	t.Run("ZSET commands don't accept the empty strings as valid score", func(t *testing.T) {
		util.ErrorRegexp(t, rdb.Do(ctx, "zadd", "myzset", "", "abc").Err(), ".*not.*float.*")
	})

	stressTests(t, rdb, ctx, "skiplist")
}
