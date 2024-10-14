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

package dump

import (
	"context"
	"fmt"
	"testing"

	"github.com/apache/kvrocks/tests/gocase/util"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestDump_String(t *testing.T) {
	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	keyValues := map[string]string{
		"test_string_key0": "hello,world!",
		"test_string_key1": "654321",
	}
	for key, value := range keyValues {
		require.NoError(t, rdb.Del(ctx, key).Err())
		require.NoError(t, rdb.Set(ctx, key, value, 0).Err())
		serialized, err := rdb.Dump(ctx, key).Result()
		require.NoError(t, err)

		restoredKey := fmt.Sprintf("restore_%s", key)
		require.NoError(t, rdb.RestoreReplace(ctx, restoredKey, 0, serialized).Err())
		require.Equal(t, value, rdb.Get(ctx, restoredKey).Val())
	}
}

func TestDump_Hash(t *testing.T) {
	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	key := "test_hash_key"
	fields := map[string]string{
		"name":        "redis tutorial",
		"description": "redis basic commands for caching",
		"likes":       "20",
		"visitors":    "23000",
	}
	require.NoError(t, rdb.Del(ctx, key).Err())
	require.NoError(t, rdb.HMSet(ctx, key, fields).Err())
	serialized, err := rdb.Dump(ctx, key).Result()
	require.NoError(t, err)

	restoredKey := fmt.Sprintf("restore_%s", key)
	require.NoError(t, rdb.RestoreReplace(ctx, restoredKey, 0, serialized).Err())
	require.EqualValues(t, fields, rdb.HGetAll(ctx, restoredKey).Val())
}

func TestDump_ZSet(t *testing.T) {
	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	memberScores := []redis.Z{{Member: "kvrocks1", Score: 1}, {Member: "kvrocks2", Score: 2}, {Member: "kvrocks3", Score: 3}}
	key := "test_zset_key"
	require.NoError(t, rdb.Del(ctx, key).Err())
	require.NoError(t, rdb.ZAdd(ctx, key, memberScores...).Err())
	serialized, err := rdb.Dump(ctx, key).Result()
	require.NoError(t, err)

	restoredKey := fmt.Sprintf("restore_%s", key)
	require.NoError(t, rdb.RestoreReplace(ctx, restoredKey, 0, serialized).Err())

	require.EqualValues(t, memberScores, rdb.ZRangeWithScores(ctx, restoredKey, 0, -1).Val())
}

func TestDump_List(t *testing.T) {
	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	elements := []string{"kvrocks1", "kvrocks2", "kvrocks3"}
	key := "test_list_key"
	require.NoError(t, rdb.Del(ctx, key).Err())
	require.NoError(t, rdb.RPush(ctx, key, elements).Err())
	serialized, err := rdb.Dump(ctx, key).Result()
	require.NoError(t, err)
	require.Equal(t, "\x0e\x03\x15\x15\x00\x00\x00\n\x00\x00\x00\x01\x00\x00\bkvrocks1\xff\x15\x15\x00\x00\x00\n\x00\x00\x00\x01\x00\x00\bkvrocks2\xff\x15\x15\x00\x00\x00\n\x00\x00\x00\x01\x00\x00\bkvrocks3\xff\x06\x00u\xc7\x19h\x1da\xd0\xd8", serialized)

	restoredKey := fmt.Sprintf("restore_%s", key)
	require.NoError(t, rdb.RestoreReplace(ctx, restoredKey, 0, serialized).Err())
	require.EqualValues(t, elements, rdb.LRange(ctx, restoredKey, 0, -1).Val())

	//test special case
	elements = []string{"A", " ", "", util.RandString(0, 4000, util.Alpha)}
	require.NoError(t, rdb.Del(ctx, key).Err())
	require.NoError(t, rdb.RPush(ctx, key, elements).Err())
	serialized, err = rdb.Dump(ctx, key).Result()
	require.NoError(t, err)

	require.NoError(t, rdb.RestoreReplace(ctx, restoredKey, 0, serialized).Err())
	require.EqualValues(t, elements, rdb.LRange(ctx, restoredKey, 0, -1).Val())
}

func TestDump_Set(t *testing.T) {
	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	members := []string{"kvrocks1", "kvrocks2", "kvrocks3"}
	key := "test_set_key"
	require.NoError(t, rdb.Del(ctx, key).Err())
	require.NoError(t, rdb.SAdd(ctx, key, members).Err())
	serialized, err := rdb.Dump(ctx, key).Result()
	require.NoError(t, err)

	restoredKey := fmt.Sprintf("restore_%s", key)
	require.NoError(t, rdb.RestoreReplace(ctx, restoredKey, 0, serialized).Err())
	require.ElementsMatch(t, members, rdb.SMembers(ctx, restoredKey).Val())
}

func TestDump_Bitset(t *testing.T) {
	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	key := "bitsetKey1"
	require.NoError(t, rdb.SetBit(ctx, key, 1, 1).Err())
	serialized, err := rdb.Dump(ctx, key).Result()
	require.NoError(t, err)

	restoredKey := fmt.Sprintf("restore_%s", key)
	require.NoError(t, rdb.RestoreReplace(ctx, restoredKey, 0, serialized).Err())
	require.Equal(t, rdb.Get(ctx, key).Val(), rdb.Get(ctx, restoredKey).Val())
}

func TestDump_IntegerEncoding(t *testing.T) {
	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	values := []int64{
		0, 1, -1, 127, -128, 128, -129, 129,
		32767, -32768, 32768, -32769,
		2147483647, -2147483648, 2147483648, -2147483649,
	}
	for i, v := range values {
		key := fmt.Sprintf("key_%d", i)
		require.NoError(t, rdb.Set(ctx, key, v, 0).Err())
		serialized, err := rdb.Dump(ctx, key).Result()
		require.NoError(t, err)
		restoreKey := fmt.Sprintf("restore_%s", key)
		require.NoError(t, rdb.Restore(ctx, restoreKey, 0, serialized).Err())
		got, err := rdb.Get(ctx, restoreKey).Int64()
		require.NoError(t, err)
		require.Equal(t, v, got)
	}
}
