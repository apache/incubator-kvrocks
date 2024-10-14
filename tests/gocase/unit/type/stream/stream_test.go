//go:build !ignore_when_tsan

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
package stream

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/apache/kvrocks/tests/gocase/util"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestStream(t *testing.T) {
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
		streamTests(t, configs)
	}
}

var streamTests = func(t *testing.T, configs util.KvrocksServerConfigs) {
	srv := util.StartServer(t, configs)
	defer srv.Close()
	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	t.Run("XADD wrong number of args", func(t *testing.T) {
		require.ErrorContains(t, rdb.Do(ctx, "XADD", "mystream").Err(), "wrong number of arguments")
		require.ErrorContains(t, rdb.Do(ctx, "XADD", "mystream", "*").Err(), "wrong number of arguments")
		require.ErrorContains(t, rdb.Do(ctx, "XADD", "mystream", "*", "field").Err(), "wrong number of arguments")
	})

	t.Run("XADD can add entries into a stream that XRANGE can fetch", func(t *testing.T) {
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", Values: []string{"item", "1", "value", "a"}}).Err())
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", Values: []string{"item", "2", "value", "b"}}).Err())

		require.EqualValues(t, 2, rdb.XLen(ctx, "mystream").Val())

		items := rdb.XRange(ctx, "mystream", "-", "+").Val()
		require.Len(t, items, 2)
		require.EqualValues(t, map[string]interface{}{"item": "1", "value": "a"}, items[0].Values)
		require.EqualValues(t, map[string]interface{}{"item": "2", "value": "b"}, items[1].Values)
	})

	t.Run("XADD stores entry value with respect to case sensitivity", func(t *testing.T) {
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{Stream: "myStream", Values: []string{"iTeM", "1", "vAluE", "a"}}).Err())
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{Stream: "myStream", Values: []string{"ItEm", "2", "VaLUe", "B"}}).Err())
		require.EqualValues(t, 2, rdb.XLen(ctx, "myStream").Val())

		items := rdb.XRange(ctx, "myStream", "-", "+").Val()
		require.Len(t, items, 2)
		require.EqualValues(t, map[string]interface{}{"iTeM": "1", "vAluE": "a"}, items[0].Values)
		require.EqualValues(t, map[string]interface{}{"ItEm": "2", "VaLUe": "B"}, items[1].Values)
	})

	t.Run("XADD IDs are incremental", func(t *testing.T) {
		x1 := rdb.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", Values: []string{"item", "1", "value", "a"}}).Val()
		x2 := rdb.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", Values: []string{"item", "2", "value", "b"}}).Val()
		x3 := rdb.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", Values: []string{"item", "3", "value", "c"}}).Val()
		require.Less(t, x1, x2)
		require.Less(t, x2, x3)
	})

	t.Run("XADD IDs are incremental when ms is the same as well", func(t *testing.T) {
		require.NoError(t, rdb.Do(ctx, "MULTI").Err())
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", Values: []string{"item", "1", "value", "a"}}).Err())
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", Values: []string{"item", "2", "value", "b"}}).Err())
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", Values: []string{"item", "3", "value", "c"}}).Err())
		v := rdb.Do(ctx, "EXEC").Val().([]interface{})
		require.Len(t, v, 3)
		require.Less(t, v[0], v[1])
		require.Less(t, v[1], v[2])
	})

	t.Run("XADD IDs correctly report an error when overflowing", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "mystream").Err())
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", ID: "18446744073709551614-18446744073709551615", Values: []string{"a", "b"}}).Err())
		require.ErrorContains(t, rdb.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", ID: "*", Values: []string{"c", "d"}}).Err(), "ERR")
	})

	t.Run("XADD auto-generated sequence is incremented for last ID", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "mystream").Err())
		x1 := rdb.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", ID: "123-456", Values: []string{"item", "1", "value", "a"}}).Val()
		x2 := rdb.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", ID: "123-*", Values: []string{"item", "2", "value", "b"}}).Val()
		require.Equal(t, "123-457", x2)
		require.Less(t, x1, x2)
	})

	t.Run("XADD auto-generated sequence is zero for future timestamp ID", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "mystream").Err())
		x1 := rdb.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", ID: "123-456", Values: []string{"item", "1", "value", "a"}}).Val()
		x2 := rdb.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", ID: "789-*", Values: []string{"item", "2", "value", "b"}}).Val()
		require.Equal(t, "789-0", x2)
		require.Less(t, x1, x2)
	})

	t.Run("XADD auto-generated sequence can't be smaller than last ID", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "mystream").Err())
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", ID: "123-456", Values: []string{"item", "1", "value", "a"}}).Err())
		require.ErrorContains(t, rdb.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", ID: "42-*", Values: []string{"item", "2", "value", "b"}}).Err(), "ERR")
	})

	t.Run("XADD auto-generated sequence can't overflow", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "mystream").Err())
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", ID: "1-18446744073709551615", Values: []string{"a", "b"}}).Err())
		require.ErrorContains(t, rdb.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", ID: "1-*", Values: []string{"c", "d"}}).Err(), "ERR")
	})

	t.Run("XADD 0-* should succeed", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "mystream").Err())
		x := rdb.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", ID: "0-*", Values: []string{"a", "b"}}).Val()
		require.Equal(t, "0-1", x)
	})

	t.Run("XADD with MAXLEN option", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "mystream").Err())
		for i := 0; i < 1000; i++ {
			if rand.Float64() < 0.9 {
				require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", MaxLen: 5, Values: []string{"xitem", strconv.Itoa(i)}}).Err())
			} else {
				require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", MaxLen: 5, Values: []string{"yitem", strconv.Itoa(i)}}).Err())
			}
		}
		require.EqualValues(t, 5, rdb.XLen(ctx, "mystream").Val())
		items := rdb.XRange(ctx, "mystream", "-", "+").Val()
		expected := 995
		for _, item := range items {
			require.Subset(t, map[string]interface{}{"xitem": strconv.Itoa(expected), "yitem": strconv.Itoa(expected)}, item.Values)
			expected++
		}
	})

	t.Run("XADD with MAXLEN option and the '=' argument", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "mystream").Err())
		for i := 0; i < 1000; i++ {
			if rand.Float64() < 0.9 {
				require.NoError(t, rdb.Do(ctx, "XADD", "mystream", "MAXLEN", "=", "5", "*", "xitem", "i").Err())
			} else {
				require.NoError(t, rdb.Do(ctx, "XADD", "mystream", "MAXLEN", "=", "5", "*", "yitem", "i").Err())
			}
		}
		require.EqualValues(t, 5, rdb.XLen(ctx, "mystream").Val())
	})

	t.Run("XADD with NOMKSTREAM option", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "mystream").Err())
		require.Empty(t, rdb.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", NoMkStream: true, Values: []string{"item", "1", "value", "a"}}).Val())
		require.Zero(t, rdb.Exists(ctx, "mystream").Val())
		require.NotEmpty(t, rdb.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", Values: []string{"item", "1", "value", "a"}}).Val())
		require.NotEmpty(t, rdb.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", NoMkStream: true, Values: []string{"item", "2", "value", "b"}}).Val())
		require.EqualValues(t, 2, rdb.XLen(ctx, "mystream").Val())
		items := rdb.XRange(ctx, "mystream", "-", "+").Val()
		require.Len(t, items, 2)
		require.EqualValues(t, map[string]interface{}{"item": "1", "value": "a"}, items[0].Values)
		require.EqualValues(t, map[string]interface{}{"item": "2", "value": "b"}, items[1].Values)
	})

	t.Run("XADD with MINID option", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "mystream").Err())
		buildXAddArgs := func(id int, tag string) *redis.XAddArgs {
			c := id - 5
			if c < 0 {
				c = 1000
			}
			return &redis.XAddArgs{Stream: "mystream", MinID: strconv.Itoa(c), ID: strconv.Itoa(id), Values: []string{"xitem", strconv.Itoa(id)}}
		}
		for i := 0; i < 1000; i++ {
			if rand.Float64() < 0.9 {
				require.NoError(t, rdb.XAdd(ctx, buildXAddArgs(i+1, "xitem")).Err())
			} else {
				require.NoError(t, rdb.XAdd(ctx, buildXAddArgs(i+1, "yitem")).Err())
			}
		}
		require.EqualValues(t, 6, rdb.XLen(ctx, "mystream").Val())
		items := rdb.XRange(ctx, "mystream", "-", "+").Val()
		expected := 995
		for _, item := range items {
			require.Subset(t, map[string]interface{}{"xitem": strconv.Itoa(expected), "yitem": strconv.Itoa(expected)}, item.Values)
			expected++
		}
	})

	t.Run("XTRIM with MINID option", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "mystream").Err())
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", ID: "1-0", Values: []string{"f", "v"}}).Err())
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", ID: "2-0", Values: []string{"f", "v"}}).Err())
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", ID: "3-0", Values: []string{"f", "v"}}).Err())
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", ID: "4-0", Values: []string{"f", "v"}}).Err())
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", ID: "5-0", Values: []string{"f", "v"}}).Err())
		require.NoError(t, rdb.XTrimMinID(ctx, "mystream", "3-0").Err())
		items := rdb.XRange(ctx, "mystream", "-", "+").Val()
		require.Len(t, items, 3)
		require.EqualValues(t, "3-0", items[0].ID)
		require.EqualValues(t, "4-0", items[1].ID)
		require.EqualValues(t, "5-0", items[2].ID)
	})

	t.Run("XTRIM with MINID option, big delta from master record", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "mystream").Err())
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", ID: "1-0", Values: []string{"f", "v"}}).Err())
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", ID: "1641544570597-0", Values: []string{"f", "v"}}).Err())
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", ID: "1641544570597-1", Values: []string{"f", "v"}}).Err())
		require.NoError(t, rdb.XTrimMinID(ctx, "mystream", "1641544570597-0").Err())
		items := rdb.XRange(ctx, "mystream", "-", "+").Val()
		require.Len(t, items, 2)
		require.EqualValues(t, "1641544570597-0", items[0].ID)
		require.EqualValues(t, "1641544570597-1", items[1].ID)
	})

	t.Run("XADD mass insertion and XLEN", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "mystream").Err())
		insertIntoStreamKey(t, rdb, "mystream")
		items := rdb.XRange(ctx, "mystream", "-", "+").Val()
		require.Len(t, items, 1000)
		for i := 0; i < 1000; i++ {
			require.Subset(t, items[i].Values, map[string]interface{}{"item": strconv.Itoa(i)})
		}
	})

	t.Run("XADD with ID 0-0", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "otherstream").Err())
		require.Error(t, rdb.XAdd(ctx, &redis.XAddArgs{
			Stream: "otherstream",
			ID:     "0-0",
			Values: []string{"k", "v"},
		}).Err())
		require.Zero(t, rdb.Exists(ctx, "otherstream").Val())
	})

	t.Run("XRANGE COUNT works as expected", func(t *testing.T) {
		require.Len(t, rdb.XRangeN(ctx, "mystream", "-", "+", 10).Val(), 10)
	})

	t.Run("XREVRANGE COUNT works as expected", func(t *testing.T) {
		require.Len(t, rdb.XRevRangeN(ctx, "mystream", "+", "-", 10).Val(), 10)
	})

	t.Run("XRANGE can be used to iterate the whole stream", func(t *testing.T) {
		lastID, c := "-", 0
		for {
			items := rdb.XRangeN(ctx, "mystream", lastID, "+", 100).Val()
			if len(items) == 0 {
				break
			}
			for _, item := range items {
				require.Subset(t, item.Values, map[string]interface{}{"item": strconv.Itoa(c)})
				c++
			}
			lastID = streamNextID(t, items[len(items)-1].ID)
		}
		require.Equal(t, 1000, c)
	})

	t.Run("XREVRANGE returns the reverse of XRANGE", func(t *testing.T) {
		items := rdb.XRange(ctx, "mystream", "-", "+").Val()
		revItems := rdb.XRevRange(ctx, "mystream", "+", "-").Val()
		util.ReverseSlice(revItems)
		require.EqualValues(t, items, revItems)
	})

	t.Run("XRANGE exclusive ranges", func(t *testing.T) {
		ids := []string{"0-1", "0-18446744073709551615", "1-0", "42-0", "42-42", "18446744073709551614-18446744073709551614", "18446744073709551614-18446744073709551615"}
		total := len(ids)
		require.NoError(t, rdb.Do(ctx, "MULTI").Err())
		// DEL returns "QUEUED" here, so we use Do to avoid ParseInt.
		require.NoError(t, rdb.Do(ctx, "DEL", "vipstream").Err())
		for _, id := range ids {
			require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{
				Stream: "vipstream",
				ID:     id,
				Values: []string{"foo", "bar"},
			}).Err())
		}
		require.NoError(t, rdb.Do(ctx, "EXEC").Err())
		require.Len(t, rdb.XRange(ctx, "vipstream", "-", "+").Val(), total)
		require.Len(t, rdb.XRange(ctx, "vipstream", "("+ids[0], "+").Val(), total-1)
		require.Len(t, rdb.XRange(ctx, "vipstream", "-", "("+ids[total-1]).Val(), total-1)
		require.Len(t, rdb.XRange(ctx, "vipstream", "(0-1", "(1-0").Val(), 1)
		require.Len(t, rdb.XRange(ctx, "vipstream", "(1-0", "(42-42").Val(), 1)
		require.ErrorContains(t, rdb.XRange(ctx, "vipstream", "(-", "+").Err(), "ERR")
		require.ErrorContains(t, rdb.XRange(ctx, "vipstream", "-", "(+").Err(), "ERR")
		require.ErrorContains(t, rdb.XRange(ctx, "vipstream", "(18446744073709551614-18446744073709551615", "+").Err(), "ERR")
		require.ErrorContains(t, rdb.XRange(ctx, "vipstream", "-", "(0-0").Err(), "ERR")
	})

	t.Run("XREAD with non empty stream", func(t *testing.T) {
		r := rdb.XRead(ctx, &redis.XReadArgs{
			Streams: []string{"mystream", "0-0"},
			Count:   1,
		}).Val()
		require.Len(t, r, 1)
		require.Equal(t, "mystream", r[0].Stream)
		require.Len(t, r[0].Messages, 1)
		require.Subset(t, r[0].Messages[0].Values, map[string]interface{}{"item": "0"})
	})

	t.Run("Non blocking XREAD with empty streams", func(t *testing.T) {
		// go-redis blocks underneath; fallback to Do
		require.Empty(t, rdb.Do(ctx, "XREAD", "STREAMS", "s1", "s2", "0-0", "0-0").Val())
	})

	t.Run("XREAD with non empty second stream", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "mystream").Err())
		insertIntoStreamKey(t, rdb, "mystream")
		r := rdb.XRead(ctx, &redis.XReadArgs{
			Streams: []string{"nostream", "mystream", "0-0", "0-0"},
			Count:   1,
		}).Val()
		require.Len(t, r, 1)
		require.Equal(t, "mystream", r[0].Stream)
		require.Len(t, r[0].Messages, 1)
		require.Subset(t, r[0].Messages[0].Values, map[string]interface{}{"item": "0"})
	})

	t.Run("Blocking XREAD waiting new data", func(t *testing.T) {
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{Stream: "s2", Values: []string{"old", "abcd1234"}}).Err())
		c := srv.NewClient()
		defer func() { require.NoError(t, c.Close()) }()
		ch := make(chan []redis.XStream)
		go func() {
			ch <- c.XRead(ctx, &redis.XReadArgs{Streams: []string{"s1", "s2", "s3", "$", "$", "$"}, Block: 20 * time.Second}).Val()
		}()
		require.Eventually(t, func() bool {
			cnt, _ := strconv.Atoi(util.FindInfoEntry(rdb, "blocked_clients"))
			return cnt > 0
		}, 5*time.Second, 100*time.Millisecond)
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{Stream: "s2", Values: []string{"new", "abcd1234"}}).Err())
		r := <-ch
		require.Len(t, r, 1)
		require.Equal(t, "s2", r[0].Stream)
		require.Len(t, r[0].Messages, 1)
		require.Subset(t, r[0].Messages[0].Values, map[string]interface{}{"new": "abcd1234"})
	})

	t.Run("Blocking XREAD waiting old data", func(t *testing.T) {
		c := srv.NewClient()
		defer func() { require.NoError(t, c.Close()) }()
		ch := make(chan []redis.XStream)
		go func() {
			ch <- c.XRead(ctx, &redis.XReadArgs{Streams: []string{"s1", "s2", "s3", "$", "0-0", "$"}, Block: 20 * time.Second}).Val()
		}()
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{Stream: "s2", Values: []string{"foo", "abcd1234"}}).Err())
		r := <-ch
		require.Len(t, r, 1)
		require.Equal(t, "s2", r[0].Stream)
		require.GreaterOrEqual(t, len(r[0].Messages), 2)
		require.Subset(t, r[0].Messages[0].Values, map[string]interface{}{"old": "abcd1234"})
	})

	t.Run("Blocking XREAD will not reply with an empty array", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "s1").Err())
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{Stream: "s1", ID: "666", Values: []string{"f", "v"}}).Err())
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{Stream: "s1", ID: "667", Values: []string{"f2", "v2"}}).Err())
		require.NoError(t, rdb.XDel(ctx, "s1", "667").Err())
		c := srv.NewTCPClient()
		defer func() { require.NoError(t, c.Close()) }()
		require.NoError(t, c.WriteArgs("XREAD", "BLOCK", "10", "STREAMS", "s1", "666"))
		time.Sleep(20 * time.Millisecond)
		c.MustRead(t, "$-1") // before the fix, client didn't even block, but was served synchronously with {s1 {}}
	})

	t.Run("Blocking XREAD for stream that ran dry (redis issue #5299)", func(t *testing.T) {
		// add an entry then delete it, now stream's last_id is 666.
		require.NoError(t, rdb.Del(ctx, "mystream").Err())
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", ID: "666", Values: []string{"key", "value"}}).Err())
		require.NoError(t, rdb.XDel(ctx, "mystream", "666").Err())
		// pass an ID smaller than stream's last_id, released on timeout
		c := srv.NewClient()
		defer func() { require.NoError(t, c.Close()) }()
		require.Empty(t, c.XRead(ctx, &redis.XReadArgs{Streams: []string{"mystream", "665"}, Block: 10 * time.Millisecond}).Val())
		// throw an error if the ID equal or smaller than the last_id
		util.ErrorRegexp(t, rdb.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", ID: "665", Values: []string{"key", "value"}}).Err(), "ERR.*equal.*smaller.*")
		util.ErrorRegexp(t, rdb.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", ID: "666", Values: []string{"key", "value"}}).Err(), "ERR.*equal.*smaller.*")
		// entered blocking state and then release because of the new entry
		ch := make(chan []redis.XStream)
		go func() {
			ch <- c.XRead(ctx, &redis.XReadArgs{Streams: []string{"mystream", "665"}}).Val()
		}()
		require.Eventually(t, func() bool {
			cnt, _ := strconv.Atoi(util.FindInfoEntry(rdb, "blocked_clients"))
			return cnt == 1
		}, 5*time.Second, 100*time.Millisecond)
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", ID: "667", Values: []string{"key", "value"}}).Err())
		r := <-ch
		require.Len(t, r, 1)
		require.Equal(t, "mystream", r[0].Stream)
		require.Len(t, r[0].Messages, 1)
		require.Equal(t, "667-0", r[0].Messages[0].ID)
		require.Subset(t, r[0].Messages[0].Values, map[string]interface{}{"key": "value"})
	})

	t.Run("XREAD with same stream name multiple times should work", func(t *testing.T) {
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{Stream: "s2", Values: []string{"old", "abcd1234"}}).Err())
		c := srv.NewClient()
		defer func() { require.NoError(t, c.Close()) }()
		ch := make(chan []redis.XStream)
		go func() {
			ch <- c.XRead(ctx, &redis.XReadArgs{Streams: []string{"s2", "s2", "s2", "$", "$", "$"}, Block: 20 * time.Second}).Val()
		}()
		require.Eventually(t, func() bool {
			cnt, _ := strconv.Atoi(util.FindInfoEntry(rdb, "blocked_clients"))
			return cnt == 1
		}, 5*time.Second, 100*time.Millisecond)
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{Stream: "s2", Values: []string{"new", "abcd1234"}}).Err())
		r := <-ch
		require.Len(t, r, 3)
		require.Equal(t, "s2", r[0].Stream)
		require.Len(t, r[0].Messages, 1)
		require.Subset(t, r[0].Messages[0].Values, map[string]interface{}{"new": "abcd1234"})
	})

	t.Run("XDEL basic test", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "somestream").Err())
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{Stream: "somestream", Values: []string{"foo", "value0"}}).Err())
		id := rdb.XAdd(ctx, &redis.XAddArgs{Stream: "somestream", Values: []string{"foo", "value1"}}).Val()
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{Stream: "somestream", Values: []string{"foo", "value2"}}).Err())
		require.NoError(t, rdb.XDel(ctx, "somestream", id).Err())
		require.EqualValues(t, 2, rdb.XLen(ctx, "somestream").Val())
		items := rdb.XRange(ctx, "somestream", "-", "+").Val()
		require.Len(t, items, 2)
		require.Subset(t, items[0].Values, map[string]interface{}{"foo": "value0"})
		require.Subset(t, items[1].Values, map[string]interface{}{"foo": "value2"})
	})

	// Here the idea is to check the consistency of the stream data structure as we remove all the elements down to zero elements.
	t.Run("XDEL fuzz test", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "somestream").Err())
		var ids []string
		// add enough elements to have a few radix tree nodes inside the stream
		cnt := 0
		for {
			ids = append(ids, rdb.XAdd(ctx, &redis.XAddArgs{Stream: "somestream", Values: map[string]interface{}{"item": cnt}}).Val())
			cnt++
			if rdb.XInfoStream(ctx, "somestream").Val().Length > 500 {
				break
			}
		}
		// Now remove all the elements till we reach an empty stream and after every deletion,
		// check that the stream is sane enough to report the right number of elements with XRANGE:
		// this will also force accessing the whole data structure to check sanity.
		require.EqualValues(t, cnt, rdb.XLen(ctx, "somestream").Val())
		// We want to remove elements in random order to really test the implementation in a better way.
		rand.Shuffle(len(ids), func(i, j int) { ids[i], ids[j] = ids[j], ids[i] })
		for _, id := range ids {
			require.EqualValues(t, 1, rdb.XDel(ctx, "somestream", id).Val())
			cnt--
			require.EqualValues(t, cnt, rdb.XLen(ctx, "somestream").Val())
			// The test would be too slow calling XRANGE for every iteration. Do it every 100 removal.
			if cnt%100 == 0 {
				require.Len(t, rdb.XRange(ctx, "somestream", "-", "+").Val(), cnt)
			}
		}
	})

	t.Run("XRANGE fuzzing", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "mystream").Err())
		insertIntoStreamKey(t, rdb, "mystream")
		items := rdb.XRange(ctx, "mystream", "-", "+").Val()
		lowID, highID := items[0].ID, items[len(items)-1].ID
		for i := 0; i < 100; i++ {
			start, end := streamRandomID(lowID, highID), streamRandomID(lowID, highID)
			realRange := rdb.XRange(ctx, "mystream", start, end).Val()
			fakeRange := streamSimulateXRANGE(items, start, end)
			require.EqualValues(t, fakeRange, realRange, fmt.Sprintf("start=%s, end=%s", start, end))
		}
	})

	t.Run("XREVRANGE regression test for (redis issue #5006)", func(t *testing.T) {
		// add non compressed entries
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{Stream: "teststream", ID: "1234567891230", Values: []string{"key1", "value1"}}).Err())
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{Stream: "teststream", ID: "1234567891240", Values: []string{"key2", "value2"}}).Err())
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{Stream: "teststream", ID: "1234567891250", Values: []string{"key3", "value3"}}).Err())
		// add SAMEFIELD compressed entries
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{Stream: "teststream2", ID: "1234567891230", Values: []string{"key1", "value1"}}).Err())
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{Stream: "teststream2", ID: "1234567891240", Values: []string{"key1", "value2"}}).Err())
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{Stream: "teststream2", ID: "1234567891250", Values: []string{"key1", "value3"}}).Err())
		items := rdb.XRevRange(ctx, "teststream", "1234567891245", "-").Val()
		require.Len(t, items, 2)
		require.EqualValues(t, redis.XMessage{ID: "1234567891240-0", Values: map[string]interface{}{"key2": "value2"}}, items[0])
		require.EqualValues(t, redis.XMessage{ID: "1234567891230-0", Values: map[string]interface{}{"key1": "value1"}}, items[1])
		items = rdb.XRevRange(ctx, "teststream2", "1234567891245", "-").Val()
		require.Len(t, items, 2)
		require.EqualValues(t, redis.XMessage{ID: "1234567891240-0", Values: map[string]interface{}{"key1": "value2"}}, items[0])
		require.EqualValues(t, redis.XMessage{ID: "1234567891230-0", Values: map[string]interface{}{"key1": "value1"}}, items[1])
	})

	t.Run("XREAD streamID edge (no-blocking)", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "x").Err())
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{Stream: "x", ID: "1-1", Values: []string{"f", "v"}}).Err())
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{Stream: "x", ID: "1-18446744073709551615", Values: []string{"f", "v"}}).Err())
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{Stream: "x", ID: "2-1", Values: []string{"f", "v"}}).Err())
		r := rdb.XRead(ctx, &redis.XReadArgs{Streams: []string{"x", "1-18446744073709551615"}}).Val()
		require.Len(t, r, 1)
		require.Equal(t, "x", r[0].Stream)
		require.Len(t, r[0].Messages, 1)
		require.Equal(t, "2-1", r[0].Messages[0].ID)
		require.Equal(t, map[string]interface{}{"f": "v"}, r[0].Messages[0].Values)
	})

	t.Run("XREAD streamID edge (blocking)", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "x").Err())
		c := srv.NewClient()
		defer func() { require.NoError(t, c.Close()) }()
		ch := make(chan []redis.XStream)
		go func() {
			ch <- c.XRead(ctx, &redis.XReadArgs{Streams: []string{"x", "1-18446744073709551615"}}).Val()
		}()
		require.Eventually(t, func() bool {
			cnt, _ := strconv.Atoi(util.FindInfoEntry(rdb, "blocked_clients"))
			return cnt == 1
		}, 5*time.Second, 100*time.Millisecond)
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{Stream: "x", ID: "1-1", Values: []string{"f", "v"}}).Err())
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{Stream: "x", ID: "1-18446744073709551615", Values: []string{"f", "v"}}).Err())
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{Stream: "x", ID: "2-1", Values: []string{"f", "v"}}).Err())
		r := <-ch
		require.Len(t, r, 1)
		require.Equal(t, "x", r[0].Stream)
		require.Len(t, r[0].Messages, 1)
		require.Equal(t, "2-1", r[0].Messages[0].ID)
		require.Equal(t, map[string]interface{}{"f": "v"}, r[0].Messages[0].Values)
	})

	t.Run("XADD streamID edge", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "x").Err())
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{Stream: "x", ID: "2577343934890-18446744073709551615", Values: []string{"f", "v"}}).Err()) // we need the timestamp to be in the future
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{Stream: "x", Values: []string{"f2", "v2"}}).Err())
		items := rdb.XRange(ctx, "x", "-", "+").Val()
		require.Len(t, items, 2)
		require.EqualValues(t, redis.XMessage{ID: "2577343934890-18446744073709551615", Values: map[string]interface{}{"f": "v"}}, items[0])
		require.EqualValues(t, redis.XMessage{ID: "2577343934891-0", Values: map[string]interface{}{"f2": "v2"}}, items[1])
	})

	t.Run("XTRIM with MAXLEN option basic test", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "mystream").Err())
		for i := 0; i < 1000; i++ {
			if rand.Float64() < 0.9 {
				require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", Values: map[string]interface{}{"xitem": i}}).Err())
			} else {
				require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", Values: map[string]interface{}{"yitem": i}}).Err())
			}
		}
		require.NoError(t, rdb.XTrimMaxLen(ctx, "mystream", 666).Err())
		require.EqualValues(t, 666, rdb.XLen(ctx, "mystream").Val())
		require.NoError(t, rdb.XTrimMaxLen(ctx, "mystream", 555).Err())
		require.EqualValues(t, 555, rdb.XLen(ctx, "mystream").Val())
	})

	t.Run("XADD with LIMIT consecutive calls", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "mystream").Err())
		for i := 0; i < 100; i++ {
			require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", Values: map[string]interface{}{"xitem": "v"}}).Err())
		}
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", MaxLen: 55, Values: map[string]interface{}{"xitem": "v"}}).Err())
		require.EqualValues(t, 55, rdb.XLen(ctx, "mystream").Val())
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", MaxLen: 55, Values: map[string]interface{}{"xitem": "v"}}).Err())
		require.EqualValues(t, 55, rdb.XLen(ctx, "mystream").Val())
	})

	t.Run("XLEN with optional parameters specifying the entry ID to start counting from and direction", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "x").Err())

		for i := 5; i <= 15; i++ {
			require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{
				Stream: "x",
				ID:     fmt.Sprintf("%d-0", i),
				Values: []string{"data", fmt.Sprintf("value-%d", i)},
			}).Err())
		}

		r := rdb.Do(ctx, "XLEN", "x", "non-id")
		require.ErrorContains(t, r.Err(), "Invalid stream ID")

		r = rdb.Do(ctx, "XLEN", "x", "15-0")
		val, err := r.Int()
		require.NoError(t, err)
		require.Equal(t, 0, val)

		r = rdb.Do(ctx, "XLEN", "x", "15-0", "+")
		val, err = r.Int()
		require.NoError(t, err)
		require.Equal(t, 0, val)

		r = rdb.Do(ctx, "XLEN", "x", "15-0", "-")
		val, err = r.Int()
		require.NoError(t, err)
		require.Equal(t, 10, val)

		r = rdb.Do(ctx, "XLEN", "x", "50-0")
		val, err = r.Int()
		require.NoError(t, err)
		require.Equal(t, 0, val)

		r = rdb.Do(ctx, "XLEN", "x", "50-0", "+")
		val, err = r.Int()
		require.NoError(t, err)
		require.Equal(t, 0, val)

		r = rdb.Do(ctx, "XLEN", "x", "50-0", "-")
		val, err = r.Int()
		require.NoError(t, err)
		require.Equal(t, 11, val)

		r = rdb.Do(ctx, "XLEN", "x", "5-0")
		val, err = r.Int()
		require.NoError(t, err)
		require.Equal(t, 10, val)

		r = rdb.Do(ctx, "XLEN", "x", "5-0", "+")
		val, err = r.Int()
		require.NoError(t, err)
		require.Equal(t, 10, val)

		r = rdb.Do(ctx, "XLEN", "x", "5-0", "-")
		val, err = r.Int()
		require.NoError(t, err)
		require.Equal(t, 0, val)

		r = rdb.Do(ctx, "XLEN", "x", "3-0")
		val, err = r.Int()
		require.NoError(t, err)
		require.Equal(t, 11, val)

		r = rdb.Do(ctx, "XLEN", "x", "3-0", "+")
		val, err = r.Int()
		require.NoError(t, err)
		require.Equal(t, 11, val)

		r = rdb.Do(ctx, "XLEN", "x", "3-0", "-")
		val, err = r.Int()
		require.NoError(t, err)
		require.Equal(t, 0, val)

		r = rdb.Do(ctx, "XLEN", "x", "8-0")
		val, err = r.Int()
		require.NoError(t, err)
		require.Equal(t, 7, val)

		r = rdb.Do(ctx, "XLEN", "x", "8-0", "+")
		val, err = r.Int()
		require.NoError(t, err)
		require.Equal(t, 7, val)

		r = rdb.Do(ctx, "XLEN", "x", "8-0", "-")
		val, err = r.Int()
		require.NoError(t, err)
		require.Equal(t, 3, val)

		require.NoError(t, rdb.XDel(ctx, "x", "8-0").Err())

		r = rdb.Do(ctx, "XLEN", "x", "8-0")
		val, err = r.Int()
		require.NoError(t, err)
		require.Equal(t, 7, val)

		r = rdb.Do(ctx, "XLEN", "x", "8-0", "+")
		val, err = r.Int()
		require.NoError(t, err)
		require.Equal(t, 7, val)

		r = rdb.Do(ctx, "XLEN", "x", "8-0", "-")
		val, err = r.Int()
		require.NoError(t, err)
		require.Equal(t, 3, val)
	})
}

// streamSimulateXRANGE simulates Redis XRANGE implementation in Golang.
func streamSimulateXRANGE(items []redis.XMessage, start, end string) []redis.XMessage {
	result := make([]redis.XMessage, 0)
	for _, item := range items {
		if streamCompareID(item.ID, start) >= 0 && streamCompareID(item.ID, end) <= 0 {
			result = append(result, item)
		}
	}
	return result
}

func streamCompareID(a, b string) int {
	aParts, bParts := strings.Split(a, "-"), strings.Split(b, "-")
	aMs, _ := strconv.Atoi(aParts[0])
	aSeq, _ := strconv.Atoi(aParts[1])
	bMs, _ := strconv.Atoi(bParts[0])
	bSeq, _ := strconv.Atoi(bParts[1])
	if aMs > bMs {
		return 1
	}
	if aMs < bMs {
		return -1
	}
	if aSeq > bSeq {
		return 1
	}
	if aSeq < bSeq {
		return -1
	}
	return 0
}

// streamRandomID generates a random stream entry ID with the ms part between min and max and
// a low sequence number (0 - 999 range), in order to stress test XRANGE against streamSimulateXRANGE.
func streamRandomID(minID, maxID string) string {
	minParts, maxParts := strings.Split(minID, "-"), strings.Split(maxID, "-")
	minMs, _ := strconv.Atoi(minParts[0])
	maxMs, _ := strconv.Atoi(maxParts[0])
	delta := int64(maxMs - minMs + 1)
	ms, seq := int64(minMs)+util.RandomInt(delta), util.RandomInt(1000)
	return fmt.Sprintf("%d-%d", ms, seq)
}

// streamNextID returns the ID immediately greater than the specified one.
//
// Note that this function does not care to handle 'seq' overflow since it's a 64 bit value.
func streamNextID(t *testing.T, id string) string {
	parts := strings.Split(id, "-")
	require.Len(t, parts, 2)
	ms, seq := parts[0], parts[1]
	seqN, err := strconv.Atoi(seq)
	require.NoError(t, err)
	return fmt.Sprintf("%s-%d", ms, seqN+1)
}

func insertIntoStreamKey(t *testing.T, rdb *redis.Client, key string) {
	ctx := context.Background()
	require.NoError(t, rdb.Do(ctx, "MULTI").Err())
	for i := 0; i < 1000; i++ {
		// From time to time insert a field with a different set
		// of fields in order to stress the stream compression code.
		if rand.Float64() < 0.9 {
			require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{
				Stream: key,
				Values: map[string]interface{}{"item": i},
			}).Err())
		} else {
			require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{
				Stream: key,
				Values: map[string]interface{}{"item": i, "otherfield": "foo"},
			}).Err())
		}
	}
	require.NoError(t, rdb.Do(ctx, "EXEC").Err())
}

func TestStreamOffset(t *testing.T) {
	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()
	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	t.Run("XADD advances the entries-added counter and sets the recorded-first-entry-id", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "x").Err())

		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{
			Stream: "x",
			ID:     "1-0",
			Values: []string{"data", "a"},
		}).Err())
		r := rdb.XInfoStreamFull(ctx, "x", 0).Val()
		require.EqualValues(t, 1, r.EntriesAdded)
		require.Equal(t, "1-0", r.RecordedFirstEntryID)

		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{
			Stream: "x",
			ID:     "2-0",
			Values: []string{"data", "a"},
		}).Err())
		r = rdb.XInfoStreamFull(ctx, "x", 0).Val()
		require.EqualValues(t, 2, r.EntriesAdded)
		require.Equal(t, "1-0", r.RecordedFirstEntryID)
	})

	t.Run("XDEL/TRIM are reflected by recorded first entry", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "x").Err())

		for i := 0; i < 5; i++ {
			require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{
				Stream: "x",
				ID:     fmt.Sprintf("%d-0", i+1),
				Values: []string{"data", "a"},
			}).Err())
		}

		r := rdb.XInfoStreamFull(ctx, "x", 0).Val()
		require.EqualValues(t, 5, r.EntriesAdded)
		require.Equal(t, "1-0", r.RecordedFirstEntryID)

		require.NoError(t, rdb.XDel(ctx, "x", "2-0").Err())
		r = rdb.XInfoStreamFull(ctx, "x", 0).Val()
		require.Equal(t, "1-0", r.RecordedFirstEntryID)

		require.NoError(t, rdb.XDel(ctx, "x", "1-0").Err())
		r = rdb.XInfoStreamFull(ctx, "x", 0).Val()
		require.Equal(t, "3-0", r.RecordedFirstEntryID)

		require.NoError(t, rdb.XTrimMaxLen(ctx, "x", 2).Err())
		r = rdb.XInfoStreamFull(ctx, "x", 0).Val()
		require.Equal(t, "4-0", r.RecordedFirstEntryID)
	})

	t.Run("Maximum XDEL ID behaves correctly", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "x").Err())

		for i := 0; i < 3; i++ {
			require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{
				Stream: "x",
				ID:     fmt.Sprintf("%d-0", i+1),
				Values: []string{"data", fmt.Sprintf("%c", 'a'+i)},
			}).Err())
		}

		r := rdb.XInfoStreamFull(ctx, "x", 0).Val()
		require.Equal(t, "0-0", r.MaxDeletedEntryID)

		require.NoError(t, rdb.XDel(ctx, "x", "2-0").Err())
		r = rdb.XInfoStreamFull(ctx, "x", 0).Val()
		require.Equal(t, "2-0", r.MaxDeletedEntryID)

		require.NoError(t, rdb.XDel(ctx, "x", "1-0").Err())
		r = rdb.XInfoStreamFull(ctx, "x", 0).Val()
		require.Equal(t, "2-0", r.MaxDeletedEntryID)
	})

	t.Run("XADD with custom sequence number and timestamp set by the server", func(t *testing.T) {
		streamName := "test-stream-1"
		require.NoError(t, rdb.Del(ctx, streamName).Err())

		now := time.Now().UTC().UnixMilli()
		providedSeqNum := 123456789
		r, err := rdb.XAdd(ctx, &redis.XAddArgs{
			Stream: streamName,
			ID:     fmt.Sprintf("*-%d", providedSeqNum),
			Values: []string{"data", fmt.Sprintf("value-%d", providedSeqNum)},
		}).Result()

		require.NoError(t, err)

		ts, seqNum := parseStreamEntryID(r)
		require.GreaterOrEqual(t, ts, now)
		require.Less(t, ts, now+5_000)
		require.EqualValues(t, providedSeqNum, seqNum)
	})

	t.Run("XGROUP CREATE with different kinds of commands and XGROUP DESTROY", func(t *testing.T) {
		streamName := "test-stream-a"
		groupName := "test-group-a"
		require.NoError(t, rdb.Del(ctx, streamName).Err())
		// No such stream (No such key)
		require.Error(t, rdb.Do(ctx, "XGROUP", "CREATE", streamName, groupName, "$").Err())
		require.Error(t, rdb.Do(ctx, "XGROUP", "CREATE", streamName, groupName, "$", "ENTRIESREAD", "10").Err())
		require.Error(t, rdb.Do(ctx, "XGROUP", "CREATE", streamName, groupName, "$", "ENTRIESREAD").Err())
		require.Error(t, rdb.Do(ctx, "XGROUP", "CREATE", streamName, groupName, "$", "MKSTREAM", "ENTRIESREAD").Err())
		require.NoError(t, rdb.Do(ctx, "XGROUP", "CREATE", streamName, groupName, "$", "MKSTREAM").Err())
		require.NoError(t, rdb.XInfoStream(ctx, streamName).Err())
		require.Error(t, rdb.Do(ctx, "XGROUP", "CREATE", streamName, groupName, "$").Err())
		// Invalid syntax
		groupName = "test-group-b"
		require.Error(t, rdb.Do(ctx, "XGROUP", "CREAT", streamName, groupName, "$").Err())
		require.Error(t, rdb.Do(ctx, "XGROUP", "CREATE", streamName, groupName, "$", "ENTRIEREAD", "10").Err())
		require.Error(t, rdb.Do(ctx, "XGROUP", "CREATE", streamName, groupName, "$", "ENTRIESREAD", "-10").Err())
		require.Error(t, rdb.Do(ctx, "XGROUP", "CREATE", streamName, "1test-group-c", "$").Err())

		require.NoError(t, rdb.Del(ctx, "myStream").Err())
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{Stream: "myStream", Values: []string{"iTeM", "1", "vAluE", "a"}}).Err())
		require.NoError(t, rdb.XGroupCreate(ctx, "myStream", "myGroup", "$").Err())
		// duplicate create group
		require.EqualError(t, rdb.XGroupCreate(ctx, "myStream", "myGroup", "$").Err(),
			"BUSYGROUP consumer group name 'myGroup' already exists")
		result, err := rdb.XGroupDestroy(ctx, "myStream", "myGroup").Result()
		require.NoError(t, err)
		require.Equal(t, int64(1), result)
		result, err = rdb.XGroupDestroy(ctx, "myStream", "myGroup").Result()
		require.NoError(t, err)
		require.Equal(t, int64(0), result)
	})

	t.Run("XGROUP CREATECONSUMER with different kinds of commands", func(t *testing.T) {
		streamName := "test-stream"
		groupName := "test-group"
		consumerName := "test-consumer"
		require.NoError(t, rdb.Del(ctx, streamName).Err())
		//No such stream
		require.Error(t, rdb.XGroupCreateConsumer(ctx, streamName, groupName, consumerName).Err())
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{
			Stream: streamName,
			ID:     "1-0",
			Values: []string{"data", "a"},
		}).Err())
		//no such group
		require.Error(t, rdb.XGroupCreateConsumer(ctx, streamName, groupName, consumerName).Err())
		require.NoError(t, rdb.XGroupCreate(ctx, streamName, groupName, "$").Err())

		r := rdb.XGroupCreateConsumer(ctx, streamName, groupName, consumerName).Val()
		require.Equal(t, int64(1), r)
		r = rdb.XGroupCreateConsumer(ctx, streamName, groupName, consumerName).Val()
		require.Equal(t, int64(0), r)
	})

	t.Run("XGROUP DELCONSUMER with different kinds of commands", func(t *testing.T) {
		streamName := "test-stream"
		groupName := "test-group"
		consumerName := "test-consumer"
		require.NoError(t, rdb.Del(ctx, streamName).Err())
		//No such stream
		require.Error(t, rdb.XGroupCreateConsumer(ctx, streamName, groupName, consumerName).Err())
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{
			Stream: streamName,
			ID:     "1-0",
			Values: []string{"data", "a"},
		}).Err())

		//no such group
		expectedError := fmt.Sprintf("NOGROUP No such consumer group %s for key name %s", groupName, streamName)
		require.EqualError(t, rdb.XGroupCreateConsumer(ctx, streamName, groupName, consumerName).Err(), expectedError)
		require.EqualError(t, rdb.XGroupDelConsumer(ctx, streamName, groupName, consumerName).Err(), expectedError)

		require.NoError(t, rdb.XGroupCreate(ctx, streamName, groupName, "$").Err())
		require.NoError(t, rdb.XGroupCreateConsumer(ctx, streamName, groupName, consumerName).Err())

		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{
			Stream: streamName,
			ID:     "*",
			Values: []string{"data1", "a1"},
		}).Err())
		require.NoError(t, rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    groupName,
			Consumer: consumerName,
			Streams:  []string{streamName, ">"},
			Count:    1,
			NoAck:    false,
		}).Err())
		ri, erri := rdb.XInfoGroups(ctx, streamName).Result()
		require.NoError(t, erri)
		require.Equal(t, int64(1), ri[0].Consumers)
		require.Equal(t, int64(1), ri[0].Pending)

		r, err := rdb.XGroupDelConsumer(ctx, streamName, groupName, consumerName).Result()
		require.NoError(t, err)
		require.Equal(t, int64(1), r)
		ri, erri = rdb.XInfoGroups(ctx, streamName).Result()
		require.NoError(t, erri)
		require.Equal(t, int64(0), ri[0].Consumers)
		require.Equal(t, int64(0), ri[0].Pending)

		require.NoError(t, rdb.XGroupCreateConsumer(ctx, streamName, groupName, consumerName).Err())
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{
			Stream: streamName,
			ID:     "*",
			Values: []string{"data2", "a2"},
		}).Err())
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{
			Stream: streamName,
			ID:     "*",
			Values: []string{"data3", "a3"},
		}).Err())
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{
			Stream: streamName,
			ID:     "*",
			Values: []string{"data4", "a4"},
		}).Err())
		require.NoError(t, rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    groupName,
			Consumer: consumerName,
			Streams:  []string{streamName, ">"},
			Count:    3,
			NoAck:    false,
		}).Err())
		ri, erri = rdb.XInfoGroups(ctx, streamName).Result()
		require.NoError(t, erri)
		require.Equal(t, int64(1), ri[0].Consumers)
		require.Equal(t, int64(3), ri[0].Pending)
		r, err = rdb.XGroupDelConsumer(ctx, streamName, groupName, consumerName).Result()
		require.NoError(t, err)
		require.Equal(t, int64(3), r)
		ri, erri = rdb.XInfoGroups(ctx, streamName).Result()
		require.NoError(t, erri)
		require.Equal(t, int64(0), ri[0].Consumers)
		require.Equal(t, int64(0), ri[0].Pending)
	})

	t.Run("XREADGROUP with empty streams returns empty arrays", func(t *testing.T) {
		streamName := "test-stream-empty1"
		streamName2 := "test-stream-empty2"
		groupName := "test-group-empty"
		consumerName := "test-consumer-empty"

		require.NoError(t, rdb.XGroupCreateMkStream(ctx, streamName, groupName, "0").Err())
		require.NoError(t, rdb.XGroupCreateMkStream(ctx, streamName2, groupName, "0").Err())

		res, err := rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    groupName,
			Consumer: consumerName,
			Streams:  []string{streamName, streamName2, "0", "0"},
		}).Result()
		require.NoError(t, err)

		expectedRes := []redis.XStream{
			{
				Stream:   streamName,
				Messages: []redis.XMessage{},
			},
			{
				Stream:   streamName2,
				Messages: []redis.XMessage{},
			},
		}

		require.Equal(t, expectedRes, res)
	})

	t.Run("XGROUP SETID with different kinds of commands", func(t *testing.T) {
		streamName := "test-stream"
		groupName := "test-group"
		require.NoError(t, rdb.Del(ctx, streamName).Err())
		//No such stream
		require.Error(t, rdb.XGroupSetID(ctx, streamName, groupName, "$").Err())
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{
			Stream: streamName,
			ID:     "1-0",
			Values: []string{"data", "a"},
		}).Err())
		//No such group
		require.EqualError(t, rdb.XGroupSetID(ctx, streamName, groupName, "$").Err(),
			fmt.Sprintf("NOGROUP No such consumer group %s for key name %s", groupName, streamName))
		require.NoError(t, rdb.XGroupCreate(ctx, streamName, groupName, "$").Err())

		require.NoError(t, rdb.XGroupSetID(ctx, streamName, groupName, "0-0").Err())
		require.Error(t, rdb.Do(ctx, "xgroup", "setid", streamName, groupName, "$", "entries", "100").Err())
		require.Error(t, rdb.Do(ctx, "xgroup", "setid", streamName, groupName, "$", "entriesread", "-100").Err())
		require.NoError(t, rdb.Do(ctx, "xgroup", "setid", streamName, groupName, "$", "entriesread", "100").Err())
	})

	t.Run("XINFO GROUPS and XINFO CONSUMERS", func(t *testing.T) {
		streamName := "test-stream"
		group1 := "t1"
		group2 := "t2"
		consumer1 := "c1"
		consumer2 := "c2"
		consumer3 := "c3"
		require.NoError(t, rdb.Del(ctx, streamName).Err())
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{
			Stream: streamName,
			ID:     "1-0",
			Values: []string{"data", "a"},
		}).Err())
		require.NoError(t, rdb.XGroupCreate(ctx, streamName, group1, "$").Err())
		r := rdb.XInfoGroups(ctx, streamName).Val()
		require.Equal(t, group1, r[0].Name)
		require.Equal(t, int64(0), r[0].Consumers)
		require.Equal(t, int64(0), r[0].Pending)
		require.Equal(t, "1-0", r[0].LastDeliveredID)
		require.Equal(t, int64(0), r[0].EntriesRead)
		require.Equal(t, int64(0), r[0].Lag)

		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{
			Stream: streamName,
			ID:     "2-0",
			Values: []string{"data1", "b"},
		}).Err())
		require.NoError(t, rdb.XGroupCreate(ctx, streamName, group2, "$").Err())
		r = rdb.XInfoGroups(ctx, streamName).Val()
		require.Equal(t, group2, r[1].Name)
		require.Equal(t, "2-0", r[1].LastDeliveredID)

		require.NoError(t, rdb.XGroupCreateConsumer(ctx, streamName, group1, consumer1).Err())
		require.NoError(t, rdb.XGroupCreateConsumer(ctx, streamName, group1, consumer2).Err())
		require.NoError(t, rdb.XGroupCreateConsumer(ctx, streamName, group2, consumer3).Err())
		r = rdb.XInfoGroups(ctx, streamName).Val()
		require.Equal(t, int64(2), r[0].Consumers)
		require.Equal(t, int64(1), r[1].Consumers)

		r1 := rdb.XInfoConsumers(ctx, streamName, group1).Val()
		require.Equal(t, consumer1, r1[0].Name)
		require.Equal(t, consumer2, r1[1].Name)
		r1 = rdb.XInfoConsumers(ctx, streamName, group2).Val()
		require.Equal(t, consumer3, r1[0].Name)
	})

	t.Run("XINFO after delete pending message and related consumer, for issue #2350", func(t *testing.T) {
		streamName := "test-stream-2350"
		groupName := "test-group-2350"
		consumerName := "test-consumer-2350"
		require.NoError(t, rdb.XGroupCreateMkStream(ctx, streamName, groupName, "$").Err())
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{
			Stream: streamName,
			ID:     "*",
			Values: []string{"testing", "overflow"},
		}).Err())
		readRsp := rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    groupName,
			Consumer: consumerName,
			Streams:  []string{streamName, ">"},
			Count:    1,
			NoAck:    false,
		})
		require.NoError(t, readRsp.Err())
		require.Len(t, readRsp.Val(), 1)
		streamRsp := readRsp.Val()[0]
		require.Len(t, streamRsp.Messages, 1)
		msgID := streamRsp.Messages[0]
		require.NoError(t, rdb.XAck(ctx, streamName, groupName, msgID.ID).Err())
		require.NoError(t, rdb.XGroupDelConsumer(ctx, streamName, groupName, consumerName).Err())
		infoRsp := rdb.XInfoGroups(ctx, streamName)
		require.NoError(t, infoRsp.Err())
		infoGroups := infoRsp.Val()
		require.Len(t, infoGroups, 1)
		infoGroup := infoGroups[0]
		require.Equal(t, groupName, infoGroup.Name)
		require.Equal(t, int64(0), infoGroup.Consumers)
		require.Equal(t, int64(0), infoGroup.Pending)
		require.Equal(t, msgID.ID, infoGroup.LastDeliveredID)
	})

	t.Run("XINFO Test idle time and pending messages, for issue #2478", func(t *testing.T) {
		streamName := "test-stream-2478"
		groupName := "test-group-2478"
		consumerName := "test-consumer-2478"

		rdb.Del(ctx, streamName)
		rdb.XGroupDestroy(ctx, streamName, groupName)

		for i := 1; i <= 5; i++ {
			require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{
				Stream: streamName,
				ID:     fmt.Sprintf("%d-0", i),
				Values: map[string]interface{}{"field": fmt.Sprintf("value%d", i)},
			}).Err())
		}

		require.NoError(t, rdb.XGroupCreate(ctx, streamName, groupName, "0").Err())
		r, err := rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    groupName,
			Consumer: consumerName,
			Streams:  []string{streamName, ">"},
			Count:    5,
		}).Result()
		require.NoError(t, err)
		require.Len(t, r[0].Messages, 5)

		time.Sleep(2 * time.Second)

		consumers, err := rdb.XInfoConsumers(ctx, streamName, groupName).Result()
		require.NoError(t, err)

		var consumerInfo redis.XInfoConsumer
		for _, c := range consumers {
			if c.Name == consumerName {
				consumerInfo = c
				break
			}
		}

		require.True(t, consumerInfo.Idle >= 2000)
		require.Equal(t, int64(5), consumerInfo.Pending)

		ackIDs := make([]string, 5)
		for i := 1; i <= 5; i++ {
			ackIDs[i-1] = fmt.Sprintf("%d-0", i)
		}
		require.NoError(t, rdb.XAck(ctx, streamName, groupName, ackIDs...).Err())

		consumers, err = rdb.XInfoConsumers(ctx, streamName, groupName).Result()
		require.NoError(t, err)

		for _, c := range consumers {
			if c.Name == consumerName {
				consumerInfo = c
				break
			}
		}

		require.Equal(t, int64(0), consumerInfo.Pending)
	})

	t.Run("XINFO Test consumer removal and inactive time, for issue #2478", func(t *testing.T) {
		streamName := "stream-test-2478"
		groupName := "group-test-2478"
		consumerName := "consumer-test-2478"

		rdb.Del(ctx, streamName)
		rdb.XGroupDestroy(ctx, streamName, groupName)

		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{
			Stream: streamName,
			ID:     "*",
			Values: map[string]interface{}{"field": "value"},
		}).Err())

		require.NoError(t, rdb.XGroupCreate(ctx, streamName, groupName, "0").Err())
		_, err := rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    groupName,
			Consumer: consumerName,
			Streams:  []string{streamName, ">"},
			Count:    1,
		}).Result()
		require.NoError(t, err)

		time.Sleep(500 * time.Millisecond)

		consumers, err := rdb.XInfoConsumers(ctx, streamName, groupName).Result()
		require.NoError(t, err)

		var consumerInfo redis.XInfoConsumer
		for _, c := range consumers {
			if c.Name == consumerName {
				consumerInfo = c
				break
			}
		}

		require.Equal(t, consumerName, consumerInfo.Name)
		require.NoError(t, rdb.XGroupDelConsumer(ctx, streamName, groupName, consumerName).Err())

		consumers, err = rdb.XInfoConsumers(ctx, streamName, groupName).Result()
		require.NoError(t, err)

		for _, c := range consumers {
			require.NotEqual(t, consumerName, c.Name)
		}
	})

	t.Run("XREAD After XGroupCreate and XGroupCreateConsumer, for issue #2109", func(t *testing.T) {
		streamName := "test-stream"
		group := "group"
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{
			Stream: streamName,
			ID:     "*",
			Values: []string{"data1", "b"},
		}).Err())
		require.NoError(t, rdb.XGroupCreate(ctx, streamName, group, "0").Err())
		require.NoError(t, rdb.XGroupCreateConsumer(ctx, streamName, group, "consumer").Err())
		require.NoError(t, rdb.XRead(ctx, &redis.XReadArgs{
			Streams: []string{streamName, "0"},
		}).Err())
	})

	t.Run("XREADGROUP with different kinds of commands", func(t *testing.T) {
		streamName := "mystream"
		groupName := "mygroup"
		require.NoError(t, rdb.Del(ctx, streamName).Err())
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{
			Stream: streamName,
			ID:     "1-0",
			Values: []string{"field1", "data1"},
		}).Err())
		require.NoError(t, rdb.XGroupCreate(ctx, streamName, groupName, "0").Err())
		consumerName := "myconsumer"
		r, err := rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    groupName,
			Consumer: consumerName,
			Streams:  []string{streamName, ">"},
			Count:    1,
			NoAck:    false,
		}).Result()
		require.NoError(t, err)
		require.Equal(t, []redis.XStream{{
			Stream:   streamName,
			Messages: []redis.XMessage{{ID: "1-0", Values: map[string]interface{}{"field1": "data1"}}},
		}}, r)

		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{
			Stream: streamName,
			ID:     "2-0",
			Values: []string{"field2", "data2"},
		}).Err())
		r, err = rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    groupName,
			Consumer: consumerName,
			Streams:  []string{streamName, ">"},
			Count:    1,
			NoAck:    false,
		}).Result()
		require.NoError(t, err)
		require.Equal(t, []redis.XStream{{
			Stream:   streamName,
			Messages: []redis.XMessage{{ID: "2-0", Values: map[string]interface{}{"field2": "data2"}}},
		}}, r)

		r, err = rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    groupName,
			Consumer: consumerName,
			Streams:  []string{streamName, "0"},
			Count:    2,
			NoAck:    false,
		}).Result()
		require.NoError(t, err)
		require.Equal(t, []redis.XStream{{
			Stream: streamName,
			Messages: []redis.XMessage{{ID: "1-0", Values: map[string]interface{}{"field1": "data1"}},
				{ID: "2-0", Values: map[string]interface{}{"field2": "data2"}}},
		}}, r)

		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{
			Stream: streamName,
			ID:     "3-0",
			Values: []string{"field3", "data3"},
		}).Err())
		r, err = rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    groupName,
			Consumer: consumerName,
			Streams:  []string{streamName, ">"},
			Count:    1,
			NoAck:    true,
		}).Result()
		require.NoError(t, err)
		require.Equal(t, []redis.XStream{{
			Stream:   streamName,
			Messages: []redis.XMessage{{ID: "3-0", Values: map[string]interface{}{"field3": "data3"}}},
		}}, r)
		r, err = rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    groupName,
			Consumer: consumerName,
			Streams:  []string{streamName, "0"},
			Count:    2,
			NoAck:    false,
		}).Result()
		require.NoError(t, err)
		require.Equal(t, []redis.XStream{{
			Stream: streamName,
			Messages: []redis.XMessage{{ID: "1-0", Values: map[string]interface{}{"field1": "data1"}},
				{ID: "2-0", Values: map[string]interface{}{"field2": "data2"}}},
		}}, r)

		c := srv.NewClient()
		defer func() { require.NoError(t, c.Close()) }()
		ch := make(chan []redis.XStream)
		go func() {
			ch <- c.XReadGroup(ctx, &redis.XReadGroupArgs{
				Group:    groupName,
				Consumer: consumerName,
				Streams:  []string{streamName, ">"},
				Count:    2,
				Block:    10 * time.Second,
				NoAck:    false,
			}).Val()
		}()
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{
			Stream: streamName,
			ID:     "4-0",
			Values: []string{"field4", "data4"},
		}).Err())
		r = <-ch
		require.Equal(t, []redis.XStream{{
			Stream:   streamName,
			Messages: []redis.XMessage{{ID: "4-0", Values: map[string]interface{}{"field4": "data4"}}},
		}}, r)

		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{
			Stream: streamName,
			ID:     "5-0",
			Values: []string{"field5", "data5"},
		}).Err())
		require.NoError(t, rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    groupName,
			Consumer: consumerName,
			Streams:  []string{streamName, ">"},
			Count:    1,
			NoAck:    false,
		}).Err())
		require.NoError(t, rdb.XDel(ctx, streamName, "5-0").Err())
		r, err = rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    groupName,
			Consumer: consumerName,
			Streams:  []string{streamName, "5"},
			Count:    1,
			NoAck:    false,
		}).Result()
		require.NoError(t, err)
		require.Equal(t, []redis.XStream{{
			Stream:   streamName,
			Messages: []redis.XMessage{{ID: "5-0", Values: map[string]interface{}(nil)}},
		}}, r)
	})

	t.Run("Check xreadgroup fetches the newest data after create consumer in the command", func(t *testing.T) {
		streamName := "mystream"
		groupName := "mygroup"
		require.NoError(t, rdb.Del(ctx, streamName).Err())
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{
			Stream: streamName,
			ID:     "1-0",
			Values: []string{"field1", "data1"},
		}).Err())
		require.NoError(t, rdb.XGroupCreate(ctx, streamName, groupName, "0").Err())
		consumerName := "myconsumer"
		err := rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    groupName,
			Consumer: consumerName,
			Streams:  []string{streamName, ">"},
			Count:    1,
			NoAck:    false,
		}).Err()
		require.NoError(t, err)
		ri, erri := rdb.XInfoGroups(ctx, streamName).Result()
		require.NoError(t, erri)
		require.Equal(t, int64(1), ri[0].Consumers)
	})

	t.Run("XACK with different kinds of commands", func(t *testing.T) {
		streamName := "mystream"
		groupName := "mygroup"
		require.NoError(t, rdb.Del(ctx, streamName).Err())
		r, err := rdb.XAck(ctx, streamName, groupName, "0-0").Result()
		require.NoError(t, err)
		require.Equal(t, int64(0), r)
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{
			Stream: streamName,
			ID:     "1-0",
			Values: []string{"field1", "data1"},
		}).Err())
		require.NoError(t, rdb.XGroupCreate(ctx, streamName, groupName, "0").Err())
		consumerName := "myconsumer"
		err = rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    groupName,
			Consumer: consumerName,
			Streams:  []string{streamName, ">"},
			Count:    1,
			NoAck:    false,
		}).Err()
		require.NoError(t, err)
		r, err = rdb.XAck(ctx, streamName, groupName, "1-0").Result()
		require.NoError(t, err)
		require.Equal(t, int64(1), r)

		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{
			Stream: streamName,
			ID:     "2-0",
			Values: []string{"field1", "data1"},
		}).Err())
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{
			Stream: streamName,
			ID:     "3-0",
			Values: []string{"field1", "data1"},
		}).Err())
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{
			Stream: streamName,
			ID:     "4-0",
			Values: []string{"field1", "data1"},
		}).Err())
		err = rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    groupName,
			Consumer: consumerName,
			Streams:  []string{streamName, ">"},
			Count:    3,
			NoAck:    false,
		}).Err()
		require.NoError(t, err)
		r, err = rdb.XAck(ctx, streamName, groupName, "2-0", "3-0", "4-0").Result()
		require.NoError(t, err)
		require.Equal(t, int64(3), r)
	})

	t.Run("Simple XCLAIM command tests", func(t *testing.T) {
		streamName := "mystream"
		groupName := "mygroup"
		consumerName := "myconsumer"
		consumer1Name := "myconsumer1"
		require.NoError(t, rdb.Del(ctx, streamName).Err())

		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{
			Stream: streamName,
			ID:     "1-0",
			Values: []string{"field1", "data1"},
		}).Err())

		// No such group
		err := rdb.XClaim(ctx, &redis.XClaimArgs{
			Stream:   streamName,
			Group:    groupName,
			Consumer: consumer1Name,
			MinIdle:  0,
			Messages: []string{"1-0"},
		}).Err()
		require.EqualError(t, err, fmt.Sprintf("NOGROUP No such key '%s' or consumer group '%s'", streamName, groupName))

		require.NoError(t, rdb.XGroupCreate(ctx, streamName, groupName, "0").Err())
		r, err := rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    groupName,
			Consumer: consumerName,
			Streams:  []string{streamName, ">"},
			Count:    1,
			NoAck:    false,
		}).Result()
		require.NoError(t, err)
		require.Equal(t, []redis.XStream{{
			Stream:   streamName,
			Messages: []redis.XMessage{{ID: "1-0", Values: map[string]interface{}{"field1": "data1"}}},
		}}, r)

		claimedMessages, err := rdb.XClaim(ctx, &redis.XClaimArgs{
			Stream:   streamName,
			Group:    groupName,
			Consumer: consumer1Name,
			MinIdle:  0,
			Messages: []string{"1-0"},
		}).Result()
		require.NoError(t, err)
		require.Len(t, claimedMessages, 1, "Expected to claim 1 message")
		require.Equal(t, "1-0", claimedMessages[0].ID, "Expected claimed message ID to match")

		time.Sleep(2000 * time.Millisecond)
		minIdleTime := 1000 * time.Millisecond
		claimedMessages, err = rdb.XClaim(ctx, &redis.XClaimArgs{
			Stream:   streamName,
			Group:    groupName,
			Consumer: consumerName,
			MinIdle:  minIdleTime,
			Messages: []string{"1-0"},
		}).Result()
		require.NoError(t, err)
		require.Len(t, claimedMessages, 1, "Expected to claim 1 message if idle time is large enough")
		require.Equal(t, "1-0", claimedMessages[0].ID, "Expected claimed message ID to match")

		minIdleTime = 60000 * time.Millisecond
		claimedMessages, err = rdb.XClaim(ctx, &redis.XClaimArgs{
			Stream:   streamName,
			Group:    groupName,
			Consumer: consumer1Name,
			MinIdle:  minIdleTime,
			Messages: []string{"1-0"},
		}).Result()

		require.NoError(t, err)
		require.Empty(t, claimedMessages, "Expected no messages to be claimed due to insufficient idle time")
	})

	t.Run("XCLAIM with different timing situations and options", func(t *testing.T) {
		streamName := "mystream"
		groupName := "mygroup"
		consumerName := "myconsumer"
		consumer1Name := "myconsumer1"
		require.NoError(t, rdb.Del(ctx, streamName).Err())

		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{
			Stream: streamName,
			ID:     "1-0",
			Values: []string{"field1", "data1"},
		}).Err())
		require.NoError(t, rdb.XGroupCreate(ctx, streamName, groupName, "0").Err())
		r, err := rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    groupName,
			Consumer: consumerName,
			Streams:  []string{streamName, ">"},
			Count:    1,
			NoAck:    false,
		}).Result()
		require.NoError(t, err)
		require.Equal(t, []redis.XStream{{
			Stream:   streamName,
			Messages: []redis.XMessage{{ID: "1-0", Values: map[string]interface{}{"field1": "data1"}}},
		}}, r)

		rawClaimedMessages, err := rdb.Do(ctx, "XCLAIM", streamName, groupName, consumer1Name, "0", "1-0", "IDLE", "5000").Result()
		require.NoError(t, err)
		messages, ok := rawClaimedMessages.([]interface{})
		require.True(t, ok, "Expected the result to be a slice of interface{}")
		firstMsg, ok := messages[0].([]interface{})
		require.True(t, ok, "Expected message details to be a slice of interface{}")
		msgID, ok := firstMsg[0].(string)
		require.True(t, ok, "Expected message ID to be a string")
		require.Equal(t, "1-0", msgID, "Expected claimed message ID to match")

		claimedMessages, err := rdb.XClaim(ctx, &redis.XClaimArgs{
			Stream:   streamName,
			Group:    groupName,
			Consumer: consumerName,
			MinIdle:  2000 * time.Millisecond,
			Messages: []string{"1-0"},
		}).Result()
		require.NoError(t, err)
		require.Len(t, claimedMessages, 1, "Expected to claim 1 message if idle time is large enough")
		require.Equal(t, "1-0", claimedMessages[0].ID, "Expected claimed message ID to match")

		tenSecondsAgo := time.Now().Add(-10 * time.Second).UnixMilli()
		rawClaimedMessages, err = rdb.Do(ctx, "XCLAIM", streamName, groupName, consumer1Name, "0", "1-0", "TIME", tenSecondsAgo).Result()
		require.NoError(t, err)
		messages, ok = rawClaimedMessages.([]interface{})
		require.True(t, ok, "Expected the result to be a slice of interface{}")
		firstMsg, ok = messages[0].([]interface{})
		require.True(t, ok, "Expected message details to be a slice of interface{}")
		msgID, ok = firstMsg[0].(string)
		require.True(t, ok, "Expected message ID to be a string")
		require.Equal(t, "1-0", msgID, "Expected claimed message ID to match")

		claimedMessages, err = rdb.XClaim(ctx, &redis.XClaimArgs{
			Stream:   streamName,
			Group:    groupName,
			Consumer: consumerName,
			MinIdle:  5000 * time.Millisecond,
			Messages: []string{"1-0"},
		}).Result()
		require.NoError(t, err)
		require.Len(t, claimedMessages, 1, "Expected to claim 1 message if idle time is large enough")
		require.Equal(t, "1-0", claimedMessages[0].ID, "Expected claimed message ID to match")
	})

	t.Run("XCLAIM command with different options", func(t *testing.T) {
		streamName := "mystream"
		groupName := "mygroup"
		consumerName := "myconsumer"
		consumer1Name := "myconsumer1"

		require.NoError(t, rdb.Del(ctx, streamName).Err())
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{
			Stream: streamName,
			ID:     "1-0",
			Values: []string{"field1", "data1"},
		}).Err())
		require.NoError(t, rdb.XGroupCreate(ctx, streamName, groupName, "0").Err())

		rawClaimedMessages, err := rdb.Do(ctx, "XCLAIM", streamName, groupName, consumerName, "0", "1-0", "FORCE").Result()
		require.NoError(t, err)
		messages, ok := rawClaimedMessages.([]interface{})
		require.True(t, ok, "Expected the result to be a slice of interface{}")
		firstMsg, ok := messages[0].([]interface{})
		require.True(t, ok, "Expected message details to be a slice of interface{}")
		msgID, ok := firstMsg[0].(string)
		require.True(t, ok, "Expected message ID to be a string")
		require.Equal(t, "1-0", msgID, "Expected claimed message ID to match")

		cmd := rdb.XClaimJustID(ctx, &redis.XClaimArgs{
			Stream:   streamName,
			Group:    groupName,
			Consumer: consumer1Name,
			MinIdle:  0,
			Messages: []string{"1-0"},
		})

		claimedIDs, err := cmd.Result()
		require.NoError(t, err)
		require.Len(t, claimedIDs, 1, "Expected to claim exactly one message ID")
		require.Equal(t, "1-0", claimedIDs[0], "Expected claimed message ID to match")
	})

	t.Run("XAUTOCLAIM can claim PEL items from another consume", func(t *testing.T) {

		streamName := "mystream"
		groupName := "mygroup"
		var id1 string
		require.NoError(t, rdb.Del(ctx, streamName).Err())
		{
			rsp := rdb.XAdd(ctx, &redis.XAddArgs{
				Stream: streamName,
				ID:     "*",
				Values: []string{"a", "1"},
			})
			require.NoError(t, rsp.Err())
			id1 = rsp.Val()
		}
		var id2 string
		{
			rsp := rdb.XAdd(ctx, &redis.XAddArgs{
				Stream: streamName,
				ID:     "*",
				Values: []string{"b", "2"},
			})
			require.NoError(t, rsp.Err())
			id2 = rsp.Val()
		}
		var id3 string
		{
			rsp := rdb.XAdd(ctx, &redis.XAddArgs{
				Stream: streamName,
				ID:     "*",
				Values: []string{"c", "3"},
			})
			require.NoError(t, rsp.Err())
			id3 = rsp.Val()
		}
		var id4 string
		{
			rsp := rdb.XAdd(ctx, &redis.XAddArgs{
				Stream: streamName,
				ID:     "*",
				Values: []string{"d", "4"},
			})
			require.NoError(t, rsp.Err())
			id4 = rsp.Val()
		}

		// No such group
		err := rdb.XAutoClaimJustID(ctx, &redis.XAutoClaimArgs{
			Stream:   streamName,
			Group:    groupName,
			Consumer: "consumer",
			MinIdle:  10 * time.Millisecond,
			Start:    "-",
		}).Err()
		require.EqualError(t, err, fmt.Sprintf("NOGROUP No such key '%s' or consumer group '%s'", streamName, groupName))
		require.NoError(t, rdb.XGroupCreate(ctx, streamName, groupName, "0").Err())

		consumer1 := "consumer1"
		consumer2 := "consumer2"
		{
			rsp := rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
				Group:    groupName,
				Consumer: consumer1,
				Streams:  []string{streamName, ">"},
				Count:    1,
			})
			require.NoError(t, rsp.Err())
			require.Len(t, rsp.Val(), 1)
			require.Len(t, rsp.Val()[0].Messages, 1)
			require.Equal(t, id1, rsp.Val()[0].Messages[0].ID)
			require.Len(t, rsp.Val()[0].Messages[0].Values, 1)
			require.Equal(t, "1", rsp.Val()[0].Messages[0].Values["a"])
		}

		{
			time.Sleep(200 * time.Millisecond)
			rsp := rdb.XAutoClaim(ctx, &redis.XAutoClaimArgs{
				Stream:   streamName,
				Group:    groupName,
				Consumer: consumer2,
				MinIdle:  10 * time.Millisecond,
				Count:    1,
				Start:    "-",
			})
			require.NoError(t, rsp.Err())
			msgs, start := rsp.Val()
			require.Equal(t, "0-0", start)
			require.Len(t, msgs, 1)
			require.Len(t, msgs[0].Values, 1)
			require.Equal(t, "1", msgs[0].Values["a"])
		}

		{
			rsp := rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
				Group:    groupName,
				Consumer: consumer1,
				Streams:  []string{streamName, ">"},
				Count:    3,
			})
			require.NoError(t, rsp.Err())

			time.Sleep(time.Millisecond * 200)
			require.NoError(t, rdb.XDel(ctx, streamName, id2).Err())
		}

		{
			cmd := rdb.Do(ctx, "XAUTOCLAIM", streamName, groupName, consumer2, 10, "-", "COUNT", 3)
			require.NoError(t, cmd.Err())
			require.Equal(t, []interface{}{
				id4,
				[]interface{}{
					[]interface{}{
						id1,
						[]interface{}{"a", "1"},
					},
					[]interface{}{
						id3,
						[]interface{}{"c", "3"},
					},
				},
				[]interface{}{
					id2,
				},
			}, cmd.Val())
		}

		{
			time.Sleep(time.Millisecond * 200)
			require.NoError(t, rdb.XDel(ctx, streamName, id4).Err())
			rsp := rdb.XAutoClaimJustID(ctx, &redis.XAutoClaimArgs{
				Stream:   streamName,
				Group:    groupName,
				Consumer: consumer2,
				MinIdle:  10 * time.Millisecond,
				Start:    "-",
			})
			require.NoError(t, rsp.Err())
			msgs, start := rsp.Val()
			require.Equal(t, "0-0", start)
			require.Len(t, msgs, 2)
			require.Equal(t, id1, msgs[0])
			require.Equal(t, id3, msgs[1])
		}
	})

	t.Run("XAUTOCLAIM as an iterator", func(t *testing.T) {
		streamName := "mystream"
		groupName := "mygroup"
		var id3, id5 string
		require.NoError(t, rdb.Del(ctx, streamName).Err())
		{
			rsp := rdb.XAdd(ctx, &redis.XAddArgs{
				Stream: streamName,
				ID:     "*",
				Values: []string{"a", "1"},
			})
			require.NoError(t, rsp.Err())
		}
		{
			rsp := rdb.XAdd(ctx, &redis.XAddArgs{
				Stream: streamName,
				ID:     "*",
				Values: []string{"b", "2"},
			})
			require.NoError(t, rsp.Err())
		}
		{
			rsp := rdb.XAdd(ctx, &redis.XAddArgs{
				Stream: streamName,
				ID:     "*",
				Values: []string{"c", "3"},
			})
			require.NoError(t, rsp.Err())
			id3 = rsp.Val()
		}
		{
			rsp := rdb.XAdd(ctx, &redis.XAddArgs{
				Stream: streamName,
				ID:     "*",
				Values: []string{"d", "4"},
			})
			require.NoError(t, rsp.Err())
		}
		{
			rsp := rdb.XAdd(ctx, &redis.XAddArgs{
				Stream: streamName,
				ID:     "*",
				Values: []string{"e", "5"},
			})
			require.NoError(t, rsp.Err())
			id5 = rsp.Val()
		}
		require.NoError(t, rdb.XGroupCreate(ctx, streamName, groupName, "0").Err())

		consumer1, consumer2 := "consumer1", "consumer2"
		{
			rsp := rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
				Group:    groupName,
				Consumer: consumer1,
				Streams:  []string{streamName, ">"},
				Count:    90,
			})
			require.NoError(t, rsp.Err())
			time.Sleep(200 * time.Millisecond)
		}
		{
			rsp := rdb.XAutoClaim(ctx, &redis.XAutoClaimArgs{
				Stream:   streamName,
				Group:    groupName,
				Consumer: consumer2,
				MinIdle:  10 * time.Millisecond,
				Count:    2,
				Start:    "-",
			})
			require.NoError(t, rsp.Err())
			msgs, start := rsp.Val()
			require.Equal(t, id3, start)
			require.Len(t, msgs, 2)
			require.Len(t, msgs[0].Values, 1)
			require.Equal(t, "1", msgs[0].Values["a"])
		}

		{
			rsp := rdb.XAutoClaim(ctx, &redis.XAutoClaimArgs{
				Stream:   streamName,
				Group:    groupName,
				Consumer: consumer2,
				MinIdle:  10 * time.Millisecond,
				Start:    id3,
				Count:    2,
			})
			require.NoError(t, rsp.Err())
			msgs, start := rsp.Val()
			require.Equal(t, id5, start)
			require.Len(t, msgs, 2)
			require.Len(t, msgs[0].Values, 1)
			require.Equal(t, "3", msgs[0].Values["c"])
		}

		{
			rsp := rdb.XAutoClaim(ctx, &redis.XAutoClaimArgs{
				Stream:   streamName,
				Group:    groupName,
				Consumer: consumer2,
				MinIdle:  10 * time.Millisecond,
				Start:    id5,
				Count:    1,
			})
			require.NoError(t, rsp.Err())
			msgs, start := rsp.Val()
			require.Equal(t, "0-0", start)
			require.Len(t, msgs, 1)
			require.Len(t, msgs[0].Values, 1)
			require.Equal(t, "5", msgs[0].Values["e"])
		}
	})

	t.Run("XAUTOCLAIM with XDEL", func(t *testing.T) {
		streamName := "x"
		groupName := "grp"
		require.NoError(t, rdb.Del(ctx, streamName).Err())
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{
			Stream: streamName,
			ID:     "1-0",
			Values: map[string]interface{}{"f": "v"},
		}).Err())
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{
			Stream: streamName,
			ID:     "2-0",
			Values: map[string]interface{}{"f": "v"},
		}).Err())
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{
			Stream: streamName,
			ID:     "3-0",
			Values: map[string]interface{}{"f": "v"},
		}).Err())
		require.NoError(t, rdb.XGroupCreate(ctx, streamName, groupName, "0").Err())
		{
			rsp := rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
				Group:    groupName,
				Consumer: "Alice",
				Streams:  []string{streamName, ">"},
			})
			require.NoError(t, rsp.Err())
			require.Len(t, rsp.Val(), 1)
			require.Len(t, rsp.Val()[0].Messages, 3)
			require.Equal(t, "1-0", rsp.Val()[0].Messages[0].ID)
			require.Equal(t, "v", rsp.Val()[0].Messages[0].Values["f"])
			require.Equal(t, "2-0", rsp.Val()[0].Messages[1].ID)
			require.Equal(t, "v", rsp.Val()[0].Messages[1].Values["f"])
			require.Equal(t, "3-0", rsp.Val()[0].Messages[2].ID)
			require.Equal(t, "v", rsp.Val()[0].Messages[2].Values["f"])
		}
		{
			require.NoError(t, rdb.XDel(ctx, streamName, "2-0").Err())
			cmd := rdb.Do(ctx, "XAUTOCLAIM", streamName, groupName, "Bob", 0, "0-0")
			require.NoError(t, cmd.Err())
			require.Equal(t, []interface{}{
				"0-0",
				[]interface{}{
					[]interface{}{
						"1-0",
						[]interface{}{"f", "v"},
					},
					[]interface{}{
						"3-0",
						[]interface{}{"f", "v"},
					},
				},
				[]interface{}{
					"2-0",
				},
			}, cmd.Val())
		}
	})

	t.Run("XAUTOCLAIM with XDEL and count", func(t *testing.T) {
		streamName := "x"
		groupName := "grp"
		require.NoError(t, rdb.Del(ctx, streamName).Err())
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{
			Stream: streamName,
			ID:     "1-0",
			Values: map[string]interface{}{"f": "v"},
		}).Err())
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{
			Stream: streamName,
			ID:     "2-0",
			Values: map[string]interface{}{"f": "v"},
		}).Err())
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{
			Stream: streamName,
			ID:     "3-0",
			Values: map[string]interface{}{"f": "v"},
		}).Err())
		require.NoError(t, rdb.XGroupCreate(ctx, streamName, groupName, "0").Err())
		{
			rsp := rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
				Group:    groupName,
				Consumer: "Alice",
				Streams:  []string{streamName, ">"},
			})
			require.NoError(t, rsp.Err())
			require.Len(t, rsp.Val(), 1)
			require.Len(t, rsp.Val()[0].Messages, 3)
			require.Equal(t, "1-0", rsp.Val()[0].Messages[0].ID)
			require.Equal(t, "v", rsp.Val()[0].Messages[0].Values["f"])
			require.Equal(t, "2-0", rsp.Val()[0].Messages[1].ID)
			require.Equal(t, "v", rsp.Val()[0].Messages[1].Values["f"])
			require.Equal(t, "3-0", rsp.Val()[0].Messages[2].ID)
			require.Equal(t, "v", rsp.Val()[0].Messages[2].Values["f"])
		}
		{
			require.NoError(t, rdb.XDel(ctx, streamName, "1-0").Err())
			require.NoError(t, rdb.XDel(ctx, streamName, "2-0").Err())
			cmd := rdb.Do(ctx, "XAUTOCLAIM", streamName, groupName, "Bob", 0, "0-0", "COUNT", 1)
			require.NoError(t, cmd.Err())
			require.Equal(t, []interface{}{
				"2-0",
				[]interface{}{},
				[]interface{}{
					"1-0",
				},
			}, cmd.Val())
		}
		{
			cmd := rdb.Do(ctx, "XAUTOCLAIM", streamName, groupName, "Bob", 0, "2-0", "COUNT", 1)
			require.NoError(t, cmd.Err())
			require.Equal(t, []interface{}{
				"3-0",
				[]interface{}{},
				[]interface{}{
					"2-0",
				},
			}, cmd.Val())
		}
		{
			cmd := rdb.Do(ctx, "XAUTOCLAIM", streamName, groupName, "Bob", 0, "3-0", "COUNT", 1)
			require.NoError(t, cmd.Err())
			require.Equal(t, []interface{}{
				"0-0",
				[]interface{}{
					[]interface{}{
						"3-0",
						[]interface{}{"f", "v"},
					},
				},
				[]interface{}{},
			}, cmd.Val())
		}
		// assert_equal [XPENDING x grp - + 10 Alice] {}
		// add xpending to this test case when it is supported
	})

	t.Run("XAUTOCLAIM with out of range count", func(t *testing.T) {
		err := rdb.XAutoClaim(ctx, &redis.XAutoClaimArgs{
			Stream:   "x",
			Group:    "grp",
			Consumer: "Bob",
			MinIdle:  0,
			Start:    "3-0",
			Count:    8070450532247928833,
		}).Err()
		require.Error(t, err)
		require.True(t, strings.HasPrefix(err.Error(), "ERR COUNT"))
	})

	t.Run("XAUTOCLAIM COUNT must be > 0", func(t *testing.T) {
		cmd := rdb.Do(ctx, "XAUTOCLAIM", "key", "group", "consumer", 1, 1, "COUNT", 0)
		require.Error(t, cmd.Err())
		require.Equal(t, "ERR COUNT must be > 0", cmd.Err().Error())
	})

	t.Run("XPending with different kinds of commands", func(t *testing.T) {
		streamName := "mystream"
		groupName := "mygroup"

		require.NoError(t, rdb.Del(ctx, streamName).Err())
		r, err := rdb.XAck(ctx, streamName, groupName, "0-0").Result()
		require.NoError(t, err)
		require.Equal(t, int64(0), r)

		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{
			Stream: streamName,
			ID:     "1-0",
			Values: []string{"field1", "data1"},
		}).Err())
		require.NoError(t, rdb.XGroupCreate(ctx, streamName, groupName, "0").Err())

		consumerName := "myconsumer"
		err = rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    groupName,
			Consumer: consumerName,
			Streams:  []string{streamName, ">"},
			Count:    1,
			NoAck:    false,
		}).Err()
		require.NoError(t, err)

		r1, err1 := rdb.XPending(ctx, streamName, groupName).Result()
		require.NoError(t, err1)

		require.Equal(t, &redis.XPending{
			Count:     1,
			Lower:     "1-0",
			Higher:    "1-0",
			Consumers: map[string]int64{consumerName: 1},
		}, r1)

		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{
			Stream: streamName,
			ID:     "2-0",
			Values: []string{"field1", "data1"},
		}).Err())

		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{
			Stream: streamName,
			ID:     "2-2",
			Values: []string{"field1", "data1"},
		}).Err())

		require.NoError(t, rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    groupName,
			Consumer: consumerName,
			Streams:  []string{streamName, ">"},
			Count:    2,
			NoAck:    false,
		}).Err())

		r1, err1 = rdb.XPending(ctx, streamName, groupName).Result()
		require.NoError(t, err1)

		require.Equal(t, &redis.XPending{
			Count:     3,
			Lower:     "1-0",
			Higher:    "2-2",
			Consumers: map[string]int64{consumerName: 3},
		}, r1)

		require.NoError(t, rdb.XAck(ctx, streamName, groupName, "2-0").Err())

		r1, err1 = rdb.XPending(ctx, streamName, groupName).Result()
		require.NoError(t, err1)

		require.Equal(t, &redis.XPending{
			Count:     2,
			Lower:     "1-0",
			Higher:    "2-2",
			Consumers: map[string]int64{consumerName: 2},
		}, r1)

		// Add a second consumer and check that XPENDING still works
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{
			Stream: streamName,
			ID:     "3-0",
			Values: []string{"field1", "data1"},
		}).Err())

		consumerName2 := "myconsumer2"
		err = rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    groupName,
			Consumer: consumerName2,
			Streams:  []string{streamName, ">"},
			Count:    1,
			NoAck:    false,
		}).Err()
		require.NoError(t, err)

		r1, err1 = rdb.XPending(ctx, streamName, groupName).Result()
		require.NoError(t, err1)

		require.Equal(t, &redis.XPending{
			Count:     3,
			Lower:     "1-0",
			Higher:    "3-0",
			Consumers: map[string]int64{consumerName: 2, consumerName2: 1},
		}, r1)
	})

	t.Run("XPENDING on a consumer group with no pending messages", func(t *testing.T) {
		streamName := "stream"
		groupName := "group"
		consumerName := "consumer"
		messageID := "1-0"

		// Remove any existing data
		require.NoError(t, rdb.Del(ctx, streamName).Err())
		r, err := rdb.XAck(ctx, streamName, groupName, "0-0").Result()
		require.NoError(t, err)
		require.Equal(t, int64(0), r)

		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{
			Stream: streamName,
			ID:     messageID,
			Values: []string{"key", "value"},
		}).Err())
		require.NoError(t, rdb.XGroupCreate(ctx, streamName, groupName, "0").Err())

		// Have the consumer claim the message with ID [messageID]
		err = rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    groupName,
			Consumer: consumerName,
			Streams:  []string{streamName, ">"},
			Count:    1,
			NoAck:    false,
		}).Err()
		require.NoError(t, err)

		// Acknowledge the message, so no messages remain pending
		require.NoError(t, rdb.XAck(ctx, streamName, groupName, messageID).Err())

		// Check that XPENDING sets the min and max to nil, matching Redis' behavior
		pending, err := rdb.XPending(ctx, streamName, groupName).Result()
		require.NoError(t, err)
		require.Equal(t, &redis.XPending{
			Count:     0,
			Lower:     "",
			Higher:    "",
			Consumers: map[string]int64{},
		}, pending)
	})

	t.Run("XINFO GROUPS, issue #2568", func(t *testing.T) {
		streamName := "mystream-2568"
		groupName := "mygroup-2568"
		consumerName := "consumer-2568"
		require.NoError(t, rdb.Del(ctx, streamName).Err())

		require.NoError(t, rdb.XGroupCreateMkStream(ctx, streamName, groupName, "0").Err())

		{
			r, err := rdb.XInfoGroups(ctx, streamName).Result()
			require.NoError(t, err)
			require.Equal(t, []redis.XInfoGroup{{
				Name:            groupName,
				Consumers:       0,
				Pending:         0,
				LastDeliveredID: "0-0",
			}}, r)
		}
		{
			_, err := rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
				Group:    groupName,
				Consumer: consumerName,
				Streams:  []string{streamName, "0"},
			}).Result()
			require.NoError(t, err)
		}
		{
			r, err := rdb.XInfoGroups(ctx, streamName).Result()
			require.NoError(t, err)
			require.Equal(t, []redis.XInfoGroup{{
				Name:            groupName,
				Consumers:       1,
				Pending:         0,
				LastDeliveredID: "0-0",
			}}, r)
		}
	})
}

func parseStreamEntryID(id string) (ts int64, seqNum int64) {
	values := strings.Split(id, "-")

	ts, _ = strconv.ParseInt(values[0], 10, 64)
	seqNum, _ = strconv.ParseInt(values[1], 10, 64)

	return
}
