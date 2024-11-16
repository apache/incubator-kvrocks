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

package multi

import (
	"context"
	"fmt"
	"testing"

	"github.com/apache/kvrocks/tests/gocase/util"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestMulti(t *testing.T) {
	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	t.Run("MULTI / EXEC basics", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "mylist").Err())
		require.NoError(t, rdb.RPush(ctx, "mylist", "a").Err())
		require.NoError(t, rdb.RPush(ctx, "mylist", "b").Err())
		require.NoError(t, rdb.RPush(ctx, "mylist", "c").Err())

		require.NoError(t, rdb.Do(ctx, "MULTI").Err())
		v1 := rdb.Do(ctx, "LRANGE", "mylist", 0, -1).Val()
		v2 := rdb.Do(ctx, "PING").Val()
		v3 := rdb.Do(ctx, "EXEC").Val()
		require.Equal(t, "QUEUED", fmt.Sprintf("%v", v1))
		require.Equal(t, "QUEUED", fmt.Sprintf("%v", v2))
		require.Equal(t, "[[a b c] PONG]", fmt.Sprintf("%v", v3))
	})

	t.Run("DISCARD", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "mylist").Err())
		require.NoError(t, rdb.RPush(ctx, "mylist", "a").Err())
		require.NoError(t, rdb.RPush(ctx, "mylist", "b").Err())
		require.NoError(t, rdb.RPush(ctx, "mylist", "c").Err())

		require.NoError(t, rdb.Do(ctx, "MULTI").Err())
		v1 := rdb.Do(ctx, "DEL", "mylist").Val()
		v2 := rdb.Do(ctx, "DISCARD").Val()
		require.Equal(t, "QUEUED", fmt.Sprintf("%v", v1))
		require.Equal(t, "OK", fmt.Sprintf("%v", v2))

		require.Equal(t, []string{"a", "b", "c"}, rdb.LRange(ctx, "mylist", 0, -1).Val())
	})

	t.Run("Nested MULTI are not allowed", func(t *testing.T) {
		require.NoError(t, rdb.Do(ctx, "MULTI").Err())
		require.EqualError(t, rdb.Do(ctx, "MULTI").Err(), "ERR MULTI calls can not be nested")
		require.NoError(t, rdb.Do(ctx, "EXEC").Err())
	})

	t.Run("MULTI where commands alter argc/argv", func(t *testing.T) {
		require.NoError(t, rdb.SAdd(ctx, "myset", "a").Err())
		require.NoError(t, rdb.Do(ctx, "MULTI").Err())
		require.NoError(t, rdb.Do(ctx, "SPOP", "myset").Err())
		v := rdb.Do(ctx, "EXEC").Val()
		require.Equal(t, "[a]", fmt.Sprintf("%v", v))
		require.Zero(t, rdb.Exists(ctx, "myset").Val())
	})

	t.Run("TRANSACTION can read its own writes", func(t *testing.T) {
		var get *redis.StringCmd
		var hgetall *redis.MapStringStringCmd
		require.NoError(t, rdb.HSet(ctx, "my_hash_key", "f0", "v0").Err())
		_, err := rdb.TxPipelined(ctx, func(pipeline redis.Pipeliner) error {
			pipeline.Set(ctx, "my_string_key", "visible", 0)
			get = pipeline.Get(ctx, "my_string_key")
			pipeline.HSet(ctx, "my_hash_key", "f1", "v1", "f2", "v2")
			hgetall = pipeline.HGetAll(ctx, "my_hash_key")
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, "visible", get.Val())
		require.Equal(t, map[string]string{"f0": "v0", "f1": "v1", "f2": "v2"}, hgetall.Val())
	})

	t.Run("EXEC fails if there are errors while queueing commands #1", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "foo1", "foo2").Err())
		require.NoError(t, rdb.Do(ctx, "MULTI").Err())
		require.NoError(t, rdb.Do(ctx, "SET", "foo1", "bar1").Err())
		require.Error(t, rdb.Do(ctx, "non-existing-command").Err())
		require.NoError(t, rdb.Do(ctx, "SET", "foo2", "bar2").Err())
		require.EqualError(t, rdb.Do(ctx, "EXEC").Err(), "EXECABORT Transaction discarded")
		require.Zero(t, rdb.Exists(ctx, "foo1").Val())
		require.Zero(t, rdb.Exists(ctx, "foo2").Val())
	})

	t.Run("If EXEC aborts, the client MULTI state is cleared", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "foo1", "foo2").Err())
		require.NoError(t, rdb.Do(ctx, "MULTI").Err())
		require.NoError(t, rdb.Do(ctx, "SET", "foo1", "bar1").Err())
		require.Error(t, rdb.Do(ctx, "non-existing-command").Err())
		require.NoError(t, rdb.Do(ctx, "SET", "foo2", "bar2").Err())
		require.EqualError(t, rdb.Do(ctx, "EXEC").Err(), "EXECABORT Transaction discarded")
		require.Equal(t, "PONG", rdb.Ping(ctx).Val())
	})

	t.Run("Blocking commands ignores the timeout", func(t *testing.T) {
		v1 := rdb.Do(ctx, "MULTI").Val()
		require.NoError(t, rdb.Do(ctx, "BLPOP", "empty_list", "0").Err())
		require.NoError(t, rdb.Do(ctx, "BRPOP", "empty_list", "0").Err())
		v2 := rdb.Do(ctx, "EXEC").Val().([]interface{})

		require.Equal(t, "OK", fmt.Sprintf("%v", v1))
		require.Len(t, v2, 2)
		require.Nil(t, v2[0])
		require.Nil(t, v2[1])
	})

	t.Run("If EXEC aborts if commands are not allowed in MULTI-EXEC", func(t *testing.T) {
		require.NoError(t, rdb.Do(ctx, "MULTI").Err())
		require.NoError(t, rdb.Do(ctx, "SET", "foo1", "bar1").Err())
		require.Error(t, rdb.Do(ctx, "monitor").Err())
		require.NoError(t, rdb.Do(ctx, "SET", "foo2", "bar2").Err())
		require.EqualError(t, rdb.Do(ctx, "EXEC").Err(), "EXECABORT Transaction discarded")
		require.Equal(t, "PONG", rdb.Ping(ctx).Val())
	})

	func() {
		newSrv := util.StartServer(t, map[string]string{})
		defer newSrv.Close()

		t.Run("MULTI-EXEC used in redis-sentinel for failover", func(t *testing.T) {
			{
				rdb := newSrv.NewClient()
				defer func() { require.NoError(t, rdb.Close()) }()
				require.NoError(t, rdb.Do(ctx, "MULTI").Err())
				util.SlaveOf(t, rdb, srv)
				require.NoError(t, rdb.ConfigRewrite(ctx).Err())
				require.NoError(t, rdb.Do(ctx, "CLIENT", "KILL", "TYPE", "normal").Err())
				require.NoError(t, rdb.Do(ctx, "CLIENT", "KILL", "TYPE", "PUBSUB").Err())
				require.NoError(t, rdb.Do(ctx, "EXEC").Err())
				require.Equal(t, "slave", util.FindInfoEntry(rdb, "role"))
			}

			{
				rdb := newSrv.NewClient()
				defer func() { require.NoError(t, rdb.Close()) }()
				require.NoError(t, rdb.Do(ctx, "MULTI").Err())
				require.NoError(t, rdb.SlaveOf(ctx, "NO", "ONE").Err())
				require.NoError(t, rdb.ConfigRewrite(ctx).Err())
				require.NoError(t, rdb.Do(ctx, "CLIENT", "KILL", "TYPE", "normal").Err())
				require.NoError(t, rdb.Do(ctx, "CLIENT", "KILL", "TYPE", "PUBSUB").Err())
				require.NoError(t, rdb.Do(ctx, "EXEC").Err())
				require.Equal(t, "master", util.FindInfoEntry(rdb, "role"))
			}
		})
	}()

	t.Run("WATCH inside MULTI is not allowed", func(t *testing.T) {
		require.NoError(t, rdb.Do(ctx, "MULTI").Err())
		require.EqualError(t, rdb.Do(ctx, "WATCH", "x").Err(), "ERR WATCH inside MULTI is not allowed")
		require.NoError(t, rdb.Do(ctx, "DISCARD").Err())
	})

	t.Run("EXEC without MULTI is not allowed", func(t *testing.T) {
		require.EqualError(t, rdb.Do(ctx, "EXEC").Err(), "ERR EXEC without MULTI")
	})

	t.Run("EXEC without MULTI should not reset watch", func(t *testing.T) {
		rdb2 := srv.NewClient()
		defer func() { require.NoError(t, rdb2.Close()) }()

		require.NoError(t, rdb.Do(ctx, "WATCH", "x").Err())
		require.NoError(t, rdb2.Do(ctx, "SET", "x", "xxx").Err())
		require.EqualError(t, rdb.Do(ctx, "EXEC").Err(), "ERR EXEC without MULTI")
		require.NoError(t, rdb.Do(ctx, "MULTI").Err())
		require.NoError(t, rdb.Do(ctx, "PING").Err())
		require.Equal(t, rdb.Do(ctx, "EXEC").Val(), nil)
	})

	t.Run("EXEC works on WATCHed key not modified", func(t *testing.T) {
		require.NoError(t, rdb.Do(ctx, "WATCH", "x", "y", "z").Err())
		require.NoError(t, rdb.Do(ctx, "WATCH", "k").Err())
		require.NoError(t, rdb.Do(ctx, "MULTI").Err())
		require.NoError(t, rdb.Do(ctx, "PING").Err())
		require.Equal(t, rdb.Do(ctx, "EXEC").Val(), []interface{}{"PONG"})
	})

	t.Run("EXEC works on non-WATCHed key modified", func(t *testing.T) {
		require.NoError(t, rdb.Do(ctx, "WATCH", "x", "y").Err())
		require.NoError(t, rdb.Set(ctx, "z", 0, 0).Err())
		require.NoError(t, rdb.Do(ctx, "MULTI").Err())
		require.NoError(t, rdb.Do(ctx, "PING").Err())
		require.Equal(t, rdb.Do(ctx, "EXEC").Val(), []interface{}{"PONG"})
	})

	t.Run("EXEC fail on WATCHed key modified (1 key of 1 watched)", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "x", 30, 0).Err())
		require.NoError(t, rdb.Do(ctx, "WATCH", "x").Err())
		require.NoError(t, rdb.Set(ctx, "x", 40, 0).Err())
		require.NoError(t, rdb.Do(ctx, "MULTI").Err())
		require.NoError(t, rdb.Do(ctx, "PING").Err())
		require.Equal(t, rdb.Do(ctx, "EXEC").Val(), nil)
	})

	t.Run("EXEC fail on WATCHed key modified (1 key of 5 watched)", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "x", 30, 0).Err())
		require.NoError(t, rdb.Do(ctx, "WATCH", "a", "b", "x", "k", "z").Err())
		require.NoError(t, rdb.Set(ctx, "x", 40, 0).Err())
		require.NoError(t, rdb.Do(ctx, "MULTI").Err())
		require.NoError(t, rdb.Do(ctx, "PING").Err())
		require.Equal(t, rdb.Do(ctx, "EXEC").Val(), nil)
	})

	t.Run("After successful EXEC key is no longer watched", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "x", 30, 0).Err())
		require.NoError(t, rdb.Do(ctx, "WATCH", "x").Err())
		require.NoError(t, rdb.Do(ctx, "MULTI").Err())
		require.NoError(t, rdb.Do(ctx, "PING").Err())
		require.Equal(t, rdb.Do(ctx, "EXEC").Val(), []interface{}{"PONG"})
		require.NoError(t, rdb.Set(ctx, "x", 40, 0).Err())
		require.NoError(t, rdb.Do(ctx, "MULTI").Err())
		require.NoError(t, rdb.Do(ctx, "PING").Err())
		require.Equal(t, rdb.Do(ctx, "EXEC").Val(), []interface{}{"PONG"})
	})

	t.Run("After failed EXEC key is no longer watched", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "x", 30, 0).Err())
		require.NoError(t, rdb.Do(ctx, "WATCH", "x").Err())
		require.NoError(t, rdb.Set(ctx, "x", 40, 0).Err())
		require.NoError(t, rdb.Do(ctx, "MULTI").Err())
		require.NoError(t, rdb.Do(ctx, "PING").Err())
		require.Equal(t, rdb.Do(ctx, "EXEC").Val(), nil)
		require.NoError(t, rdb.Set(ctx, "x", 40, 0).Err())
		require.NoError(t, rdb.Do(ctx, "MULTI").Err())
		require.NoError(t, rdb.Do(ctx, "PING").Err())
		require.Equal(t, rdb.Do(ctx, "EXEC").Val(), []interface{}{"PONG"})
	})

	t.Run("It is possible to UNWATCH", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "x", 30, 0).Err())
		require.NoError(t, rdb.Do(ctx, "WATCH", "x").Err())
		require.NoError(t, rdb.Set(ctx, "x", 40, 0).Err())
		require.NoError(t, rdb.Do(ctx, "UNWATCH").Err())
		require.NoError(t, rdb.Do(ctx, "MULTI").Err())
		require.NoError(t, rdb.Do(ctx, "PING").Err())
		require.Equal(t, rdb.Do(ctx, "EXEC").Val(), []interface{}{"PONG"})
	})

	t.Run("UNWATCH when there is nothing watched works as expected", func(t *testing.T) {
		require.NoError(t, rdb.Do(ctx, "UNWATCH").Err())
	})

	t.Run("FLUSHALL is able to touch the watched keys", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "x", 30, 0).Err())
		require.NoError(t, rdb.Do(ctx, "WATCH", "x").Err())
		require.NoError(t, rdb.Do(ctx, "FLUSHALL").Err())
		require.NoError(t, rdb.Do(ctx, "MULTI").Err())
		require.NoError(t, rdb.Do(ctx, "PING").Err())
		require.Equal(t, rdb.Do(ctx, "EXEC").Val(), nil)
	})

	t.Run("FLUSHDB is able to touch the watched keys", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "x", 30, 0).Err())
		require.NoError(t, rdb.Do(ctx, "WATCH", "x").Err())
		require.NoError(t, rdb.Do(ctx, "FLUSHDB").Err())
		require.NoError(t, rdb.Do(ctx, "MULTI").Err())
		require.NoError(t, rdb.Do(ctx, "PING").Err())
		require.Equal(t, rdb.Do(ctx, "EXEC").Val(), nil)
	})

	t.Run("DISCARD without MULTI is not allowed", func(t *testing.T) {
		require.EqualError(t, rdb.Do(ctx, "DISCARD").Err(), "ERR DISCARD without MULTI")
	})

	t.Run("DISCARD without MULTI should not reset watch", func(t *testing.T) {
		rdb2 := srv.NewClient()
		defer func() { require.NoError(t, rdb2.Close()) }()

		require.NoError(t, rdb.Do(ctx, "WATCH", "x").Err())
		require.NoError(t, rdb2.Do(ctx, "SET", "x", "xxx").Err())
		require.EqualError(t, rdb.Do(ctx, "DISCARD").Err(), "ERR DISCARD without MULTI")
		require.NoError(t, rdb.Do(ctx, "MULTI").Err())
		require.NoError(t, rdb.Do(ctx, "PING").Err())
		require.Equal(t, rdb.Do(ctx, "EXEC").Val(), nil)
	})

	t.Run("DISCARD should clear the WATCH dirty flag on the client", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "x", 30, 0).Err())
		require.NoError(t, rdb.Do(ctx, "WATCH", "x").Err())
		require.NoError(t, rdb.Set(ctx, "x", 40, 0).Err())
		require.NoError(t, rdb.Do(ctx, "MULTI").Err())
		require.NoError(t, rdb.Do(ctx, "DISCARD").Err())
		require.NoError(t, rdb.Do(ctx, "MULTI").Err())
		require.NoError(t, rdb.Do(ctx, "INCR", "x").Err())
		require.Equal(t, rdb.Do(ctx, "EXEC").Val(), []interface{}{int64(41)})
	})

	t.Run("DISCARD should UNWATCH all the keys", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "x", 30, 0).Err())
		require.NoError(t, rdb.Do(ctx, "WATCH", "x").Err())
		require.NoError(t, rdb.Set(ctx, "x", 40, 0).Err())
		require.NoError(t, rdb.Do(ctx, "MULTI").Err())
		require.NoError(t, rdb.Do(ctx, "DISCARD").Err())
		require.NoError(t, rdb.Set(ctx, "x", 50, 0).Err())
		require.NoError(t, rdb.Do(ctx, "MULTI").Err())
		require.NoError(t, rdb.Do(ctx, "INCR", "x").Err())
		require.Equal(t, rdb.Do(ctx, "EXEC").Val(), []interface{}{int64(51)})
	})
}
