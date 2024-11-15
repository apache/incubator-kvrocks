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

package command

import (
	"context"
	"testing"

	"github.com/apache/kvrocks/tests/gocase/util"
	"github.com/stretchr/testify/require"
)

func TestCommand(t *testing.T) {
	srv := util.StartServer(t, map[string]string{
		"rename-command KEYS":   "RENAMED_KEYS",
		"rename-command CONFIG": "''", // remove CONFIG command
	})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	t.Run("acquire GET command info by COMMAND INFO", func(t *testing.T) {
		r := rdb.Do(ctx, "COMMAND", "INFO", "GET")
		vs, err := r.Slice()
		require.NoError(t, err)
		require.Len(t, vs, 1)
		v := vs[0].([]interface{})
		require.Len(t, v, 6)
		require.Equal(t, "get", v[0])
		require.EqualValues(t, 2, v[1])
		require.Equal(t, []interface{}{"readonly"}, v[2])
		require.EqualValues(t, 1, v[3])
		require.EqualValues(t, 1, v[4])
		require.EqualValues(t, 1, v[5])
	})

	t.Run("acquire renamed command info by COMMAND INFO", func(t *testing.T) {
		r := rdb.Do(ctx, "COMMAND", "INFO", "KEYS")
		vs, err := r.Slice()
		require.NoError(t, err)
		require.Len(t, vs, 1)
		require.Nil(t, vs[0])

		r = rdb.Do(ctx, "COMMAND", "INFO", "RENAMED_KEYS")
		vs, err = r.Slice()
		require.NoError(t, err)
		require.Len(t, vs, 1)
		v := vs[0].([]interface{})
		require.Len(t, v, 6)
		require.Equal(t, "keys", v[0])
		require.EqualValues(t, 2, v[1])
		require.Equal(t, []interface{}{"readonly"}, v[2])
		require.EqualValues(t, 0, v[3])
		require.EqualValues(t, 0, v[4])
		require.EqualValues(t, 0, v[5])
	})

	t.Run("renamed command can be acquired by COMMAND", func(t *testing.T) {
		commandInfos := rdb.Command(ctx).Val()
		require.NotNil(t, commandInfos)
		_, found := commandInfos["keys"]
		require.True(t, found)
	})

	t.Run("removed command can not be acquired by COMMAND", func(t *testing.T) {
		commandInfos := rdb.Command(ctx).Val()
		require.NotNil(t, commandInfos)
		_, found := commandInfos["config"]
		require.True(t, found)
	})

	t.Run("command entry length check", func(t *testing.T) {
		r := rdb.Do(ctx, "COMMAND")
		vs, err := r.Slice()
		require.NoError(t, err)
		v := vs[0].([]interface{})
		require.Len(t, v, 6)
	})

	t.Run("get keys of commands by COMMAND GETKEYS", func(t *testing.T) {
		r := rdb.Do(ctx, "COMMAND", "GETKEYS", "GET", "test")
		vs, err := r.Slice()
		require.NoError(t, err)
		require.Len(t, vs, 1)
		require.Equal(t, "test", vs[0])
	})

	t.Run("COMMAND GETKEYS SINTERCARD", func(t *testing.T) {
		r := rdb.Do(ctx, "COMMAND", "GETKEYS", "SINTERCARD", "2", "key1", "key2")
		vs, err := r.Slice()
		require.NoError(t, err)
		require.Len(t, vs, 2)
		require.Equal(t, "key1", vs[0])
		require.Equal(t, "key2", vs[1])
	})

	t.Run("COMMAND GETKEYS ZINTER", func(t *testing.T) {
		r := rdb.Do(ctx, "COMMAND", "GETKEYS", "ZINTER", "2", "key1", "key2")
		vs, err := r.Slice()
		require.NoError(t, err)
		require.Len(t, vs, 2)
		require.Equal(t, "key1", vs[0])
		require.Equal(t, "key2", vs[1])
	})

	t.Run("COMMAND GETKEYS ZINTERSTORE", func(t *testing.T) {
		r := rdb.Do(ctx, "COMMAND", "GETKEYS", "ZINTERSTORE", "dst", "2", "src1", "src2")
		vs, err := r.Slice()
		require.NoError(t, err)
		require.Len(t, vs, 3)
		require.Equal(t, "dst", vs[0])
		require.Equal(t, "src1", vs[1])
		require.Equal(t, "src2", vs[2])
	})

	t.Run("COMMAND GETKEYS ZINTERCARD", func(t *testing.T) {
		r := rdb.Do(ctx, "COMMAND", "GETKEYS", "ZINTERCARD", "2", "key1", "key2")
		vs, err := r.Slice()
		require.NoError(t, err)
		require.Len(t, vs, 2)
		require.Equal(t, "key1", vs[0])
		require.Equal(t, "key2", vs[1])
	})

	t.Run("COMMAND GETKEYS ZUNION", func(t *testing.T) {
		r := rdb.Do(ctx, "COMMAND", "GETKEYS", "ZUNION", "2", "key1", "key2")
		vs, err := r.Slice()
		require.NoError(t, err)
		require.Len(t, vs, 2)
		require.Equal(t, "key1", vs[0])
		require.Equal(t, "key2", vs[1])
	})

	t.Run("COMMAND GETKEYS ZUNIONSTORE", func(t *testing.T) {
		r := rdb.Do(ctx, "COMMAND", "GETKEYS", "ZUNIONSTORE", "dst", "2", "src1", "src2")
		vs, err := r.Slice()
		require.NoError(t, err)
		require.Len(t, vs, 3)
		require.Equal(t, "dst", vs[0])
		require.Equal(t, "src1", vs[1])
		require.Equal(t, "src2", vs[2])
	})

	t.Run("COMMAND GETKEYS ZDIFF", func(t *testing.T) {
		r := rdb.Do(ctx, "COMMAND", "GETKEYS", "ZDIFF", "2", "key1", "key2")
		vs, err := r.Slice()
		require.NoError(t, err)
		require.Len(t, vs, 2)
		require.Equal(t, "key1", vs[0])
		require.Equal(t, "key2", vs[1])
	})

	t.Run("COMMAND GETKEYS ZDIFFSTORE", func(t *testing.T) {
		r := rdb.Do(ctx, "COMMAND", "GETKEYS", "ZDIFFSTORE", "dst", "2", "src1", "src2")
		vs, err := r.Slice()
		require.NoError(t, err)
		require.Len(t, vs, 3)
		require.Equal(t, "dst", vs[0])
		require.Equal(t, "src1", vs[1])
		require.Equal(t, "src2", vs[2])
	})

	t.Run("COMMAND GETKEYS ZMPOP", func(t *testing.T) {
		r := rdb.Do(ctx, "COMMAND", "GETKEYS", "ZMPOP", "2", "key1", "key2", "min")
		vs, err := r.Slice()
		require.NoError(t, err)
		require.Len(t, vs, 2)
		require.Equal(t, "key1", vs[0])
		require.Equal(t, "key2", vs[1])
	})

	t.Run("COMMAND GETKEYS BZMPOP", func(t *testing.T) {
		r := rdb.Do(ctx, "COMMAND", "GETKEYS", "BZMPOP", "0", "2", "key1", "key2", "min")
		vs, err := r.Slice()
		require.NoError(t, err)
		require.Len(t, vs, 2)
		require.Equal(t, "key1", vs[0])
		require.Equal(t, "key2", vs[1])
	})

	t.Run("COMMAND GETKEYS LMPOP", func(t *testing.T) {
		r := rdb.Do(ctx, "COMMAND", "GETKEYS", "LMPOP", "2", "key1", "key2", "left")
		vs, err := r.Slice()
		require.NoError(t, err)
		require.Len(t, vs, 2)
		require.Equal(t, "key1", vs[0])
		require.Equal(t, "key2", vs[1])
	})

	t.Run("COMMAND GETKEYS BLMPOP", func(t *testing.T) {
		r := rdb.Do(ctx, "COMMAND", "GETKEYS", "BLMPOP", "0", "2", "key1", "key2", "left")
		vs, err := r.Slice()
		require.NoError(t, err)
		require.Len(t, vs, 2)
		require.Equal(t, "key1", vs[0])
		require.Equal(t, "key2", vs[1])
	})

	t.Run("COMMAND GETKEYS GEORADIUS", func(t *testing.T) {
		// non-store
		r := rdb.Do(ctx, "COMMAND", "GETKEYS", "GEORADIUS", "src", "1", "1", "1", "km")
		vs, err := r.Slice()
		require.NoError(t, err)
		require.Len(t, vs, 1)
		require.Equal(t, "src", vs[0])

		// store
		r = rdb.Do(ctx, "COMMAND", "GETKEYS", "GEORADIUS", "src", "1", "1", "1", "km", "store", "dst")
		vs, err = r.Slice()
		require.NoError(t, err)
		require.Len(t, vs, 2)
		require.Equal(t, "src", vs[0])
		require.Equal(t, "dst", vs[1])

		// storedist
		r = rdb.Do(ctx, "COMMAND", "GETKEYS", "GEORADIUS", "src", "1", "1", "1", "km", "storedist", "dst")
		vs, err = r.Slice()
		require.NoError(t, err)
		require.Len(t, vs, 2)
		require.Equal(t, "src", vs[0])
		require.Equal(t, "dst", vs[1])

		// store + storedist
		r = rdb.Do(ctx, "COMMAND", "GETKEYS", "GEORADIUS", "src", "1", "1", "1", "km", "store", "dst1", "storedist", "dst2")
		vs, err = r.Slice()
		require.NoError(t, err)
		require.Len(t, vs, 2)
		require.Equal(t, "src", vs[0])
		require.Equal(t, "dst2", vs[1])
	})

	t.Run("COMMAND GETKEYS GEORADIUSBYMEMBER", func(t *testing.T) {
		// non-store
		r := rdb.Do(ctx, "COMMAND", "GETKEYS", "GEORADIUSBYMEMBER", "src", "member", "100", "m")
		vs, err := r.Slice()
		require.NoError(t, err)
		require.Len(t, vs, 1)
		require.Equal(t, "src", vs[0])

		// store
		r = rdb.Do(ctx, "COMMAND", "GETKEYS", "GEORADIUSBYMEMBER", "src", "member", "100", "m", "store", "dst")
		vs, err = r.Slice()
		require.NoError(t, err)
		require.Len(t, vs, 2)
		require.Equal(t, "src", vs[0])
		require.Equal(t, "dst", vs[1])

		// storedist
		r = rdb.Do(ctx, "COMMAND", "GETKEYS", "GEORADIUSBYMEMBER", "src", "member", "100", "m", "storedist", "dst")
		vs, err = r.Slice()
		require.NoError(t, err)
		require.Len(t, vs, 2)
		require.Equal(t, "src", vs[0])
		require.Equal(t, "dst", vs[1])

		// store + storedist
		r = rdb.Do(ctx, "COMMAND", "GETKEYS", "GEORADIUSBYMEMBER", "src", "member", "100", "m", "store", "dst1", "storedist", "dst2")
		vs, err = r.Slice()
		require.NoError(t, err)
		require.Len(t, vs, 2)
		require.Equal(t, "src", vs[0])
		require.Equal(t, "dst2", vs[1])
	})

	t.Run("COMMAND GETKEYS GEOSEARCHSTORE", func(t *testing.T) {
		r := rdb.Do(ctx, "COMMAND", "GETKEYS", "GEOSEARCHSTORE", "dst", "src", "frommember", "member", "byradius", "10", "m")
		vs, err := r.Slice()
		require.NoError(t, err)
		require.Len(t, vs, 2)
		require.Equal(t, "dst", vs[0])
		require.Equal(t, "src", vs[1])
	})

	t.Run("COMMAND GETKEYS XREAD", func(t *testing.T) {
		{
			r := rdb.Do(ctx, "COMMAND", "GETKEYS", "XREAD",
				"STREAMS", "k0-1", "k0-2", "0-0", "0-0")
			vs, err := r.Slice()
			require.NoError(t, err)
			require.Len(t, vs, 2)
			require.Equal(t, "k0-1", vs[0])
			require.Equal(t, "k0-2", vs[1])
		}

		{
			r := rdb.Do(ctx, "COMMAND", "GETKEYS", "XREAD", "COUNT", "10",
				"STREAMS", "k1-1", "k1-2", "0-0", "0-0")
			vs, err := r.Slice()
			require.NoError(t, err)
			require.Len(t, vs, 2)
			require.Equal(t, "k1-1", vs[0])
			require.Equal(t, "k1-2", vs[1])
		}

		{
			r := rdb.Do(ctx, "COMMAND", "GETKEYS", "XREAD", "BLOCK", "1000",
				"STREAMS", "k2-1", "k2-2", "0-0", "0-0")
			vs, err := r.Slice()
			require.NoError(t, err)
			require.Len(t, vs, 2)
			require.Equal(t, "k2-1", vs[0])
			require.Equal(t, "k2-2", vs[1])
		}

		{
			r := rdb.Do(ctx, "COMMAND", "GETKEYS", "XREAD", "COUNT", "10",
				"BLOCK", "1000", "STREAMS", "k3-1", "k3-2", "0-0", "0-0")
			vs, err := r.Slice()
			require.NoError(t, err)
			require.Len(t, vs, 2)
			require.Equal(t, "k3-1", vs[0])
			require.Equal(t, "k3-2", vs[1])
		}
	})

	t.Run("COMMAND GETKEYS XREADGROUP", func(t *testing.T) {
		{
			r := rdb.Do(ctx, "COMMAND", "GETKEYS", "XREADGROUP", "GROUP", "group1", "consumer1",
				"STREAMS", "gk0-1", "gk0-2", "0-0", "0-0")
			vs, err := r.Slice()
			require.NoError(t, err)
			require.Len(t, vs, 2)
			require.Equal(t, "gk0-1", vs[0])
			require.Equal(t, "gk0-2", vs[1])
		}

		{
			r := rdb.Do(ctx, "COMMAND", "GETKEYS", "XREADGROUP", "GROUP", "streams", "streams",
				"STREAMS", "gk1-1", "gk1-2", "0-0", "0-0")
			vs, err := r.Slice()
			require.NoError(t, err)
			require.Len(t, vs, 2)
			require.Equal(t, "gk1-1", vs[0])
			require.Equal(t, "gk1-2", vs[1])
		}

		{
			r := rdb.Do(ctx, "COMMAND", "GETKEYS", "XREADGROUP", "GROUP", "group1", "consumer1",
				"COUNT", "10", "STREAMS", "gk3-1", "gk3-2", "0-0", "0-0")
			vs, err := r.Slice()
			require.NoError(t, err)
			require.Len(t, vs, 2)
			require.Equal(t, "gk3-1", vs[0])
			require.Equal(t, "gk3-2", vs[1])
		}

		{
			r := rdb.Do(ctx, "COMMAND", "GETKEYS", "XREADGROUP", "GROUP", "group1", "consumer1",
				"BLOCK", "10", "STREAMS", "gk4-1", "gk4-2", "0-0", "0-0")
			vs, err := r.Slice()
			require.NoError(t, err)
			require.Len(t, vs, 2)
			require.Equal(t, "gk4-1", vs[0])
			require.Equal(t, "gk4-2", vs[1])
		}

		{
			r := rdb.Do(ctx, "COMMAND", "GETKEYS", "XREADGROUP", "GROUP", "group1", "consumer1",
				"NOACK", "STREAMS", "gk5-1", "gk5-2", "0-0", "0-0")
			vs, err := r.Slice()
			require.NoError(t, err)
			require.Len(t, vs, 2)
			require.Equal(t, "gk5-1", vs[0])
			require.Equal(t, "gk5-2", vs[1])
		}

		{
			r := rdb.Do(ctx, "COMMAND", "GETKEYS", "XREADGROUP", "GROUP", "group1", "consumer1",
				"COUNT", "10", "NOACK", "STREAMS", "gk6-1", "gk6-2", "0-0", "0-0")
			vs, err := r.Slice()
			require.NoError(t, err)
			require.Len(t, vs, 2)
			require.Equal(t, "gk6-1", vs[0])
			require.Equal(t, "gk6-2", vs[1])
		}

		{
			r := rdb.Do(ctx, "COMMAND", "GETKEYS", "XREADGROUP", "GROUP", "group1", "consumer1",
				"BLOCK", "1000", "NOACK", "STREAMS", "gk7-1", "gk7-2", "0-0", "0-0")
			vs, err := r.Slice()
			require.NoError(t, err)
			require.Len(t, vs, 2)
			require.Equal(t, "gk7-1", vs[0])
			require.Equal(t, "gk7-2", vs[1])
		}

		{
			r := rdb.Do(ctx, "COMMAND", "GETKEYS", "XREADGROUP", "GROUP", "group1", "consumer1",
				"COUNT", "10", "BLOCK", "1000", "STREAMS", "gk8-1", "gk8-2", "0-0", "0-0")
			vs, err := r.Slice()
			require.NoError(t, err)
			require.Len(t, vs, 2)
			require.Equal(t, "gk8-1", vs[0])
			require.Equal(t, "gk8-2", vs[1])
		}

		{
			r := rdb.Do(ctx, "COMMAND", "GETKEYS", "XREADGROUP", "GROUP", "group1", "consumer1",
				"COUNT", "10", "BLOCK", "1000", "NOACK", "STREAMS", "gk9-1", "gk9-2", "0-0", "0-0")
			vs, err := r.Slice()
			require.NoError(t, err)
			require.Len(t, vs, 2)
			require.Equal(t, "gk9-1", vs[0])
			require.Equal(t, "gk9-2", vs[1])
		}
	})
}
