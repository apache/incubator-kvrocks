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

package scripting

import (
	"context"
	_ "embed"
	"fmt"
	"strings"
	"testing"

	"github.com/redis/go-redis/v9"

	"github.com/apache/kvrocks/tests/gocase/util"
	"github.com/stretchr/testify/require"
)

//go:embed mylib1.lua
var luaMylib1 string

//go:embed mylib2.lua
var luaMylib2 string

//go:embed mylib3.lua
var luaMylib3 string

type ListFuncResult struct {
	Name    string
	Library string
}

func decodeListFuncResult(t *testing.T, v interface{}) ListFuncResult {
	switch res := v.(type) {
	case []interface{}:
		require.EqualValues(t, 4, len(res))
		require.EqualValues(t, "function_name", res[0])
		require.EqualValues(t, "from_library", res[2])
		return ListFuncResult{
			Name:    res[1].(string),
			Library: res[3].(string),
		}
	case map[interface{}]interface{}:
		require.EqualValues(t, 2, len(res))
		return ListFuncResult{
			Name:    res["function_name"].(string),
			Library: res["from_library"].(string),
		}
	}
	require.Fail(t, "unexpected type")
	return ListFuncResult{}
}

type ListLibResult struct {
	Name      string
	Engine    string
	Functions []interface{}
}

func decodeListLibResult(t *testing.T, v interface{}) ListLibResult {
	switch res := v.(type) {
	case []interface{}:
		require.EqualValues(t, 6, len(res))
		require.EqualValues(t, "library_name", res[0])
		require.EqualValues(t, "engine", res[2])
		require.EqualValues(t, "functions", res[4])
		return ListLibResult{
			Name:      res[1].(string),
			Engine:    res[3].(string),
			Functions: res[5].([]interface{}),
		}
	case map[interface{}]interface{}:
		require.EqualValues(t, 3, len(res))
		return ListLibResult{
			Name:      res["library_name"].(string),
			Engine:    res["engine"].(string),
			Functions: res["functions"].([]interface{}),
		}
	}
	require.Fail(t, "unexpected type")
	return ListLibResult{}
}

func TestFunctions(t *testing.T) {
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
		testFunctions(t, configs)
	}
}

var testFunctions = func(t *testing.T, config util.KvrocksServerConfigs) {
	srv := util.StartServer(t, config)
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	t.Run("FUNCTION LOAD errors", func(t *testing.T) {
		code := strings.Join(strings.Split(luaMylib1, "\n")[1:], "\n")
		require.Error(t, rdb.Do(ctx, "FUNCTION", "LOAD", code).Err(), "ERR Missing library meta")

		code2 := "#!lua\n" + code
		require.Error(t, rdb.Do(ctx, "FUNCTION", "LOAD", code2).Err(), "ERR Library name was not given")

		code2 = "#!lua name=$$$\n" + code
		require.Error(t, rdb.Do(ctx, "FUNCTION", "LOAD", code2).Err(), "ERR Library names can only contain letters, numbers, or underscores(_) and must be at least one character long")
	})

	t.Run("FUNCTION LOAD and FCALL mylib1", func(t *testing.T) {
		util.ErrorRegexp(t, rdb.Do(ctx, "FCALL", "inc", 0, 1).Err(), ".*No such function name.*")
		require.NoError(t, rdb.Do(ctx, "FUNCTION", "LOAD", luaMylib1).Err())
		require.Equal(t, rdb.Do(ctx, "FCALL", "inc", 0, 1).Val(), int64(2))
		require.Equal(t, rdb.Do(ctx, "FCALL", "add", 0, 122, 111).Val(), int64(233))
	})

	t.Run("FUNCTION LIST and FUNCTION LISTFUNC mylib1", func(t *testing.T) {
		libraries, err := rdb.FunctionList(ctx, redis.FunctionListQuery{
			WithCode: true,
		}).Result()
		require.NoError(t, err)
		require.EqualValues(t, 1, len(libraries))
		require.Equal(t, "mylib1", libraries[0].Name)
		require.Equal(t, luaMylib1, libraries[0].Code)

		list := rdb.Do(ctx, "FUNCTION", "LISTFUNC").Val().([]interface{})
		require.EqualValues(t, 2, len(list))
		f1 := decodeListFuncResult(t, list[0])
		require.Equal(t, "add", f1.Name)
		require.Equal(t, "mylib1", f1.Library)
		f2 := decodeListFuncResult(t, list[1])
		require.Equal(t, "inc", f2.Name)
		require.Equal(t, "mylib1", f2.Library)
	})

	t.Run("FUNCTION LOAD and FCALL mylib2", func(t *testing.T) {
		util.ErrorRegexp(t, rdb.Do(ctx, "FCALL", "hello", 0, "x").Err(), ".*No such function name.*")
		require.NoError(t, rdb.Do(ctx, "FUNCTION", "LOAD", luaMylib2).Err())
		require.Equal(t, rdb.Do(ctx, "FCALL", "hello", 0, "x").Val(), "Hello, x!")
		require.Equal(t, rdb.Do(ctx, "FCALL", "reverse", 0, "abc").Val(), "cba")
		require.Equal(t, rdb.Do(ctx, "FCALL", "inc", 0, 2).Val(), int64(3))
	})

	t.Run("FUNCTION LIST and FUNCTION LISTFUNC mylib2", func(t *testing.T) {
		libraries, err := rdb.FunctionList(ctx, redis.FunctionListQuery{
			WithCode: true,
		}).Result()
		require.NoError(t, err)
		require.EqualValues(t, 2, len(libraries))

		list := rdb.Do(ctx, "FUNCTION", "LISTFUNC").Val().([]interface{})
		expected := []ListFuncResult{
			{Name: "add", Library: "mylib1"},
			{Name: "hello", Library: "mylib2"},
			{Name: "inc", Library: "mylib1"},
			{Name: "reverse", Library: "mylib2"},
		}
		require.EqualValues(t, len(expected), len(list))
		for i, f := range expected {
			actual := decodeListFuncResult(t, list[i])
			require.Equal(t, f.Name, actual.Name)
			require.Equal(t, f.Library, actual.Library)
		}
	})

	t.Run("FUNCTION DELETE", func(t *testing.T) {
		require.Equal(t, rdb.Do(ctx, "FCALL", "hello", 0, "yy").Val(), "Hello, yy!")
		require.NoError(t, rdb.Do(ctx, "FUNCTION", "DELETE", "mylib2").Err())
		util.ErrorRegexp(t, rdb.Do(ctx, "FCALL", "hello", 0, "x").Err(), ".*No such function name.*")
		util.ErrorRegexp(t, rdb.Do(ctx, "FCALL", "reverse", 0, "x").Err(), ".*No such function name.*")
		require.Equal(t, rdb.Do(ctx, "FCALL", "inc", 0, 3).Val(), int64(4))

		libraries, err := rdb.FunctionList(ctx, redis.FunctionListQuery{
			WithCode: true,
		}).Result()
		require.NoError(t, err)
		require.EqualValues(t, 1, len(libraries))
		require.Equal(t, "mylib1", libraries[0].Name)

		list := rdb.Do(ctx, "FUNCTION", "LISTFUNC").Val().([]interface{})
		expected := []ListFuncResult{
			{Name: "add", Library: "mylib1"},
			{Name: "inc", Library: "mylib1"},
		}
		require.EqualValues(t, len(expected), len(list))
		for i, f := range expected {
			actual := decodeListFuncResult(t, list[i])
			require.Equal(t, f.Name, actual.Name)
			require.Equal(t, f.Library, actual.Library)
		}
	})

	t.Run("FUNCTION LOAD REPLACE", func(t *testing.T) {
		code := strings.ReplaceAll(luaMylib2, "name=mylib2", "name=mylib1")
		util.ErrorRegexp(t, rdb.Do(ctx, "FUNCTION", "LOAD", code).Err(), ".*library already exists.*")
		require.NoError(t, rdb.Do(ctx, "FUNCTION", "LOAD", "REPLACE", code).Err())

		require.Equal(t, rdb.Do(ctx, "FCALL", "hello", 0, "xxx").Val(), "Hello, xxx!")
		require.Equal(t, rdb.Do(ctx, "FCALL", "reverse", 0, "xyz").Val(), "zyx")
		util.ErrorRegexp(t, rdb.Do(ctx, "FCALL", "inc", 0, 1).Err(), ".*No such function name.*")

		libraries, err := rdb.FunctionList(ctx, redis.FunctionListQuery{
			WithCode: true,
		}).Result()
		require.NoError(t, err)
		require.EqualValues(t, 1, len(libraries))
		require.Equal(t, "mylib1", libraries[0].Name)

		list := rdb.Do(ctx, "FUNCTION", "LISTFUNC").Val().([]interface{})
		expected := []ListFuncResult{
			{Name: "hello", Library: "mylib1"},
			{Name: "reverse", Library: "mylib1"},
		}
		require.EqualValues(t, len(expected), len(list))
		for i, f := range expected {
			actual := decodeListFuncResult(t, list[i])
			require.Equal(t, f.Name, actual.Name)
			require.Equal(t, f.Library, actual.Library)
		}
	})

	t.Run("FCALL_RO", func(t *testing.T) {
		require.NoError(t, rdb.Do(ctx, "FUNCTION", "LOAD", luaMylib3).Err())

		require.NoError(t, rdb.Set(ctx, "x", 1, 0).Err())
		require.Equal(t, rdb.Do(ctx, "FCALL", "myget", 1, "x").Val(), "1")
		require.Equal(t, rdb.Do(ctx, "FCALL_RO", "myget", 1, "x").Val(), "1")

		require.Equal(t, rdb.Do(ctx, "FCALL", "myset", 1, "x", 2).Val(), "OK")
		require.Equal(t, rdb.Get(ctx, "x").Val(), "2")

		util.ErrorRegexp(t, rdb.Do(ctx, "FCALL_RO", "myset", 1, "x", 3).Err(), ".*Write commands are not allowed.*")
	})

	t.Run("Restart server and test again", func(t *testing.T) {
		srv.Restart()

		require.Equal(t, rdb.Do(ctx, "FCALL", "myget", 1, "x").Val(), "2")
		require.Equal(t, rdb.Do(ctx, "FCALL", "hello", 0, "xxx").Val(), "Hello, xxx!")

		libraries, err := rdb.FunctionList(ctx, redis.FunctionListQuery{
			WithCode: true,
		}).Result()
		require.NoError(t, err)
		require.EqualValues(t, 2, len(libraries))
		require.Equal(t, libraries[0].Name, "mylib1")
		require.Equal(t, libraries[1].Name, "mylib3")
	})

	t.Run("FUNCTION LISTLIB", func(t *testing.T) {
		r := rdb.Do(ctx, "FUNCTION", "LISTLIB", "mylib1").Val()
		require.EqualValues(t, ListLibResult{
			Name: "mylib1", Engine: "lua", Functions: []interface{}{"hello", "reverse"},
		}, decodeListLibResult(t, r))

		r = rdb.Do(ctx, "FUNCTION", "LISTLIB", "mylib3").Val()
		require.EqualValues(t, ListLibResult{
			Name: "mylib3", Engine: "lua", Functions: []interface{}{"myget", "myset"},
		}, decodeListLibResult(t, r))
	})
}

func TestFunctionScriptFlags(t *testing.T) {
	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	t.Run("Function extract-libname-error", func(t *testing.T) {
		r := rdb.Do(ctx, "FUNCTION", "LOAD",
			`#!lua name=mylibname flags=no-writes
		redis.register_function('extract_libname_error_func', function(keys, args) end)`)
		util.ErrorRegexp(t, r.Err(), "ERR Unknown lua shebang option:*")

		r = rdb.Do(ctx, "FUNCTION", "LOAD",
			`#!lua flags=no-writes name=mylibname
		redis.register_function('extract_libname_error_func', function(keys, args) end)`)
		util.ErrorRegexp(t, r.Err(), "ERR Unknown lua shebang option:*")

		r = rdb.Do(ctx, "FUNCTION", "LOAD",
			`#!lua name=mylibname name=mylibname2
		redis.register_function('extract_libname_error_func', function(keys, args) end)`)
		util.ErrorRegexp(t, r.Err(), "Redundant library name in script shebang")

		r = rdb.Do(ctx, "FUNCTION", "LOAD",
			`#!errorenine name=mylibname
		redis.register_function('extract_libname_error_func', function(keys, args) end)`)
		util.ErrorRegexp(t, r.Err(), "ERR Unexpected engine in script shebang:*")

		r = rdb.Do(ctx, "FUNCTION", "LOAD",
			`#!luaname=mylibname
		redis.register_function('extract_libname_error_func', function(keys, args) end)`)
		util.ErrorRegexp(t, r.Err(), "ERR Unexpected engine in script shebang:*")

		r = rdb.Do(ctx, "FUNCTION", "LOAD",
			`#!lua xxxname=mylibname
		redis.register_function('extract_libname_error_func', function(keys, args) end)`)
		util.ErrorRegexp(t, r.Err(), "ERR Unknown lua shebang option:*")

		r = rdb.Do(ctx, "FUNCTION", "LOAD",
			`#!lua name=mylibname key=value
		redis.register_function('extract_libname_error_func', function(keys, args) end)`)
		util.ErrorRegexp(t, r.Err(), "ERR Unknown lua shebang option:*")
	})

	t.Run("Function extract-flags-error", func(t *testing.T) {
		r := rdb.Do(ctx, "FUNCTION", "LOAD",
			`#!lua name=myflags
		redis.register_function('extract_flags_error_func', function(keys, args) end, { 'invalid-flag' })`)
		require.Error(t, r.Err(), "ERR Error while running new function lib: Unknown flag given: invalid-flag")

		r = rdb.Do(ctx, "FUNCTION", "LOAD",
			`#!lua name=myflags
		redis.register_function('extract_flags_error_func', function(keys, args) end, { 'no-writes', 'invalid-flag' })`)
		require.Error(t, r.Err(), "ERR Error while running new function lib: Unknown flag given: invalid-flag")

		r = rdb.Do(ctx, "FUNCTION", "LOAD",
			`#!lua name=myflags
		redis.register_function('extract_flags_error_func', function(keys, args) end, { {} }`)
		require.Error(t, r.Err(), "ERR Expects a valid flags argument to register_function, e.g. flags={ 'no-writes' })")

		r = rdb.Do(ctx, "FUNCTION", "LOAD",
			`#!lua name=myflags
		redis.register_function('extract_flags_error_func', function(keys, args) end, { 123 }`)
		require.Error(t, r.Err(), "ERR Expects a valid flags argument to register_function, e.g. flags={ 'no-writes' })")

		r = rdb.Do(ctx, "FUNCTION", "LOAD",
			`#!lua name=myflags
		redis.register_function('extract_flags_error_func', function(keys, args) end, 'no-writes'`)
		require.Error(t, r.Err(), "ERR Expects a valid flags argument to register_function, e.g. flags={ 'no-writes' })")
	})

	t.Run("no-writes", func(t *testing.T) {
		r := rdb.Do(ctx, "FUNCTION", "LOAD",
			`#!lua name=nowriteslib
		redis.register_function('default_flag_func', function(keys, args) return redis.call("set", keys[1], args[1]) end)
		redis.register_function('no_writes_func', function(keys, args) return redis.call("set", keys[1], args[1]) end, { 'no-writes' })`)
		require.NoError(t, r.Err())

		r = rdb.Do(ctx, "FCALL", "default_flag_func", 1, "k1", "v1")
		require.NoError(t, r.Err())
		r = rdb.Do(ctx, "FCALL", "no_writes_func", 1, "k2", "v2")
		util.ErrorRegexp(t, r.Err(), "ERR .* Write commands are not allowed from read-only scripts")

		r = rdb.Do(ctx, "FCALL_RO", "default_flag_func", 1, "k1", "v1")
		util.ErrorRegexp(t, r.Err(), "ERR .* Write commands are not allowed from read-only scripts")
		r = rdb.Do(ctx, "FCALL_RO", "no_writes_func", 1, "k2", "v2")
		util.ErrorRegexp(t, r.Err(), "ERR .* Write commands are not allowed from read-only scripts")
	})

	srv0 := util.StartServer(t, map[string]string{"cluster-enabled": "yes"})
	rdb0 := srv0.NewClient()
	defer func() { require.NoError(t, rdb0.Close()) }()
	defer func() { srv0.Close() }()
	id0 := "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx00"
	require.NoError(t, rdb0.Do(ctx, "clusterx", "SETNODEID", id0).Err())

	srv1 := util.StartServer(t, map[string]string{"cluster-enabled": "yes"})
	srv1Alive := true
	defer func() {
		if srv1Alive {
			srv1.Close()
		}
	}()

	rdb1 := srv1.NewClient()
	defer func() { require.NoError(t, rdb1.Close()) }()
	id1 := "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx01"
	require.NoError(t, rdb1.Do(ctx, "clusterx", "SETNODEID", id1).Err())

	clusterNodes := fmt.Sprintf("%s %s %d master - 0-10000\n", id0, srv0.Host(), srv0.Port())
	clusterNodes += fmt.Sprintf("%s %s %d master - 10001-16383", id1, srv1.Host(), srv1.Port())
	require.NoError(t, rdb0.Do(ctx, "clusterx", "SETNODES", clusterNodes, "1").Err())
	require.NoError(t, rdb1.Do(ctx, "clusterx", "SETNODES", clusterNodes, "1").Err())

	// Node0: bar-slot = 5061, test-slot = 6918
	// Node1: foo-slot = 12182
	// Different slots of different nodes are not affected by allow-cross-slot-keys,
	// and different slots of the same node can be allowed
	require.NoError(t, rdb0.Set(ctx, "bar", "bar_value", 0).Err())
	require.NoError(t, rdb0.Set(ctx, "test", "test_value", 0).Err())
	require.NoError(t, rdb1.Set(ctx, "foo", "foo_value", 0).Err())

	t.Run("no-cluster", func(t *testing.T) {
		r := rdb0.Do(ctx, "FUNCTION", "LOAD",
			`#!lua name=noclusterlib
		redis.register_function('default_flag_func', function(keys) return redis.call('get', keys[1]) end)
		redis.register_function('no_cluster_func', function(keys) return redis.call('get', keys[1]) end, { 'no-cluster' })`)
		require.NoError(t, r.Err())

		require.NoError(t, rdb0.Do(ctx, "FCALL", "default_flag_func", 1, "bar").Err())

		r = rdb0.Do(ctx, "FCALL", "no_cluster_func", 1, "bar")
		util.ErrorRegexp(t, r.Err(), "ERR .* Can not run script on cluster, 'no-cluster' flag is set")

		// Only valid in cluster mode
		require.NoError(t, rdb.Set(ctx, "bar", "rdb_bar_value", 0).Err())
		r = rdb.Do(ctx, "FUNCTION", "LOAD",
			`#!lua name=noclusterlib
		redis.register_function('no_cluster_func', function(keys) return redis.call('get', keys[1]) end, { 'no-cluster' })`)
		require.NoError(t, r.Err())
		require.NoError(t, rdb.Do(ctx, "FCALL", "no_cluster_func", 1, "bar").Err())
	})

	t.Run("allow-cross-slot-keys", func(t *testing.T) {
		r := rdb0.Do(ctx, "FUNCTION", "LOAD",
			`#!lua name=crossslotlib
		redis.register_function('default_flag_func_1', 
		function() 
			redis.call('get', 'bar')
			return redis.call('get', 'test') 
		end
		)

		redis.register_function('default_flag_func_2', 
		function() 
			redis.call('get', 'bar')
			return redis.call('get', 'foo') 
		end
		)

		redis.register_function('default_flag_func_3', 
		function(keys) 
			redis.call('get', keys[1])
			return redis.call('get', keys[2]) 
		end
		)

		redis.register_function(
		'allow_cross_slot_keys_func_1', 
		function() 
			redis.call('get', 'bar')
			return redis.call('get', 'test') 
		end,
		{ 'allow-cross-slot-keys' })
		 
		redis.register_function(
		'allow_cross_slot_keys_func_2', 
		function() 
			redis.call('get', 'bar')
			return redis.call('get', 'foo') 
		end,
		{ 'allow-cross-slot-keys' })
		 
		redis.register_function(
		'allow_cross_slot_keys_func_3', 
		function(keys) 
			redis.call('get', key[1])
			return redis.call('get', key[2]) 
		end,
		{ 'allow-cross-slot-keys' })
		
		`)
		require.NoError(t, r.Err())

		r = rdb0.Do(ctx, "FCALL", "default_flag_func_1", 0)
		util.ErrorRegexp(t, r.Err(), "ERR .* Script attempted to access keys that do not hash to the same slot")

		r = rdb0.Do(ctx, "FCALL", "default_flag_func_2", 0)
		util.ErrorRegexp(t, r.Err(), "ERR .* Script attempted to access a non local key in a cluster node script")

		r = rdb0.Do(ctx, "FCALL", "default_flag_func_3", 2, "bar", "test")
		require.EqualError(t, r.Err(), "CROSSSLOT Attempted to access keys that don't hash to the same slot")

		r = rdb0.Do(ctx, "FCALL", "allow_cross_slot_keys_func_1", 0)
		require.NoError(t, r.Err())

		r = rdb0.Do(ctx, "FCALL", "allow_cross_slot_keys_func_2", 0)
		util.ErrorRegexp(t, r.Err(), "ERR .* Script attempted to access a non local key in a cluster node script")

		// Pre-declared keys are not affected by allow-cross-slot-keys
		r = rdb0.Do(ctx, "FCALL", "allow_cross_slot_keys_func_3", 2, "bar", "test")
		require.EqualError(t, r.Err(), "CROSSSLOT Attempted to access keys that don't hash to the same slot")
	})

	t.Run("mixed-use", func(t *testing.T) {
		r := rdb0.Do(ctx, "FUNCTION", "LOAD",
			`#!lua name=mixeduselib
		redis.register_function('no_write_cluster_func_1', function()  redis.call('get', 'bar') end, { 'no-writes', 'no-cluster' })
		
		redis.register_function('no_write_allow_cross_func_1', 
		function() redis.call('get', 'bar'); return redis.call('get', 'test'); end, 
		{ 'no-writes', 'allow-cross-slot-keys' })

		redis.register_function('no_write_allow_cross_func_2', 
		function() redis.call('set', 'bar'); return redis.call('set', 'test'); end, 
		{ 'no-writes', 'allow-cross-slot-keys' })

		redis.register_function('no_write_allow_cross_func_3', 
		function() redis.call('get', 'bar'); return redis.call('get', 'foo'); end, 
		{ 'no-writes', 'allow-cross-slot-keys' })
		`)
		require.NoError(t, r.Err())

		r = rdb0.Do(ctx, "FCALL", "no_write_cluster_func_1", 0)
		util.ErrorRegexp(t, r.Err(), "ERR .* Can not run script on cluster, 'no-cluster' flag is set")

		// no-cluster Only valid in cluster mode
		r = rdb.Do(ctx, "FUNCTION", "LOAD",
			`#!lua name=mixeduselib2
		redis.register_function('no_write_cluster_func_2', 
		function()  return redis.call('set', 'bar', 'bar_value') end, 
		{ 'no-writes', 'no-cluster' }
		 )

		redis.register_function('no_write_cluster_func_3', 
		function()  return redis.call('get', 'bar') end, 
		{ 'no-writes', 'no-cluster' }
		 )
		`)
		require.NoError(t, r.Err())

		r = rdb.Do(ctx, "FCALL", "no_write_cluster_func_2", 0)
		util.ErrorRegexp(t, r.Err(), "ERR .* Write commands are not allowed from read-only scripts")

		require.NoError(t, rdb.Set(ctx, "bar", "bar_value_rdb", 0).Err())
		require.NoError(t, rdb.Do(ctx, "FCALL", "no_write_cluster_func_3", 0).Err())

		require.NoError(t, rdb0.Do(ctx, "FCALL", "no_write_allow_cross_func_1", 0).Err())
		r = rdb0.Do(ctx, "FCALL", "no_write_allow_cross_func_2", 0)
		util.ErrorRegexp(t, r.Err(), "ERR .* Write commands are not allowed from read-only scripts")
		r = rdb0.Do(ctx, "FCALL", "no_write_allow_cross_func_3", 0)
		util.ErrorRegexp(t, r.Err(), "ERR .* Script attempted to access a non local key in a cluster node script")
	})
}
