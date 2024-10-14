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

package namespace

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/kvrocks/tests/gocase/util"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestNamespace(t *testing.T) {
	password := "pwd"
	srv := util.StartServer(t, map[string]string{
		"requirepass": password,
	})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClientWithOption(&redis.Options{
		Password: password,
	})
	defer func() { require.NoError(t, rdb.Close()) }()

	t.Run("Basic operation", func(t *testing.T) {
		nsTokens := map[string]string{
			"ns1": "token1",
			"ns2": "token2",
			"ns3": "token3",
			"ns4": "token4",
		}
		for ns, token := range nsTokens {
			r := rdb.Do(ctx, "NAMESPACE", "ADD", ns, token)
			require.NoError(t, r.Err())
			require.Equal(t, "OK", r.Val())
		}
		// duplicate add the same namespace
		for ns, token := range nsTokens {
			r := rdb.Do(ctx, "NAMESPACE", "ADD", ns, "new"+token)
			util.ErrorRegexp(t, r.Err(), ".*ERR the namespace already exists.*")
		}
		for ns, token := range nsTokens {
			r := rdb.Do(ctx, "NAMESPACE", "GET", ns)
			require.NoError(t, r.Err())
			require.Equal(t, token, r.Val())
		}
		for ns := range nsTokens {
			r := rdb.Do(ctx, "NAMESPACE", "DEL", ns)
			require.NoError(t, r.Err())
			require.Equal(t, "OK", r.Val())
		}
	})

	t.Run("Namespace exists after restart", func(t *testing.T) {
		for _, enableNamespaceReplication := range []string{"no", "yes"} {
			require.NoError(t, rdb.ConfigSet(ctx,
				"repl-namespace-enabled",
				enableNamespaceReplication,
			).Err())
			require.NoError(t, rdb.ConfigRewrite(ctx).Err())
			nsTokens := map[string]string{
				"n1": "t1",
				"n2": "t2",
				"n3": "t3",
				"n4": "t4",
			}
			for ns, token := range nsTokens {
				r := rdb.Do(ctx, "NAMESPACE", "ADD", ns, token)
				require.NoError(t, r.Err())
				require.Equal(t, "OK", r.Val())
			}
			for ns, token := range nsTokens {
				r := rdb.Do(ctx, "NAMESPACE", "GET", ns)
				require.NoError(t, r.Err())
				require.Equal(t, token, r.Val())
			}

			srv.Restart()
			for ns, token := range nsTokens {
				r := rdb.Do(ctx, "NAMESPACE", "GET", ns)
				require.NoError(t, r.Err())
				require.Equal(t, token, r.Val())
			}
			for ns := range nsTokens {
				r := rdb.Do(ctx, "NAMESPACE", "DEL", ns)
				require.NoError(t, r.Err())
				require.Equal(t, "OK", r.Val())
			}
		}
	})

	t.Run("Concurrent creating namespaces", func(t *testing.T) {
		threads := 4
		countPerThread := 10

		var wg sync.WaitGroup
		var nsTokens sync.Map
		for i := 0; i < threads; i++ {
			wg.Add(1)
			go func(index int) {
				defer wg.Done()
				for j := 0; j < countPerThread; j++ {
					ns := "ns" + util.RandString(16, 16, util.Alpha)
					token := util.RandString(16, 16, util.Alpha)
					nsTokens.Store(ns, token)
					r := rdb.Do(ctx, "NAMESPACE", "ADD", ns, token)
					require.NoError(t, r.Err())
					require.Equal(t, "OK", r.Val())
				}
			}(i)
		}
		wg.Wait()

		nsTokens.Range(func(key, value interface{}) bool {
			r := rdb.Do(ctx, "NAMESPACE", "GET", key)
			require.NoError(t, r.Err())
			require.Equal(t, value, r.Val())
			return true
		})
	})
}

func TestNamespaceReplicate(t *testing.T) {
	password := "pwd"
	masterSrv := util.StartServer(t, map[string]string{
		"requirepass": password,
	})
	defer masterSrv.Close()
	masterRdb := masterSrv.NewClientWithOption(&redis.Options{
		Password: password,
	})
	defer func() { require.NoError(t, masterRdb.Close()) }()

	slaveSrv := util.StartServer(t, map[string]string{
		"masterauth":  password,
		"requirepass": password,
	})
	defer slaveSrv.Close()
	slaveRdb := slaveSrv.NewClientWithOption(&redis.Options{
		Password: password,
	})
	defer func() { require.NoError(t, slaveRdb.Close()) }()

	util.SlaveOf(t, slaveRdb, masterSrv)
	util.WaitForSync(t, slaveRdb)

	ctx := context.Background()
	nsTokens := map[string]string{
		"n1": "t1",
		"n2": "t2",
		"n3": "t3",
		"n4": "t4",
	}

	t.Run("Disable Replicate namespces", func(t *testing.T) {
		require.NoError(t, masterRdb.ConfigSet(ctx, "repl-namespace-enabled", "no").Err())
		require.NoError(t, slaveRdb.ConfigSet(ctx, "repl-namespace-enabled", "no").Err())

		for ns, token := range nsTokens {
			r := masterRdb.Do(ctx, "NAMESPACE", "ADD", ns, token)
			require.NoError(t, r.Err())
			require.Equal(t, "OK", r.Val())
		}
		util.WaitForOffsetSync(t, slaveRdb, masterRdb, 5*time.Second)

		// Can read namespaces on master
		for ns, token := range nsTokens {
			r := masterRdb.Do(ctx, "NAMESPACE", "GET", ns)
			require.NoError(t, r.Err())
			require.Equal(t, token, r.Val())
		}
		// Can't read namespaces on slave
		for ns := range nsTokens {
			r := slaveRdb.Do(ctx, "NAMESPACE", "GET", ns)
			require.EqualError(t, r.Err(), redis.Nil.Error())
		}

		for ns := range nsTokens {
			r := masterRdb.Do(ctx, "NAMESPACE", "DEL", ns)
			require.NoError(t, r.Err())
			require.Equal(t, "OK", r.Val())
		}
	})

	t.Run("Enable Replicate namespaces", func(t *testing.T) {
		require.NoError(t, masterRdb.ConfigSet(ctx, "repl-namespace-enabled", "yes").Err())
		require.NoError(t, slaveRdb.ConfigSet(ctx, "repl-namespace-enabled", "yes").Err())

		for ns, token := range nsTokens {
			r := masterRdb.Do(ctx, "NAMESPACE", "ADD", ns, token)
			require.NoError(t, r.Err())
			require.Equal(t, "OK", r.Val())
		}
		util.WaitForOffsetSync(t, slaveRdb, masterRdb, 5*time.Second)

		for ns, token := range nsTokens {
			r := slaveRdb.Do(ctx, "NAMESPACE", "GET", ns)
			require.NoError(t, r.Err())
			require.Equal(t, token, r.Val())
		}

		for ns := range nsTokens {
			r := masterRdb.Do(ctx, "NAMESPACE", "DEL", ns)
			require.NoError(t, r.Err())
			require.Equal(t, "OK", r.Val())
		}
		util.WaitForOffsetSync(t, slaveRdb, masterRdb, 5*time.Second)

		for ns := range nsTokens {
			r := slaveRdb.Do(ctx, "NAMESPACE", "GET", ns)
			require.EqualError(t, r.Err(), redis.Nil.Error())
		}
	})

	t.Run("Don't allow to operate slave's namespace if replication is enabled", func(t *testing.T) {
		r := slaveRdb.Do(ctx, "NAMESPACE", "ADD", "ns_xxxx", "token_xxxx")
		util.ErrorRegexp(t, r.Err(), ".*ERR namespace is read-only for slave.*")
	})

	t.Run("Turn off namespace replication is not allowed", func(t *testing.T) {
		r := masterRdb.Do(ctx, "NAMESPACE", "ADD", "test-ns", "ns-token")
		require.NoError(t, r.Err())
		require.Equal(t, "OK", r.Val())
		util.ErrorRegexp(t, masterRdb.ConfigSet(ctx, "repl-namespace-enabled", "no").Err(), ".*cannot switch off repl_namespace_enabled when namespaces exist in db.*")

		// it should be allowed after deleting all namespaces
		r = masterRdb.Do(ctx, "NAMESPACE", "DEL", "test-ns")
		require.NoError(t, r.Err())
		require.Equal(t, "OK", r.Val())
		require.NoError(t, masterRdb.ConfigSet(ctx, "repl-namespace-enabled", "no").Err())
	})
}

func TestNamespaceReplicateWithFullSync(t *testing.T) {
	config := map[string]string{
		"rocksdb.write_buffer_size":       "4",
		"rocksdb.target_file_size_base":   "16",
		"rocksdb.max_write_buffer_number": "1",
		"rocksdb.wal_ttl_seconds":         "0",
		"rocksdb.wal_size_limit_mb":       "0",
		"repl-namespace-enabled":          "yes",
		"requirepass":                     "123",
		"masterauth":                      "123",
	}
	master := util.StartServer(t, config)
	defer master.Close()
	masterClient := master.NewClientWithOption(&redis.Options{Password: "123"})
	defer func() { require.NoError(t, masterClient.Close()) }()

	slave := util.StartServer(t, config)
	defer slave.Close()
	slaveClient := slave.NewClientWithOption(&redis.Options{Password: "123"})
	defer func() { require.NoError(t, slaveClient.Close()) }()

	ctx := context.Background()
	value := strings.Repeat("a", 128*1024)
	for i := 0; i < 1024; i++ {
		require.NoError(t, masterClient.Set(ctx, fmt.Sprintf("key%d", i), value, 0).Err())
	}
	require.NoError(t, masterClient.Do(ctx, "NAMESPACE", "ADD", "foo", "bar").Err())

	util.SlaveOf(t, slaveClient, master)
	util.WaitForOffsetSync(t, masterClient, slaveClient, 60*time.Second)

	// Namespaces should be replicated after the full sync
	require.Eventually(t, func() bool {
		token, _ := slaveClient.Do(ctx, "NAMESPACE", "GET", "foo").Val().(string)
		return token == "bar"
	}, 5*time.Second, 100*time.Millisecond)
}

func TestNamespaceRewrite(t *testing.T) {
	password := "pwd"
	srv := util.StartServerWithCLIOptions(t, false, map[string]string{
		"requirepass": password,
	}, []string{})
	defer srv.Close()

	rdb := srv.NewClientWithOption(&redis.Options{
		Password: password,
	})
	defer func() { require.NoError(t, rdb.Close()) }()
	ctx := context.Background()

	t.Run("Cannot modify namespace if running without the configuration", func(t *testing.T) {
		errMsg := ".*ERR modify namespace requires the server is running with a configuration file or enabled namespace replication.*"
		r := rdb.Do(ctx, "NAMESPACE", "ADD", "ns1", "token1")
		util.ErrorRegexp(t, r.Err(), errMsg)
		r = rdb.Do(ctx, "NAMESPACE", "SET", "ns1", "token1")
		util.ErrorRegexp(t, r.Err(), errMsg)
		r = rdb.Do(ctx, "NAMESPACE", "DEL", "ns1")
		util.ErrorRegexp(t, r.Err(), errMsg)

		// Good to go after enabling namespace replication
		require.NoError(t, rdb.ConfigSet(ctx, "repl-namespace-enabled", "yes").Err())
		require.NoError(t, rdb.Do(ctx, "NAMESPACE", "ADD", "ns1", "token1").Err())
		require.NoError(t, rdb.Do(ctx, "NAMESPACE", "SET", "ns1", "token2").Err())
		require.NoError(t, rdb.Do(ctx, "NAMESPACE", "DEL", "ns1").Err())
	})
}
