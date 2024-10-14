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

package rsid

import (
	"context"
	"testing"
	"time"

	"github.com/apache/kvrocks/tests/gocase/util"
	"github.com/stretchr/testify/require"
)

func TestRSIDMasterAndReplicaYes(t *testing.T) {
	ctx := context.Background()

	srvA := util.StartServer(t, map[string]string{"use-rsid-psync": "yes"})
	defer func() { srvA.Close() }()
	rdbA := srvA.NewClient()
	defer func() { require.NoError(t, rdbA.Close()) }()
	require.NoError(t, rdbA.Set(ctx, "a", "b", 0).Err())

	srvB := util.StartServer(t, map[string]string{"use-rsid-psync": "yes"})
	defer func() { srvB.Close() }()
	rdbB := srvB.NewClient()
	defer func() { require.NoError(t, rdbB.Close()) }()
	require.NoError(t, rdbB.Set(ctx, "c", "d", 0).Err())

	t.Run("replica psync sequence is in the range the master wal but full sync", func(t *testing.T) {
		require.Equal(t, util.FindInfoEntry(rdbA, "master_repl_offset"), util.FindInfoEntry(rdbB, "master_repl_offset"))
		util.SlaveOf(t, rdbA, srvB)
		util.WaitForSync(t, rdbA)
		require.Equal(t, "1", util.FindInfoEntry(rdbB, "sync_full"))
		require.Equal(t, "1", util.FindInfoEntry(rdbB, "sync_partial_ok"))
	})

	// A -->->-- B
	// C -->->-- B
	srvC := util.StartServer(t, map[string]string{"use-rsid-psync": "yes"})
	defer func() { srvC.Close() }()
	rdbC := srvC.NewClient()
	defer func() { require.NoError(t, rdbC.Close()) }()
	util.SlaveOf(t, rdbC, srvB)
	util.WaitForSync(t, rdbC)

	t.Run("chained replication can partially re-sync", func(t *testing.T) {
		// C never sync with any slave
		require.Equal(t, "0", util.FindInfoEntry(rdbC, "sync_partial_ok"))

		// A -->>-- C, currently topology is A -->>-- C -->>-- B
		util.SlaveOf(t, rdbA, srvC)
		util.WaitForSync(t, rdbA)

		require.Equal(t, "0", util.FindInfoEntry(rdbC, "sync_full"))
		require.Equal(t, "1", util.FindInfoEntry(rdbC, "sync_partial_ok"))
	})

	t.Run("chained replication can propagate updates", func(t *testing.T) {
		require.NoError(t, rdbB.Set(ctx, "master", "B", 0).Err())
		util.WaitForOffsetSync(t, rdbB, rdbA, 5*time.Second)
		require.Equal(t, "B", rdbA.Get(ctx, "master").Val())
	})

	t.Run("replica can partially re-sync after changing master but having the same history", func(t *testing.T) {
		require.NoError(t, rdbA.SlaveOf(ctx, "127.0.0.1", "1025").Err())
		time.Sleep(time.Second)

		// now topology is:
		// A -->->-- B
		// C -->->-- B
		util.SlaveOf(t, rdbA, srvB)
		util.WaitForSync(t, rdbA)

		// only partial sync, no full sync
		require.Equal(t, "2", util.FindInfoEntry(rdbB, "sync_full"))
		require.Equal(t, "3", util.FindInfoEntry(rdbB, "sync_partial_ok"))
	})

	t.Run("replica can partially re-sync again after restarting", func(t *testing.T) {
		require.NoError(t, rdbC.ConfigRewrite(ctx).Err())

		srvC.Restart()
		rdbC := srvC.NewClient()
		defer func() { require.NoError(t, rdbC.Close()) }()
		util.WaitForSync(t, rdbC)

		// do not increase sync_full, but increase sync_partial_ok
		require.Equal(t, "2", util.FindInfoEntry(rdbB, "sync_full"))
		require.Equal(t, "4", util.FindInfoEntry(rdbB, "sync_partial_ok"))
	})
}

func TestRSIDMasterNoAndReplicaYes(t *testing.T) {
	replica := util.StartServer(t, map[string]string{"use-rsid-psync": "yes"})
	defer func() { replica.Close() }()
	replicaClient := replica.NewClient()
	defer func() { require.NoError(t, replicaClient.Close()) }()

	srv := util.StartServer(t, map[string]string{"use-rsid-psync": "no"})
	defer func() { srv.Close() }()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	t.Run("Replica (use-rsid-psync yes) can slaveof the master (use-rsid-psync no)", func(t *testing.T) {
		util.SlaveOf(t, replicaClient, srv)
		util.WaitForSync(t, replicaClient)
		require.Equal(t, "1", util.FindInfoEntry(rdb, "sync_full"))
		require.Equal(t, "1", util.FindInfoEntry(rdb, "sync_partial_ok"))
	})
}

func TestRSIDMasterYesAndReplicaNo(t *testing.T) {
	replica := util.StartServer(t, map[string]string{"use-rsid-psync": "no"})
	defer func() { replica.Close() }()
	replicaClient := replica.NewClient()
	defer func() { require.NoError(t, replicaClient.Close()) }()

	srv := util.StartServer(t, map[string]string{"use-rsid-psync": "yes"})
	defer func() { srv.Close() }()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	t.Run("Replica (use-rsid-psync no) can slaveof the master (use-rsid-psync yes)", func(t *testing.T) {
		util.SlaveOf(t, replicaClient, srv)
		util.WaitForSync(t, replicaClient)
		require.Equal(t, "0", util.FindInfoEntry(rdb, "sync_full"))
		require.Equal(t, "1", util.FindInfoEntry(rdb, "sync_partial_ok"))
	})
}
