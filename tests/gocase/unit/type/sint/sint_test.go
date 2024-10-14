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

package sint

import (
	"context"
	"testing"

	"github.com/apache/kvrocks/tests/gocase/util"
	"github.com/stretchr/testify/require"
)

func TestString(t *testing.T) {
	configOptions := []util.ConfigOptions{
		{
			Name:       "txn-context-enabled",
			Options:    []string{"yes", "no"},
			ConfigType: util.YesNo,
		},
	}

	configsMatrix, err := util.GenerateConfigsMatrix(configOptions)
	require.NoError(t, err)

	for _, configs := range configsMatrix {
		testSint(t, configs)
	}
}

func testSint(t *testing.T, configs util.KvrocksServerConfigs) {
	srv := util.StartServer(t, configs)
	defer srv.Close()
	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	t.Run("sorted-int", func(t *testing.T) {
		require.EqualValues(t, 1, rdb.Do(ctx, "SIADD", "mysi", 1).Val())
		require.EqualValues(t, 1, rdb.Do(ctx, "SIADD", "mysi", 2).Val())
		require.EqualValues(t, 0, rdb.Do(ctx, "SIADD", "mysi", 2).Val())
		require.EqualValues(t, 5, rdb.Do(ctx, "SIADD", "mysi", 3, 4, 5, 123, 245).Val())
		require.EqualValues(t, 7, rdb.Do(ctx, "SICARD", "mysi").Val())
		require.EqualValues(t, []interface{}{"245", "123", "5"}, rdb.Do(ctx, "SIREVRANGE", "mysi", 0, 3).Val())
		require.EqualValues(t, []interface{}{"4", "3", "2"}, rdb.Do(ctx, "SIREVRANGE", "mysi", 0, 3, "cursor", 5).Val())
		require.EqualValues(t, []interface{}{"245"}, rdb.Do(ctx, "SIRANGE", "mysi", 0, 3, "cursor", 123).Val())
		require.EqualValues(t, []interface{}{"1", "2", "3", "4"}, rdb.Do(ctx, "SIRANGEBYVALUE", "mysi", 1, "(5").Val())
		require.EqualValues(t, []interface{}{"5", "4", "3", "2"}, rdb.Do(ctx, "SIREVRANGEBYVALUE", "mysi", 5, "(1").Val())
		require.EqualValues(t, []interface{}{int64(1), int64(0), int64(1)}, rdb.Do(ctx, "SIEXISTS", "mysi", 1, 88, 2).Val())
		require.EqualValues(t, 1, rdb.Do(ctx, "SIREM", "mysi", 2).Val())
	})

}
