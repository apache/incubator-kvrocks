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

package protocol

import (
	"context"
	"fmt"
	"testing"

	"github.com/apache/kvrocks/tests/gocase/util"
	"github.com/stretchr/testify/require"
)

func TestRegression(t *testing.T) {
	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	c := srv.NewTCPClient()
	defer func() { require.NoError(t, c.Close()) }()

	proto := "*3\r\n$5\r\nBLPOP\r\n$6\r\nhandle\r\n$1\r\n0\r\n"
	require.NoError(t, c.Write(fmt.Sprintf("%s%s", proto, proto)))
	resList := []string{"*2", "$6", "handle", "$1", "a"}

	v := rdb.RPush(ctx, "handle", "a")
	require.EqualValues(t, 1, v.Val())
	for _, res := range resList {
		c.MustRead(t, res)
	}

	v = rdb.RPush(ctx, "handle", "a")
	require.EqualValues(t, 1, v.Val())

	for _, res := range resList {
		c.MustRead(t, res)
	}
}
