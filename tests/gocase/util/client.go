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

package util

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func FindInfoEntry(rdb *redis.Client, key string, section ...string) string {
	r := rdb.Info(context.Background(), section...)
	p := regexp.MustCompile(fmt.Sprintf("%s:(.+)", key))
	ms := p.FindStringSubmatch(r.Val())
	if len(ms) != 2 {
		return ""
	}
	return strings.TrimSpace(ms[1])
}

func WaitForSync(t testing.TB, slave *redis.Client) {
	require.Eventually(t, func() bool {
		r := FindInfoEntry(slave, "master_link_status")
		return r == "up"
	}, 5*time.Second, 100*time.Millisecond)
}

func WaitForOffsetSync(t testing.TB, master, slave *redis.Client, waitFor time.Duration) {
	require.Eventually(t, func() bool {
		o1 := FindInfoEntry(master, "master_repl_offset")
		o2 := FindInfoEntry(slave, "master_repl_offset")
		return o1 == o2
	}, waitFor, 100*time.Millisecond)
}

func SlaveOf(t testing.TB, slave *redis.Client, master *KvrocksServer) {
	require.NoError(t, slave.SlaveOf(context.Background(), master.Host(), fmt.Sprintf("%d", master.Port())).Err())
}

func Populate(t testing.TB, rdb *redis.Client, prefix string, n, size int) {
	ctx := context.Background()
	p := rdb.Pipeline()

	for i := 0; i < n; i++ {
		p.Do(ctx, "SET", fmt.Sprintf("%s%d", prefix, i), strings.Repeat("A", size))
	}

	_, err := p.Exec(ctx)
	require.NoError(t, err)
}
