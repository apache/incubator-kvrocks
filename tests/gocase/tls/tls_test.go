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

package tls

import (
	"context"
	"crypto/tls"
	"fmt"
	"testing"
	"time"

	"github.com/apache/kvrocks/tests/gocase/util"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestTLS(t *testing.T) {
	if !util.TLSEnable() {
		t.Skip("TLS tests run only if tls enabled.")
	}

	ctx := context.Background()

	srv := util.StartTLSServer(t, map[string]string{})
	defer srv.Close()

	defaultTLSConfig, err := util.DefaultTLSConfig()
	require.NoError(t, err)

	rdb := srv.NewClientWithOption(&redis.Options{TLSConfig: defaultTLSConfig, Addr: srv.TLSAddr()})
	defer func() { require.NoError(t, rdb.Close()) }()

	doWithTLSClient := func(tlsConfig *tls.Config, f func(c *redis.Client)) {
		c := srv.NewClientWithOption(&redis.Options{TLSConfig: tlsConfig, Addr: srv.TLSAddr()})
		defer func() { require.NoError(t, c.Close()) }()
		f(c)
	}

	t.Run("TLS: Not accepting non-TLS connections on a TLS port", func(t *testing.T) {
		c := srv.NewClientWithOption(&redis.Options{Addr: srv.TLSAddr()})
		defer func() { require.NoError(t, c.Close()) }()
		require.Error(t, c.Ping(ctx).Err())
	})

	t.Run("TLS: Verify tls-auth-clients behaves as expected", func(t *testing.T) {
		tlsConfig := &tls.Config{InsecureSkipVerify: true}
		doWithTLSClient(tlsConfig, func(c *redis.Client) { require.Error(t, c.Ping(ctx).Err()) })

		require.NoError(t, rdb.ConfigSet(ctx, "tls-auth-clients", "no").Err())
		doWithTLSClient(tlsConfig, func(c *redis.Client) { require.Equal(t, "PONG", c.Ping(ctx).Val()) })

		require.NoError(t, rdb.ConfigSet(ctx, "tls-auth-clients", "optional").Err())
		doWithTLSClient(tlsConfig, func(c *redis.Client) { require.Equal(t, "PONG", c.Ping(ctx).Val()) })

		require.NoError(t, rdb.ConfigSet(ctx, "tls-auth-clients", "yes").Err())
		doWithTLSClient(tlsConfig, func(c *redis.Client) { require.Error(t, c.Ping(ctx).Err()) })
	})

	t.Run("TLS: Verify tls-protocols behaves as expected", func(t *testing.T) {
		require.NoError(t, rdb.ConfigSet(ctx, "tls-protocols", "TLSv1.2").Err())

		tlsConfig, err := util.DefaultTLSConfig()
		require.NoError(t, err)

		tlsConfig.MaxVersion = tls.VersionTLS11
		doWithTLSClient(tlsConfig, func(c *redis.Client) { require.Error(t, c.Ping(ctx).Err()) })

		tlsConfig.MaxVersion = tls.VersionTLS12
		doWithTLSClient(tlsConfig, func(c *redis.Client) { require.Equal(t, "PONG", c.Ping(ctx).Val()) })
	})

	t.Run("TLS: Verify tls-ciphers behaves as expected", func(t *testing.T) {
		require.NoError(t, rdb.ConfigSet(ctx, "tls-protocols", "TLSv1.2").Err())
		require.NoError(t, rdb.ConfigSet(ctx, "tls-ciphers", "DEFAULT:-AES128-SHA256").Err())

		tlsConfig, err := util.DefaultTLSConfig()
		require.NoError(t, err)

		tlsConfig.CipherSuites = []uint16{tls.TLS_RSA_WITH_AES_128_CBC_SHA256}
		doWithTLSClient(tlsConfig, func(c *redis.Client) { require.Error(t, c.Ping(ctx).Err()) })

		tlsConfig.CipherSuites = []uint16{tls.TLS_RSA_WITH_AES_256_GCM_SHA384}
		doWithTLSClient(tlsConfig, func(c *redis.Client) { require.Equal(t, "PONG", c.Ping(ctx).Val()) })

		require.NoError(t, rdb.ConfigSet(ctx, "tls-ciphers", "DEFAULT").Err())
		tlsConfig.CipherSuites = []uint16{tls.TLS_RSA_WITH_AES_128_CBC_SHA256}
		doWithTLSClient(tlsConfig, func(c *redis.Client) { require.Equal(t, "PONG", c.Ping(ctx).Val()) })

		require.NoError(t, rdb.ConfigSet(ctx, "tls-protocols", "").Err())
		require.NoError(t, rdb.ConfigSet(ctx, "tls-ciphers", "DEFAULT").Err())
	})

	doWithTCPTLSClient := func(tlsConfig *tls.Config, f func(c *util.TCPClient)) {
		c := srv.NewTCPTLSClient(tlsConfig)
		defer func() { require.NoError(t, c.Close()) }()
		f(c)
	}

	t.Run("TLS: Verify tls-prefer-server-ciphers behaves as expected", func(t *testing.T) {
		require.NoError(t, rdb.ConfigSet(ctx, "tls-protocols", "TLSv1.2").Err())
		require.NoError(t, rdb.ConfigSet(ctx, "tls-ciphers", "AES128-SHA256:AES256-GCM-SHA384").Err())

		tlsConfig, err := util.DefaultTLSConfig()
		require.NoError(t, err)
		tlsConfig.CipherSuites = []uint16{tls.TLS_RSA_WITH_AES_256_GCM_SHA384, tls.TLS_RSA_WITH_AES_128_CBC_SHA256}

		doWithTCPTLSClient(tlsConfig, func(c *util.TCPClient) {
			require.NoError(t, c.WriteArgs("PING"))
			c.MustRead(t, "+PONG")
			require.Equal(t, c.TLSState().CipherSuite, tls.TLS_RSA_WITH_AES_256_GCM_SHA384)
		})

		require.NoError(t, rdb.ConfigSet(ctx, "tls-prefer-server-ciphers", "yes").Err())
		doWithTCPTLSClient(tlsConfig, func(c *util.TCPClient) {
			require.NoError(t, c.WriteArgs("PING"))
			c.MustRead(t, "+PONG")
			require.Equal(t, c.TLSState().CipherSuite, tls.TLS_RSA_WITH_AES_128_CBC_SHA256)
		})

		require.NoError(t, rdb.ConfigSet(ctx, "tls-protocols", "").Err())
		require.NoError(t, rdb.ConfigSet(ctx, "tls-ciphers", "DEFAULT").Err())
	})
}

func TestTLSReplica(t *testing.T) {
	t.Skip("FIXME: flaky test with a high frequency of failure")

	if !util.TLSEnable() {
		t.Skip("TLS tests run only if tls enabled.")
	}

	ctx := context.Background()

	srv := util.StartTLSServer(t, map[string]string{})
	defer srv.Close()

	defaultTLSConfig, err := util.DefaultTLSConfig()
	require.NoError(t, err)

	sc := srv.NewClientWithOption(&redis.Options{TLSConfig: defaultTLSConfig, Addr: srv.TLSAddr()})
	defer func() { require.NoError(t, sc.Close()) }()

	replica := util.StartTLSServer(t, map[string]string{
		"tls-replication": "yes",
		"slaveof":         fmt.Sprintf("%s %d", srv.Host(), srv.TLSPort()),
	})
	defer replica.Close()

	rc := replica.NewClientWithOption(&redis.Options{TLSConfig: defaultTLSConfig, Addr: replica.TLSAddr()})
	defer func() { require.NoError(t, rc.Close()) }()

	t.Run("TLS: Replication (incremental)", func(t *testing.T) {
		time.Sleep(1000 * time.Millisecond)
		require.Equal(t, rc.Get(ctx, "a").Val(), "")
		require.Equal(t, rc.Get(ctx, "b").Val(), "")
		require.NoError(t, sc.Set(ctx, "a", "1", 0).Err())
		require.NoError(t, sc.Set(ctx, "b", "2", 0).Err())
		util.WaitForOffsetSync(t, sc, rc, 5*time.Second)
		require.Equal(t, rc.Get(ctx, "a").Val(), "1")
		require.Equal(t, rc.Get(ctx, "b").Val(), "2")
	})

	require.NoError(t, sc.Set(ctx, "c", "3", 0).Err())

	replica2 := util.StartTLSServer(t, map[string]string{
		"tls-replication": "yes",
		"slaveof":         fmt.Sprintf("%s %d", srv.Host(), srv.TLSPort()),
	})
	defer replica2.Close()

	rc2 := replica2.NewClientWithOption(&redis.Options{TLSConfig: defaultTLSConfig, Addr: replica2.TLSAddr()})
	defer func() { require.NoError(t, rc2.Close()) }()

	t.Run("TLS: Replication (full)", func(t *testing.T) {
		util.WaitForOffsetSync(t, sc, rc2, 5*time.Second)
		require.Equal(t, rc2.Get(ctx, "a").Val(), "1")
		require.Equal(t, rc2.Get(ctx, "b").Val(), "2")
		require.Equal(t, rc2.Get(ctx, "c").Val(), "3")
	})
}
