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

package pubsub

import (
	"context"
	"fmt"
	"testing"

	"github.com/apache/kvrocks/tests/gocase/util"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func receiveType[T any](t *testing.T, pubsub *redis.PubSub, typ T) T {
	msg, err := pubsub.Receive(context.Background())
	require.NoError(t, err)
	require.IsType(t, typ, msg)
	return msg.(T)
}

func TestPubSub(t *testing.T) {
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
		testPubSub(t, configs)
	}
}

func testPubSub(t *testing.T, configs util.KvrocksServerConfigs) {
	srv := util.StartServer(t, configs)
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	t.Run("PUBLISH/SUBSCRIBE PING", func(t *testing.T) {
		pubsub := rdb.Subscribe(ctx, "somechannel")
		receiveType(t, pubsub, &redis.Subscription{})
		require.NoError(t, pubsub.Ping(ctx))
		require.NoError(t, pubsub.Ping(ctx))
		require.NoError(t, pubsub.Unsubscribe(ctx, "somechannel"))
		require.Equal(t, "PONG", rdb.Ping(ctx).Val())
		receiveType(t, pubsub, &redis.Pong{})
		receiveType(t, pubsub, &redis.Pong{})
	})

	t.Run("PUBLISH/SUBSCRIBE basics", func(t *testing.T) {
		// subscribe to two channels
		pubsub := rdb.Subscribe(ctx, "chan1", "chan2")
		require.EqualValues(t, 1, receiveType(t, pubsub, &redis.Subscription{}).Count)
		require.EqualValues(t, 2, receiveType(t, pubsub, &redis.Subscription{}).Count)

		require.EqualValues(t, 1, rdb.Publish(ctx, "chan1", "hello").Val())
		require.EqualValues(t, 1, rdb.Publish(ctx, "chan2", "world").Val())

		msg := receiveType(t, pubsub, &redis.Message{})
		require.Equal(t, "chan1", msg.Channel)
		require.Equal(t, "hello", msg.Payload)
		msg = receiveType(t, pubsub, &redis.Message{})
		require.Equal(t, "chan2", msg.Channel)
		require.Equal(t, "world", msg.Payload)

		// unsubscribe from one of the channels
		require.NoError(t, pubsub.Unsubscribe(ctx, "chan1"))
		require.EqualValues(t, &redis.Subscription{
			Kind:    "unsubscribe",
			Channel: "chan1",
			Count:   1,
		}, receiveType(t, pubsub, &redis.Subscription{}))
		require.EqualValues(t, 0, rdb.Publish(ctx, "chan1", "hello").Val())
		require.EqualValues(t, 1, rdb.Publish(ctx, "chan2", "world").Val())

		// drain incoming messages
		receiveType(t, pubsub, &redis.Message{})

		// unsubscribe from the remaining channel
		require.NoError(t, pubsub.Unsubscribe(ctx, "chan2"))
		require.EqualValues(t, &redis.Subscription{
			Kind:    "unsubscribe",
			Channel: "chan2",
			Count:   0,
		}, receiveType(t, pubsub, &redis.Subscription{}))
		require.EqualValues(t, 0, rdb.Publish(ctx, "chan1", "hello").Val())
		require.EqualValues(t, 0, rdb.Publish(ctx, "chan2", "world").Val())
	})

	t.Run("PUBLISH/SUBSCRIBE with two clients", func(t *testing.T) {
		c1 := srv.NewClient()
		defer func() { require.NoError(t, c1.Close()) }()
		c2 := srv.NewClient()
		defer func() { require.NoError(t, c2.Close()) }()

		p1 := c1.Subscribe(ctx, "chan1")
		p2 := c2.Subscribe(ctx, "chan1")
		require.EqualValues(t, 1, receiveType(t, p1, &redis.Subscription{}).Count)
		require.EqualValues(t, 1, receiveType(t, p2, &redis.Subscription{}).Count)

		require.EqualValues(t, 2, rdb.Publish(ctx, "chan1", "hello").Val())
		msg := receiveType(t, p1, &redis.Message{})
		require.Equal(t, "chan1", msg.Channel)
		require.Equal(t, "hello", msg.Payload)
		msg = receiveType(t, p2, &redis.Message{})
		require.Equal(t, "chan1", msg.Channel)
		require.Equal(t, "hello", msg.Payload)
	})

	t.Run("MPUBLISH basics", func(t *testing.T) {
		var (
			channelName = "channel1"
			msg1        = "hello"
			msg2        = "world"
			msg3        = "!"
			msg4        = "foo-bar"
		)

		c1 := srv.NewClient()
		defer func() { require.NoError(t, c1.Close()) }()
		c2 := srv.NewClient()
		defer func() { require.NoError(t, c2.Close()) }()

		pubsub1 := c1.Subscribe(ctx, channelName)
		pubsub2 := c2.Subscribe(ctx, channelName)

		require.EqualValues(t, 1, receiveType(t, pubsub1, &redis.Subscription{}).Count)
		require.EqualValues(t, 1, receiveType(t, pubsub2, &redis.Subscription{}).Count)

		require.EqualValues(t, 6, rdb.Do(ctx, "MPUBLISH", channelName, msg1, msg2, msg3).Val())

		msg := receiveType(t, pubsub1, &redis.Message{})
		require.Equal(t, channelName, msg.Channel)
		require.Equal(t, msg1, msg.Payload)

		msg = receiveType(t, pubsub2, &redis.Message{})
		require.Equal(t, channelName, msg.Channel)
		require.Equal(t, msg1, msg.Payload)

		msg = receiveType(t, pubsub1, &redis.Message{})
		require.Equal(t, channelName, msg.Channel)
		require.Equal(t, msg2, msg.Payload)

		msg = receiveType(t, pubsub2, &redis.Message{})
		require.Equal(t, channelName, msg.Channel)
		require.Equal(t, msg2, msg.Payload)

		msg = receiveType(t, pubsub1, &redis.Message{})
		require.Equal(t, channelName, msg.Channel)
		require.Equal(t, msg3, msg.Payload)

		msg = receiveType(t, pubsub2, &redis.Message{})
		require.Equal(t, channelName, msg.Channel)
		require.Equal(t, msg3, msg.Payload)

		require.EqualValues(t, 2, rdb.Do(ctx, "MPUBLISH", channelName, msg4).Val())

		msg = receiveType(t, pubsub1, &redis.Message{})
		require.Equal(t, channelName, msg.Channel)
		require.Equal(t, msg4, msg.Payload)

		msg = receiveType(t, pubsub2, &redis.Message{})
		require.Equal(t, channelName, msg.Channel)
		require.Equal(t, msg4, msg.Payload)
	})

	t.Run("PUBLISH/SUBSCRIBE after UNSUBSCRIBE without arguments", func(t *testing.T) {
		pubsub := rdb.Subscribe(ctx, "chan1", "chan2", "chan3")
		require.EqualValues(t, 1, receiveType(t, pubsub, &redis.Subscription{}).Count)
		require.EqualValues(t, 2, receiveType(t, pubsub, &redis.Subscription{}).Count)
		require.EqualValues(t, 3, receiveType(t, pubsub, &redis.Subscription{}).Count)

		require.NoError(t, pubsub.Unsubscribe(ctx))
		require.EqualValues(t, &redis.Subscription{
			Kind:    "unsubscribe",
			Channel: "chan1",
			Count:   2,
		}, receiveType(t, pubsub, &redis.Subscription{}))
		require.EqualValues(t, &redis.Subscription{
			Kind:    "unsubscribe",
			Channel: "chan2",
			Count:   1,
		}, receiveType(t, pubsub, &redis.Subscription{}))
		require.EqualValues(t, &redis.Subscription{
			Kind:    "unsubscribe",
			Channel: "chan3",
			Count:   0,
		}, receiveType(t, pubsub, &redis.Subscription{}))

		require.EqualValues(t, 0, rdb.Publish(ctx, "chan1", "hello").Val())
		require.EqualValues(t, 0, rdb.Publish(ctx, "chan2", "hello").Val())
		require.EqualValues(t, 0, rdb.Publish(ctx, "chan3", "hello").Val())
	})

	t.Run("SUBSCRIBE to one channel more than once", func(t *testing.T) {
		pubsub := rdb.Subscribe(ctx, "chan1", "chan1", "chan1")
		require.EqualValues(t, 1, receiveType(t, pubsub, &redis.Subscription{}).Count)
		require.EqualValues(t, 1, receiveType(t, pubsub, &redis.Subscription{}).Count)
		require.EqualValues(t, 1, receiveType(t, pubsub, &redis.Subscription{}).Count)
		require.EqualValues(t, 1, rdb.Publish(ctx, "chan1", "hello").Val())
		msg := receiveType(t, pubsub, &redis.Message{})
		require.Equal(t, "chan1", msg.Channel)
		require.Equal(t, "hello", msg.Payload)
	})

	t.Run("UNSUBSCRIBE from non-subscribed channels", func(t *testing.T) {
		pubsub := rdb.Subscribe(ctx)
		require.NoError(t, pubsub.Unsubscribe(ctx, "foo", "bar", "baz"))
		require.EqualValues(t, &redis.Subscription{
			Kind:    "unsubscribe",
			Channel: "foo",
			Count:   0,
		}, receiveType(t, pubsub, &redis.Subscription{}))
		require.EqualValues(t, &redis.Subscription{
			Kind:    "unsubscribe",
			Channel: "bar",
			Count:   0,
		}, receiveType(t, pubsub, &redis.Subscription{}))
		require.EqualValues(t, &redis.Subscription{
			Kind:    "unsubscribe",
			Channel: "baz",
			Count:   0,
		}, receiveType(t, pubsub, &redis.Subscription{}))
	})

	t.Run("PUBLISH/PSUBSCRIBE basics", func(t *testing.T) {
		// subscribe to two patterns
		pubsub := rdb.PSubscribe(ctx, "foo.*", "bar.*")
		require.EqualValues(t, 1, receiveType(t, pubsub, &redis.Subscription{}).Count)
		require.EqualValues(t, 2, receiveType(t, pubsub, &redis.Subscription{}).Count)
		require.EqualValues(t, 1, rdb.Publish(ctx, "foo.1", "hello").Val())
		require.EqualValues(t, 1, rdb.Publish(ctx, "bar.1", "hello").Val())
		require.EqualValues(t, 0, rdb.Publish(ctx, "foo1", "hello").Val())
		require.EqualValues(t, 0, rdb.Publish(ctx, "barfoo.1", "hello").Val())
		require.EqualValues(t, 0, rdb.Publish(ctx, "qux.1", "hello").Val())
		msg := receiveType(t, pubsub, &redis.Message{})
		require.Equal(t, "foo.1", msg.Channel)
		require.Equal(t, "foo.*", msg.Pattern)
		require.Equal(t, "hello", msg.Payload)
		msg = receiveType(t, pubsub, &redis.Message{})
		require.Equal(t, "bar.1", msg.Channel)
		require.Equal(t, "bar.*", msg.Pattern)
		require.Equal(t, "hello", msg.Payload)

		// unsubscribe from one of the patterns
		require.NoError(t, pubsub.PUnsubscribe(ctx, "foo.*"))
		require.EqualValues(t, 1, receiveType(t, pubsub, &redis.Subscription{}).Count)
		require.EqualValues(t, 0, rdb.Publish(ctx, "foo.1", "hello").Val())
		require.EqualValues(t, 1, rdb.Publish(ctx, "bar.1", "hello").Val())
		msg = receiveType(t, pubsub, &redis.Message{})
		require.Equal(t, "bar.1", msg.Channel)
		require.Equal(t, "bar.*", msg.Pattern)
		require.Equal(t, "hello", msg.Payload)

		// unsubscribe from the remaining pattern
		require.NoError(t, pubsub.PUnsubscribe(ctx, "bar.*"))
		require.EqualValues(t, 0, receiveType(t, pubsub, &redis.Subscription{}).Count)
		require.EqualValues(t, 0, rdb.Publish(ctx, "foo.1", "hello").Val())
		require.EqualValues(t, 0, rdb.Publish(ctx, "bar.1", "hello").Val())
	})

	t.Run("PUBLISH/PSUBSCRIBE with two clients", func(t *testing.T) {
		c1 := srv.NewClient()
		defer func() { require.NoError(t, c1.Close()) }()
		c2 := srv.NewClient()
		defer func() { require.NoError(t, c2.Close()) }()

		p1 := c1.PSubscribe(ctx, "chan.*")
		p2 := c2.PSubscribe(ctx, "chan.*")
		require.EqualValues(t, 1, receiveType(t, p1, &redis.Subscription{}).Count)
		require.EqualValues(t, 1, receiveType(t, p2, &redis.Subscription{}).Count)

		require.EqualValues(t, 2, rdb.Publish(ctx, "chan.foo", "hello").Val())
		msg := receiveType(t, p1, &redis.Message{})
		require.Equal(t, "chan.foo", msg.Channel)
		require.Equal(t, "chan.*", msg.Pattern)
		require.Equal(t, "hello", msg.Payload)
		msg = receiveType(t, p2, &redis.Message{})
		require.Equal(t, "chan.foo", msg.Channel)
		require.Equal(t, "chan.*", msg.Pattern)
		require.Equal(t, "hello", msg.Payload)
	})

	t.Run("PUBLISH/PSUBSCRIBE after PUNSUBSCRIBE without arguments", func(t *testing.T) {
		pubsub := rdb.PSubscribe(ctx, "chan1.*", "chan2.*", "chan3.*")
		require.EqualValues(t, 1, receiveType(t, pubsub, &redis.Subscription{}).Count)
		require.EqualValues(t, 2, receiveType(t, pubsub, &redis.Subscription{}).Count)
		require.EqualValues(t, 3, receiveType(t, pubsub, &redis.Subscription{}).Count)

		require.NoError(t, pubsub.PUnsubscribe(ctx))
		require.EqualValues(t, &redis.Subscription{
			Kind:    "punsubscribe",
			Channel: "chan1.*",
			Count:   2,
		}, receiveType(t, pubsub, &redis.Subscription{}))
		require.EqualValues(t, &redis.Subscription{
			Kind:    "punsubscribe",
			Channel: "chan2.*",
			Count:   1,
		}, receiveType(t, pubsub, &redis.Subscription{}))
		require.EqualValues(t, &redis.Subscription{
			Kind:    "punsubscribe",
			Channel: "chan3.*",
			Count:   0,
		}, receiveType(t, pubsub, &redis.Subscription{}))

		require.EqualValues(t, 0, rdb.Publish(ctx, "chan1.hi", "hello").Val())
		require.EqualValues(t, 0, rdb.Publish(ctx, "chan2.hi", "hello").Val())
		require.EqualValues(t, 0, rdb.Publish(ctx, "chan3.hi", "hello").Val())
	})

	t.Run("PUNSUBSCRIBE from non-subscribed channels", func(t *testing.T) {
		pubsub := rdb.PSubscribe(ctx)
		require.NoError(t, pubsub.PUnsubscribe(ctx, "foo.*", "bar.*", "baz.*"))
		require.EqualValues(t, &redis.Subscription{
			Kind:    "punsubscribe",
			Channel: "foo.*",
			Count:   0,
		}, receiveType(t, pubsub, &redis.Subscription{}))
		require.EqualValues(t, &redis.Subscription{
			Kind:    "punsubscribe",
			Channel: "bar.*",
			Count:   0,
		}, receiveType(t, pubsub, &redis.Subscription{}))
		require.EqualValues(t, &redis.Subscription{
			Kind:    "punsubscribe",
			Channel: "baz.*",
			Count:   0,
		}, receiveType(t, pubsub, &redis.Subscription{}))
	})

	t.Run("NUMSUB returns numbers, not strings (redis#1561)", func(t *testing.T) {
		require.EqualValues(t, map[string]int64{
			"abc": 0,
			"def": 0,
		}, rdb.PubSubNumSub(ctx, "abc", "def").Val())
	})

	readSub := func(t *testing.T, c *util.TCPClient, sub redis.Subscription) {
		c.MustRead(t, "*3")
		c.MustRead(t, fmt.Sprintf("$%d", len(sub.Kind)))
		c.MustRead(t, sub.Kind)
		c.MustRead(t, fmt.Sprintf("$%d", len(sub.Channel)))
		c.MustRead(t, sub.Channel)
		c.MustRead(t, fmt.Sprintf(":%d", sub.Count))
	}

	readMsg := func(t *testing.T, c *util.TCPClient, kind string, msg redis.Message) {
		if msg.Pattern != "" {
			c.MustRead(t, "*4")
		} else {
			c.MustRead(t, "*3")
		}
		c.MustRead(t, fmt.Sprintf("$%d", len(kind)))
		c.MustRead(t, kind)
		if msg.Pattern != "" {
			c.MustRead(t, fmt.Sprintf("$%d", len(msg.Pattern)))
			c.MustRead(t, msg.Pattern)
		}
		c.MustRead(t, fmt.Sprintf("$%d", len(msg.Channel)))
		c.MustRead(t, msg.Channel)
		c.MustRead(t, fmt.Sprintf("$%d", len(msg.Payload)))
		c.MustRead(t, msg.Payload)
	}

	t.Run("Mix SUBSCRIBE and PSUBSCRIBE", func(t *testing.T) {
		c := srv.NewTCPClient()
		defer func() { require.NoError(t, c.Close()) }()

		require.NoError(t, c.WriteArgs("subscribe", "foo.bar"))
		require.NoError(t, c.WriteArgs("psubscribe", "foo.*"))

		readSub(t, c, redis.Subscription{
			Kind:    "subscribe",
			Channel: "foo.bar",
			Count:   1,
		})
		readSub(t, c, redis.Subscription{
			Kind:    "psubscribe",
			Channel: "foo.*",
			Count:   2,
		})

		require.EqualValues(t, 2, rdb.Publish(ctx, "foo.bar", "hello").Val())

		readMsg(t, c, "message", redis.Message{
			Channel:      "foo.bar",
			Pattern:      "",
			Payload:      "hello",
			PayloadSlice: nil,
		})
		readMsg(t, c, "pmessage", redis.Message{
			Channel:      "foo.bar",
			Pattern:      "foo.*",
			Payload:      "hello",
			PayloadSlice: nil,
		})
	})

	t.Run("PUNSUBSCRIBE and UNSUBSCRIBE should always reply", func(t *testing.T) {
		// make sure we are not subscribed to any channel at all.
		c := srv.NewClient()
		defer func() { require.NoError(t, c.Close()) }()

		pubsub := c.Subscribe(ctx)
		require.NoError(t, pubsub.PUnsubscribe(ctx))
		require.EqualValues(t, 0, receiveType(t, pubsub, &redis.Subscription{}).Count)
		require.NoError(t, pubsub.Unsubscribe(ctx))
		require.EqualValues(t, 0, receiveType(t, pubsub, &redis.Subscription{}).Count)

		// now check if the commands still reply correctly.
		pubsub = rdb.Subscribe(ctx)
		require.NoError(t, pubsub.PUnsubscribe(ctx))
		require.EqualValues(t, 0, receiveType(t, pubsub, &redis.Subscription{}).Count)
		require.NoError(t, pubsub.Unsubscribe(ctx))
		require.EqualValues(t, 0, receiveType(t, pubsub, &redis.Subscription{}).Count)
	})
}
