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
 *
 */

#include "commander.h"
#include "error_constants.h"
#include "server/server.h"
#include "storage/redis_pubsub.h"

namespace redis {

class CommandPublish : public Commander {
 public:
  Status Execute([[maybe_unused]] engine::Context &ctx, Server *srv, [[maybe_unused]] Connection *conn,
                 std::string *output) override {
    if (!srv->IsSlave()) {
      // Compromise: can't replicate a message to sub-replicas in a cascading-like structure.
      // Replication relies on WAL seq; increasing the seq on a replica will break the replication process,
      // hence the compromise solution
      redis::PubSub pubsub_db(srv->storage);

      auto s = pubsub_db.Publish(ctx, args_[1], args_[2]);
      if (!s.ok()) {
        return {Status::RedisExecErr, s.ToString()};
      }
    }

    int receivers = srv->PublishMessage(args_[1], args_[2]);

    *output = redis::Integer(receivers);

    return Status::OK();
  }
};

class CommandMPublish : public Commander {
 public:
  Status Execute(engine::Context &ctx, Server *srv, [[maybe_unused]] Connection *conn, std::string *output) override {
    int total_receivers = 0;

    for (size_t i = 2; i < args_.size(); i++) {
      if (!srv->IsSlave()) {
        redis::PubSub pubsub_db(srv->storage);

        auto s = pubsub_db.Publish(ctx, args_[1], args_[i]);
        if (!s.ok()) {
          return {Status::RedisExecErr, s.ToString()};
        }
      }

      int receivers = srv->PublishMessage(args_[1], args_[i]);
      total_receivers += receivers;
    }

    *output = redis::Integer(total_receivers);

    return Status::OK();
  }
};

void SubscribeCommandReply(const Connection *conn, std::string *output, const std::string &name,
                           const std::string &sub_name, int num) {
  output->append(conn->HeaderOfPush(3));
  output->append(redis::BulkString(name));
  output->append(sub_name.empty() ? conn->NilString() : BulkString(sub_name));
  output->append(redis::Integer(num));
}

class CommandSubscribe : public Commander {
 public:
  Status Execute([[maybe_unused]] engine::Context &ctx, [[maybe_unused]] Server *srv, Connection *conn,
                 std::string *output) override {
    for (unsigned i = 1; i < args_.size(); i++) {
      conn->SubscribeChannel(args_[i]);
      SubscribeCommandReply(conn, output, "subscribe", args_[i],
                            conn->SubscriptionsCount() + conn->PSubscriptionsCount());
    }
    return Status::OK();
  }
};

class CommandUnSubscribe : public Commander {
 public:
  Status Execute([[maybe_unused]] engine::Context &ctx, [[maybe_unused]] Server *srv, Connection *conn,
                 std::string *output) override {
    if (args_.size() == 1) {
      conn->UnsubscribeAll([conn, output](const std::string &sub_name, int num) {
        SubscribeCommandReply(conn, output, "unsubscribe", sub_name, num);
      });
    } else {
      for (size_t i = 1; i < args_.size(); i++) {
        conn->UnsubscribeChannel(args_[i]);
        SubscribeCommandReply(conn, output, "unsubscribe", args_[i],
                              conn->SubscriptionsCount() + conn->PSubscriptionsCount());
      }
    }
    return Status::OK();
  }
};

class CommandPSubscribe : public Commander {
 public:
  Status Execute([[maybe_unused]] engine::Context &ctx, [[maybe_unused]] Server *srv, Connection *conn,
                 std::string *output) override {
    for (size_t i = 1; i < args_.size(); i++) {
      conn->PSubscribeChannel(args_[i]);
      SubscribeCommandReply(conn, output, "psubscribe", args_[i],
                            conn->SubscriptionsCount() + conn->PSubscriptionsCount());
    }
    return Status::OK();
  }
};

class CommandPUnSubscribe : public Commander {
 public:
  Status Execute([[maybe_unused]] engine::Context &ctx, [[maybe_unused]] Server *srv, Connection *conn,
                 std::string *output) override {
    if (args_.size() == 1) {
      conn->PUnsubscribeAll([conn, output](const std::string &sub_name, int num) {
        SubscribeCommandReply(conn, output, "punsubscribe", sub_name, num);
      });
    } else {
      for (size_t i = 1; i < args_.size(); i++) {
        conn->PUnsubscribeChannel(args_[i]);
        SubscribeCommandReply(conn, output, "punsubscribe", args_[i],
                              conn->SubscriptionsCount() + conn->PSubscriptionsCount());
      }
    }
    return Status::OK();
  }
};

class CommandSSubscribe : public Commander {
 public:
  Status Execute([[maybe_unused]] engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    uint16_t slot = 0;
    if (srv->GetConfig()->cluster_enabled) {
      slot = GetSlotIdFromKey(args_[1]);
      for (unsigned int i = 2; i < args_.size(); i++) {
        if (GetSlotIdFromKey(args_[i]) != slot) {
          return {Status::RedisExecErr, "CROSSSLOT Keys in request don't hash to the same slot"};
        }
      }
    }

    for (unsigned int i = 1; i < args_.size(); i++) {
      conn->SSubscribeChannel(args_[i], slot);
      SubscribeCommandReply(conn, output, "ssubscribe", args_[i], conn->SSubscriptionsCount());
    }
    return Status::OK();
  }
};

class CommandSUnSubscribe : public Commander {
 public:
  Status Execute([[maybe_unused]] engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    if (args_.size() == 1) {
      conn->SUnsubscribeAll([conn, output](const std::string &sub_name, int num) {
        SubscribeCommandReply(conn, output, "sunsubscribe", sub_name, num);
      });
    } else {
      for (size_t i = 1; i < args_.size(); i++) {
        conn->SUnsubscribeChannel(args_[i], srv->GetConfig()->cluster_enabled ? GetSlotIdFromKey(args_[i]) : 0);
        SubscribeCommandReply(conn, output, "sunsubscribe", args_[i], conn->SSubscriptionsCount());
      }
    }
    return Status::OK();
  }
};

class CommandPubSub : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    subcommand_ = util::ToLower(args[1]);
    if (subcommand_ == "numpat" && args.size() == 2) {
      return Status::OK();
    }

    if ((subcommand_ == "numsub" || subcommand_ == "shardnumsub") && args.size() >= 2) {
      if (args.size() > 2) {
        channels_ = std::vector<std::string>(args.begin() + 2, args.end());
      }
      return Status::OK();
    }

    if ((subcommand_ == "channels" || subcommand_ == "shardchannels") && args.size() <= 3) {
      if (args.size() == 3) {
        pattern_ = args[2];
      }
      return Status::OK();
    }

    return {Status::RedisInvalidCmd, errUnknownSubcommandOrWrongArguments};
  }

  Status Execute([[maybe_unused]] engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    if (subcommand_ == "numpat") {
      *output = redis::Integer(srv->GetPubSubPatternSize());
      return Status::OK();
    }

    if (subcommand_ == "numsub" || subcommand_ == "shardnumsub") {
      std::vector<ChannelSubscribeNum> channel_subscribe_nums;
      if (subcommand_ == "numsub") {
        srv->ListChannelSubscribeNum(channels_, &channel_subscribe_nums);
      } else {
        srv->ListSChannelSubscribeNum(channels_, &channel_subscribe_nums);
      }

      output->append(redis::MultiLen(channel_subscribe_nums.size() * 2));
      for (const auto &chan_subscribe_num : channel_subscribe_nums) {
        output->append(redis::BulkString(chan_subscribe_num.channel));
        output->append(redis::Integer(chan_subscribe_num.subscribe_num));
      }

      return Status::OK();
    }

    if (subcommand_ == "channels" || subcommand_ == "shardchannels") {
      std::vector<std::string> channels;
      if (subcommand_ == "channels") {
        srv->GetChannelsByPattern(pattern_, &channels);
      } else {
        srv->GetSChannelsByPattern(pattern_, &channels);
      }
      *output = conn->MultiBulkString(channels);
      return Status::OK();
    }

    return {Status::RedisInvalidCmd, errUnknownSubcommandOrWrongArguments};
  }

 private:
  std::string pattern_;
  std::vector<std::string> channels_;
  std::string subcommand_;
};

REDIS_REGISTER_COMMANDS(
    Pubsub, MakeCmdAttr<CommandPublish>("publish", 3, "read-only pub-sub", NO_KEY),
    MakeCmdAttr<CommandMPublish>("mpublish", -3, "read-only pub-sub", NO_KEY),
    MakeCmdAttr<CommandSubscribe>("subscribe", -2, "read-only pub-sub no-multi no-script", NO_KEY),
    MakeCmdAttr<CommandUnSubscribe>("unsubscribe", -1, "read-only pub-sub no-multi no-script", NO_KEY),
    MakeCmdAttr<CommandPSubscribe>("psubscribe", -2, "read-only pub-sub no-multi no-script", NO_KEY),
    MakeCmdAttr<CommandPUnSubscribe>("punsubscribe", -1, "read-only pub-sub no-multi no-script", NO_KEY),
    MakeCmdAttr<CommandSSubscribe>("ssubscribe", -2, "read-only pub-sub no-multi no-script", NO_KEY),
    MakeCmdAttr<CommandSUnSubscribe>("sunsubscribe", -1, "read-only pub-sub no-multi no-script", NO_KEY),
    MakeCmdAttr<CommandPubSub>("pubsub", -2, "read-only pub-sub no-script", NO_KEY), )

}  // namespace redis
