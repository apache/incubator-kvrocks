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
#include "commands/command_parser.h"
#include "error_constants.h"
#include "server/server.h"
#include "status.h"
#include "types/redis_bitmap.h"

namespace redis {

Status GetBitOffsetFromArgument(const std::string &arg, uint32_t *offset) {
  auto parse_result = ParseInt<uint32_t>(arg, 10);
  if (!parse_result) {
    return parse_result.ToStatus();
  }

  *offset = *parse_result;
  return Status::OK();
}

class CommandGetBit : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    Status s = GetBitOffsetFromArgument(args[2], &offset_);
    if (!s.IsOK()) return {Status::RedisParseErr, "bit offset is not an integer or out of range"};

    return Commander::Parse(args);
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    bool bit = false;
    redis::Bitmap bitmap_db(srv->storage, conn->GetNamespace());

    auto s = bitmap_db.GetBit(ctx, args_[1], offset_, &bit);
    if (!s.ok()) return {Status::RedisExecErr, s.ToString()};

    *output = redis::Integer(bit ? 1 : 0);
    return Status::OK();
  }

 private:
  uint32_t offset_ = 0;
};

class CommandSetBit : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    Status s = GetBitOffsetFromArgument(args[2], &offset_);
    if (!s.IsOK()) return s;

    if (args[3] == "0") {
      bit_ = false;
    } else if (args[3] == "1") {
      bit_ = true;
    } else {
      return {Status::RedisParseErr, "bit is not an integer or out of range"};
    }

    return Commander::Parse(args);
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    bool old_bit = false;
    redis::Bitmap bitmap_db(srv->storage, conn->GetNamespace());

    auto s = bitmap_db.SetBit(ctx, args_[1], offset_, bit_, &old_bit);
    if (!s.ok()) return {Status::RedisExecErr, s.ToString()};

    *output = redis::Integer(old_bit ? 1 : 0);
    return Status::OK();
  }

 private:
  uint32_t offset_ = 0;
  bool bit_ = false;
};

// BITCOUNT key [start end [BYTE | BIT]]
class CommandBitCount : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    if (args.size() == 3) {
      return {Status::RedisParseErr, errInvalidSyntax};
    }

    if (args.size() > 5) {
      return {Status::RedisParseErr, errInvalidSyntax};
    }

    if (args.size() >= 4) {
      auto parse_start = ParseInt<int64_t>(args[2], 10);
      if (!parse_start) {
        return {Status::RedisParseErr, errValueNotInteger};
      }

      start_ = *parse_start;
      auto parse_stop = ParseInt<int64_t>(args[3], 10);
      if (!parse_stop) {
        return {Status::RedisParseErr, errValueNotInteger};
      }

      stop_ = *parse_stop;
    }

    if (args.size() == 5) {
      if (util::EqualICase(args[4], "BYTE")) {
        is_bit_index_ = false;
      } else if (util::EqualICase(args[4], "BIT")) {
        is_bit_index_ = true;
      } else {
        return {Status::RedisParseErr, errInvalidSyntax};
      }
    }

    return Commander::Parse(args);
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    uint32_t cnt = 0;
    redis::Bitmap bitmap_db(srv->storage, conn->GetNamespace());

    auto s = bitmap_db.BitCount(ctx, args_[1], start_, stop_, is_bit_index_, &cnt);
    if (!s.ok()) return {Status::RedisExecErr, s.ToString()};

    *output = redis::Integer(cnt);
    return Status::OK();
  }

 private:
  int64_t start_ = 0;
  int64_t stop_ = -1;
  bool is_bit_index_ = false;
};

class CommandBitPos : public Commander {
 public:
  using Commander::Parse;

  Status Parse(const std::vector<std::string> &args) override {
    if (args.size() >= 4) {
      auto parse_start = ParseInt<int64_t>(args[3], 10);
      if (!parse_start) {
        return {Status::RedisParseErr, errValueNotInteger};
      }

      start_ = *parse_start;
    }

    if (args.size() >= 5) {
      auto parse_stop = ParseInt<int64_t>(args[4], 10);
      if (!parse_stop) {
        return {Status::RedisParseErr, errValueNotInteger};
      }

      stop_given_ = true;
      stop_ = *parse_stop;
    }

    if (args.size() >= 6 && util::EqualICase(args[5], "BIT")) {
      is_bit_index_ = true;
    }

    auto parse_arg = ParseInt<int64_t>(args[2], 10);
    if (!parse_arg) {
      return {Status::RedisParseErr, errValueNotInteger};
    }
    if (*parse_arg == 0) {
      bit_ = false;
    } else if (*parse_arg == 1) {
      bit_ = true;
    } else {
      return {Status::RedisParseErr, "The bit argument must be 1 or 0."};
    }

    return Commander::Parse(args);
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    int64_t pos = 0;
    redis::Bitmap bitmap_db(srv->storage, conn->GetNamespace());

    auto s = bitmap_db.BitPos(ctx, args_[1], bit_, start_, stop_, stop_given_, &pos, is_bit_index_);
    if (!s.ok()) return {Status::RedisExecErr, s.ToString()};

    *output = redis::Integer(pos);
    return Status::OK();
  }

 private:
  int64_t start_ = 0;
  int64_t stop_ = -1;
  bool bit_ = false;
  bool stop_given_ = false;
  bool is_bit_index_ = false;
};

class CommandBitOp : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    std::string opname = util::ToLower(args[1]);
    if (opname == "and")
      op_flag_ = kBitOpAnd;
    else if (opname == "or")
      op_flag_ = kBitOpOr;
    else if (opname == "xor")
      op_flag_ = kBitOpXor;
    else if (opname == "not")
      op_flag_ = kBitOpNot;
    else
      return {Status::RedisInvalidCmd, errInvalidSyntax};
    if (op_flag_ == kBitOpNot && args.size() != 4) {
      return {Status::RedisInvalidCmd, "BITOP NOT must be called with a single source key."};
    }

    return Commander::Parse(args);
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    std::vector<Slice> op_keys;
    op_keys.reserve(args_.size() - 2);
    for (uint64_t i = 3; i < args_.size(); i++) {
      op_keys.emplace_back(args_[i]);
    }

    int64_t dest_key_len = 0;
    redis::Bitmap bitmap_db(srv->storage, conn->GetNamespace());

    auto s = bitmap_db.BitOp(ctx, op_flag_, args_[1], args_[2], op_keys, &dest_key_len);
    if (!s.ok()) return {Status::RedisExecErr, s.ToString()};

    *output = redis::Integer(dest_key_len);
    return Status::OK();
  }

 private:
  BitOpFlags op_flag_;
};

template <bool ReadOnly>
class CommandBitfield : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    BitfieldOperation cmd;

    read_only_ = true;
    // BITFIELD <key> [commands...]
    for (CommandParser group(args, 2); group.Good();) {
      auto remains = group.Remains();

      std::string opcode = util::ToLower(group[0]);
      if (opcode == "get") {
        cmd.type = BitfieldOperation::Type::kGet;
      } else if (opcode == "set") {
        cmd.type = BitfieldOperation::Type::kSet;
        read_only_ = false;
      } else if (opcode == "incrby") {
        cmd.type = BitfieldOperation::Type::kIncrBy;
        read_only_ = false;
      } else if (opcode == "overflow") {
        constexpr auto kOverflowCmdSize = 2;
        if (remains < kOverflowCmdSize) {
          return {Status::RedisParseErr, errWrongNumOfArguments};
        }
        auto s = parseOverflowSubCommand(group[1], &cmd);
        if (!s.IsOK()) {
          return s;
        }

        group.Skip(kOverflowCmdSize);
        continue;
      } else {
        return {Status::RedisParseErr, errUnknownSubcommandOrWrongArguments};
      }

      if (remains < 3) {
        return {Status::RedisParseErr, errWrongNumOfArguments};
      }

      // parse encoding
      auto encoding = parseBitfieldEncoding(group[1]);
      if (!encoding.IsOK()) {
        return encoding.ToStatus();
      }
      cmd.encoding = encoding.GetValue();

      // parse offset
      if (!GetBitOffsetFromArgument(group[2], &cmd.offset).IsOK()) {
        return {Status::RedisParseErr, "bit offset is not an integer or out of range"};
      }

      if (cmd.type != BitfieldOperation::Type::kGet) {
        if (remains < 4) {
          return {Status::RedisParseErr, errWrongNumOfArguments};
        }

        auto value = ParseInt<int64_t>(group[3], 10);
        if (!value.IsOK()) {
          return value.ToStatus();
        }
        cmd.value = value.GetValue();

        // SET|INCRBY <encoding> <offset> <value>
        group.Skip(4);
      } else {
        // GET <encoding> <offset>
        group.Skip(3);
      }

      cmds_.push_back(cmd);
    }

    if constexpr (ReadOnly) {
      if (!read_only_) {
        return {Status::RedisParseErr, "BITFIELD_RO only supports the GET subcommand"};
      }
    }

    return Commander::Parse(args);
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::Bitmap bitmap_db(srv->storage, conn->GetNamespace());
    std::vector<std::optional<BitfieldValue>> rets;
    rocksdb::Status s;

    if (read_only_) {
      s = bitmap_db.BitfieldReadOnly(ctx, args_[1], cmds_, &rets);
    } else {
      s = bitmap_db.Bitfield(ctx, args_[1], cmds_, &rets);
    }
    std::vector<std::string> str_rets(rets.size());
    for (size_t i = 0; i != rets.size(); ++i) {
      if (rets[i].has_value()) {
        if (rets[i]->Encoding().IsSigned()) {
          str_rets[i] = redis::Integer(CastToSignedWithoutBitChanges(rets[i]->Value()));
        } else {
          str_rets[i] = redis::Integer(rets[i]->Value());
        }
      } else {
        str_rets[i] = conn->NilString();
      }
    }
    *output = redis::Array(str_rets);
    return Status::OK();
  }

 private:
  static Status parseOverflowSubCommand(const std::string &overflow, BitfieldOperation *cmd) {
    std::string lower = util::ToLower(overflow);
    if (lower == "wrap") {
      cmd->overflow = BitfieldOverflowBehavior::kWrap;
    } else if (lower == "sat") {
      cmd->overflow = BitfieldOverflowBehavior::kSat;
    } else if (lower == "fail") {
      cmd->overflow = BitfieldOverflowBehavior::kFail;
    } else {
      return {Status::RedisParseErr, errUnknownSubcommandOrWrongArguments};
    }
    return Status::OK();
  }

  static StatusOr<BitfieldEncoding> parseBitfieldEncoding(const std::string &token) {
    if (token.empty()) {
      return {Status::RedisParseErr, errUnknownSubcommandOrWrongArguments};
    }

    auto sign = std::tolower(token[0]);
    if (sign != 'u' && sign != 'i') {
      return {Status::RedisParseErr, errUnknownSubcommandOrWrongArguments};
    }

    auto type = BitfieldEncoding::Type::kUnsigned;
    if (sign == 'i') {
      type = BitfieldEncoding::Type::kSigned;
    }

    auto bits_parse = ParseInt<uint8_t>(token.substr(1), 10);
    if (!bits_parse.IsOK()) {
      return bits_parse.ToStatus();
    }
    uint8_t bits = bits_parse.GetValue();

    auto encoding = BitfieldEncoding::Create(type, bits);
    if (!encoding.IsOK()) {
      return {Status::RedisParseErr, errUnknownSubcommandOrWrongArguments};
    }
    return encoding.GetValue();
  }

  std::vector<BitfieldOperation> cmds_;
  bool read_only_;
};

REDIS_REGISTER_COMMANDS(Bit, MakeCmdAttr<CommandGetBit>("getbit", 3, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandSetBit>("setbit", 4, "write", 1, 1, 1),
                        MakeCmdAttr<CommandBitCount>("bitcount", -2, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandBitPos>("bitpos", -3, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandBitOp>("bitop", -4, "write", 2, -1, 1),
                        MakeCmdAttr<CommandBitfield<false>>("bitfield", -2, "write", 1, 1, 1),
                        MakeCmdAttr<CommandBitfield<true>>("bitfield_ro", -2, "read-only", 1, 1, 1), )

}  // namespace redis
