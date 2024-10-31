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

#pragma once

#include "commander.h"
#include "common/lock_manager.h"
#include "event_util.h"
#include "server/redis_connection.h"

namespace redis {

class BlockingCommander : public Commander,
                          private EvbufCallbackBase<BlockingCommander, false>,
                          private EventCallbackBase<BlockingCommander> {
 public:
  // method to reply when no operation happens
  virtual std::string NoopReply(const Connection *conn) = 0;

  // method to block keys
  virtual void BlockKeys() = 0;

  // method to unblock keys
  virtual void UnblockKeys() = 0;

  // method to access database in write callback
  // the return value indicates if the real database operation happens
  // in other words, returning true indicates ending the blocking
  virtual bool OnBlockingWrite() = 0;

  // GetLocks() locks the keys of the BlockingCommander with MultiLockGuard.
  // When OnWrite() is triggered, BlockingCommander needs to relock the keys.
  virtual MultiLockGuard GetLocks() = 0;

  // to start the blocking process
  // usually put to the end of the Execute method
  Status StartBlocking(int64_t timeout, std::string *output) {
    if (conn_->IsInExec()) {
      *output = NoopReply(conn_);
      return Status::OK();  // no blocking in multi-exec
    }

    BlockKeys();
    SetCB(conn_->GetBufferEvent());

    if (timeout) {
      InitTimer(timeout);
    }

    return {Status::BlockingCmd};
  }

  void OnWrite(bufferevent *bev) {
    bool done{false};
    {
      auto guard = GetLocks();
      done = OnBlockingWrite();
    }

    if (!done) {
      // The connection may be waked up but can't pop from the datatype.
      // For example, connection A is blocked on it and connection B added a new element;
      // then connection A was unblocked, but this element may be taken by
      // another connection C. So we need to block connection A again
      // and wait for the element being added by disabling the WRITE event.
      bufferevent_disable(bev, EV_WRITE);
      return;
    }

    if (timer_) {
      timer_.reset();
    }

    UnblockKeys();
    conn_->SetCB(bev);
    bufferevent_enable(bev, EV_READ);
    // We need to manually trigger the read event since we will stop processing commands
    // in connection after the blocking command, so there may have some commands to be processed.
    // Related issue: https://github.com/apache/kvrocks/issues/831
    bufferevent_trigger(bev, EV_READ, BEV_TRIG_IGNORE_WATERMARKS);
  }

  void OnEvent(bufferevent *bev, int16_t events) {
    if (events & (BEV_EVENT_EOF | BEV_EVENT_ERROR)) {
      if (timer_ != nullptr) {
        timer_.reset();
      }
      UnblockKeys();
    }
    conn_->OnEvent(bev, events);
  }

  // Usually put to the top of the Execute method
  void InitConnection(Connection *conn) { conn_ = conn; }

  void InitTimer(int64_t timeout) {
    auto bev = conn_->GetBufferEvent();
    timer_.reset(NewTimer(bufferevent_get_base(bev)));
    int64_t timeout_second = timeout / 1000 / 1000;
    int64_t timeout_microsecond = timeout % (1000 * 1000);
    timeval tm = {timeout_second, static_cast<int>(timeout_microsecond)};
    evtimer_add(timer_.get(), &tm);
  }

  void TimerCB(int, int16_t) {
    conn_->Reply(NoopReply(conn_));
    timer_.reset();
    UnblockKeys();
    auto bev = conn_->GetBufferEvent();
    conn_->SetCB(bev);
    bufferevent_enable(bev, EV_READ);
  }

 protected:
  Connection *conn_ = nullptr;
  UniqueEvent timer_;
};

}  // namespace redis
