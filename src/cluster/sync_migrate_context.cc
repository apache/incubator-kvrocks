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

#include "cluster/sync_migrate_context.h"

void SyncMigrateContext::Suspend() {
  auto bev = conn_->GetBufferEvent();
  SetCB(bev);

  if (timeout_ > 0) {
    timer_.reset(NewTimer(bufferevent_get_base(bev)));
    timeval tm = {timeout_, 0};
    evtimer_add(timer_.get(), &tm);
  }
}

void SyncMigrateContext::Resume(const Status &migrate_result) {
  migrate_result_ = migrate_result;
  auto s = conn_->Owner()->EnableWriteEvent(conn_->GetFD());
  if (!s.IsOK()) {
    LOG(ERROR) << "[server] Failed to enable write event on the sync migrate connection " << conn_->GetFD() << ": "
               << s.Msg();
  }
}

void SyncMigrateContext::OnEvent(bufferevent *bev, int16_t events) {
  auto &&slot_migrator = srv_->slot_migrator;

  if (events & (BEV_EVENT_EOF | BEV_EVENT_ERROR)) {
    timer_.reset();

    slot_migrator->CancelSyncCtx();
  }
  conn_->OnEvent(bev, events);
}

void SyncMigrateContext::TimerCB(int, [[maybe_unused]] int16_t events) {
  auto &&slot_migrator = srv_->slot_migrator;

  conn_->Reply(conn_->NilString());
  timer_.reset();

  slot_migrator->CancelSyncCtx();

  auto bev = conn_->GetBufferEvent();
  conn_->SetCB(bev);
  bufferevent_enable(bev, EV_READ);
}

void SyncMigrateContext::OnWrite(bufferevent *bev) {
  if (migrate_result_) {
    conn_->Reply(redis::RESP_OK);
  } else {
    conn_->Reply(redis::Error(migrate_result_));
  }

  timer_.reset();

  conn_->SetCB(bev);
  bufferevent_enable(bev, EV_READ);

  bufferevent_trigger(bev, EV_READ, BEV_TRIG_IGNORE_WATERMARKS);
}
