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

#include "worker.h"

#include <event2/util.h>
#include <glog/logging.h>
#include <unistd.h>

#include <stdexcept>
#include <string>

#include "event2/bufferevent.h"
#include "io_util.h"
#include "scope_exit.h"
#include "thread_util.h"

#ifdef ENABLE_OPENSSL
#include <event2/bufferevent_ssl.h>
#include <openssl/err.h>
#include <openssl/ssl.h>
#endif

#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/un.h>

#include <algorithm>
#include <utility>

#include "redis_connection.h"
#include "redis_request.h"
#include "server.h"
#include "storage/scripting.h"

Worker::Worker(Server *srv, Config *config) : srv(srv), base_(event_base_new()) {
  if (!base_) throw std::runtime_error{"event base failed to be created"};

  timer_.reset(NewEvent(base_, -1, EV_PERSIST));
  timeval tm = {10, 0};
  evtimer_add(timer_.get(), &tm);

  if (config->socket_fd != -1) {
    if (const Status s = listenFD(config->socket_fd, config->port, config->backlog); !s.IsOK()) {
      LOG(ERROR) << "[worker] Failed to listen to socket with fd: " << config->socket_fd << ". Error: " << s.Msg();
      exit(1);
    }
  } else {
    const uint32_t ports[3] = {config->port, config->tls_port, 0};

    for (const uint32_t *port = ports; *port; ++port) {
      for (const auto &bind : config->binds) {
        if (const Status s = listenTCP(bind, *port, config->backlog); !s.IsOK()) {
          LOG(ERROR) << "[worker] Failed to listen on: " << bind << ":" << *port << ". Error: " << s.Msg();
          exit(1);
        }
        LOG(INFO) << "[worker] Listening on: " << bind << ":" << *port;
      }
    }
  }
  lua_ = lua::CreateState(srv);
}

Worker::~Worker() {
  std::vector<redis::Connection *> conns;
  conns.reserve(conns_.size() + monitor_conns_.size());

  for (const auto &iter : conns_) {
    conns.emplace_back(iter.second);
  }
  for (const auto &iter : monitor_conns_) {
    conns.emplace_back(iter.second);
  }
  for (const auto &iter : conns) {
    iter->Close();
  }

  timer_.reset();
  if (rate_limit_group_) {
    bufferevent_rate_limit_group_free(rate_limit_group_);
  }
  if (rate_limit_group_cfg_) {
    ev_token_bucket_cfg_free(rate_limit_group_cfg_);
  }
  event_base_free(base_);
  lua::DestroyState(lua_);
}

void Worker::TimerCB(int, [[maybe_unused]] int16_t events) {
  auto config = srv->GetConfig();
  if (config->timeout == 0) return;
  KickoutIdleClients(config->timeout);
}

void Worker::newTCPConnection(evconnlistener *listener, evutil_socket_t fd, [[maybe_unused]] sockaddr *address,
                              [[maybe_unused]] int socklen) {
  int local_port = util::GetLocalPort(fd);  // NOLINT
  DLOG(INFO) << "[worker] New connection: fd=" << fd << " from port: " << local_port << " thread #" << tid_;

  auto s = util::SockSetTcpKeepalive(fd, 120);
  if (!s.IsOK()) {
    LOG(ERROR) << "[worker] Failed to set tcp-keepalive on socket. Error: " << s.Msg();
    evutil_closesocket(fd);
    return;
  }

  s = util::SockSetTcpNoDelay(fd, 1);
  if (!s.IsOK()) {
    LOG(ERROR) << "[worker] Failed to set tcp-nodelay on socket. Error: " << s.Msg();
    evutil_closesocket(fd);
    return;
  }

  event_base *base = evconnlistener_get_base(listener);
  auto ev_thread_safe_flags =
      BEV_OPT_THREADSAFE | BEV_OPT_DEFER_CALLBACKS | BEV_OPT_UNLOCK_CALLBACKS | BEV_OPT_CLOSE_ON_FREE;

  bufferevent *bev = nullptr;
  ssl_st *ssl = nullptr;
#ifdef ENABLE_OPENSSL
  if (uint32_t(local_port) == srv->GetConfig()->tls_port) {
    ssl = SSL_new(srv->ssl_ctx.get());
    if (!ssl) {
      LOG(ERROR) << "Failed to construct SSL structure for new connection: " << SSLErrors{};
      evutil_closesocket(fd);
      return;
    }
    bev = bufferevent_openssl_socket_new(base, fd, ssl, BUFFEREVENT_SSL_ACCEPTING, ev_thread_safe_flags);
  } else {
    bev = bufferevent_socket_new(base, fd, ev_thread_safe_flags);
  }
#else
  bev = bufferevent_socket_new(base, fd, ev_thread_safe_flags);
#endif
  if (!bev) {
    auto socket_err = evutil_socket_error_to_string(EVUTIL_SOCKET_ERROR());
#ifdef ENABLE_OPENSSL
    LOG(ERROR) << "Failed to construct socket for new connection: " << socket_err << ", SSL error: " << SSLErrors{};
    if (ssl) SSL_free(ssl);
#else
    LOG(ERROR) << "Failed to construct socket for new connection: " << socket_err;
#endif
    evutil_closesocket(fd);
    return;
  }
#ifdef ENABLE_OPENSSL
  if (uint32_t(local_port) == srv->GetConfig()->tls_port) {
    bufferevent_openssl_set_allow_dirty_shutdown(bev, 1);
  }
#endif
  auto conn = new redis::Connection(bev, this);
  conn->SetCB(bev);
  bufferevent_enable(bev, EV_READ);

  s = AddConnection(conn);
  if (!s.IsOK()) {
    std::string err_msg = redis::Error({Status::NotOK, s.Msg()});
    s = util::SockSend(fd, err_msg, ssl);
    if (!s.IsOK()) {
      LOG(WARNING) << "Failed to send error response to socket: " << s.Msg();
    }
    conn->Close();
    return;
  }

  if (auto s = util::GetPeerAddr(fd)) {
    auto [ip, port] = std::move(*s);
    conn->SetAddr(ip, port);
  }

  if (rate_limit_group_) {
    bufferevent_add_to_rate_limit_group(bev, rate_limit_group_);
  }
}

void Worker::newUnixSocketConnection(evconnlistener *listener, evutil_socket_t fd, [[maybe_unused]] sockaddr *address,
                                     [[maybe_unused]] int socklen) {
  DLOG(INFO) << "[worker] New connection: fd=" << fd << " from unixsocket: " << srv->GetConfig()->unixsocket
             << " thread #" << tid_;
  event_base *base = evconnlistener_get_base(listener);
  auto ev_thread_safe_flags =
      BEV_OPT_THREADSAFE | BEV_OPT_DEFER_CALLBACKS | BEV_OPT_UNLOCK_CALLBACKS | BEV_OPT_CLOSE_ON_FREE;
  bufferevent *bev = bufferevent_socket_new(base, fd, ev_thread_safe_flags);

  auto conn = new redis::Connection(bev, this);
  conn->SetCB(bev);
  bufferevent_enable(bev, EV_READ);

  auto s = AddConnection(conn);
  if (!s.IsOK()) {
    s = util::SockSend(fd, redis::Error(s));
    if (!s.IsOK()) {
      LOG(WARNING) << "Failed to send error response to socket: " << s.Msg();
    }
    conn->Close();
    return;
  }

  conn->SetAddr(srv->GetConfig()->unixsocket, 0);
  if (rate_limit_group_) {
    bufferevent_add_to_rate_limit_group(bev, rate_limit_group_);
  }
}

Status Worker::listenFD(int fd, uint32_t expected_port, int backlog) {
  const uint32_t port = util::GetLocalPort(fd);
  if (port != expected_port) {
    return {Status::NotOK, "The port of the provided socket fd doesn't match the configured port"};
  }
  const int dup_fd = dup(fd);
  if (dup_fd == -1) {
    return {Status::NotOK, evutil_socket_error_to_string(EVUTIL_SOCKET_ERROR())};
  }
  evconnlistener *lev =
      NewEvconnlistener<&Worker::newTCPConnection>(base_, LEV_OPT_THREADSAFE | LEV_OPT_CLOSE_ON_FREE, backlog, dup_fd);
  listen_events_.emplace_back(lev);
  LOG(INFO) << "Listening on dup'ed fd: " << dup_fd;
  return Status::OK();
}

Status Worker::listenTCP(const std::string &host, uint32_t port, int backlog) {
  bool ipv6_used = strchr(host.data(), ':');

  addrinfo hints = {};
  hints.ai_family = ipv6_used ? AF_INET6 : AF_INET;
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_flags = AI_PASSIVE;

  addrinfo *srv_info = nullptr;
  if (int rv = getaddrinfo(host.data(), std::to_string(port).c_str(), &hints, &srv_info); rv != 0) {
    return {Status::NotOK, gai_strerror(rv)};
  }
  auto exit = MakeScopeExit([srv_info] { freeaddrinfo(srv_info); });

  for (auto p = srv_info; p != nullptr; p = p->ai_next) {
    int fd = socket(p->ai_family, p->ai_socktype, p->ai_protocol);
    if (fd == -1) continue;

    int sock_opt = 1;
    if (ipv6_used && setsockopt(fd, IPPROTO_IPV6, IPV6_V6ONLY, &sock_opt, sizeof(sock_opt)) == -1) {
      return {Status::NotOK, evutil_socket_error_to_string(EVUTIL_SOCKET_ERROR())};
    }

    if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &sock_opt, sizeof(sock_opt)) < 0) {
      return {Status::NotOK, evutil_socket_error_to_string(EVUTIL_SOCKET_ERROR())};
    }

    // to support multi-thread binding on macOS
    if (setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &sock_opt, sizeof(sock_opt)) < 0) {
      return {Status::NotOK, evutil_socket_error_to_string(EVUTIL_SOCKET_ERROR())};
    }

    if (bind(fd, p->ai_addr, p->ai_addrlen)) {
      return {Status::NotOK, evutil_socket_error_to_string(EVUTIL_SOCKET_ERROR())};
    }

    evutil_make_socket_nonblocking(fd);
    auto lev =
        NewEvconnlistener<&Worker::newTCPConnection>(base_, LEV_OPT_THREADSAFE | LEV_OPT_CLOSE_ON_FREE, backlog, fd);
    listen_events_.emplace_back(lev);
  }

  return Status::OK();
}

Status Worker::ListenUnixSocket(const std::string &path, int perm, int backlog) {
  unlink(path.c_str());
  sockaddr_un sa{};
  if (path.size() > sizeof(sa.sun_path) - 1) {
    return {Status::NotOK, "unix socket path too long"};
  }

  sa.sun_family = AF_LOCAL;
  strncpy(sa.sun_path, path.c_str(), sizeof(sa.sun_path) - 1);
  int fd = socket(AF_LOCAL, SOCK_STREAM, 0);
  if (fd == -1) {
    return {Status::NotOK, evutil_socket_error_to_string(EVUTIL_SOCKET_ERROR())};
  }

  if (bind(fd, (sockaddr *)&sa, sizeof(sa)) < 0) {
    return {Status::NotOK, evutil_socket_error_to_string(EVUTIL_SOCKET_ERROR())};
  }

  evutil_make_socket_nonblocking(fd);
  auto lev = NewEvconnlistener<&Worker::newUnixSocketConnection>(base_, LEV_OPT_CLOSE_ON_FREE, backlog, fd);
  listen_events_.emplace_back(lev);
  if (perm != 0) {
    chmod(sa.sun_path, (mode_t)perm);
  }

  return Status::OK();
}

void Worker::Run(std::thread::id tid) {
  tid_ = tid;
  if (event_base_dispatch(base_) != 0) {
    LOG(ERROR) << "[worker] Failed to run server, err: " << strerror(errno);
  }
  is_terminated_ = true;
}

void Worker::Stop(uint32_t wait_seconds) {
  for (const auto &lev : listen_events_) {
    // It's unnecessary to close the listener fd since we have set the LEV_OPT_CLOSE_ON_FREE flag
    evconnlistener_free(lev);
  }
  // wait_seconds == 0 means stop immediately, or it will wait N seconds
  // for the worker to process the remaining requests before stopping.
  if (wait_seconds > 0) {
    timeval tv = {wait_seconds, 0};
    event_base_loopexit(base_, &tv);
  } else {
    event_base_loopbreak(base_);
  }
}

Status Worker::AddConnection(redis::Connection *c) {
  std::unique_lock<std::mutex> lock(conns_mu_);
  auto iter = conns_.find(c->GetFD());
  if (iter != conns_.end()) {
    return {Status::NotOK, "connection was exists"};
  }

  int max_clients = srv->GetConfig()->maxclients;
  if (srv->IncrClientNum() >= max_clients) {
    srv->DecrClientNum();
    return {Status::NotOK, "max number of clients reached"};
  }

  conns_.emplace(c->GetFD(), c);
  uint64_t id = srv->GetClientID();
  c->SetID(id);

  return Status::OK();
}

redis::Connection *Worker::removeConnection(int fd) {
  redis::Connection *conn = nullptr;

  std::unique_lock<std::mutex> lock(conns_mu_);
  auto iter = conns_.find(fd);
  if (iter != conns_.end()) {
    conn = iter->second;
    conns_.erase(iter);
    srv->DecrClientNum();
  }

  iter = monitor_conns_.find(fd);
  if (iter != monitor_conns_.end()) {
    conn = iter->second;
    monitor_conns_.erase(iter);
    srv->DecrClientNum();
    srv->DecrMonitorClientNum();
  }

  return conn;
}

// MigrateConnection moves the connection to another worker
// when reducing the number of workers.
//
// To make it simple, we would close the connection if it's
// blocked on a key or stream.
void Worker::MigrateConnection(Worker *target, redis::Connection *conn) {
  if (!target || !conn) return;

  auto bev = conn->GetBufferEvent();
  // disable read/write event to prevent the connection from being processed during migration
  bufferevent_disable(bev, EV_READ | EV_WRITE);
  // We cannot migrate the connection if it has a running command
  // since it will cause data race since the old worker may still process the command.
  if (!conn->CanMigrate()) {
    // Need to enable read/write event again since we disabled them before
    bufferevent_enable(bev, EV_READ | EV_WRITE);
    return;
  }

  // remove the connection from current worker
  DetachConnection(conn);
  if (!target->AddConnection(conn).IsOK()) {
    conn->Close();
    return;
  }
  bufferevent_base_set(target->base_, bev);
  conn->SetCB(bev);
  bufferevent_enable(bev, EV_READ | EV_WRITE);
  conn->SetOwner(target);
}

void Worker::DetachConnection(redis::Connection *conn) {
  if (!conn) return;

  removeConnection(conn->GetFD());

  if (rate_limit_group_) {
    bufferevent_remove_from_rate_limit_group(conn->GetBufferEvent());
  }

  auto bev = conn->GetBufferEvent();
  bufferevent_disable(bev, EV_READ | EV_WRITE);
  bufferevent_setcb(bev, nullptr, nullptr, nullptr, nullptr);
}

void Worker::FreeConnection(redis::Connection *conn) {
  if (!conn) return;

  removeConnection(conn->GetFD());
  srv->ResetWatchedKeys(conn);
  if (rate_limit_group_) {
    bufferevent_remove_from_rate_limit_group(conn->GetBufferEvent());
  }
  delete conn;
}

void Worker::FreeConnectionByID(int fd, uint64_t id) {
  std::unique_lock<std::mutex> lock(conns_mu_);
  auto iter = conns_.find(fd);
  if (iter != conns_.end() && iter->second->GetID() == id) {
    if (rate_limit_group_ != nullptr) {
      bufferevent_remove_from_rate_limit_group(iter->second->GetBufferEvent());
    }
    delete iter->second;
    conns_.erase(iter);
    srv->DecrClientNum();
  }

  iter = monitor_conns_.find(fd);
  if (iter != monitor_conns_.end() && iter->second->GetID() == id) {
    delete iter->second;
    monitor_conns_.erase(iter);
    srv->DecrClientNum();
    srv->DecrMonitorClientNum();
  }
}

Status Worker::EnableWriteEvent(int fd) {
  std::unique_lock<std::mutex> lock(conns_mu_);
  auto iter = conns_.find(fd);
  if (iter != conns_.end()) {
    auto bev = iter->second->GetBufferEvent();
    bufferevent_enable(bev, EV_WRITE);
    return Status::OK();
  }

  return {Status::NotOK, "connection doesn't exist"};
}

Status Worker::Reply(int fd, const std::string &reply) {
  std::unique_lock<std::mutex> lock(conns_mu_);
  auto iter = conns_.find(fd);
  if (iter != conns_.end()) {
    iter->second->SetLastInteraction();
    redis::Reply(iter->second->Output(), reply);
    return Status::OK();
  }

  return {Status::NotOK, "connection doesn't exist"};
}

void Worker::BecomeMonitorConn(redis::Connection *conn) {
  {
    std::lock_guard<std::mutex> guard(conns_mu_);
    conns_.erase(conn->GetFD());
    monitor_conns_[conn->GetFD()] = conn;
  }
  srv->IncrMonitorClientNum();
  conn->EnableFlag(redis::Connection::kMonitor);
}

void Worker::QuitMonitorConn(redis::Connection *conn) {
  {
    std::lock_guard<std::mutex> guard(conns_mu_);
    monitor_conns_.erase(conn->GetFD());
    conns_[conn->GetFD()] = conn;
  }
  srv->DecrMonitorClientNum();
  conn->DisableFlag(redis::Connection::kMonitor);
}

void Worker::FeedMonitorConns(redis::Connection *conn, const std::string &response) {
  std::unique_lock<std::mutex> lock(conns_mu_);

  for (const auto &iter : monitor_conns_) {
    if (conn == iter.second) continue;  // skip the monitor command

    if (conn->GetNamespace() == iter.second->GetNamespace() || iter.second->GetNamespace() == kDefaultNamespace) {
      iter.second->Reply(response);
    }
  }
}

std::string Worker::GetClientsStr() {
  std::unique_lock<std::mutex> lock(conns_mu_);

  std::string output;
  for (const auto &iter : conns_) {
    redis::Connection *conn = iter.second;
    output.append(conn->ToString());
  }

  return output;
}

void Worker::KillClient(redis::Connection *self, uint64_t id, const std::string &addr, uint64_t type, bool skipme,
                        int64_t *killed) {
  std::lock_guard<std::mutex> guard(conns_mu_);

  for (const auto &iter : conns_) {
    redis::Connection *conn = iter.second;
    if (skipme && self == conn) continue;

    // no need to kill the client again if the kCloseAfterReply flag is set
    if (conn->IsFlagEnabled(redis::Connection::kCloseAfterReply)) {
      continue;
    }

    if ((type & conn->GetClientType()) ||
        (!addr.empty() && (conn->GetAddr() == addr || conn->GetAnnounceAddr() == addr)) ||
        (id != 0 && conn->GetID() == id)) {
      conn->EnableFlag(redis::Connection::kCloseAfterReply);
      // enable write event to notify worker wake up ASAP, and remove the connection
      if (!conn->IsFlagEnabled(redis::Connection::kSlave)) {  // don't enable any event in slave connection
        auto bev = conn->GetBufferEvent();
        bufferevent_enable(bev, EV_WRITE);
      }
      (*killed)++;
    }
  }
}

void Worker::KickoutIdleClients(int timeout) {
  std::vector<std::pair<int, uint64_t>> to_be_killed_conns;

  {
    std::lock_guard<std::mutex> guard(conns_mu_);
    if (conns_.empty()) {
      return;
    }

    int iterations = std::min(static_cast<int>(conns_.size()), 50);
    auto iter = conns_.upper_bound(last_iter_conn_fd_);
    while (iterations--) {
      if (iter == conns_.end()) iter = conns_.begin();
      if (static_cast<int>(iter->second->GetIdleTime()) >= timeout) {
        to_be_killed_conns.emplace_back(iter->first, iter->second->GetID());
      }
      iter++;
    }
    iter--;
    last_iter_conn_fd_ = iter->first;
  }

  for (const auto &conn : to_be_killed_conns) {
    FreeConnectionByID(conn.first, conn.second);
  }
}

void WorkerThread::Start() {
  auto s = util::CreateThread("worker", [this] { this->worker_->Run(std::this_thread::get_id()); });

  if (s) {
    t_ = std::move(*s);
  } else {
    LOG(ERROR) << "[worker] Failed to start worker thread, err: " << s.Msg();
    return;
  }

  LOG(INFO) << "[worker] Thread #" << t_.get_id() << " started";
}

void WorkerThread::Stop(uint32_t wait_seconds) { worker_->Stop(wait_seconds); }

void WorkerThread::Join() {
  if (auto s = util::ThreadJoin(t_); !s) {
    LOG(WARNING) << "[worker] " << s.Msg();
  }
}
