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

#include <dlfcn.h>
#include <fcntl.h>
#include <getopt.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

#include <iomanip>
#ifdef __linux__
#define _XOPEN_SOURCE 700
#else
#define _XOPEN_SOURCE
#endif
#include <event2/thread.h>
#include <execinfo.h>
#include <glog/logging.h>
#include <signal.h>
#include <sys/un.h>
#include <ucontext.h>

#include "config.h"
#include "fd_util.h"
#include "server/server.h"
#include "storage/storage.h"
#include "util.h"
#include "version.h"

namespace google {
bool Symbolize(void *pc, char *out, size_t out_size);
}  // namespace google

std::function<void()> hup_handler;

Server *srv = nullptr;

Server *GetServer() { return srv; }

extern "C" void signal_handler(int sig) {
  if (hup_handler) hup_handler();
}

extern "C" void segvHandler(int sig, siginfo_t *info, void *secret) {
  void *trace[100];

  LOG(ERROR) << "======= Ooops! kvrocks " << VERSION << " @" << GIT_COMMIT << " got signal: " << strsignal(sig) << " ("
             << sig << ") =======";
  int trace_size = backtrace(trace, sizeof(trace) / sizeof(void *));
  char **messages = backtrace_symbols(trace, trace_size);
  for (int i = 1; i < trace_size; ++i) {
    char func_info[1024] = {};
    if (google::Symbolize(trace[i], func_info, sizeof(func_info) - 1)) {
      LOG(ERROR) << messages[i] << ": " << func_info;
    } else {
      LOG(ERROR) << messages[i];
    }
  }

  struct sigaction act;
  /* Make sure we exit with the right signal at the end. So for instance
   * the core will be dumped if enabled.
   */
  sigemptyset(&act.sa_mask);
  /* When the SA_SIGINFO flag is set in sa_flags then sa_sigaction
   * is used. Otherwise, sa_handler is used
   */
  act.sa_flags = SA_NODEFER | SA_ONSTACK | SA_RESETHAND;
  act.sa_handler = SIG_DFL;
  sigaction(sig, &act, nullptr);
  kill(getpid(), sig);
}

void setupSigSegvAction() {
  struct sigaction act;

  sigemptyset(&act.sa_mask);
  /* When the SA_SIGINFO flag is set in sa_flags then sa_sigaction
   * is used. Otherwise, sa_handler is used */
  act.sa_flags = SA_NODEFER | SA_ONSTACK | SA_RESETHAND | SA_SIGINFO;
  act.sa_sigaction = segvHandler;
  sigaction(SIGSEGV, &act, nullptr);
  sigaction(SIGBUS, &act, nullptr);
  sigaction(SIGFPE, &act, nullptr);
  sigaction(SIGILL, &act, nullptr);
  sigaction(SIGABRT, &act, nullptr);

  act.sa_flags = SA_NODEFER | SA_ONSTACK | SA_RESETHAND;
  act.sa_handler = signal_handler;
  sigaction(SIGTERM, &act, nullptr);
  sigaction(SIGINT, &act, nullptr);
}

constexpr const char *description =
    "a distributed key value NoSQL database that uses RocksDB as storage engine and is compatible with Redis protocol";

static void printUsage(const char *program) {
  const int width = 32;

  std::cout << program << description << std::endl
            << std::left << std::setw(width) << "-c, --config <filename>"
            << "set config file to <filename>, or `-' for stdin" << std::endl
            << std::setw(width) << "-v, --version"
            << "print version information" << std::endl
            << std::setw(width) << "-h, --help"
            << "print this help message" << std::endl
            << std::setw(width) << "--<config-key> <config-value>"
            << "overwrite specific config option <config-key> to <config-value>" << std::endl;
}

static void printVersion(std::ostream &os) { os << "kvrocks version " << VERSION << " @" << GIT_COMMIT << std::endl; }

static CLIOptions parseCommandLineOptions(int argc, char **argv) {
  using namespace std::string_view_literals;
  CLIOptions opts;

  for (int i = 1; i < argc; ++i) {
    if ((argv[i] == "-c"sv || argv[i] == "--config"sv) && i + 1 < argc) {
      opts.conf_file = argv[++i];
    } else if (argv[i] == "-v"sv || argv[i] == "--version"sv) {
      printVersion(std::cout);
      std::exit(0);
    } else if (argv[i] == "-h"sv || argv[i] == "--help"sv) {
      printUsage(*argv);
      std::exit(0);
    } else if (std::string_view(argv[i], 2) == "--" && std::string_view(argv[i]).size() > 2 && i + 1 < argc) {
      auto key = std::string_view(argv[i] + 2);
      opts.cli_options.emplace_back(key, argv[++i]);
    } else {
      printUsage(*argv);
      std::exit(1);
    }
  }

  return opts;
}

static void initGoogleLog(const Config *config) {
  FLAGS_minloglevel = config->loglevel;
  FLAGS_max_log_size = 100;
  FLAGS_logbufsecs = 0;

  if (Util::ToLower(config->log_dir) == "stdout") {
    for (int level = google::INFO; level <= google::FATAL; level++) {
      google::SetLogDestination(level, "");
    }
    FLAGS_stderrthreshold = google::ERROR;
    FLAGS_logtostdout = true;
  } else {
    FLAGS_log_dir = config->log_dir;
  }
}

bool supervisedUpstart() {
  const char *upstart_job = getenv("UPSTART_JOB");
  if (!upstart_job) {
    LOG(WARNING) << "upstart supervision requested, but UPSTART_JOB not found";
    return false;
  }
  LOG(INFO) << "supervised by upstart, will stop to signal readiness";
  raise(SIGSTOP);
  unsetenv("UPSTART_JOB");
  return true;
}

bool supervisedSystemd() {
  const char *notify_socket = getenv("NOTIFY_SOCKET");
  if (!notify_socket) {
    LOG(WARNING) << "systemd supervision requested, but NOTIFY_SOCKET not found";
    return false;
  }

  auto fd = UniqueFD(socket(AF_UNIX, SOCK_DGRAM, 0));
  if (!fd) {
    LOG(WARNING) << "Can't connect to systemd socket " << notify_socket;
    return false;
  }

  struct sockaddr_un su;
  memset(&su, 0, sizeof(su));
  su.sun_family = AF_UNIX;
  strncpy(su.sun_path, notify_socket, sizeof(su.sun_path) - 1);
  su.sun_path[sizeof(su.sun_path) - 1] = '\0';
  if (notify_socket[0] == '@') su.sun_path[0] = '\0';

  struct iovec iov;
  memset(&iov, 0, sizeof(iov));
  std::string ready = "READY=1";
  iov.iov_base = &ready[0];
  iov.iov_len = ready.size();

  struct msghdr hdr;
  memset(&hdr, 0, sizeof(hdr));
  hdr.msg_name = &su;
  hdr.msg_namelen = offsetof(struct sockaddr_un, sun_path) + strlen(notify_socket);
  hdr.msg_iov = &iov;
  hdr.msg_iovlen = 1;

  int sendto_flags = 0;
  unsetenv("NOTIFY_SOCKET");
#ifdef HAVE_MSG_NOSIGNAL
  sendto_flags |= MSG_NOSIGNAL;
#endif
  if (sendmsg(*fd, &hdr, sendto_flags) < 0) {
    LOG(WARNING) << "Can't send notification to systemd";
    return false;
  }
  return true;
}

bool isSupervisedMode(int mode) {
  if (mode == kSupervisedAutoDetect) {
    const char *upstart_job = getenv("UPSTART_JOB");
    const char *notify_socket = getenv("NOTIFY_SOCKET");
    if (upstart_job) {
      mode = kSupervisedUpStart;
    } else if (notify_socket) {
      mode = kSupervisedSystemd;
    }
  }
  if (mode == kSupervisedUpStart) {
    return supervisedUpstart();
  } else if (mode == kSupervisedSystemd) {
    return supervisedSystemd();
  }
  return false;
}

static Status createPidFile(const std::string &path) {
  auto fd = UniqueFD(open(path.data(), O_RDWR | O_CREAT, 0660));
  if (!fd) {
    return Status(Status::NotOK, strerror(errno));
  }
  std::string pid_str = std::to_string(getpid());
  write(*fd, pid_str.data(), pid_str.size());
  return Status::OK();
}

static void removePidFile(const std::string &path) { std::remove(path.data()); }

static void daemonize() {
  pid_t pid;

  pid = fork();
  if (pid < 0) {
    LOG(ERROR) << "Failed to fork the process, err: " << strerror(errno);
    exit(1);
  }
  if (pid > 0) exit(EXIT_SUCCESS);  // parent process
  // change the file mode
  umask(0);
  if (setsid() < 0) {
    LOG(ERROR) << "Failed to setsid, err: %s" << strerror(errno);
    exit(1);
  }
  close(STDIN_FILENO);
  close(STDOUT_FILENO);
  close(STDERR_FILENO);
}

int main(int argc, char *argv[]) {
  google::InitGoogleLogging("kvrocks");
  evthread_use_pthreads();

  signal(SIGPIPE, SIG_IGN);
  signal(SIGINT, signal_handler);
  signal(SIGTERM, signal_handler);
  setupSigSegvAction();

  auto opts = parseCommandLineOptions(argc, argv);

  Redis::InitCommandsTable();
  Redis::PopulateCommands();

  Config config;
  Status s = config.Load(opts);
  if (!s.IsOK()) {
    std::cout << "Failed to load config, err: " << s.Msg() << std::endl;
    exit(1);
  }
  initGoogleLog(&config);
  printVersion(LOG(INFO));
  // Tricky: We don't expect that different instances running on the same port,
  // but the server use REUSE_PORT to support the multi listeners. So we connect
  // the listen port to check if the port has already listened or not.
  if (!config.binds.empty()) {
    int ports[] = {config.port, config.tls_port, 0};
    for (int *port = ports; *port; ++port) {
      if (Util::IsPortInUse(*port)) {
        LOG(ERROR) << "Could not create server TCP since the specified port[" << *port << "] is already in use"
                   << std::endl;
        exit(1);
      }
    }
  }
  bool is_supervised = isSupervisedMode(config.supervised_mode);
  if (config.daemonize && !is_supervised) daemonize();
  s = createPidFile(config.pidfile);
  if (!s.IsOK()) {
    LOG(ERROR) << "Failed to create pidfile: " << s.Msg();
    exit(1);
  }

#ifdef ENABLE_OPENSSL
  // initialize OpenSSL
  if (config.tls_port) {
    InitSSL();
  }
#endif

  Engine::Storage storage(&config);
  s = storage.Open();
  if (!s.IsOK()) {
    LOG(ERROR) << "Failed to open: " << s.Msg();
    removePidFile(config.pidfile);
    exit(1);
  }
  srv = new Server(&storage, &config);
  hup_handler = [] {
    if (!srv->IsStopped()) {
      LOG(INFO) << "Bye Bye";
      srv->Stop();
    }
  };
  s = srv->Start();
  if (!s.IsOK()) {
    removePidFile(config.pidfile);
    exit(1);
  }
  srv->Join();

  delete srv;
  removePidFile(config.pidfile);
  google::ShutdownGoogleLogging();
  libevent_global_shutdown();
  return 0;
}
