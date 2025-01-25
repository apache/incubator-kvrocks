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
	"crypto/tls"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/shirou/gopsutil/v4/process"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/slices"
)

type KvrocksServer struct {
	t   testing.TB
	cmd *exec.Cmd

	addr    *net.TCPAddr
	tlsAddr *net.TCPAddr

	configs map[string]string

	clean func(bool)
}

func (s *KvrocksServer) HostPort() string {
	return s.addr.AddrPort().String()
}

func (s *KvrocksServer) Host() string {
	return s.addr.AddrPort().Addr().String()
}

func (s *KvrocksServer) Port() uint64 {
	return uint64(s.addr.AddrPort().Port())
}

func (s *KvrocksServer) TLSPort() uint64 {
	return uint64(s.tlsAddr.AddrPort().Port())
}

func (s *KvrocksServer) TLSAddr() string {
	return s.tlsAddr.String()
}

func (s *KvrocksServer) LogFileMatches(t testing.TB, pattern string) bool {
	dir := s.configs["dir"]
	content, err := os.ReadFile(dir + "/kvrocks.INFO")
	require.NoError(t, err)
	p := regexp.MustCompile(pattern)
	return p.Match(content)
}

func (s *KvrocksServer) NewClient() *redis.Client {
	return s.NewClientWithOption(&redis.Options{})
}

func optionsWithTimeouts(options *redis.Options) *redis.Options {
	options.DialTimeout = 30 * time.Second
	options.ReadTimeout = 30 * time.Second
	options.WriteTimeout = 30 * time.Second
	return options
}

func (s *KvrocksServer) NewClientWithOption(options *redis.Options) *redis.Client {
	if options.Addr == "" {
		options.Addr = s.addr.String()
	}

	return redis.NewClient(optionsWithTimeouts(options))
}

func (s *KvrocksServer) NewTCPClient() *TCPClient {
	c, err := net.Dial(s.addr.Network(), s.addr.String())
	require.NoError(s.t, err)
	return newTCPClient(c)
}

func (s *KvrocksServer) NewTCPTLSClient(conf *tls.Config) *TCPClient {
	c, err := tls.Dial(s.tlsAddr.Network(), s.tlsAddr.String(), conf)
	require.NoError(s.t, err)
	return newTCPClient(c)
}

func (s *KvrocksServer) Close() {
	s.close(false)
}

func (s *KvrocksServer) CloseWithoutCleanup() {
	s.close(true)
}

func (s *KvrocksServer) close(keepDir bool) {
	require.NoError(s.t, s.cmd.Process.Signal(syscall.SIGTERM))
	f := func(err error) { require.NoError(s.t, err) }

	var wg sync.WaitGroup
	timer := time.AfterFunc(defaultGracePeriod, func() {
		defer wg.Done()
		wg.Add(1)

		require.NoError(s.t, s.cmd.Process.Kill())
		f = func(err error) {
			// The process may have already exited, so we can't use `require.NoError` here.
			if err != nil {
				require.EqualError(s.t, err, "signal: killed")
			}
		}
	})

	defer func() {
		_ = timer.Stop()
		// Stop function won't wait for the timer routine if it's already expired,
		// so we need to wait for it here to prevent panic.
		wg.Wait()
	}()
	f(s.cmd.Wait())
	s.clean(keepDir)
}

func (s *KvrocksServer) Restart() {
	s.close(true)
	s.Start()
}

func (s *KvrocksServer) Start() {
	b := *binPath
	require.NotEmpty(s.t, b, "please set the binary path by `-binPath`")
	cmd := exec.Command(b)

	dir := s.configs["dir"]
	f, err := os.Open(filepath.Join(dir, "kvrocks.conf"))
	require.NoError(s.t, err)
	defer func() { require.NoError(s.t, f.Close()) }()

	cmd.Args = append(cmd.Args, "-c", f.Name())

	stdout, err := os.Create(filepath.Join(dir, "stdout"))
	require.NoError(s.t, err)
	cmd.Stdout = stdout
	stderr, err := os.Create(filepath.Join(dir, "stderr"))
	require.NoError(s.t, err)
	cmd.Stderr = stderr

	require.NoError(s.t, cmd.Start())

	c := redis.NewClient(&redis.Options{Addr: s.addr.String()})
	defer func() { require.NoError(s.t, c.Close()) }()
	require.Eventually(s.t, func() bool {
		err := c.Ping(context.Background()).Err()
		return err == nil || err.Error() == "NOAUTH Authentication required."
	}, time.Minute, time.Second)

	s.cmd = cmd
	s.clean = func(keepDir bool) {
		require.NoError(s.t, stdout.Close())
		require.NoError(s.t, stderr.Close())
		if *deleteOnExit && !keepDir {
			require.NoError(s.t, os.RemoveAll(dir))
		}
	}
}

func StartTLSServer(t testing.TB, configs map[string]string) *KvrocksServer {
	dir := *workspace
	require.NotEmpty(t, dir, "please set the workspace by `-workspace`")
	dir = filepath.Join(dir, "..", "tls", "cert")

	configs["tls-cert-file"] = filepath.Join(dir, "server.crt")
	configs["tls-key-file"] = filepath.Join(dir, "server.key")
	configs["tls-ca-cert-file"] = filepath.Join(dir, "ca.crt")

	addr, err := findFreePort()
	require.NoError(t, err)
	configs["tls-port"] = fmt.Sprintf("%d", addr.Port)

	s := StartServer(t, configs)
	s.tlsAddr = addr

	return s
}

func StartServer(t testing.TB, configs map[string]string) *KvrocksServer {
	return StartServerWithCLIOptions(t, true, configs, []string{})
}

func StartServerWithCLIOptions(
	t testing.TB,
	withConfigFile bool,
	configs map[string]string,
	options []string,
) *KvrocksServer {

	b := *binPath
	require.NotEmpty(t, b, "please set the binary path by `-binPath`")
	cmd := exec.Command(b)

	addr, err := findFreePort()
	require.NoError(t, err)
	if configs["bind"] == "" {
		configs["bind"] = addr.IP.String()
	}
	configs["port"] = fmt.Sprintf("%d", addr.Port)

	dir := *workspace
	require.NotEmpty(t, dir, "please set the workspace by `-workspace`")
	dir, err = os.MkdirTemp(dir, fmt.Sprintf("%s-%d-*", t.Name(), time.Now().UnixMilli()))
	require.NoError(t, err)
	configs["dir"] = dir

	if withConfigFile {
		f, err := os.Create(filepath.Join(dir, "kvrocks.conf"))
		require.NoError(t, err)
		defer func() { require.NoError(t, f.Close()) }()

		for k := range configs {
			_, err := f.WriteString(fmt.Sprintf("%s %s\n", k, configs[k]))
			require.NoError(t, err)
		}
		cmd.Args = append(cmd.Args, "-c", f.Name())
	} else {
		for k := range configs {
			cmd.Args = append(cmd.Args, fmt.Sprintf("--%s", k), configs[k])
		}
	}
	cmd.Args = append(cmd.Args, options...)

	stdout, err := os.Create(filepath.Join(dir, "stdout"))
	require.NoError(t, err)
	cmd.Stdout = stdout
	stderr, err := os.Create(filepath.Join(dir, "stderr"))
	require.NoError(t, err)
	cmd.Stderr = stderr

	require.NoError(t, cmd.Start())

	c := redis.NewClient(&redis.Options{Addr: addr.String()})
	defer func() { require.NoError(t, c.Close()) }()

	proc, err := process.NewProcess(int32(cmd.Process.Pid))
	require.NoError(t, err)

	var status []string
	require.Eventually(t, func() bool {
		err := c.Ping(context.Background()).Err()
		status, _ = proc.Status()
		return err == nil || err.Error() == "NOAUTH Authentication required." || slices.Contains(status, process.Zombie)
	}, time.Minute, time.Second)
	require.NotContains(t, status, process.Zombie, "Kvrocks has been unexpectedly exited while starting server")

	return &KvrocksServer{
		t:       t,
		cmd:     cmd,
		addr:    addr,
		configs: configs,
		clean: func(keepDir bool) {
			require.NoError(t, stdout.Close())
			require.NoError(t, stderr.Close())
			if *deleteOnExit && !keepDir {
				require.NoError(t, os.RemoveAll(dir))
			}
		},
	}
}

func findFreePort() (*net.TCPAddr, error) {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		return nil, err
	}
	lis, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return nil, err
	}
	defer func() { _ = lis.Close() }()
	return lis.Addr().(*net.TCPAddr), nil
}
