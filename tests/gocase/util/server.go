package util

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v9"
	"github.com/stretchr/testify/require"
	"net"
	"os"
	"os/exec"
	"testing"
	"time"
)

type KvrocksServer struct {
	t   *testing.T
	cmd *exec.Cmd
	r   *redis.Client

	clean func()
}

func (s *KvrocksServer) Client() *redis.Client {
	return s.r
}

func (s *KvrocksServer) Close() {
	require.NoError(s.t, s.cmd.Process.Kill())
	require.EqualError(s.t, s.cmd.Wait(), "signal: killed")
}

func StartServer(t *testing.T, configs map[string]string) (*KvrocksServer, error) {
	b := os.Getenv("KVROCKS_BIN_PATH")
	cmd := exec.Command(b)

	addr, err := findFreePort()
	if err != nil {
		return nil, err
	}
	configs["bind"] = addr.IP.String()
	configs["port"] = fmt.Sprintf("%d", addr.Port)

	dir := os.Getenv("GO_CASE_WORKSPACE")
	require.NoError(t, err)
	dir, err = os.MkdirTemp(dir, "Server-*")
	require.NoError(t, err)
	configs["dir"] = dir

	f, err := os.CreateTemp(dir, "*.conf")
	if err != nil {
		return nil, err
	}

	for k := range configs {
		_, err := f.WriteString(fmt.Sprintf("%s %s\n", k, configs[k]))
		if err != nil {
			return nil, err
		}
	}

	cmd.Args = append(cmd.Args, "-c", f.Name())

	stdout, err := os.Create(fmt.Sprintf("%s/%s", dir, "stdout"))
	require.NoError(t, err)
	cmd.Stdout = stdout
	stderr, err := os.Create(fmt.Sprintf("%s/%s", dir, "stderr"))
	require.NoError(t, err)
	cmd.Stderr = stderr

	if err := cmd.Start(); err != nil {
		return nil, err
	}

	r := redis.NewClient(&redis.Options{Addr: addr.String()})
	require.Eventually(t, func() bool {
		return r.Ping(context.Background()).Err() == nil
	}, time.Minute, time.Second)

	return &KvrocksServer{
		t:   t,
		cmd: cmd,
		r:   r,
		clean: func() {
			stdout.Close()
			stderr.Close()
		},
	}, nil
}

func findFreePort() (*net.TCPAddr, error) {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		return nil, err
	}
	lis, err := net.ListenTCP("tcp", addr)
	defer func() { _ = lis.Close() }()
	return lis.Addr().(*net.TCPAddr), nil
}
