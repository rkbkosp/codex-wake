package ipc

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/rkbkosp/codex-wake/internal/protocol"
)

const maxMessageBytes = 1 << 20

type Handler func(context.Context, protocol.Request) protocol.Response

type Server struct {
	path       string
	listener   net.Listener
	handler    Handler
	socketInfo os.FileInfo
}

func Listen(path string, handler Handler) (*Server, error) {
	if len(path) > 96 {
		return nil, fmt.Errorf("unix socket path is too long (%d bytes); choose a shorter --socket path", len(path))
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, fmt.Errorf("create socket directory: %w", err)
	}
	if info, err := os.Lstat(path); err == nil {
		if info.Mode()&os.ModeSocket == 0 {
			return nil, fmt.Errorf("refusing to replace non-socket path %s", path)
		}
		if conn, dialErr := net.DialTimeout("unix", path, 250*time.Millisecond); dialErr == nil {
			conn.Close()
			return nil, fmt.Errorf("another daemon is already listening at %s", path)
		}
		if err := os.Remove(path); err != nil {
			return nil, fmt.Errorf("remove stale socket: %w", err)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	l, err := net.Listen("unix", path)
	if err != nil {
		return nil, err
	}
	if err := os.Chmod(path, 0o600); err != nil {
		l.Close()
		return nil, err
	}
	info, err := os.Lstat(path)
	if err != nil {
		l.Close()
		return nil, err
	}
	return &Server{path: path, listener: l, handler: handler, socketInfo: info}, nil
}

func (s *Server) Serve(ctx context.Context) error {
	go func() {
		<-ctx.Done()
		s.listener.Close()
	}()
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return nil
			}
			return err
		}
		go s.serveConn(ctx, conn)
	}
}

func (s *Server) serveConn(ctx context.Context, conn net.Conn) {
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(15 * time.Second))
	reader := bufio.NewReaderSize(conn, 32*1024)
	line, err := reader.ReadBytes('\n')
	if err != nil || len(line) > maxMessageBytes {
		return
	}
	var req protocol.Request
	if err := json.Unmarshal(line, &req); err != nil {
		_ = json.NewEncoder(conn).Encode(protocol.Response{Version: protocol.Version, Error: &protocol.Error{Code: "invalid_json", Message: err.Error()}})
		return
	}
	resp := s.handler(ctx, req)
	resp.Version = protocol.Version
	resp.ID = req.ID
	_ = json.NewEncoder(conn).Encode(resp)
}

func (s *Server) Close() error {
	err := s.listener.Close()
	var removeErr error
	if current, statErr := os.Lstat(s.path); statErr == nil {
		if os.SameFile(s.socketInfo, current) {
			removeErr = os.Remove(s.path)
		}
	} else if !errors.Is(statErr, os.ErrNotExist) {
		removeErr = statErr
	}
	if err != nil && !errors.Is(err, net.ErrClosed) {
		return err
	}
	if removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
		return removeErr
	}
	return nil
}

type Client struct {
	SocketPath string
	Timeout    time.Duration
}

func (c Client) Call(ctx context.Context, method string, params any, result any) error {
	if c.Timeout == 0 {
		c.Timeout = 5 * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, c.Timeout)
	defer cancel()
	d := net.Dialer{}
	conn, err := d.DialContext(ctx, "unix", c.SocketPath)
	if err != nil {
		return fmt.Errorf("connect to codex-waitd at %s: %w", c.SocketPath, err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(c.Timeout))
	b, err := json.Marshal(params)
	if err != nil {
		return err
	}
	req := protocol.Request{Version: protocol.Version, ID: "cli", Method: method, Params: b}
	if err := json.NewEncoder(conn).Encode(req); err != nil {
		return err
	}
	var resp protocol.Response
	if err := json.NewDecoder(conn).Decode(&resp); err != nil {
		return fmt.Errorf("read daemon response: %w", err)
	}
	if resp.Version != protocol.Version {
		return fmt.Errorf("daemon protocol version %d is not supported", resp.Version)
	}
	if resp.Error != nil {
		return fmt.Errorf("%s: %s", resp.Error.Code, resp.Error.Message)
	}
	if result == nil {
		return nil
	}
	if err := json.Unmarshal(resp.Result, result); err != nil {
		return fmt.Errorf("decode daemon result: %w", err)
	}
	return nil
}
