package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/rkbkosp/codex-wake/internal/codex"
	"github.com/rkbkosp/codex-wake/internal/config"
	"github.com/rkbkosp/codex-wake/internal/daemon"
	"github.com/rkbkosp/codex-wake/internal/ipc"
	"github.com/rkbkosp/codex-wake/internal/storage"
	"github.com/rkbkosp/codex-wake/internal/version"
)

func main() {
	if err := run(); err != nil {
		slog.Error("codex-waitd stopped", "error", err)
		os.Exit(1)
	}
}

func run() error {
	defaultDir, err := config.DefaultDataDir()
	if err != nil {
		return err
	}
	dataDirDefault := argValue(os.Args[1:], "--data-dir", envOr("CODEX_WAIT_DATA_DIR", defaultDir))
	configDefault := argValue(os.Args[1:], "--config", filepath.Join(dataDirDefault, "config.toml"))
	local, err := config.LoadLocal(configDefault)
	if err != nil {
		return err
	}
	if local.QueueRetryMaxSeconds == 0 {
		local.QueueRetryMaxSeconds = 300
	}
	if local.CodexBinary == "" {
		if path, lookupErr := exec.LookPath("codex"); lookupErr == nil {
			local.CodexBinary, _ = filepath.Abs(path)
		}
	}
	fs := flag.NewFlagSet("codex-waitd", flag.ContinueOnError)
	dataDir := fs.String("data-dir", dataDirDefault, "state directory")
	configPath := fs.String("config", configDefault, "TOML config file")
	socket := fs.String("socket", config.DefaultSocket(dataDirDefault), "Unix socket path")
	relayURL := fs.String("relay-url", local.RelayURL, "relay WebSocket URL")
	deviceID := fs.String("device-id", local.DeviceID, "relay device id")
	deviceToken := fs.String("device-token", local.DeviceToken, "relay device token")
	codexBinary := fs.String("codex-binary", local.CodexBinary, "absolute Codex CLI path")
	retryMax := fs.Int("queue-retry-max-seconds", local.QueueRetryMaxSeconds, "maximum queue retry interval")
	showVersion := fs.Bool("version", false, "show version")
	if err := fs.Parse(os.Args[1:]); err != nil {
		return err
	}
	_ = configPath
	if *showVersion {
		fmt.Println("codex-waitd " + version.Current)
		return nil
	}
	if fs.NArg() != 0 {
		return errors.New("codex-waitd accepts flags only")
	}
	if *socket == config.DefaultSocket(dataDirDefault) && *dataDir != dataDirDefault {
		*socket = config.DefaultSocket(*dataDir)
	}
	if (*relayURL == "") != (*deviceID == "" || *deviceToken == "") {
		return errors.New("relay-url, device-id, and device-token must be configured together")
	}
	if *relayURL != "" && !strings.HasPrefix(*relayURL, "wss://") && os.Getenv("CODEX_WAIT_ALLOW_INSECURE_WS") != "1" {
		return errors.New("relay URL must use wss:// (set CODEX_WAIT_ALLOW_INSECURE_WS=1 only for local testing)")
	}
	if !filepath.IsAbs(*codexBinary) {
		return errors.New("codex-binary must be an absolute path")
	}
	if err := os.MkdirAll(*dataDir, 0o700); err != nil {
		return err
	}
	if err := os.Chmod(*dataDir, 0o700); err != nil {
		return fmt.Errorf("secure data directory: %w", err)
	}
	db, err := storage.Open(filepath.Join(*dataDir, "state.db"))
	if err != nil {
		return err
	}
	defer db.Close()
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := db.MigrateLocal(ctx); err != nil {
		return err
	}
	binary := codex.Binary{Path: *codexBinary}
	service := daemon.NewService(db, binary)
	service.CodexCheck = binary.Check
	service.RetryMaxInterval = time.Duration(*retryMax) * time.Second
	server, err := ipc.Listen(*socket, service.Handle)
	if err != nil {
		return err
	}
	defer server.Close()
	errCh := make(chan error, 3)
	go func() { errCh <- service.RunDeliveryWorker(ctx) }()
	if *relayURL != "" {
		relay := daemon.NewRelayClient(*relayURL, *deviceID, *deviceToken, service)
		service.Notifier = relay
		go func() { errCh <- relay.Run(ctx) }()
	}
	go func() { errCh <- server.Serve(ctx) }()
	slog.Info("codex-waitd ready", "socket", *socket, "relay", *relayURL != "")
	select {
	case <-ctx.Done():
		return nil
	case err := <-errCh:
		if err != nil {
			cancel()
			return err
		}
		return nil
	}
}

func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
func argValue(args []string, name, fallback string) string {
	for i := 0; i < len(args); i++ {
		if args[i] == name && i+1 < len(args) {
			return args[i+1]
		}
		if strings.HasPrefix(args[i], name+"=") {
			return strings.TrimPrefix(args[i], name+"=")
		}
	}
	return fallback
}
