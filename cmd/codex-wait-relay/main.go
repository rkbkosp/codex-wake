package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/rkbkosp/codex-wake/internal/relay"
	"github.com/rkbkosp/codex-wake/internal/storage"
	"github.com/rkbkosp/codex-wake/internal/version"
)

func main() {
	if err := run(); err != nil {
		slog.Error("relay stopped", "error", err)
		os.Exit(1)
	}
}

func run() error {
	fs := flag.NewFlagSet("codex-wait-relay", flag.ContinueOnError)
	listen := fs.String("listen", envOr("LISTEN_ADDR", "127.0.0.1:8787"), "listen address")
	database := fs.String("database", envOr("DATABASE_PATH", "relay.db"), "SQLite database path")
	webhookSecret := fs.String("webhook-secret", os.Getenv("WEBHOOK_SECRET"), "GitHub webhook secret")
	deviceJSON := fs.String("device-auth-keys", os.Getenv("DEVICE_AUTH_KEYS"), "JSON object mapping device ids to bearer tokens")
	debugToken := fs.String("debug-token", os.Getenv("DEBUG_TOKEN"), "optional debug endpoint bearer token")
	tlsCert := fs.String("tls-cert", os.Getenv("TLS_CERT_FILE"), "TLS certificate file")
	tlsKey := fs.String("tls-key", os.Getenv("TLS_KEY_FILE"), "TLS private key file")
	showVersion := fs.Bool("version", false, "show version")
	if err := fs.Parse(os.Args[1:]); err != nil {
		return err
	}
	if *showVersion {
		fmt.Println("codex-wait-relay " + version.Current)
		return nil
	}
	if *webhookSecret == "" {
		return errors.New("WEBHOOK_SECRET is required")
	}
	var tokens map[string]string
	if err := json.Unmarshal([]byte(*deviceJSON), &tokens); err != nil || len(tokens) == 0 {
		return errors.New("DEVICE_AUTH_KEYS must be a non-empty JSON object")
	}
	for id, token := range tokens {
		if id == "" || token == "" {
			return errors.New("device ids and tokens must be non-empty")
		}
	}
	if (*tlsCert == "") != (*tlsKey == "") {
		return errors.New("tls-cert and tls-key must be provided together")
	}
	db, err := storage.Open(*database)
	if err != nil {
		return err
	}
	defer db.Close()
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := db.MigrateRelay(ctx); err != nil {
		return err
	}
	hub := relay.NewHub(db)
	app := &relay.Server{DB: db, Hub: hub, WebhookSecret: []byte(*webhookSecret), DeviceTokens: tokens, DebugToken: *debugToken}
	httpServer := &http.Server{Addr: *listen, Handler: app.Handler(), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 15 * time.Second, IdleTimeout: 75 * time.Second, MaxHeaderBytes: 32 << 10}
	errCh := make(chan error, 1)
	go func() {
		slog.Info("codex-wait-relay ready", "listen", *listen, "tls", *tlsCert != "")
		if *tlsCert != "" {
			errCh <- httpServer.ListenAndServeTLS(*tlsCert, *tlsKey)
		} else {
			errCh <- httpServer.ListenAndServe()
		}
	}()
	select {
	case <-ctx.Done():
		shutdownCtx, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		return httpServer.Shutdown(shutdownCtx)
	case err := <-errCh:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}

func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
