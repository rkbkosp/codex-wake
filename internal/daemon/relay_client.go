package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
	"github.com/rkbkosp/codex-wake/internal/model"
	"github.com/rkbkosp/codex-wake/internal/protocol"
)

type relayUpdate struct {
	typeName string
	sub      model.Subscription
	id       string
}

type RelayClient struct {
	URL      string
	DeviceID string
	Token    string
	Service  *Service
	updates  chan relayUpdate
	dirty    atomic.Bool
}

func NewRelayClient(url, deviceID, token string, service *Service) *RelayClient {
	return &RelayClient{URL: url, DeviceID: deviceID, Token: token, Service: service, updates: make(chan relayUpdate, 256)}
}

func (c *RelayClient) Add(sub model.Subscription) {
	c.enqueue(relayUpdate{typeName: "subscription.add", sub: sub})
}
func (c *RelayClient) Remove(id string) {
	c.enqueue(relayUpdate{typeName: "subscription.remove", id: id})
}
func (c *RelayClient) enqueue(update relayUpdate) {
	select {
	case c.updates <- update:
	default:
		c.dirty.Store(true)
	}
}

func (c *RelayClient) Run(ctx context.Context) error {
	backoff := time.Second
	for ctx.Err() == nil {
		err := c.runConnection(ctx)
		c.Service.SetRelayConnected(false)
		if ctx.Err() != nil {
			return nil
		}
		if err == nil {
			backoff = time.Second
		}
		jitter := time.Duration(rand.Int64N(int64(backoff/2 + 1)))
		timer := time.NewTimer(backoff + jitter)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-timer.C:
		}
		if backoff < 30*time.Second {
			backoff *= 2
			if backoff > 30*time.Second {
				backoff = 30 * time.Second
			}
		}
	}
	return nil
}

func (c *RelayClient) runConnection(ctx context.Context) error {
	header := http.Header{}
	header.Set("Authorization", "Bearer "+c.Token)
	header.Set("X-Codex-Wait-Device", c.DeviceID)
	conn, _, err := websocket.Dial(ctx, c.URL, &websocket.DialOptions{HTTPHeader: header})
	if err != nil {
		return err
	}
	defer conn.CloseNow()
	conn.SetReadLimit(1 << 20)
	if err := c.sync(ctx, conn); err != nil {
		return err
	}
	c.Service.SetRelayConnected(true)
	readErr := make(chan error, 1)
	go func() {
		for {
			_, data, err := conn.Read(ctx)
			if err != nil {
				readErr <- err
				return
			}
			var envelope model.EventEnvelope
			if err := json.Unmarshal(data, &envelope); err != nil {
				readErr <- fmt.Errorf("decode relay event: %w", err)
				return
			}
			if _, _, err := c.Service.AcceptEvent(ctx, envelope); err != nil {
				// A mismatched or stale event is rejected locally without terminating healthy delivery.
				continue
			}
		}
	}()
	dirtyTick := time.NewTicker(time.Second)
	pingTick := time.NewTicker(30 * time.Second)
	defer dirtyTick.Stop()
	defer pingTick.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case err := <-readErr:
			return err
		case update := <-c.updates:
			if err := c.writeUpdate(ctx, conn, update); err != nil {
				return err
			}
		case <-dirtyTick.C:
			if c.dirty.Swap(false) {
				if err := c.sync(ctx, conn); err != nil {
					return err
				}
			}
		case <-pingTick.C:
			pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			err := conn.Ping(pingCtx)
			cancel()
			if err != nil {
				return err
			}
		}
	}
}

func (c *RelayClient) sync(ctx context.Context, conn *websocket.Conn) error {
	state := model.StateArmed
	waits, err := c.Service.DB.ListWaits(ctx, &state)
	if err != nil {
		return err
	}
	subs := make([]model.Subscription, 0, len(waits))
	for _, w := range waits {
		subs = append(subs, w.Subscription())
	}
	msg := protocol.WSMessage{Type: "subscriptions.sync", Version: protocol.Version, Subscriptions: subs}
	return writeJSON(ctx, conn, msg)
}

func (c *RelayClient) writeUpdate(ctx context.Context, conn *websocket.Conn, update relayUpdate) error {
	switch update.typeName {
	case "subscription.add":
		return writeJSON(ctx, conn, protocol.WSMessage{Type: update.typeName, Version: protocol.Version, Subscription: &update.sub})
	case "subscription.remove":
		return writeJSON(ctx, conn, protocol.WSMessage{Type: update.typeName, Version: protocol.Version, ID: update.id})
	default:
		return errors.New("invalid relay update")
	}
}

func writeJSON(ctx context.Context, conn *websocket.Conn, value any) error {
	b, err := json.Marshal(value)
	if err != nil {
		return err
	}
	writeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	return conn.Write(writeCtx, websocket.MessageText, b)
}
