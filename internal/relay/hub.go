package relay

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/google/uuid"
	"github.com/rkbkosp/codex-wake/internal/model"
	"github.com/rkbkosp/codex-wake/internal/protocol"
	"github.com/rkbkosp/codex-wake/internal/storage"
)

type Authenticator struct{ Tokens map[string]string }

func (a Authenticator) Authenticate(r *http.Request) (string, bool) {
	header := r.Header.Get("Authorization")
	if !strings.HasPrefix(header, "Bearer ") {
		return "", false
	}
	wantDevice := r.Header.Get("X-Codex-Wait-Device")
	presented := strings.TrimPrefix(header, "Bearer ")
	matched := ""
	for device, token := range a.Tokens {
		tokenOK := subtle.ConstantTimeCompare([]byte(presented), []byte(token)) == 1
		deviceOK := wantDevice == "" || subtle.ConstantTimeCompare([]byte(wantDevice), []byte(device)) == 1
		if tokenOK && deviceOK {
			matched = device
		}
	}
	return matched, matched != ""
}

type wsClient struct {
	device string
	conn   *websocket.Conn
	ctx    context.Context
	cancel context.CancelFunc
	send   chan model.EventEnvelope
	mu     sync.RWMutex
	subs   map[string]model.Subscription
}

func (c *wsClient) subscriptions() []model.Subscription {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]model.Subscription, 0, len(c.subs))
	for _, sub := range c.subs {
		out = append(out, sub)
	}
	return out
}

func (c *wsClient) replace(subs []model.Subscription) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.subs = make(map[string]model.Subscription, len(subs))
	for _, sub := range subs {
		c.subs[sub.ID] = sub
	}
}

func (c *wsClient) add(sub model.Subscription) { c.mu.Lock(); c.subs[sub.ID] = sub; c.mu.Unlock() }
func (c *wsClient) remove(id string)           { c.mu.Lock(); delete(c.subs, id); c.mu.Unlock() }
func (c *wsClient) enqueue(e model.EventEnvelope) {
	select {
	case c.send <- e:
	case <-c.ctx.Done():
	default:
		c.cancel()
	}
}

type Hub struct {
	DB      *storage.DB
	mu      sync.RWMutex
	clients map[*wsClient]struct{}
}

func NewHub(db *storage.DB) *Hub  { return &Hub{DB: db, clients: make(map[*wsClient]struct{})} }
func (h *Hub) add(c *wsClient)    { h.mu.Lock(); h.clients[c] = struct{}{}; h.mu.Unlock() }
func (h *Hub) remove(c *wsClient) { h.mu.Lock(); delete(h.clients, c); h.mu.Unlock() }

func (h *Hub) Dispatch(f storage.MergeFact) {
	h.mu.RLock()
	clients := make([]*wsClient, 0, len(h.clients))
	for c := range h.clients {
		clients = append(clients, c)
	}
	h.mu.RUnlock()
	for _, c := range clients {
		for _, sub := range c.subscriptions() {
			if subscriptionMatches(sub, f) {
				c.enqueue(eventFor(sub, f))
			}
		}
	}
}

func subscriptionMatches(sub model.Subscription, f storage.MergeFact) bool {
	return sub.Provider == model.ProviderGitHub && sub.Event == model.EventPRMerged && strings.ToLower(sub.Repository) == f.RepositoryKey && sub.PRNumber == f.PRNumber
}

func eventFor(sub model.Subscription, f storage.MergeFact) model.EventEnvelope {
	return model.EventEnvelope{Type: "event", Version: protocol.Version, SubscriptionID: sub.ID, EventID: f.DeliveryID, Event: f.Event()}
}

type WSServer struct {
	Hub  *Hub
	Auth Authenticator
}

func (s WSServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	device, ok := s.Auth.Authenticate(r)
	if !ok {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	conn, err := websocket.Accept(w, r, nil)
	if err != nil {
		return
	}
	conn.SetReadLimit(1 << 20)
	ctx, cancel := context.WithCancel(r.Context())
	c := &wsClient{device: device, conn: conn, ctx: ctx, cancel: cancel, send: make(chan model.EventEnvelope, 128), subs: make(map[string]model.Subscription)}
	s.Hub.add(c)
	defer func() { cancel(); s.Hub.remove(c); conn.CloseNow() }()
	go c.writeLoop()
	for {
		_, data, err := conn.Read(ctx)
		if err != nil {
			return
		}
		if err := s.handleMessage(ctx, c, data); err != nil {
			_ = conn.Close(websocket.StatusPolicyViolation, err.Error())
			return
		}
	}
}

func (c *wsClient) writeLoop() {
	for {
		select {
		case <-c.ctx.Done():
			return
		case event := <-c.send:
			b, err := json.Marshal(event)
			if err != nil {
				c.cancel()
				return
			}
			ctx, cancel := context.WithTimeout(c.ctx, 10*time.Second)
			err = c.conn.Write(ctx, websocket.MessageText, b)
			cancel()
			if err != nil {
				c.cancel()
				c.conn.CloseNow()
				return
			}
		}
	}
}

func (s WSServer) handleMessage(ctx context.Context, c *wsClient, data []byte) error {
	var msg protocol.WSMessage
	dec := json.NewDecoder(strings.NewReader(string(data)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&msg); err != nil {
		return fmt.Errorf("invalid message: %w", err)
	}
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("trailing JSON values are not allowed")
	}
	if msg.Version != protocol.Version {
		return errors.New("unsupported protocol version")
	}
	switch msg.Type {
	case "subscriptions.sync":
		if len(msg.Subscriptions) > 10000 {
			return errors.New("too many subscriptions")
		}
		for _, sub := range msg.Subscriptions {
			if err := validateSubscription(sub); err != nil {
				return err
			}
		}
		c.replace(msg.Subscriptions)
		for _, sub := range msg.Subscriptions {
			if err := s.replayIfSatisfied(ctx, c, sub); err != nil {
				return err
			}
		}
	case "subscription.add":
		if msg.Subscription == nil {
			return errors.New("subscription is required")
		}
		if err := validateSubscription(*msg.Subscription); err != nil {
			return err
		}
		c.add(*msg.Subscription)
		return s.replayIfSatisfied(ctx, c, *msg.Subscription)
	case "subscription.remove":
		if err := validateWaitID(msg.ID); err != nil {
			return err
		}
		c.remove(msg.ID)
	default:
		return fmt.Errorf("unsupported message type %q", msg.Type)
	}
	return nil
}

func validateSubscription(sub model.Subscription) error {
	if err := validateWaitID(sub.ID); err != nil {
		return err
	}
	if sub.Provider != model.ProviderGitHub || sub.Event != model.EventPRMerged || sub.PRNumber <= 0 {
		return errors.New("unsupported subscription")
	}
	ref, err := model.ParseGitHubPRURL(model.ConstructPRURL(sub.Repository, sub.PRNumber))
	if err != nil || ref.RepositoryKey != sub.Repository {
		return errors.New("repository must be a lowercase canonical key")
	}
	return nil
}

func validateWaitID(id string) error {
	if !strings.HasPrefix(id, "wait_") {
		return errors.New("invalid wait id")
	}
	parsed, err := uuid.Parse(strings.TrimPrefix(id, "wait_"))
	if err != nil || "wait_"+parsed.String() != id {
		return errors.New("invalid wait id")
	}
	return nil
}

func (s WSServer) replayIfSatisfied(ctx context.Context, c *wsClient, sub model.Subscription) error {
	fact, ok, err := s.Hub.DB.GetMergeFact(ctx, sub.Repository, sub.PRNumber)
	if err != nil {
		return err
	}
	if ok {
		c.enqueue(eventFor(sub, fact))
	}
	return nil
}
