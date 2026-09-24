package mcp

import (
	"container/list"
	"context"
	"crypto/rand"
	"encoding/hex"
	"net/http"
	"sync"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/middleware"
)

// HeaderSessionID is the Streamable HTTP session header.
const HeaderSessionID = "Mcp-Session-Id"

// Session defaults.
const (
	DefaultSessionIdleTimeout = 30 * time.Minute
	DefaultMaxSessions        = 10000
	maxSessionIDLen           = 128
)

// Implementation-defined JSON-RPC error codes used for HTTP-level session
// errors (they match the codes used by the reference SDKs).
const (
	CodeSessionRequired = -32000
	CodeSessionNotFound = -32001
)

// SessionInfo describes an MCP session.
type SessionInfo struct {
	ID              string
	ProtocolVersion string
	ClientName      string
	ClientVersion   string
	CreatedAt       time.Time
	LastSeen        time.Time
}

type mcpSession struct {
	info  SessionInfo
	owner string
	elem  *list.Element
}

// sessionManager tracks sessions in LRU order (front = most recently used).
// Idle sessions expire; when full, the least recently used is evicted (its
// client gets 404 and re-initializes, as the spec prescribes).
type sessionManager struct {
	mu   sync.Mutex
	idle time.Duration
	max  int
	now  func() time.Time
	byID map[string]*mcpSession
	lru  *list.List
}

func newSessionManager() *sessionManager {
	return &sessionManager{
		idle: DefaultSessionIdleTimeout,
		max:  DefaultMaxSessions,
		now:  time.Now,
		byID: make(map[string]*mcpSession),
		lru:  list.New(),
	}
}

func newSessionID() (string, error) {
	var b [32]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

// validSessionID reports whether id is a plausible session ID: non-empty,
// bounded and made of visible ASCII (0x21-0x7E) as the spec requires.
func validSessionID(id string) bool {
	if id == "" || len(id) > maxSessionIDLen {
		return false
	}
	for i := 0; i < len(id); i++ {
		if id[i] < 0x21 || id[i] > 0x7E {
			return false
		}
	}
	return true
}

func (m *sessionManager) create(owner, version, clientName, clientVersion string) (SessionInfo, error) {
	id, err := newSessionID()
	if err != nil {
		return SessionInfo{}, err
	}
	now := m.now()
	m.mu.Lock()
	defer m.mu.Unlock()
	m.expireLocked(now)
	for len(m.byID) >= m.max && m.lru.Len() > 0 {
		m.removeLocked(m.lru.Back().Value.(*mcpSession))
	}
	sess := &mcpSession{
		info: SessionInfo{
			ID:              id,
			ProtocolVersion: version,
			ClientName:      clientName,
			ClientVersion:   clientVersion,
			CreatedAt:       now,
			LastSeen:        now,
		},
		owner: owner,
	}
	sess.elem = m.lru.PushFront(sess)
	m.byID[id] = sess
	return sess.info, nil
}

// touch returns the live session with id owned by owner and marks it used.
// Unknown, expired and foreign sessions are indistinguishable (not found).
func (m *sessionManager) touch(id, owner string) (SessionInfo, bool) {
	if !validSessionID(id) {
		return SessionInfo{}, false
	}
	now := m.now()
	m.mu.Lock()
	defer m.mu.Unlock()
	m.expireLocked(now)
	sess, ok := m.byID[id]
	if !ok || sess.owner != owner {
		return SessionInfo{}, false
	}
	sess.info.LastSeen = now
	m.lru.MoveToFront(sess.elem)
	return sess.info, true
}

// remove terminates the session with id owned by owner.
func (m *sessionManager) remove(id, owner string) bool {
	if !validSessionID(id) {
		return false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.expireLocked(m.now())
	sess, ok := m.byID[id]
	if !ok || sess.owner != owner {
		return false
	}
	m.removeLocked(sess)
	return true
}

func (m *sessionManager) count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.expireLocked(m.now())
	return len(m.byID)
}

// expireLocked drops idle sessions; they sit at the back of the LRU list.
func (m *sessionManager) expireLocked(now time.Time) {
	for el := m.lru.Back(); el != nil; el = m.lru.Back() {
		sess := el.Value.(*mcpSession)
		if now.Sub(sess.info.LastSeen) < m.idle {
			return
		}
		m.removeLocked(sess)
	}
}

func (m *sessionManager) removeLocked(sess *mcpSession) {
	m.lru.Remove(sess.elem)
	delete(m.byID, sess.info.ID)
}

// defaultSessionOwner binds sessions to the authenticated gateway principal
// (the key ID set by the auth middleware), so a leaked session ID cannot be
// used with another key.
func defaultSessionOwner(r *http.Request) string {
	if p, ok := middleware.PrincipalFromContext(r.Context()); ok {
		return p.KeyID
	}
	return ""
}

type sessionCtxKey struct{}

func withSessionID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, sessionCtxKey{}, id)
}

// SessionIDFromContext returns the MCP session ID of the request being
// served (empty in stateless mode or for session-less pings). Tool handlers,
// resource and prompt providers and the tool biller receive it in ctx.
func SessionIDFromContext(ctx context.Context) string {
	id, _ := ctx.Value(sessionCtxKey{}).(string)
	return id
}
