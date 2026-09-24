package keymanager

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"
)

// User represents an agency user.
type User struct {
	ID        string                 `json:"id"`
	Email     string                 `json:"email"`
	Role      string                 `json:"role"`
	Metadata  map[string]interface{} `json:"metadata"`
	CreatedAt time.Time              `json:"created_at"`
}

// Team represents a team within an agency.
//
// Budget is the team's maximum budget in USD (0 = unlimited). It is enforced
// across all keys of the team: spend recorded for any key whose TeamID is
// the team's ID also accrues to Spend (see Manager.RecordSpendByHash), and
// Validate/CheckBudget reject every key of the team once Spend reaches
// Budget (ErrTeamBudgetExceeded, which matches ErrBudgetExceeded).
type Team struct {
	ID        string                 `json:"id"`
	Name      string                 `json:"name"`
	Members   []string               `json:"members"`
	Budget    float64                `json:"budget"`
	Metadata  map[string]interface{} `json:"metadata"`
	CreatedAt time.Time              `json:"created_at"`

	// Spend is the accumulated spend in USD of all keys of the team in the
	// current budget period.
	Spend float64 `json:"spend"`
	// BudgetDuration, if set (e.g. "30d"), resets Spend periodically.
	BudgetDuration string `json:"budget_duration,omitempty"`
	// BudgetResetAt is when Spend is next reset (zero if no BudgetDuration).
	BudgetResetAt time.Time `json:"budget_reset_at,omitempty"`
}

// UserStore persists users. Get returns an error matching ErrUserNotFound
// for unknown users.
type UserStore interface {
	Create(ctx context.Context, u *User) error
	Get(ctx context.Context, id string) (*User, error)
	Update(ctx context.Context, u *User) error
}

// TeamStore persists teams. Get returns an error matching ErrTeamNotFound
// for unknown teams. Update replaces the stored team (keeping CreatedAt);
// stores should also implement AtomicTeamUpdater so read-modify-write
// updates (spend accrual, partial updates) never lose concurrent changes.
type TeamStore interface {
	Create(ctx context.Context, t *Team) error
	Get(ctx context.Context, id string) (*Team, error)
	Update(ctx context.Context, t *Team) error
}

// AtomicTeamUpdater is optionally implemented by team stores that can apply
// a read-modify-write atomically. fn receives a private copy; the team's ID
// and CreatedAt are immutable. The updated team is returned.
type AtomicTeamUpdater interface {
	UpdateFunc(ctx context.Context, id string, fn func(*Team) error) (*Team, error)
}

// ---------------------------------------------------------------------------
// Principals and authorization
// ---------------------------------------------------------------------------

// Principal is the authenticated caller of the key-management API.
type Principal struct {
	// Admin callers (master key, configured admin keys, or a request marked
	// by ContextWithAdmin) may manage every key, user and team.
	Admin bool
	// Key is the caller's virtual key when the caller is not an admin.
	Key *VirtualKey
}

type principalContextKey struct{}

// ContextWithPrincipal marks a request context with an already-authenticated
// principal. Use it from trusted middleware (e.g. the gateway's admin auth)
// so the key handlers do not re-authenticate.
func ContextWithPrincipal(ctx context.Context, p Principal) context.Context {
	return context.WithValue(ctx, principalContextKey{}, p)
}

// ContextWithAdmin marks a request context as authenticated by an admin.
func ContextWithAdmin(ctx context.Context) context.Context {
	return ContextWithPrincipal(ctx, Principal{Admin: true})
}

// PrincipalFromContext returns the principal stored by ContextWithPrincipal.
func PrincipalFromContext(ctx context.Context) (Principal, bool) {
	p, ok := ctx.Value(principalContextKey{}).(Principal)
	return p, ok
}

var errUnauthenticated = errors.New("missing or invalid credentials")

// KeyHandler holds the manager and stores for HTTP handlers.
//
// Authorization: every handler authenticates the caller from (in order) a
// principal placed in the context by trusted middleware, the Authorization
// header ("Bearer <token>" or a bare token) or the "x-api-key" header.
// Admin credentials are the Manager's master key (if MasterKeyUsable) and
// keys registered with AddAdminKey; any active virtual key authenticates as
// a non-admin principal scoped to its own key and team.
type KeyHandler struct {
	Manager *Manager
	Users   UserStore
	Teams   TeamStore
	Logger  func(msg string, kv ...interface{})
	// MaxBodyBytes caps request bodies (default 64 KiB).
	MaxBodyBytes int64

	adminMu     sync.RWMutex
	adminHashes [][sha256.Size]byte
}

// NewKeyHandler creates a new key handler. Nil user/team stores are replaced
// by in-memory stores so the corresponding endpoints work out of the box.
func NewKeyHandler(mgr *Manager, users UserStore, teams TeamStore, logger func(msg string, kv ...interface{})) *KeyHandler {
	if logger == nil {
		logger = func(string, ...interface{}) {}
	}
	if mgr == nil {
		mgr = NewManager(nil, "")
	}
	if users == nil {
		users = NewInMemoryUserStore()
	}
	if teams == nil {
		teams = NewInMemoryTeamStore()
	}
	// Enforce team budgets for the teams managed by this handler unless the
	// manager was already given a team store.
	if mgr.Teams() == nil {
		mgr.SetTeamStore(teams)
	}
	return &KeyHandler{Manager: mgr, Users: users, Teams: teams, Logger: logger, MaxBodyBytes: defaultBodyLimit}
}

// AddAdminKey registers an additional admin credential (e.g. the gateway's
// static admin API key). Empty keys are ignored. Only a hash is kept.
func (h *KeyHandler) AddAdminKey(key string) {
	key = stripBearer(key)
	if key == "" {
		return
	}
	sum := sha256.Sum256([]byte(key))
	h.adminMu.Lock()
	defer h.adminMu.Unlock()
	for _, existing := range h.adminHashes {
		if existing == sum {
			return
		}
	}
	h.adminHashes = append(h.adminHashes, sum)
}

func (h *KeyHandler) isAdminKey(token string) bool {
	sum := sha256.Sum256([]byte(token))
	h.adminMu.RLock()
	defer h.adminMu.RUnlock()
	match := 0
	for _, a := range h.adminHashes {
		match |= subtle.ConstantTimeCompare(sum[:], a[:])
	}
	return match == 1
}

// TokenFromRequest extracts the API credential from the Authorization header
// (with or without the "Bearer " scheme) or the x-api-key header.
func TokenFromRequest(r *http.Request) string {
	if tok := stripBearer(r.Header.Get("Authorization")); tok != "" {
		return tok
	}
	return strings.TrimSpace(r.Header.Get("X-Api-Key"))
}

// Authenticate resolves the caller of r.
func (h *KeyHandler) Authenticate(r *http.Request) (Principal, error) {
	if p, ok := PrincipalFromContext(r.Context()); ok && (p.Admin || p.Key != nil) {
		return p, nil
	}
	tok := TokenFromRequest(r)
	if tok == "" {
		return Principal{}, errUnauthenticated
	}
	if h.Manager.IsMasterKey(tok) || h.isAdminKey(tok) {
		return Principal{Admin: true}, nil
	}
	if IsVirtualKey(tok) {
		vk, err := h.Manager.validateForManagement(r.Context(), tok)
		if err != nil {
			return Principal{}, errUnauthenticated
		}
		return Principal{Key: vk}, nil
	}
	return Principal{}, errUnauthenticated
}

// canView reports whether p may see key vk: admins see everything; a key
// sees itself and keys of its own (non-empty) team.
func canView(p Principal, vk *VirtualKey) bool {
	if p.Admin {
		return true
	}
	if p.Key == nil || vk == nil {
		return false
	}
	if p.Key.KeyHash == vk.KeyHash {
		return true
	}
	return p.Key.TeamID != "" && p.Key.TeamID == vk.TeamID
}

// canManage reports whether p may modify key vk: admins may modify every
// key; team admins may modify member keys of their own team. (Any key may
// additionally revoke or rotate itself; see loadForCaller.)
func canManage(p Principal, vk *VirtualKey) bool {
	if p.Admin {
		return true
	}
	if p.Key == nil || vk == nil || p.Key.Role != RoleTeamAdmin || p.Key.TeamID == "" {
		return false
	}
	// Team admins cannot modify their own key (e.g. raise their own budget)
	// nor other team admins; that requires an admin.
	return vk.TeamID == p.Key.TeamID && vk.Role != RoleTeamAdmin
}

// ---------------------------------------------------------------------------
// HTTP helpers
// ---------------------------------------------------------------------------

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func allowMethods(w http.ResponseWriter, r *http.Request, methods ...string) bool {
	if slices.Contains(methods, r.Method) {
		return true
	}
	w.Header().Set("Allow", strings.Join(methods, ", "))
	writeError(w, http.StatusMethodNotAllowed, "method not allowed")
	return false
}

// decodeBody decodes a JSON body capped at MaxBodyBytes. It writes the error
// response itself and returns false on failure.
func (h *KeyHandler) decodeBody(w http.ResponseWriter, r *http.Request, dst interface{}) bool {
	limit := h.MaxBodyBytes
	if limit <= 0 {
		limit = defaultBodyLimit
	}
	if r.Body == nil || r.Body == http.NoBody {
		writeError(w, http.StatusBadRequest, "missing request body")
		return false
	}
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, limit))
	if err := dec.Decode(dst); err != nil {
		var mbe *http.MaxBytesError
		if errors.As(err, &mbe) {
			writeError(w, http.StatusRequestEntityTooLarge, "request body too large")
			return false
		}
		writeError(w, http.StatusBadRequest, "invalid JSON request body")
		return false
	}
	if err := dec.Decode(&struct{}{}); err != io.EOF {
		writeError(w, http.StatusBadRequest, "request body must contain a single JSON object")
		return false
	}
	return true
}

func (h *KeyHandler) authenticate(w http.ResponseWriter, r *http.Request) (Principal, bool) {
	p, err := h.Authenticate(r)
	if err != nil {
		w.Header().Set("WWW-Authenticate", `Bearer realm="aerollm"`)
		writeError(w, http.StatusUnauthorized, "missing or invalid credentials")
		return Principal{}, false
	}
	return p, true
}

// writeManagerError maps Manager errors onto HTTP responses without leaking
// internals.
func (h *KeyHandler) writeManagerError(w http.ResponseWriter, op string, err error) {
	var ve *ValidationError
	switch {
	case errors.As(err, &ve):
		writeError(w, http.StatusBadRequest, ve.Error())
	case errors.Is(err, ErrKeyNotFound):
		writeError(w, http.StatusNotFound, "key not found")
	case errors.Is(err, ErrKeyRevoked):
		writeError(w, http.StatusConflict, "key is revoked")
	default:
		h.Logger("key management operation failed", "op", op, "error", err.Error())
		writeError(w, http.StatusInternalServerError, "internal error")
	}
}

// loadForCaller fetches a key and hides it (404) from callers who may not
// see it, so existence of other teams' keys is not revealed.
func (h *KeyHandler) loadForCaller(w http.ResponseWriter, r *http.Request, p Principal, keyHash string, manage bool) (*VirtualKey, bool) {
	if keyHash == "" {
		writeError(w, http.StatusBadRequest, "missing or malformed key_hash/key")
		return nil, false
	}
	vk, err := h.Manager.get(r.Context(), keyHash)
	if err != nil {
		if errors.Is(err, ErrKeyNotFound) {
			writeError(w, http.StatusNotFound, "key not found")
		} else {
			h.writeManagerError(w, "get", err)
		}
		return nil, false
	}
	allowed := canView(p, vk)
	if manage {
		allowed = canManage(p, vk) || (p.Key != nil && p.Key.KeyHash == vk.KeyHash)
	}
	if !allowed {
		if canView(p, vk) {
			writeError(w, http.StatusForbidden, "insufficient permissions for this key")
		} else {
			writeError(w, http.StatusNotFound, "key not found")
		}
		return nil, false
	}
	return vk, true
}

func noStore(w http.ResponseWriter) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Pragma", "no-cache")
}

// ---------------------------------------------------------------------------
// Key handlers
// ---------------------------------------------------------------------------

// GenerateKeys handles POST /key/generate.
// Admins may generate any key. Team-admin keys may generate member keys for
// their own team only. The plaintext key is returned once, with
// Cache-Control: no-store.
// @Summary Generate virtual key
// @Description Create a new virtual key with allowed models, TTL, and budget limits.
// @Tags keys
// @Accept json
// @Produce json
// @Param req body GenerateRequest true "Generate key request"
// @Success 200 {object} GenerateResponse
// @Router /key/generate [post]
func (h *KeyHandler) GenerateKeys(w http.ResponseWriter, r *http.Request) {
	if !allowMethods(w, r, http.MethodPost) {
		return
	}
	p, ok := h.authenticate(w, r)
	if !ok {
		return
	}
	if !p.Admin && (p.Key == nil || p.Key.Role != RoleTeamAdmin || p.Key.TeamID == "") {
		writeError(w, http.StatusForbidden, "admin access required")
		return
	}
	var req GenerateRequest
	if !h.decodeBody(w, r, &req) {
		return
	}
	if !p.Admin {
		if req.TeamID != "" && req.TeamID != p.Key.TeamID {
			writeError(w, http.StatusForbidden, "team admins can only create keys for their own team")
			return
		}
		if req.Role == RoleTeamAdmin {
			writeError(w, http.StatusForbidden, "team admins cannot create team admin keys")
			return
		}
		req.TeamID = p.Key.TeamID
		if req.TenantID == "" {
			req.TenantID = p.Key.TenantID
		}
	}
	resp, err := h.Manager.Generate(r.Context(), &req)
	if err != nil {
		h.writeManagerError(w, "generate", err)
		return
	}
	h.Logger("virtual key generated", "key_hash", resp.KeyHash, "team_id", req.TeamID, "admin", p.Admin)
	noStore(w)
	writeJSON(w, http.StatusOK, resp)
}

// DeleteKey handles POST /key/delete (revocation). Admins may revoke any key,
// team admins keys of their team, and any key may revoke itself.
// @Summary Delete virtual key
// @Description Soft-delete a virtual key.
// @Tags keys
// @Accept json
// @Produce json
// @Param req body DeleteRequest true "Delete key request"
// @Success 200 {object} map[string]string
// @Router /key/delete [post]
func (h *KeyHandler) DeleteKey(w http.ResponseWriter, r *http.Request) {
	if !allowMethods(w, r, http.MethodPost, http.MethodDelete) {
		return
	}
	p, ok := h.authenticate(w, r)
	if !ok {
		return
	}
	var req DeleteRequest
	if !h.decodeBody(w, r, &req) {
		return
	}
	keyHash := ParseKeyOrHash(h.Manager, req.Key, req.KeyHash, req.Token)
	vk, ok := h.loadForCaller(w, r, p, keyHash, true)
	if !ok {
		return
	}
	if err := h.Manager.Delete(r.Context(), vk.KeyHash); err != nil {
		h.writeManagerError(w, "delete", err)
		return
	}
	h.Logger("virtual key revoked", "key_hash", vk.KeyHash, "admin", p.Admin)
	writeJSON(w, http.StatusOK, map[string]string{"status": "deleted", "key_hash": vk.KeyHash})
}

// InfoKey handles POST /key/info (JSON body) and GET /key/info?key_hash=...
// Callers can see their own key and keys of their own team; admins see all.
// The plaintext key is never returned.
// @Summary Get key info
// @Description Get usage, budget, and metadata for a specific virtual key.
// @Tags keys
// @Accept json
// @Produce json
// @Param req body InfoRequest true "Key info request"
// @Success 200 {object} InfoResponse
// @Router /key/info [post]
func (h *KeyHandler) InfoKey(w http.ResponseWriter, r *http.Request) {
	if !allowMethods(w, r, http.MethodGet, http.MethodPost) {
		return
	}
	p, ok := h.authenticate(w, r)
	if !ok {
		return
	}
	var req InfoRequest
	if r.Method == http.MethodGet {
		q := r.URL.Query()
		req.KeyHash, req.Key = q.Get("key_hash"), q.Get("key")
	} else if !h.decodeBody(w, r, &req) {
		return
	}
	keyHash := ParseKeyOrHash(h.Manager, req.Key, req.KeyHash, req.Token)
	if keyHash == "" && req.Key == "" && req.KeyHash == "" && req.Token == "" && p.Key != nil {
		keyHash = p.Key.KeyHash // "who am I"
	}
	vk, ok := h.loadForCaller(w, r, p, keyHash, false)
	if !ok {
		return
	}
	writeJSON(w, http.StatusOK, infoFromKey(vk, time.Now()))
}

// UpdateKey handles POST /key/update (partial update of limits, models,
// expiry, blocking). Admins may update any key; team admins member keys of
// their own team (they cannot grant the team_admin role).
func (h *KeyHandler) UpdateKey(w http.ResponseWriter, r *http.Request) {
	if !allowMethods(w, r, http.MethodPost, http.MethodPatch) {
		return
	}
	p, ok := h.authenticate(w, r)
	if !ok {
		return
	}
	var req UpdateRequest
	if !h.decodeBody(w, r, &req) {
		return
	}
	keyHash := ParseKeyOrHash(h.Manager, req.Key, req.KeyHash, req.Token)
	vk, ok := h.loadForCaller(w, r, p, keyHash, true)
	if !ok {
		return
	}
	if !p.Admin {
		if !canManage(p, vk) {
			writeError(w, http.StatusForbidden, "insufficient permissions for this key")
			return
		}
		if req.Role != nil && *req.Role != vk.Role {
			writeError(w, http.StatusForbidden, "only admins can change key roles")
			return
		}
	}
	info, err := h.Manager.Update(r.Context(), vk.KeyHash, &req)
	if err != nil {
		h.writeManagerError(w, "update", err)
		return
	}
	writeJSON(w, http.StatusOK, info)
}

// BlockKey handles POST /key/block.
func (h *KeyHandler) BlockKey(w http.ResponseWriter, r *http.Request) { h.setBlocked(w, r, true) }

// UnblockKey handles POST /key/unblock.
func (h *KeyHandler) UnblockKey(w http.ResponseWriter, r *http.Request) { h.setBlocked(w, r, false) }

func (h *KeyHandler) setBlocked(w http.ResponseWriter, r *http.Request, blocked bool) {
	if !allowMethods(w, r, http.MethodPost) {
		return
	}
	p, ok := h.authenticate(w, r)
	if !ok {
		return
	}
	var req DeleteRequest
	if !h.decodeBody(w, r, &req) {
		return
	}
	keyHash := ParseKeyOrHash(h.Manager, req.Key, req.KeyHash, req.Token)
	vk, ok := h.loadForCaller(w, r, p, keyHash, true)
	if !ok {
		return
	}
	if !canManage(p, vk) {
		writeError(w, http.StatusForbidden, "insufficient permissions for this key")
		return
	}
	var err error
	if blocked {
		err = h.Manager.Block(r.Context(), vk.KeyHash)
	} else {
		err = h.Manager.Unblock(r.Context(), vk.KeyHash)
	}
	if err != nil {
		h.writeManagerError(w, "block", err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"key_hash": vk.KeyHash, "blocked": blocked})
}

// RegenerateKey handles POST /key/regenerate: rotates a key, returning the
// new plaintext once and revoking the old key.
func (h *KeyHandler) RegenerateKey(w http.ResponseWriter, r *http.Request) {
	if !allowMethods(w, r, http.MethodPost) {
		return
	}
	p, ok := h.authenticate(w, r)
	if !ok {
		return
	}
	var req DeleteRequest
	if !h.decodeBody(w, r, &req) {
		return
	}
	keyHash := ParseKeyOrHash(h.Manager, req.Key, req.KeyHash, req.Token)
	vk, ok := h.loadForCaller(w, r, p, keyHash, true)
	if !ok {
		return
	}
	resp, err := h.Manager.Regenerate(r.Context(), vk.KeyHash)
	if err != nil {
		h.writeManagerError(w, "regenerate", err)
		return
	}
	h.Logger("virtual key rotated", "old_key_hash", vk.KeyHash, "new_key_hash", resp.KeyHash)
	noStore(w)
	writeJSON(w, http.StatusOK, resp)
}

// ListKeys handles GET /key/list[?team_id=&user_id=&include_revoked=true].
// Admins see all keys; other callers only keys of their own team (or only
// their own key when they have no team).
func (h *KeyHandler) ListKeys(w http.ResponseWriter, r *http.Request) {
	if !allowMethods(w, r, http.MethodGet) {
		return
	}
	p, ok := h.authenticate(w, r)
	if !ok {
		return
	}
	q := r.URL.Query()
	filter := KeyFilter{
		TeamID:         q.Get("team_id"),
		UserID:         q.Get("user_id"),
		IncludeRevoked: q.Get("include_revoked") == "true",
	}
	if !p.Admin {
		if filter.TeamID != "" && filter.TeamID != p.Key.TeamID {
			writeJSON(w, http.StatusOK, map[string]interface{}{"keys": []*InfoResponse{}})
			return
		}
		filter.TeamID = p.Key.TeamID
	}
	infos, err := h.Manager.List(r.Context(), filter)
	if err != nil {
		h.writeManagerError(w, "list", err)
		return
	}
	if !p.Admin && p.Key.TeamID == "" {
		own := infos[:0]
		for _, in := range infos {
			if in.KeyHash == p.Key.KeyHash {
				own = append(own, in)
			}
		}
		infos = own
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"keys": infos})
}

// ---------------------------------------------------------------------------
// User and team handlers
// ---------------------------------------------------------------------------

// UserInfo handles POST /user/info ({"user_id": ...}) and GET ?user_id=.
// Admins can read any user; a virtual key only the user it belongs to.
// @Summary Get user info
// @Tags users
// @Accept json
// @Produce json
// @Param req body keymanager.InfoRequest true "User info request"
// @Success 200 {object} User
// @Router /user/info [post]
func (h *KeyHandler) UserInfo(w http.ResponseWriter, r *http.Request) {
	if !allowMethods(w, r, http.MethodGet, http.MethodPost) {
		return
	}
	p, ok := h.authenticate(w, r)
	if !ok {
		return
	}
	var req struct {
		UserID string `json:"user_id"`
	}
	if r.Method == http.MethodGet {
		req.UserID = r.URL.Query().Get("user_id")
	} else if !h.decodeBody(w, r, &req) {
		return
	}
	req.UserID = strings.TrimSpace(req.UserID)
	if req.UserID == "" && p.Key != nil {
		req.UserID = p.Key.UserID
	}
	if req.UserID == "" {
		writeError(w, http.StatusBadRequest, "missing user_id")
		return
	}
	if !p.Admin && (p.Key == nil || p.Key.UserID == "" || p.Key.UserID != req.UserID) {
		writeError(w, http.StatusNotFound, "user not found")
		return
	}
	u, err := h.Users.Get(r.Context(), req.UserID)
	if err != nil || u == nil {
		writeError(w, http.StatusNotFound, "user not found")
		return
	}
	writeJSON(w, http.StatusOK, u)
}

// CreateUser handles user creation (admin only).
func (h *KeyHandler) CreateUser(w http.ResponseWriter, r *http.Request) {
	if !allowMethods(w, r, http.MethodPost) {
		return
	}
	p, ok := h.authenticate(w, r)
	if !ok {
		return
	}
	if !p.Admin {
		writeError(w, http.StatusForbidden, "admin access required")
		return
	}
	var u User
	if !h.decodeBody(w, r, &u) {
		return
	}
	if err := firstErr(
		validText("email", u.Email, 320),
		validText("role", u.Role, 64),
		validMetadata(u.Metadata),
	); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	u.ID = generateID("user")
	u.CreatedAt = time.Now().UTC()
	if err := h.Users.Create(r.Context(), &u); err != nil {
		h.Logger("user creation failed", "error", err.Error())
		writeError(w, http.StatusInternalServerError, "failed to create user")
		return
	}
	writeJSON(w, http.StatusOK, u)
}

// TeamCreate handles POST /team/create (admin only).
// @Summary Create team
// @Description Create a new team for an agency.
// @Tags teams
// @Accept json
// @Produce json
// @Param req body Team true "Team creation request"
// @Success 200 {object} Team
// @Router /team/create [post]
func (h *KeyHandler) TeamCreate(w http.ResponseWriter, r *http.Request) {
	if !allowMethods(w, r, http.MethodPost) {
		return
	}
	p, ok := h.authenticate(w, r)
	if !ok {
		return
	}
	if !p.Admin {
		writeError(w, http.StatusForbidden, "admin access required")
		return
	}
	var t Team
	if !h.decodeBody(w, r, &t) {
		return
	}
	t.Name = strings.TrimSpace(t.Name)
	if t.Name == "" {
		writeError(w, http.StatusBadRequest, "invalid name: required")
		return
	}
	if err := validateTeamFields(&t.Name, &t.Members, &t.Budget, t.Metadata); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	now := h.Manager.now().UTC()
	// Spend state is server-controlled.
	t.Spend, t.BudgetResetAt = 0, time.Time{}
	t.BudgetDuration = strings.TrimSpace(t.BudgetDuration)
	period, err := ParseDuration(t.BudgetDuration)
	if err != nil {
		writeError(w, http.StatusBadRequest, invalid("budget_duration", err.Error()).Error())
		return
	}
	if period > 0 {
		t.BudgetResetAt = now.Add(period)
	} else {
		t.BudgetDuration = ""
	}
	t.ID = generateID("team")
	t.CreatedAt = now
	if err := h.Teams.Create(r.Context(), &t); err != nil {
		h.Logger("team creation failed", "error", err.Error())
		writeError(w, http.StatusInternalServerError, "failed to create team")
		return
	}
	writeJSON(w, http.StatusOK, t)
}

// teamUpdateRequest is a partial update: absent fields are left unchanged.
type teamUpdateRequest struct {
	ID             string                 `json:"id"`
	Name           *string                `json:"name"`
	Members        *[]string              `json:"members"`
	Budget         *float64               `json:"budget"`
	Metadata       map[string]interface{} `json:"metadata"`
	BudgetDuration *string                `json:"budget_duration"`
	ResetSpend     bool                   `json:"reset_spend"`
}

// TeamUpdate handles POST /team/update. Only fields present in the body are
// changed. Admins may update any team; team-admin keys may update the name,
// members and metadata of their own team (not its budget, budget period or
// spend). The update is applied atomically, so concurrent spend accrual is
// never lost.
// @Summary Update team
// @Description Update an existing team's details.
// @Tags teams
// @Accept json
// @Produce json
// @Param req body Team true "Team update request"
// @Success 200 {object} Team
// @Router /team/update [post]
func (h *KeyHandler) TeamUpdate(w http.ResponseWriter, r *http.Request) {
	if !allowMethods(w, r, http.MethodPost, http.MethodPatch) {
		return
	}
	p, ok := h.authenticate(w, r)
	if !ok {
		return
	}
	var req teamUpdateRequest
	if !h.decodeBody(w, r, &req) {
		return
	}
	req.ID = strings.TrimSpace(req.ID)
	if req.ID == "" {
		writeError(w, http.StatusBadRequest, "missing id")
		return
	}
	if !p.Admin {
		if p.Key == nil || p.Key.Role != RoleTeamAdmin || p.Key.TeamID != req.ID {
			writeError(w, http.StatusNotFound, "team not found")
			return
		}
		if req.Budget != nil || req.BudgetDuration != nil || req.ResetSpend {
			writeError(w, http.StatusForbidden, "only admins can change team budgets")
			return
		}
	}
	if req.Name != nil {
		trimmed := strings.TrimSpace(*req.Name)
		if trimmed == "" {
			writeError(w, http.StatusBadRequest, "invalid name: must not be empty")
			return
		}
		req.Name = &trimmed
	}
	if err := validateTeamFields(req.Name, req.Members, req.Budget, req.Metadata); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	var period time.Duration
	if req.BudgetDuration != nil {
		var err error
		if period, err = ParseDuration(*req.BudgetDuration); err != nil {
			writeError(w, http.StatusBadRequest, invalid("budget_duration", err.Error()).Error())
			return
		}
	}
	now := h.Manager.now().UTC()
	updated, err := h.Manager.updateTeamIn(r.Context(), h.Teams, req.ID, func(t *Team) error {
		if req.Name != nil {
			t.Name = *req.Name
		}
		if req.Members != nil {
			t.Members = append([]string(nil), (*req.Members)...)
		}
		if req.Budget != nil {
			t.Budget = *req.Budget
		}
		if req.Metadata != nil {
			t.Metadata = maps.Clone(req.Metadata)
		}
		if req.BudgetDuration != nil {
			t.BudgetDuration, t.BudgetResetAt = "", time.Time{}
			if period > 0 {
				t.BudgetDuration = strings.TrimSpace(*req.BudgetDuration)
				t.BudgetResetAt = now.Add(period)
			}
		}
		if req.ResetSpend {
			t.Spend = 0
		}
		return nil
	})
	if err != nil {
		if errors.Is(err, ErrTeamNotFound) {
			writeError(w, http.StatusNotFound, "team not found")
			return
		}
		h.Logger("team update failed", "team_id", req.ID, "error", err.Error())
		writeError(w, http.StatusInternalServerError, "failed to update team")
		return
	}
	writeJSON(w, http.StatusOK, updated)
}

// TeamInfoResponse is the response for /team/info: the team (with Spend
// reporting the spend of the current budget period) plus derived budget
// information.
type TeamInfoResponse struct {
	*Team
	// RemainingBudget is Budget - Spend (never negative); absent when the
	// team has no budget.
	RemainingBudget *float64 `json:"remaining_budget,omitempty"`
	// BudgetExceeded reports whether keys of the team are currently
	// rejected because of the team budget.
	BudgetExceeded bool `json:"budget_exceeded"`
	// Keys is the number of non-revoked keys assigned to the team.
	Keys int `json:"keys"`
}

// TeamInfo handles GET /team/info?team_id= and POST /team/info
// ({"team_id": ...}). Admins may read any team; other keys only their own
// team (defaulting to it when team_id is omitted).
// @Summary Get team info
// @Description Get a team's budget, spend and key count.
// @Tags teams
// @Produce json
// @Success 200 {object} TeamInfoResponse
// @Router /team/info [get]
func (h *KeyHandler) TeamInfo(w http.ResponseWriter, r *http.Request) {
	if !allowMethods(w, r, http.MethodGet, http.MethodPost) {
		return
	}
	p, ok := h.authenticate(w, r)
	if !ok {
		return
	}
	var req struct {
		TeamID string `json:"team_id"`
		ID     string `json:"id"`
	}
	if r.Method == http.MethodGet {
		q := r.URL.Query()
		req.TeamID, req.ID = q.Get("team_id"), q.Get("id")
	} else if !h.decodeBody(w, r, &req) {
		return
	}
	id := strings.TrimSpace(req.TeamID)
	if id == "" {
		id = strings.TrimSpace(req.ID)
	}
	if id == "" && p.Key != nil {
		id = p.Key.TeamID
	}
	if id == "" {
		writeError(w, http.StatusBadRequest, "missing team_id")
		return
	}
	if !p.Admin && (p.Key == nil || p.Key.TeamID == "" || p.Key.TeamID != id) {
		writeError(w, http.StatusNotFound, "team not found")
		return
	}
	t, err := h.Teams.Get(r.Context(), id)
	if err != nil || t == nil {
		if err == nil || errors.Is(err, ErrTeamNotFound) {
			writeError(w, http.StatusNotFound, "team not found")
			return
		}
		h.Logger("team lookup failed", "team_id", id, "error", err.Error())
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	now := h.Manager.now()
	t.normalizeSpend(now)
	resp := &TeamInfoResponse{Team: t, BudgetExceeded: t.overBudget(now)}
	if rem, ok := t.remaining(now); ok {
		resp.RemainingBudget = &rem
	}
	if infos, err := h.Manager.List(r.Context(), KeyFilter{TeamID: id}); err == nil {
		resp.Keys = len(infos)
	} else {
		h.Logger("team key listing failed", "team_id", id, "error", err.Error())
	}
	writeJSON(w, http.StatusOK, resp)
}

func validateTeamFields(name *string, members *[]string, budget *float64, md map[string]interface{}) error {
	var errs []error
	if name != nil {
		errs = append(errs, validText("name", *name, 128))
	}
	if members != nil {
		errs = append(errs, validList("members", *members, 1000, maxIDLen))
	}
	if budget != nil {
		errs = append(errs, validBudget("budget", *budget))
	}
	errs = append(errs, validMetadata(md))
	return firstErr(errs...)
}

func validMetadata(md map[string]interface{}) error {
	if len(md) > maxMetadataKeys {
		return invalid("metadata", fmt.Sprintf("more than %d keys", maxMetadataKeys))
	}
	return nil
}

func firstErr(errs ...error) error {
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// In-memory stores
// ---------------------------------------------------------------------------

// InMemoryKeyStore is an in-memory implementation of KeyStore. It stores and
// returns copies, so callers can never mutate stored keys without going
// through Update/UpdateFunc, and it implements AtomicKeyUpdater.
type InMemoryKeyStore struct {
	mu   sync.RWMutex
	keys map[string]*VirtualKey
}

// NewInMemoryKeyStore creates a new in-memory key store.
func NewInMemoryKeyStore() *InMemoryKeyStore {
	return &InMemoryKeyStore{keys: make(map[string]*VirtualKey)}
}

// Create stores a virtual key.
func (s *InMemoryKeyStore) Create(ctx context.Context, key *VirtualKey) error {
	if key == nil || key.KeyHash == "" {
		return errors.New("keymanager: invalid key")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.keys[key.KeyHash]; exists {
		return ErrKeyExists
	}
	s.keys[key.KeyHash] = key.Clone()
	return nil
}

// Get retrieves a copy of a virtual key by hash.
func (s *InMemoryKeyStore) Get(ctx context.Context, keyHash string) (*VirtualKey, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	key, ok := s.keys[keyHash]
	if !ok {
		return nil, ErrKeyNotFound
	}
	return key.Clone(), nil
}

// GetByPrefix returns a copy of the key whose display prefix equals prefix.
// Prefixes are non-secret hints and are not guaranteed unique; if several
// keys share it, the most recently created one is returned.
func (s *InMemoryKeyStore) GetByPrefix(ctx context.Context, prefix string) (*VirtualKey, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var best *VirtualKey
	for _, k := range s.keys {
		if prefix != "" && k.Prefix == prefix && (best == nil || k.CreatedAt.After(best.CreatedAt)) {
			best = k
		}
	}
	if best == nil {
		return nil, ErrKeyNotFound
	}
	return best.Clone(), nil
}

// Update replaces a stored virtual key.
func (s *InMemoryKeyStore) Update(ctx context.Context, key *VirtualKey) error {
	if key == nil {
		return ErrKeyNotFound
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.keys[key.KeyHash]; !ok {
		return ErrKeyNotFound
	}
	s.keys[key.KeyHash] = key.Clone()
	return nil
}

// UpdateFunc atomically applies fn to a copy of the stored key and stores the
// result if fn returns nil. It returns a copy of the updated key.
func (s *InMemoryKeyStore) UpdateFunc(ctx context.Context, keyHash string, fn func(*VirtualKey) error) (*VirtualKey, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, ok := s.keys[keyHash]
	if !ok {
		return nil, ErrKeyNotFound
	}
	next := cur.Clone()
	if err := fn(next); err != nil {
		return nil, err
	}
	next.KeyHash, next.HashedKey = cur.KeyHash, cur.HashedKey // identity is immutable
	s.keys[keyHash] = next
	return next.Clone(), nil
}

// Delete removes a virtual key permanently.
func (s *InMemoryKeyStore) Delete(ctx context.Context, keyHash string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.keys[keyHash]; !ok {
		return ErrKeyNotFound
	}
	delete(s.keys, keyHash)
	return nil
}

// List returns copies of all virtual keys.
func (s *InMemoryKeyStore) List(ctx context.Context) ([]*VirtualKey, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*VirtualKey, 0, len(s.keys))
	for _, k := range s.keys {
		out = append(out, k.Clone())
	}
	return out, nil
}

// InMemoryUserStore is a concurrency-safe in-memory UserStore.
type InMemoryUserStore struct {
	mu    sync.RWMutex
	users map[string]User
}

// NewInMemoryUserStore creates an empty user store.
func NewInMemoryUserStore() *InMemoryUserStore {
	return &InMemoryUserStore{users: make(map[string]User)}
}

// Create stores a new user.
func (s *InMemoryUserStore) Create(ctx context.Context, u *User) error {
	if u == nil || u.ID == "" {
		return errors.New("keymanager: invalid user")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.users[u.ID]; ok {
		return ErrUserExists
	}
	c := *u
	c.Metadata = maps.Clone(u.Metadata)
	s.users[u.ID] = c
	return nil
}

// Get returns a copy of a user.
func (s *InMemoryUserStore) Get(ctx context.Context, id string) (*User, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	u, ok := s.users[id]
	if !ok {
		return nil, ErrUserNotFound
	}
	u.Metadata = maps.Clone(u.Metadata)
	return &u, nil
}

// Update replaces an existing user.
func (s *InMemoryUserStore) Update(ctx context.Context, u *User) error {
	if u == nil {
		return errors.New("keymanager: invalid user")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.users[u.ID]; !ok {
		return ErrUserNotFound
	}
	c := *u
	c.Metadata = maps.Clone(u.Metadata)
	s.users[u.ID] = c
	return nil
}

// InMemoryTeamStore is a concurrency-safe in-memory TeamStore.
type InMemoryTeamStore struct {
	mu    sync.RWMutex
	teams map[string]Team
}

// NewInMemoryTeamStore creates an empty team store.
func NewInMemoryTeamStore() *InMemoryTeamStore {
	return &InMemoryTeamStore{teams: make(map[string]Team)}
}

func cloneTeam(t Team) Team {
	t.Members = append([]string(nil), t.Members...)
	t.Metadata = maps.Clone(t.Metadata)
	return t
}

// Create stores a new team.
func (s *InMemoryTeamStore) Create(ctx context.Context, t *Team) error {
	if t == nil || t.ID == "" {
		return errors.New("keymanager: invalid team")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.teams[t.ID]; ok {
		return ErrTeamExists
	}
	s.teams[t.ID] = cloneTeam(*t)
	return nil
}

// Get returns a copy of a team.
func (s *InMemoryTeamStore) Get(ctx context.Context, id string) (*Team, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	t, ok := s.teams[id]
	if !ok {
		return nil, ErrTeamNotFound
	}
	c := cloneTeam(t)
	return &c, nil
}

// Update replaces an existing team; CreatedAt is preserved.
func (s *InMemoryTeamStore) Update(ctx context.Context, t *Team) error {
	if t == nil {
		return errors.New("keymanager: invalid team")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, ok := s.teams[t.ID]
	if !ok {
		return ErrTeamNotFound
	}
	c := cloneTeam(*t)
	c.CreatedAt = cur.CreatedAt
	s.teams[t.ID] = c
	return nil
}

// UpdateFunc atomically applies fn to a copy of the stored team and stores
// the result if fn returns nil. ID and CreatedAt are immutable.
func (s *InMemoryTeamStore) UpdateFunc(ctx context.Context, id string, fn func(*Team) error) (*Team, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, ok := s.teams[id]
	if !ok {
		return nil, ErrTeamNotFound
	}
	next := cloneTeam(cur)
	if err := fn(&next); err != nil {
		return nil, err
	}
	next.ID, next.CreatedAt = cur.ID, cur.CreatedAt
	s.teams[id] = next
	out := cloneTeam(next)
	return &out, nil
}

// ErrKeyExists is returned when a key already exists.
var ErrKeyExists = newKeyError("key already exists")

// ErrKeyNotFound is returned when a key is not found.
var ErrKeyNotFound = newKeyError("key not found")

// KeyError is a lightweight error type.
type KeyError struct{ msg string }

func (e *KeyError) Error() string { return e.msg }

func newKeyError(msg string) *KeyError { return &KeyError{msg: msg} }

// generateID creates an unpredictable, collision-resistant ID.
func generateID(kind string) string {
	b := make([]byte, 12)
	_, _ = rand.Read(b) // crypto/rand.Read never fails on supported platforms
	return kind + "_" + hex.EncodeToString(b)
}
