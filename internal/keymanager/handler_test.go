package keymanager

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

const testMaster = "unit-test-master-key-0123456789"

func newTestHandler(t *testing.T) *KeyHandler {
	t.Helper()
	h := NewKeyHandler(NewManager(NewInMemoryKeyStore(), testMaster), nil, nil, nil)
	h.AddAdminKey("sk-demo")
	return h
}

func do(t *testing.T, hf http.HandlerFunc, method, token, body string) *httptest.ResponseRecorder {
	t.Helper()
	var rdr *bytes.Reader
	if body != "" {
		rdr = bytes.NewReader([]byte(body))
	} else {
		rdr = bytes.NewReader(nil)
	}
	req := httptest.NewRequest(method, "/", rdr)
	if body == "" {
		req.Body = http.NoBody
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	hf(rec, req)
	return rec
}

func mustGenerate(t *testing.T, h *KeyHandler, token, body string) GenerateResponse {
	t.Helper()
	rec := do(t, h.GenerateKeys, http.MethodPost, token, body)
	if rec.Code != http.StatusOK {
		t.Fatalf("generate: %d %s", rec.Code, rec.Body.String())
	}
	var resp GenerateResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	return resp
}

func TestHandlerGenerateRequiresAdmin(t *testing.T) {
	h := newTestHandler(t)

	if rec := do(t, h.GenerateKeys, http.MethodPost, "", `{}`); rec.Code != http.StatusUnauthorized {
		t.Fatalf("no auth: expected 401, got %d", rec.Code)
	}
	if rec := do(t, h.GenerateKeys, http.MethodPost, "default-master-key", `{}`); rec.Code != http.StatusUnauthorized {
		t.Fatalf("default master key must not work: got %d", rec.Code)
	}
	if rec := do(t, h.GenerateKeys, http.MethodGet, testMaster, ""); rec.Code != http.StatusMethodNotAllowed || rec.Header().Get("Allow") != "POST" {
		t.Fatalf("expected 405 with Allow, got %d %q", rec.Code, rec.Header().Get("Allow"))
	}

	resp := mustGenerate(t, h, testMaster, `{"models":["gpt-4o"],"team_id":"t1"}`)
	if !IsVirtualKey(resp.Key) {
		t.Fatalf("bad key %q", resp.Key)
	}
	// Static admin key works too.
	mustGenerate(t, h, "sk-demo", `{}`)

	// A member virtual key cannot generate keys.
	if rec := do(t, h.GenerateKeys, http.MethodPost, resp.Key, `{}`); rec.Code != http.StatusForbidden {
		t.Fatalf("member key: expected 403, got %d", rec.Code)
	}
	// Validation errors are 400 JSON.
	rec := do(t, h.GenerateKeys, http.MethodPost, testMaster, `{"max_budget":-3}`)
	if rec.Code != http.StatusBadRequest || rec.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("expected 400 json, got %d %s", rec.Code, rec.Header().Get("Content-Type"))
	}
	if rec := do(t, h.GenerateKeys, http.MethodPost, testMaster, `{"models":`); rec.Code != http.StatusBadRequest {
		t.Fatalf("malformed JSON: expected 400, got %d", rec.Code)
	}
	big := `{"metadata":{"x":"` + strings.Repeat("a", 70<<10) + `"}}`
	if rec := do(t, h.GenerateKeys, http.MethodPost, testMaster, big); rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized body: expected 413, got %d", rec.Code)
	}
}

func TestHandlerContextPrincipal(t *testing.T) {
	h := newTestHandler(t)
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{}`))
	req = req.WithContext(ContextWithAdmin(context.Background()))
	rec := httptest.NewRecorder()
	h.GenerateKeys(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("admin context should be honoured, got %d", rec.Code)
	}
	if rec.Header().Get("Cache-Control") != "no-store" {
		t.Fatal("key response must not be cacheable")
	}
}

func TestHandlerInfoScoping(t *testing.T) {
	h := newTestHandler(t)
	a1 := mustGenerate(t, h, testMaster, `{"team_id":"team-a"}`)
	a2 := mustGenerate(t, h, testMaster, `{"team_id":"team-a"}`)
	b1 := mustGenerate(t, h, testMaster, `{"team_id":"team-b"}`)

	// Own key and same-team key are visible.
	for _, target := range []string{a1.KeyHash, a2.KeyHash} {
		rec := do(t, h.InfoKey, http.MethodPost, a1.Key, `{"key_hash":"`+target+`"}`)
		if rec.Code != http.StatusOK {
			t.Fatalf("same team info: %d %s", rec.Code, rec.Body.String())
		}
		if strings.Contains(rec.Body.String(), a1.Key) || strings.Contains(rec.Body.String(), a2.Key) {
			t.Fatal("info must never contain plaintext keys")
		}
	}
	// Other team's key is hidden as 404.
	rec := do(t, h.InfoKey, http.MethodPost, a1.Key, `{"key_hash":"`+b1.KeyHash+`"}`)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("cross-team info: expected 404, got %d", rec.Code)
	}
	// Whoami with empty body.
	rec = do(t, h.InfoKey, http.MethodPost, b1.Key, `{}`)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), b1.KeyHash) {
		t.Fatalf("whoami: %d %s", rec.Code, rec.Body.String())
	}
	// Admin can see everything, GET works.
	req := httptest.NewRequest(http.MethodGet, "/key/info?key_hash="+b1.KeyHash, nil)
	req.Header.Set("Authorization", "Bearer "+testMaster)
	rr := httptest.NewRecorder()
	h.InfoKey(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("admin GET info: %d", rr.Code)
	}
	// Malformed hash is a 400.
	if rec := do(t, h.InfoKey, http.MethodPost, testMaster, `{"key_hash":"zz"}`); rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}

	// List for a member shows only its team.
	req = httptest.NewRequest(http.MethodGet, "/key/list", nil)
	req.Header.Set("Authorization", "Bearer "+a1.Key)
	rr = httptest.NewRecorder()
	h.ListKeys(rr, req)
	var list struct{ Keys []InfoResponse }
	_ = json.Unmarshal(rr.Body.Bytes(), &list)
	if rr.Code != http.StatusOK || len(list.Keys) != 2 {
		t.Fatalf("member list: %d %d %s", rr.Code, len(list.Keys), rr.Body.String())
	}
	for _, k := range list.Keys {
		if k.TeamID != "team-a" {
			t.Fatal("member list leaked another team's key")
		}
	}
}

func TestHandlerDeleteAuthorization(t *testing.T) {
	h := newTestHandler(t)
	a1 := mustGenerate(t, h, testMaster, `{"team_id":"team-a"}`)
	a2 := mustGenerate(t, h, testMaster, `{"team_id":"team-a"}`)

	// A member cannot delete a teammate's key.
	if rec := do(t, h.DeleteKey, http.MethodPost, a1.Key, `{"key_hash":"`+a2.KeyHash+`"}`); rec.Code != http.StatusForbidden {
		t.Fatalf("member delete teammate: expected 403, got %d", rec.Code)
	}
	// It can revoke itself.
	if rec := do(t, h.DeleteKey, http.MethodPost, a1.Key, `{"key":"`+a1.Key+`"}`); rec.Code != http.StatusOK {
		t.Fatalf("self revoke: %d %s", rec.Code, rec.Body.String())
	}
	// A revoked key can no longer authenticate.
	if rec := do(t, h.InfoKey, http.MethodPost, a1.Key, `{}`); rec.Code != http.StatusUnauthorized {
		t.Fatalf("revoked key: expected 401, got %d", rec.Code)
	}
	// Admin deletes any key; unknown key is 404.
	if rec := do(t, h.DeleteKey, http.MethodPost, testMaster, `{"key_hash":"`+a2.KeyHash+`"}`); rec.Code != http.StatusOK {
		t.Fatalf("admin delete: %d", rec.Code)
	}
	if rec := do(t, h.DeleteKey, http.MethodPost, testMaster, `{"key_hash":"`+strings.Repeat("0", 64)+`"}`); rec.Code != http.StatusNotFound {
		t.Fatalf("unknown key: expected 404, got %d", rec.Code)
	}
}

func TestHandlerTeamAdminScoping(t *testing.T) {
	h := newTestHandler(t)
	ta := mustGenerate(t, h, testMaster, `{"team_id":"team-a","role":"team_admin"}`)
	other := mustGenerate(t, h, testMaster, `{"team_id":"team-b"}`)

	// Team admin generates keys, forced into its own team.
	m := mustGenerate(t, h, ta.Key, `{"max_budget":5}`)
	info, _ := h.Manager.Info(context.Background(), m.KeyHash)
	if info.TeamID != "team-a" {
		t.Fatalf("team admin key must be scoped to own team, got %q", info.TeamID)
	}
	if rec := do(t, h.GenerateKeys, http.MethodPost, ta.Key, `{"team_id":"team-b"}`); rec.Code != http.StatusForbidden {
		t.Fatalf("cross-team generate: expected 403, got %d", rec.Code)
	}
	if rec := do(t, h.GenerateKeys, http.MethodPost, ta.Key, `{"role":"team_admin"}`); rec.Code != http.StatusForbidden {
		t.Fatalf("minting team admins: expected 403, got %d", rec.Code)
	}
	// Team admin can update/block members of its team, not its own key or other teams.
	if rec := do(t, h.UpdateKey, http.MethodPost, ta.Key, `{"key_hash":"`+m.KeyHash+`","max_budget":7}`); rec.Code != http.StatusOK {
		t.Fatalf("update member: %d %s", rec.Code, rec.Body.String())
	}
	if rec := do(t, h.UpdateKey, http.MethodPost, ta.Key, `{"key_hash":"`+ta.KeyHash+`","max_budget":1e9}`); rec.Code != http.StatusForbidden {
		t.Fatalf("self-escalation: expected 403, got %d", rec.Code)
	}
	if rec := do(t, h.UpdateKey, http.MethodPost, ta.Key, `{"key_hash":"`+m.KeyHash+`","role":"team_admin"}`); rec.Code != http.StatusForbidden {
		t.Fatalf("role grant: expected 403, got %d", rec.Code)
	}
	if rec := do(t, h.BlockKey, http.MethodPost, ta.Key, `{"key_hash":"`+other.KeyHash+`"}`); rec.Code != http.StatusNotFound {
		t.Fatalf("cross-team block: expected 404, got %d", rec.Code)
	}
	if rec := do(t, h.BlockKey, http.MethodPost, ta.Key, `{"key_hash":"`+m.KeyHash+`"}`); rec.Code != http.StatusOK {
		t.Fatalf("block member: %d", rec.Code)
	}
	if _, err := h.Manager.Validate(context.Background(), m.Key); err == nil {
		t.Fatal("blocked key must not validate")
	}
	// Team update: own team only, no budget changes.
	if rec := do(t, h.TeamUpdate, http.MethodPost, ta.Key, `{"id":"team-b","name":"x"}`); rec.Code != http.StatusNotFound {
		t.Fatalf("cross-team update: expected 404, got %d", rec.Code)
	}
	if rec := do(t, h.TeamUpdate, http.MethodPost, ta.Key, `{"id":"team-a","budget":100}`); rec.Code != http.StatusForbidden {
		t.Fatalf("team budget by team admin: expected 403, got %d", rec.Code)
	}
}

func TestHandlerRegenerate(t *testing.T) {
	h := newTestHandler(t)
	k := mustGenerate(t, h, testMaster, `{}`)
	rec := do(t, h.RegenerateKey, http.MethodPost, k.Key, `{"key":"`+k.Key+`"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("self rotate: %d %s", rec.Code, rec.Body.String())
	}
	var nk GenerateResponse
	_ = json.Unmarshal(rec.Body.Bytes(), &nk)
	if !IsVirtualKey(nk.Key) || nk.Key == k.Key {
		t.Fatal("expected a fresh key")
	}
	if _, err := h.Manager.Validate(context.Background(), k.Key); err == nil {
		t.Fatal("old key must be revoked")
	}
}

func TestHandlerTeamsAndUsers(t *testing.T) {
	h := newTestHandler(t)
	member := mustGenerate(t, h, testMaster, `{"user_id":"u1"}`)

	if rec := do(t, h.TeamCreate, http.MethodPost, member.Key, `{"name":"x"}`); rec.Code != http.StatusForbidden {
		t.Fatalf("member team create: expected 403, got %d", rec.Code)
	}
	if rec := do(t, h.TeamCreate, http.MethodPost, testMaster, `{"name":""}`); rec.Code != http.StatusBadRequest {
		t.Fatalf("empty team name: expected 400, got %d", rec.Code)
	}
	rec := do(t, h.TeamCreate, http.MethodPost, testMaster, `{"name":"Alpha","budget":50,"members":["u1"]}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("team create: %d %s", rec.Code, rec.Body.String())
	}
	var team Team
	_ = json.Unmarshal(rec.Body.Bytes(), &team)
	if !strings.HasPrefix(team.ID, "team_") || team.CreatedAt.IsZero() {
		t.Fatalf("unexpected team %+v", team)
	}
	// Partial update keeps other fields (CreatedAt, budget, members).
	rec = do(t, h.TeamUpdate, http.MethodPost, testMaster, `{"id":"`+team.ID+`","name":"Beta"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("team update: %d %s", rec.Code, rec.Body.String())
	}
	var updated Team
	_ = json.Unmarshal(rec.Body.Bytes(), &updated)
	if updated.Name != "Beta" || updated.Budget != 50 || len(updated.Members) != 1 || !updated.CreatedAt.Equal(team.CreatedAt) {
		t.Fatalf("partial update clobbered fields: %+v", updated)
	}
	if rec := do(t, h.TeamUpdate, http.MethodPost, testMaster, `{"id":"nope","name":"x"}`); rec.Code != http.StatusNotFound {
		t.Fatalf("unknown team: expected 404, got %d", rec.Code)
	}

	// User info with nil stores no longer panics; members only see themselves.
	_ = h.Users.Create(context.Background(), &User{ID: "u1", Email: "a@b.c"})
	_ = h.Users.Create(context.Background(), &User{ID: "u2", Email: "x@y.z"})
	if rec := do(t, h.UserInfo, http.MethodPost, member.Key, `{"user_id":"u1"}`); rec.Code != http.StatusOK {
		t.Fatalf("own user info: %d", rec.Code)
	}
	if rec := do(t, h.UserInfo, http.MethodPost, member.Key, `{"user_id":"u2"}`); rec.Code != http.StatusNotFound {
		t.Fatalf("other user info: expected 404, got %d", rec.Code)
	}
	if rec := do(t, h.UserInfo, http.MethodPost, testMaster, `{"user_id":"u2"}`); rec.Code != http.StatusOK {
		t.Fatalf("admin user info: %d", rec.Code)
	}
}
