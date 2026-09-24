package secrets

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/ayoubzulfiqar/aerollm/internal/persist"
)

func newKey(t *testing.T) []byte {
	t.Helper()
	k := make([]byte, 32)
	if _, err := rand.Read(k); err != nil {
		t.Fatal(err)
	}
	return k
}

// hookStore lets tests fail individual Put/Delete calls.
type hookStore struct {
	persist.Store
	mu     sync.Mutex
	put    func(bucket, key string) error
	delete func(bucket, key string) error
}

func (h *hookStore) Put(b, k string, v any) error {
	h.mu.Lock()
	fn := h.put
	h.mu.Unlock()
	if fn != nil {
		if err := fn(b, k); err != nil {
			return err
		}
	}
	return h.Store.Put(b, k, v)
}

func (h *hookStore) Delete(b, k string) error {
	h.mu.Lock()
	fn := h.delete
	h.mu.Unlock()
	if fn != nil {
		if err := fn(b, k); err != nil {
			return err
		}
	}
	return h.Store.Delete(b, k)
}

func (h *hookStore) setPut(fn func(bucket, key string) error) {
	h.mu.Lock()
	h.put = fn
	h.mu.Unlock()
}

func rawDump(t *testing.T, ps persist.Store, buckets ...string) string {
	t.Helper()
	var b strings.Builder
	for _, bucket := range buckets {
		_ = ps.ForEach(bucket, func(key string, raw json.RawMessage) error {
			b.WriteString(key)
			b.Write(raw)
			return nil
		})
	}
	return b.String()
}

func mustReveal(t *testing.T, s *Store, id, want string) {
	t.Helper()
	sec, err := s.Reveal(id)
	if err != nil {
		t.Fatalf("reveal %s: %v", id, err)
	}
	if sec.Value != want {
		t.Fatalf("reveal %s = %q, want %q", id, sec.Value, want)
	}
}

func TestPersistentStoreRequiresConfiguredKey(t *testing.T) {
	t.Setenv(EnvKey, "")
	if _, err := NewPersistentStore(persist.NewMemory()); !errors.Is(err, ErrEphemeralKey) {
		t.Fatalf("unset key: %v", err)
	}
	s := NewStore()
	if err := s.EnablePersistence(persist.NewMemory()); !errors.Is(err, ErrEphemeralKey) {
		t.Fatalf("EnablePersistence with ephemeral key: %v", err)
	}
	t.Setenv(EnvKey, "not-a-key")
	if _, err := NewPersistentStore(persist.NewMemory()); !errors.Is(err, ErrKeyUnavailable) {
		t.Fatalf("invalid key: %v", err)
	}
	key := newKey(t)
	t.Setenv(EnvKey, hex.EncodeToString(key))
	ps := persist.NewMemory()
	s, err := NewPersistentStore(ps)
	if err != nil {
		t.Fatal(err)
	}
	if !s.Persistent() || s.IsEphemeral() {
		t.Fatal("expected a persistent, non-ephemeral store")
	}
	if err := s.EnablePersistence(ps); err == nil {
		t.Fatal("enabling persistence twice must fail")
	}
	if _, err := NewPersistentStoreWithKey(key, nil); err == nil {
		t.Fatal("nil persist store must be rejected")
	}
}

func TestPersistentSecretsSurviveRestartAsCiphertextOnly(t *testing.T) {
	key := newKey(t)
	path := filepath.Join(t.TempDir(), "secrets.db")
	ps, err := persist.OpenBolt(path)
	if err != nil {
		t.Fatal(err)
	}
	s, err := NewPersistentStoreWithKey(key, ps)
	if err != nil {
		t.Fatal(err)
	}
	s.SetMaxVersions(2)
	for _, v := range []string{"value-one-PLAINTEXT", "value-two-PLAINTEXT", "value-three-PLAINTEXT"} {
		if err := s.Upsert(Secret{Name: "openai", Value: v, Type: "api_key", Metadata: map[string]string{"env": "prod"}}); err != nil {
			t.Fatal(err)
		}
	}
	_ = s.Upsert(Secret{Name: "gone", Value: "deleted-PLAINTEXT"})
	if ok, err := s.Remove("sec_gone"); !ok || err != nil {
		t.Fatalf("remove: %v %v", ok, err)
	}
	dump := rawDump(t, ps, BucketSecrets, BucketMeta)
	if strings.Contains(dump, "PLAINTEXT") {
		t.Fatal("plaintext value persisted")
	}
	if !strings.Contains(dump, `"env":"prod"`) || !strings.Contains(dump, "sec_openai") {
		t.Fatalf("metadata not persisted: %s", dump)
	}
	_ = ps.Close()

	ps, err = persist.OpenBolt(path)
	if err != nil {
		t.Fatal(err)
	}
	defer ps.Close()
	s, err = NewPersistentStoreWithKey(key, ps)
	if err != nil {
		t.Fatal(err)
	}
	mustReveal(t, s, "sec_openai", "value-three-PLAINTEXT")
	m, ok := s.Metadata("sec_openai")
	if !ok || m.Version != 3 || len(m.Versions) != 2 || m.Type != "api_key" || m.Metadata["env"] != "prod" {
		t.Fatalf("metadata after restart: %+v", m)
	}
	if sec, err := s.RevealVersion("sec_openai", 2); err != nil || sec.Value != "value-two-PLAINTEXT" {
		t.Fatalf("version 2: %v", err)
	}
	if _, ok := s.Get("sec_gone"); ok {
		t.Fatal("deleted secret resurrected")
	}
	// The wrong key is detected at open time.
	if _, err := NewPersistentStoreWithKey(newKey(t), ps); !errors.Is(err, ErrKeyMismatch) {
		t.Fatalf("wrong key: %v", err)
	}
}

func TestPersistentSecretsWriteErrorsLeaveStoreUnchanged(t *testing.T) {
	hs := &hookStore{Store: persist.NewMemory()}
	s, err := NewPersistentStoreWithKey(newKey(t), hs)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Upsert(Secret{Name: "a", Value: "v1"}); err != nil {
		t.Fatal(err)
	}
	fail := errors.New("disk full")
	hs.setPut(func(string, string) error { return fail })
	hs.delete = func(string, string) error { return fail }
	if err := s.Upsert(Secret{Name: "a", Value: "v2"}); !errors.Is(err, fail) {
		t.Fatalf("upsert: %v", err)
	}
	if err := s.Upsert(Secret{Name: "b", Value: "x"}); !errors.Is(err, fail) {
		t.Fatalf("upsert new: %v", err)
	}
	mustReveal(t, s, "sec_a", "v1")
	if m, _ := s.Metadata("sec_a"); m.Version != 1 {
		t.Fatalf("failed upsert changed memory: %+v", m)
	}
	if _, ok := s.Get("sec_b"); ok {
		t.Fatal("failed create visible")
	}
	if ok, err := s.Remove("sec_a"); !ok || !errors.Is(err, fail) {
		t.Fatalf("remove: %v %v", ok, err)
	}
	if s.Delete("sec_a") {
		t.Fatal("Delete must report failure")
	}
	mustReveal(t, s, "sec_a", "v1")

	// The HTTP handler reports persistence failures as 503.
	h := WebhookHandler(s)
	req := httptest.NewRequest(http.MethodDelete, "/?id=sec_a", nil)
	rec := httptest.NewRecorder()
	h(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("delete status %d", rec.Code)
	}
	req = httptest.NewRequest(http.MethodPost, "/", bytes.NewReader([]byte(`{"name":"c","value":"x"}`)))
	rec = httptest.NewRecorder()
	h(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("upsert status %d", rec.Code)
	}
}

func TestPersistentRotationReencryptsPersistedData(t *testing.T) {
	oldKey, nk := newKey(t), newKey(t)
	ps := persist.NewMemory()
	s, _ := NewPersistentStoreWithKey(oldKey, ps)
	_ = s.Upsert(Secret{Name: "a", Value: "alpha"})
	_ = s.Upsert(Secret{Name: "a", Value: "alpha2"})
	_ = s.Upsert(Secret{Name: "b", Value: "beta"})
	before := rawDump(t, ps, BucketSecrets)
	if err := s.RotateKey(nk); err != nil {
		t.Fatal(err)
	}
	mustReveal(t, s, "sec_a", "alpha2")
	after := rawDump(t, ps, BucketSecrets)
	if before == after || strings.Contains(after, keyFingerprint(oldKey)) || !strings.Contains(after, keyFingerprint(nk)) {
		t.Fatal("persisted ciphertexts were not re-encrypted")
	}
	if rawDump(t, ps, BucketRotation) != "" {
		t.Fatal("staging not cleaned up")
	}
	if _, err := NewPersistentStoreWithKey(oldKey, ps); !errors.Is(err, ErrKeyMismatch) {
		t.Fatalf("old key after rotation: %v", err)
	}
	re, err := NewPersistentStoreWithKey(nk, ps)
	if err != nil {
		t.Fatal(err)
	}
	mustReveal(t, re, "sec_a", "alpha2")
	mustReveal(t, re, "sec_b", "beta")
	if sec, err := re.RevealVersion("sec_a", 1); err != nil || sec.Value != "alpha" {
		t.Fatalf("old version after rotation: %v", err)
	}
	// Rotating to the same key only refreshes nonces.
	if err := re.RotateKey(nk); err != nil {
		t.Fatal(err)
	}
	re2, err := NewPersistentStoreWithKey(nk, ps)
	if err != nil {
		t.Fatal(err)
	}
	mustReveal(t, re2, "sec_b", "beta")
}

func TestPersistentRotationFailureRollsBack(t *testing.T) {
	oldKey, nk := newKey(t), newKey(t)
	hs := &hookStore{Store: persist.NewMemory()}
	s, _ := NewPersistentStoreWithKey(oldKey, hs)
	for _, n := range []string{"a", "b", "c"} {
		_ = s.Upsert(Secret{Name: n, Value: "v-" + n})
	}
	// Fail the second overwrite of a live document.
	writes := 0
	hs.setPut(func(bucket, _ string) error {
		if bucket == BucketSecrets {
			writes++
			if writes == 2 {
				return errors.New("io error")
			}
		}
		return nil
	})
	err := s.RotateKey(nk)
	if err == nil || errors.Is(err, ErrRotationIncomplete) {
		t.Fatalf("expected a rolled-back failure, got %v", err)
	}
	hs.setPut(nil)
	mustReveal(t, s, "sec_b", "v-b") // still on the old key
	if err := s.Upsert(Secret{Name: "d", Value: "v-d"}); err != nil {
		t.Fatal(err)
	}
	re, err := NewPersistentStoreWithKey(oldKey, hs)
	if err != nil {
		t.Fatalf("reopen with old key after rollback: %v", err)
	}
	for _, n := range []string{"a", "b", "c", "d"} {
		mustReveal(t, re, "sec_"+n, "v-"+n)
	}
	if _, err := NewPersistentStoreWithKey(nk, hs); !errors.Is(err, ErrKeyMismatch) {
		t.Fatalf("new key after rollback: %v", err)
	}
}

func TestPersistentRotationCrashRecovery(t *testing.T) {
	oldKey, nk := newKey(t), newKey(t)
	hs := &hookStore{Store: persist.NewMemory()}
	s, _ := NewPersistentStoreWithKey(oldKey, hs)
	for _, n := range []string{"a", "b", "c", "d"} {
		_ = s.Upsert(Secret{Name: n, Value: "v-" + n})
	}
	// Simulate a crash in the middle of step 3: after two documents were
	// rewritten every further write fails (including the rollback).
	writes := 0
	hs.setPut(func(bucket, _ string) error {
		if bucket == BucketSecrets {
			writes++
			if writes > 2 {
				return errors.New("crashed")
			}
		}
		return nil
	})
	err := s.RotateKey(nk)
	if !errors.Is(err, ErrPersistenceBroken) {
		t.Fatalf("expected ErrPersistenceBroken, got %v", err)
	}
	hs.setPut(nil)
	if err := s.Upsert(Secret{Name: "e", Value: "x"}); !errors.Is(err, ErrPersistenceBroken) {
		t.Fatalf("writes after a broken rotation must be refused: %v", err)
	}
	// Restarting with the old key cannot recover: two documents use the new key.
	if _, err := NewPersistentStoreWithKey(oldKey, hs); !errors.Is(err, ErrRotationIncomplete) {
		t.Fatalf("old key during interrupted rotation: %v", err)
	}
	if _, err := NewPersistentStoreWithKey(newKey(t), hs); !errors.Is(err, ErrKeyMismatch) {
		t.Fatalf("unrelated key: %v", err)
	}
	// Restarting with the new key completes the rotation.
	re, err := NewPersistentStoreWithKey(nk, hs)
	if err != nil {
		t.Fatalf("roll forward: %v", err)
	}
	for _, n := range []string{"a", "b", "c", "d"} {
		mustReveal(t, re, "sec_"+n, "v-"+n)
	}
	if rawDump(t, hs, BucketRotation) != "" {
		t.Fatal("staging not cleaned after recovery")
	}
	if _, err := NewPersistentStoreWithKey(nk, hs); err != nil {
		t.Fatalf("reopen after recovery: %v", err)
	}
}

func TestPersistentRotationFinalizeFailure(t *testing.T) {
	oldKey, nk := newKey(t), newKey(t)
	hs := &hookStore{Store: persist.NewMemory()}
	s, _ := NewPersistentStoreWithKey(oldKey, hs)
	_ = s.Upsert(Secret{Name: "a", Value: "v-a"})
	_ = s.Upsert(Secret{Name: "b", Value: "v-b"})
	// Fail only the final state update (step 4).
	hs.setPut(func(bucket, _ string) error {
		var st secretsState
		if bucket == BucketMeta {
			if ok, _ := hs.Store.Get(BucketMeta, metaStateKey, &st); ok && st.Rotating {
				return errors.New("io error")
			}
		}
		return nil
	})
	if err := s.RotateKey(nk); !errors.Is(err, ErrRotationIncomplete) {
		t.Fatalf("expected ErrRotationIncomplete, got %v", err)
	}
	hs.setPut(nil)
	// The new key is in effect: later writes use it.
	if err := s.Upsert(Secret{Name: "a", Value: "v-a2"}); err != nil {
		t.Fatal(err)
	}
	if ok, err := s.Remove("sec_b"); !ok || err != nil {
		t.Fatal(err)
	}
	re, err := NewPersistentStoreWithKey(nk, hs)
	if err != nil {
		t.Fatalf("reopen with new key: %v", err)
	}
	mustReveal(t, re, "sec_a", "v-a2") // later write not overwritten by staging
	if _, ok := re.Get("sec_b"); ok {
		t.Fatal("deleted secret resurrected from staging")
	}
}

func TestEnablePersistenceMergesMemory(t *testing.T) {
	key := newKey(t)
	ps := persist.NewMemory()
	first, _ := NewPersistentStoreWithKey(key, ps)
	_ = first.Upsert(Secret{Name: "disk", Value: "d"})
	_ = first.Upsert(Secret{Name: "both", Value: "old"})

	s, _ := NewStoreWithKey(key)
	_ = s.Upsert(Secret{Name: "mem", Value: "m"})
	_ = s.Upsert(Secret{Name: "both", Value: "new"})
	if err := s.EnablePersistence(ps); err != nil {
		t.Fatal(err)
	}
	mustReveal(t, s, "sec_disk", "d")
	mustReveal(t, s, "sec_both", "new")
	re, _ := NewPersistentStoreWithKey(key, ps)
	mustReveal(t, re, "sec_mem", "m")
	mustReveal(t, re, "sec_both", "new")
}
