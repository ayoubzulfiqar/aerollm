package compliance

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/ayoubzulfiqar/aerollm/internal/persist"
)

type flakyStore struct {
	persist.Store
	failPut, failDelete atomic.Bool
}

func (f *flakyStore) Put(b, k string, v any) error {
	if f.failPut.Load() {
		return errors.New("disk full")
	}
	return f.Store.Put(b, k, v)
}

func (f *flakyStore) Delete(b, k string) error {
	if f.failDelete.Load() {
		return errors.New("io error")
	}
	return f.Store.Delete(b, k)
}

func TestPersistentHTTPPolicyStore(t *testing.T) {
	path := filepath.Join(t.TempDir(), "policy.db")
	ps, err := persist.OpenBolt(path)
	if err != nil {
		t.Fatal(err)
	}
	s, err := NewPersistentHTTPPolicyStore(ps)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.UpsertRule(HTTPPolicyRule{ID: "no-delete", Expression: "deny-DELETE", Severity: "BLOCK"}); err != nil {
		t.Fatal(err)
	}
	if err := s.UpsertRule(HTTPPolicyRule{ID: "tmp", Expression: "allow"}); err != nil {
		t.Fatal(err)
	}
	if err := s.UpsertRule(HTTPPolicyRule{ID: "bad", Expression: "rm -rf"}); err == nil || errors.Is(err, ErrPersistence) {
		t.Fatalf("invalid rule: %v", err)
	}
	if ok, err := s.RemoveRule("tmp"); !ok || err != nil {
		t.Fatalf("remove: %v %v", ok, err)
	}
	_ = ps.Close()

	ps, err = persist.OpenBolt(path)
	if err != nil {
		t.Fatal(err)
	}
	defer ps.Close()
	s, err = NewPersistentHTTPPolicyStore(ps)
	if err != nil {
		t.Fatal(err)
	}
	rules := s.ListRules()
	if len(rules) != 1 || rules[0].ID != "no-delete" || rules[0].Severity != "block" {
		t.Fatalf("rules after restart: %+v", rules)
	}
	// The restored rule is enforced.
	h := HTTPBlockHandler(s)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodDelete, "/x", nil))
	if rec.Code != http.StatusUnavailableForLegalReasons {
		t.Fatalf("restored rule not enforced: %d", rec.Code)
	}
	if err := s.EnablePersistence(ps); err == nil {
		t.Fatal("enabling twice must fail")
	}
}

func TestPersistentHTTPPolicyStoreErrors(t *testing.T) {
	fs := &flakyStore{Store: persist.NewMemory()}
	s, err := NewPersistentHTTPPolicyStore(fs)
	if err != nil {
		t.Fatal(err)
	}
	_ = s.UpsertRule(HTTPPolicyRule{ID: "r1", Expression: "allow"})
	fs.failPut.Store(true)
	fs.failDelete.Store(true)
	if err := s.UpsertRule(HTTPPolicyRule{ID: "r2", Expression: "deny"}); !errors.Is(err, ErrPersistence) {
		t.Fatalf("upsert: %v", err)
	}
	if _, ok := s.GetRule("r2"); ok {
		t.Fatal("failed upsert visible")
	}
	if ok, err := s.RemoveRule("r1"); !ok || !errors.Is(err, ErrPersistence) {
		t.Fatalf("remove: %v %v", ok, err)
	}
	if s.DeleteRule("r1") {
		t.Fatal("DeleteRule must report failure")
	}
	handler := HTTPPolicyHandler(s)
	rec := httptest.NewRecorder()
	handler(rec, httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"id":"r3","expression":"allow"}`)))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("handler upsert: %d", rec.Code)
	}
	rec = httptest.NewRecorder()
	handler(rec, httptest.NewRequest(http.MethodDelete, "/?id=r1", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("handler delete: %d", rec.Code)
	}

	bad := persist.NewMemory()
	_ = bad.Put(BucketHTTPRules, "x", HTTPPolicyRule{ID: "x", Expression: "not-an-expression"})
	if _, err := NewPersistentHTTPPolicyStore(bad); err == nil {
		t.Fatal("invalid stored rule must be rejected")
	}
	if _, err := NewPersistentHTTPPolicyStore(nil); err == nil {
		t.Fatal("nil store must be rejected")
	}
}

func TestPersistentAuditLoggerCappedAndDurable(t *testing.T) {
	ps := persist.NewMemory()
	l, err := NewPersistentAuditLogger(ps, 3)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		if err := l.Append(&AuditEvent{Policy: fmt.Sprintf("p%d", i), Decision: "deny", Input: map[string]interface{}{
			"authorization": "Bearer sk-live-secret", "path": "/v1/x",
		}}); err != nil {
			t.Fatal(err)
		}
	}
	if len(l.Events()) != 3 || l.Dropped() != 2 {
		t.Fatalf("events=%d dropped=%d", len(l.Events()), l.Dropped())
	}
	onDisk := 0
	_ = ps.ForEach(BucketAudit, func(_ string, raw json.RawMessage) error {
		onDisk++
		if strings.Contains(string(raw), "sk-live-secret") {
			t.Fatal("credential persisted in audit log")
		}
		return nil
	})
	if onDisk != 3 {
		t.Fatalf("evicted events must be deleted from disk: %d stored", onDisk)
	}

	re, err := NewPersistentAuditLogger(ps, 3)
	if err != nil {
		t.Fatal(err)
	}
	evs := re.Events()
	if len(evs) != 3 || evs[0].Policy != "p2" || evs[2].Policy != "p4" || evs[0].Input["authorization"] != "[REDACTED]" {
		t.Fatalf("restored events: %+v", evs)
	}
	re.Log(&AuditEvent{Policy: "p5"})
	if evs := re.Events(); evs[2].Policy != "p5" || evs[0].Policy != "p3" {
		t.Fatal("ordering after restart")
	}

	// A smaller capacity on restart trims (and deletes) the oldest events.
	small, err := NewPersistentAuditLogger(ps, 1)
	if err != nil {
		t.Fatal(err)
	}
	if evs := small.Events(); len(evs) != 1 || evs[0].Policy != "p5" {
		t.Fatalf("trimmed: %+v", evs)
	}
	onDisk = 0
	_ = ps.ForEach(BucketAudit, func(string, json.RawMessage) error { onDisk++; return nil })
	if onDisk != 1 {
		t.Fatalf("trim must delete from disk: %d", onDisk)
	}
	small.Clear()
	if again, _ := NewPersistentAuditLogger(ps, 10); len(again.Events()) != 0 {
		t.Fatal("Clear must remove persisted events")
	}
	if !small.Persistent() || NewMemoryAuditLogger().Persistent() {
		t.Fatal("Persistent() mismatch")
	}
}

func TestPersistentAuditLoggerSurfacesErrors(t *testing.T) {
	fs := &flakyStore{Store: persist.NewMemory()}
	l, err := NewPersistentAuditLogger(fs, 2)
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var seen []error
	l.SetErrorHandler(func(err error) {
		// Runs outside the logger's lock, so it may use the logger.
		_, _ = l.PersistErrors()
		mu.Lock()
		seen = append(seen, err)
		mu.Unlock()
	})
	l.Log(&AuditEvent{Policy: "a"})
	fs.failPut.Store(true)
	l.Log(&AuditEvent{Policy: "b"})
	if err := l.Append(&AuditEvent{Policy: "c"}); !errors.Is(err, ErrPersistence) {
		t.Fatalf("append: %v", err)
	}
	if n, last := l.PersistErrors(); n != 2 || last == nil {
		t.Fatalf("PersistErrors = %d, %v", n, last)
	}
	if evs := l.Events(); len(evs) != 1 {
		t.Fatalf("unpersisted events must not be retained: %d", len(evs))
	}
	fs.failPut.Store(false)
	fs.failDelete.Store(true)
	l.Log(&AuditEvent{Policy: "d"})
	if err := l.Append(&AuditEvent{Policy: "e"}); !errors.Is(err, ErrPersistence) {
		t.Fatalf("eviction failure must be reported: %v", err)
	}
	fs.failDelete.Store(false)
	if err := l.Append(&AuditEvent{Policy: "f"}); err != nil {
		t.Fatal(err)
	}
	// The failed deletion was retried: only the retained window is stored.
	onDisk := 0
	_ = fs.ForEach(BucketAudit, func(string, json.RawMessage) error { onDisk++; return nil })
	if onDisk != 2 {
		t.Fatalf("stored events = %d, want 2", onDisk)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(seen) != 3 {
		t.Fatalf("error handler calls = %d", len(seen))
	}
	// Unencodable input is persisted as a marker instead of being dropped.
	if err := l.Append(&AuditEvent{Policy: "nan", Input: map[string]interface{}{"x": math.NaN()}}); err != nil {
		t.Fatal(err)
	}
	re, err := NewPersistentAuditLogger(fs, 2)
	if err != nil {
		t.Fatal(err)
	}
	if evs := re.Events(); evs[1].Policy != "nan" || evs[1].Input["_unserializable_input"] != true {
		t.Fatalf("unencodable input: %+v", evs[1])
	}
}
