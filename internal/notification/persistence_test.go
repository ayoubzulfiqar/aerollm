package notification

import (
	"encoding/json"
	"errors"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/ayoubzulfiqar/aerollm/internal/persist"
)

// failingStore wraps a persist.Store and fails writes when fail is set.
type failingStore struct {
	persist.Store
	mu   sync.Mutex
	fail bool
}

var errDisk = errors.New("disk on fire")

func (f *failingStore) setFail(v bool) {
	f.mu.Lock()
	f.fail = v
	f.mu.Unlock()
}

func (f *failingStore) failing() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.fail
}

func (f *failingStore) Put(bucket, key string, v any) error {
	if f.failing() {
		return errDisk
	}
	return f.Store.Put(bucket, key, v)
}

func (f *failingStore) Delete(bucket, key string) error {
	if f.failing() {
		return errDisk
	}
	return f.Store.Delete(bucket, key)
}

func (f *failingStore) ForEach(bucket string, fn func(string, json.RawMessage) error) error {
	if f.failing() {
		return errDisk
	}
	return f.Store.ForEach(bucket, fn)
}

func seed(t *testing.T, s *Store) {
	t.Helper()
	if _, err := s.UpsertChannel(Channel{ID: "hook", Name: "hook", Type: ChannelWebhook, Target: "https://hooks.example.com/abc", Enabled: true, Metadata: map[string]string{SigningSecretKey: "s3cret"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.UpsertChannel(Channel{ID: "mail", Name: "mail", Type: ChannelEmail, Target: "ops@example.com", Enabled: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.UpsertSubscription(Subscription{ID: "s1", AlertID: "cpu", ChannelID: "hook", Enabled: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.UpsertSubscription(Subscription{ID: "s2", AlertID: "cpu", ChannelID: "mail", Enabled: true}); err != nil {
		t.Fatal(err)
	}
}

func TestPersistenceReloadMemoryAndBolt(t *testing.T) {
	bolt, err := persist.OpenBolt(filepath.Join(t.TempDir(), "n.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { bolt.Close() })
	for name, ps := range map[string]persist.Store{"memory": persist.NewMemory(), "bolt": bolt} {
		t.Run(name, func(t *testing.T) {
			s1, err := NewStoreWithPersistence(ps)
			if err != nil {
				t.Fatal(err)
			}
			seed(t, s1)
			if _, err := s1.ModifyChannel("hook", func(c *Channel) error { c.Name = "renamed"; return nil }); err != nil {
				t.Fatal(err)
			}
			if _, err := s1.ModifySubscription("s2", func(s *Subscription) error { s.Enabled = false; return nil }); err != nil {
				t.Fatal(err)
			}
			if err := s1.RemoveSubscription("s1"); err != nil {
				t.Fatal(err)
			}

			s2, err := NewStoreWithPersistence(ps)
			if err != nil {
				t.Fatal(err)
			}
			ch, ok := s2.GetChannel("hook")
			if !ok || ch.Name != "renamed" || ch.Metadata[SigningSecretKey] != "s3cret" {
				t.Fatalf("channel not reloaded with secrets: %+v", ch)
			}
			if _, ok := s2.GetSubscription("s1"); ok {
				t.Fatal("deleted subscription came back")
			}
			if sub, ok := s2.GetSubscription("s2"); !ok || sub.Enabled {
				t.Fatalf("subscription update not persisted: %+v", sub)
			}
			// Cascade delete is durable too.
			if err := s2.RemoveChannel("mail"); err != nil {
				t.Fatal(err)
			}
			s3, err := NewStoreWithPersistence(ps)
			if err != nil {
				t.Fatal(err)
			}
			if len(s3.ListChannels()) != 1 || len(s3.ListSubscriptions()) != 0 {
				t.Fatalf("cascade not persisted: %v %v", s3.ListChannels(), s3.ListSubscriptions())
			}
		})
	}
}

func TestEnablePersistenceMergesAndRejectsMisuse(t *testing.T) {
	ps := persist.NewMemory()
	s := NewStore()
	if err := s.EnablePersistence(nil); err == nil {
		t.Fatal("nil store must be rejected")
	}
	seed(t, s) // in-memory entries written on enable
	if err := s.EnablePersistence(ps); err != nil {
		t.Fatal(err)
	}
	if err := s.EnablePersistence(ps); err == nil {
		t.Fatal("second enable must fail")
	}
	got, err := persist.LoadAll[Channel](ps, BucketChannels)
	if err != nil || len(got) != 2 {
		t.Fatalf("in-memory channels not written: %v %v", got, err)
	}
	subs, err := persist.LoadAll[Subscription](ps, BucketSubscriptions)
	if err != nil || len(subs) != 2 {
		t.Fatalf("in-memory subscriptions not written: %v %v", subs, err)
	}
}

func TestEnablePersistenceSkipsInvalidDocuments(t *testing.T) {
	ps := persist.NewMemory()
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(ps.Put(BucketChannels, "ok", Channel{ID: "ok", Name: "ok", Type: ChannelEmail, Target: "ops@example.com", Enabled: true}))
	must(ps.Put(BucketChannels, "private", Channel{ID: "private", Name: "p", Type: ChannelWebhook, Target: "https://127.0.0.1/x", Enabled: true}))
	must(ps.Put(BucketChannels, "mismatch", Channel{ID: "other", Name: "m", Type: ChannelEmail, Target: "ops@example.com"}))
	must(ps.Put(BucketChannels, "garbage", "not a channel"))
	must(ps.Put(BucketSubscriptions, "s-ok", Subscription{ID: "s-ok", AlertID: "a", ChannelID: "ok", Enabled: true}))
	must(ps.Put(BucketSubscriptions, "s-orphan", Subscription{ID: "s-orphan", AlertID: "a", ChannelID: "private", Enabled: true}))

	s, err := NewStoreWithPersistence(ps)
	if s == nil {
		t.Fatalf("store must be usable despite invalid documents: %v", err)
	}
	if !errors.Is(err, ErrInvalidDocument) || errors.Is(err, ErrPersistence) {
		t.Fatalf("expected ErrInvalidDocument, got %v", err)
	}
	for _, key := range []string{"private", "mismatch", "garbage", "s-orphan"} {
		if !strings.Contains(err.Error(), key) {
			t.Errorf("error does not mention %q: %v", key, err)
		}
	}
	if len(s.ListChannels()) != 1 || len(s.ListSubscriptions()) != 1 {
		t.Fatalf("unexpected contents: %v %v", s.ListChannels(), s.ListSubscriptions())
	}
	// Persistence is enabled: new writes go through.
	if _, err := s.UpsertChannel(Channel{ID: "new", Name: "n", Type: ChannelEmail, Target: "a@example.com"}); err != nil {
		t.Fatal(err)
	}
	var ch Channel
	if ok, _ := ps.Get(BucketChannels, "new", &ch); !ok {
		t.Fatal("write-through after enable failed")
	}
}

func TestPersistenceFailuresLeaveMemoryUnchanged(t *testing.T) {
	fs := &failingStore{Store: persist.NewMemory()}
	s, err := NewStoreWithPersistence(fs)
	if err != nil {
		t.Fatal(err)
	}
	seed(t, s)
	fs.setFail(true)

	if _, err := s.UpsertChannel(Channel{ID: "x", Name: "x", Type: ChannelEmail, Target: "x@example.com"}); !errors.Is(err, ErrPersistence) {
		t.Fatalf("upsert: expected ErrPersistence, got %v", err)
	}
	if _, ok := s.GetChannel("x"); ok {
		t.Fatal("failed upsert changed memory")
	}
	if _, err := s.ModifyChannel("hook", func(c *Channel) error { c.Name = "changed"; return nil }); !errors.Is(err, ErrPersistence) {
		t.Fatalf("modify: expected ErrPersistence, got %v", err)
	}
	if ch, _ := s.GetChannel("hook"); ch.Name != "hook" {
		t.Fatal("failed modify changed memory")
	}
	if _, err := s.UpsertSubscription(Subscription{ID: "s3", AlertID: "a", ChannelID: "hook"}); !errors.Is(err, ErrPersistence) {
		t.Fatalf("subscription upsert: expected ErrPersistence, got %v", err)
	}
	if _, err := s.ModifySubscription("s1", func(s *Subscription) error { s.Enabled = false; return nil }); !errors.Is(err, ErrPersistence) {
		t.Fatalf("subscription modify: expected ErrPersistence, got %v", err)
	}
	if sub, _ := s.GetSubscription("s1"); !sub.Enabled {
		t.Fatal("failed subscription modify changed memory")
	}
	if err := s.RemoveChannel("hook"); !errors.Is(err, ErrPersistence) {
		t.Fatalf("remove channel: expected ErrPersistence, got %v", err)
	}
	if s.DeleteChannel("hook") {
		t.Fatal("DeleteChannel must report false when the delete was not persisted")
	}
	if _, ok := s.GetChannel("hook"); !ok {
		t.Fatal("failed delete removed the channel from memory")
	}
	if _, ok := s.GetSubscription("s1"); !ok {
		t.Fatal("failed cascade removed a subscription from memory")
	}
	if err := s.RemoveSubscription("s2"); !errors.Is(err, ErrPersistence) {
		t.Fatalf("remove subscription: expected ErrPersistence, got %v", err)
	}
	if s.DeleteSubscription("s2") {
		t.Fatal("DeleteSubscription must report false on persistence failure")
	}

	// HTTP handlers map persistence failures to 500 without details.
	h := WebhookHandler(s)
	for _, tc := range []struct{ method, target, body string }{
		{http.MethodPost, "/v1/notification/channels", `{"id":"y","name":"y","type":"email","target":"y@example.com"}`},
		{http.MethodDelete, "/v1/notification/channels?id=hook", ""},
		{http.MethodDelete, "/v1/notification/subscriptions?id=s1", ""},
		{http.MethodPatch, "/v1/notification/subscriptions?id=s1", `{"enabled":false}`},
	} {
		rec := serve(h, tc.method, tc.target, tc.body)
		if rec.Code != http.StatusInternalServerError || !strings.Contains(rec.Body.String(), `"persistence failure"`) || strings.Contains(rec.Body.String(), errDisk.Error()) {
			t.Fatalf("%s %s: got %d %s", tc.method, tc.target, rec.Code, rec.Body.String())
		}
	}

	// Reads keep working; once the disk recovers, writes succeed again.
	fs.setFail(false)
	if err := s.RemoveChannel("hook"); err != nil {
		t.Fatal(err)
	}
}

func TestEnablePersistenceLoadFailure(t *testing.T) {
	fs := &failingStore{Store: persist.NewMemory()}
	fs.setFail(true)
	s, err := NewStoreWithPersistence(fs)
	if s != nil || !errors.Is(err, ErrPersistence) {
		t.Fatalf("expected ErrPersistence and nil store, got %v %v", s, err)
	}
	// A failed enable leaves the store in-memory and allows a retry.
	st := NewStore()
	if err := st.EnablePersistence(fs); !errors.Is(err, ErrPersistence) {
		t.Fatalf("expected ErrPersistence, got %v", err)
	}
	fs.setFail(false)
	if err := st.EnablePersistence(fs); err != nil {
		t.Fatalf("retry after failure: %v", err)
	}
}
