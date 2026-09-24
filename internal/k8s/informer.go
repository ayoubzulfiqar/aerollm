package k8s

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"math/rand/v2"
	"sync/atomic"
	"time"
)

// Backoff computes exponential retry delays with jitter.
type Backoff struct {
	// Initial is the first delay (default 500ms).
	Initial time.Duration
	// Max caps the delay (default 30s).
	Max time.Duration
	// Factor multiplies the delay after each failure (default 2).
	Factor float64

	attempt int
}

// Next returns the delay before the next retry: the exponential step with
// "equal jitter" (between half and all of it), capped at Max.
func (b *Backoff) Next() time.Duration {
	initial, maxD, factor := b.Initial, b.Max, b.Factor
	if initial <= 0 {
		initial = 500 * time.Millisecond
	}
	if maxD <= 0 {
		maxD = 30 * time.Second
	}
	if maxD < initial {
		maxD = initial
	}
	if factor < 1 {
		factor = 2
	}
	d := float64(initial)
	for i := 0; i < b.attempt && d < float64(maxD); i++ {
		d *= factor
	}
	if d > float64(maxD) {
		d = float64(maxD)
	}
	b.attempt++
	half := d / 2
	return time.Duration(half + rand.Float64()*half)
}

// Reset starts the sequence over.
func (b *Backoff) Reset() { b.attempt = 0 }

// Attempts reports how many delays were handed out since the last Reset.
func (b *Backoff) Attempts() int { return b.attempt }

// Sleep waits for d or until ctx is done; it reports whether the full
// delay elapsed.
func Sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// Informer keeps a local view of one collection in sync by listing it and
// then watching from the list's resourceVersion. It handles:
//
//   - watch resumption from the last seen resourceVersion (including
//     BOOKMARK events) when the server ends a stream;
//   - 410 Gone / Expired (HTTP status or ERROR event) with a full relist,
//     synthesizing DELETED events for objects that vanished meanwhile;
//   - exponential backoff with jitter on errors; and
//   - context cancellation (Run returns nil).
//
// Handle is called sequentially from Run's goroutine with ADDED, MODIFIED
// and DELETED events; it should not block for long.
type Informer struct {
	Client    *Client
	Resource  GroupVersionResource
	Namespace string // "" = all namespaces
	Handle    func(WatchEvent)

	// WatchTimeout is the server-side watch duration (default 5m; each
	// watch uses a random value in [WatchTimeout, 2*WatchTimeout) to spread
	// reconnects).
	WatchTimeout time.Duration
	// PageSize for list pagination (default 500).
	PageSize int64
	// Backoff between failed attempts (zero value = 500ms..30s).
	Backoff Backoff
	Logger  *slog.Logger

	store  map[string]map[string]interface{} // key -> last object
	synced atomic.Bool
	lists  atomic.Int64
	lastRV atomic.Value // string
}

// LastResourceVersion reports the most recent resourceVersion observed
// (from a list, event or bookmark).
func (i *Informer) LastResourceVersion() string {
	v, _ := i.lastRV.Load().(string)
	return v
}

// HasSynced reports whether the initial list completed.
func (i *Informer) HasSynced() bool { return i.synced.Load() }

// Lists reports how many full lists were performed (initial + relists).
func (i *Informer) Lists() int64 { return i.lists.Load() }

// ObjectKey returns "namespace/name" (or "name" for cluster objects).
func ObjectKey(obj map[string]interface{}) string {
	ns, name := ObjectNamespace(obj), ObjectName(obj)
	if ns == "" {
		return name
	}
	return ns + "/" + name
}

func metadata(obj map[string]interface{}) map[string]interface{} {
	md, _ := obj["metadata"].(map[string]interface{})
	return md
}

// ObjectName returns metadata.name.
func ObjectName(obj map[string]interface{}) string {
	s, _ := metadata(obj)["name"].(string)
	return s
}

// ObjectNamespace returns metadata.namespace.
func ObjectNamespace(obj map[string]interface{}) string {
	s, _ := metadata(obj)["namespace"].(string)
	return s
}

// ObjectResourceVersion returns metadata.resourceVersion.
func ObjectResourceVersion(obj map[string]interface{}) string {
	s, _ := metadata(obj)["resourceVersion"].(string)
	return s
}

// ObjectGeneration returns metadata.generation (0 if absent).
func ObjectGeneration(obj map[string]interface{}) int64 {
	switch g := metadata(obj)["generation"].(type) {
	case float64:
		return int64(g)
	case int64:
		return g
	case int:
		return int64(g)
	}
	return 0
}

func (i *Informer) logger() *slog.Logger {
	if i.Logger != nil {
		return i.Logger
	}
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func (i *Informer) emit(ev WatchEvent) {
	if i.Handle != nil {
		i.Handle(ev)
	}
}

// Run lists and watches until ctx is cancelled.
func (i *Informer) Run(ctx context.Context) error {
	if i.Client == nil {
		return errors.New("k8s: informer has no client")
	}
	if i.store == nil {
		i.store = make(map[string]map[string]interface{})
	}
	log := i.logger().With("resource", i.Resource.String(), "namespace", i.Namespace)
	rv := ""
	for ctx.Err() == nil {
		if rv == "" {
			newRV, err := i.relist(ctx)
			if err != nil {
				if ctx.Err() != nil {
					break
				}
				d := i.Backoff.Next()
				log.Warn("list failed; retrying", "error", err.Error(), "backoff", d.String())
				Sleep(ctx, d)
				continue
			}
			rv = newRV
		}
		started := time.Now()
		newRV, err := i.watch(ctx, rv)
		if ctx.Err() != nil {
			break
		}
		switch {
		case (err == nil || errors.Is(err, io.EOF)) && time.Since(started) < time.Second && newRV == rv:
			// A stream that ends immediately without progress is treated
			// as a failure so a misbehaving server cannot cause a hot loop.
			rv = newRV
			Sleep(ctx, i.Backoff.Next())
		case err == nil || errors.Is(err, io.EOF):
			// Server closed the stream (timeout): resume immediately.
			rv = newRV
		case IsGone(err):
			log.Info("watch resourceVersion expired; relisting", "resource_version", newRV)
			if newRV == rv {
				// No progress on this stream: back off so a server that
				// answers every watch with 410 cannot cause a relist loop.
				Sleep(ctx, i.Backoff.Next())
			}
			rv = ""
		default:
			rv = newRV
			d := i.Backoff.Next()
			log.Warn("watch failed; retrying", "error", err.Error(), "backoff", d.String(), "resource_version", rv)
			Sleep(ctx, d)
		}
	}
	return nil
}

// relist replaces the local store with a fresh list, emitting ADDED for
// new objects, MODIFIED for changed ones and DELETED for vanished ones.
func (i *Informer) relist(ctx context.Context) (string, error) {
	items, rv, err := i.Client.ListAll(ctx, i.Resource, i.Namespace, i.PageSize)
	if err != nil {
		return "", err
	}
	i.lists.Add(1)
	seen := make(map[string]bool, len(items))
	for _, obj := range items {
		key := ObjectKey(obj)
		if key == "" {
			continue
		}
		seen[key] = true
		old, existed := i.store[key]
		i.store[key] = obj
		switch {
		case !existed:
			i.emit(WatchEvent{Type: EventAdded, Object: obj})
		case ObjectResourceVersion(old) != ObjectResourceVersion(obj):
			i.emit(WatchEvent{Type: EventModified, Object: obj})
		}
	}
	for key, old := range i.store {
		if !seen[key] {
			delete(i.store, key)
			i.emit(WatchEvent{Type: EventDeleted, Object: old})
		}
	}
	i.synced.Store(true)
	i.lastRV.Store(rv)
	return rv, nil
}

// watch streams events from rv and returns the last resourceVersion seen.
func (i *Informer) watch(ctx context.Context, rv string) (string, error) {
	timeout := i.WatchTimeout
	if timeout <= 0 {
		timeout = 5 * time.Minute
	}
	secs := int64(timeout/time.Second) + rand.Int64N(int64(timeout/time.Second)+1)
	if secs < 1 {
		secs = 1
	}
	w, err := i.Client.Watch(ctx, i.Resource, i.Namespace, WatchOptions{ResourceVersion: rv, TimeoutSeconds: secs, AllowBookmarks: true})
	if err != nil {
		return rv, err
	}
	defer w.Close()
	for {
		ev, err := w.Next()
		if err != nil {
			return rv, err
		}
		switch ev.Type {
		case EventError:
			return rv, ev.StatusError()
		case EventBookmark:
			if v := ObjectResourceVersion(ev.Object); v != "" {
				rv = v
				i.lastRV.Store(rv)
			}
			i.Backoff.Reset()
			continue
		}
		if ev.Object == nil {
			continue
		}
		if v := ObjectResourceVersion(ev.Object); v != "" {
			rv = v
			i.lastRV.Store(rv)
		}
		key := ObjectKey(ev.Object)
		if key == "" {
			continue
		}
		i.Backoff.Reset()
		switch ev.Type {
		case EventAdded, EventModified:
			i.store[key] = ev.Object
		case EventDeleted:
			delete(i.store, key)
		}
		i.emit(ev)
	}
}
