package marketplace

import (
	"context"
	"errors"
	"time"
)

var (
	// ErrLockTimeout is returned by Publish when the per-plugin publish lock
	// could not be acquired within the store's bounded wait (another instance
	// is publishing the same plugin). The request can be retried.
	ErrLockTimeout = errors.New("marketplace: timed out waiting for publish lock")
	// ErrLockLost is returned by Publish when the publish lock expired before
	// the write was committed (for example after a long stall); nothing was
	// written. The request can be retried.
	ErrLockLost = errors.New("marketplace: publish lock lost before write")
)

// Defaults for the Redis publish lock.
const (
	// DefaultPublishLockTTL is how long a publish lock lives if its holder
	// dies without releasing it.
	DefaultPublishLockTTL = 15 * time.Second
	// DefaultPublishLockWait bounds how long Publish waits for the lock.
	DefaultPublishLockWait = 5 * time.Second
	// unlockTimeout bounds the release round trip, which runs even when the
	// publish context has been canceled.
	unlockTimeout = 2 * time.Second
)

// PublishLocker is an optional Store extension that serialises publishes of
// a plugin across every registry instance sharing the store. RegistryService
// acquires it (before its in-process lock) around the read-check-write
// sequence of Publish, so two instances cannot both pass the ownership and
// version checks for the same plugin. RedisStore implements it.
type PublishLocker interface {
	// LockPublish blocks until the lock for pluginID is held, ctx is done or
	// the implementation's bounded wait elapses (ErrLockTimeout).
	LockPublish(ctx context.Context, pluginID string) (PublishLock, error)
}

// PublishLock is a held per-plugin publish lock.
type PublishLock interface {
	// Unlock releases the lock if it is still held by this holder; a lock
	// that expired and was taken over by someone else is left alone. It is
	// safe to call more than once.
	Unlock(ctx context.Context) error
}

// fencedPublishLock is implemented by locks whose store can commit the
// publish atomically with a check that the lock is still held (a fenced
// write), and optionally with a store-wide trust-on-first-use pin of the
// creator's key.
type fencedPublishLock interface {
	PublishLock
	// putFenced writes manifest and meta only while the lock is held
	// (ErrLockLost otherwise). With pinCreatorKey the write also fails with
	// ErrUntrustedKey unless manifest.PublicKey is pinned for its creator in
	// the shared store, pinning it atomically when the creator has no key yet.
	putFenced(ctx context.Context, manifest VerifiedManifest, meta Metadata, pinCreatorKey bool) error
}

// creatorKeySeeder is implemented by stores that keep a shared creator key
// registry; seedTrustLocked back-fills it with the keys of manifests that are
// already stored.
type creatorKeySeeder interface {
	seedCreatorKeys(ctx context.Context, keys map[string][][]byte) error
}
