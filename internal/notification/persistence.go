package notification

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	"github.com/ayoubzulfiqar/aerollm/internal/persist"
)

// Buckets used when persistence is enabled.
const (
	BucketChannels      = "notification.channels"
	BucketSubscriptions = "notification.subscriptions"
)

var (
	// ErrPersistence wraps every failure of the durable backing store. The
	// in-memory state is left unchanged when a write-through fails.
	ErrPersistence = errors.New("notification: persistence failed")
	// ErrInvalidDocument wraps persisted documents that were skipped while
	// loading because they failed to decode or validate.
	ErrInvalidDocument = errors.New("notification: invalid persisted document skipped")
)

// NewStoreWithPersistence creates a store with strict (production)
// validation backed by ps. See EnablePersistence for the error contract:
// the store is nil only when persistence could not be enabled
// (errors.Is(err, ErrPersistence)); a non-nil store with a non-nil error
// means some persisted documents were skipped (ErrInvalidDocument).
func NewStoreWithPersistence(ps persist.Store) (*Store, error) {
	s := NewStore()
	if err := s.EnablePersistence(ps); err != nil {
		if errors.Is(err, ErrPersistence) || ps == nil {
			return nil, err
		}
		return s, err
	}
	return s, nil
}

// EnablePersistence loads the channels and subscriptions stored in ps into
// the store and writes every later mutation through to ps before applying
// it in memory. Persisted documents win over in-memory entries with the same
// id; in-memory entries missing from ps are written to it.
//
// Channel targets and metadata (which may embed webhook tokens and signing
// secrets) are persisted verbatim; protect the backing file accordingly
// (persist.OpenBolt creates it with 0600 permissions).
//
// Documents that fail to decode or validate, exceed capacity, or reference
// a missing channel are skipped; persistence is still enabled and the
// returned error wraps ErrInvalidDocument. When ps cannot be read or written
// the error wraps ErrPersistence and persistence is not enabled. Calling it
// with a nil store or more than once is an error.
func (s *Store) EnablePersistence(ps persist.Store) error {
	if ps == nil {
		return errors.New("notification: EnablePersistence: nil persist.Store")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ps != nil {
		return errors.New("notification: persistence already enabled")
	}

	var skipped []error
	skip := func(bucket, key, why string) {
		skipped = append(skipped, fmt.Errorf("%w: %s/%s: %s", ErrInvalidDocument, bucket, key, why))
	}

	channels := make(map[string]Channel, len(s.channels))
	for id, ch := range s.channels {
		channels[id] = ch
	}
	loadedCh := make(map[string]bool)
	err := ps.ForEach(BucketChannels, func(key string, raw json.RawMessage) error {
		var ch Channel
		if err := json.Unmarshal(raw, &ch); err != nil {
			skip(BucketChannels, key, "undecodable")
			return nil
		}
		if ch.ID != key {
			skip(BucketChannels, key, "id does not match key")
			return nil
		}
		if err := ValidateChannel(ch, s.opts); err != nil {
			skip(BucketChannels, key, err.Error())
			return nil
		}
		if _, exists := channels[key]; !exists && len(channels) >= s.opts.MaxChannels {
			skip(BucketChannels, key, "channel capacity exceeded")
			return nil
		}
		channels[key] = copyChannel(ch)
		loadedCh[key] = true
		return nil
	})
	if err != nil {
		return fmt.Errorf("%w: load %s: %w", ErrPersistence, BucketChannels, err)
	}

	subs := make(map[string]Subscription, len(s.subscriptions))
	for id, sub := range s.subscriptions {
		subs[id] = sub
	}
	loadedSub := make(map[string]bool)
	err = ps.ForEach(BucketSubscriptions, func(key string, raw json.RawMessage) error {
		var sub Subscription
		if err := json.Unmarshal(raw, &sub); err != nil {
			skip(BucketSubscriptions, key, "undecodable")
			return nil
		}
		if sub.ID != key || !validID(key) {
			skip(BucketSubscriptions, key, "id does not match key")
			return nil
		}
		if err := ValidateSubscription(sub); err != nil {
			skip(BucketSubscriptions, key, err.Error())
			return nil
		}
		if _, ok := channels[sub.ChannelID]; !ok {
			skip(BucketSubscriptions, key, "references a missing channel")
			return nil
		}
		if _, exists := subs[key]; !exists && len(subs) >= s.opts.MaxSubscriptions {
			skip(BucketSubscriptions, key, "subscription capacity exceeded")
			return nil
		}
		subs[key] = sub
		loadedSub[key] = true
		return nil
	})
	if err != nil {
		return fmt.Errorf("%w: load %s: %w", ErrPersistence, BucketSubscriptions, err)
	}

	// Write in-memory entries that the backing store does not know yet.
	for id, ch := range channels {
		if loadedCh[id] {
			continue
		}
		if err := ps.Put(BucketChannels, id, ch); err != nil {
			return fmt.Errorf("%w: write %s/%s: %w", ErrPersistence, BucketChannels, id, err)
		}
	}
	for id, sub := range subs {
		if loadedSub[id] {
			continue
		}
		if err := ps.Put(BucketSubscriptions, id, sub); err != nil {
			return fmt.Errorf("%w: write %s/%s: %w", ErrPersistence, BucketSubscriptions, id, err)
		}
	}

	s.channels = channels
	s.subscriptions = subs
	s.ps = ps
	return errors.Join(skipped...)
}

// putLocked writes a document through to the backing store, if any. The
// caller holds s.mu.
func (s *Store) putLocked(bucket, key string, v any) error {
	if s.ps == nil {
		return nil
	}
	if err := s.ps.Put(bucket, key, v); err != nil {
		return fmt.Errorf("%w: write %s/%s: %w", ErrPersistence, bucket, key, err)
	}
	return nil
}

// deleteLocked removes a document from the backing store, if any. The
// caller holds s.mu.
func (s *Store) deleteLocked(bucket, key string) error {
	if s.ps == nil {
		return nil
	}
	if err := s.ps.Delete(bucket, key); err != nil {
		return fmt.Errorf("%w: delete %s/%s: %w", ErrPersistence, bucket, key, err)
	}
	return nil
}

// RemoveChannel deletes a channel and every subscription referencing it.
// It returns ErrNotFound when id is unknown and an error wrapping
// ErrPersistence when the deletion could not be persisted. Subscriptions are
// removed before the channel, so a failure never leaves a subscription
// pointing at a deleted channel; whatever was already removed durably is
// also removed from memory.
func (s *Store) RemoveChannel(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.channels[id]; !ok {
		return ErrNotFound
	}
	for sid, sub := range s.subscriptions {
		if sub.ChannelID != id {
			continue
		}
		if err := s.deleteLocked(BucketSubscriptions, sid); err != nil {
			return err
		}
		delete(s.subscriptions, sid)
	}
	if err := s.deleteLocked(BucketChannels, id); err != nil {
		return err
	}
	delete(s.channels, id)
	return nil
}

// RemoveSubscription deletes a subscription. It returns ErrNotFound when id
// is unknown and an error wrapping ErrPersistence when the deletion could
// not be persisted (memory is then unchanged).
func (s *Store) RemoveSubscription(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.subscriptions[id]; !ok {
		return ErrNotFound
	}
	if err := s.deleteLocked(BucketSubscriptions, id); err != nil {
		return err
	}
	delete(s.subscriptions, id)
	return nil
}

func logPersistError(op, id string, err error) {
	slog.Error("notification: persistence failure", "op", op, "id", id, "error", err.Error())
}
