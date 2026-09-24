package federated

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/persist"
)

// Replay protection defaults and limits.
const (
	// DefaultMaxClockSkew is the default maximum distance between an
	// update's timestamp and the aggregator's clock.
	DefaultMaxClockSkew = 5 * time.Minute
	// DefaultRoundHistory is the default number of past round IDs that are
	// remembered (and therefore cannot be reopened).
	DefaultRoundHistory = 1024
	// MaxRoundIDLength is the maximum length of a round ID.
	MaxRoundIDLength = 128

	signedUpdateDomain = "aerollm/federated/update/v2"

	// Persistence buckets.
	bucketReplay = "federated_replay"
	bucketRounds = "federated_rounds"
	roundsKey    = "state"
)

var (
	// ErrInvalidSignedUpdate is returned for malformed signed updates
	// (missing fields, bad owner/round syntax, zero sequence or timestamp).
	ErrInvalidSignedUpdate = errors.New("federated: invalid signed update")
	// ErrInvalidRoundID is returned for malformed round IDs.
	ErrInvalidRoundID = errors.New("federated: invalid round id")
	// ErrNoOpenRound is returned when an update arrives while no round is open.
	ErrNoOpenRound = errors.New("federated: no round is open")
	// ErrStaleRound is returned when an update is bound to a round other than
	// the currently open one.
	ErrStaleRound = errors.New("federated: update is not for the current round")
	// ErrReplay is returned when an update's sequence number is not strictly
	// greater than the last sequence accepted from the same node.
	ErrReplay = errors.New("federated: replayed update")
	// ErrTimestampOutOfWindow is returned when an update's timestamp is too
	// far from the aggregator's clock or predates the round.
	ErrTimestampOutOfWindow = errors.New("federated: update timestamp outside the accepted window")
	// ErrAlreadyContributed is returned when a node submits a second update
	// for the same round.
	ErrAlreadyContributed = errors.New("federated: node already contributed to this round")
	// ErrRoundReused is returned when opening a round whose ID was used before.
	ErrRoundReused = errors.New("federated: round id was already used")
	// ErrRoundInProgress is returned when opening a round while the current
	// round still holds contributions (aggregate or abort it first).
	ErrRoundInProgress = errors.New("federated: current round has pending contributions")
	// ErrRoundFull is returned when the round reached its contributor limit.
	ErrRoundFull = errors.New("federated: round contributor limit reached")
	// ErrPersistence is returned when replay state cannot be persisted. The
	// update is rejected (fail closed) because accepting it without recording
	// its sequence would allow a replay after a restart.
	ErrPersistence = errors.New("federated: persistence failure")
)

var roundIDPattern = regexp.MustCompile(`^[A-Za-z0-9._:-]+$`)

// ValidateRoundID checks a round ID: 1-MaxRoundIDLength chars of
// [A-Za-z0-9._:-].
func ValidateRoundID(id string) error {
	if id == "" || len(id) > MaxRoundIDLength || !roundIDPattern.MatchString(id) {
		return fmt.Errorf("%w: must be 1-%d chars of [A-Za-z0-9._:-]", ErrInvalidRoundID, MaxRoundIDLength)
	}
	return nil
}

// DataDigest returns the hex SHA-256 of the matrix shape and contents in a
// language-neutral binary encoding: Rows and Cols as big-endian uint64,
// followed by the IEEE-754 bits of every element as big-endian uint64.
// Unlike Checksum it does not depend on Go's float formatting, so non-Go
// nodes can reproduce it exactly.
func (m *LoRAMatrix) DataDigest() string {
	h := sha256.New()
	var buf [4096]byte
	var rows, cols int
	var data []float64
	if m != nil {
		rows, cols, data = m.Rows, m.Cols, m.Data
	}
	binary.BigEndian.PutUint64(buf[0:8], uint64(rows))
	binary.BigEndian.PutUint64(buf[8:16], uint64(cols))
	n := 16
	for _, v := range data {
		if n+8 > len(buf) {
			h.Write(buf[:n])
			n = 0
		}
		binary.BigEndian.PutUint64(buf[n:n+8], math.Float64bits(v))
		n += 8
	}
	h.Write(buf[:n])
	return hex.EncodeToString(h.Sum(nil))
}

// SignedUpdate is a LoRA update bound to an aggregation round, a per-node
// sequence number and a timestamp, signed by the node's ed25519 key.
//
// The node identity is Update.Owner (the NodeID it registered with).
// Sequence acts as the per-node nonce: it must be strictly greater than every
// sequence previously accepted from the node (it need not be contiguous). A
// node that cannot persist a counter may use its clock, e.g.
// time.Now().UnixNano(). Sequence 0 is invalid.
//
// JSON: Signature is base64 (standard alphabet), TimestampMs is Unix
// milliseconds.
type SignedUpdate struct {
	Update      *LoRAMatrix `json:"update"`
	RoundID     string      `json:"round_id"`
	Sequence    uint64      `json:"sequence"`
	TimestampMs int64       `json:"timestamp_ms"`
	Signature   []byte      `json:"signature"`
}

// Validate checks every field except the signature itself.
func (su *SignedUpdate) Validate() error {
	if su == nil {
		return fmt.Errorf("%w: nil update", ErrInvalidSignedUpdate)
	}
	if su.Update == nil {
		return fmt.Errorf("%w: missing matrix", ErrInvalidSignedUpdate)
	}
	owner := su.Update.Owner
	if owner == "" || len(owner) > MaxNodeIDLength || !nodeIDPattern.MatchString(owner) {
		return fmt.Errorf("%w: owner must be 1-%d chars of [A-Za-z0-9._:-]", ErrInvalidSignedUpdate, MaxNodeIDLength)
	}
	if err := ValidateRoundID(su.RoundID); err != nil {
		return err
	}
	if su.Sequence == 0 {
		return fmt.Errorf("%w: sequence must be positive", ErrInvalidSignedUpdate)
	}
	if su.TimestampMs <= 0 {
		return fmt.Errorf("%w: timestamp_ms must be positive", ErrInvalidSignedUpdate)
	}
	return su.Update.Validate()
}

// SignedUpdatePayload returns the canonical bytes a node signs. All fields
// are syntax-restricted (no newlines), so the encoding is unambiguous:
//
//	aerollm/federated/update/v2
//	node=<owner>
//	round=<round_id>
//	seq=<sequence>
//	ts_ms=<timestamp_ms>
//	rows=<rows>
//	cols=<cols>
//	digest=<DataDigest()>
//
// Lines are separated by '\n' with no trailing newline.
func SignedUpdatePayload(su *SignedUpdate) []byte {
	if su == nil || su.Update == nil {
		return nil
	}
	var b strings.Builder
	b.WriteString(signedUpdateDomain)
	b.WriteString("\nnode=")
	b.WriteString(su.Update.Owner)
	b.WriteString("\nround=")
	b.WriteString(su.RoundID)
	b.WriteString("\nseq=")
	b.WriteString(strconv.FormatUint(su.Sequence, 10))
	b.WriteString("\nts_ms=")
	b.WriteString(strconv.FormatInt(su.TimestampMs, 10))
	b.WriteString("\nrows=")
	b.WriteString(strconv.Itoa(su.Update.Rows))
	b.WriteString("\ncols=")
	b.WriteString(strconv.Itoa(su.Update.Cols))
	b.WriteString("\ndigest=")
	b.WriteString(su.Update.DataDigest())
	return []byte(b.String())
}

// Sign validates su and sets its Signature using the node's private key.
func (su *SignedUpdate) Sign(priv ed25519.PrivateKey) error {
	if len(priv) != ed25519.PrivateKeySize {
		return fmt.Errorf("federated: invalid private key")
	}
	if err := su.Validate(); err != nil {
		return err
	}
	su.Signature = ed25519.Sign(priv, SignedUpdatePayload(su))
	return nil
}

// NewSignedUpdate builds and signs a replay-protected update for roundID.
func NewSignedUpdate(priv ed25519.PrivateKey, update *LoRAMatrix, roundID string, sequence uint64, ts time.Time) (*SignedUpdate, error) {
	su := &SignedUpdate{Update: update, RoundID: roundID, Sequence: sequence, TimestampMs: ts.UnixMilli()}
	if err := su.Sign(priv); err != nil {
		return nil, err
	}
	return su, nil
}

// NodeReplayState is the per-node replay protection state.
type NodeReplayState struct {
	LastSequence    uint64    `json:"last_sequence"`
	LastRound       string    `json:"last_round,omitempty"`
	LastTimestampMs int64     `json:"last_timestamp_ms,omitempty"`
	UpdatedAt       time.Time `json:"updated_at"`
}

// RoundStatus describes the currently open round.
type RoundStatus struct {
	Open         bool      `json:"open"`
	RoundID      string    `json:"round_id,omitempty"`
	OpenedAt     time.Time `json:"opened_at,omitempty"`
	Contributors []string  `json:"contributors"`
	Rows         int       `json:"rows,omitempty"`
	Cols         int       `json:"cols,omitempty"`
}

// RoundResult is the outcome of a finalized round.
type RoundResult struct {
	RoundID      string      `json:"round_id"`
	Aggregate    *LoRAMatrix `json:"aggregate"`
	Contributors []string    `json:"contributors"`
}

// SecureAggregatorConfig configures a SecureAggregator. Zero values select
// the defaults.
type SecureAggregatorConfig struct {
	// MaxClockSkew bounds |now - timestamp| for accepted updates
	// (default DefaultMaxClockSkew).
	MaxClockSkew time.Duration
	// RoundHistory is the number of past round IDs remembered to prevent
	// reopening them (default DefaultRoundHistory).
	RoundHistory int
	// MaxContributors caps the contributions per round (default MaxUpdates).
	MaxContributors int
	// Now overrides the clock (tests).
	Now func() time.Time
}

type openRound struct {
	id           string
	openedAt     time.Time
	contributors map[string]struct{}
	order        []string
	rows, cols   int
	mean         []float64
}

type persistedRounds struct {
	Current  string    `json:"current,omitempty"`
	OpenedAt time.Time `json:"opened_at,omitempty"`
	Used     []string  `json:"used"`
}

// SecureAggregator accepts signed, replay-protected updates into rounds and
// averages them (unweighted FedAvg, computed as a running mean so memory per
// round is one matrix regardless of the number of contributors).
//
// An update is accepted only if all of the following hold:
//   - it is well formed (SignedUpdate.Validate);
//   - its owner resolves to an ed25519 public key and the signature over
//     SignedUpdatePayload verifies;
//   - a round is open and RoundID equals it (other rounds are stale);
//   - Sequence is strictly greater than the node's last accepted sequence;
//   - |now - TimestampMs| <= MaxClockSkew and the timestamp is not earlier
//     than the round's opening time minus MaxClockSkew;
//   - the node has not already contributed to the round;
//   - its shape matches the round's first contribution.
//
// Cheap checks run before the (expensive) digest and signature check, and
// all checks are repeated under the lock before state is committed, so
// concurrent submissions of the same update cannot both succeed.
//
// SecureAggregator is safe for concurrent use.
type SecureAggregator struct {
	resolver        PublicKeyResolver
	skew            time.Duration
	roundHistory    int
	maxContributors int
	now             func() time.Time

	mu        sync.Mutex
	nodes     map[string]*NodeReplayState
	round     *openRound
	used      map[string]struct{}
	usedOrder []string
	store     persist.Store
}

// NewSecureAggregator creates a replay-protected aggregator that resolves
// node public keys through resolver (typically the *GatewayRegistry). A nil
// resolver is rejected: verification fails closed.
func NewSecureAggregator(resolver PublicKeyResolver, cfg SecureAggregatorConfig) (*SecureAggregator, error) {
	if resolver == nil {
		return nil, ErrVerificationNotConfigured
	}
	if cfg.MaxClockSkew < 0 || cfg.RoundHistory < 0 || cfg.MaxContributors < 0 {
		return nil, fmt.Errorf("federated: negative secure aggregator limits")
	}
	s := &SecureAggregator{
		resolver:        resolver,
		skew:            cfg.MaxClockSkew,
		roundHistory:    cfg.RoundHistory,
		maxContributors: cfg.MaxContributors,
		now:             cfg.Now,
		nodes:           make(map[string]*NodeReplayState),
		used:            make(map[string]struct{}),
	}
	if s.skew == 0 {
		s.skew = DefaultMaxClockSkew
	}
	if s.roundHistory == 0 {
		s.roundHistory = DefaultRoundHistory
	}
	if s.maxContributors == 0 || s.maxContributors > MaxUpdates {
		s.maxContributors = MaxUpdates
	}
	if s.now == nil {
		s.now = time.Now
	}
	return s, nil
}

// EnablePersistence loads replay state (per-node sequences, the open round
// ID and the used round IDs) from ps and writes every later change through
// to it (buckets "federated_replay" and "federated_rounds"). It must be
// called once, before the aggregator accepts updates.
//
// The partial aggregate of an open round is not persisted: after a restart
// the round is still open (its ID stays reserved) but holds no
// contributions, so nodes must resubmit with a new sequence number.
func (s *SecureAggregator) EnablePersistence(ps persist.Store) error {
	if s == nil || ps == nil {
		return fmt.Errorf("federated: nil aggregator or store")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.store != nil {
		return fmt.Errorf("federated: persistence already enabled")
	}
	if len(s.nodes) > 0 || s.round != nil || len(s.usedOrder) > 0 {
		return fmt.Errorf("federated: enable persistence before using the aggregator")
	}
	var rs persistedRounds
	if _, err := ps.Get(bucketRounds, roundsKey, &rs); err != nil {
		// A corrupt round document must not silently re-enable old round
		// IDs; refuse to start.
		return fmt.Errorf("federated: load round state: %w", err)
	}
	nodes := make(map[string]*NodeReplayState)
	var quarantined []string
	err := ps.ForEach(bucketReplay, func(id string, raw json.RawMessage) error {
		if id == "" || len(id) > MaxNodeIDLength || !nodeIDPattern.MatchString(id) {
			return nil
		}
		if len(nodes) >= MaxNodes {
			return nil
		}
		var st NodeReplayState
		if err := json.Unmarshal(raw, &st); err != nil {
			// Fail closed: without the last sequence an old update could
			// be replayed, so block the node until an operator calls
			// ResetNode.
			st = NodeReplayState{LastSequence: math.MaxUint64}
			quarantined = append(quarantined, id)
		}
		nodes[id] = &st
		return nil
	})
	if err != nil {
		return fmt.Errorf("federated: load replay state: %w", err)
	}
	s.nodes = nodes
	for _, id := range rs.Used {
		if ValidateRoundID(id) == nil {
			s.markUsedLocked(id)
		}
	}
	if rs.Current != "" && ValidateRoundID(rs.Current) == nil {
		s.markUsedLocked(rs.Current)
		s.round = &openRound{id: rs.Current, openedAt: rs.OpenedAt, contributors: make(map[string]struct{})}
	}
	s.store = ps
	if len(quarantined) > 0 {
		sort.Strings(quarantined)
		return fmt.Errorf("federated: undecodable replay state; nodes blocked until ResetNode: %s", strings.Join(quarantined, ","))
	}
	return nil
}

func (s *SecureAggregator) markUsedLocked(id string) {
	if _, ok := s.used[id]; ok {
		return
	}
	s.used[id] = struct{}{}
	s.usedOrder = append(s.usedOrder, id)
	for len(s.usedOrder) > s.roundHistory {
		delete(s.used, s.usedOrder[0])
		s.usedOrder[0] = ""
		s.usedOrder = s.usedOrder[1:]
	}
}

func (s *SecureAggregator) persistRoundsLocked(current *openRound) error {
	if s.store == nil {
		return nil
	}
	rs := persistedRounds{Used: append([]string(nil), s.usedOrder...)}
	if current != nil {
		rs.Current = current.id
		rs.OpenedAt = current.openedAt
	}
	if err := s.store.Put(bucketRounds, roundsKey, rs); err != nil {
		return fmt.Errorf("%w: %v", ErrPersistence, err)
	}
	return nil
}

// OpenRound opens a new round. Round IDs cannot be reused (within the
// remembered RoundHistory). Opening fails with ErrRoundInProgress while the
// current round holds contributions; an empty open round is replaced.
func (s *SecureAggregator) OpenRound(roundID string) error {
	if s == nil {
		return ErrVerificationNotConfigured
	}
	if err := ValidateRoundID(roundID); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.round != nil && len(s.round.order) > 0 {
		return fmt.Errorf("%w: round %q", ErrRoundInProgress, s.round.id)
	}
	if _, ok := s.used[roundID]; ok {
		return fmt.Errorf("%w: %q", ErrRoundReused, roundID)
	}
	r := &openRound{id: roundID, openedAt: s.now(), contributors: make(map[string]struct{})}
	prevUsed := append([]string(nil), s.usedOrder...)
	s.markUsedLocked(roundID)
	if err := s.persistRoundsLocked(r); err != nil {
		// Roll back the used-set change.
		s.used = make(map[string]struct{}, len(prevUsed))
		s.usedOrder = prevUsed
		for _, id := range prevUsed {
			s.used[id] = struct{}{}
		}
		return err
	}
	s.round = r
	return nil
}

// AbortRound closes the current round and discards its contributions. It
// returns the number of discarded contributions. The round ID stays used.
func (s *SecureAggregator) AbortRound() (int, error) {
	if s == nil {
		return 0, ErrVerificationNotConfigured
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.round == nil {
		return 0, ErrNoOpenRound
	}
	if err := s.persistRoundsLocked(nil); err != nil {
		return 0, err
	}
	n := len(s.round.order)
	s.round = nil
	return n, nil
}

// Round returns the status of the current round.
func (s *SecureAggregator) Round() RoundStatus {
	if s == nil {
		return RoundStatus{Contributors: []string{}}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.round == nil {
		return RoundStatus{Contributors: []string{}}
	}
	return RoundStatus{
		Open:         true,
		RoundID:      s.round.id,
		OpenedAt:     s.round.openedAt,
		Contributors: append([]string{}, s.round.order...),
		Rows:         s.round.rows,
		Cols:         s.round.cols,
	}
}

// NodeState returns a copy of a node's replay state.
func (s *SecureAggregator) NodeState(nodeID string) (NodeReplayState, bool) {
	if s == nil {
		return NodeReplayState{}, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	st, ok := s.nodes[nodeID]
	if !ok {
		return NodeReplayState{}, false
	}
	return *st, true
}

// ResetNode forgets a node's replay state (administrative action, e.g. after
// a node lost its sequence counter). Stale updates from the node remain
// bounded by the round binding and the timestamp window.
func (s *SecureAggregator) ResetNode(nodeID string) error {
	if s == nil {
		return ErrVerificationNotConfigured
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.store != nil {
		if err := s.store.Delete(bucketReplay, nodeID); err != nil {
			return fmt.Errorf("%w: %v", ErrPersistence, err)
		}
	}
	delete(s.nodes, nodeID)
	return nil
}

// checkLocked runs every replay/round/window check for su. It does not
// verify the signature.
func (s *SecureAggregator) checkLocked(su *SignedUpdate, now time.Time) error {
	r := s.round
	if r == nil {
		return ErrNoOpenRound
	}
	if su.RoundID != r.id {
		return ErrStaleRound
	}
	owner := su.Update.Owner
	if st, ok := s.nodes[owner]; ok && su.Sequence <= st.LastSequence {
		return ErrReplay
	}
	ts := time.UnixMilli(su.TimestampMs)
	if ts.Before(now.Add(-s.skew)) || ts.After(now.Add(s.skew)) || ts.Before(r.openedAt.Add(-s.skew)) {
		return ErrTimestampOutOfWindow
	}
	if _, ok := r.contributors[owner]; ok {
		return ErrAlreadyContributed
	}
	if len(r.order) >= s.maxContributors {
		return ErrRoundFull
	}
	if r.rows != 0 && (su.Update.Rows != r.rows || su.Update.Cols != r.cols) {
		return fmt.Errorf("%w: round is %dx%d", ErrDimensionMismatch, r.rows, r.cols)
	}
	if _, ok := s.nodes[owner]; !ok && len(s.nodes) >= MaxNodes {
		return ErrRegistryFull
	}
	return nil
}

func (s *SecureAggregator) verifySignature(su *SignedUpdate, pub ed25519.PublicKey) error {
	if len(su.Signature) != ed25519.SignatureSize {
		return ErrInvalidSignature
	}
	if !ed25519.Verify(pub, SignedUpdatePayload(su), su.Signature) {
		return ErrInvalidSignature
	}
	return nil
}

func (s *SecureAggregator) resolve(owner string) (ed25519.PublicKey, error) {
	pub, ok := s.resolver.PublicKey(owner)
	if !ok || len(pub) != ed25519.PublicKeySize {
		return nil, fmt.Errorf("%w: %q", ErrUnknownOwner, owner)
	}
	return pub, nil
}

// commitLocked persists and applies the replay state for accepted updates,
// then folds them into the round's running mean. On a persistence error
// nothing is applied.
func (s *SecureAggregator) commitLocked(sus []*SignedUpdate, now time.Time) error {
	states := make([]NodeReplayState, len(sus))
	for i, su := range sus {
		states[i] = NodeReplayState{
			LastSequence:    su.Sequence,
			LastRound:       su.RoundID,
			LastTimestampMs: su.TimestampMs,
			UpdatedAt:       now,
		}
		if s.store != nil {
			if err := s.store.Put(bucketReplay, su.Update.Owner, states[i]); err != nil {
				return fmt.Errorf("%w: %v", ErrPersistence, err)
			}
		}
	}
	r := s.round
	for i, su := range sus {
		st := states[i]
		s.nodes[su.Update.Owner] = &st
		r.contributors[su.Update.Owner] = struct{}{}
		r.order = append(r.order, su.Update.Owner)
		if r.mean == nil {
			r.rows, r.cols = su.Update.Rows, su.Update.Cols
			r.mean = make([]float64, len(su.Update.Data))
		}
		// mean_k = mean_{k-1}*(k-1)/k + x/k keeps every value bounded by
		// max|x|, so finite inputs cannot overflow.
		f := 1 / float64(len(r.order))
		for j, v := range su.Update.Data {
			r.mean[j] = r.mean[j]*(1-f) + v*f
		}
	}
	return nil
}

// Submit verifies a signed update and, if it passes every check, adds it to
// the current round.
func (s *SecureAggregator) Submit(ctx context.Context, su *SignedUpdate) error {
	return s.SubmitBatch(ctx, []*SignedUpdate{su})
}

// SubmitBatch verifies every update and adds all of them to the current
// round atomically: if any update fails a check, none is accepted and no
// replay state changes.
func (s *SecureAggregator) SubmitBatch(ctx context.Context, sus []*SignedUpdate) error {
	if s == nil {
		return ErrVerificationNotConfigured
	}
	if len(sus) == 0 {
		return ErrNoUpdates
	}
	if len(sus) > MaxUpdates {
		return fmt.Errorf("%w: %d > %d", ErrTooManyUpdates, len(sus), MaxUpdates)
	}
	seen := make(map[string]struct{}, len(sus))
	keys := make([]ed25519.PublicKey, len(sus))
	for i, su := range sus {
		if err := su.Validate(); err != nil {
			return fmt.Errorf("update %d: %w", i, err)
		}
		if _, dup := seen[su.Update.Owner]; dup {
			return fmt.Errorf("update %d: %w: duplicate owner %q in batch", i, ErrAlreadyContributed, su.Update.Owner)
		}
		seen[su.Update.Owner] = struct{}{}
		if i > 0 && (su.Update.Rows != sus[0].Update.Rows || su.Update.Cols != sus[0].Update.Cols) {
			return fmt.Errorf("update %d: %w", i, ErrDimensionMismatch)
		}
		pub, err := s.resolve(su.Update.Owner)
		if err != nil {
			return fmt.Errorf("update %d: %w", i, err)
		}
		keys[i] = pub
	}

	// Cheap pre-check before hashing and signature verification.
	if err := s.checkAll(sus); err != nil {
		return err
	}
	for i, su := range sus {
		if ctx != nil {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		if err := s.verifySignature(su, keys[i]); err != nil {
			return fmt.Errorf("update %d: %w", i, err)
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	for i, su := range sus {
		if err := s.checkLocked(su, now); err != nil {
			return fmt.Errorf("update %d: %w", i, err)
		}
	}
	if s.round != nil && s.maxContributors-len(s.round.order) < len(sus) {
		return ErrRoundFull
	}
	return s.commitLocked(sus, now)
}

func (s *SecureAggregator) checkAll(sus []*SignedUpdate) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	for i, su := range sus {
		if err := s.checkLocked(su, now); err != nil {
			return fmt.Errorf("update %d: %w", i, err)
		}
	}
	return nil
}

// AggregateRound finalizes the current round: it returns the average of the
// accepted contributions and closes the round. It fails with ErrNoUpdates
// (leaving the round open) when nothing was contributed.
func (s *SecureAggregator) AggregateRound(ctx context.Context) (*RoundResult, error) {
	if s == nil {
		return nil, ErrVerificationNotConfigured
	}
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.round
	if r == nil {
		return nil, ErrNoOpenRound
	}
	if len(r.order) == 0 {
		return nil, fmt.Errorf("%w: round %q has no contributions", ErrNoUpdates, r.id)
	}
	for j, v := range r.mean {
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return nil, fmt.Errorf("%w: non-finite aggregate at index %d", ErrInvalidMatrix, j)
		}
	}
	if err := s.persistRoundsLocked(nil); err != nil {
		return nil, err
	}
	s.round = nil
	contributors := append([]string(nil), r.order...)
	sort.Strings(contributors)
	return &RoundResult{
		RoundID:      r.id,
		Aggregate:    &LoRAMatrix{Rows: r.rows, Cols: r.cols, Data: r.mean},
		Contributors: contributors,
	}, nil
}
