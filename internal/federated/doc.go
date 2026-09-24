// Package federated implements federated aggregation of LoRA updates for
// AeroLLM edge nodes: node registration, authenticated and replay-protected
// update submission, and FedAvg aggregation.
//
// # Node registration
//
// A node is identified by a NodeID and pins an ed25519 public key on first
// registration (GatewayRegistry). Network registrations go through
// RegisterNodeHandlerWithAuth, which requires one of:
//
//   - Registration token: the node sends a shared secret in the
//     X-AeroLLM-Federation-Token header. The gateway stores only SHA-256
//     digests of the configured tokens and compares in constant time.
//     Suitable for trusted provisioning pipelines.
//   - Operator attestation: an operator holding a pre-trusted ed25519 key
//     signs RegistrationPayload(reg, issued_at, expires_at) offline (see
//     SignRegistration) and hands the signature to the node, which submits
//     it as operator_signature with issued_at/expires_at. The attestation
//     binds node_id, endpoint, public key and algorithms; its lifetime is
//     capped (DefaultMaxAttestationValidity) and deregistering a node
//     revokes every attestation issued before the deregistration.
//
// Configure the credentials with NewRegistrationAuth. With neither
// configured the handler fails closed (503). The legacy RegisterNodeHandler
// performs no authentication and must only be mounted behind admin auth.
//
// # Rounds and signed updates
//
//  1. An administrator opens a round: SecureAggregator.OpenRound(id)
//     (HTTP: RoundHandler, POST {"round_id": "..."}). Round IDs cannot be
//     reused.
//  2. Each node builds a SignedUpdate bound to the round ID, a per-node
//     sequence number strictly greater than any it used before (a persisted
//     counter or its clock in nanoseconds) and the current time, signs
//     SignedUpdatePayload with its registered key (NewSignedUpdate) and
//     submits it (HTTP: SubmitUpdateHandler).
//  3. The aggregator checks, in order: well-formedness, that the owner is
//     registered with a key, the round is the open one, the sequence is
//     fresh, the timestamp is within MaxClockSkew of the gateway clock and
//     not earlier than the round opening, the node has not contributed to
//     the round yet, the shape matches; then it verifies the ed25519
//     signature (over a language-neutral digest, see DataDigest) and
//     re-checks everything under its lock before committing, so concurrent
//     duplicates cannot both be accepted.
//  4. The administrator finalizes the round: AggregateRound (HTTP:
//     AggregateRoundHandler) returns the average of the accepted updates
//     and closes the round. SignedAggregateHandler combines an atomic batch
//     submission with finalization.
//
// Replayed updates fail on the sequence check, updates for other rounds on
// the round check, and captured-then-delayed updates on the timestamp
// window. Verify, AggregateVerified and AggregateHandler predate this scheme
// and provide no replay protection; use them only for offline checks.
//
// # Persistence
//
// Both GatewayRegistry and SecureAggregator keep state in memory by default.
// EnablePersistence(persist.Store) loads prior state and writes every change
// through before applying it (buckets federated_nodes, federated_tombstones,
// federated_replay, federated_rounds). Replay state that cannot be persisted
// causes the update to be rejected, and undecodable replay state blocks the
// affected node until ResetNode, so a restart never re-opens a replay window.
package federated
