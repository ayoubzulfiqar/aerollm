// Package autoscale contains the infrastructure meta-agent loop that reacts to
// mesh compute deficits by provisioning GPU nodes.
//
// IMPORTANT: the AWS and GCP provisioners in this package are in-memory
// SIMULATORS. They do not call any cloud API and never create, bill or
// terminate real resources; every Node they return has Simulated=true and no
// public IP. A real provisioner must implement InfraProvisioner separately.
package autoscale

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"time"
)

// Limits and defaults.
const (
	// MaxDeficit is the largest compute deficit Evaluate accepts (10.0 = 1000%).
	MaxDeficit = 10.0
	// DefaultThreshold is the deficit at or above which Evaluate provisions.
	DefaultThreshold = 0.2
	// DefaultMaxNodes is the default cap on live nodes per simulated
	// provisioner and per MetaAgentInfraLoop.
	DefaultMaxNodes = 10
	// DefaultProvisionCooldown is the default minimum time between two
	// provisioning attempts made by a MetaAgentInfraLoop.
	DefaultProvisionCooldown = 5 * time.Minute
	// MaxGPUCount is the largest GPU count accepted in a NodeSpec.
	MaxGPUCount   = 16
	maxNodesLimit = 1000
)

var (
	// ErrInvalidDeficit is returned for NaN/Inf/negative deficits or deficits
	// above MaxDeficit. HTTP callers should map it to 400.
	ErrInvalidDeficit = errors.New("autoscale: invalid deficit")
	// ErrCooldown is returned when a provision is requested before the
	// cooldown elapsed (or while another provision is in flight). HTTP callers
	// should map it to 429.
	ErrCooldown = errors.New("autoscale: provisioning cooldown in effect")
	// ErrMaxNodes is returned when the node limit has been reached.
	ErrMaxNodes = errors.New("autoscale: maximum node count reached")
	// ErrInvalidSpec is returned for malformed NodeSpecs.
	ErrInvalidSpec = errors.New("autoscale: invalid node spec")
	// ErrNodeNotFound is returned when terminating an unknown node.
	ErrNodeNotFound = errors.New("autoscale: node not found")
)

// NodeSpec describes the desired infrastructure node.
type NodeSpec struct {
	Provider     string
	InstanceType string
	GPUCount     int
	Region       string
	SSHKey       string
}

// Node represents a provisioned infrastructure node.
type Node struct {
	ID           string
	Provider     string
	PublicIP     string
	InstanceType string
	Region       string
	GPUCount     int
	LaunchedAt   int64
	// Simulated is true when the node only exists in an in-memory simulator
	// and does not correspond to any real cloud resource.
	Simulated bool `json:"simulated"`
}

// InfraProvisioner defines the contract for cloud providers.
type InfraProvisioner interface {
	ProvisionGPU(ctx context.Context, spec NodeSpec) (*Node, error)
	Terminate(ctx context.Context, nodeID string) error
	List(ctx context.Context) ([]Node, error)
}

func isIdentChar(r rune, extra string) bool {
	return (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || strings.ContainsRune(extra, r)
}

func validIdent(s string, maxLen int, extra string) bool {
	if s == "" || len(s) > maxLen {
		return false
	}
	for _, r := range s {
		if !isIdentChar(r, extra) {
			return false
		}
	}
	return true
}

// normalizeSpec validates spec and applies defaults (GPUCount 0 -> 1).
func normalizeSpec(spec NodeSpec) (NodeSpec, error) {
	if spec.GPUCount == 0 {
		spec.GPUCount = 1
	}
	if spec.GPUCount < 1 || spec.GPUCount > MaxGPUCount {
		return spec, fmt.Errorf("%w: gpu count must be in [1, %d]", ErrInvalidSpec, MaxGPUCount)
	}
	if !validIdent(spec.InstanceType, 64, "._-") {
		return spec, fmt.Errorf("%w: instance type must be 1-64 chars of [A-Za-z0-9._-]", ErrInvalidSpec)
	}
	if spec.Region != "" && !validIdent(spec.Region, 32, "-") {
		return spec, fmt.Errorf("%w: region must be at most 32 chars of [A-Za-z0-9-]", ErrInvalidSpec)
	}
	if len(spec.SSHKey) > 8192 || strings.ContainsAny(spec.SSHKey, "\r\n\x00") {
		return spec, fmt.Errorf("%w: ssh key too long or contains control characters", ErrInvalidSpec)
	}
	return spec, nil
}

func ctxErr(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	return ctx.Err()
}

func randSuffix() string {
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "0000"
	}
	return hex.EncodeToString(b[:])
}

// nodeSimulator is the shared in-memory implementation behind the simulated
// cloud provisioners. The zero value is ready to use.
type nodeSimulator struct {
	mu       sync.Mutex
	nodes    map[string]Node
	seq      uint64
	maxNodes int
}

func (s *nodeSimulator) provision(ctx context.Context, provider string, spec NodeSpec) (*Node, error) {
	if err := ctxErr(ctx); err != nil {
		return nil, err
	}
	spec, err := normalizeSpec(spec)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	limit := s.maxNodes
	if limit <= 0 {
		limit = DefaultMaxNodes
	}
	if len(s.nodes) >= limit {
		return nil, fmt.Errorf("%w: %s simulator holds %d nodes", ErrMaxNodes, provider, len(s.nodes))
	}
	if s.nodes == nil {
		s.nodes = make(map[string]Node)
	}
	s.seq++
	n := Node{
		ID:           fmt.Sprintf("%s-sim-%s-%d-%s", provider, spec.InstanceType, s.seq, randSuffix()),
		Provider:     provider,
		InstanceType: spec.InstanceType,
		Region:       spec.Region,
		GPUCount:     spec.GPUCount,
		LaunchedAt:   timeNow(),
		Simulated:    true,
	}
	s.nodes[n.ID] = n
	out := n
	return &out, nil
}

func (s *nodeSimulator) terminate(ctx context.Context, provider, nodeID string) error {
	if err := ctxErr(ctx); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.nodes[nodeID]; !ok {
		return fmt.Errorf("%w: %s node %q", ErrNodeNotFound, provider, nodeID)
	}
	delete(s.nodes, nodeID)
	return nil
}

func (s *nodeSimulator) list(ctx context.Context) ([]Node, error) {
	if err := ctxErr(ctx); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Node, 0, len(s.nodes))
	for _, n := range s.nodes {
		out = append(out, n)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

func (s *nodeSimulator) setMaxNodes(n int) error {
	if n < 1 || n > maxNodesLimit {
		return fmt.Errorf("autoscale: max nodes must be in [1, %d]", maxNodesLimit)
	}
	s.mu.Lock()
	s.maxNodes = n
	s.mu.Unlock()
	return nil
}

// AWSProvisioner is an in-memory SIMULATOR of an AWS GPU provisioner. It does
// not call AWS: nodes exist only in process memory (Simulated=true, no
// public IP). It is safe for concurrent use.
type AWSProvisioner struct {
	sim nodeSimulator
}

// NewAWSProvisioner creates a new simulated AWS provisioner.
func NewAWSProvisioner() *AWSProvisioner {
	return &AWSProvisioner{}
}

// ProvisionGPU records a simulated GPU node.
func (p *AWSProvisioner) ProvisionGPU(ctx context.Context, spec NodeSpec) (*Node, error) {
	if p == nil {
		return nil, fmt.Errorf("autoscale: aws provisioner is nil")
	}
	return p.sim.provision(ctx, "aws", spec)
}

// Terminate removes a simulated node; unknown IDs return ErrNodeNotFound.
func (p *AWSProvisioner) Terminate(ctx context.Context, nodeID string) error {
	if p == nil {
		return fmt.Errorf("autoscale: aws provisioner is nil")
	}
	return p.sim.terminate(ctx, "aws", nodeID)
}

// List returns the live simulated nodes, sorted by ID.
func (p *AWSProvisioner) List(ctx context.Context) ([]Node, error) {
	if p == nil {
		return nil, fmt.Errorf("autoscale: aws provisioner is nil")
	}
	return p.sim.list(ctx)
}

// SetMaxNodes changes the live-node cap (default DefaultMaxNodes).
func (p *AWSProvisioner) SetMaxNodes(n int) error {
	if p == nil {
		return fmt.Errorf("autoscale: aws provisioner is nil")
	}
	return p.sim.setMaxNodes(n)
}

// GCPProvisioner is an in-memory SIMULATOR of a GCP GPU provisioner. It does
// not call GCP: nodes exist only in process memory (Simulated=true, no
// public IP). It is safe for concurrent use.
type GCPProvisioner struct {
	sim nodeSimulator
}

// NewGCPProvisioner creates a new simulated GCP provisioner.
func NewGCPProvisioner() *GCPProvisioner {
	return &GCPProvisioner{}
}

// ProvisionGPU records a simulated GPU node.
func (p *GCPProvisioner) ProvisionGPU(ctx context.Context, spec NodeSpec) (*Node, error) {
	if p == nil {
		return nil, fmt.Errorf("autoscale: gcp provisioner is nil")
	}
	return p.sim.provision(ctx, "gcp", spec)
}

// Terminate removes a simulated node; unknown IDs return ErrNodeNotFound.
func (p *GCPProvisioner) Terminate(ctx context.Context, nodeID string) error {
	if p == nil {
		return fmt.Errorf("autoscale: gcp provisioner is nil")
	}
	return p.sim.terminate(ctx, "gcp", nodeID)
}

// List returns the live simulated nodes, sorted by ID.
func (p *GCPProvisioner) List(ctx context.Context) ([]Node, error) {
	if p == nil {
		return nil, fmt.Errorf("autoscale: gcp provisioner is nil")
	}
	return p.sim.list(ctx)
}

// SetMaxNodes changes the live-node cap (default DefaultMaxNodes).
func (p *GCPProvisioner) SetMaxNodes(n int) error {
	if p == nil {
		return fmt.Errorf("autoscale: gcp provisioner is nil")
	}
	return p.sim.setMaxNodes(n)
}

// MetaAgentInfraLoop polls mesh compute deficit and triggers provisioning.
// It is safe for concurrent use: at most one provision is in flight, and
// provisions are separated by a cooldown and bounded by a node limit.
type MetaAgentInfraLoop struct {
	mu            sync.Mutex
	provisioner   InfraProvisioner
	threshold     float64
	cooldown      time.Duration
	maxNodes      int
	spec          NodeSpec
	lastProvision time.Time
	inFlight      bool
	now           func() time.Time
}

func defaultLoopSpec() NodeSpec {
	return NodeSpec{InstanceType: "A100", GPUCount: 1, Region: "us-east-1"}
}

// NewMetaAgentInfraLoop creates a new loop. A non-positive or non-finite
// threshold falls back to DefaultThreshold.
func NewMetaAgentInfraLoop(provisioner InfraProvisioner, threshold float64) *MetaAgentInfraLoop {
	if !(threshold > 0) || math.IsInf(threshold, 0) || threshold > MaxDeficit {
		threshold = DefaultThreshold
	}
	return &MetaAgentInfraLoop{
		provisioner: provisioner,
		threshold:   threshold,
		cooldown:    DefaultProvisionCooldown,
		maxNodes:    DefaultMaxNodes,
		spec:        defaultLoopSpec(),
		now:         time.Now,
	}
}

// SetCooldown sets the minimum time between provisions (0 disables it).
func (l *MetaAgentInfraLoop) SetCooldown(d time.Duration) error {
	if l == nil {
		return fmt.Errorf("autoscale: loop is nil")
	}
	if d < 0 {
		return fmt.Errorf("autoscale: cooldown must be >= 0")
	}
	l.mu.Lock()
	l.cooldown = d
	l.mu.Unlock()
	return nil
}

// SetMaxNodes sets the maximum number of live nodes (as reported by the
// provisioner's List) above which Evaluate refuses to provision.
func (l *MetaAgentInfraLoop) SetMaxNodes(n int) error {
	if l == nil {
		return fmt.Errorf("autoscale: loop is nil")
	}
	if n < 1 || n > maxNodesLimit {
		return fmt.Errorf("autoscale: max nodes must be in [1, %d]", maxNodesLimit)
	}
	l.mu.Lock()
	l.maxNodes = n
	l.mu.Unlock()
	return nil
}

// Evaluate checks the deficit and provisions at most one node if needed.
//
// It returns (nil, nil) when the deficit is below the threshold,
// ErrInvalidDeficit for NaN/Inf/negative deficits or deficits above
// MaxDeficit, ErrCooldown when a provision happened within the cooldown or is
// in flight, and ErrMaxNodes when the node limit is reached. The node limit is
// enforced against the provisioner's List; a List error fails closed.
func (l *MetaAgentInfraLoop) Evaluate(ctx context.Context, deficit float64) (*Node, error) {
	if l == nil {
		return nil, fmt.Errorf("autoscale: loop is nil")
	}
	if math.IsNaN(deficit) || math.IsInf(deficit, 0) || deficit < 0 || deficit > MaxDeficit {
		return nil, fmt.Errorf("%w: must be a finite number in [0, %g]", ErrInvalidDeficit, MaxDeficit)
	}
	if err := ctxErr(ctx); err != nil {
		return nil, err
	}

	l.mu.Lock()
	if deficit < l.threshold {
		l.mu.Unlock()
		return nil, nil
	}
	if l.provisioner == nil {
		l.mu.Unlock()
		return nil, fmt.Errorf("no provisioner configured")
	}
	nowFn := l.now
	if nowFn == nil {
		nowFn = time.Now
	}
	now := nowFn()
	if l.inFlight || (!l.lastProvision.IsZero() && l.cooldown > 0 && now.Sub(l.lastProvision) < l.cooldown) {
		l.mu.Unlock()
		return nil, ErrCooldown
	}
	// Reserve the slot so concurrent callers observe the cooldown.
	l.inFlight = true
	prevProvision := l.lastProvision
	l.lastProvision = now
	prov, maxNodes := l.provisioner, l.maxNodes
	spec := l.spec
	if spec.InstanceType == "" {
		spec = defaultLoopSpec()
	}
	if maxNodes <= 0 {
		maxNodes = DefaultMaxNodes
	}
	l.mu.Unlock()

	node, err := func() (*Node, error) {
		existing, err := prov.List(ctx)
		if err != nil {
			return nil, fmt.Errorf("autoscale: listing nodes: %w", err)
		}
		if len(existing) >= maxNodes {
			return nil, fmt.Errorf("%w: %d live nodes (limit %d)", ErrMaxNodes, len(existing), maxNodes)
		}
		n, err := prov.ProvisionGPU(ctx, spec)
		if err == nil && n == nil {
			err = fmt.Errorf("autoscale: provisioner returned no node")
		}
		return n, err
	}()

	l.mu.Lock()
	l.inFlight = false
	if err != nil {
		// A failed attempt must not consume the cooldown window.
		l.lastProvision = prevProvision
	}
	l.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return node, nil
}

// NewServerMetaAgentLoop creates a loop with the (simulated) AWS provisioner as
// primary and the (simulated) GCP provisioner as fallback.
func NewServerMetaAgentLoop() *MetaAgentInfraLoop {
	return NewServerMetaAgentLoopWith(NewAWSProvisioner(), NewGCPProvisioner(), DefaultThreshold)
}

// NewServerMetaAgentLoopWith creates a loop with explicit provisioners.
func NewServerMetaAgentLoopWith(primary, fallback InfraProvisioner, threshold float64) *MetaAgentInfraLoop {
	return NewMetaAgentInfraLoop(&failoverProvisioner{primary: primary, fallback: fallback}, threshold)
}

// failoverProvisioner tries primary then fallback, remembering which
// provisioner owns each node so Terminate is routed correctly.
type failoverProvisioner struct {
	primary  InfraProvisioner
	fallback InfraProvisioner

	mu     sync.Mutex
	owners map[string]InfraProvisioner
}

func (f *failoverProvisioner) record(id string, p InfraProvisioner) {
	if id == "" {
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.owners == nil {
		f.owners = make(map[string]InfraProvisioner)
	}
	f.owners[id] = p
}

func (f *failoverProvisioner) ProvisionGPU(ctx context.Context, spec NodeSpec) (*Node, error) {
	var primaryErr error
	if f.primary != nil {
		n, err := f.primary.ProvisionGPU(ctx, spec)
		if err == nil && n != nil {
			f.record(n.ID, f.primary)
			return n, nil
		}
		if err == nil {
			err = fmt.Errorf("primary returned no node")
		}
		primaryErr = err
	}
	if err := ctxErr(ctx); err != nil {
		return nil, err
	}
	if f.fallback != nil {
		n, err := f.fallback.ProvisionGPU(ctx, spec)
		if err == nil && n != nil {
			f.record(n.ID, f.fallback)
			return n, nil
		}
		if err == nil {
			err = fmt.Errorf("fallback returned no node")
		}
		return nil, errors.Join(primaryErr, err)
	}
	if primaryErr != nil {
		return nil, primaryErr
	}
	return nil, fmt.Errorf("no provisioner available")
}

func (f *failoverProvisioner) Terminate(ctx context.Context, nodeID string) error {
	f.mu.Lock()
	owner, ok := f.owners[nodeID]
	f.mu.Unlock()
	if !ok || owner == nil {
		return fmt.Errorf("%w: %q", ErrNodeNotFound, nodeID)
	}
	if err := owner.Terminate(ctx, nodeID); err != nil {
		return err
	}
	f.mu.Lock()
	delete(f.owners, nodeID)
	f.mu.Unlock()
	return nil
}

func (f *failoverProvisioner) List(ctx context.Context) ([]Node, error) {
	var out []Node
	var errs []error
	seen := make(map[string]struct{})
	for _, p := range []InfraProvisioner{f.primary, f.fallback} {
		if p == nil {
			continue
		}
		nodes, err := p.List(ctx)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		for _, n := range nodes {
			if n.ID != "" {
				if _, dup := seen[n.ID]; dup {
					continue
				}
				seen[n.ID] = struct{}{}
			}
			out = append(out, n)
		}
	}
	return out, errors.Join(errs...)
}

func timeNow() int64 {
	return time.Now().Unix()
}
