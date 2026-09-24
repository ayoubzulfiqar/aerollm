package rsi

import (
	"context"
	"fmt"
	"sort"
)

// ---------------------------------------------------------------------------
// ModularRSI: Benchmark-Disjoint Evaluation
// ---------------------------------------------------------------------------

// EvaluationPartition represents a single train/test split for k-fold evaluation.
// TrainScenarios and TestScenarios are guaranteed to be disjoint (no overlap).
type EvaluationPartition struct {
	TrainScenarios []*DreamReplay
	TestScenarios  []*DreamReplay
}

// DisjointMetrics holds averaged train/test scores and the generalization gap.
// The detailed per-partition metrics are stored in TrainEval and TestEval as
// averaged SimulatedMetrics values.
type DisjointMetrics struct {
	TrainScore        float64
	TestScore         float64
	GeneralizationGap float64
	TrainEval         *SimulatedMetrics
	TestEval          *SimulatedMetrics
}

// ModularEvaluator performs k-fold disjoint evaluation using a DreamSimulator
// for offline policy evaluation.
type ModularEvaluator struct {
	sim *DreamSimulator
}

// NewModularEvaluator creates an evaluator backed by the given simulator.
func NewModularEvaluator(sim *DreamSimulator) *ModularEvaluator {
	return &ModularEvaluator{sim: sim}
}

// CreatePartitions splits scenarios into kFold disjoint train/test partitions
// using interleaving after a diversity sort to ensure each fold contains a
// balanced mix of scenarios.
func (m *ModularEvaluator) CreatePartitions(scenarios []*DreamReplay, kFolds int) []*EvaluationPartition {
	if len(scenarios) == 0 {
		return nil
	}
	if kFolds <= 1 {
		// k<=0 selects, and k==1 (a "fold" with no training data) degrades
		// to, a single partition with an 80/20 train/test split.
		split := len(scenarios) * 4 / 5 // 80% train
		if split == 0 {
			split = 1
		}
		return []*EvaluationPartition{{
			TrainScenarios: scenarios[:split],
			TestScenarios:  scenarios[split:],
		}}
	}
	if kFolds > len(scenarios) {
		kFolds = len(scenarios)
	}

	// Create a copy and sort by content length for diversity.
	sorted := make([]*DreamReplay, len(scenarios))
	copy(sorted, scenarios)
	sort.SliceStable(sorted, func(i, j int) bool {
		return scenarioContentLength(sorted[i]) < scenarioContentLength(sorted[j])
	})

	// Interleave: distribute across folds in round-robin from the sorted list.
	// This ensures each fold gets a mix of short and long scenarios.
	folds := make([][]*DreamReplay, kFolds)
	for i, s := range sorted {
		folds[i%kFolds] = append(folds[i%kFolds], s)
	}

	partitions := make([]*EvaluationPartition, kFolds)
	for i := 0; i < kFolds; i++ {
		testFold := folds[i]
		var trainFold []*DreamReplay
		for j := 0; j < kFolds; j++ {
			if j != i {
				trainFold = append(trainFold, folds[j]...)
			}
		}
		partitions[i] = &EvaluationPartition{
			TrainScenarios: trainFold,
			TestScenarios:  testFold,
		}
	}

	return partitions
}

// EvaluateDisjoint evaluates a policy across all partitions, returning
// averaged train/test scores and the generalization gap.
func (m *ModularEvaluator) EvaluateDisjoint(ctx context.Context, policy Policy, partitions []*EvaluationPartition) (*DisjointMetrics, error) {
	if m == nil || m.sim == nil {
		return nil, fmt.Errorf("modular evaluator: simulator is required")
	}
	if policy == nil {
		return nil, fmt.Errorf("modular evaluator: policy is nil")
	}
	if len(partitions) == 0 {
		return nil, fmt.Errorf("modular evaluator: no partitions provided")
	}

	var trainScores, testScores []float64
	var trainAccum, testAccum *SimulatedMetrics

	for _, part := range partitions {
		if part == nil {
			continue
		}
		// Evaluate on train scenarios.
		if len(part.TrainScenarios) > 0 {
			trainM, err := m.sim.ReplayTraffic(ctx, policy, part.TrainScenarios)
			if err != nil {
				return nil, fmt.Errorf("evaluating on train scenarios: %w", err)
			}
			trainScores = append(trainScores, scoreMetrics(trainM))
			if trainAccum == nil {
				trainAccum = &SimulatedMetrics{}
			}
			accumulateSimulated(trainAccum, trainM)
		}

		// Evaluate on test scenarios.
		if len(part.TestScenarios) > 0 {
			testM, err := m.sim.ReplayTraffic(ctx, policy, part.TestScenarios)
			if err != nil {
				return nil, fmt.Errorf("evaluating on test scenarios: %w", err)
			}
			testScores = append(testScores, scoreMetrics(testM))
			if testAccum == nil {
				testAccum = &SimulatedMetrics{}
			}
			accumulateSimulated(testAccum, testM)
		}
	}

	trainScore := averageScores(trainScores)
	testScore := averageScores(testScores)

	return &DisjointMetrics{
		TrainScore:        trainScore,
		TestScore:         testScore,
		GeneralizationGap: trainScore - testScore,
		TrainEval:         finalizeSimulated(trainAccum, len(trainScores)),
		TestEval:          finalizeSimulated(testAccum, len(testScores)),
	}, nil
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func scenarioContentLength(s *DreamReplay) int {
	if s == nil {
		return 0
	}
	return requestContentLength(s.Request)
}

func accumulateSimulated(dst, src *SimulatedMetrics) {
	if dst == nil || src == nil {
		return
	}
	dst.AvgLatency += src.AvgLatency
	dst.P99Latency += src.P99Latency
	dst.Cost += src.Cost
	dst.ErrorRate += src.ErrorRate
	dst.CacheHitRate += src.CacheHitRate
	dst.Requests += src.Requests
}

func finalizeSimulated(m *SimulatedMetrics, count int) *SimulatedMetrics {
	if m == nil || count == 0 {
		return &SimulatedMetrics{}
	}
	return &SimulatedMetrics{
		AvgLatency:   m.AvgLatency / float64(count),
		P99Latency:   m.P99Latency / float64(count),
		Cost:         m.Cost / float64(count),
		ErrorRate:    m.ErrorRate / float64(count),
		CacheHitRate: m.CacheHitRate / float64(count),
		// Cost and Requests are both averaged, so Cost/Requests remains the
		// per-request cost. Round to nearest to limit integer truncation.
		Requests: (m.Requests + count/2) / count,
	}
}

func averageScores(scores []float64) float64 {
	if len(scores) == 0 {
		return 0.0
	}
	total := 0.0
	for _, s := range scores {
		total += s
	}
	return total / float64(len(scores))
}
