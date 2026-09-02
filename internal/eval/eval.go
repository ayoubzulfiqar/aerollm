package eval

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"sort"
	"time"
)

// ScoreRecord is a stored evaluation score.
type ScoreRecord struct {
	ID          string
	Prompt      string
	Model       string
	Provider    string
	Score       float64
	Rubric      string
	PromptVersion string
	RecordedAt  int64
}

// ScoreStore persists scores.
type ScoreStore interface {
	AppendScore(ctx context.Context, record ScoreRecord) error
	ListScores(ctx context.Context, filter ScoreFilter) ([]ScoreRecord, error)
}

// ScoreFilter selects score subsets.
type ScoreFilter struct {
	Model         string
	Provider      string
	PromptVersion string
	Limit         int
}

// JudgeClient sends scoring requests to a judge model.
type JudgeClient interface {
	ChatCompletion(ctx context.Context, prompt string) (string, error)
}

// JudgePipeline scores completed requests from the ledger using a judge model.
type JudgePipeline struct {
	client JudgeClient
	store  ScoreStore
	rubric string
}

// NewJudgePipeline creates a new pipeline.
func NewJudgePipeline(client JudgeClient, store ScoreStore, rubric string) *JudgePipeline {
	return &JudgePipeline{client: client, store: store, rubric: rubric}
}

// ScoreRequest evaluates a single ledger response.
func (p *JudgePipeline) ScoreRequest(ctx context.Context, prompt, response, model, provider, promptVersion string) (float64, error) {
	judgePrompt := fmt.Sprintf("RUBRIC: %s\nPROMPT: %s\nRESPONSE: %s\nScore 0-100:", p.rubric, prompt, response)
	text, err := p.client.ChatCompletion(ctx, judgePrompt)
	if err != nil {
		return 0, err
	}
	score := parseScore(text)
	record := ScoreRecord{
		ID:           fmt.Sprintf("eval_%d", timeNow()),
		Prompt:       prompt,
		Model:        model,
		Provider:     provider,
		Score:        score,
		Rubric:       p.rubric,
		PromptVersion: promptVersion,
		RecordedAt:   time.Now().UnixNano(),
	}
	if err := p.store.AppendScore(ctx, record); err != nil {
		return score, err
	}
	return score, nil
}

// RegressionDetector detects prompt-version score regressions.
type RegressionDetector struct {
	store ScoreStore
}

// NewRegressionDetector creates a new detector.
func NewRegressionDetector(store ScoreStore) *RegressionDetector {
	return &RegressionDetector{store: store}
}

// Detect finds prompt versions whose average score dropped >10% versus previous version.
func (d *RegressionDetector) Detect(ctx context.Context, filter ScoreFilter) ([]Regression, error) {
	records, err := d.store.ListScores(ctx, filter)
	if err != nil {
		return nil, err
	}
	byVersion := groupByVersion(records)
	var regressions []Regression
	versions := sortedKeys(byVersion)
	for i := 1; i < len(versions); i++ {
		prev := avgScore(byVersion[versions[i-1]])
		curr := avgScore(byVersion[versions[i]])
		if prev > 0 && curr < prev*0.9 {
			regressions = append(regressions, Regression{
				PromptVersion:    versions[i],
				PreviousVersion:  versions[i-1],
				PreviousAvg:      prev,
				CurrentAvg:       curr,
				DropPercent:      (prev - curr) / prev * 100,
			})
		}
	}
	return regressions, nil
}

// Regression describes a detected quality regression.
type Regression struct {
	PromptVersion   string
	PreviousVersion string
	PreviousAvg     float64
	CurrentAvg      float64
	DropPercent     float64
}

// BenchmarkRunner runs JSONL datasets through a model and returns aggregate scores.
type BenchmarkRunner struct {
	client JudgeClient
	store  ScoreStore
}

// NewBenchmarkRunner creates a new runner.
func NewBenchmarkRunner(client JudgeClient, store ScoreStore) *BenchmarkRunner {
	return &BenchmarkRunner{client: client, store: store}
}

// Run executes a JSONL benchmark dataset.
type RunResult struct {
	Total      int
	AvgScore   float64
	MinScore   float64
	MaxScore   float64
	ScoresByModel map[string]float64
}

// Run consumes JSONL prompts and scores them.
func (r *BenchmarkRunner) Run(ctx context.Context, dataset io.Reader, model, provider, rubric string) (RunResult, error) {
	dec := json.NewDecoder(dataset)
	var sum float64
	var min float64 = math.MaxFloat64
	var max float64
	count := 0
	byModel := map[string]float64{}
	byModelCount := map[string]int{}
	for dec.More() {
		var line struct {
			Prompt string `json:"prompt"`
		}
		if err := dec.Decode(&line); err != nil {
			if err == io.EOF {
				break
			}
			continue
		}
		score, err := r.client.ChatCompletion(ctx, fmt.Sprintf("RUBRIC: %s\nPROMPT: %s\nScore 0-100:", rubric, line.Prompt))
		if err != nil {
			continue
		}
		val := parseScore(score)
		sum += val
		if val < min {
			min = val
		}
		if val > max {
			max = val
		}
		byModel[model] += val
		byModelCount[model]++
		count++
		_ = provider
	}
	if count == 0 {
		return RunResult{}, fmt.Errorf("no benchmark items scored")
	}
	out := RunResult{Total: count, AvgScore: sum / float64(count), MinScore: min, MaxScore: max, ScoresByModel: map[string]float64{}}
	for m, s := range byModel {
		if byModelCount[m] > 0 {
			out.ScoresByModel[m] = s / float64(byModelCount[m])
		}
	}
	return out, nil
}

func groupByVersion(records []ScoreRecord) map[string][]ScoreRecord {
	out := map[string][]ScoreRecord{}
	for _, r := range records {
		out[r.PromptVersion] = append(out[r.PromptVersion], r)
	}
	return out
}

func sortedKeys(m map[string][]ScoreRecord) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func avgScore(records []ScoreRecord) float64 {
	if len(records) == 0 {
		return 0
	}
	var sum float64
	for _, r := range records {
		sum += r.Score
	}
	return sum / float64(len(records))
}

func parseScore(text string) float64 {
	var v float64
	if _, err := fmt.Sscanf(text, "%f", &v); err == nil {
		return v
	}
	return 0
}

func timeNow() int64 {
	return 0
}
