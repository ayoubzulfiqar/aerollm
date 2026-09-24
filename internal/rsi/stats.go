package rsi

import (
	"errors"
	"fmt"
	"math"
)

// ---------------------------------------------------------------------------
// Statistical comparison of candidate vs baseline policies
// ---------------------------------------------------------------------------
//
// The RSI deploy gate must not fire on noise. A candidate is only deployed
// when, on held-out scenarios it was NOT optimised on, it beats the baseline
// by the configured margin AND a one-sided paired t-test rejects "candidate
// is no better than baseline" at the configured significance level. Both
// policies are replayed against the same scenarios, so the paired test on
// per-scenario score differences is the appropriate (and most powerful)
// choice.

// ErrComparisonInput is returned by ComparePaired for unusable input.
var ErrComparisonInput = errors.New("rsi: invalid comparison input")

// Comparison summarizes a paired comparison of per-scenario scores between
// a baseline policy and a candidate policy evaluated on the same scenarios.
// All fields are always finite so the struct is safe to JSON-encode.
type Comparison struct {
	// Samples is the number of paired observations.
	Samples int `json:"samples"`
	// BaselineMean and CandidateMean are the mean per-scenario scores.
	BaselineMean  float64 `json:"baseline_mean"`
	CandidateMean float64 `json:"candidate_mean"`
	// MeanDiff is mean(candidate - baseline); StdDevDiff its sample stddev.
	MeanDiff   float64 `json:"mean_diff"`
	StdDevDiff float64 `json:"stddev_diff"`
	// TStat is the paired t statistic. It is 0 when the differences have
	// zero variance (the p-value is then decided by the sign of MeanDiff).
	TStat float64 `json:"t_stat"`
	// PValue is the one-sided p-value for H1: candidate > baseline.
	PValue float64 `json:"p_value"`
	// ImprovementPct is the relative improvement of the candidate mean over
	// the baseline mean (see relativeImprovementPct).
	ImprovementPct float64 `json:"improvement_pct"`
}

// ComparePaired runs a one-sided paired t-test of candidate against baseline.
// Both slices must be non-empty, of equal length and contain only finite
// values. With a single sample no variance estimate exists, so PValue is 1
// (the improvement cannot be established).
func ComparePaired(baseline, candidate []float64) (Comparison, error) {
	if len(baseline) == 0 || len(baseline) != len(candidate) {
		return Comparison{}, fmt.Errorf("%w: need equal, non-empty samples (got %d and %d)",
			ErrComparisonInput, len(baseline), len(candidate))
	}
	n := len(baseline)
	var sumB, sumC, sumD float64
	for i := 0; i < n; i++ {
		if !isFinite(baseline[i]) || !isFinite(candidate[i]) {
			return Comparison{}, fmt.Errorf("%w: non-finite score at index %d", ErrComparisonInput, i)
		}
		sumB += baseline[i]
		sumC += candidate[i]
		sumD += candidate[i] - baseline[i]
	}
	fn := float64(n)
	c := Comparison{
		Samples:       n,
		BaselineMean:  sumB / fn,
		CandidateMean: sumC / fn,
		MeanDiff:      sumD / fn,
		PValue:        1,
	}
	c.ImprovementPct = relativeImprovementPct(c.BaselineMean, c.CandidateMean)

	if n < 2 {
		return c, nil
	}
	var ss float64
	for i := 0; i < n; i++ {
		d := candidate[i] - baseline[i] - c.MeanDiff
		ss += d * d
	}
	sd := math.Sqrt(ss / (fn - 1))
	c.StdDevDiff = sd

	// Treat numerically-zero variance as exactly zero: every scenario moved
	// by the same amount, so the sign of the mean decides.
	if sd <= 1e-12*math.Max(1, math.Abs(c.MeanDiff)) {
		c.StdDevDiff = 0
		if c.MeanDiff > 1e-12 {
			c.PValue = 0
		}
		return c, nil
	}
	t := c.MeanDiff / (sd / math.Sqrt(fn))
	c.TStat = t
	c.PValue = studentTUpperTail(t, fn-1)
	return c, nil
}

// relativeImprovementPct returns (candidate-baseline)/|baseline|*100.
// Scores live in [0,1]; when the baseline is (near) zero a relative change is
// undefined, so the result is capped at +100 for any strict improvement and
// 0 otherwise. Non-finite inputs yield 0. The result is always finite.
func relativeImprovementPct(baseline, candidate float64) float64 {
	if !isFinite(baseline) || !isFinite(candidate) {
		return 0
	}
	const eps = 1e-9
	if math.Abs(baseline) <= eps {
		if candidate > baseline+eps {
			return 100
		}
		return 0
	}
	pct := (candidate - baseline) / math.Abs(baseline) * 100
	if !isFinite(pct) {
		return 0
	}
	return pct
}

// studentTUpperTail returns P(T > t) for a Student-t distribution with df
// degrees of freedom.
func studentTUpperTail(t, df float64) float64 {
	if math.IsNaN(t) || !(df > 0) {
		return 1
	}
	if math.IsInf(t, 1) {
		return 0
	}
	if math.IsInf(t, -1) {
		return 1
	}
	x := df / (df + t*t)
	twoSided := regIncBeta(df/2, 0.5, x)
	if t > 0 {
		return clamp(0.5*twoSided, 0, 1)
	}
	return clamp(1-0.5*twoSided, 0, 1)
}

// regIncBeta computes the regularized incomplete beta function I_x(a, b)
// using the continued-fraction expansion (Numerical Recipes, betai/betacf).
func regIncBeta(a, b, x float64) float64 {
	if x <= 0 {
		return 0
	}
	if x >= 1 {
		return 1
	}
	lab, _ := math.Lgamma(a + b)
	la, _ := math.Lgamma(a)
	lb, _ := math.Lgamma(b)
	front := math.Exp(lab - la - lb + a*math.Log(x) + b*math.Log1p(-x))
	if x < (a+1)/(a+b+2) {
		return front * betaContinuedFraction(a, b, x) / a
	}
	return 1 - front*betaContinuedFraction(b, a, 1-x)/b
}

func betaContinuedFraction(a, b, x float64) float64 {
	const (
		maxIter = 500
		eps     = 1e-15
		fpMin   = 1e-300
	)
	qab := a + b
	qap := a + 1
	qam := a - 1
	c := 1.0
	d := 1 - qab*x/qap
	if math.Abs(d) < fpMin {
		d = fpMin
	}
	d = 1 / d
	h := d
	for m := 1; m <= maxIter; m++ {
		fm := float64(m)
		m2 := 2 * fm
		aa := fm * (b - fm) * x / ((qam + m2) * (a + m2))
		d = 1 + aa*d
		if math.Abs(d) < fpMin {
			d = fpMin
		}
		c = 1 + aa/c
		if math.Abs(c) < fpMin {
			c = fpMin
		}
		d = 1 / d
		h *= d * c
		aa = -(a + fm) * (qab + fm) * x / ((a + m2) * (qap + m2))
		d = 1 + aa*d
		if math.Abs(d) < fpMin {
			d = fpMin
		}
		c = 1 + aa/c
		if math.Abs(c) < fpMin {
			c = fpMin
		}
		d = 1 / d
		del := d * c
		h *= del
		if math.Abs(del-1) < eps {
			break
		}
	}
	return h
}

// isFinite reports whether v is neither NaN nor ±Inf.
func isFinite(v float64) bool {
	return !math.IsNaN(v) && !math.IsInf(v, 0)
}
