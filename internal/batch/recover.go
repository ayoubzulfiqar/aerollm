package batch

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
)

// RecoveryReport lists what Recover did with each unfinished batch.
type RecoveryReport struct {
	// Resumed batches continue with the requests that have no result yet
	// (batches whose completion window elapsed finish as "expired").
	Resumed []string `json:"resumed,omitempty"`
	// Failed batches could not be resumed (their input is gone, e.g. the
	// work directory was not persistent) and were marked failed with code
	// "interrupted".
	Failed []string `json:"failed,omitempty"`
	// Cancelled batches were being cancelled when the process stopped.
	Cancelled []string `json:"cancelled,omitempty"`
}

// Interrupted-batch error messages.
const (
	interruptedCode       = "interrupted"
	interruptedNoInput    = "batch interrupted by a gateway restart; its input file is no longer available, so it cannot be resumed"
	interruptedBadInput   = "batch interrupted by a gateway restart; its input file could not be re-read"
	interruptedBadResults = "batch interrupted by a gateway restart; its partial result files could not be read"
)

// Recover resumes or settles the batches that a previous process left
// unfinished (status validating, in_progress, finalizing or cancelling) in
// the store. Call it once at startup, after NewBatchProcessor, when the
// store is persistent (see PersistentStore).
//
// A batch whose input file is still in the work directory (which requires
// PersistentWorkDir) is resumed: its result files are scanned, requests
// that already have a result line are skipped, counts are rebuilt from the
// files and only the remaining requests run (so each request is executed
// at least once; one whose result line had not been written before a
// crash runs again). Otherwise the batch is marked failed with an
// "interrupted" error. Batches that were being cancelled become cancelled.
func (bp *BatchProcessor) Recover(ctx context.Context) (RecoveryReport, error) {
	var rep RecoveryReport
	if bp.initErr != nil {
		return rep, fmt.Errorf("batch: work directory unavailable: %w", bp.initErr)
	}
	all, err := bp.store.ListBatches(ctx)
	if err != nil {
		return rep, err
	}
	// Oldest first, so resumed batches keep their relative order.
	sort.SliceStable(all, func(i, j int) bool { return all[i].CreatedAt.Before(all[j].CreatedAt) })
	var errs []error
	for _, b := range all {
		if b.Status.Terminal() || !ValidBatchID(b.ID) {
			continue
		}
		if err := ctx.Err(); err != nil {
			return rep, err
		}
		bp.mu.Lock()
		_, running := bp.running[b.ID]
		closed := bp.closed
		bp.mu.Unlock()
		if closed {
			return rep, ErrProcessorClosed
		}
		if running {
			continue
		}
		if err := bp.recoverOne(ctx, b, &rep); err != nil {
			errs = append(errs, fmt.Errorf("batch %s: %w", b.ID, err))
		}
	}
	return rep, errors.Join(errs...)
}

func (bp *BatchProcessor) recoverOne(ctx context.Context, b *Batch, rep *RecoveryReport) error {
	now := bp.now().UTC()
	outPath, _ := bp.OutputPath(b.ID)
	errPath, _ := bp.ErrorPath(b.ID)
	inPath, _ := bp.path("input", b.ID)

	fail := func(msg string) error {
		b.Status, b.FailedAt, b.Error = StatusFailed, &now, msg
		b.Errors = append(b.Errors, BatchError{Code: interruptedCode, Message: msg})
		// Partial results stay downloadable when the files survived.
		if fileExists(outPath) && fileExists(errPath) {
			b.OutputFileID, b.ErrorFileID = "output_"+b.ID, "errors_"+b.ID
		}
		b.syncCounts()
		if err := bp.store.UpdateBatch(ctx, b); err != nil {
			return err
		}
		rep.Failed = append(rep.Failed, b.ID)
		return nil
	}

	if b.Status == StatusCancelling {
		if res, err := scanResultFiles(outPath, errPath); err == nil && res.found {
			b.RequestCounts.Completed, b.RequestCounts.Failed = res.completed, res.failed
			b.OutputFileID, b.ErrorFileID = "output_"+b.ID, "errors_"+b.ID
		}
		b.Status, b.CancelledAt = StatusCancelled, &now
		b.syncCounts()
		if err := bp.store.UpdateBatch(ctx, b); err != nil {
			return err
		}
		rep.Cancelled = append(rep.Cancelled, b.ID)
		return nil
	}

	data, err := os.ReadFile(inPath)
	if err != nil {
		return fail(interruptedNoInput)
	}
	reqs, verrs := bp.parse(data)
	if len(verrs) > 0 {
		return fail(interruptedBadInput)
	}
	res, err := scanResultFiles(outPath, errPath)
	if err != nil {
		return fail(interruptedBadResults)
	}
	remaining := reqs[:0:0]
	for _, pl := range reqs {
		if !res.done[pl.customID] {
			remaining = append(remaining, pl)
		}
	}
	b.RequestCounts = RequestCounts{Total: len(reqs), Completed: res.completed, Failed: res.failed}
	b.Error, b.OutputFileID, b.ErrorFileID = "", "", ""
	if b.Status == StatusValidating || b.InProgressAt == nil {
		b.InProgressAt = &now
	}
	b.Status = StatusInProgress
	b.syncCounts()
	if err := bp.store.UpdateBatch(ctx, b); err != nil {
		return err
	}

	expires := now.Add(bp.cfg.CompletionWindow)
	if b.ExpiresAt != nil {
		expires = *b.ExpiresAt
	}
	bp.mu.Lock()
	defer bp.mu.Unlock()
	if bp.closed {
		return ErrProcessorClosed
	}
	runCtx, cancel := context.WithDeadline(bp.baseCtx, expires)
	rs := &runState{batch: b.Clone(), cancel: cancel}
	bp.running[b.ID] = rs
	bp.wg.Add(1)
	go bp.run(runCtx, rs, remaining, true)
	rep.Resumed = append(rep.Resumed, b.ID)
	return nil
}

func fileExists(path string) bool {
	fi, err := os.Lstat(path)
	return err == nil && fi.Mode().IsRegular()
}

type scanResult struct {
	done      map[string]bool
	completed int
	failed    int
	found     bool
}

// scanResultFiles collects the custom IDs that already have a result line
// in the output and error files of an interrupted batch. A torn trailing
// line (the process died mid-write) is truncated away.
func scanResultFiles(outPath, errPath string) (scanResult, error) {
	res := scanResult{done: map[string]bool{}}
	for i, p := range []string{outPath, errPath} {
		ids, found, err := scanResultFile(p)
		if err != nil {
			return res, err
		}
		res.found = res.found || found
		for id := range ids {
			if res.done[id] {
				continue
			}
			res.done[id] = true
			if i == 0 {
				res.completed++
			} else {
				res.failed++
			}
		}
	}
	return res, nil
}

func scanResultFile(path string) (map[string]bool, bool, error) {
	ids := map[string]bool{}
	if fi, err := os.Lstat(path); errors.Is(err, os.ErrNotExist) {
		return ids, false, nil
	} else if err != nil {
		return nil, false, err
	} else if !fi.Mode().IsRegular() {
		return nil, false, fmt.Errorf("batch: %s is not a regular file", path)
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, false, err
	}
	r := bufio.NewReaderSize(f, 64<<10)
	var good, off int64
	for {
		line, rerr := r.ReadBytes('\n')
		off += int64(len(line))
		complete := len(line) > 0 && line[len(line)-1] == '\n'
		if complete {
			var resp BatchResponse
			if json.Unmarshal(line, &resp) == nil && resp.CustomID != "" {
				ids[resp.CustomID] = true
			}
			good = off
		}
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			f.Close()
			return nil, true, rerr
		}
	}
	f.Close()
	if good < off {
		if err := os.Truncate(path, good); err != nil {
			return nil, true, err
		}
	}
	return ids, true, nil
}
