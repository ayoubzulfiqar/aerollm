package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/marketplace"
	"go.etcd.io/bbolt"
)

func receiptIDs(t *testing.T, db *bbolt.DB) (ids []string, indexed int, count uint64) {
	t.Helper()
	_ = db.View(func(tx *bbolt.Tx) error {
		eb := tx.Bucket(bucketEdge)
		if eb == nil {
			return nil
		}
		if rb := eb.Bucket(bucketReceipts); rb != nil {
			_ = rb.ForEach(func(k, _ []byte) error {
				ids = append(ids, string(k))
				return nil
			})
		}
		if ib := eb.Bucket(bucketReceiptIndex); ib != nil {
			indexed = ib.Stats().KeyN
			count = ib.Sequence()
		}
		return nil
	})
	return ids, indexed, count
}

func testReceipt(id string, at time.Time) marketplace.BillingReceipt {
	return marketplace.BillingReceipt{ReceiptID: id, EventName: "token", Value: 1, Currency: "USD", RecordedAt: at}
}

func TestReceiptBucketCountCapEvictsOldest(t *testing.T) {
	db := openTestDB(t)
	limits := receiptLimits{maxCount: 3}
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := 1; i <= 5; i++ {
		// Recorded out of ID order to show eviction follows recording time.
		rec := testReceipt(fmt.Sprintf("r-%d", 6-i), base.Add(time.Duration(i)*time.Minute))
		if _, created, err := storeReceiptBounded(db, rec, limits, base); err != nil || !created {
			t.Fatalf("store %s: created=%v err=%v", rec.ReceiptID, created, err)
		}
	}
	ids, indexed, count := receiptIDs(t, db)
	if strings.Join(ids, ",") != "r-1,r-2,r-3" || indexed != 3 || count != 3 {
		t.Fatalf("expected the three newest receipts, got %v (index %d, count %d)", ids, indexed, count)
	}
	// Replays of kept receipts stay idempotent and do not evict anything.
	if _, created, err := storeReceiptBounded(db, testReceipt("r-1", base.Add(time.Hour)), limits, base); err != nil || created {
		t.Fatalf("replay: created=%v err=%v", created, err)
	}
	if ids, _, count := receiptIDs(t, db); len(ids) != 3 || count != 3 {
		t.Fatalf("replay changed the bucket: %v %d", ids, count)
	}
}

func TestReceiptBucketAgeEviction(t *testing.T) {
	db := openTestDB(t)
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	limits := receiptLimits{maxCount: 100, maxAge: 24 * time.Hour}
	for i, age := range []time.Duration{72 * time.Hour, 48 * time.Hour, time.Hour} {
		if _, _, err := storeReceiptBounded(db, testReceipt(fmt.Sprintf("old-%d", i), now.Add(-age)), receiptLimits{maxCount: 100}, now); err != nil {
			t.Fatal(err)
		}
	}
	if _, _, err := storeReceiptBounded(db, testReceipt("fresh", now), limits, now); err != nil {
		t.Fatal(err)
	}
	ids, indexed, count := receiptIDs(t, db)
	if strings.Join(ids, ",") != "fresh,old-2" || indexed != 2 || count != 2 {
		t.Fatalf("expected receipts older than a day to be evicted, got %v (index %d, count %d)", ids, indexed, count)
	}
}

func TestReceiptIndexMigrationFromUnindexedState(t *testing.T) {
	db := openTestDB(t)
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	// State written by an edge-node without the index.
	err := db.Update(func(tx *bbolt.Tx) error {
		eb, _ := tx.CreateBucketIfNotExists(bucketEdge)
		rb, _ := eb.CreateBucketIfNotExists(bucketReceipts)
		for i := 0; i < 4; i++ {
			b, _ := json.Marshal(testReceipt(fmt.Sprintf("legacy-%d", i), base.Add(time.Duration(i)*time.Minute)))
			if err := rb.Put([]byte(fmt.Sprintf("legacy-%d", i)), b); err != nil {
				return err
			}
		}
		return rb.Put([]byte("corrupt"), []byte("{not json"))
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := ensureReceiptIndex(db, receiptLimits{maxCount: 3}, base); err != nil {
		t.Fatal(err)
	}
	ids, indexed, count := receiptIDs(t, db)
	// The unreadable record sorts oldest and is evicted first, then legacy-0.
	if strings.Join(ids, ",") != "legacy-1,legacy-2,legacy-3" || indexed != 3 || count != 3 {
		t.Fatalf("unexpected migrated state %v (index %d, count %d)", ids, indexed, count)
	}
	// An index out of step with the bucket is rebuilt.
	_ = db.Update(func(tx *bbolt.Tx) error {
		return tx.Bucket(bucketEdge).Bucket(bucketReceiptIndex).SetSequence(42)
	})
	if err := ensureReceiptIndex(db, receiptLimits{maxCount: 10}, base); err != nil {
		t.Fatal(err)
	}
	if _, indexed, count := receiptIDs(t, db); indexed != 3 || count != 3 {
		t.Fatalf("index not rebuilt: %d %d", indexed, count)
	}
	// An empty state file is fine.
	if err := ensureReceiptIndex(openTestDB(t), receiptLimits{}, base); err != nil {
		t.Fatal(err)
	}
}

func TestReceiptRouteIsBounded(t *testing.T) {
	s, srv := newTestEdge(t, func(c *edgeConfig) { c.receipts = receiptLimits{maxCount: 2} })
	tick := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	s.now = func() time.Time { tick = tick.Add(time.Second); return tick }
	post := func(id string) int {
		body := `{"receipt_id":"` + id + `","event_name":"token","value":1,"currency":"USD"}`
		resp, _ := doReq(t, http.MethodPost, srv.URL+"/v1/marketplace/openstandard/receipt", body, nil)
		return resp.StatusCode
	}
	for _, id := range []string{"a", "b", "c"} {
		if code := post(id); code != http.StatusCreated {
			t.Fatalf("post %s: %d", id, code)
		}
	}
	if ids, _, _ := receiptIDs(t, s.db); strings.Join(ids, ",") != "b,c" {
		t.Fatalf("expected the oldest receipt to be evicted, got %v", ids)
	}
	if code := post("c"); code != http.StatusOK {
		t.Fatalf("replay of a kept receipt: %d", code)
	}
	// An evicted receipt id is forgotten, so it is created again.
	if code := post("a"); code != http.StatusCreated {
		t.Fatalf("replay of an evicted receipt: %d", code)
	}
}

func TestDefaultReceiptLimits(t *testing.T) {
	if (receiptLimits{}).count() != defaultMaxReceipts || (receiptLimits{maxCount: 7}).count() != 7 {
		t.Fatal("unexpected receipt limit defaults")
	}
	db := openTestDB(t)
	if err := queueReceipt(db, testReceipt("x", time.Now())); err != nil {
		t.Fatal(err)
	}
	if _, _, count := receiptIDs(t, db); count != 1 {
		t.Fatalf("default path must maintain the index, count=%d", count)
	}
}
