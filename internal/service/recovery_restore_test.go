package service

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/wyw14/solo-039/internal/model"
)

func uniqueEventIDs(events []model.Event) map[string]int {
	ids := make(map[string]int)
	for _, event := range events {
		ids[event.ID.String()]++
	}
	return ids
}

func TestRestartDoesNotReexecuteConfirmedActions(t *testing.T) {
	dir := t.TempDir()

	first := NewPlant(dir)
	if _, err := first.Start(context.Background(), "grade-a", "tank-1"); err != nil {
		t.Fatalf("start: %v", err)
	}
	durableCount := len(first.Journal.Events())
	if durableCount == 0 {
		t.Fatal("expected durable events from start")
	}
	if _, err := os.Stat(filepath.Join(dir, "checkpoint.json")); err != nil {
		t.Fatalf("checkpoint not persisted: %v", err)
	}

	// First restart: checkpoint is already at the chain head, so every action
	// is confirmed and nothing is re-executed.
	second := NewPlant(dir)
	if err := second.Restore(); err != nil {
		t.Fatalf("restore: %v", err)
	}
	review := second.RecoveryReview()
	if !review["checkpoint_bound"].(bool) {
		t.Fatalf("checkpoint should bind: %+v", review)
	}
	if review["replayed"].(int) != 0 {
		t.Fatalf("confirmed actions re-executed on restart: %+v", review)
	}
	restoredEvents := second.Journal.Events()
	if len(restoredEvents) != durableCount {
		t.Fatalf("restored journal has %d events, want %d", len(restoredEvents), durableCount)
	}
	for id, count := range uniqueEventIDs(restoredEvents) {
		if count != 1 {
			t.Fatalf("event %s present %d times after restore", id, count)
		}
	}

	// A repeat restart must reach the identical boundary and replay nothing.
	third := NewPlant(dir)
	if err := third.Restore(); err != nil {
		t.Fatalf("second restore: %v", err)
	}
	if third.RecoveryReview()["replayed"].(int) != 0 {
		t.Fatal("repeat restart re-executed actions")
	}
	if len(third.Journal.Events()) != durableCount {
		t.Fatal("repeat restart changed the recovered event count")
	}
}

func TestColdStartReplaysThenConfirms(t *testing.T) {
	dir := t.TempDir()

	first := NewPlant(dir)
	if _, err := first.Start(context.Background(), "grade-a", "tank-1"); err != nil {
		t.Fatalf("start: %v", err)
	}
	durableCount := len(first.Journal.Events())

	// Simulate a log that predates checkpoints: drop both markers but keep the
	// chained event log.
	if err := os.Remove(filepath.Join(dir, "checkpoint.json")); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(dir, "snapshot.json")); err != nil {
		t.Fatal(err)
	}

	second := NewPlant(dir)
	if err := second.Restore(); err != nil {
		t.Fatalf("cold restore: %v", err)
	}
	if got := second.RecoveryReview()["replayed"].(int); got != durableCount {
		t.Fatalf("cold restart should replay %d unconfirmed events, replayed %d", durableCount, got)
	}

	// Replay published a fresh checkpoint at the head. The next restart skips.
	third := NewPlant(dir)
	if err := third.Restore(); err != nil {
		t.Fatalf("restore after replay: %v", err)
	}
	if got := third.RecoveryReview()["replayed"].(int); got != 0 {
		t.Fatalf("replayed events were not confirmed: second restart replayed %d", got)
	}
}

func TestSnapshotTrailingLogIsCaughtUpByReplay(t *testing.T) {
	dir := t.TempDir()

	first := NewPlant(dir)
	if _, err := first.Start(context.Background(), "grade-a", "tank-1"); err != nil {
		t.Fatalf("start: %v", err)
	}

	// Simulate a crash window: an event reached the durable log, but neither a
	// fresh snapshot nor an advanced checkpoint was written.
	campaignState := first.Campaign.Snapshot()
	campaignState.Epoch++
	pending := model.NewEvent("post.crash", campaignState, "late")
	pending.At = 1234
	if err := first.FileLog.Append(pending); err != nil {
		t.Fatalf("append trailing event: %v", err)
	}

	second := NewPlant(dir)
	if err := second.Restore(); err != nil {
		t.Fatalf("restore must catch a trailing snapshot up via replay, got: %v", err)
	}
	review := second.RecoveryReview()
	if review["replayed"].(int) < 1 {
		t.Fatalf("trailing event should be replayed: %+v", review)
	}
	if got := second.Campaign.Snapshot().Epoch; got != campaignState.Epoch {
		t.Fatalf("campaign epoch not caught up: got %d want %d", got, campaignState.Epoch)
	}
}

func TestCorruptTailIsQuarantinedOnRestore(t *testing.T) {
	dir := t.TempDir()

	first := NewPlant(dir)
	if _, err := first.Start(context.Background(), "grade-a", "tank-1"); err != nil {
		t.Fatalf("start: %v", err)
	}
	trustedCount := len(first.Journal.Events())

	logPath := filepath.Join(dir, "events.jsonl")
	raw, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(logPath, append(raw, []byte("{\"seq\":99,\"broken\n")...), 0644); err != nil {
		t.Fatal(err)
	}

	second := NewPlant(dir)
	if err := second.Restore(); err != nil {
		t.Fatalf("restore with corrupt tail should quarantine, not fail: %v", err)
	}
	review := second.RecoveryReview()
	if !review["needs_review"].(bool) {
		t.Fatal("corrupt tail must be flagged for manual review")
	}
	if review["quarantine"] == "" {
		t.Fatal("corrupt tail must be quarantined")
	}
	if _, err := os.Stat(review["quarantine"].(string)); err != nil {
		t.Fatalf("quarantine file missing: %v", err)
	}
	if len(second.Journal.Events()) != trustedCount {
		t.Fatalf("only trusted events may be loaded, got %d want %d", len(second.Journal.Events()), trustedCount)
	}

	// Restarting again is stable: the tail stays isolated and needs no new work.
	third := NewPlant(dir)
	if err := third.Restore(); err != nil {
		t.Fatalf("repeat restore: %v", err)
	}
	if len(third.Journal.Events()) != trustedCount {
		t.Fatal("trusted prefix changed across repeat restart")
	}
}
