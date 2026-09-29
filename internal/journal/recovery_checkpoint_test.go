package journal

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/wyw14/solo-039/internal/model"
)

func testEvent(kind string, campaign model.Campaign, payload string) model.Event {
	return model.NewEvent(kind, campaign, payload)
}

func mustUUID(t *testing.T, value string) uuid.UUID {
	t.Helper()
	parsed, err := uuid.Parse(value)
	if err != nil {
		t.Fatalf("bad uuid %q: %v", value, err)
	}
	return parsed
}

func appendN(t *testing.T, log *FileLog, n int) []model.Event {
	t.Helper()
	campaignState := model.NewCampaign("g")
	events := make([]model.Event, 0, n)
	for i := 0; i < n; i++ {
		event := testEvent("test.event", campaignState, "payload")
		if err := log.Append(event); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
		events = append(events, event)
	}
	return events
}

func TestRecordChainVerifiesAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.jsonl")
	log := NewFileLog(path)
	events := appendN(t, log, 3)

	reopened := NewFileLog(path)
	got, err := reopened.Read()
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("want 3 events, got %d", len(got))
	}
	for i, event := range got {
		if event.ID != events[i].ID {
			t.Fatalf("event %d id mismatch", i)
		}
	}
	// A new event continues the chain seamlessly after reopen at seq 4.
	next := testEvent("test.event", model.NewCampaign("g"), "more")
	if err := reopened.Append(next); err != nil {
		t.Fatalf("append after reopen: %v", err)
	}
	if seq, _ := reopened.Head(); seq != 4 {
		t.Fatalf("want head seq 4, got %d", seq)
	}
	_ = got
}

func TestAppendReadbackVerifies(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.jsonl")
	log := NewFileLog(path)
	c := model.NewCampaign("g")
	if err := log.Append(testEvent("a", c, "x")); err != nil {
		t.Fatal(err)
	}
	seq, digest := log.Head()
	if seq != 1 || digest == "" || digest == GenZero {
		t.Fatalf("unexpected head %d %q", seq, digest)
	}
}

func TestCorruptTailIsIsolated(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "events.jsonl")
	log := NewFileLog(path)
	events := appendN(t, log, 3)

	// Append a torn, non-JSON suffix as a fourth line.
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString("{\"seq\":4,\"broken\"\n"); err != nil {
		t.Fatal(err)
	}
	f.Close()

	fresh := NewFileLog(path)
	scan, err := fresh.Scan()
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if scan.Trusted() {
		t.Fatal("expected corrupt tail")
	}
	if len(scan.Events) != 3 {
		t.Fatalf("want 3 trusted events, got %d", len(scan.Events))
	}
	if scan.Tail.Seq != 4 || scan.Tail.Line != 4 {
		t.Fatalf("unexpected tail location %+v", scan.Tail)
	}

	quarantine, err := fresh.Isolate(scan)
	if err != nil {
		t.Fatalf("isolate: %v", err)
	}
	if quarantine == "" {
		t.Fatal("expected a quarantine path")
	}
	if _, err := os.Stat(quarantine); err != nil {
		t.Fatalf("quarantine file missing: %v", err)
	}

	// After isolation the log verifies cleanly with the trusted prefix intact.
	after, err := NewFileLog(path).Read()
	if err != nil {
		t.Fatalf("read after isolate: %v", err)
	}
	if len(after) != 3 || after[2].ID != events[2].ID {
		t.Fatal("trusted prefix was damaged by isolation")
	}

	// A repeat restart must see the same boundary and not re-quarantine.
	again, err := NewFileLog(path).Read()
	if err != nil || len(again) != 3 {
		t.Fatalf("repeat restart unstable: %d %v", len(again), err)
	}
}

func TestTamperedMiddleRecordBreaksChain(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "events.jsonl")
	log := NewFileLog(path)
	appendN(t, log, 3)

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimRight(string(raw), "\n"), "\n")
	// Flip a character inside the second record's payload.
	lines[1] = strings.Replace(lines[1], "payload", "xayload", 1)
	os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0644)

	scan, err := NewFileLog(path).Scan()
	if err != nil {
		t.Fatal(err)
	}
	if scan.Trusted() {
		t.Fatal("tampered record must fail verification")
	}
	// The tampered record and everything after it is untrusted.
	if len(scan.Events) != 1 {
		t.Fatalf("want only the first record trusted, got %d", len(scan.Events))
	}
}

func TestCheckpointSaveLoadAndTamper(t *testing.T) {
	dir := t.TempDir()
	log := NewFileLog(filepath.Join(dir, "events.jsonl"))
	cps := NewCheckpointFile(filepath.Join(dir, "checkpoint.json"))

	c := model.NewCampaign("g")
	event := testEvent("plant.started", c, "src")
	if err := log.Append(event); err != nil {
		t.Fatal(err)
	}
	rec, headEvent, ok := log.HeadRecord()
	if !ok {
		t.Fatal("missing head record")
	}
	cp, err := BoundCheckpoint(rec, headEvent, []string{event.ID.String()}, []string{c.Revision.String()}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := cps.Save(cp); err != nil {
		t.Fatalf("save: %v", err)
	}
	loaded, err := cps.Load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if loaded.Seq != 1 || !loaded.ConfirmedAction(event.ID.String()) {
		t.Fatalf("checkpoint not restored: %+v", loaded)
	}

	// Tamper the sealed file on disk by changing a field the seal covers
	// without recomputing the seal.
	raw, _ := os.ReadFile(cps.Path())
	tampered := strings.Replace(string(raw), `"seq":1`, `"seq":2`, 1)
	if tampered == string(raw) {
		t.Fatal("test setup failed to alter the checkpoint")
	}
	os.WriteFile(cps.Path(), []byte(tampered), 0644)
	_, err = cps.Load()
	if err == nil {
		t.Fatal("tampered checkpoint must fail verification")
	}
	if _, statErr := os.Stat(cps.Path() + ".suspect"); statErr != nil {
		t.Fatal("suspect checkpoint should be moved aside")
	}
}

func TestResolveBoundaryColdStartReplaysAll(t *testing.T) {
	dir := t.TempDir()
	log := NewFileLog(filepath.Join(dir, "events.jsonl"))
	cps := NewCheckpointFile(filepath.Join(dir, "checkpoint.json"))
	events := appendN(t, log, 3)

	boundary, err := ResolveBoundary(log, cps)
	if err != nil {
		t.Fatal(err)
	}
	if boundary.CheckpointPresent || boundary.CheckpointBound {
		t.Fatal("cold start must have no checkpoint")
	}
	if len(boundary.ReplayEvents) != 3 {
		t.Fatalf("want 3 replay events, got %d", len(boundary.ReplayEvents))
	}
	for _, decision := range boundary.Decisions {
		if decision.Action != ActionReplay {
			t.Fatalf("cold start event %d marked %s", decision.Seq, decision.Action)
		}
	}
	if boundary.NeedsManualReview() {
		t.Fatal("clean log needs no review")
	}
	_ = events
}

func TestConfirmedActionsAreNotReplayedTwice(t *testing.T) {
	dir := t.TempDir()
	log := NewFileLog(filepath.Join(dir, "events.jsonl"))
	cps := NewCheckpointFile(filepath.Join(dir, "checkpoint.json"))

	c := model.NewCampaign("g")
	first := testEvent("plant.started", c, "a")
	second := testEvent("feed.transition", c, "b")
	for _, event := range []model.Event{first, second} {
		if err := log.Append(event); err != nil {
			t.Fatal(err)
		}
	}

	// Simulate the first restart: both replay, then checkpoint at the head
	// confirming BOTH actions.
	boundary, err := ResolveBoundary(log, cps)
	if err != nil {
		t.Fatal(err)
	}
	cp, err := boundary.NextCheckpoint([]string{first.ID.String(), second.ID.String()})
	if err != nil {
		t.Fatal(err)
	}
	if err := cps.Save(cp); err != nil {
		t.Fatal(err)
	}

	// Repeat restart: nothing may be replayed.
	again, err := ResolveBoundary(NewFileLog(log.Path()), NewCheckpointFile(cps.Path()))
	if err != nil {
		t.Fatal(err)
	}
	if !again.CheckpointBound {
		t.Fatal("checkpoint should bind")
	}
	if len(again.ReplayEvents) != 0 {
		t.Fatalf("confirmed events replayed twice: %d", len(again.ReplayEvents))
	}
	for _, decision := range again.Decisions {
		if decision.Action != ActionSkipCheckpoint && decision.Action != ActionSkipConfirmed {
			t.Fatalf("event %d disposition %s", decision.Seq, decision.Action)
		}
	}
}

func TestOnlyEventsPastBoundaryReplay(t *testing.T) {
	dir := t.TempDir()
	log := NewFileLog(filepath.Join(dir, "events.jsonl"))
	cps := NewCheckpointFile(filepath.Join(dir, "checkpoint.json"))

	c := model.NewCampaign("g")
	e1 := testEvent("plant.started", c, "1")
	e2 := testEvent("feed.transition", c, "2")
	if err := log.Append(e1); err != nil {
		t.Fatal(err)
	}
	if err := log.Append(e2); err != nil {
		t.Fatal(err)
	}

	// Checkpoint binds to seq 2 and confirms e1 and e2 (everything at or before
	// the boundary is skipped even without an explicit confirmed entry).
	scan, _ := log.Scan()
	cp, err := BoundCheckpoint(scan.Records[1], scan.Events[1], []string{e1.ID.String(), e2.ID.String()}, []string{c.Revision.String()}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := cps.Save(cp); err != nil {
		t.Fatal(err)
	}

	// A third event lands after the checkpoint, unconfirmed.
	e3 := testEvent("product.routed", c, "3")
	if err := NewFileLog(log.Path()).Append(e3); err != nil {
		t.Fatal(err)
	}

	boundary, err := ResolveBoundary(NewFileLog(log.Path()), NewCheckpointFile(cps.Path()))
	if err != nil {
		t.Fatal(err)
	}
	if len(boundary.ReplayEvents) != 1 || boundary.ReplayEvents[0].ID != e3.ID {
		t.Fatalf("only the unconfirmed tail must replay, got %d", len(boundary.ReplayEvents))
	}
	actions := map[uint64]string{}
	for _, d := range boundary.Decisions {
		actions[d.Seq] = d.Action
	}
	if actions[1] != ActionSkipCheckpoint || actions[2] != ActionSkipCheckpoint || actions[3] != ActionReplay {
		t.Fatalf("unexpected dispositions %v", actions)
	}
}

func TestGroupsRespectBatchAndGeneration(t *testing.T) {
	events := []model.Event{
		{Revision: mustUUID(t, "11111111-1111-1111-1111-111111111111"), Epoch: 1},
		{Revision: mustUUID(t, "11111111-1111-1111-1111-111111111111"), Epoch: 1},
		{Revision: mustUUID(t, "22222222-2222-2222-2222-222222222222"), Epoch: 2},
		{Revision: mustUUID(t, "11111111-1111-1111-1111-111111111111"), Epoch: 3},
	}
	groups := GroupByBatchGeneration(events)
	if len(groups) != 3 {
		t.Fatalf("want 3 runs, got %d", len(groups))
	}
	if groups[0].Events[0].Epoch != 1 || len(groups[0].Events) != 2 {
		t.Fatal("first group wrong")
	}
	if groups[1].Revision != events[2].Revision.String() {
		t.Fatal("second group wrong generation")
	}
	if groups[2].Epoch != 3 {
		t.Fatal("generation must restart a group even if seen earlier")
	}
}

func TestCorruptTailFlagsManualReview(t *testing.T) {
	dir := t.TempDir()
	log := NewFileLog(filepath.Join(dir, "events.jsonl"))
	cps := NewCheckpointFile(filepath.Join(dir, "checkpoint.json"))
	appendN(t, log, 2)

	raw, _ := os.ReadFile(log.Path())
	os.WriteFile(log.Path(), append(raw, []byte("not-a-record\n")...), 0644)

	boundary, err := ResolveBoundary(NewFileLog(log.Path()), NewCheckpointFile(cps.Path()))
	if err != nil {
		t.Fatal(err)
	}
	if !boundary.NeedsManualReview() {
		t.Fatal("corrupt tail must require manual review")
	}
	if boundary.Quarantine == "" {
		t.Fatal("tail must be quarantined")
	}
	found := false
	for _, review := range boundary.Manual {
		if review.Quarantine == boundary.Quarantine {
			found = true
		}
	}
	if !found {
		t.Fatal("manual review must reference the quarantine file")
	}
}

func TestMismatchedCheckpointHoldsEverythingForReview(t *testing.T) {
	dir := t.TempDir()
	log := NewFileLog(filepath.Join(dir, "events.jsonl"))
	cps := NewCheckpointFile(filepath.Join(dir, "checkpoint.json"))
	events := appendN(t, log, 3)

	// Forge a checkpoint that points past the log end.
	scan, _ := log.Scan()
	forge, err := BoundCheckpoint(scan.Records[len(scan.Records)-1], scan.Events[len(scan.Events)-1], nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	forge.Seq = 99
	forge = forge.Sealed()
	if err := cps.Save(forge); err != nil {
		t.Fatal(err)
	}

	boundary, err := ResolveBoundary(NewFileLog(log.Path()), NewCheckpointFile(cps.Path()))
	if err != nil {
		t.Fatal(err)
	}
	if boundary.BindError == "" {
		t.Fatal("out-of-range checkpoint must fail to bind")
	}
	if len(boundary.ReplayEvents) != 0 {
		t.Fatal("nothing may auto-replay past an untrusted checkpoint")
	}
	for _, decision := range boundary.Decisions {
		if decision.Action != ActionManualReview {
			t.Fatalf("event %d not held for review", decision.Seq)
		}
	}
	_ = events
}

func TestRecoveryPlanHoldForReview(t *testing.T) {
	plan := DefaultRecoveryPlan()
	if !plan.MarkReady("campaign") {
		t.Fatal("campaign should be ready")
	}
	plan.Apply("campaign")
	if !plan.MarkReady("column") {
		t.Fatal("column should be ready")
	}
	if !plan.HoldForReview("column", "fencing token unverifiable") {
		t.Fatal("hold should succeed")
	}
	if !plan.NeedsReview() {
		t.Fatal("plan must report a held step")
	}
	// Held steps are not counted as pending automation, so automated recovery
	// is "complete" while the review remains surfaced.
	for _, name := range plan.Pending() {
		if name == "column" {
			t.Fatal("held step must not be in the automated pending set")
		}
	}
	if reason, ok := plan.ReviewSteps()["column"]; !ok || reason == "" {
		t.Fatal("review reason must be exposed")
	}
}

func TestAppendAfterIsolationContinuesChain(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "events.jsonl")
	log := NewFileLog(path)
	appendN(t, log, 2)
	raw, _ := os.ReadFile(path)
	os.WriteFile(path, append(raw, []byte("garbage\n")...), 0644)

	fresh := NewFileLog(path)
	// A plain append self-heals the corrupt tail first.
	c := model.NewCampaign("g")
	if err := fresh.Append(testEvent("after", c, "z")); err != nil {
		t.Fatalf("append after corruption: %v", err)
	}
	got, err := NewFileLog(path).Read()
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("want 3 records after self-heal, got %d", len(got))
	}
}

func TestCheckpointReadbackRejectsUnsealed(t *testing.T) {
	dir := t.TempDir()
	cps := NewCheckpointFile(filepath.Join(dir, "checkpoint.json"))
	if err := cps.Save(Checkpoint{Seq: 1}); err == nil {
		t.Fatal("an unsealed checkpoint must not persist")
	}
	if _, err := os.Stat(cps.Path()); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("no checkpoint file should exist, got %v", err)
	}
}
