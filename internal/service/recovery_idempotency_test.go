package service

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

// 第一次"进程"执行 Start + AcquireReflux；第二次启动必须从检查点/快照恢复，
// 不追加事件、不重复累加回流容量、活动身份一致、检查点水位==链尖。
func TestRestartDoesNotReexecuteConfirmedActions(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	first := NewPlant(dir)
	if _, err := first.Start(ctx, "baseline", "primary"); err != nil {
		t.Fatalf("start: %v", err)
	}
	if _, err := first.AcquireReflux("T-101", 0.2); err != nil {
		t.Fatalf("acquire: %v", err)
	}
	// 跨代次换牌号：产生新 Revision 与 feed 切换事件。
	if _, err := first.Transition(ctx, "premium", "secondary"); err != nil {
		t.Fatalf("transition: %v", err)
	}
	wantCampaign := first.Campaign.Snapshot()
	if first.Feed.Current.Source != "secondary" {
		t.Fatalf("feed not switched: %+v", first.Feed.Current)
	}
	if used := first.Reflux.Used(); used != 0.2 {
		t.Fatalf("first run used=%v want 0.2", used)
	}
	firstEvents := len(first.Journal.Events())

	// 模拟一次全新进程：重新构造 Plant 并恢复同一数据目录。
	second := NewPlant(dir)
	report, err := second.Restore()
	if err != nil {
		t.Fatalf("restore: %v", err)
	}
	if len(second.Journal.Events()) != firstEvents {
		t.Fatalf("restore appended events: %d != %d", len(second.Journal.Events()), firstEvents)
	}
	if used := second.Reflux.Used(); used != 0.2 {
		t.Fatalf("confirmed lease was re-executed, used=%v want 0.2", used)
	}
	got := second.Campaign.Snapshot()
	if got.ID != wantCampaign.ID || got.Revision != wantCampaign.Revision || got.Epoch != wantCampaign.Epoch {
		t.Fatalf("campaign identity drifted across restart: %+v vs %+v", got, wantCampaign)
	}
	if report.ConfirmedSeq != report.TrustedSeq || report.ConfirmedSeq == 0 {
		t.Fatalf("checkpoint not at chain tip after restart: confirmed=%d trusted=%d", report.ConfirmedSeq, report.TrustedSeq)
	}
	if len(report.ReplayedEventIDs) != 0 {
		t.Fatalf("confirmed actions must not be replayed, got %v", report.ReplayedEventIDs)
	}
	if second.Feed.Current.Source != "secondary" || !second.Feed.Current.Open {
		t.Fatalf("confirmed feed route not hydrated: %+v", second.Feed.Current)
	}
}

// 损坏事件链尾部：恢复必须隔离坏尾、标记人工复核、开 incident，且服务仍可恢复（降级，不致命）。
func TestCorruptTailIsQuarantinedAndDegrades(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	first := NewPlant(dir)
	if _, err := first.Start(ctx, "baseline", "primary"); err != nil {
		t.Fatalf("start: %v", err)
	}

	path := filepath.Join(dir, "events.jsonl")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	// 砍掉最后一条事件的结尾若干字节，制造坏尾。
	if err := os.WriteFile(path, raw[:len(raw)-6], 0o644); err != nil {
		t.Fatal(err)
	}

	second := NewPlant(dir)
	report, err := second.Restore()
	if err != nil {
		t.Fatalf("corrupt tail must degrade, not fail restore: %v", err)
	}
	if !report.ManualReview {
		t.Fatalf("corrupt tail must require manual review: %+v", report)
	}
	if report.Quarantined == nil {
		t.Fatal("corrupt tail must be quarantined")
	}
	if matches, _ := filepath.Glob(filepath.Join(dir, "events.jsonl.quarantine.*.log")); len(matches) == 0 {
		t.Fatal("quarantine file missing")
	}
	open := second.Incidents.List(true)
	found := false
	for _, inc := range open {
		if inc.Kind == "journal.manual-review" {
			found = true
		}
	}
	if !found {
		t.Fatal("expected an open journal.manual-review incident")
	}
	// 隔离后活动日志已截断，可继续追加且序号正确接续。
	if _, _, boundary, err := second.Durable.Scan(); err != nil || boundary != nil {
		t.Fatalf("active log must be clean after quarantine: boundary=%v err=%v", boundary, err)
	}
}

// 检查点丢失（水位 0）但快照仍在：事件全量幂等重放一次，租约容量仍不得翻倍。
func TestRestartWithoutCheckpointReplaysIdempotently(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	first := NewPlant(dir)
	if _, err := first.Start(ctx, "baseline", "primary"); err != nil {
		t.Fatalf("start: %v", err)
	}
	if _, err := first.AcquireReflux("T-101", 0.2); err != nil {
		t.Fatalf("acquire: %v", err)
	}
	firstEvents := len(first.Journal.Events())

	// 删除检查点，保留事件链与快照。
	if err := os.Remove(filepath.Join(dir, "checkpoint.json")); err != nil {
		t.Fatal(err)
	}

	second := NewPlant(dir)
	report, err := second.Restore()
	if err != nil {
		t.Fatalf("restore: %v", err)
	}
	if used := second.Reflux.Used(); used != 0.2 {
		t.Fatalf("replayed lease accumulated twice, used=%v want 0.2", used)
	}
	if len(report.ReplayedEventIDs) != firstEvents {
		t.Fatalf("all %d trusted events should replay once, got %d", firstEvents, len(report.ReplayedEventIDs))
	}
	if !report.ManualReview {
		t.Fatalf("missing checkpoint should be flagged for review, reasons=%v", report.ReviewReasons)
	}
	if second.Feed.Current == nil || second.Feed.Current.Source != "primary" || !second.Feed.Current.Open {
		t.Fatalf("replay did not restore feed state: %+v", second.Feed.Current)
	}
}
