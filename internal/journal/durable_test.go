package journal

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/wyw14/solo-039/internal/model"
)

func mkEvent(kind, payload string) model.Event {
	c := model.NewCampaign("grade")
	e := model.NewEvent(kind, c, payload)
	return e
}

func appendN(t *testing.T, log *DurableLog, n int, kind string) []Envelope {
	t.Helper()
	envelopes := make([]Envelope, 0, n)
	for i := 0; i < n; i++ {
		env, err := log.Append(mkEvent(kind, "payload"))
		if err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
		envelopes = append(envelopes, env)
	}
	return envelopes
}

// 1. 连续追加后重扫：seq 连续、genesis prev 空、digest 全链接。
func TestDurableLogChainIntegrity(t *testing.T) {
	log := NewDurableLog(filepath.Join(t.TempDir(), "events.jsonl"))
	appended := appendN(t, log, 3, "x")

	got, offset, boundary, err := log.Scan()
	if err != nil || boundary != nil {
		t.Fatalf("scan unexpected err=%v boundary=%v", err, boundary)
	}
	if len(got) != 3 {
		t.Fatalf("want 3 envelopes, got %d", len(got))
	}
	if got[0].PrevDigest != "" || got[0].Seq != 1 {
		t.Fatal("genesis envelope must have seq 1 and empty prev")
	}
	for i, env := range got {
		if env.Seq != uint64(i+1) {
			t.Fatalf("seq not contiguous at %d: %d", i, env.Seq)
		}
		if env.Digest != appended[i].Digest {
			t.Fatalf("digest mismatch at %d", i)
		}
		if i > 0 && env.PrevDigest != got[i-1].Digest {
			t.Fatalf("chain broken at %d", i)
		}
	}
	if offset <= 0 {
		t.Fatal("expected non-zero trusted offset")
	}
}

// 2. 篡改中间行 payload：在该行停止，TrustedSeq 指向前一条。
func TestDurableLogTamperedMiddle(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.jsonl")
	log := NewDurableLog(path)
	c := model.NewCampaign("g")
	events := []model.Event{
		model.NewEvent("k", c, "p1"),
		model.NewEvent("k", c, "p2"),
		model.NewEvent("k", c, "p3"),
	}
	for _, e := range events {
		if _, err := log.Append(e); err != nil {
			t.Fatal(err)
		}
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	tampered := bytesReplaceOnce(raw, `"Payload":"p2"`, `"Payload":"pX"`)
	if err := os.WriteFile(path, tampered, 0o644); err != nil {
		t.Fatal(err)
	}

	got, _, boundary, err := log.Scan()
	if err != nil {
		t.Fatal(err)
	}
	if boundary == nil || boundary.Line != 2 {
		t.Fatalf("expected boundary at line 2, got %v", boundary)
	}
	if len(got) != 1 || got[0].Seq != 1 {
		t.Fatalf("trusted prefix must be exactly the first event, got %d", len(got))
	}
}

func bytesReplaceOnce(in []byte, old, new string) []byte {
	return []byte(stringReplaceOnce(string(in), old, new))
}

func stringReplaceOnce(s, old, new string) string {
	for i := 0; i+len(old) <= len(s); i++ {
		if s[i:i+len(old)] == old {
			return s[:i] + new + s[i+len(old):]
		}
	}
	return s
}

// 3. 截断末行字节：隔离内容等于被切尾部、活动日志精确截断、之后可正确接续。
func TestQuarantineTruncatesAndResumes(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "events.jsonl")
	log := NewDurableLog(path)
	appendN(t, log, 3, "k")

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	// 切掉落行末尾 4 字节（含换行），第三条变成无换行的半行。
	truncated := raw[:len(raw)-4]
	if err := os.WriteFile(path, truncated, 0o644); err != nil {
		t.Fatal(err)
	}

	got, trustedOffset, boundary, err := NewDurableLog(path).Scan()
	if err != nil || boundary == nil {
		t.Fatalf("expected corrupt-tail boundary, got=%v err=%v", boundary, err)
	}
	if len(got) != 2 {
		t.Fatalf("want 2 trusted, got %d", len(got))
	}
	// 隔离范围是坏行起始偏移到 EOF 的整条坏行。
	wantTail := truncated[boundary.Offset:]

	report, err := Quarantine(path, *boundary, 123)
	if err != nil {
		t.Fatal(err)
	}
	quarantined, err := os.ReadFile(report.Path)
	if err != nil {
		t.Fatal(err)
	}
	if string(quarantined) != string(wantTail) {
		t.Fatalf("quarantined bytes %q != corrupt tail %q", quarantined, wantTail)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != trustedOffset {
		t.Fatalf("active log size %d != trusted offset %d", info.Size(), trustedOffset)
	}
	if report.Rows != 1 || report.Bytes != len(wantTail) || report.Reason == "" {
		t.Fatalf("quarantine report not populated: %+v", report)
	}

	// 隔离后新事件应从 seq=3 正确接续。
	log.resetTip()
	env, err := log.Append(mkEvent("k", "after"))
	if err != nil {
		t.Fatalf("append after quarantine: %v", err)
	}
	if env.Seq != 3 {
		t.Fatalf("expected resume seq 3, got %d", env.Seq)
	}
}

// 4. seq 跳号判损坏。
func TestDurableLogSeqGap(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.jsonl")
	e1 := mkEvent("k", "a")
	e2 := mkEvent("k", "b")
	env1 := NewEnvelope(1, "", e1)
	// 人为构造 seq=3（期望 seq=2）。
	env2 := NewEnvelope(3, env1.Digest, e2)
	for _, env := range []Envelope{env1, env2} {
		line, _ := json.Marshal(env)
		line = append(line, '\n')
		f, _ := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
		_, _ = f.Write(line)
		_ = f.Close()
	}
	got, _, boundary, err := NewDurableLog(path).Scan()
	if err != nil {
		t.Fatal(err)
	}
	if boundary == nil || boundary.Reason != "seq" {
		t.Fatalf("expected seq-gap boundary, got %v", boundary)
	}
	if len(got) != 1 {
		t.Fatalf("want prefix of 1, got %d", len(got))
	}
}

// 5. 检查点原子读写与自摘要；篡改字段即失效。
func TestCheckpointVerify(t *testing.T) {
	cp := NewCheckpointFile(filepath.Join(t.TempDir(), "checkpoint.json"))
	env := NewEnvelope(5, "prev", mkEvent("k", "p"))
	rec := NewCheckpointRecord(5, env, 999)
	if !rec.Verify() {
		t.Fatal("fresh checkpoint should verify")
	}
	if err := cp.Save(rec); err != nil {
		t.Fatal(err)
	}
	loaded, present, err := cp.Load()
	if err != nil || !present || !loaded.Verify() {
		t.Fatalf("load present=%v verify=%v err=%v", present, loaded.Verify(), err)
	}
	loaded.ConfirmedSeq = 4
	if loaded.Verify() {
		t.Fatal("tampered checkpoint must not verify")
	}
}

// 6&7. 检查点与可信边界的判定：缺失、命中、超前。
func TestRecoverJournalCheckpointWatermark(t *testing.T) {
	dir := t.TempDir()
	log := NewDurableLog(filepath.Join(dir, "events.jsonl"))
	cp := NewCheckpointFile(filepath.Join(dir, "checkpoint.json"))
	envelopes := appendN(t, log, 3, "k")

	// 无检查点：水位回 0，并标注初始化复核。
	report, got, err := RecoverJournal(dir, log, cp, 1)
	if err != nil {
		t.Fatal(err)
	}
	if report.ConfirmedSeq != 0 || !report.ManualReview || len(got) != 3 {
		t.Fatalf("unexpected missing-checkpoint report: %+v", report)
	}

	// 写入命中链尖的检查点：水位=3，无需复核。
	if err := cp.Save(NewCheckpointRecord(3, envelopes[2], 2)); err != nil {
		t.Fatal(err)
	}
	report, _, err = RecoverJournal(dir, log, cp, 3)
	if err != nil {
		t.Fatal(err)
	}
	if !report.Checkpoint.Valid || report.ConfirmedSeq != 3 || report.ManualReview {
		t.Fatalf("expected valid checkpoint at seq 3, got %+v", report)
	}

	// 超前检查点：拒绝、回零、留证。
	if err := cp.Save(NewCheckpointRecord(3, envelopes[2], 4)); err != nil {
		t.Fatal(err)
	}
	ahead := NewCheckpointRecord(99, envelopes[2], 5)
	if err := cp.Save(ahead); err != nil {
		t.Fatal(err)
	}
	report, _, err = RecoverJournal(dir, log, cp, 6)
	if err != nil {
		t.Fatal(err)
	}
	if report.ConfirmedSeq != 0 || !report.ManualReview || report.Checkpoint.Valid {
		t.Fatalf("expected rejected ahead checkpoint, got %+v", report)
	}
	matches, _ := filepath.Glob(filepath.Join(dir, "checkpoint.rejected.*.json"))
	if len(matches) == 0 {
		t.Fatal("expected a rejected checkpoint file")
	}
}

// 8. 跨代次：第二代尾部损坏后，代次分组只见第一代全组与第二代前缀，边界外不入组。
func TestReplayByGenerationRespectsBoundary(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.jsonl")
	log := NewDurableLog(path)
	genA := model.NewCampaign("a")
	genB := model.NewCampaign("b")
	var envs []Envelope
	for _, e := range []model.Event{
		model.NewEvent("k", genA, "1"),
		model.NewEvent("k", genA, "2"),
		model.NewEvent("k", genB, "3"),
		model.NewEvent("k", genB, "4"),
	} {
		env, err := log.Append(e)
		if err != nil {
			t.Fatal(err)
		}
		envs = append(envs, env)
	}
	// 破坏最后一条（genB 的第二事件）。
	raw, _ := os.ReadFile(path)
	_ = os.WriteFile(path, raw[:len(raw)-3], 0o644)

	got, _, boundary, err := log.Scan()
	if err != nil || boundary == nil {
		t.Fatalf("expected boundary, got %v err %v", boundary, err)
	}
	groups := ReplayByGeneration(EnvelopeEvents(got))
	if len(groups[genA.Revision.String()]) != 2 {
		t.Fatalf("gen A should retain 2 events, got %d", len(groups[genA.Revision.String()]))
	}
	if len(groups[genB.Revision.String()]) != 1 {
		t.Fatalf("gen B should retain only the prefix event, got %d", len(groups[genB.Revision.String()]))
	}
	if _, leaked := groups[envs[3].Event.Revision.String()]; leaked {
		// genB 的 revision 与被损事件相同，故不应多出第 2 条。
	}
}

// 9. legacy 裸事件文件：备份后迁移为链，计数正确，坏尾被隔离，.bak 留证。
func TestRecoverJournalMigratesLegacy(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "events.jsonl")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	enc := json.NewEncoder(f)
	c := model.NewCampaign("g")
	for i := 0; i < 3; i++ {
		if err := enc.Encode(model.NewEvent("k", c, "legacy")); err != nil {
			t.Fatal(err)
		}
	}
	_, _ = f.Write([]byte("{broken json tail"))
	_ = f.Close()

	log := NewDurableLog(path)
	cp := NewCheckpointFile(filepath.Join(dir, "checkpoint.json"))
	report, got, err := RecoverJournal(dir, log, cp, 7)
	if err != nil {
		t.Fatal(err)
	}
	if report.Format != FormatLegacyMigrated || report.LegacyMigrated != 3 {
		t.Fatalf("migration report wrong: %+v", report)
	}
	if len(got) != 3 {
		t.Fatalf("want 3 migrated chain events, got %d", len(got))
	}
	if report.Quarantined == nil {
		t.Fatal("corrupt legacy tail should be quarantined")
	}
	baks, _ := filepath.Glob(filepath.Join(dir, "events.jsonl.legacy.*.bak"))
	if len(baks) != 1 {
		t.Fatalf("expected one legacy backup, got %v", baks)
	}
	// 迁移后链必须可验。
	if _, _, boundary, err := log.Scan(); err != nil || boundary != nil {
		t.Fatalf("migrated chain must verify: boundary=%v err=%v", boundary, err)
	}
}

// 10. proof.durable 绑定持久日志后真正落盘，重建 Store 后 RequireProof 通过。
func TestPersistProofDurableAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "events.jsonl")
	log := NewDurableLog(path)
	store := New()
	store.Bind(log)

	c := model.NewCampaign("g")
	proof := model.Proof{ID: model.NewCampaign("p").ID, Revision: c.Revision, Source: "s", At: 1, Durable: true}
	if err := PersistProof(store, proof, c); err != nil {
		t.Fatal(err)
	}

	envelopes, _, boundary, err := NewDurableLog(path).Scan()
	if err != nil || boundary != nil || len(envelopes) != 1 {
		t.Fatalf("proof event not durable on disk: n=%d boundary=%v err=%v", len(envelopes), boundary, err)
	}

	rebuilt := New()
	rebuilt.LoadEnvelopes(envelopes)
	if err := RequireProof(rebuilt, c.Revision.String()); err != nil {
		t.Fatalf("rebuilt store should hold durable proof: %v", err)
	}
	if err := RequireProofConfirmed(rebuilt, c.Revision.String(), 0); err == nil {
		t.Fatal("proof beyond confirmed watermark must not count as confirmed")
	}
	if err := RequireProofConfirmed(rebuilt, c.Revision.String(), 1); err != nil {
		t.Fatalf("proof within watermark should be confirmed: %v", err)
	}
}
