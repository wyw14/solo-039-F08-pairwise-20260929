package journal

import (
	"encoding/json"
	"os"
	"strconv"
)

// 日志格式。
const (
	FormatEmpty          = "empty"
	FormatChainV1        = "chain-v1"
	FormatLegacyMigrated = "legacy-migrated"
)

// CheckpointStatus 描述持久检查点的核验结果。
type CheckpointStatus struct {
	Present bool   `json:"present"`
	Valid   bool   `json:"valid"`
	Seq     uint64 `json:"seq"`
	Issue   string `json:"issue"`
}

// RecoveryReport 是一次启动恢复的结论：可信边界、确认水位、隔离与人工复核标记。
type RecoveryReport struct {
	DataDir              string            `json:"data_dir"`
	Format               string            `json:"format"`
	TrustedSeq           uint64            `json:"trusted_seq"`
	TrustedEvents        int               `json:"trusted_events"`
	ConfirmedSeq         uint64            `json:"confirmed_seq"`
	Checkpoint           CheckpointStatus  `json:"checkpoint"`
	ReplayedEventIDs     []string          `json:"replayed_event_ids"`
	ReplayedByGeneration map[string]int    `json:"replayed_by_generation"`
	Quarantined          *QuarantineReport `json:"quarantined"`
	LegacyMigrated       int               `json:"legacy_migrated"`
	Snapshot             string            `json:"snapshot"`
	ManualReview         bool              `json:"manual_review"`
	ReviewReasons        []string          `json:"review_reasons"`
	IncidentID           string            `json:"incident_id"`
}

func (r *RecoveryReport) addReason(reason string) {
	r.ManualReview = true
	r.ReviewReasons = append(r.ReviewReasons, reason)
}

// AddReviewReason 记录一条需要人工复核的原因（跨包使用）。
func (r *RecoveryReport) AddReviewReason(reason string) { r.addReason(reason) }

// RecoverJournal 是 journal 层的启动恢复编排（不含设备知识）：
// 探测格式 →（旧格式则备份后迁移为链）→ 逐行验链定位可信边界 →
// 隔离损坏尾部 → 校验持久检查点得出确认水位。
// I/O 错误为致命 error；内容损坏不致命，已隔离并在报告标记人工复核。
func RecoverJournal(dataDir string, log *DurableLog, cp *CheckpointFile, now int64) (*RecoveryReport, []Envelope, error) {
	report := &RecoveryReport{
		DataDir:              dataDir,
		Format:               FormatEmpty,
		ReplayedEventIDs:     []string{},
		ReplayedByGeneration: map[string]int{},
		ReviewReasons:        []string{},
	}

	format, err := detectFormat(log.path)
	if err != nil {
		return nil, nil, err
	}

	switch format {
	case FormatEmpty:
		// 全新目录：空链，genesis 将从 seq=1 开始。
	case FormatLegacyMigrated:
		migrated, qerr := migrateLegacy(log.path, now)
		if qerr != nil {
			return nil, nil, qerr
		}
		report.Format = FormatLegacyMigrated
		report.LegacyMigrated = migrated.count
		if migrated.quarantine != nil {
			report.Quarantined = migrated.quarantine
			report.addReason("tail-quarantined: " + migrated.quarantine.Reason + " at line " + strconv.Itoa(migrated.quarantine.StartLine))
		}
		report.addReason("legacy-log-migrated: pre-chain events trusted on first use (TOFU)")
		log.resetTip()
	default: // chain-v1（含首行即损坏、交给扫描判边界的情况）
		report.Format = FormatChainV1
	}

	// 扫描（链文件或刚迁移好的文件）验链，定位并隔离损坏尾部。
	envelopes, _, boundary, err := log.Scan()
	if err != nil {
		return nil, nil, err
	}
	if boundary != nil {
		// 迁移路径若已隔离过 legacy 坏尾，正常情况下这里不会再有边界。
		q, qerr := Quarantine(log.path, *boundary, now)
		if qerr != nil {
			return nil, nil, qerr
		}
		report.Quarantined = q
		report.addReason("tail-quarantined: " + boundary.Reason + " at line " + strconv.Itoa(boundary.Line))
		envelopes, _, _, err = log.Scan()
		if err != nil {
			return nil, nil, err
		}
		log.resetTip()
	}

	if n := len(envelopes); n > 0 {
		report.TrustedSeq = envelopes[n-1].Seq
		report.TrustedEvents = n
		for revision, events := range ReplayByGeneration(EnvelopeEvents(envelopes)) {
			report.ReplayedByGeneration[revision] = len(events)
		}
	}

	// 校验持久检查点，得出确认水位。
	confirmed, status, err := resolveCheckpoint(cp, envelopes, now)
	if err != nil {
		return nil, nil, err
	}
	report.Checkpoint = status
	report.ConfirmedSeq = confirmed
	if status.Issue != "" {
		report.addReason(status.Issue)
	}

	return report, envelopes, nil
}

// resolveCheckpoint 按检查点判定矩阵返回确认水位与状态。
func resolveCheckpoint(cp *CheckpointFile, trusted []Envelope, now int64) (uint64, CheckpointStatus, error) {
	rec, present, err := cp.Load()
	if err != nil {
		// 检查点文件存在但解析失败：不可信，改名留证后按"无检查点"处理。
		rejected, rerr := cp.Reject(now)
		if rerr != nil {
			return 0, CheckpointStatus{}, rerr
		}
		issue := "checkpoint-unreadable: rejected to " + rejected
		return 0, CheckpointStatus{Present: true, Valid: false, Issue: issue}, nil
	}
	if !present {
		status := CheckpointStatus{Present: false, Valid: false}
		if len(trusted) > 0 {
			status.Issue = "checkpoint-initialized: no durable checkpoint, replaying trusted prefix"
		}
		return 0, status, nil
	}
	if !rec.Verify() {
		rejected, rerr := cp.Reject(now)
		if rerr != nil {
			return 0, CheckpointStatus{}, rerr
		}
		return 0, CheckpointStatus{Present: true, Valid: false, Issue: "checkpoint-invalid: digest mismatch, rejected to " + rejected}, nil
	}
	if rec.ConfirmedSeq > uint64(len(trusted)) {
		rejected, rerr := cp.Reject(now)
		if rerr != nil {
			return 0, CheckpointStatus{}, rerr
		}
		return 0, CheckpointStatus{Present: true, Valid: false, Seq: rec.ConfirmedSeq, Issue: "checkpoint-ahead: confirmed seq beyond trusted log, rejected to " + rejected}, nil
	}
	if rec.ConfirmedSeq > 0 {
		watermark := trusted[rec.ConfirmedSeq-1]
		if rec.PrefixDigest != watermark.Digest || rec.EventID != watermark.Event.ID.String() {
			rejected, rerr := cp.Reject(now)
			if rerr != nil {
				return 0, CheckpointStatus{}, rerr
			}
			return 0, CheckpointStatus{Present: true, Valid: false, Seq: rec.ConfirmedSeq, Issue: "checkpoint-mismatch: prefix digest not bound to event chain, rejected to " + rejected}, nil
		}
	}
	return rec.ConfirmedSeq, CheckpointStatus{Present: true, Valid: true, Seq: rec.ConfirmedSeq}, nil
}

// detectFormat 通过首行判定日志是链式信封、旧版裸事件还是空文件。
func detectFormat(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return FormatEmpty, nil
		}
		return "", err
	}
	if len(data) == 0 {
		return FormatEmpty, nil
	}
	// 取第一行做探测。
	first := data
	for i, b := range data {
		if b == '\n' {
			first = data[:i]
			break
		}
	}
	var env Envelope
	if err := json.Unmarshal(first, &env); err == nil && env.Digest != "" && env.Seq > 0 {
		return FormatChainV1, nil
	}
	return FormatLegacyMigrated, nil
}

type legacyMigration struct {
	count      int
	quarantine *QuarantineReport
}

// migrateLegacy 备份完整旧日志后，把可解析前缀重写为链式格式；坏尾走隔离。
func migrateLegacy(path string, now int64) (legacyMigration, error) {
	result := legacyMigration{}

	// 1. 完整备份原文件留证。
	original, err := os.ReadFile(path)
	if err != nil {
		return result, err
	}
	backup := path + ".legacy." + strconv.FormatInt(now, 10) + ".bak"
	if err := writeFileAtomic(backup, original); err != nil {
		return result, err
	}

	// 2. 容忍式读取旧前缀，坏尾先隔离（含 truncate）。
	legacy := NewFileLog(path)
	lines, _, boundary, err := legacy.ReadLegacyPrefix()
	if err != nil {
		return result, err
	}
	if boundary != nil {
		q, err := Quarantine(path, *boundary, now)
		if err != nil {
			return result, err
		}
		result.quarantine = q
	}

	// 3. 把可信前缀重写为链式信封。
	var prev string
	var seq uint64
	content := make([]byte, 0, len(original))
	for _, line := range lines {
		seq++
		env := NewEnvelope(seq, prev, line.Event)
		raw, err := json.Marshal(env)
		if err != nil {
			return result, err
		}
		content = append(content, raw...)
		content = append(content, '\n')
		prev = env.Digest
	}
	if err := writeFileAtomic(path, content); err != nil {
		return result, err
	}
	result.count = len(lines)
	return result, nil
}
