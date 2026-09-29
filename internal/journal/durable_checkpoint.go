package journal

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strconv"
)

const checkpointVersion = 1

// CheckpointRecord 是与事件链绑定的持久检查点。
// ConfirmedSeq 是连续前缀确认水位：seq <= ConfirmedSeq 的事件，其副作用
// 均已确认完成，重启后不得重放；其后的可信事件才允许幂等重放一次。
type CheckpointRecord struct {
	Version      int    `json:"v"`
	ConfirmedSeq uint64 `json:"confirmed_seq"`
	EventID      string `json:"event_id"`
	Revision     string `json:"revision"`
	Epoch        uint64 `json:"epoch"`
	PrefixDigest string `json:"prefix_digest"` // 水位处信封的链摘要
	UpdatedAt    int64  `json:"updated_at"`
	Digest       string `json:"digest"` // 除自身外字段的摘要
}

// checkpointDigest 覆盖除 Digest 外的全部字段，字段顺序固定。
func checkpointDigest(r CheckpointRecord) string {
	return hashFields(
		"v"+strconv.Itoa(checkpointVersion),
		strconv.FormatUint(r.ConfirmedSeq, 10),
		r.EventID,
		r.Revision,
		strconv.FormatUint(r.Epoch, 10),
		r.PrefixDigest,
		strconv.FormatInt(r.UpdatedAt, 10),
	)
}

// NewCheckpointRecord 构造一条带自摘要的检查点。
func NewCheckpointRecord(confirmedSeq uint64, env Envelope, now int64) CheckpointRecord {
	rec := CheckpointRecord{
		Version:      checkpointVersion,
		ConfirmedSeq: confirmedSeq,
		EventID:      env.Event.ID.String(),
		Revision:     env.Event.Revision.String(),
		Epoch:        env.Event.Epoch,
		PrefixDigest: env.Digest,
		UpdatedAt:    now,
	}
	rec.Digest = checkpointDigest(rec)
	return rec
}

// Verify 校验版本、非零水位与自摘要。
func (r CheckpointRecord) Verify() bool {
	if r.Version != checkpointVersion || r.ConfirmedSeq == 0 {
		return false
	}
	if r.Digest == "" {
		return false
	}
	return r.Digest == checkpointDigest(r)
}

// CheckpointFile 原子读写检查点文件。
type CheckpointFile struct{ path string }

func NewCheckpointFile(path string) *CheckpointFile { return &CheckpointFile{path: path} }

func (f *CheckpointFile) Path() string { return f.path }

func (f *CheckpointFile) Save(rec CheckpointRecord) error {
	data, err := json.MarshalIndent(rec, "", "  ")
	if err != nil {
		return err
	}
	return writeFileAtomic(f.path, data)
}

// Load 读取检查点。present=false 表示文件缺失（全新场景，非错误）。
func (f *CheckpointFile) Load() (rec CheckpointRecord, present bool, err error) {
	data, err := os.ReadFile(f.path)
	if errors.Is(err, os.ErrNotExist) {
		return CheckpointRecord{}, false, nil
	}
	if err != nil {
		return CheckpointRecord{}, false, err
	}
	if err := json.Unmarshal(data, &rec); err != nil {
		return CheckpointRecord{}, true, err
	}
	return rec, true, nil
}

// Reject 把不可信的检查点改名留证（不删除），返回新路径。
func (f *CheckpointFile) Reject(now int64) (string, error) {
	if _, err := os.Stat(f.path); errors.Is(err, os.ErrNotExist) {
		return "", nil
	}
	dir := filepath.Dir(f.path)
	rejected := filepath.Join(dir, "checkpoint.rejected."+strconv.FormatInt(now, 10)+".json")
	if err := os.Rename(f.path, rejected); err != nil {
		return "", err
	}
	if err := fsyncDir(dir); err != nil {
		return "", err
	}
	return rejected, nil
}
