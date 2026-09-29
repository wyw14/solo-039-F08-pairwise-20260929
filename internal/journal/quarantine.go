package journal

import (
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strconv"
)

// QuarantineReport 记录被隔离的损坏尾部，供人工复核，绝不静默丢弃。
type QuarantineReport struct {
	Path        string `json:"path"`
	Source      string `json:"source"`
	StartLine   int    `json:"start_line"`
	StartOffset int64  `json:"start_offset"`
	Bytes       int    `json:"bytes"`
	Rows        int    `json:"rows"`
	Reason      string `json:"reason"`
	CreatedAt   int64  `json:"created_at"`
}

// Quarantine 把活动日志 [boundary.Offset, EOF) 的可疑字节复制到带时间戳的
// quarantine 文件并写 JSON 侧车，然后把活动日志精确截断到可信边界并 fsync。
func Quarantine(logPath string, boundary TailBoundary, now int64) (*QuarantineReport, error) {
	in, err := os.Open(logPath)
	if err != nil {
		return nil, err
	}
	if _, err := in.Seek(boundary.Offset, io.SeekStart); err != nil {
		in.Close()
		return nil, err
	}
	tail, err := io.ReadAll(in)
	in.Close()
	if err != nil {
		return nil, err
	}

	dir := filepath.Dir(logPath)
	base := filepath.Base(logPath)
	stamp := strconv.FormatInt(now, 10)
	qPath := filepath.Join(dir, base+".quarantine."+stamp+".log")
	rPath := filepath.Join(dir, base+".quarantine."+stamp+".report.json")

	// 隔离文件同样原子落盘，留作证据。
	if err := writeFileAtomic(qPath, tail); err != nil {
		return nil, err
	}

	report := &QuarantineReport{
		Path:        qPath,
		Source:      logPath,
		StartLine:   boundary.Line,
		StartOffset: boundary.Offset,
		Bytes:       len(tail),
		Rows:        countRows(tail),
		Reason:      boundary.Reason,
		CreatedAt:   now,
	}
	sidecar, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return nil, err
	}
	if err := writeFileAtomic(rPath, sidecar); err != nil {
		return nil, err
	}

	// 截断活动日志到可信前缀并 fsync 文件与目录。
	if err := os.Truncate(logPath, boundary.Offset); err != nil {
		return nil, err
	}
	active, err := os.OpenFile(logPath, os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	if err := active.Sync(); err != nil {
		active.Close()
		return nil, err
	}
	if err := active.Close(); err != nil {
		return nil, err
	}
	if err := fsyncDir(dir); err != nil {
		return nil, err
	}
	return report, nil
}

func countRows(data []byte) int {
	if len(data) == 0 {
		return 0
	}
	rows := 0
	for _, b := range data {
		if b == '\n' {
			rows++
		}
	}
	if data[len(data)-1] != '\n' {
		rows++ // 末尾未以换行结束的半行也算一行
	}
	return rows
}
