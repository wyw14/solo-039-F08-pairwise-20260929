package journal

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/wyw14/solo-039/internal/model"
)

// TailBoundary 指向第一条不可信行：其之前为可信前缀，从 Offset 到 EOF 为可疑尾部。
type TailBoundary struct {
	Line   int    // 可疑尾部起始行（1 起）
	Offset int64  // 可疑尾部在文件中的字节偏移
	Reason string // decode | digest | chain | seq
}

func (b *TailBoundary) Error() string {
	return fmt.Sprintf("corrupt journal tail at line %d offset %d: %s", b.Line, b.Offset, b.Reason)
}

// DurableLog 是哈希链式 append-only 事件日志。每条事件单行 JSON 并 fsync。
type DurableLog struct {
	mu          sync.Mutex
	path        string
	lastSeq     uint64
	lastDigest  string
	initialized bool
}

func NewDurableLog(path string) *DurableLog {
	return &DurableLog{path: path}
}

func (l *DurableLog) Path() string { return l.path }

// Tip 返回当前链尖序号与摘要。
func (l *DurableLog) Tip() (uint64, string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.lastSeq, l.lastDigest
}

// loadTip 惰性从文件恢复链尖。文件存在未隔离的损坏尾部时拒绝追加，
// 强制先走恢复/隔离流程，避免把新事件接到坏尾之后。
func (l *DurableLog) loadTip() error {
	if l.initialized {
		return nil
	}
	envelopes, _, tail, err := scanPath(l.path)
	if err != nil {
		return err
	}
	if tail != nil {
		return tail
	}
	if n := len(envelopes); n > 0 {
		l.lastSeq = envelopes[n-1].Seq
		l.lastDigest = envelopes[n-1].Digest
	}
	l.initialized = true
	return nil
}

// Append 计算下一条链式信封，单行写入并 fsync，返回已落盘的信封。
func (l *DurableLog) Append(event model.Event) (Envelope, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.loadTip(); err != nil {
		return Envelope{}, err
	}
	if err := os.MkdirAll(filepath.Dir(l.path), 0o755); err != nil {
		return Envelope{}, err
	}
	env := NewEnvelope(l.lastSeq+1, l.lastDigest, event)
	data, err := json.Marshal(env)
	if err != nil {
		return Envelope{}, err
	}
	data = append(data, '\n')
	file, err := os.OpenFile(l.path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return Envelope{}, err
	}
	if _, err := file.Write(data); err != nil {
		file.Close()
		return Envelope{}, err
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return Envelope{}, err
	}
	if err := file.Close(); err != nil {
		return Envelope{}, err
	}
	l.lastSeq = env.Seq
	l.lastDigest = env.Digest
	return env, nil
}

// Scan 重放整份日志，返回可信前缀信封、可信前缀字节长度与第一条坏行边界。
// I/O 错误作为 error 返回；内容损坏不作为 error，而由 boundary 报告。
func (l *DurableLog) Scan() ([]Envelope, int64, *TailBoundary, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return scanPath(l.path)
}

// Reset 清空内存链尖缓存（截断/迁移后调用，下一次 Append 重新读盘）。
func (l *DurableLog) resetTip() {
	l.lastSeq = 0
	l.lastDigest = ""
	l.initialized = false
}

// scanPath 逐行扫描并验链，记录每行字节偏移以定位可信边界。
func scanPath(path string) ([]Envelope, int64, *TailBoundary, error) {
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return []Envelope{}, 0, nil, nil
	}
	if err != nil {
		return nil, 0, nil, err
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	envelopes := make([]Envelope, 0, 64)
	var offset int64
	var prevDigest string
	var wantSeq uint64 = 1
	lineNo := 0

	for {
		lineStart := offset
		line, readErr := reader.ReadBytes('\n')
		offset += int64(len(line))
		if len(line) > 0 {
			lineNo++
			// 容忍末行无换行符：读到 io.EOF 但有完整字节仍尝试解析。
			if readErr != nil && readErr != io.EOF {
				return nil, 0, nil, readErr
			}
			var env Envelope
			if jerr := json.Unmarshal(line, &env); jerr != nil {
				return envelopes, lineStart, &TailBoundary{Line: lineNo, Offset: lineStart, Reason: "decode"}, nil
			}
			if verr := env.Verify(prevDigest, wantSeq); verr != nil {
				reason := "digest"
				if errors.Is(verr, errChainBroken) {
					reason = "chain"
				} else if errors.Is(verr, errSeqGap) {
					reason = "seq"
				}
				return envelopes, lineStart, &TailBoundary{Line: lineNo, Offset: lineStart, Reason: reason}, nil
			}
			envelopes = append(envelopes, env)
			prevDigest = env.Digest
			wantSeq++
		}
		if readErr != nil {
			if readErr == io.EOF {
				break
			}
			return nil, 0, nil, readErr
		}
	}
	return envelopes, offset, nil, nil
}
