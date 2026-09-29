package journal

import (
	"bufio"
	"encoding/json"
	"errors"
	"github.com/wyw14/solo-039/internal/model"
	"io"
	"os"
	"path/filepath"
	"sync"
)

type FileLog struct {
	mu   sync.Mutex
	path string
}

func NewFileLog(path string) *FileLog { return &FileLog{path: path} }

func (f *FileLog) Append(event model.Event) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := os.MkdirAll(filepath.Dir(f.path), 0755); err != nil {
		return err
	}
	file, err := os.OpenFile(f.path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(file)
	if err := encoder.Encode(event); err != nil {
		file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return err
	}
	return file.Close()
}

func (f *FileLog) Read() ([]model.Event, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	file, err := os.Open(f.path)
	if errors.Is(err, os.ErrNotExist) {
		return []model.Event{}, nil
	}
	if err != nil {
		return nil, err
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	events := make([]model.Event, 0, 64)
	for scanner.Scan() {
		var event model.Event
		if err := json.Unmarshal(scanner.Bytes(), &event); err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return events, nil
}

func (f *FileLog) Path() string { return f.path }

// LegacyLine 是旧版裸事件文件中的一条记录及其字节偏移。
type LegacyLine struct {
	Event  model.Event
	Offset int64
}

// ReadLegacyPrefix 容忍式读取旧版裸 JSON 事件文件：逐行记录偏移，
// 遇到第一条坏行即停止，返回此前的可信前缀与坏行边界（无坏行时 boundary 为 nil）。
func (f *FileLog) ReadLegacyPrefix() ([]LegacyLine, int64, *TailBoundary, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	file, err := os.Open(f.path)
	if errors.Is(err, os.ErrNotExist) {
		return []LegacyLine{}, 0, nil, nil
	}
	if err != nil {
		return nil, 0, nil, err
	}
	defer file.Close()
	reader := bufio.NewReader(file)
	lines := make([]LegacyLine, 0, 64)
	var offset int64
	lineNo := 0
	for {
		lineStart := offset
		line, readErr := reader.ReadBytes('\n')
		offset += int64(len(line))
		if len(line) > 0 {
			lineNo++
			var event model.Event
			if jerr := json.Unmarshal(line, &event); jerr != nil {
				return lines, lineStart, &TailBoundary{Line: lineNo, Offset: lineStart, Reason: "decode"}, nil
			}
			lines = append(lines, LegacyLine{Event: event, Offset: lineStart})
		}
		if readErr != nil {
			if readErr == io.EOF {
				break
			}
			return nil, 0, nil, readErr
		}
	}
	return lines, offset, nil, nil
}
