package journal

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/wyw14/solo-039/internal/model"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// FileLog is an append-only event log where each line is a chained Record.
// Corruption at the tail (torn write, tampered line) never invalidates the
// verified prefix: Scan locates the first broken line, Isolate moves it and
// every following byte aside, and the log is truncated back to the boundary.
type FileLog struct {
	mu        sync.Mutex
	path      string
	batch     uint64
	ready     bool
	lastSeq   uint64
	lastChain string
	headRec   Record
	headEvent model.Event
	hasHead   bool
}

func NewFileLog(path string) *FileLog {
	return &FileLog{path: path, lastChain: GenZero}
}

func (f *FileLog) Path() string { return f.path }

// Batch returns the current writer batch. Writers bump the batch when a new
// process incarnation starts so recovery can tell generations of replay apart.
func (f *FileLog) Batch() uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.batch
}

// SetBatch is used by recovery to resume the batch counter from a checkpoint.
func (f *FileLog) SetBatch(batch uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if batch > f.batch {
		f.batch = batch
	}
}

// BeginBatch starts a new writer batch and returns its number.
func (f *FileLog) BeginBatch() uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.batch++
	return f.batch
}

// ScanResult is the outcome of walking the log from line one.
type ScanResult struct {
	Records   []Record
	Events    []model.Event
	Tail      *TailError
	TailBytes [][]byte
	// rawLines are the exact on-disk bytes (incl. newline) of each verified
	// record; used to truncate at the byte-accurate trusted prefix.
	rawLines [][]byte
}

// Trusted reports whether the whole file verified.
func (r ScanResult) Trusted() bool { return r.Tail == nil }

// Last returns the final verified record and the decoded event.
func (r ScanResult) Last() (Record, model.Event, bool) {
	if len(r.Records) == 0 {
		return Record{}, model.Event{}, false
	}
	return r.Records[len(r.Records)-1], r.Events[len(r.Events)-1], true
}

func tailError(line int, seq uint64, format string, args ...any) *TailError {
	return &TailError{Line: line, Seq: seq, Reason: fmt.Sprintf(format, args...)}
}

// scan reads the file and verifies every record against the chain. It is
// lock-free internally; callers hold f.mu.
func (f *FileLog) scan() (ScanResult, error) {
	result := ScanResult{Records: make([]Record, 0, 64), Events: make([]model.Event, 0, 64)}
	file, err := os.Open(f.path)
	if errors.Is(err, os.ErrNotExist) {
		return result, nil
	}
	if err != nil {
		return result, err
	}
	defer file.Close()

	reader := bufio.NewReaderSize(file, 1<<20)
	expectedSeq := uint64(1)
	prev := GenZero
	lineNo := 0
	for {
		raw, readErr := reader.ReadBytes('\n')
		if len(raw) > 0 {
			lineNo++
			trimmed := bytes.TrimRight(raw, "\r\n")
			if len(bytes.TrimSpace(trimmed)) == 0 {
				// A trailing blank line is harmless but cannot carry a record;
				// anything after a gap is treated as corrupt tail.
				if readErr == io.EOF {
					break
				}
				result.Tail = tailError(lineNo, expectedSeq, "blank line inside log")
				result.TailBytes = append(result.TailBytes, raw)
			} else if result.Tail == nil {
				var record Record
				if err := json.Unmarshal(trimmed, &record); err != nil {
					result.Tail = tailError(lineNo, expectedSeq, "record is not valid json: %v", err)
					result.TailBytes = append(result.TailBytes, raw)
				} else if record.Seq != expectedSeq {
					result.Tail = tailError(lineNo, record.Seq, "seq gap: expected %d", expectedSeq)
					result.TailBytes = append(result.TailBytes, raw)
				} else if record.Prev != prev {
					result.Tail = tailError(lineNo, record.Seq, "chain link mismatch")
					result.TailBytes = append(result.TailBytes, raw)
				} else if event, err := record.Verify(); err != nil {
					result.Tail = tailError(lineNo, record.Seq, "%v", err)
					result.TailBytes = append(result.TailBytes, raw)
				} else {
					lineCopy := append([]byte(nil), raw...)
					result.Records = append(result.Records, record)
					result.Events = append(result.Events, event)
					result.rawLines = append(result.rawLines, lineCopy)
					prev = record.Digest
					expectedSeq++
				}
			} else {
				result.TailBytes = append(result.TailBytes, raw)
			}
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			if result.Tail == nil {
				return result, readErr
			}
			break
		}
	}
	return result, nil
}

// Isolate moves the corrupt tail described by result into a sibling quarantine
// file and truncates the log back to the verified prefix. The returned path is
// empty when there was nothing to isolate.
func (f *FileLog) Isolate(result ScanResult) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.isolateLocked(result)
}

func (f *FileLog) isolateLocked(result ScanResult) (string, error) {
	if result.Tail == nil || len(result.TailBytes) == 0 {
		f.adoptLocked(result)
		return "", nil
	}
	quarantine := fmt.Sprintf("%s.quarantine.%d", f.path, result.Tail.Seq)
	if err := os.MkdirAll(filepath.Dir(f.path), 0755); err != nil {
		return "", err
	}
	out, err := os.OpenFile(quarantine, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		return "", err
	}
	writer := bufio.NewWriter(out)
	for _, line := range result.TailBytes {
		if _, err := writer.Write(line); err != nil {
			out.Close()
			return "", err
		}
	}
	if err := writer.Flush(); err != nil {
		out.Close()
		return "", err
	}
	if err := out.Sync(); err != nil {
		out.Close()
		return "", err
	}
	if err := out.Close(); err != nil {
		return "", err
	}

	trustedBytes := 0
	for _, line := range result.rawLines {
		trustedBytes += len(line)
	}
	file, err := os.OpenFile(f.path, os.O_WRONLY, 0644)
	if err != nil {
		return "", err
	}
	if err := file.Truncate(int64(trustedBytes)); err != nil {
		file.Close()
		return "", err
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return "", err
	}
	if err := file.Close(); err != nil {
		return "", err
	}
	f.adoptLocked(ScanResult{Records: result.Records, Events: result.Events})
	return quarantine, nil
}

func (f *FileLog) adoptLocked(result ScanResult) {
	if len(result.Records) > 0 {
		i := len(result.Records) - 1
		f.lastSeq = result.Records[i].Seq
		f.lastChain = result.Records[i].Digest
		f.headRec = result.Records[i]
		f.headEvent = result.Events[i]
		f.hasHead = true
	} else {
		f.lastSeq = 0
		f.lastChain = GenZero
		f.headRec = Record{}
		f.headEvent = model.Event{}
		f.hasHead = false
	}
	f.ready = true
}

// prepareLocked loads and self-heals the log before the first write.
func (f *FileLog) prepareLocked() error {
	if f.ready {
		return nil
	}
	result, err := f.scan()
	if err != nil {
		return err
	}
	if result.Tail != nil {
		if _, err := f.isolateLocked(result); err != nil {
			return fmt.Errorf("isolate corrupt tail before append: %w", err)
		}
		return nil
	}
	f.adoptLocked(result)
	return nil
}

// Append seals an event into the chain, fsyncs it, and reads it back from disk
// to prove the durable line verifies. A torn or unreadable write is quarantined
// and truncated so the log never advances past an unverified record.
func (f *FileLog) Append(event model.Event) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.prepareLocked(); err != nil {
		return err
	}
	if f.batch == 0 {
		f.batch = 1
	}
	record, err := NewRecord(event, f.lastSeq+1, f.batch, f.lastChain)
	if err != nil {
		return err
	}
	line, err := json.Marshal(record)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(f.path), 0755); err != nil {
		return err
	}

	offset := int64(-1)
	if info, statErr := os.Stat(f.path); statErr == nil {
		offset = info.Size()
	}
	file, err := os.OpenFile(f.path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	if _, err := file.Write(append(line, '\n')); err != nil {
		file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return f.rollbackLocked(offset, line, "fsync failed: "+err.Error())
	}
	if err := file.Close(); err != nil {
		return err
	}

	// Read the just-persisted line back from disk and verify it before the
	// record is allowed to extend the in-memory chain.
	verified, persistedEvent, verifyErr := f.readbackLocked(offset, record)
	if verifyErr != nil {
		return f.rollbackLocked(offset, line, verifyErr.Error())
	}
	f.lastSeq = verified.Seq
	f.lastChain = verified.Digest
	f.headRec = verified
	f.headEvent = persistedEvent
	f.hasHead = true
	return nil
}

func (f *FileLog) readbackLocked(offset int64, record Record) (Record, model.Event, error) {
	file, err := os.Open(f.path)
	if err != nil {
		return Record{}, model.Event{}, err
	}
	defer file.Close()
	if offset >= 0 {
		if _, err := file.Seek(offset, io.SeekStart); err != nil {
			return Record{}, model.Event{}, err
		}
	}
	raw, err := bufio.NewReader(file).ReadBytes('\n')
	if err != nil && !errors.Is(err, io.EOF) {
		return Record{}, model.Event{}, err
	}
	var persisted Record
	if err := json.Unmarshal(bytes.TrimRight(raw, "\r\n"), &persisted); err != nil {
		return Record{}, model.Event{}, fmt.Errorf("durable record failed readback: %w", err)
	}
	event, err := persisted.Verify()
	if err != nil {
		return Record{}, model.Event{}, fmt.Errorf("durable record failed verification: %w", err)
	}
	if persisted.Digest != record.Digest || persisted.Seq != record.Seq {
		return Record{}, model.Event{}, errors.New("durable record does not match sealed record")
	}
	return persisted, event, nil
}

// rollbackLocked quarantines a failed write's bytes and truncates back to the
// last trusted offset. The in-memory chain head is left untouched.
func (f *FileLog) rollbackLocked(offset int64, attempted []byte, reason string) error {
	if offset >= 0 {
		if raw, err := os.ReadFile(f.path); err == nil && int64(len(raw)) > offset {
			bad := raw[offset:]
			if len(bytes.TrimSpace(bad)) > 0 {
				quarantine := fmt.Sprintf("%s.quarantine.write.%d", f.path, f.lastSeq+1)
				_ = os.WriteFile(quarantine, append(append(bad, '\n'), attempted...), 0644)
			}
			if file, err := os.OpenFile(f.path, os.O_WRONLY, 0644); err == nil {
				_ = file.Truncate(offset)
				_ = file.Sync()
				_ = file.Close()
			}
		}
	}
	return fmt.Errorf("event append not durable: %s", reason)
}

// Read returns every event in the verified prefix. When a corrupt tail exists
// the trusted events are returned together with a *TailError describing it;
// callers can then Isolate the tail.
func (f *FileLog) Read() ([]model.Event, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	result, err := f.scan()
	if err != nil {
		return nil, err
	}
	f.adoptLocked(result)
	if result.Tail != nil {
		// Keep the writer unready so the next Append isolates the tail instead
		// of appending behind it.
		f.ready = false
		return result.Events, result.Tail
	}
	return result.Events, nil
}

// Scan verifies the full chain without mutating the file.
func (f *FileLog) Scan() (ScanResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	result, err := f.scan()
	if err == nil {
		f.adoptLocked(result)
	}
	return result, err
}

// Head returns the verified chain head without touching disk once loaded.
func (f *FileLog) Head() (seq uint64, digest string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lastSeq, f.lastChain
}

// HeadRecord returns the last verified record and event, ready to bind a
// checkpoint to. False is returned for an empty log.
func (f *FileLog) HeadRecord() (Record, model.Event, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.hasHead {
		return Record{}, model.Event{}, false
	}
	return f.headRec, f.headEvent, true
}

// Checkpoint seals a new checkpoint at the current chain head, carrying forward
// the confirmed event ids, generations and manual reviews supplied by the
// caller. Returns the zero checkpoint for an empty log.
func (f *FileLog) Checkpoint(confirmedIDs, generations []string, manual []ManualReview) (Checkpoint, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.hasHead {
		return Checkpoint{}, nil
	}
	return BoundCheckpoint(f.headRec, f.headEvent, confirmedIDs, generations, manual)
}
