package journal

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
)

// CheckpointFile stores a sealed Checkpoint atomically. Save writes a temp
// file, fsyncs it, reads it back and re-verifies the seal before renaming it
// over the previous checkpoint, so a checkpoint on disk is always intact and
// bound to exactly what the caller sealed.
type CheckpointFile struct {
	path string
}

func NewCheckpointFile(path string) *CheckpointFile { return &CheckpointFile{path: path} }

func (f *CheckpointFile) Path() string { return f.path }

// Save validates, persists with readback verification, then atomically renames.
func (f *CheckpointFile) Save(cp Checkpoint) error {
	if err := cp.VerifySeal(); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(f.path), 0755); err != nil {
		return err
	}
	data, err := json.Marshal(cp)
	if err != nil {
		return err
	}
	temporary := f.path + ".tmp"
	if err := os.WriteFile(temporary, data, 0644); err != nil {
		return err
	}
	file, err := os.OpenFile(temporary, os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return err
	}
	if _, err := file.Seek(0, 0); err != nil {
		file.Close()
		return err
	}
	readBack, err := io.ReadAll(file)
	closeErr := file.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}

	// Verify the bytes that physically landed before publishing them.
	var durable Checkpoint
	if err := json.Unmarshal(readBack, &durable); err != nil {
		return err
	}
	if err := durable.VerifySeal(); err != nil {
		return err
	}
	if !bytes.Equal(readBack, data) || durable.Seq != cp.Seq || durable.TailDigest != cp.TailDigest {
		return errors.New("checkpoint durable readback does not match sealed checkpoint")
	}
	return os.Rename(temporary, f.path)
}

// Load reads the checkpoint and verifies its seal. A missing file yields the
// empty checkpoint. A tampered or otherwise unverifiable checkpoint is moved
// aside (never silently discarded) and returned with its error.
func (f *CheckpointFile) Load() (Checkpoint, error) {
	data, err := os.ReadFile(f.path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return Checkpoint{}, nil
		}
		return Checkpoint{}, err
	}
	var cp Checkpoint
	if err := json.Unmarshal(data, &cp); err != nil {
		return f.quarantine(data, err)
	}
	if err := cp.VerifySeal(); err != nil {
		return f.quarantine(data, err)
	}
	return cp, nil
}

func (f *CheckpointFile) quarantine(data []byte, cause error) (Checkpoint, error) {
	side := f.path + ".suspect"
	if writeErr := os.WriteFile(side, data, 0644); writeErr == nil {
		_ = os.Remove(f.path)
	}
	return Checkpoint{}, errors.New("checkpoint failed verification and was moved to " + side + ": " + cause.Error())
}
