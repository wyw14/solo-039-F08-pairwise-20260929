package journal

import (
	"encoding/json"
	"github.com/wyw14/solo-039/internal/model"
	"os"
)

type SnapshotFile struct{ path string }

func NewSnapshotFile(path string) *SnapshotFile { return &SnapshotFile{path: path} }

func (f *SnapshotFile) Save(snapshot model.Snapshot) error {
	data, err := json.Marshal(snapshot)
	if err != nil {
		return err
	}
	return writeFileAtomic(f.path, data)
}

func (f *SnapshotFile) Load() (model.Snapshot, error) {
	data, err := os.ReadFile(f.path)
	if err != nil {
		return model.Snapshot{}, err
	}
	var snapshot model.Snapshot
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return model.Snapshot{}, err
	}
	return snapshot, nil
}
