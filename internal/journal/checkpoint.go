package journal

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"github.com/wyw14/solo-039/internal/model"
)

// Checkpoint 是仅在内存中由单条事件临时构造的旧式检查点，不落盘、不链接前序。
//
// Deprecated: 持久化与链绑定请使用 CheckpointRecord 与 CheckpointFile。
// 该类型仅保留给 operational audit 的兼容性探针使用。
type Checkpoint struct {
	EventID  string
	Revision string
	Epoch    uint64
	Digest   string
}

// NewCheckpoint 构造旧式内存检查点。
//
// Deprecated: 使用 NewCheckpointRecord。
func NewCheckpoint(event model.Event) Checkpoint {
	raw := event.Kind + "|" + event.Payload + "|" + event.Revision.String()
	hash := sha256.Sum256([]byte(raw))
	return Checkpoint{EventID: event.ID.String(), Revision: event.Revision.String(), Epoch: event.Epoch, Digest: hex.EncodeToString(hash[:])}
}

func (c Checkpoint) Valid(event model.Event) bool {
	return c.EventID == event.ID.String() && c.Revision == event.Revision.String() && c.Epoch == event.Epoch && c.Digest != ""
}

func ValidateSequence(events []model.Event) error {
	var epoch uint64
	for _, event := range events {
		if event.Epoch < epoch {
			return errors.New("event epoch regressed")
		}
		epoch = event.Epoch
	}
	return nil
}

func Latest(events []model.Event) (model.Event, bool) {
	if len(events) == 0 {
		return model.Event{}, false
	}
	return events[len(events)-1], true
}
