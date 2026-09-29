package journal

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/wyw14/solo-039/internal/model"
)

// GenZero is the digest that seeds a record chain and marks an empty prefix.
const GenZero = "sha256:0000000000000000000000000000000000000000000000000000000000000000"

// Record is the durable envelope for one event. Records form a tamper-evident
// chain: every digest binds the event payload, its position (seq), its batch
// and generation (revision), and the digest of the preceding record.
type Record struct {
	Seq      uint64          `json:"seq"`
	Batch    uint64          `json:"batch"`
	Revision string          `json:"revision"`
	Epoch    uint64          `json:"epoch"`
	Prev     string          `json:"prev"`
	Event    json.RawMessage `json:"event"`
	Digest   string          `json:"digest"`
}

func recordDigest(seq, batch uint64, revision string, epoch uint64, prev string, event []byte) string {
	value := fmt.Sprintf("%d|%d|%s|%d|%s|", seq, batch, revision, epoch, prev)
	hash := sha256.Sum256(append([]byte(value), event...))
	return "sha256:" + hex.EncodeToString(hash[:])
}

// NewRecord seals an event into an envelope that links to prevDigest.
func NewRecord(event model.Event, seq, batch uint64, prevDigest string) (Record, error) {
	raw, err := json.Marshal(event)
	if err != nil {
		return Record{}, err
	}
	revision := event.Revision.String()
	digest := recordDigest(seq, batch, revision, event.Epoch, prevDigest, raw)
	return Record{Seq: seq, Batch: batch, Revision: revision, Epoch: event.Epoch, Prev: prevDigest, Event: raw, Digest: digest}, nil
}

// Verify checks structural fields against the decoded event and recomputes the
// chained digest. It does NOT inspect the predecessor; callers walk the chain
// with Prev to do that.
func (r Record) Verify() (model.Event, error) {
	var event model.Event
	if len(r.Event) == 0 {
		return event, fmt.Errorf("record %d carries no event", r.Seq)
	}
	if err := json.Unmarshal(r.Event, &event); err != nil {
		return event, fmt.Errorf("record %d event is corrupt: %w", r.Seq, err)
	}
	if event.ID.String() == "00000000-0000-0000-0000-000000000000" {
		return event, fmt.Errorf("record %d event id is missing", r.Seq)
	}
	if event.Revision.String() != r.Revision {
		return event, fmt.Errorf("record %d envelope revision %q does not match event %q", r.Seq, r.Revision, event.Revision.String())
	}
	if event.Epoch != r.Epoch {
		return event, fmt.Errorf("record %d envelope epoch %d does not match event %d", r.Seq, r.Epoch, event.Epoch)
	}
	want := recordDigest(r.Seq, r.Batch, r.Revision, r.Epoch, r.Prev, r.Event)
	if r.Digest != want {
		return event, fmt.Errorf("record %d digest mismatch", r.Seq)
	}
	return event, nil
}

// TailError describes the first line that breaks a record chain and why.
type TailError struct {
	Line   int    `json:"line"`
	Seq    uint64 `json:"seq"`
	Reason string `json:"reason"`
}

func (e TailError) Error() string {
	if e.Seq > 0 {
		return fmt.Sprintf("corrupt tail at line %d (seq %d): %s", e.Line, e.Seq, e.Reason)
	}
	return fmt.Sprintf("corrupt tail at line %d: %s", e.Line, e.Reason)
}
