package journal

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/wyw14/solo-039/internal/model"
)

// ManualReview flags a step that recovery could not prove safe to replay.
// Operators must inspect the quarantined bytes (or the named event) and either
// discard them or append corrected records before the plant resumes.
type ManualReview struct {
	Seq        uint64 `json:"seq"`
	EventID    string `json:"event_id"`
	Kind       string `json:"kind"`
	Batch      uint64 `json:"batch"`
	Revision   string `json:"revision"`
	Reason     string `json:"reason"`
	Quarantine string `json:"quarantine,omitempty"`
}

// Checkpoint is the durable marker of the last trusted event-sequence
// boundary. It binds to a specific record position (seq within batch) and to
// the record's chained digest, so a checkpoint copied onto a different or
// tampered log fails verification on load. Confirmed holds event IDs whose
// side effects already happened; recovery never re-executes them.
type Checkpoint struct {
	Seq         uint64         `json:"seq"`
	Batch       uint64         `json:"batch"`
	Revision    string         `json:"revision"`
	Epoch       uint64         `json:"epoch"`
	EventID     string         `json:"event_id"`
	TailDigest  string         `json:"tail_digest"`
	Confirmed   []string       `json:"confirmed"`
	Generations []string       `json:"generations"`
	Manual      []ManualReview `json:"manual_review,omitempty"`
	Seal        string         `json:"seal"`
}

// sealBody is the exact field set covered by the self seal.
func (c Checkpoint) sealBody() []byte {
	body := struct {
		Seq         uint64         `json:"seq"`
		Batch       uint64         `json:"batch"`
		Revision    string         `json:"revision"`
		Epoch       uint64         `json:"epoch"`
		EventID     string         `json:"event_id"`
		TailDigest  string         `json:"tail_digest"`
		Confirmed   []string       `json:"confirmed"`
		Generations []string       `json:"generations"`
		Manual      []ManualReview `json:"manual_review,omitempty"`
	}{Seq: c.Seq, Batch: c.Batch, Revision: c.Revision, Epoch: c.Epoch, EventID: c.EventID, TailDigest: c.TailDigest, Confirmed: c.Confirmed, Generations: c.Generations, Manual: c.Manual}
	raw, _ := json.Marshal(body)
	return raw
}

func sealDigest(body []byte) string {
	hash := sha256.Sum256(append([]byte("checkpoint|"), body...))
	return "sha256:" + hex.EncodeToString(hash[:])
}

// Sealed returns a copy with the seal recomputed.
func (c Checkpoint) Sealed() Checkpoint {
	c.Confirmed = dedupe(c.Confirmed)
	c.Seal = sealDigest(c.sealBody())
	return c
}

// VerifySeal recomputes the seal and proves the checkpoint was not altered.
func (c Checkpoint) VerifySeal() error {
	if c.Seq > 0 && c.TailDigest == "" {
		return errors.New("checkpoint is not bound to a record digest")
	}
	if c.Seal == "" {
		return errors.New("checkpoint seal is missing")
	}
	if want := sealDigest(c.sealBody()); want != c.Seal {
		return errors.New("checkpoint seal mismatch")
	}
	return nil
}

// MatchesRecord proves the checkpoint belongs on top of the given record:
// same position, same batch/generation and the record's digest equals the
// bound tail digest.
func (c Checkpoint) MatchesRecord(rec Record, event model.Event) error {
	if c.Seq != rec.Seq {
		return fmt.Errorf("checkpoint seq %d does not match record seq %d", c.Seq, rec.Seq)
	}
	if c.Batch != rec.Batch {
		return fmt.Errorf("checkpoint batch %d does not match record batch %d", c.Batch, rec.Batch)
	}
	if c.TailDigest != rec.Digest {
		return errors.New("checkpoint tail digest does not match the record")
	}
	if c.EventID != event.ID.String() {
		return errors.New("checkpoint event id does not match the record")
	}
	if c.Revision != rec.Revision {
		return errors.New("checkpoint revision does not match the record")
	}
	return nil
}

// Exists reports whether a checkpoint has ever been persisted.
func (c Checkpoint) Exists() bool { return c.Seq > 0 || c.Seal != "" }

// IsEmpty is the zero checkpoint used when none exists yet.
func (c Checkpoint) IsEmpty() bool { return !c.Exists() }

// ConfirmedAction reports whether an event's side effect already happened.
func (c Checkpoint) ConfirmedAction(eventID string) bool {
	for _, id := range c.Confirmed {
		if id == eventID {
			return true
		}
	}
	return false
}

// BoundCheckpoint builds a sealed checkpoint at the given verified record.
func BoundCheckpoint(rec Record, event model.Event, confirmed []string, generations []string, manual []ManualReview) (Checkpoint, error) {
	if _, err := rec.Verify(); err != nil {
		return Checkpoint{}, err
	}
	cp := Checkpoint{
		Seq:         rec.Seq,
		Batch:       rec.Batch,
		Revision:    rec.Revision,
		Epoch:       rec.Epoch,
		EventID:     event.ID.String(),
		TailDigest:  rec.Digest,
		Confirmed:   dedupe(confirmed),
		Generations: dedupe(generations),
		Manual:      manual,
	}
	return cp.Sealed(), nil
}

// LegacyCheckpoint retains the in-memory, single-event digest used by audits
// that only see the ephemeral store (no durable records).
type LegacyCheckpoint struct {
	EventID  string
	Revision string
	Epoch    uint64
	Digest   string
}

// NewCheckpoint builds the legacy memory-only marker for an event.
func NewCheckpoint(event model.Event) LegacyCheckpoint {
	raw := event.Kind + "|" + event.Payload + "|" + event.Revision.String()
	hash := sha256.Sum256([]byte(raw))
	return LegacyCheckpoint{EventID: event.ID.String(), Revision: event.Revision.String(), Epoch: event.Epoch, Digest: hex.EncodeToString(hash[:])}
}

func (c LegacyCheckpoint) Valid(event model.Event) bool {
	return c.EventID == event.ID.String() && c.Revision == event.Revision.String() && c.Epoch == event.Epoch && c.Digest != ""
}

func dedupe(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, v := range values {
		if v == "" {
			continue
		}
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}
	return out
}

// ValidateSequence enforces that epochs never regress within an event slice.
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

// Latest returns the last event in a slice.
func Latest(events []model.Event) (model.Event, bool) {
	if len(events) == 0 {
		return model.Event{}, false
	}
	return events[len(events)-1], true
}
