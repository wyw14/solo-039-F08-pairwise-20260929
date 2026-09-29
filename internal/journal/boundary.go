package journal

import (
	"fmt"
	"github.com/wyw14/solo-039/internal/model"
)

// Replay action labels.
const (
	ActionReplay         = "replay"
	ActionSkipConfirmed  = "skip-confirmed"
	ActionManualReview   = "manual-review"
	ActionSkipCheckpoint = "skip-before-checkpoint"
)

// ReplayDecision records what recovery must do with one trusted event.
type ReplayDecision struct {
	Seq    uint64
	Batch  uint64
	Event  model.Event
	Action string
	Reason string
}

// EventGroup is an ordered run of replay events sharing a writer batch and
// generation (campaign revision).
type EventGroup struct {
	Batch    uint64
	Revision string
	Epoch    uint64
	Events   []model.Event
}

// Boundary is the resolved, verified recovery frontier.
type Boundary struct {
	// TrustedRecords/TrustedEvents are everything that survived chain
	// verification, in sequence order.
	TrustedRecords []Record
	TrustedEvents  []model.Event
	// ReplayEvents are exactly the events past the last checkpoint that were
	// not already confirmed and not flagged for review — the only events
	// recovery re-applies.
	ReplayEvents []model.Event
	// Groups partitions ReplayEvents by batch then generation, preserving order.
	Groups []EventGroup
	// Decisions explains the disposition of every trusted event.
	Decisions []ReplayDecision
	// Manual lists every item an operator must inspect before it is safe.
	Manual []ManualReview
	// Checkpoint is the sealed checkpoint that was bound (zero if none).
	Checkpoint Checkpoint
	// BoundarySeq is the seq of the record the checkpoint binds to.
	BoundarySeq uint64
	// CheckpointBound reports whether a persisted checkpoint bound to the log.
	CheckpointBound bool
	// CheckpointPresent reports whether a checkpoint file existed.
	CheckpointPresent bool
	// BindError explains why a present checkpoint could not be trusted.
	BindError string
	// Tail describes the corrupt suffix when one was found.
	Tail *TailError
	// Quarantine is the path the corrupt tail was moved to.
	Quarantine string
}

// NeedsManualReview reports whether an operator must intervene.
func (b *Boundary) NeedsManualReview() bool { return len(b.Manual) > 0 }

// ResolveBoundary scans the log, isolates a corrupt tail, binds the checkpoint
// and decides the disposition of every trusted event. The tail is isolated
// immediately when present so a repeat restart sees the same trusted prefix.
func ResolveBoundary(log *FileLog, checkpoints *CheckpointFile) (*Boundary, error) {
	scan, err := log.Scan()
	if err != nil {
		return nil, err
	}
	boundary := &Boundary{
		TrustedRecords: append([]Record(nil), scan.Records...),
		TrustedEvents:  append([]model.Event(nil), scan.Events...),
		Tail:           scan.Tail,
	}

	if scan.Tail != nil {
		quarantine, isolateErr := log.Isolate(scan)
		if isolateErr != nil {
			return nil, fmt.Errorf("isolate corrupt tail: %w", isolateErr)
		}
		boundary.Quarantine = quarantine
		boundary.Manual = append(boundary.Manual, tailReviews(scan, quarantine)...)
	}

	cp, err := checkpoints.Load()
	if err != nil {
		// The suspect checkpoint was already quarantined by Load. Nothing past
		// an untrusted marker may auto-replay.
		boundary.BindError = err.Error()
		boundary.flagAllManual(scan, "checkpoint failed verification: "+err.Error())
		return boundary, nil
	}
	boundary.Checkpoint = cp
	boundary.CheckpointPresent = cp.Exists()

	if !cp.Exists() {
		// Cold start: the verified prefix has never been confirmed, so every
		// event is a candidate for replay.
		boundary.markColdStart(scan)
		boundary.finalize(scan, 0)
		return boundary, nil
	}

	if err := bindCheckpoint(cp, scan.Records, scan.Events); err != nil {
		boundary.BindError = err.Error()
		boundary.flagAllManual(scan, err.Error())
		return boundary, nil
	}

	boundary.CheckpointBound = true
	boundary.BoundarySeq = cp.Seq
	boundary.markFromCheckpoint(scan, cp)
	boundary.finalize(scan, cp.Seq)
	return boundary, nil
}

func bindCheckpoint(cp Checkpoint, records []Record, events []model.Event) error {
	if cp.Seq == 0 || cp.Seq > uint64(len(records)) {
		return fmt.Errorf("checkpoint seq %d is outside the trusted log of %d records", cp.Seq, len(records))
	}
	rec := records[cp.Seq-1]
	event := events[cp.Seq-1]
	return cp.MatchesRecord(rec, event)
}

func (b *Boundary) markColdStart(scan ScanResult) {
	b.Decisions = make([]ReplayDecision, 0, len(scan.Events))
	for i, event := range scan.Events {
		b.Decisions = append(b.Decisions, ReplayDecision{
			Seq: scan.Records[i].Seq, Batch: scan.Records[i].Batch, Event: event,
			Action: ActionReplay, Reason: "no checkpoint exists; event was never confirmed durable",
		})
	}
}

func (b *Boundary) markFromCheckpoint(scan ScanResult, cp Checkpoint) {
	b.Decisions = make([]ReplayDecision, 0, len(scan.Events))
	for i, event := range scan.Events {
		rec := scan.Records[i]
		switch {
		case rec.Seq <= cp.Seq:
			b.Decisions = append(b.Decisions, ReplayDecision{
				Seq: rec.Seq, Batch: rec.Batch, Event: event,
				Action: ActionSkipCheckpoint, Reason: "at or before the last trusted checkpoint",
			})
		case cp.ConfirmedAction(event.ID.String()):
			b.Decisions = append(b.Decisions, ReplayDecision{
				Seq: rec.Seq, Batch: rec.Batch, Event: event,
				Action: ActionSkipConfirmed, Reason: "action already confirmed by checkpoint; restart must not execute it twice",
			})
		default:
			b.Decisions = append(b.Decisions, ReplayDecision{
				Seq: rec.Seq, Batch: rec.Batch, Event: event,
				Action: ActionReplay, Reason: "past the checkpoint and not yet confirmed",
			})
		}
	}
}

// finalize downgrades structurally fine but semantically unsafe replay
// candidates to manual review (duplicate event ids risk double execution; an
// epoch regression inside the replay suffix means the order cannot be
// trusted), then builds the replay set and its batch/generation groups.
func (b *Boundary) finalize(scan ScanResult, afterSeq uint64) {
	seenIDs := make(map[string]uint64)
	var lastEpoch uint64
	for i := range b.Decisions {
		decision := &b.Decisions[i]
		if decision.Seq <= afterSeq || decision.Action != ActionReplay {
			continue
		}
		id := decision.Event.ID.String()
		if prior, dup := seenIDs[id]; dup {
			reason := fmt.Sprintf("event id %s already appeared at seq %d; replaying risks executing an action twice", id, prior)
			decision.Action = ActionManualReview
			decision.Reason = reason
			b.Manual = append(b.Manual, ManualReview{Seq: decision.Seq, EventID: id, Kind: decision.Event.Kind, Batch: decision.Batch, Revision: decision.Event.Revision.String(), Reason: reason})
			continue
		}
		seenIDs[id] = decision.Seq
		if decision.Event.Epoch < lastEpoch {
			reason := fmt.Sprintf("epoch regressed from %d to %d inside replay suffix", lastEpoch, decision.Event.Epoch)
			decision.Action = ActionManualReview
			decision.Reason = reason
			b.Manual = append(b.Manual, ManualReview{Seq: decision.Seq, EventID: id, Kind: decision.Event.Kind, Batch: decision.Batch, Revision: decision.Event.Revision.String(), Reason: reason})
			continue
		}
		lastEpoch = decision.Event.Epoch
	}

	b.ReplayEvents = make([]model.Event, 0)
	b.Groups = make([]EventGroup, 0)
	for _, decision := range b.Decisions {
		if decision.Action != ActionReplay {
			continue
		}
		b.ReplayEvents = append(b.ReplayEvents, decision.Event)
		if len(b.Groups) > 0 {
			last := &b.Groups[len(b.Groups)-1]
			if last.Batch == decision.Batch && last.Revision == decision.Event.Revision.String() {
				last.Events = append(last.Events, decision.Event)
				continue
			}
		}
		b.Groups = append(b.Groups, EventGroup{
			Batch: decision.Batch, Revision: decision.Event.Revision.String(),
			Epoch: decision.Event.Epoch, Events: []model.Event{decision.Event},
		})
	}
}

func (b *Boundary) flagAllManual(scan ScanResult, reason string) {
	b.Decisions = make([]ReplayDecision, 0, len(scan.Events))
	b.ReplayEvents = nil
	b.Groups = nil
	for i, event := range scan.Events {
		rec := scan.Records[i]
		b.Decisions = append(b.Decisions, ReplayDecision{Seq: rec.Seq, Batch: rec.Batch, Event: event, Action: ActionManualReview, Reason: reason})
		b.Manual = append(b.Manual, ManualReview{Seq: rec.Seq, EventID: event.ID.String(), Kind: event.Kind, Batch: rec.Batch, Revision: rec.Revision, Reason: reason})
	}
}

func tailReviews(scan ScanResult, quarantine string) []ManualReview {
	tail := scan.Tail
	return []ManualReview{{
		Seq: tail.Seq, Reason: tail.Error(), Quarantine: quarantine,
	}}
}

// GroupByBatchGeneration splits events into ordered runs by writer batch and
// generation (campaign revision). A generation revisited after another one
// starts a new run, so replay applies batches/generations in sequence order.
func GroupByBatchGeneration(events []model.Event) []EventGroup {
	groups := make([]EventGroup, 0)
	for _, event := range events {
		key := event.Revision.String()
		if len(groups) > 0 && groups[len(groups)-1].Revision == key {
			groups[len(groups)-1].Events = append(groups[len(groups)-1].Events, event)
			continue
		}
		groups = append(groups, EventGroup{Revision: key, Epoch: event.Epoch, Events: []model.Event{event}})
	}
	return groups
}

// NextCheckpoint seals a checkpoint at the final trusted record after replay
// has succeeded. confirmedIDs are the event ids whose side effects are now
// known to have happened (carried forward plus the newly applied ones); the
// generation list and open manual reviews are bound into the seal.
func (b *Boundary) NextCheckpoint(confirmedIDs []string) (Checkpoint, error) {
	if len(b.TrustedRecords) == 0 {
		return Checkpoint{}, nil
	}
	last := len(b.TrustedRecords) - 1
	generations := make([]string, 0)
	seen := make(map[string]struct{})
	for _, rec := range b.TrustedRecords {
		if _, ok := seen[rec.Revision]; !ok {
			seen[rec.Revision] = struct{}{}
			generations = append(generations, rec.Revision)
		}
	}
	// Preserve manual-review entries that are still inside the trusted prefix.
	manual := append([]ManualReview(nil), b.Manual...)
	return BoundCheckpoint(b.TrustedRecords[last], b.TrustedEvents[last], confirmedIDs, generations, manual)
}
