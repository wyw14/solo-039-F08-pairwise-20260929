package journal

import (
	"errors"
	"github.com/wyw14/solo-039/internal/model"
)

func ValidateRecovery(snapshot model.Snapshot, events []model.Event) error {
	if !model.ValidCampaign(snapshot.Campaign) {
		return errors.New("snapshot campaign is invalid")
	}
	if err := ValidateSequence(events); err != nil {
		return err
	}
	if latest, ok := Latest(events); ok && latest.Epoch > snapshot.Campaign.Epoch {
		return errors.New("snapshot trails durable events")
	}
	return nil
}

// ValidateRecoveryWithWatermark 放宽快照校验：只要求快照不得"领先"可信前缀
// （落后的部分由水位之后的幂等重放补齐）。confirmedSeq 为已确认水位，
// 快照至少要与已确认前缀一致，未确认尾部允许超前于快照。
func ValidateRecoveryWithWatermark(snapshot model.Snapshot, trusted []Envelope, confirmedSeq uint64) error {
	if !model.ValidCampaign(snapshot.Campaign) {
		return errors.New("snapshot campaign is invalid")
	}
	events := EnvelopeEvents(trusted)
	if err := ValidateSequence(events); err != nil {
		return err
	}
	for _, env := range trusted {
		if env.Seq > confirmedSeq {
			break
		}
		if env.Event.Epoch > snapshot.Campaign.Epoch {
			return errors.New("snapshot trails confirmed events")
		}
	}
	return nil
}

// EnvelopeEvents 取出信封信封内的事件，顺序不变。
func EnvelopeEvents(envelopes []Envelope) []model.Event {
	events := make([]model.Event, 0, len(envelopes))
	for _, env := range envelopes {
		events = append(events, env.Event)
	}
	return events
}

func RecoveryEvents(store *Store, campaign string) []model.Event {
	replay := NewReplay(store.Events())
	out := make([]model.Event, 0)
	replay.Campaign(campaign, func(event model.Event) { out = append(out, event) })
	return out
}
