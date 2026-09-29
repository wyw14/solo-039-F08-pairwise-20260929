package journal

import (
	"errors"
	"github.com/wyw14/solo-039/internal/model"
	"sync"
)

type Store struct {
	mu       sync.Mutex
	events   []model.Event
	seqByID  map[string]uint64
	tip      Envelope
	hasTip   bool
	durable  *DurableLog
	failNext bool
}

func New() *Store {
	return &Store{events: make([]model.Event, 0, 64), seqByID: make(map[string]uint64)}
}

// Bind 把内存 Store 绑定到链式持久日志；绑定后 Append 会先落盘+fsync 再入内存。
func (s *Store) Bind(log *DurableLog) {
	s.mu.Lock()
	s.durable = log
	s.mu.Unlock()
}

func (s *Store) FailNext() { s.mu.Lock(); s.failNext = true; s.mu.Unlock() }

// Append 在绑定持久层时先链式落盘（失败则内存不入账），未绑定时维持纯内存行为。
func (s *Store) Append(event model.Event) error {
	_, err := s.AppendEnvelope(event)
	return err
}

// AppendEnvelope 与 Append 相同，但返回已落盘的链式信封（供确认检查点使用）。
func (s *Store) AppendEnvelope(event model.Event) (Envelope, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failNext {
		s.failNext = false
		return Envelope{}, errors.New("journal short write")
	}
	var env Envelope
	if s.durable != nil {
		durableEnv, err := s.durable.Append(event)
		if err != nil {
			return Envelope{}, err
		}
		env = durableEnv
	}
	s.events = append(s.events, event)
	if env.Seq > 0 {
		s.seqByID[event.ID.String()] = env.Seq
		s.tip = env
		s.hasTip = true
	}
	return env, nil
}

// LoadEnvelopes 用可信链前缀重建内存状态（启动恢复时调用）。
func (s *Store) LoadEnvelopes(envelopes []Envelope) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = make([]model.Event, 0, len(envelopes))
	s.seqByID = make(map[string]uint64, len(envelopes))
	for _, env := range envelopes {
		s.events = append(s.events, env.Event)
		s.seqByID[env.Event.ID.String()] = env.Seq
		s.tip = env
		s.hasTip = true
	}
}

func (s *Store) Events() []model.Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	copyEvents := make([]model.Event, len(s.events))
	copy(copyEvents, s.events)
	return copyEvents
}

// SeqOf 返回事件落盘时的链序号。
func (s *Store) SeqOf(eventID string) (uint64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	seq, ok := s.seqByID[eventID]
	return seq, ok
}

// TipEnvelope 返回当前链尖信封（已落盘的最新事件）。
func (s *Store) TipEnvelope() (Envelope, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.tip, s.hasTip
}

func (s *Store) Last(kind string) (model.Event, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := len(s.events) - 1; i >= 0; i-- {
		if s.events[i].Kind == kind {
			return s.events[i], true
		}
	}
	return model.Event{}, false
}
