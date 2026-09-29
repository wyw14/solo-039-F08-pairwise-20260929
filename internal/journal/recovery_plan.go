package journal

import (
	"errors"
	"github.com/wyw14/solo-039/internal/model"
	"sort"
)

type RecoveryStep struct {
	Name     string
	Requires []string
	Ready    bool
	Applied  bool
	// Review marks a step that automated recovery could not prove safe and
	// that an operator must inspect before it is considered complete.
	Review bool
	// ReviewReason explains why the step was held for manual review.
	ReviewReason string
}

type RecoveryPlan struct {
	steps map[string]*RecoveryStep
	order []string
}

func NewRecoveryPlan() *RecoveryPlan { return &RecoveryPlan{steps: make(map[string]*RecoveryStep)} }

func (p *RecoveryPlan) Add(name string, requires ...string) error {
	if name == "" {
		return errors.New("recovery step name required")
	}
	if _, exists := p.steps[name]; exists {
		return errors.New("duplicate recovery step")
	}
	step := &RecoveryStep{Name: name, Requires: append([]string(nil), requires...)}
	p.steps[name] = step
	p.order = append(p.order, name)
	return nil
}

func (p *RecoveryPlan) MarkReady(name string) bool {
	step := p.steps[name]
	if step == nil {
		return false
	}
	for _, dependency := range step.Requires {
		required := p.steps[dependency]
		if required == nil || !required.Applied {
			return false
		}
	}
	step.Ready = true
	return true
}

func (p *RecoveryPlan) Apply(name string) bool {
	step := p.steps[name]
	if step == nil || !step.Ready {
		return false
	}
	step.Applied = true
	return true
}

func (p *RecoveryPlan) Pending() []string {
	pending := make([]string, 0)
	for _, name := range p.order {
		step := p.steps[name]
		if !step.Applied && !step.Review {
			pending = append(pending, name)
		}
	}
	return pending
}

// HoldForReview parks a step for manual inspection instead of auto-applying it.
// It is dropped from the automated pending set but surfaced through ReviewSteps
// and NeedsReview so an operator cannot miss it.
func (p *RecoveryPlan) HoldForReview(name, reason string) bool {
	step := p.steps[name]
	if step == nil || step.Applied {
		return false
	}
	step.Review = true
	step.ReviewReason = reason
	return true
}

// ReviewSteps returns the names held for manual review with their reasons.
func (p *RecoveryPlan) ReviewSteps() map[string]string {
	held := make(map[string]string)
	for _, name := range p.order {
		if step := p.steps[name]; step != nil && step.Review && !step.Applied {
			held[name] = step.ReviewReason
		}
	}
	return held
}

// NeedsReview reports whether any step is parked for an operator.
func (p *RecoveryPlan) NeedsReview() bool {
	for _, name := range p.order {
		if step := p.steps[name]; step != nil && step.Review && !step.Applied {
			return true
		}
	}
	return false
}

// Complete reports that no step remains for automated recovery. Steps held for
// manual review are intentionally excluded; pair with NeedsReview.
func (p *RecoveryPlan) Complete() bool { return len(p.Pending()) == 0 }

func DefaultRecoveryPlan() *RecoveryPlan {
	plan := NewRecoveryPlan()
	_ = plan.Add("campaign")
	_ = plan.Add("column", "campaign")
	_ = plan.Add("fencing", "campaign", "column")
	_ = plan.Add("interlocks", "fencing")
	_ = plan.Add("controllers", "interlocks")
	return plan
}

func ReplayByGeneration(events []model.Event) map[string][]model.Event {
	groups := make(map[string][]model.Event)
	for _, event := range events {
		key := event.Revision.String()
		groups[key] = append(groups[key], event)
	}
	for key := range groups {
		sort.SliceStable(groups[key], func(i, j int) bool { return groups[key][i].At < groups[key][j].At })
	}
	return groups
}
