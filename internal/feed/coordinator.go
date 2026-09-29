package feed

import (
	"context"
	"errors"
	"github.com/wyw14/solo-039/internal/model"
)

type Transition struct {
	Stage       string
	OldSource   string
	OldOpen     bool
	OldFlow     float64
	NewSource   string
	NewOpen     bool
	NewRevision string
}

type Coordinator struct {
	Current  *Route
	Previous *Route
	Observe  func(Transition)
}

func NewCoordinator() *Coordinator { return &Coordinator{Current: &Route{}, Previous: &Route{}} }

func (c *Coordinator) Switch(ctx context.Context, source string, campaign model.Campaign) error {
	old := c.Current
	if old.Source != "" {
		if err := old.Close(ctx); err != nil {
			return err
		}
		c.emit("old-isolated", old, nil)
	}
	c.Previous = old
	c.Current = &Route{}
	c.Current.Prepare(source)
	if err := c.Current.Activate(); err != nil {
		return err
	}
	c.emit("new-active", old, c.Current)
	return nil
}

func (c *Coordinator) emit(stage string, old, next *Route) {
	if c.Observe == nil {
		return
	}
	value := Transition{Stage: stage}
	if old != nil {
		value.OldSource = old.Source
		value.OldOpen = old.Open
		value.OldFlow = old.Flow
	}
	if next != nil {
		value.NewSource = next.Source
		value.NewOpen = next.Open
		value.NewRevision = next.Revision.String()
	}
	c.Observe(value)
}

func (c *Coordinator) SafeSwitch(ctx context.Context, source string, campaign model.Campaign) error {
	if c.Current.Source != "" && !c.Current.Isolated() {
		return errors.New("old source is not isolated")
	}
	return c.Switch(ctx, source, campaign)
}
