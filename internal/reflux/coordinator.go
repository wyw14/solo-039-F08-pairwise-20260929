package reflux

import (
	"errors"
	"github.com/wyw14/solo-039/internal/model"
	"sync"
)

type Coordinator struct {
	mu       sync.Mutex
	capacity float64
	used     float64
	states   map[string]*State
}

func NewCoordinator(capacity float64) *Coordinator {
	return &Coordinator{capacity: capacity, states: make(map[string]*State)}
}

func (c *Coordinator) Register(name string, state *State) {
	c.mu.Lock()
	c.states[name] = state
	c.mu.Unlock()
}

func (c *Coordinator) Acquire(name string, amount float64, fence uint64) (model.Lease, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if amount <= 0 || c.used+amount > c.capacity {
		return model.Lease{}, errors.New("reflux capacity unavailable")
	}
	c.used += amount
	lease := model.Lease{ID: model.NewCampaign(name).ID, Owner: name, Capacity: amount, Fence: fence, Active: true}
	if state := c.states[name]; state != nil {
		state.SetLease(lease)
	}
	return lease, nil
}

// AcquireForReplay 用于恢复重放：若该 owner 已持有同一 fence 的活动租约，
// 直接原样返回而不再累加 used，保证同一条 lease 事件重放不重复占用容量。
// 进程重启后协调器为空，首次重放仍会像正常路径一样重建一次容量占用。
func (c *Coordinator) AcquireForReplay(name string, amount float64, fence uint64) (model.Lease, error) {
	c.mu.Lock()
	if state := c.states[name]; state != nil {
		if existing := state.Snapshot().Lease; existing.Active && existing.Owner == name && existing.Fence == fence {
			c.mu.Unlock()
			return existing, nil
		}
	}
	c.mu.Unlock()
	return c.Acquire(name, amount, fence)
}

func (c *Coordinator) Release(lease model.Lease) {
	c.mu.Lock()
	if lease.Active {
		c.used -= lease.Capacity
	}
	c.mu.Unlock()
}

func (c *Coordinator) Used() float64 { c.mu.Lock(); defer c.mu.Unlock(); return c.used }
