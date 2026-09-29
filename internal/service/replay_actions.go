package service

import (
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/wyw14/solo-039/internal/campaign"
	"github.com/wyw14/solo-039/internal/feed"
	"github.com/wyw14/solo-039/internal/journal"
	"github.com/wyw14/solo-039/internal/model"
)

// applyRecoveredAction 对检查点水位之后的单条可信事件做幂等副作用重放。
// 以 event.ID 为去重主键：同一事件在同一次恢复内至多执行一次；水位内事件
// 根本不会进入这里，因此重复启动不会重复执行已确认动作。
func (p *Plant) applyRecoveredAction(env journal.Envelope) error {
	id := env.Event.ID.String()
	if _, ok := p.applied[id]; ok {
		return nil
	}
	// 自然键仅作 advisory：同 kind|revision|payload 出现不同 ID 时提示人工，
	// 不自动合并（合法的重复业务操作不应被误杀）。
	natural := env.Event.Kind + "|" + env.Event.Revision.String() + "|" + env.Event.Payload
	if other, exists := p.naturalKeys[natural]; exists && other != id {
		p.RecoveryReport.AddReviewReason("duplicate-natural-key: " + env.Event.Kind + " events " + other + " and " + id)
	}
	p.naturalKeys[natural] = id

	switch env.Event.Kind {
	case "feed.transition":
		if err := p.replayFeedTransition(env.Event); err != nil {
			return err
		}
	case "plant.started":
		p.restoreCampaign(env.Event, model.Stabilizing, "")
		p.Column.Apply(p.Campaign.Snapshot(), 1.1, 0.5)
	case "campaign.transition":
		p.restoreCampaign(env.Event, model.Transitioning, env.Event.Payload)
	case "product.routed":
		p.replayProductRoute(env.Event)
	case "reflux.lease":
		if err := p.replayRefluxLease(env.Event); err != nil {
			return err
		}
	case "plant.stopped":
		p.restoreCampaign(env.Event, model.Stopped, "")
	case "proof.durable":
		// 无外部动作：落账本身就是该事件的链式 fsync，Store 已在恢复时重建。
	default:
		// 未知事件类型不驱动副作用，仅保留在日志与状态重建中。
	}

	p.applied[id] = struct{}{}
	return nil
}

// restoreCampaign 用事件携带的代次/纪元与给定阶段幂等覆盖活动状态。
func (p *Plant) restoreCampaign(event model.Event, phase model.Phase, grade string) {
	current := p.Campaign.Snapshot()
	current.ID = event.Campaign
	current.Revision = event.Revision
	current.Epoch = event.Epoch
	current.Phase = phase
	if grade != "" {
		current.Grade = grade
	}
	campaign.Restore(p.Campaign, current)
}

// replayFeedTransition 依据负载重建 feed 路由（不生成新 uuid）并重新绑定下游。
func (p *Plant) replayFeedTransition(event model.Event) error {
	transition, err := feed.ParseTransitionPayload(event.Payload)
	if err != nil {
		return err
	}
	p.Feed.Replay(transition)
	if transition.Stage == "new-active" && transition.NewRevision != "" {
		if revision, err := uuid.Parse(transition.NewRevision); err == nil {
			p.Column.SetFeedRevision(revision)
			p.Analyzer.BindFeed(revision)
		}
	}
	return nil
}

// replayProductRoute 幂等重放产品路由打开（Route.Open 本身是纯赋值）。
func (p *Plant) replayProductRoute(event model.Event) {
	current := p.Campaign.Snapshot()
	plan := model.ProductPlan{
		Campaign: event.Campaign,
		Revision: event.Revision,
		Grade:    current.Grade,
		Target:   event.Payload,
	}
	_ = p.Product.Route.Open(plan)
}

// replayRefluxLease 依据负载幂等重建租约占用，不重复累加容量。
func (p *Plant) replayRefluxLease(event model.Event) error {
	owner, amount, fence := parseLeasePayload(event.Payload, event)
	lease, err := p.Reflux.AcquireForReplay(owner, amount, fence)
	if err != nil {
		return err
	}
	p.Leases.Put(lease)
	p.UtilityLedger.Commit(lease)
	p.RefluxTelemetry.Record(p.RefluxState.Snapshot())
	return nil
}

// parseLeasePayload 解析 owner[:amount[:fence]]；缺字段时退回事件纪元与默认额度。
func parseLeasePayload(payload string, event model.Event) (string, float64, uint64) {
	parts := strings.Split(payload, ":")
	owner := parts[0]
	amount := 0.0
	fence := event.Epoch
	if len(parts) > 1 {
		if v, err := strconv.ParseFloat(parts[1], 64); err == nil {
			amount = v
		}
	}
	if len(parts) > 2 {
		if v, err := strconv.ParseUint(parts[2], 10, 64); err == nil {
			fence = v
		}
	}
	return owner, amount, fence
}

// restoreConfirmedFromSnapshot 恢复"检查点水位内、不会再重放副作用"的已确认状态：
// 回流租约占用、回流设备租约、已打开的产品路由，以及由事件水合的 feed 路由。
// 这些是纯状态重建（幂等），不重复执行任何外部动作。
func (p *Plant) restoreConfirmedFromSnapshot(snapshot model.Snapshot, confirmed []journal.Envelope) {
	lease := snapshot.Reflux.Lease
	if lease.Active && lease.Owner != "" {
		// 以快照容量占用重建，幂等且不重复累加（AcquireForReplay 在空协调器上重建一次）。
		if rebuilt, err := p.Reflux.AcquireForReplay(lease.Owner, lease.Capacity, lease.Fence); err == nil {
			p.Leases.Put(rebuilt)
			p.UtilityLedger.Commit(rebuilt)
		}
		p.RefluxState.SetLease(lease)
	}
	for _, target := range snapshot.Products {
		if target == "" {
			continue
		}
		current := p.Campaign.Snapshot()
		plan := model.ProductPlan{Campaign: current.ID, Revision: current.Revision, Grade: current.Grade, Target: target}
		_ = p.Product.Route.Open(plan)
	}
	// feed 路由不在快照结构中，从水位内的 feed.transition 事件顺序水合出最终路由。
	for _, env := range confirmed {
		if env.Event.Kind == "feed.transition" {
			_ = p.replayFeedTransition(env.Event)
		}
	}
}
