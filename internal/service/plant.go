package service

import (
	"context"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/wyw14/solo-039/internal/analyzer"
	"github.com/wyw14/solo-039/internal/campaign"
	"github.com/wyw14/solo-039/internal/column"
	"github.com/wyw14/solo-039/internal/feed"
	"github.com/wyw14/solo-039/internal/interlock"
	"github.com/wyw14/solo-039/internal/journal"
	"github.com/wyw14/solo-039/internal/model"
	"github.com/wyw14/solo-039/internal/product"
	"github.com/wyw14/solo-039/internal/reboiler"
	"github.com/wyw14/solo-039/internal/reflux"
	"github.com/wyw14/solo-039/internal/relief"
	"github.com/wyw14/solo-039/internal/utility"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type Plant struct {
	mu                sync.RWMutex
	dataDir           string
	Campaign          *campaign.State
	Column            *column.State
	Feed              *feed.Coordinator
	Reflux            *reflux.Coordinator
	RefluxState       *reflux.State
	Reboiler          *reboiler.State
	Analyzer          *analyzer.State
	Product           *product.Coordinator
	Interlock         *interlock.Coordinator
	Relief            *relief.Coordinator
	Capacity          *utility.Capacity
	Leases            *utility.LeaseBook
	Journal           *journal.Store
	Durable           *journal.DurableLog
	Checkpoints       *journal.CheckpointFile
	RecoveryReport    *journal.RecoveryReport
	applied           map[string]struct{}
	naturalKeys       map[string]string
	Snapshots         *journal.SnapshotFile
	FeedMetrics       *feed.Metrics
	RefluxTelemetry   *reflux.Telemetry
	ReboilerTelemetry *reboiler.Telemetry
	ProductAudit      *product.Audit
	UtilityLedger     *utility.Ledger
	Metrics           *Metrics
	CampaignHistory   *campaign.History
	Profile           *column.Profile
	RefluxAllocator   *reflux.Allocator
	ReboilerCommands  *reboiler.CommandBook
	Evidence          *analyzer.EvidenceCatalog
	ReliefHistory     *relief.History
	Incidents         *Incidents
	started           bool
	lastError         string
}

func NewPlant(dataDir string) *Plant {
	campaignState := campaign.New("baseline")
	analyzerState := analyzer.New()
	store := journal.New()
	durable := journal.NewDurableLog(filepath.Join(dataDir, "events.jsonl"))
	store.Bind(durable)
	refluxCoordinator := reflux.NewCoordinator(1)
	plant := &Plant{
		dataDir:           dataDir,
		Campaign:          campaignState,
		Column:            column.New("T-101"),
		Feed:              feed.NewCoordinator(),
		Reflux:            refluxCoordinator,
		RefluxState:       reflux.New(),
		Reboiler:          reboiler.New(),
		Analyzer:          analyzerState,
		Product:           product.NewCoordinator(campaignState, analyzerState, store),
		Interlock:         interlock.NewCoordinator(),
		Relief:            relief.NewCoordinator(),
		Capacity:          utility.NewCapacity(1),
		Leases:            utility.NewLeaseBook(),
		Journal:           store,
		Durable:           durable,
		Checkpoints:       journal.NewCheckpointFile(filepath.Join(dataDir, "checkpoint.json")),
		RecoveryReport:    nil,
		applied:           make(map[string]struct{}),
		naturalKeys:       make(map[string]string),
		Snapshots:         journal.NewSnapshotFile(filepath.Join(dataDir, "snapshot.json")),
		FeedMetrics:       feed.NewMetrics(),
		RefluxTelemetry:   reflux.NewTelemetry(),
		ReboilerTelemetry: reboiler.NewTelemetry(),
		ProductAudit:      product.NewAudit(),
		UtilityLedger:     utility.NewLedger(),
		Metrics:           NewMetrics(),
		CampaignHistory:   campaign.NewHistory(),
		Profile:           column.NewProfile(),
		RefluxAllocator:   reflux.NewAllocator(refluxCoordinator),
		ReboilerCommands:  reboiler.NewCommandBook(),
		Evidence:          analyzer.NewEvidenceCatalog(),
		ReliefHistory:     relief.NewHistory(),
		Incidents:         NewIncidents(),
	}
	plant.Reflux.Register("T-101", plant.RefluxState)
	plant.Feed.Observe = plant.observeFeed
	return plant
}

func (p *Plant) Start(ctx context.Context, grade, source string) (model.Campaign, error) {
	var operationErr error
	defer func() { p.Metrics.Record("start", operationErr) }()
	if err := model.ValidateGrade(grade); err != nil {
		operationErr = err
		return model.Campaign{}, err
	}
	p.mu.Lock()
	if p.started {
		current := p.Campaign.Snapshot()
		p.mu.Unlock()
		return current, errors.New("plant already started")
	}
	p.started = true
	p.lastError = ""
	p.mu.Unlock()
	current := p.Campaign.Snapshot()
	before := current
	current.Grade = grade
	campaign.Restore(p.Campaign, current)
	p.CampaignHistory.Record(before, current, "start grade selected")
	if err := p.Feed.Switch(ctx, source, current); err != nil {
		operationErr = err
		p.fail(err)
		return model.Campaign{}, err
	}
	p.Column.SetFeedRevision(p.Feed.Current.Revision)
	p.Analyzer.BindFeed(p.Feed.Current.Revision)
	p.Campaign.Advance(model.Warming)
	if err := p.Reboiler.Ramp(ctx, p.Campaign.Snapshot(), 0.5); err != nil {
		operationErr = err
		p.fail(err)
		return model.Campaign{}, err
	}
	p.Campaign.Advance(model.Stabilizing)
	p.Column.Apply(p.Campaign.Snapshot(), 1.1, 0.5)
	_ = p.Profile.Put(column.TrayReading{Tray: 1, Temperature: 118, Pressure: 1.1})
	_ = p.Profile.Put(column.TrayReading{Tray: 20, Temperature: 82, Pressure: 0.9})
	p.ReboilerTelemetry.Record(p.Reboiler.Snapshot())
	if _, err := p.appendIntent("plant.started", source); err != nil {
		p.fail(err)
		operationErr = err
		return model.Campaign{}, err
	}
	if err := p.confirm(); err != nil {
		p.fail(err)
		operationErr = err
		return model.Campaign{}, err
	}
	return p.Campaign.Snapshot(), nil
}

func (p *Plant) Transition(ctx context.Context, grade, source string) (model.Campaign, error) {
	if err := model.ValidateGrade(grade); err != nil {
		return model.Campaign{}, err
	}
	before := p.Campaign.Snapshot()
	current := p.Campaign.Advance(model.Transitioning)
	current.Grade = grade
	current.Revision = uuid.New()
	campaign.Restore(p.Campaign, current)
	p.CampaignHistory.Record(before, current, "grade transition")
	if err := p.Feed.Switch(ctx, source, current); err != nil {
		p.fail(err)
		return model.Campaign{}, err
	}
	p.Column.SetFeedRevision(p.Feed.Current.Revision)
	p.FeedMetrics.Record(source)
	p.Analyzer.BindFeed(p.Feed.Current.Revision)
	if _, err := p.appendIntent("campaign.transition", grade); err != nil {
		p.fail(err)
		return model.Campaign{}, err
	}
	if err := p.confirm(); err != nil {
		p.fail(err)
		return model.Campaign{}, err
	}
	return p.Campaign.Snapshot(), nil
}

func (p *Plant) AddSample(value float64, at int64) (int, error) {
	revision := p.Feed.Current.Revision
	calibration := p.Analyzer.Calibration()
	ok := p.Analyzer.Add(model.Sample{ID: uuid.New(), FeedRevision: revision, Calibration: calibration, At: at, Component: "top", Value: value, Valid: true})
	if !ok {
		return p.Analyzer.SampleCount(), errors.New("sample rejected by current evidence generation")
	}
	return p.Analyzer.SampleCount(), nil
}

func (p *Plant) CalibrateAnalyzer() uuid.UUID {
	revision := p.Analyzer.Calibrate()
	p.Analyzer.BindFeed(p.Feed.Current.Revision)
	return revision
}

func (p *Plant) RouteProduct(target string) error {
	current := p.Campaign.Snapshot()
	proof, ok := p.Analyzer.Proof(current.Revision)
	if !ok {
		return errors.New("composition evidence is incomplete")
	}
	_ = p.Evidence.Add(proof)
	pressureProof := model.Proof{ID: uuid.New(), Revision: current.Revision, Source: "column-pressure", Digest: fmt.Sprintf("%.3f", p.Column.Snapshot().Pressure), Durable: true, Independent: true, At: time.Now().Unix()}
	permit := p.Interlock.Evaluate([]model.Proof{proof, pressureProof}, model.Campaign{ID: current.ID, Revision: current.Revision, Phase: model.OnSpec, Epoch: current.Epoch, Fencing: current.Fencing})
	if err := permit.Error(); err != nil {
		return err
	}
	current.Phase = model.OnSpec
	campaign.Restore(p.Campaign, current)
	plan := model.ProductPlan{Campaign: current.ID, Revision: current.Revision, Grade: current.Grade, Target: target}
	if err := p.Product.PublishWithProof(plan, proof); err != nil {
		p.ProductAudit.Record(plan, proof, false, err.Error())
		p.fail(err)
		return err
	}
	p.ProductAudit.Record(plan, proof, true, "durable proof accepted")
	if _, err := p.appendIntent("product.routed", target); err != nil {
		p.fail(err)
		return err
	}
	return p.confirm()
}

func (p *Plant) AcquireReflux(owner string, amount float64) (model.Lease, error) {
	current := p.Campaign.Snapshot()
	lease, err := p.Reflux.Acquire(owner, amount, current.Fencing)
	if err != nil {
		p.Incidents.Open("reflux-capacity", err.Error(), current.Revision.String())
		return model.Lease{}, err
	}
	p.Leases.Put(lease)
	p.UtilityLedger.Commit(lease)
	p.RefluxTelemetry.Record(p.RefluxState.Snapshot())
	payload := fmt.Sprintf("%s:%g:%d", owner, amount, current.Fencing)
	if _, err := p.appendIntent("reflux.lease", payload); err != nil {
		p.Reflux.Release(lease)
		p.fail(err)
		return model.Lease{}, err
	}
	if err := p.confirm(); err != nil {
		p.fail(err)
		return model.Lease{}, err
	}
	return lease, nil
}

func (p *Plant) Shutdown(ctx context.Context, reliefTimeout bool) (relief.ShutdownResult, error) {
	current := p.Campaign.Advance(model.Shutting)
	reliefErr := p.Relief.Shutdown(ctx, current, "flare", reliefTimeout)
	steamErr := p.ReboilerCommands.Close(ctx, p.Reboiler, current)
	p.ReboilerTelemetry.Record(p.Reboiler.Snapshot())
	result := relief.CombineShutdown(reliefErr, steamErr, p.Relief.State.Snapshot(), p.Reboiler.ClosureConfirmed(current.Epoch))
	p.ReliefHistory.Record(p.Relief.State.Snapshot(), reliefErr)
	if err := result.Error(); err != nil {
		p.Incidents.Open("shutdown", err.Error(), current.Revision.String())
		p.fail(err)
		return result, err
	}
	p.Campaign.Stop()
	p.mu.Lock()
	p.started = false
	p.mu.Unlock()
	if _, err := p.appendIntent("plant.stopped", "safe"); err != nil {
		p.fail(err)
		return result, err
	}
	return result, p.confirm()
}

// RecoveryStatus 返回最近一次启动恢复报告（未恢复时返回 nil）。
func (p *Plant) RecoveryStatus() *journal.RecoveryReport { return p.RecoveryReport }

func (p *Plant) Snapshot() model.Snapshot {
	return model.Snapshot{Campaign: p.Campaign.Snapshot(), Column: p.Column.Snapshot(), Reflux: p.RefluxState.Snapshot(), Reboiler: p.Reboiler.Snapshot(), Products: p.ProductTargets(), Relief: p.Relief.State.Snapshot()}
}

func (p *Plant) Save() error { return p.Snapshots.Save(p.Snapshot()) }

func (p *Plant) Restore() (*journal.RecoveryReport, error) {
	now := time.Now().UnixNano()
	report, envelopes, err := journal.RecoverJournal(p.dataDir, p.Durable, p.Checkpoints, now)
	if err != nil {
		return nil, err // 致命 I/O 错误
	}
	p.RecoveryReport = report
	trustedEvents := journal.EnvelopeEvents(envelopes)

	// 由可信链前缀重建内存 Store（修复此前内存恒为空、校验形同虚设的问题）。
	p.Journal.LoadEnvelopes(envelopes)

	// 加载快照。快照缺失/损坏不再致命：有可信事件时由事件重建并标记复核。
	snapshot, snapshotErr := p.Snapshots.Load()
	snapshotUsable := snapshotErr == nil && model.ValidCampaign(snapshot.Campaign)
	var activeCampaign uuid.UUID
	if snapshotUsable {
		campaign.Restore(p.Campaign, snapshot.Campaign)
		column.Restore(p.Column, snapshot.Column)
		if err := reboiler.Restore(p.Reboiler, snapshot.Reboiler, snapshot.Campaign); err != nil {
			return report, err
		}
		if err := journal.ValidateRecoveryWithWatermark(snapshot, envelopes, report.ConfirmedSeq); err != nil {
			return report, err
		}
		report.Snapshot = "loaded"
		activeCampaign = snapshot.Campaign.ID
	} else if len(trustedEvents) > 0 {
		// 无可用快照：用首条事件 seed，再由 RecoverState 重建活动身份与代次/纪元。
		seed := model.Campaign{ID: trustedEvents[0].Campaign}
		recovered := journal.RecoverState(trustedEvents, seed)
		campaign.Restore(p.Campaign, recovered.Campaign)
		activeCampaign = recovered.Campaign.ID
		report.Snapshot = "absent"
		report.AddReviewReason("snapshot-missing: campaign rebuilt from durable events; device state defaulted")
	} else {
		report.Snapshot = "absent" // 全新目录，正常情况
		activeCampaign = p.Campaign.Snapshot().ID
	}

	// 决定副作用重放下界：有可用快照且检查点水位>0 时只重放水位之后，
	// 水位内累积型已确认副作用（租约占用、产品路由）由快照承载，避免与重放叠加翻倍。
	// 水位为 0（无检查点或检查点被拒）时改由事件全量幂等重放，不从快照叠加。
	replayFrom := uint64(0)
	if snapshotUsable {
		replayFrom = report.ConfirmedSeq
		if replayFrom > 0 {
			confirmed := make([]journal.Envelope, 0, replayFrom)
			for _, env := range envelopes {
				if env.Seq > replayFrom {
					break
				}
				if env.Event.Campaign == activeCampaign {
					confirmed = append(confirmed, env)
				}
			}
			p.restoreConfirmedFromSnapshot(snapshot, confirmed)
		}
	}

	// 按批次（活动 ID）与代次（Revision）只重放下界之后、可信边界之内的事件。
	// 边界外事件已在 journal 层被剔除，代次分组见 report.ReplayedByGeneration。
	for _, env := range envelopes {
		if env.Seq <= replayFrom || env.Event.Campaign != activeCampaign {
			continue
		}
		if err := p.applyRecoveredAction(env); err != nil {
			p.Incidents.Open("journal.replay-failed", err.Error(), env.Event.Revision.String())
			return report, err
		}
		report.ReplayedEventIDs = append(report.ReplayedEventIDs, env.Event.ID.String())
	}

	final := p.Campaign.Snapshot()
	// started 仅在快照或已持久事件表明设备确在运行时置位；全新初始化保持 false。
	running := false
	if snapshotUsable {
		running = final.Phase != model.Stopped
	} else if len(trustedEvents) > 0 {
		running = trustedEvents[len(trustedEvents)-1].Kind != "plant.stopped"
	}
	p.mu.Lock()
	p.started = running
	p.mu.Unlock()
	if !model.PhaseAllowsHeat(final.Phase) {
		p.Reboiler.Stop()
	}

	// 全部重放成功后把检查点推进到链尖，并保存快照（先检查点后快照）。
	if tip, ok := p.Journal.TipEnvelope(); ok {
		if err := p.Checkpoints.Save(journal.NewCheckpointRecord(tip.Seq, tip, time.Now().UnixNano())); err != nil {
			return report, err
		}
		report.ConfirmedSeq = tip.Seq
	}
	if err := p.Save(); err != nil {
		return report, err
	}

	// 损坏隔离 / 检查点异常 / 快照缺失等：开 incident 并出现在 /api/incidents。
	if report.ManualReview {
		message := strings.Join(report.ReviewReasons, "; ")
		incident := p.Incidents.Open("journal.manual-review", message, final.Revision.String())
		report.IncidentID = incident.ID.String()
	}
	return report, nil
}

func (p *Plant) ProductTargets() []string {
	if p.Product.Route.Opened() {
		return []string{p.Product.Route.Target()}
	}
	return []string{}
}

func (p *Plant) Status() map[string]any {
	p.mu.RLock()
	started := p.started
	lastError := p.lastError
	p.mu.RUnlock()
	gradient, _ := p.Profile.Gradient()
	return map[string]any{"started": started, "last_error": lastError, "campaign": p.Campaign.Snapshot(), "column": p.Column.Snapshot(), "reflux_used": p.Reflux.Used(), "products": p.ProductTargets(), "relief": p.Relief.State.Snapshot(), "feed_metrics": p.FeedMetrics.Snapshot(), "reflux_telemetry": p.RefluxTelemetry.Snapshot(), "reboiler_telemetry": p.ReboilerTelemetry.Snapshot(), "product_decisions": p.ProductAudit.Count(), "utility_used": p.UtilityLedger.Used(), "metrics": p.Metrics.Snapshot(), "campaign_changes": p.CampaignHistory.Count(), "profile_gradient": gradient, "profile_stable": p.Profile.Stable(40), "evidence_count": p.Evidence.Count(p.Campaign.Snapshot().Revision.String()), "relief_attempts": p.ReliefHistory.Count(), "open_incidents": p.Incidents.CountOpen(), "operational_audit": p.OperationalAudit()}
}

// appendIntent 把一条事件作为"意图"链式落盘（fsync）并进入内存 Store。
// 此时事件已持久，但其副作用尚未由检查点确认。
func (p *Plant) appendIntent(kind, payload string) (journal.Envelope, error) {
	event := model.NewEvent(kind, p.Campaign.Snapshot(), payload)
	event.At = time.Now().UnixNano()
	return p.Journal.AppendEnvelope(event)
}

// confirm 把检查点水位推进到当前链尖，再保存快照（先检查点后快照）。
// 水位语义：seq <= 水位的事件副作用均已确认，重启后绝不重放。
func (p *Plant) confirm() error {
	tip, ok := p.Journal.TipEnvelope()
	if !ok {
		return p.Save()
	}
	if err := p.Checkpoints.Save(journal.NewCheckpointRecord(tip.Seq, tip, time.Now().UnixNano())); err != nil {
		return err
	}
	return p.Save()
}

func (p *Plant) observeFeed(value feed.Transition) {
	payload := feed.FormatTransitionPayload(value)
	if _, err := p.appendIntent("feed.transition", payload); err != nil {
		p.fail(err)
		return
	}
	if value.Stage == "new-active" {
		p.FeedMetrics.Record(value.NewSource)
	}
}

func (p *Plant) fail(err error) {
	p.mu.Lock()
	p.lastError = err.Error()
	p.mu.Unlock()
	p.Incidents.Open("runtime", err.Error(), p.Campaign.Snapshot().Revision.String())
}
