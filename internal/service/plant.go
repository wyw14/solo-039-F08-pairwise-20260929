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
	"sync"
	"time"
)

type Plant struct {
	mu                sync.RWMutex
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
	FileLog           *journal.FileLog
	Snapshots         *journal.SnapshotFile
	Checkpoints       *journal.CheckpointFile
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
	boundary          *journal.Boundary
	confirmed         map[string]struct{}
	generations       []string
	manual            []journal.ManualReview
	bindError         string
	replayStats       replayStats
}

func NewPlant(dataDir string) *Plant {
	campaignState := campaign.New("baseline")
	analyzerState := analyzer.New()
	store := journal.New()
	refluxCoordinator := reflux.NewCoordinator(1)
	plant := &Plant{
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
		FileLog:           journal.NewFileLog(filepath.Join(dataDir, "events.jsonl")),
		Snapshots:         journal.NewSnapshotFile(filepath.Join(dataDir, "snapshot.json")),
		Checkpoints:       journal.NewCheckpointFile(filepath.Join(dataDir, "checkpoint.json")),
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
	if err := p.record("plant.started", source); err != nil {
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
	if err := p.record("campaign.transition", grade); err != nil {
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
	return p.record("product.routed", target)
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
	if err := p.record("reflux.lease", owner); err != nil {
		p.Reflux.Release(lease)
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
	return result, p.record("plant.stopped", "safe")
}

func (p *Plant) Snapshot() model.Snapshot {
	return model.Snapshot{Campaign: p.Campaign.Snapshot(), Column: p.Column.Snapshot(), Reflux: p.RefluxState.Snapshot(), Reboiler: p.Reboiler.Snapshot(), Products: p.ProductTargets(), Relief: p.Relief.State.Snapshot()}
}

func (p *Plant) Save() error { return p.Snapshots.Save(p.Snapshot()) }

// Restore rebuilds the plant from durable state. It verifies the event hash
// chain, isolates any corrupt tail for manual review, binds the persisted
// checkpoint and replays only the events inside the last trusted boundary that
// were not already confirmed. Confirmed actions are never executed twice;
// anything that cannot be proven safe is parked for manual review instead of
// being replayed.
func (p *Plant) Restore() error {
	snapshot, snapshotErr := p.Snapshots.Load()

	// Resolve the durable frontier first: this verifies the chain and moves a
	// torn or tampered tail into quarantine before anything reads it.
	boundary, err := journal.ResolveBoundary(p.FileLog, p.Checkpoints)
	if err != nil {
		return err
	}

	confirmed := make(map[string]struct{})
	for _, id := range boundary.Checkpoint.Confirmed {
		confirmed[id] = struct{}{}
	}
	generations := make([]string, 0)
	genSeen := make(map[string]struct{})
	if boundary.CheckpointPresent && len(boundary.Checkpoint.Generations) > 0 {
		for _, g := range boundary.Checkpoint.Generations {
			if _, ok := genSeen[g]; !ok {
				genSeen[g] = struct{}{}
				generations = append(generations, g)
			}
		}
	}

	// New writer incarnation: resume above the highest durable batch so records
	// written after this restart are attributable to this replay generation.
	var maxBatch uint64
	for _, rec := range boundary.TrustedRecords {
		if rec.Batch > maxBatch {
			maxBatch = rec.Batch
		}
		if _, ok := genSeen[rec.Revision]; !ok {
			genSeen[rec.Revision] = struct{}{}
			generations = append(generations, rec.Revision)
		}
	}
	p.FileLog.SetBatch(maxBatch)
	p.FileLog.BeginBatch()

	// Restore the richer subsystem snapshot first. A snapshot that trails the
	// durable log is expected: the replay suffix below brings it forward, so
	// validation only covers the prefix the snapshot actually represents.
	snapshotRestored := false
	if snapshotErr == nil {
		campaign.Restore(p.Campaign, snapshot.Campaign)
		column.Restore(p.Column, snapshot.Column)
		if err := reboiler.Restore(p.Reboiler, snapshot.Reboiler, snapshot.Campaign); err != nil {
			return err
		}
		covered := make([]model.Event, 0)
		for _, event := range boundary.TrustedEvents {
			if event.Epoch <= snapshot.Campaign.Epoch {
				covered = append(covered, event)
			}
		}
		if err := journal.ValidateRecovery(snapshot, covered); err != nil {
			return err
		}
		snapshotRestored = true
	}

	// Load the verified prefix into the read model and walk the per-event
	// disposition. Only "replay" decisions execute recovery actions; confirmed
	// events are skipped (idempotency across restarts) and reviewed events are
	// left well alone.
	replayed, skipped, reviewed := 0, 0, 0
	for _, decision := range boundary.Decisions {
		// The in-memory journal is a read model of durable truth, so every
		// verified event is visible regardless of its action disposition.
		if err := p.Journal.Append(decision.Event); err != nil {
			return err
		}
		switch decision.Action {
		case journal.ActionReplay:
			p.applyReplayedEvent(decision.Event)
			confirmed[decision.Event.ID.String()] = struct{}{}
			replayed++
		case journal.ActionSkipConfirmed, journal.ActionSkipCheckpoint:
			skipped++
		case journal.ActionManualReview:
			reviewed++
		}
	}

	if snapshotRestored {
		p.mu.Lock()
		p.started = snapshot.Campaign.Phase != model.Stopped
		p.mu.Unlock()
		if !model.PhaseAllowsHeat(snapshot.Campaign.Phase) {
			p.Reboiler.Stop()
		}
	}

	// Replay succeeded: publish a checkpoint over the new head so a repeat
	// restart skips everything just applied. Open manual reviews stay bound.
	if replayed > 0 || (boundary.CheckpointPresent && !boundary.CheckpointBound) {
		if cpErr := p.persistCheckpoint(confirmed, generations, boundary.Manual); cpErr != nil {
			return cpErr
		}
	}

	p.mu.Lock()
	p.boundary = boundary
	p.confirmed = confirmed
	p.generations = generations
	p.manual = append([]journal.ManualReview(nil), boundary.Manual...)
	p.bindError = boundary.BindError
	p.replayStats = replayStats{replayed: replayed, skipped: skipped, reviewed: reviewed}
	p.mu.Unlock()
	return nil
}

// applyReplayedEvent re-applies the idempotent state projection for one event.
// Side-effecting actions are intentionally NOT re-invoked here; the event only
// moves campaign revision/epoch forward, which is what downstream fencing and
// proof checks read.
func (p *Plant) applyReplayedEvent(event model.Event) {
	current := p.Campaign.Snapshot()
	current.Revision = event.Revision
	current.Epoch = event.Epoch
	campaign.Restore(p.Campaign, current)
}

func (p *Plant) persistCheckpoint(confirmed map[string]struct{}, generations []string, manual []journal.ManualReview) error {
	ids := make([]string, 0, len(confirmed))
	for id := range confirmed {
		ids = append(ids, id)
	}
	cp, err := p.FileLog.Checkpoint(ids, generations, manual)
	if err != nil {
		return err
	}
	if !cp.Exists() {
		return nil
	}
	return p.Checkpoints.Save(cp)
}

// RecoveryReview reports the durable recovery state for operators: any corrupt
// tail that was quarantined, any checkpoint that failed to bind, and the steps
// parked for manual inspection.
func (p *Plant) RecoveryReview() map[string]any {
	p.mu.RLock()
	defer p.mu.RUnlock()
	manual := append([]journal.ManualReview(nil), p.manual...)
	stats := p.replayStats
	return map[string]any{
		"needs_review":       len(manual) > 0 || p.bindError != "",
		"manual_review":      manual,
		"checkpoint_bound":   p.boundary != nil && p.boundary.CheckpointBound,
		"checkpoint_present": p.boundary != nil && p.boundary.CheckpointPresent,
		"bind_error":         p.bindError,
		"quarantine":         quarantinePath(p.boundary),
		"replayed":           stats.replayed,
		"skipped_confirmed":  stats.skipped,
		"held_for_review":    stats.reviewed,
	}
}

type replayStats struct {
	replayed int
	skipped  int
	reviewed int
}

func quarantinePath(b *journal.Boundary) string {
	if b == nil {
		return ""
	}
	return b.Quarantine
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

func (p *Plant) record(kind, payload string) error {
	event := model.NewEvent(kind, p.Campaign.Snapshot(), payload)
	event.At = time.Now().UnixNano()
	if err := p.Journal.Append(event); err != nil {
		return err
	}
	// FileLog.Append seals the chained record, fsyncs and verifies readback;
	// it fails closed rather than reporting an unverified write as durable.
	if err := p.FileLog.Append(event); err != nil {
		return err
	}
	if err := p.Save(); err != nil {
		return err
	}
	// Advance the durable boundary to the now-proven chain head and mark this
	// action confirmed, so a crash immediately after still never re-executes it.
	p.mu.Lock()
	if p.confirmed == nil {
		p.confirmed = make(map[string]struct{})
	}
	p.confirmed[event.ID.String()] = struct{}{}
	revision := event.Revision.String()
	if !containsString(p.generations, revision) {
		p.generations = append(p.generations, revision)
	}
	confirmed := p.confirmed
	generations := p.generations
	manual := append([]journal.ManualReview(nil), p.manual...)
	p.mu.Unlock()
	return p.persistCheckpoint(confirmed, generations, manual)
}

func containsString(values []string, target string) bool {
	for _, v := range values {
		if v == target {
			return true
		}
	}
	return false
}

func (p *Plant) observeFeed(value feed.Transition) {
	payload := fmt.Sprintf("%s:%s:%t:%.2f:%s:%t", value.Stage, value.OldSource, value.OldOpen, value.OldFlow, value.NewSource, value.NewOpen)
	if err := p.record("feed.transition", payload); err != nil {
		p.fail(err)
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
