package service

import (
	"context"
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
)

func auditError(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func (p *Plant) OperationalAudit() map[string]any {
	current := p.Campaign.Snapshot()
	snapshot := p.Snapshot()
	revision := current.Revision.String()
	proofs := p.Evidence.Independent(revision)

	analyzerRecovery := analyzer.Capture(p.Analyzer)
	analyzerClone := analyzer.New()
	analyzer.Restore(analyzerClone, analyzerRecovery)
	analyzerClone.SetOnline(true)
	analyzerClone.ConfigureWindow(1, 60)
	analyzerCoordinator := analyzer.NewCoordinator()
	analyzerCoordinator.State = analyzerClone
	analyzerCoordinator.Begin(analyzerRecovery.FeedRevision)
	for _, sample := range analyzerRecovery.Samples {
		analyzerCoordinator.Record(sample.FeedRevision, sample.Calibration, sample.Value, sample.At)
	}
	analyzerProof, analyzerProofOK := analyzerCoordinator.CurrentProof(analyzerRecovery.FeedRevision)
	quality := analyzer.DefaultQuality()
	qualityErr := quality.Evaluate(analyzerRecovery.Samples)
	_, qualityProofErr := quality.Proof(analyzerClone, analyzerRecovery.FeedRevision.String())
	analyzerCatalog := analyzer.NewEvidenceCatalog()
	for _, proof := range proofs {
		_ = analyzerCatalog.Add(proof)
	}
	analyzerCatalog.ClearExcept(revision)

	campaignClone := campaign.New(current.Grade)
	campaign.Restore(campaignClone, current)
	boundContext := campaignClone.Bind(context.Background())
	phaseErr := campaign.RequirePhase(campaignClone, current.Phase)
	revisionMatches := campaign.RevisionMatches(campaignClone, revision)
	canRoute := campaign.CanRoute(current)
	nextPhase := current.Phase
	for _, phase := range model.Phases() {
		if model.CanTransition(current.Phase, phase) {
			nextPhase = phase
			break
		}
	}
	_, advanceErr := campaign.AdvanceChecked(campaignClone, nextPhase)
	leaseApplied := campaign.ApplyLease(campaignClone, snapshot.Reflux.Lease)
	restarted := campaign.Restart(campaignClone)
	transitioned := campaign.TransitionRevision(campaignClone, current.Grade)
	completed := campaign.CompleteTransition(campaignClone)
	campaignClone.Stop()
	select {
	case <-boundContext.Done():
	default:
	}
	campaignCoordinator := campaign.NewCoordinator(current.Grade)
	campaign.Restore(campaignCoordinator.State, current)
	campaignStartErr := campaignCoordinator.Start(context.Background())
	campaignRunningErr := campaignCoordinator.EnsureRunning()
	campaignCoordinator.Cancel()
	historyLatest, historyPresent := p.CampaignHistory.Latest()
	historyForRevision := p.CampaignHistory.ForRevision(revision)
	historyPhases := p.CampaignHistory.Phases()

	columnClone := column.New(snapshot.Column.ID)
	column.Restore(columnClone, snapshot.Column)
	columnController := column.NewController(columnClone)
	columnApplyErr := columnController.ApplySignals(current, snapshot.Column.Pressure, snapshot.Column.Level, snapshot.Column.Temperature)
	columnReady := columnController.Ready(current)
	columnSnapshotValid := columnController.SnapshotValid()
	columnStable := column.Stabilize(columnClone, current, snapshot.Column.Pressure, snapshot.Column.Level)
	columnHeatPermit := column.PermitHeat(columnClone, current)
	columnRecoveryReady := column.ColumnReady(columnClone.Snapshot())

	feedCoordinator := feed.NewCoordinator()
	feedCurrent := *p.Feed.Current
	feedPrevious := *p.Feed.Previous
	feedCoordinator.Current = &feedCurrent
	feedCoordinator.Previous = &feedPrevious
	feedSwitchErr := feedCoordinator.SafeSwitch(context.Background(), feedCurrent.Source, current)
	feedProof := feedCoordinator.Proof()
	feedProofErr := feedProof.Validate()
	feedBuiltProof := feed.BuildSwitchProof(feedCoordinator.Previous, feedCoordinator.Current)
	feedRouteEvent := feedCoordinator.Current.Event(current)
	feedState := feed.New()
	feedOpenErr := feedState.Open(context.Background(), current)
	feedActive := feedState.Active()
	feedStateRevision := feedState.Revision()
	feedCloseErr := feedState.Close(context.Background())
	feedMetrics := feed.NewMetrics()
	feedMetrics.Record(feedCurrent.Source)
	feedMetricSnapshot := feedMetrics.Snapshot()
	feedMetrics.Reset()

	permit := p.Interlock.Evaluate(proofs, current)
	explanation := interlock.Explain(permit)
	evidenceSources := interlock.EvidenceSources(proofs)
	sameSource := false
	if len(proofs) > 1 {
		sameSource = interlock.SameSource(proofs[0], proofs[1])
	}
	heatPermit := interlock.HeatPermit(p.Interlock, p.Column, current)
	routePermit := interlock.RoutePermit(p.Interlock, proofs, current)
	restoredPermit := interlock.RestorePermit(p.Interlock, interlock.RecoveryState{Campaign: current, Lease: snapshot.Reflux.Lease, Proofs: proofs})
	recoverySafe := interlock.RecoverySafe(p.Interlock, interlock.RecoveryState{Campaign: current, Lease: snapshot.Reflux.Lease, Proofs: proofs})

	events := p.Journal.Events()
	sequenceErr := journal.ValidateSequence(events)
	latestEvent, latestEventPresent := journal.Latest(events)
	checkpointValid := false
	if latestEventPresent {
		checkpoint := journal.NewCheckpoint(latestEvent)
		checkpointValid = checkpoint.Valid(latestEvent)
	}
	fileEvents, fileReadErr := p.FileLog.Read()
	durableCheckpoint, durableCheckpointErr := p.Checkpoints.Load()
	recoveryReview := p.RecoveryReview()
	_, durableProofPresent := p.Journal.Last("proof.durable")
	durableProofErr := journal.RequireProof(p.Journal, revision)
	recoveryValidationErr := journal.ValidateRecovery(snapshot, events)
	recoveryEvents := journal.RecoveryEvents(p.Journal, current.ID.String())
	replay := journal.NewReplay(events)
	replayEvents := replay.Events()
	replayedForCampaign := 0
	replay.Campaign(current.ID.String(), func(model.Event) { replayedForCampaign++ })
	recoveredState := journal.RecoverState(events, current)
	recoveryPlan := journal.DefaultRecoveryPlan()
	for _, name := range append([]string(nil), recoveryPlan.Pending()...) {
		if recoveryPlan.MarkReady(name) {
			recoveryPlan.Apply(name)
		}
	}
	eventGenerations := journal.ReplayByGeneration(events)
	loadedSnapshot, snapshotLoadErr := p.Snapshots.Load()
	journalProbe := journal.New()
	journalProbe.FailNext()
	journalProbeErr := journalProbe.Append(model.NewEvent("journal.audit", current, ""))

	productTargets := p.ProductTargets()
	target := ""
	if len(productTargets) > 0 {
		target = productTargets[0]
	}
	plan := model.ProductPlan{Campaign: current.ID, Revision: current.Revision, Grade: current.Grade, Target: target}
	planErr := model.ValidatePlan(plan)
	command := model.NewCommand(current, snapshot.Relief.Valve, "close", snapshot.Reboiler.Steam)
	commandErr := command.Validate()
	acknowledgement := model.Acknowledgement{Command: command.ID, Campaign: command.Campaign, Revision: command.Revision, Epoch: command.Epoch, Equipment: command.Equipment, State: "observed", Value: snapshot.Reboiler.Steam}
	ackMatches := acknowledgement.Matches(command)
	ackErr := acknowledgement.Validate(command)
	limits := model.DefaultLimits()
	columnLimitErr := limits.ValidateColumn(snapshot.Column)
	energyLimitErr := limits.ValidateEnergy(snapshot.Reboiler, snapshot.Reflux)
	limitSafe := limits.Safe(snapshot)

	productCampaign := campaign.New(current.Grade)
	campaign.Restore(productCampaign, current)
	productAnalyzer := analyzer.New()
	analyzer.Restore(productAnalyzer, analyzerRecovery)
	productStore := journal.New()
	for _, event := range events {
		_ = productStore.Append(event)
	}
	productCoordinator := product.NewCoordinator(productCampaign, productAnalyzer, productStore)
	productProof := analyzerProof
	if !analyzerProofOK {
		productProof = p.Product.Proof()
	}
	productPublishErr := productCoordinator.Publish(plan)
	productRecoverErr := productCoordinator.Recover(plan)
	productPermit := p.Interlock.Evaluate(proofs, current)
	productRouteErr := product.RouteWithPermit(productCoordinator, productPermit, plan, productProof)
	productClose := product.CloseOnRevision(productCoordinator.Route, revision)
	productTransitionErr := product.PublishGradeTransition(productCoordinator, plan, productProof)
	productRecovery := product.BuildRecovery(events, p.ProductAudit)
	productDurable := productRecovery.Durable(revision)
	productRouted := productRecovery.Routed(revision)
	productRecoveryErr := productRecovery.Validate(revision)
	productRestoreErr := product.RestoreRoute(productCoordinator, plan, productStore, productRecovery)
	productLatest, productLatestErr := p.ProductAudit.Latest()
	productByRevision := p.ProductAudit.ForRevision(revision)
	productState := product.NewState()
	productState.Put(plan, *productCoordinator.Route)
	_, productStatePresent := productState.Get(target)
	productStateTargets := productState.Targets()
	productCoordinator.Route.Close()

	reboilerClone := reboiler.New()
	reboilerRestoreErr := reboiler.Restore(reboilerClone, snapshot.Reboiler, current)
	reboilerCoordinator := reboiler.NewCoordinator()
	reboilerCoordinator.State = reboilerClone
	reboilerRampErr := reboilerCoordinator.Ramp(context.Background(), current, snapshot.Reboiler.Steam)
	reboilerCoordinator.Stop()
	reboilerTripErr := reboiler.CloseForTrip(context.Background(), reboilerClone, current)
	reboilerPermit := reboiler.Permit(reboilerClone, current)
	reboilerRamp := reboiler.NewRamp()
	_ = reboiler.Restore(reboilerRamp.State, snapshot.Reboiler, current)
	reboilerRampApplyErr := reboilerRamp.Apply(context.Background(), current, snapshot.Reboiler.Steam)
	reboilerRamp.State.Stop()
	reboilerRecoverable := reboiler.Recoverable(snapshot.Reboiler, current)
	reboilerConfirmed := p.ReboilerCommands.Confirmed("steam-valve", "closed")

	refluxState := reflux.New()
	_ = refluxState.Ramp(context.Background(), current, snapshot.Reflux.Ratio, 0)
	refluxState.SetLease(snapshot.Reflux.Lease)
	refluxController := reflux.NewController()
	refluxController.State = refluxState
	refluxRampErr := refluxController.Ramp(context.Background(), current, snapshot.Reflux.Ratio)
	refluxController.Stop()
	refluxCoordinator := reflux.NewCoordinator(snapshot.Reflux.PumpCapacity)
	owner := snapshot.Reflux.Lease.Owner
	if owner == "" {
		owner = snapshot.Column.ID
	}
	refluxCoordinator.Register(owner, refluxState)
	refluxAllocator := reflux.NewAllocator(refluxCoordinator)
	requestedCapacity := snapshot.Reflux.Lease.Capacity
	if requestedCapacity <= 0 {
		requestedCapacity = snapshot.Reflux.PumpCapacity / 10
	}
	refluxSubmitErr := refluxAllocator.Submit(reflux.Request{Owner: owner, Capacity: requestedCapacity, Fence: current.Fencing, Priority: 1})
	refluxPending := refluxAllocator.Pending()
	refluxLease, refluxNextErr := refluxAllocator.Next()
	refluxCancelled := refluxAllocator.Cancel(owner)

	reliefSafe := p.Relief.Safe()
	shutdownResult := relief.CombineShutdown(nil, nil, snapshot.Relief, p.Reboiler.ClosureConfirmed(current.Epoch))
	shutdownSafe := shutdownResult.Safe()
	reliefEvaluation := relief.Evaluate(snapshot.Relief, snapshot.Relief.Pressure)
	reliefEvaluationErr := reliefEvaluation.Error()
	reliefEvaluationSafe := reliefEvaluation.Safe()
	reliefLatest, reliefLatestPresent := p.ReliefHistory.Latest()
	reliefFailures := p.ReliefHistory.Failures()
	reliefClone := relief.New()
	reliefClone.Begin(snapshot.Relief.Valve)
	if snapshot.Relief.TimedOut {
		reliefClone.Timeout()
	} else if snapshot.Relief.Released {
		reliefClone.Release()
	}
	reliefBoiler := reboiler.New()
	_ = reboiler.Restore(reliefBoiler, snapshot.Reboiler, current)
	reliefIsolateErr := relief.Isolate(context.Background(), reliefClone, reliefBoiler, current)

	capacity := utility.NewCapacity(snapshot.Reflux.PumpCapacity)
	capacityLease, capacityReserveErr := capacity.Reserve(owner, requestedCapacity, current.Fencing)
	capacityUsed := capacity.Used()
	capacity.Release(capacityLease)
	leaseBook := utility.NewLeaseBook()
	leaseBook.Put(capacityLease)
	bookLease, bookLeasePresent := leaseBook.Get(owner)
	bookLeaseFenced := leaseBook.Fenced(owner, current.Fencing)
	ledger := utility.NewLedger()
	ledgerFence := ledger.Commit(capacityLease)
	ledgerActive := ledger.Active()
	ledgerUsed := ledger.Used()
	ledgerRevoked := ledger.Revoke(owner, ledgerFence)
	leaseValid := utility.ValidLease(bookLease, current.Fencing)
	leaseCanOpen := utility.CanOpen(bookLease, owner)

	serviceCoordinator := NewCoordinator(current.Grade)
	campaign.Restore(serviceCoordinator.Campaign.State, current)
	serviceRunErr := serviceCoordinator.Run(context.Background())
	servicePhase := serviceCoordinator.Phase()
	serviceCoordinator.Cancel()
	runtime := NewRuntime(current.Grade)
	campaign.Restore(runtime.Campaign, current)
	column.Restore(runtime.Column, snapshot.Column)
	runtimeStartErr := runtime.Start(context.Background())
	runtimeRunning := runtime.Running()
	serviceRecoverErr := Recover(p.Journal, runtime, reboilerClone)
	runtime.Stop()
	orderedSteps, orderedRecoveryErr := RecoverOrdered(RecoveryHooks{
		LoadCampaign:     func() error { return nil },
		LoadColumn:       func() error { return nil },
		LoadFencing:      func() error { return nil },
		LoadInterlocks:   func() error { return nil },
		StartControllers: func() error { return nil },
	})
	p.mu.RLock()
	incidentMessage := p.lastError
	p.mu.RUnlock()
	incidentRegistry := NewIncidents()
	incident := incidentRegistry.Open("operational-audit", incidentMessage, revision)
	incidentResolved := incidentRegistry.Resolve(incident.ID)

	return map[string]any{
		"analyzer":  map[string]any{"compatible": analyzer.Compatible(analyzerRecovery, analyzerRecovery.Calibration, analyzerRecovery.FeedRevision), "proof": analyzerProofOK, "quality_error": auditError(qualityErr), "quality_proof_error": auditError(qualityProofErr), "last_proof": analyzerClone.LastProof(), "catalog_count": analyzerCatalog.Count(revision)},
		"campaign":  map[string]any{"phase_error": auditError(phaseErr), "revision_matches": revisionMatches, "can_route": canRoute, "advance_error": auditError(advanceErr), "lease_applied": leaseApplied, "restarted": restarted.Phase, "transitioned": transitioned.Phase, "completed": completed.Phase, "start_error": auditError(campaignStartErr), "running_error": auditError(campaignRunningErr), "history_present": historyPresent, "history_latest": historyLatest, "history_for_revision": len(historyForRevision), "history_phases": historyPhases},
		"column":    map[string]any{"apply_error": auditError(columnApplyErr), "ready": columnReady, "snapshot_valid": columnSnapshotValid, "stable": columnStable, "heat_permit": columnHeatPermit, "recovery_ready": columnRecoveryReady, "profile_count": p.Profile.Count()},
		"feed":      map[string]any{"switch_error": auditError(feedSwitchErr), "proof_error": auditError(feedProofErr), "proof_safe": feedProof.Safe(), "built_proof_safe": feedBuiltProof.Safe(), "route_isolated": feedCoordinator.Current.Isolated(), "route_event": feedRouteEvent.ID, "open_error": auditError(feedOpenErr), "active": feedActive, "revision": feedStateRevision, "close_error": auditError(feedCloseErr), "metrics": feedMetricSnapshot},
		"interlock": map[string]any{"explanation": explanation, "sources": evidenceSources, "same_source": sameSource, "heat": heatPermit.Allowed, "route": routePermit, "restored": restoredPermit.Allowed, "recovery_safe": recoverySafe},
		"journal":   map[string]any{"sequence_error": auditError(sequenceErr), "latest": latestEventPresent, "checkpoint_valid": checkpointValid, "file_path": p.FileLog.Path(), "file_events": len(fileEvents), "file_error": auditError(fileReadErr), "durable_checkpoint": durableCheckpoint.Seq, "durable_checkpoint_seal": auditError(durableCheckpointErr), "durable_checkpoint_bound": recoveryReview["checkpoint_bound"], "recovery_review": recoveryReview, "durable_proof": durableProofPresent, "proof_error": auditError(durableProofErr), "recovery_error": auditError(recoveryValidationErr), "recovery_events": len(recoveryEvents), "replay_events": len(replayEvents), "campaign_events": replayedForCampaign, "state_ready": recoveredState.Ready(), "plan_complete": recoveryPlan.Complete(), "plan_needs_review": recoveryPlan.NeedsReview(), "plan_review_steps": recoveryPlan.ReviewSteps(), "generations": len(eventGenerations), "loaded_campaign": loadedSnapshot.Campaign.ID, "load_error": auditError(snapshotLoadErr), "failure_probe_rejected": journalProbeErr != nil},
		"model":     map[string]any{"plan_error": auditError(planErr), "command_error": auditError(commandErr), "command_key": command.Key(), "ack_matches": ackMatches, "ack_error": auditError(ackErr), "column_limit_error": auditError(columnLimitErr), "energy_limit_error": auditError(energyLimitErr), "safe": limitSafe},
		"product":   map[string]any{"publish_error": auditError(productPublishErr), "recover_error": auditError(productRecoverErr), "route_error": auditError(productRouteErr), "closed_for_revision": productClose, "transition_error": auditError(productTransitionErr), "durable": productDurable, "routed": productRouted, "recovery_error": auditError(productRecoveryErr), "restore_error": auditError(productRestoreErr), "latest": productLatest, "latest_error": auditError(productLatestErr), "revision_decisions": len(productByRevision), "state_present": productStatePresent, "state_targets": productStateTargets},
		"reboiler":  map[string]any{"restore_error": auditError(reboilerRestoreErr), "ramp_error": auditError(reboilerRampErr), "trip_error": auditError(reboilerTripErr), "permit": reboilerPermit, "ramp_apply_error": auditError(reboilerRampApplyErr), "recoverable": reboilerRecoverable, "confirmed": reboilerConfirmed},
		"reflux":    map[string]any{"ramp_error": auditError(refluxRampErr), "submit_error": auditError(refluxSubmitErr), "pending": len(refluxPending), "lease": refluxLease, "next_error": auditError(refluxNextErr), "cancelled": refluxCancelled},
		"relief":    map[string]any{"safe": reliefSafe, "shutdown_safe": shutdownSafe, "evaluation_error": auditError(reliefEvaluationErr), "evaluation_safe": reliefEvaluationSafe, "latest": reliefLatest, "latest_present": reliefLatestPresent, "failures": len(reliefFailures), "isolate_error": auditError(reliefIsolateErr)},
		"utility":   map[string]any{"reserve_error": auditError(capacityReserveErr), "capacity_used": capacityUsed, "book_present": bookLeasePresent, "book_fenced": bookLeaseFenced, "ledger_active": len(ledgerActive), "ledger_used": ledgerUsed, "revoked": ledgerRevoked, "valid": leaseValid, "can_open": leaseCanOpen},
		"service":   map[string]any{"run_error": auditError(serviceRunErr), "phase": servicePhase, "runtime_start_error": auditError(runtimeStartErr), "runtime_running": runtimeRunning, "recover_error": auditError(serviceRecoverErr), "ordered_steps": orderedSteps, "ordered_error": auditError(orderedRecoveryErr), "incident_resolved": incidentResolved, "start_failure_rate": p.Metrics.FailureRate("start")},
	}
}
