package api

import (
	"github.com/wyw14/solo-039/internal/journal"
	"net/http"
)

func (s *Server) snapshot(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.plant.Snapshot())
}

func (s *Server) evidence(w http.ResponseWriter, r *http.Request) {
	revision := s.plant.Campaign.Snapshot().Revision.String()
	writeJSON(w, http.StatusOK, map[string]any{"revision": revision, "proofs": s.plant.Evidence.Independent(revision)})
}

func (s *Server) recovery(w http.ResponseWriter, r *http.Request) {
	plan := journal.DefaultRecoveryPlan()
	response := map[string]any{"pending": plan.Pending(), "complete": plan.Complete()}

	if report := s.plant.RecoveryStatus(); report != nil {
		// 致命错误之外的恢复都已走完各步骤，按依赖顺序把恢复计划标记为已应用。
		for _, name := range append([]string(nil), plan.Pending()...) {
			if plan.MarkReady(name) {
				plan.Apply(name)
			}
		}
		response["pending"] = plan.Pending()
		response["complete"] = plan.Complete()
		response["trusted_seq"] = report.TrustedSeq
		response["confirmed_seq"] = report.ConfirmedSeq
		response["manual_review"] = report.ManualReview
		response["review_reasons"] = report.ReviewReasons
		response["checkpoint"] = report.Checkpoint
		response["quarantine"] = report.Quarantined
		response["replayed_event_ids"] = report.ReplayedEventIDs
		response["generations"] = report.ReplayedByGeneration
		response["legacy_migrated"] = report.LegacyMigrated
		response["snapshot"] = report.Snapshot
		response["incident_id"] = report.IncidentID
	}

	writeJSON(w, http.StatusOK, response)
}

func (s *Server) profile(w http.ResponseWriter, r *http.Request) {
	gradient, err := s.plant.Profile.Gradient()
	if err != nil {
		writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"readings": s.plant.Profile.Ordered(), "gradient": gradient, "stable": s.plant.Profile.Stable(40)})
}

func (s *Server) metrics(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.plant.Metrics.Snapshot())
}
