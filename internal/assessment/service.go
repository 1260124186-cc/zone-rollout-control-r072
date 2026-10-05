package assessment

import (
	"context"
	"sort"

	"solo-0014-zone-rollout-control/internal/domain"
	"solo-0014-zone-rollout-control/internal/platform"
	"solo-0014-zone-rollout-control/internal/store"
)

type Service struct {
	store *store.Store
	clock platform.Clock
	ids   platform.IDGenerator
}

func New(stateStore *store.Store, clock platform.Clock, ids platform.IDGenerator) *Service {
	return &Service{store: stateStore, clock: clock, ids: ids}
}

func (s *Service) Evaluate(
	ctx context.Context,
	planID string,
	input domain.AssessmentInput,
) (domain.RolloutAssessment, error) {
	if err := domain.ValidateAssessmentInput(input); err != nil {
		return domain.RolloutAssessment{}, err
	}
	now := s.clock.Now().UTC()
	requestIndex := planID + "\x00" + input.RequestKey
	var result domain.RolloutAssessment
	err := s.store.Write(ctx, func(state *domain.State) error {
		plan, ok := state.Plans[planID]
		if !ok {
			return domain.NotFound("plan", planID)
		}
		if plan.State == domain.PlanDraft {
			return domain.InvalidState("plan must be active or completed for assessment", map[string]any{
				"plan_id": planID,
				"state":   plan.State,
			})
		}
		// A completed request key always replays its stored assessment.
		// Replay never mutates state and stays available after completion.
		if existingID, exists := state.AssessmentRequestKeys[requestIndex]; exists {
			existing, found := state.Assessments[existingID]
			if !found {
				return domain.NewError(domain.CodeStorageFailure, "assessment index is inconsistent", map[string]any{
					"assessment_id": existingID,
				})
			}
			result = existing
			return nil
		}
		// Once the plan is completed its final interpretation is frozen.
		// New request keys are rejected with a terminal error unless the
		// caller explicitly opts into the controlled supplement exception.
		if plan.State == domain.PlanCompleted && !input.AllowPostCompletionSupplement {
			return domain.PlanTerminal("plan is completed; only completed assessment request keys can be replayed", map[string]any{
				"plan_id":             planID,
				"state":               plan.State,
				"final_assessment_id": plan.FinalAssessmentID,
			})
		}
		revision, probes, err := loadAssessmentInputs(state, planID, plan)
		if err != nil {
			return err
		}
		result = domain.BuildAssessment(s.ids.New("asm"), plan, revision, probes, now)
		result.RequestKey = input.RequestKey
		if plan.State == domain.PlanCompleted {
			// Controlled exception: the supplemental record is kept as
			// immutable history but never replaces the final/latest view
			// and never advances the plan version.
			result.Supplemental = true
			state.Assessments[result.ID] = result
			state.AssessmentRequestKeys[requestIndex] = result.ID
			store.AddAudit(state, s.ids, "assessment", result.ID, "supplemented", now, map[string]any{
				"plan_id":             planID,
				"verdict":             result.Verdict,
				"final_assessment_id": plan.FinalAssessmentID,
			})
			return nil
		}
		state.Assessments[result.ID] = result
		state.AssessmentRequestKeys[requestIndex] = result.ID
		plan.LatestAssessmentID = result.ID
		plan.Version++
		state.Plans[planID] = plan
		store.AddAudit(state, s.ids, "assessment", result.ID, "completed", now, map[string]any{
			"plan_id": planID,
			"verdict": result.Verdict,
		})
		return nil
	})
	return result, err
}

func loadAssessmentInputs(
	state *domain.State,
	planID string,
	plan domain.RolloutPlan,
) (domain.ZoneRevision, []domain.Probe, error) {
	if plan.LatestRevisionID == "" {
		return domain.ZoneRevision{}, nil, domain.InvalidState("plan has no zone revision", map[string]any{
			"plan_id": planID,
		})
	}
	revision, ok := state.Revisions[plan.LatestRevisionID]
	if !ok {
		return domain.ZoneRevision{}, nil, domain.NewError(domain.CodeStorageFailure, "plan references a missing revision", map[string]any{
			"revision_id": plan.LatestRevisionID,
		})
	}
	probes := make([]domain.Probe, 0)
	for _, probe := range state.Probes {
		if probe.PlanID == planID {
			probes = append(probes, probe)
		}
	}
	if len(probes) == 0 {
		return domain.ZoneRevision{}, nil, domain.NewError(domain.CodeNoProbes, "plan has no probes to assess", map[string]any{
			"plan_id": planID,
		})
	}
	return revision, probes, nil
}

// Latest returns the frozen final assessment for a completed plan and the
// most recent assessment for an active plan.
func (s *Service) Latest(ctx context.Context, planID string) (domain.RolloutAssessment, error) {
	var result domain.RolloutAssessment
	err := s.store.Read(ctx, func(state *domain.State) error {
		plan, ok := state.Plans[planID]
		if !ok {
			return domain.NotFound("plan", planID)
		}
		assessmentID := plan.LatestAssessmentID
		if plan.State == domain.PlanCompleted {
			assessmentID = plan.FinalAssessmentID
		}
		if assessmentID == "" {
			return domain.NotFound("assessment", planID)
		}
		found, ok := state.Assessments[assessmentID]
		if !ok {
			return domain.NewError(domain.CodeStorageFailure, "plan references a missing assessment", map[string]any{
				"assessment_id": assessmentID,
			})
		}
		result = found
		return nil
	})
	return result, err
}

// History returns every stored assessment for a plan, including the frozen
// final record and any post-completion supplemental records.
func (s *Service) History(
	ctx context.Context,
	planID string,
	limit int,
	cursor string,
) (domain.AssessmentPage, error) {
	if limit < 1 || limit > 100 {
		return domain.AssessmentPage{}, domain.Validation("limit must be between 1 and 100", map[string]any{
			"limit": limit,
		})
	}
	after, err := platform.DecodeCursor(cursor)
	if err != nil {
		return domain.AssessmentPage{}, domain.Validation("cursor is invalid", nil)
	}
	var page domain.AssessmentPage
	err = s.store.Read(ctx, func(state *domain.State) error {
		if _, ok := state.Plans[planID]; !ok {
			return domain.NotFound("plan", planID)
		}
		items := make([]domain.RolloutAssessment, 0)
		for _, assessment := range state.Assessments {
			if assessment.PlanID != planID {
				continue
			}
			if after != "" && assessment.ID <= after {
				continue
			}
			items = append(items, assessment)
		}
		sort.Slice(items, func(i, j int) bool {
			return items[i].ID < items[j].ID
		})
		page.Meta.Limit = limit
		if len(items) > limit {
			page.Items = append(page.Items, items[:limit]...)
			page.Meta.NextCursor = platform.EncodeCursor(items[limit-1].ID)
			return nil
		}
		page.Items = append(page.Items, items...)
		return nil
	})
	return page, err
}
