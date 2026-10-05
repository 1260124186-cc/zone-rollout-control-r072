package workflowprobe

import (
	"fmt"
	"net/http"

	"solo-0014-zone-rollout-control/internal/domain"
)

func runConsistencyAssessment() error {
	harness, err := newHarness()
	if err != nil {
		return err
	}
	defer harness.close()

	if _, err := enrollNode(harness, "resolver-assess-01", "west"); err != nil {
		return err
	}
	if _, err := enrollNode(harness, "resolver-assess-02", "west"); err != nil {
		return err
	}
	plan, revision, err := createActivePlan(harness, "assess.example", 3001, 2, 1, 40)
	if err != nil {
		return err
	}
	group, err := allocate(harness, plan.ID, "assess-lease-0001", "west", 2)
	if err != nil {
		return err
	}
	for index, leaseID := range group.LeaseIDs {
		requestKey := fmt.Sprintf("assess-probe-%s", leaseID)
		if _, err := submitProbe(harness, leaseID, requestKey, true, revision.Serial, 10+index); err != nil {
			return err
		}
	}

	var consistent domain.RolloutAssessment
	if err := harness.call(http.MethodPatch, "/v1/plans/"+plan.ID+"/assessments", domain.AssessmentInput{
		RequestKey: "assess-request-0001",
	}, &consistent); err != nil {
		return err
	}
	if consistent.Verdict != domain.VerdictConsistent || consistent.ProbeCount != 2 {
		return ensure(false, "consistent assessment did not include both probes")
	}
	if consistent.Final || consistent.Supplemental {
		return ensure(false, "pre-completion assessment must not be flagged final or supplemental")
	}
	var replay domain.RolloutAssessment
	if err := harness.call(http.MethodPatch, "/v1/plans/"+plan.ID+"/assessments", domain.AssessmentInput{
		RequestKey: "assess-request-0001",
	}, &replay); err != nil {
		return err
	}
	if replay.ID != consistent.ID {
		return ensure(false, "assessment replay returned another result")
	}
	var latest domain.RolloutAssessment
	if err := harness.call(http.MethodGet, "/v1/plans/"+plan.ID+"/assessments/latest", nil, &latest); err != nil {
		return err
	}
	if latest.ID != consistent.ID {
		return ensure(false, "latest assessment does not match")
	}

	if err := completePlan(harness, plan.ID); err != nil {
		return err
	}

	// The matched assessment is frozen as the final interpretation.
	var finalView domain.RolloutAssessment
	if err := harness.call(http.MethodGet, "/v1/plans/"+plan.ID+"/assessments/latest", nil, &finalView); err != nil {
		return err
	}
	if finalView.ID != consistent.ID || !finalView.Final {
		return ensure(false, "latest after completion is not the frozen final assessment")
	}
	var completedPlan domain.RolloutPlan
	if err := harness.call(http.MethodGet, "/v1/plans/"+plan.ID, nil, &completedPlan); err != nil {
		return err
	}
	if completedPlan.FinalAssessmentID != consistent.ID ||
		completedPlan.LatestAssessmentID != consistent.ID {
		return ensure(false, "plan did not freeze the matched assessment as final")
	}
	versionAfterCompletion := completedPlan.Version

	// The completed request key can still be replayed after completion.
	var replayAfterCompletion domain.RolloutAssessment
	if err := harness.call(http.MethodPatch, "/v1/plans/"+plan.ID+"/assessments", domain.AssessmentInput{
		RequestKey: "assess-request-0001",
	}, &replayAfterCompletion); err != nil {
		return err
	}
	if replayAfterCompletion.ID != consistent.ID || !replayAfterCompletion.Final {
		return ensure(false, "completed request key did not replay the final assessment")
	}

	// A new request key is rejected with a terminal error and changes nothing.
	err = harness.call(http.MethodPatch, "/v1/plans/"+plan.ID+"/assessments", domain.AssessmentInput{
		RequestKey: "assess-request-0002",
	}, nil)
	if err := expectFailure(err, http.StatusConflict, string(domain.CodePlanTerminal)); err != nil {
		return err
	}

	// The controlled exception requires an explicit opt-in and records a
	// supplemental assessment that never replaces the final interpretation.
	var supplemental domain.RolloutAssessment
	if err := harness.call(http.MethodPatch, "/v1/plans/"+plan.ID+"/assessments", domain.AssessmentInput{
		RequestKey:                    "assess-request-0003",
		AllowPostCompletionSupplement: true,
	}, &supplemental); err != nil {
		return err
	}
	if supplemental.ID == consistent.ID || !supplemental.Supplemental || supplemental.Final {
		return ensure(false, "supplemental assessment was not recorded separately")
	}
	var supplementalReplay domain.RolloutAssessment
	if err := harness.call(http.MethodPatch, "/v1/plans/"+plan.ID+"/assessments", domain.AssessmentInput{
		RequestKey:                    "assess-request-0003",
		AllowPostCompletionSupplement: true,
	}, &supplementalReplay); err != nil {
		return err
	}
	if supplementalReplay.ID != supplemental.ID {
		return ensure(false, "supplemental request key did not replay its record")
	}
	var latestAfterSupplement domain.RolloutAssessment
	if err := harness.call(http.MethodGet, "/v1/plans/"+plan.ID+"/assessments/latest", nil, &latestAfterSupplement); err != nil {
		return err
	}
	if latestAfterSupplement.ID != consistent.ID {
		return ensure(false, "supplemental assessment overwrote the final latest view")
	}
	var planAfterSupplement domain.RolloutPlan
	if err := harness.call(http.MethodGet, "/v1/plans/"+plan.ID, nil, &planAfterSupplement); err != nil {
		return err
	}
	if planAfterSupplement.Version != versionAfterCompletion ||
		planAfterSupplement.LatestAssessmentID != consistent.ID ||
		planAfterSupplement.FinalAssessmentID != consistent.ID {
		return ensure(false, "supplemental assessment mutated the completed plan summary")
	}

	// History remains queryable and contains every stored assessment.
	var history domain.AssessmentPage
	if err := harness.call(http.MethodGet, "/v1/plans/"+plan.ID+"/assessments", nil, &history); err != nil {
		return err
	}
	if len(history.Items) != 2 {
		return ensure(false, fmt.Sprintf("history should contain final and supplemental, got %d", len(history.Items)))
	}
	historyIDs := map[string]bool{}
	for _, item := range history.Items {
		historyIDs[item.ID] = true
	}
	if !historyIDs[consistent.ID] || !historyIDs[supplemental.ID] {
		return ensure(false, "history is missing the final or supplemental assessment")
	}

	reviewPlan, reviewRevision, err := createActivePlan(harness, "review.example", 4001, 1, 1, 20)
	if err != nil {
		return err
	}
	reviewGroup, err := allocate(harness, reviewPlan.ID, "review-lease-0001", "west", 1)
	if err != nil {
		return err
	}
	if _, err := submitProbe(
		harness,
		reviewGroup.LeaseIDs[0],
		"review-probe-0001",
		true,
		reviewRevision.Serial+1,
		30,
	); err != nil {
		return err
	}
	var review domain.RolloutAssessment
	if err := harness.call(http.MethodPatch, "/v1/plans/"+reviewPlan.ID+"/assessments", domain.AssessmentInput{
		RequestKey: "review-request-0001",
	}, &review); err != nil {
		return err
	}
	if review.Verdict != domain.VerdictReviewRequired {
		return ensure(false, "mismatched serial did not require review")
	}
	return nil
}

func completePlan(harness *harness, planID string) error {
	var current domain.RolloutPlan
	if err := harness.call(http.MethodGet, "/v1/plans/"+planID, nil, &current); err != nil {
		return err
	}
	var completed domain.RolloutPlan
	return harness.call(http.MethodPatch, "/v1/plans/"+planID+"/complete", map[string]any{
		"expected_version": current.Version,
	}, &completed)
}
