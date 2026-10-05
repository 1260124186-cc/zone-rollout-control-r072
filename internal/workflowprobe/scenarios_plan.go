package workflowprobe

import (
	"net/http"

	"solo-0014-zone-rollout-control/internal/domain"
)

func runPlanRevisionLifecycle() error {
	harness, err := newHarness()
	if err != nil {
		return err
	}
	defer harness.close()

	plan, revision, err := createActivePlan(
		harness,
		"edge.example",
		2026092501,
		2,
		1,
		80,
	)
	if err != nil {
		return err
	}
	if plan.State != domain.PlanActive || revision.Serial != 2026092501 {
		return ensure(false, "plan and revision were not activated")
	}

	err = harness.call(http.MethodPatch, "/v1/plans/"+plan.ID+"/revisions", domain.RevisionCreateInput{
		Serial:    2026092502,
		Authority: "ns1.edge.example",
		Entries: []domain.ZoneEntry{
			{Owner: "edge.example", Type: domain.EntryA, Value: "192.0.2.11"},
		},
	}, nil)
	if err := expectFailure(err, http.StatusConflict, string(domain.CodeInvalidState)); err != nil {
		return err
	}

	var completed domain.RolloutPlan
	if err := harness.call(http.MethodPatch, "/v1/plans/"+plan.ID+"/complete", map[string]any{
		"expected_version": plan.Version,
	}, &completed); err != nil {
		return err
	}
	if completed.State != domain.PlanCompleted {
		return ensure(false, "plan did not complete")
	}
	if completed.FinalAssessmentID != "" {
		return ensure(false, "plan without a matched assessment must not invent a final")
	}
	err = harness.call(http.MethodPatch, "/v1/plans/"+plan.ID+"/complete", map[string]any{
		"expected_version": completed.Version,
	}, nil)
	if err := expectFailure(err, http.StatusConflict, string(domain.CodeInvalidState)); err != nil {
		return err
	}

	// Completing without an assessment freezes the plan: new assessment
	// request keys are rejected and no latest verdict can be established.
	err = harness.call(http.MethodPatch, "/v1/plans/"+plan.ID+"/assessments", domain.AssessmentInput{
		RequestKey: "edge-assess-0001",
	}, nil)
	if err := expectFailure(err, http.StatusConflict, string(domain.CodePlanTerminal)); err != nil {
		return err
	}
	err = harness.call(http.MethodGet, "/v1/plans/"+plan.ID+"/assessments/latest", nil, nil)
	return expectFailure(err, http.StatusNotFound, string(domain.CodeNotFound))
}
