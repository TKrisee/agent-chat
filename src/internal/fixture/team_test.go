package fixture

import (
	"agentchat/src/internal/runtimeapi"
	"testing"
)

func TestFixtureTeamAndAggregateCompatibility(t *testing.T) {
	req := runtimeapi.Request{Role: "po", Instructions: "Guide\nRegistered project workers: \n\nEvent: assignment\nBuild the feature"}
	result, err := execute(req)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Actions) != 2 || result.Actions[0].Type != "create_worker" || result.Actions[1].Type != "queue_work_item" || result.Actions[1].Recipient != result.Actions[0].Title {
		t.Fatalf("new project actions %+v", result.Actions)
	}
	req.Instructions = "Guide\nRegistered project workers: Existing (id)\n\nEvent: assignment\nLegacy task"
	result, err = execute(req)
	if err != nil || len(result.Actions) != 1 || result.Actions[0].Type != "queue_worker" {
		t.Fatalf("legacy assignment %+v %v", result, err)
	}
	req.Instructions = "Guide\n\nEvent: po_followup\nReview the recorded child work item outcomes."
	req.Context = []runtimeapi.Message{{Sender: "service", Text: "Work item outcomes:\nFixture work [completed]: PO reviewed: Completed: Build the feature"}}
	result, err = execute(req)
	if err != nil || len(result.Actions) != 1 || result.Actions[0].Type != "report" || result.Actions[0].Body != "PO reviewed: Completed: Build the feature" {
		t.Fatalf("aggregate %+v %v", result, err)
	}
}
