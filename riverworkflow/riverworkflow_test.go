package riverworkflow

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/riverqueue/river/rivertype"
)

func TestMetadataHelpers(t *testing.T) {
	metadata, err := json.Marshal(map[string]any{
		MetadataKeyWorkflowID:   "wf-id",
		MetadataKeyWorkflowName: "wf-name",
		MetadataKeyWorkflowTask: "task-a",
		MetadataKeyWorkflowDeps: []string{"dep-a", "dep-b"},
	})
	if err != nil {
		t.Fatal(err)
	}
	job := &rivertype.JobRow{Metadata: metadata}
	if IDFromJobRow(job) != "wf-id" || NameFromJobRow(job) != "wf-name" || TaskFromJobRow(job) != "task-a" {
		t.Fatalf("metadata helpers returned wrong values")
	}
	deps := DepsFromJobRow(job)
	if len(deps) != 2 || deps[0] != "dep-a" || deps[1] != "dep-b" {
		t.Fatalf("unexpected deps: %#v", deps)
	}
}

func TestWaitSpecValidationAndRoundTrip(t *testing.T) {
	wait := &WaitSpec{Expr: "cooldown", Inputs: WaitInputs{Timers: []Timer{TimerAfterWaitStarted("cooldown", time.Second)}}}
	if err := wait.Validate([]string{"root"}); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	metadata, err := json.Marshal(map[string]any{MetadataKeyWorkflowWait: wait})
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := WaitFromMetadata(metadata)
	if err != nil {
		t.Fatalf("WaitFromMetadata: %v", err)
	}
	if decoded.Expr != wait.Expr || decoded.Phase != WaitPhaseNotStarted {
		t.Fatalf("unexpected decoded wait: %#v", decoded)
	}
}
