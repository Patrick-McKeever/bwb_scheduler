package parsing

import (
    "fmt"
    "encoding/json"
    "os"
	"testing"
)

// This is very lazy and should be replaced with more thorough testing later.
func TestBulkRnaWorkflowConversion(t *testing.T) {
    wf, err := ParseWorkflow("testdata/star_salmon_aws")
    if err != nil {
        t.Fatalf("error parsing bulk rna seq workflow: %s", err)
    }

    _, err = DryRun(wf)
    if err != nil {
        t.Fatalf("error running bulk RNA seq workflow: %s", err)
    }

    //jsonBytes, err := json.MarshalIndent(&workflow, "", "\t")
    //if err != nil {
    //    t.Fatalf("error marshaling bulk rna seq workflow: %s", err)
    //}

    //if err = os.WriteFile("bulk_rna_seq.json", jsonBytes, 0644); err != nil {
    //    t.Fatalf("error writing file: %s", err)
    //}
}

func TestDashboardWorkflowConversion(t *testing.T) {
    wf, err := ParseWorkflow("testdata/star_salmon_dashboard")
    if err != nil {
        t.Fatalf("error parsing star salmon dashboard workflow: %s", err)
    }

    _, err = DryRun(wf)
    if err != nil {
        t.Fatalf("error running star salmon dashboard workflow: %s", err)
    }
}

func TestSCRNAWorkflowConversion(t *testing.T) {
    // NOTE: The version of this workflow on the MORPHIC github as of 09/10/2025
    // has an error with the assign cell type nodes (essentially a dummy node rn)
    // where the command tries to substitute a non-existent parameter. The scheduler
    // will (correctly) fail this version of the workflow in the dry run, so I edited
    // the copy in test data to correct this error.
    wf, err := ParseWorkflow("testdata/scRNA_seq_features")
    if err != nil {
        t.Fatalf("error parsing star salmon dashboard workflow: %s", err)
    }

    _, err = DryRun(wf)
    if err != nil {
        t.Fatalf("error running star salmon dashboard workflow: %s", err)
    }
}

func TestNanoporeWorkflowConversion(t *testing.T) {
    wf, err := ParseWorkflow("testdata/nanopore_dorado")
    if err != nil {
        t.Fatalf("error parsing star salmon dashboard workflow: %s", err)
    }

    _, err = DryRun(wf)
    if err != nil {
        t.Fatalf("error running star salmon dashboard workflow: %s", err)
    }
}

func TestFusionFinderWorkflowConversion(t *testing.T) {
    wf, err := ParseWorkflow("testdata/fast_fusion_finder")
    if err != nil {
        t.Fatalf("error parsing star salmon dashboard workflow: %s", err)
    }

    _, err = DryRun(wf)
    if err != nil {
        t.Fatalf("error running star salmon dashboard workflow: %s", err)
    }
}

type ResolvedWorkflowEnvelope struct {
	Schema      string          `json:"schema"` // must equal "biodepot.resolved_workflow/v1"
	WorkflowRaw json.RawMessage `json:"resolved_workflow"`
}
func TestBulkRNADryRun(t *testing.T) {
    wfPath := "../test_workflows/bulkrna_async.json"
	data, err := os.ReadFile(wfPath)
	if err != nil {
		t.Fatalf("failed to read workflow file %s: %v", wfPath, err)
	}
	var rawWf ResolvedWorkflowEnvelope
	if err := json.Unmarshal(data, &rawWf); err != nil {
		t.Fatalf("failed to unmarshal JSON file %s: %v", wfPath, err)
	}
	var bwbWorkflow WorkflowV0
	if err := json.Unmarshal(rawWf.WorkflowRaw, &bwbWorkflow); err != nil {
        t.Fatalf("could not unmarshal: %s", err)
    }
    bwbWorkflow.SetParam(4, "fastqDir", "FASTQ_DIR")
    fmt.Println(bwbWorkflow.GetNodeIds())
	if err := PropagateArgTypes(&bwbWorkflow); err != nil {
		t.Fatalf(
			"error propagating arg types in %s: %s",
			wfPath, err,
		)
	}
    _, err = DryRun(bwbWorkflow)
    if err != nil {
        t.Fatalf("error running bulk RNA async workflow: %s", err)
    }
}