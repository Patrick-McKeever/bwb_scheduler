package parsing

import (
	"testing"
)

// This is very lazy and should be replaced with more thorough testing later.
func TestOwsToJsonConversion(t *testing.T) {
    _, err := ParseWorkflow("testdata/star_salmon_aws")
    if err != nil {
        t.Fatalf("error parsing bulk rna seq workflow: %s", err)
    }

    //jsonBytes, err := json.MarshalIndent(&workflow, "", "\t")
    //if err != nil {
    //    t.Fatalf("error marshaling bulk rna seq workflow: %s", err)
    //}

    //if err = os.WriteFile("bulk_rna_seq.json", jsonBytes, 0644); err != nil {
    //    t.Fatalf("error writing file: %s", err)
    //}
}