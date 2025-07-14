package main

import (
	"encoding/json"
	"fmt"
	"go-scheduler/parsing"
	"os"
)

func main() {
    workflow, err := parsing.ParseWorkflow("tests/OWS/Bulk-RNA-seq/star_salmon_aws")
    if err != nil {
        panic(fmt.Errorf("error parsing bulk rna seq workflow: %s", err))
    }

    marshalledWf, err := json.MarshalIndent(&workflow, "", "\t")
    if err != nil {
        panic(fmt.Errorf("error marshaling bulk rna seq workflow: %s", err))
    }

    err = os.WriteFile("bulkrna_seq.json", marshalledWf, 0644)
    if err != nil {
        panic(fmt.Errorf("error writing file: %s", err))
    }
}