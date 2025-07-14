package parsing

import (
	"fmt"
	"testing"
)

// Top sort should error on cyclic graphs.
func TestTopSortValidation(t *testing.T) {
    workflow := Workflow{
        Nodes: map[int]WorkflowNode {
            1: {},
            2: {},
            3: {},
        },  Links: []WorkflowLink{
            {
                SourceNodeId: 1,
                SinkNodeId: 2,
            },
            {
                SourceNodeId: 2,
                SinkNodeId: 3,
            },
            {
                SourceNodeId: 3,
                SinkNodeId: 1,
            },
        },  
    }
    _, err := layeredTopSort(workflow)
    if err == nil {
        t.Fatalf("failed to error when top-sorting cyclic graph")
    }
}

// If async node A with barrier B topologically precedes async node A'
// with barrier B', it should be the case that B' topologically precedes 
// B. Similar to open and closing brackets for nested for loops.
func TestBarrierNestingValidation(t *testing.T) {
    id1 := 1
    id2 := 2
    workflow := Workflow{
        Nodes: map[int]WorkflowNode {
            1: {
                Async: true,
            },
            2: {
                Async: true,
            },
            3: {
                BarrierFor: &id1,
            },
            4: {
                BarrierFor: &id2,
            },
        },    
    }

    topSort := [][]int {{1}, {2}, {3}, {4}}
    inLinks := map[int]map[string]WorkflowLink {
        2: {"": WorkflowLink {
                SourceNodeId: 1,
                SinkNodeId: 2,
        }},
        3: {"": WorkflowLink {
                SourceNodeId: 2,
                SinkNodeId: 3,
        }},
        4: {"": WorkflowLink {
                SourceNodeId: 3,
                SinkNodeId: 4,
        }},
    }
    outLinks := map[int]map[string]WorkflowLink {
        1: {"": WorkflowLink {
                SourceNodeId: 1,
                SinkNodeId: 2,
        }},
        2: {"": WorkflowLink {
                SourceNodeId: 2,
                SinkNodeId: 3,
        }},
        3: {"": WorkflowLink {
                SourceNodeId: 3,
                SinkNodeId: 4,
        }},
    }

    _, descendants := getAncestorsAndDescendants(topSort, inLinks, outLinks)
    _, _, err := parseAsyncAndBarriers(workflow, topSort, descendants)
    if err == nil {
        t.Fatalf("failed to error on non-nested barriers")
    }
    fmt.Println(err)
}

// Should refuse workflows where some node has async ancestors A and B
// s.t. A is not descended from B or vice versa.
func TestAsyncAncestorValidation(t *testing.T) {
    workflow := Workflow{
        Nodes: map[int]WorkflowNode {
            1: {
                Async: true,
            },
            2: {
                Async: true,
            },
            3: {},
        },    
    }

    topSort := [][]int {{1}, {2}, {3}, {4}}
    inLinks := map[int]map[string]WorkflowLink {
        3: {
            "linkFrom1": WorkflowLink {
                SourceNodeId: 1,
                SinkNodeId: 3,
            }, 
            "linkFrom2": WorkflowLink {
                SourceNodeId: 2,
                SinkNodeId: 3,
            },
        },
    }
    outLinks := map[int]map[string]WorkflowLink {
        1: {"linkFrom1": WorkflowLink {
                SourceNodeId: 1,
                SinkNodeId: 3,
        }},
        2: {"linkFrom2": WorkflowLink {
                SourceNodeId: 2,
                SinkNodeId: 3,
        }},
    }

    _, descendants := getAncestorsAndDescendants(topSort, inLinks, outLinks)
    _, _, err := parseAsyncAndBarriers(workflow, topSort, descendants)
    if err == nil {
        t.Fatalf(
            "failed to error on node w/ multiple non-related async ancestors",
        )
    }
    fmt.Println(err)
}