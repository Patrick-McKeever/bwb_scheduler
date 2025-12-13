package parsing

import (
	"maps"
	"testing"
)

// Top sort should error on cyclic graphs.
func TestTopSortValidation(t *testing.T) {
	workflow := Workflow{
		Nodes: map[int]WorkflowNode{
			1: {},
			2: {},
			3: {},
		}, Links: []WorkflowLink{
			{
				SourceNodeId: 1,
				SinkNodeId:   2,
			},
			{
				SourceNodeId: 2,
				SinkNodeId:   3,
			},
			{
				SourceNodeId: 3,
				SinkNodeId:   1,
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
		Nodes: map[int]WorkflowNode{
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

	topSort := [][]int{{1}, {2}, {3}, {4}}
	inLinks := map[int]map[string]WorkflowLink{
		2: {"": WorkflowLink{
			SourceNodeId: 1,
			SinkNodeId:   2,
		}},
		3: {"": WorkflowLink{
			SourceNodeId: 2,
			SinkNodeId:   3,
		}},
		4: {"": WorkflowLink{
			SourceNodeId: 3,
			SinkNodeId:   4,
		}},
	}
	outLinks := map[int]map[string]WorkflowLink{
		1: {"": WorkflowLink{
			SourceNodeId: 1,
			SinkNodeId:   2,
		}},
		2: {"": WorkflowLink{
			SourceNodeId: 2,
			SinkNodeId:   3,
		}},
		3: {"": WorkflowLink{
			SourceNodeId: 3,
			SinkNodeId:   4,
		}},
	}

	_, descendants := getAncestorsAndDescendants(topSort, inLinks, outLinks)
	_, _, _, err := parseAsyncAndBarriers(workflow, topSort, descendants)
	if err == nil {
		t.Fatalf("failed to error on non-nested barriers")
	}
}

// Should refuse workflows where some node has async ancestors A and B
// s.t. A is not descended from B or vice versa.
func TestAsyncAncestorValidation(t *testing.T) {
	workflow := Workflow{
		Nodes: map[int]WorkflowNode{
			1: {
				Async: true,
			},
			2: {
				Async: true,
			},
			3: {},
		},
	}

	topSort := [][]int{{1}, {2}, {3}, {4}}
	inLinks := map[int]map[string]WorkflowLink{
		3: {
			"linkFrom1": WorkflowLink{
				SourceNodeId: 1,
				SinkNodeId:   3,
			},
			"linkFrom2": WorkflowLink{
				SourceNodeId: 2,
				SinkNodeId:   3,
			},
		},
	}
	outLinks := map[int]map[string]WorkflowLink{
		1: {"linkFrom1": WorkflowLink{
			SourceNodeId: 1,
			SinkNodeId:   3,
		}},
		2: {"linkFrom2": WorkflowLink{
			SourceNodeId: 2,
			SinkNodeId:   3,
		}},
	}

	_, descendants := getAncestorsAndDescendants(topSort, inLinks, outLinks)
	_, _, _, err := parseAsyncAndBarriers(workflow, topSort, descendants)
	if err == nil {
		t.Fatalf(
			"failed to error on node w/ multiple non-related async ancestors",
		)
	}
}

func TestSinkDistances(t *testing.T) {
    var testCases = []struct{
        name        string
        preds       map[int][]int
        succs       map[int][]int
        expected    map[int]int
    }{
        {
            name: "Single-node base case",
            preds: map[int][]int{1: {}},
            succs: map[int][]int{1: {}},
            expected: map[int]int{1: 0},
        }, {
            name: "Graph with unique top sort",
            preds: map[int][]int{3: {2}, 2: {1}, 1: {}},
            succs: map[int][]int{1: {2}, 2: {3}, 3: {}},
            expected: map[int]int{1: 2, 2: 1, 3: 0},
        }, {
            name: "Graph with multiple top sorts",
            preds: map[int][]int{5: {4, 2}, 4: {3}, 3: {1}, 2: {1}, 1: {}},
            succs: map[int][]int{1: {2, 3}, 2: {5}, 3: {4}, 4: {5}, 5: {}},
            expected: map[int]int{1: 3, 2: 1, 3: 2, 4: 1, 5: 0},
        }, {
            name: "Distjoint graph",
            preds: map[int][]int{5: {4}, 4: {}, 3: {2}, 2: {1}, 1: {}},
            succs: map[int][]int{1: {2}, 2: {3}, 3: {}, 4: {5}, 5: {}},
            expected: map[int]int{1: 2, 2: 1, 3: 0, 4: 1, 5: 0},
        },
    }

    for _, tt := range testCases {
        t.Run(tt.name, func(t *testing.T) {
            actual := getMaxSinkDists(tt.preds, tt.succs)
            if !maps.Equal(actual, tt.expected) {
                t.Fatalf(
                    "incorrect max sink distance for pred list %v; " +
                    "got %v, expected %v", tt.preds, actual, tt.expected,
                )
            }
        })
    }
}

func Test123(t *testing.T) {
    preds := map[int][]int{
      0: {
        4,
      },
      1: {
        4,
        5,
        0,
      },
      2: {
        4,
        1,
        7,
      },
      3: {
        4,
        5,
        0,
      },
      5: {
        0,
        4,
      },
      6: {
        4,
        5,
        2,
        3,
      },
      7: {
        4,
      },
      9: {
        4,
        2,
        6,
      },
    }

    succs := map[int][]int{
        0: {
          5,
          1,
          3,
        },
        1: {
          2,
        },
        2: {
          6,
          9,
        },
        3: {
          6,
        },
        4: {
          7,
          0,
          3,
          1,
          5,
          2,
          6,
          9,
        },
        5: {
          3,
          1,
          6,
        },
        6: {
          9,
        },
        7: {
          2,
        },
    }

    PrettyPrint(getMaxSinkDists(preds, succs))
}