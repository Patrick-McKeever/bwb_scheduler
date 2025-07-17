package parsing

import (
    "fmt"
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
    _, _, err := parseAsyncAndBarriers(workflow, topSort, descendants)
    if err == nil {
        t.Fatalf(
            "failed to error on node w/ multiple non-related async ancestors",
        )
    }
    fmt.Println(err)
}

func Test1234(t *testing.T) {
    workflow := Workflow{
        Nodes: map[int]WorkflowNode{
            1: {
                Async: true,
                ArgTypes: map[string]WorkflowArgType{
                    "p1": {
                        ArgType: "str",
                    },
                },
            },
            2: {
                Async: true,
                ArgTypes: map[string]WorkflowArgType{
                    "p1": {
                        ArgType: "str",
                    },
                },
            },
            3: {
                Async: false,
                ArgTypes: map[string]WorkflowArgType{
                    "p1": {
                        ArgType: "str",
                    },
                },
            },
        },
        Links: []WorkflowLink{
            {
                SourceNodeId:  1,
                SinkNodeId:    2,
                SourceChannel: "p1",
                SinkChannel:   "p1",
            },
            {
                SourceNodeId:  2,
                SinkNodeId:    3,
                SourceChannel: "p1",
                SinkChannel:   "p1",
            },
        },
    }

    index, err := parseAndValidateWorkflow(&workflow)
    PrettyPrint(index)
    state := WorkflowExecutionState{
        workflow: workflow,
        index:    index,
    }
    err = state.addCmdResults(
        NodeParams{ancList: []int{}, nodeId: 1},
        []TypedParams{
            {
                Strings: map[string]string{
                    "p1": "1",
                },
            },
        })
    fmt.Println(err)
    succs, err := state.getEligibleSuccessors(1)
    fmt.Println(err)
    PrettyPrint(succs)
}

func getSuccInputsOrFail(
    t *testing.T, succs map[int][]NodeParams, err error, 
    sourceId int, succId int, expectedLen int,
) []NodeParams {
    // These should be the same thing, but want to check our bases.
    if err != nil || succs == nil {
        t.Fatalf(
            "generating successors for %d failed with err %s", 
            sourceId, err,
        )
    }

    inputsForSucc, inputsForSuccExist := succs[succId]
    if !inputsForSuccExist {
        t.Fatalf(
            "expected node %d to produce input sets for %d, but it did not",
            sourceId, succId,
        )
    }

    if len(inputsForSucc) != expectedLen {
        t.Fatalf(
            "expected node %d to produce %d input sets for %d, but it produced %d",
            sourceId, expectedLen, succId, len(inputsForSucc),
        )
    }

    return inputsForSucc
}

func getArgTypesOrFail(
    t *testing.T, pname string, nodeId int, workflow Workflow,
) WorkflowArgType {
    // Panic on misformed inputs, since this indicates bad test setup.
    node, nodeExists := workflow.Nodes[nodeId]
    if !nodeExists {
        t.Fatalf(
            "requesting nonexistent node ID %d from workflow %#v",
            nodeId, workflow,
        )
    }

    pnameArgTypes, pnamesArgTypesExist := node.ArgTypes[pname]
    if !pnamesArgTypesExist {
        t.Fatalf(
            "requesting nonexistent pname %s from workflow %#v, node ID %d",
            pname, workflow, nodeId,
        )
    }

    return pnameArgTypes
}

func getKeyOrFail(
    t *testing.T, pname string, nodeId int, 
    params TypedParams, workflow Workflow,
) any {
    pnameArgTypes := getArgTypesOrFail(t, pname, nodeId, workflow)
    val, ok := params.lookupParam(pname, pnameArgTypes)
    if !ok {
        t.Fatalf(
            "expected key %s in params %v from node %d", 
            pname, params, nodeId,
        )
    }
    return val
}

func genArbitraryOutputs(
    t *testing.T, pname string, numVals int, 
    startVal int, nodeId int, workflow Workflow,
) []TypedParams {
    pnameArgTypes := getArgTypesOrFail(t, pname, nodeId, workflow)

    var outputs []TypedParams
    for i := 0; i < numVals; i++ {
        var output TypedParams
        output.addParam(
            fmt.Sprintf("%d", startVal + i), 
            pname,
            pnameArgTypes,
        )
        outputs = append(outputs, output)
    }

    return outputs
}


// If node 3 is a descendant of non async node 1 and async
// node 2, then the values received from async node 1 should
// be constant, even though the values of 2 vary.
func TestNonAsyncTransferToAsyncDescendant(t *testing.T) {
    workflow := Workflow{
        Nodes: map[int]WorkflowNode{
            1: {
                Async: false,
                ArgTypes: map[string]WorkflowArgType{
                    "p1": {
                        ArgType: "str",
                    },
                },
            },
            2: {
                Async: true,
                ArgTypes: map[string]WorkflowArgType{
                    "p2": {
                        ArgType: "str",
                    },
                },
            },
            3: {
                Async: false,
                ArgTypes: map[string]WorkflowArgType{
                    "p1": {
                        ArgType: "str",
                    },
                    "p2": {
                        ArgType: "str",
                    },
                },
            },
        },
        Links: []WorkflowLink{
            {
                SourceNodeId:  1,
                SinkNodeId:    3,
                SourceChannel: "p1",
                SinkChannel:   "p1",
            },
            {
                SourceNodeId:  2,
                SinkNodeId:    3,
                SourceChannel: "p2",
                SinkChannel:   "p2",
            },
        },
    }

    index, err := parseAndValidateWorkflow(&workflow)
    if err != nil {
        t.Fatalf("could not build index: %s", err)
    }

    state := WorkflowExecutionState{
        workflow: workflow,
        index:    index,
    }
    
    p1Vals := genArbitraryOutputs(t, "p1", 1, 0, 1, workflow)
    state.TriggerSuccessors(
        NodeParams{ancList: []int{}, nodeId: 1}, p1Vals,
    )

    numIterationsOf2 := 3
    p2Vals := genArbitraryOutputs(t, "p2", numIterationsOf2, 0, 2, workflow)
    succsOf2, err := state.TriggerSuccessors(
        NodeParams{ancList: []int{}, nodeId: 2}, p2Vals,
    )
    inputsFor3 := getSuccInputsOrFail(t, succsOf2, err, 2, 3, numIterationsOf2)

    for i, inputSet := range inputsFor3 {
        p1 := getKeyOrFail(t, "p1", 3, inputSet.params, workflow)
        p2 := getKeyOrFail(t, "p2", 3, inputSet.params, workflow)
        expectedP1Val := getKeyOrFail(t, "p1", 1, p1Vals[0], workflow)
        expectedP2Val := getKeyOrFail(t, "p2", 2, p2Vals[i], workflow)
        if p1 != expectedP1Val {
            t.Fatalf(
                "expected parameter p1 from non-async node 1 to stay static: expected %d, got %d",
                expectedP1Val, p1,
            )
        }
        if p2 != expectedP2Val {
            t.Fatalf(
                "expected parameter p2 from async node 2 to vary: expected %d, got %d",
                expectedP2Val, p2,
            )
        }
    }
}

// If node 1 is async, node 2 is its async descendant, and node 3
// is an async descendant of nodes 1 and 2, then node 3 should
// generate a set of inputs for each combination of outputs of
// 2 and 3.
func TestMultipleAsyncTransfer(t *testing.T) {
    workflow := Workflow{
        Nodes: map[int]WorkflowNode{
            1: {
                Async: true,
                ArgTypes: map[string]WorkflowArgType{
                    "p1": {
                        ArgType: "str",
                    },
                },
            },
            2: {
                Async: true,
                ArgTypes: map[string]WorkflowArgType{
                    "p2": {
                        ArgType: "str",
                    },
                },
            },
            3: {
                Async: false,
                ArgTypes: map[string]WorkflowArgType{
                    "p1": {
                        ArgType: "str",
                    },
                    "p2": {
                        ArgType: "str",
                    },
                },
            },
        },
        Links: []WorkflowLink{
            {
                SourceNodeId:  1,
                SinkNodeId:    3,
                SourceChannel: "p1",
                SinkChannel:   "p1",
            },
            {
                SourceNodeId:  1,
                SinkNodeId:    2,
                SourceChannel: "p1",
                SinkChannel:   "p1",
            },
            {
                SourceNodeId:  2,
                SinkNodeId:    3,
                SourceChannel: "p2",
                SinkChannel:   "p2",
            },
        },
    }

    index, err := parseAndValidateWorkflow(&workflow)
    if err != nil {
        t.Fatalf("could not build index: %s", err)
    }

    state := WorkflowExecutionState{
        workflow: workflow,
        index:    index,
    }
    
    // Hash set of (p1, p2) vals that should be present in
    // generated parameters for node 3.
    p1ToP2 := make(map[string]map[string]struct{})

    // Generate 3 values for node 1 param p1
    numIterationsOf1 := 3
    p1Vals := genArbitraryOutputs(t, "p1", numIterationsOf1, 0, 1, workflow)
    succsOf1, err := state.TriggerSuccessors(
        NodeParams{ancList: []int{}, nodeId: 1}, p1Vals,
    )
    inputsFor2 := getSuccInputsOrFail(t, succsOf1, err, 1, 2, numIterationsOf1)

    // The idea here is to correlate different values of p2 with each value of p1.
    // I.e. simulate a situation where for different values of p1, async node p2
    // generates different values of output p2. We also have it generate different
    // numbers of outputs for p2 for different inputs to further test robustness.
    expNumSuccsOf2 := 0
    for i, inputFor2 := range inputsFor2 {
        expNumSuccsOf2 += (i+1)
        p1 := getKeyOrFail(t, "p1", 2, inputFor2.params, workflow).(string)
        p2Vals := genArbitraryOutputs(t, "p2", (i+1), (i+1)*len(inputsFor2), 2, workflow)
        if _, innerSetExists := p1ToP2[p1]; !innerSetExists {
            p1ToP2[p1] = make(map[string]struct{})
        }

        for _, val := range p2Vals {
            p2Val := getKeyOrFail(t, "p2", 2, val, workflow).(string)
            p1ToP2[p1][p2Val] = struct{}{}
        }
        err := state.addCmdResults(inputFor2, p2Vals)
        if err != nil {
            t.Fatalf("error adding outputs of 2: %s", err)
        }
    }

    succsOf2, err := state.getEligibleSuccessors(2)
    inputsFor3 := getSuccInputsOrFail(t, succsOf2, err, 2, 3, expNumSuccsOf2)

    for _, inputSet := range inputsFor3 {
        p1 := getKeyOrFail(t, "p1", 3, inputSet.params, workflow).(string)
        p2 := getKeyOrFail(t, "p2", 3, inputSet.params, workflow).(string)
        if _, validPair := p1ToP2[p1][p2]; !validPair {
            t.Fatalf("got invalid p1, p2 pair %s, %s", p1, p2)
        }
        
        delete(p1ToP2[p1], p2)
        if len(p1ToP2[p1]) == 0 {
            delete(p1ToP2, p1)
        }
    }

    if len(p1ToP2) > 0 {
        t.Fatalf("did not generate all p1, p2 pairs: remaining are %v", p1ToP2)
    }
}

// 
func TestAsyncAndNonAsyncSiblingsWhichDescendFromAsyncNode(t *testing.T) {
    workflow := Workflow{
        Nodes: map[int]WorkflowNode{
            1: {
                Async: true,
                ArgTypes: map[string]WorkflowArgType{
                    "p1": {
                        ArgType: "str",
                    },
                },
            },
            2: {
                Async: true,
                ArgTypes: map[string]WorkflowArgType{
                    "p2": {
                        ArgType: "str",
                    },
                },
            },
            3: {
                Async: false,
                ArgTypes: map[string]WorkflowArgType{
                    "p1": {
                        ArgType: "str",
                    },
                    "p3": {
                        ArgType: "str",
                    },
                },
            },
            4: {
                Async: false,
                ArgTypes: map[string]WorkflowArgType{
                    "p1": {
                        ArgType: "str",
                    },
                    "p2": {
                        ArgType: "str",
                    },
                    "p3": {
                        ArgType: "str",
                    },
                },
            },
        },
        Links: []WorkflowLink{
            {
                SourceNodeId:  1,
                SinkNodeId:    2,
                SourceChannel: "p1",
                SinkChannel:   "p1",
            },
            {
                SourceNodeId:  1,
                SinkNodeId:    3,
                SourceChannel: "p1",
                SinkChannel:   "p1",
            },
            {
                SourceNodeId:  1,
                SinkNodeId:    4,
                SourceChannel: "p1",
                SinkChannel:   "p1",
            },
            {
                SourceNodeId:  2,
                SinkNodeId:    4,
                SourceChannel: "p2",
                SinkChannel:   "p2",
            },
            {
                SourceNodeId:  3,
                SinkNodeId:    4,
                SourceChannel: "p3",
                SinkChannel:   "p3",
            },
        },
    }

    index, err := parseAndValidateWorkflow(&workflow)
    if err != nil {
        t.Fatalf("could not build index: %s", err)
    }

    state := WorkflowExecutionState{
        workflow: workflow,
        index:    index,
    }
    
    // Hash set of (p1, p2) and (p1, p3) vals that should be present in
    // generated parameters for node 3.
    p1ToP2 := make(map[string]map[string]struct{})
    p1ToP3 := make(map[string]string)

    // Generate 3 values for node 1 param p1
    numIterationsOf1 := 3
    p1Vals := genArbitraryOutputs(t, "p1", numIterationsOf1, 0, 1, workflow)
    succsOf1, err := state.TriggerSuccessors(
        NodeParams{ancList: []int{}, nodeId: 1}, p1Vals,
    )
    inputsFor2 := getSuccInputsOrFail(t, succsOf1, err, 1, 2, numIterationsOf1)

    // The idea here is to correlate different values of p2 with each value of p1.
    // I.e. simulate a situation where for different values of p1, async node p2
    // generates different values of output p2. We also have it generate different
    // numbers of outputs for p2 for different inputs to further test robustness.
    expNumSuccsOf2 := 0
    for i, inputFor2 := range inputsFor2 {
        expNumSuccsOf2 += (i+1)
        p1 := getKeyOrFail(t, "p1", 1, inputFor2.params, workflow).(string)
        p2Vals := genArbitraryOutputs(t, "p2", (i+1), (i+1)*len(inputsFor2), 2, workflow)
        if _, innerSetExists := p1ToP2[p1]; !innerSetExists {
            p1ToP2[p1] = make(map[string]struct{})
        }

        for _, val := range p2Vals {
            p2Val := getKeyOrFail(t, "p2", 2, val, workflow).(string)
            p1ToP2[p1][p2Val] = struct{}{}
        }
        err := state.addCmdResults(inputFor2, p2Vals)
        if err != nil {
            t.Fatalf("error adding outputs of 2: %s", err)
        }
    }

    inputsFor3 := getSuccInputsOrFail(t, succsOf1, err, 1, 3, numIterationsOf1)
    for i, inputsFor3 := range inputsFor3 {
        p1 := getKeyOrFail(t, "p1", 1, inputsFor3.params, workflow).(string)
        p3Vals := genArbitraryOutputs(t, "p3", 1, i, 3, workflow)
        p1ToP3[p1] = getKeyOrFail(t, "p3", 3, p3Vals[0], workflow).(string)
    }

    succsOf3, err := state.getEligibleSuccessors(3)
    inputsFor4 := getSuccInputsOrFail(t, succsOf3, err, 3, 4, expNumSuccsOf2)

    for _, inputSet := range inputsFor4 {
        p1 := getKeyOrFail(t, "p1", 4, inputSet.params, workflow).(string)
        p2 := getKeyOrFail(t, "p2", 4, inputSet.params, workflow).(string)
        p3 := getKeyOrFail(t, "p3", 4, inputSet.params, workflow).(string)
        if _, validPair := p1ToP2[p1][p2]; !validPair {
            t.Fatalf("got invalid p1, p2 pair %s, %s", p1, p2)
        }
        if expP3, expP3Exists := p1ToP3[p1]; !expP3Exists || p3 != expP3 {
            t.Fatalf(
                "got invalid p1, p3 pair %s, %s: expected %s, %s", 
                p1, p3, p1, expP3,
            )
        }
        
        delete(p1ToP3, p1)
        delete(p1ToP2[p1], p2)
        if len(p1ToP2[p1]) == 0 {
            delete(p1ToP2, p1)
        }
    }

    if len(p1ToP2) > 0 {
        t.Fatalf("did not generate all p1, p2 pairs: remaining are %v", p1ToP2)
    }
    if len(p1ToP3) > 0 {
        t.Fatalf("did not generate all p1, p3 pairs: remaining are %v", p1ToP3)
    }
}

func TestNegativeVal(t *testing.T) {
    workflow := Workflow{
        Nodes: map[int]WorkflowNode{
            1: {
                Async: false,
                ArgTypes: map[string]WorkflowArgType{
                    "p1": {
                        ArgType: "str",
                    },
                    "p2": {
                        ArgType: "str",
                    },
                },
            },
            2: {
                Async: true,
                ArgTypes: map[string]WorkflowArgType{
                    "p1": {
                        ArgType: "str",
                    },
                },
            },
            3: {
                Async: false,
                ArgTypes: map[string]WorkflowArgType{
                    "p1": {
                        ArgType: "str",
                    },
                },
            },
        },
        Links: []WorkflowLink{
            {
                SourceNodeId:  1,
                SinkNodeId:    2,
                SourceChannel: "p1",
                SinkChannel:   "p1",
            },
            {
                SourceNodeId:  1,
                SinkNodeId:    3,
                SourceChannel: "p2",
                SinkChannel:   "p2",
            },
            {
                SourceNodeId:  2,
                SinkNodeId:    3,
                SourceChannel: "p1",
                SinkChannel:   "p1",
            },
        },
    }

    index, err := parseAndValidateWorkflow(&workflow)
    if err != nil {
        t.Fatalf("could not build index")
    }

    state := WorkflowExecutionState{
        workflow: workflow,
        index:    index,
    }
    succsOf1, err := state.TriggerSuccessors(
        NodeParams{ancList: []int{}, nodeId: 1},
        []TypedParams{
            {
                Strings: map[string]string{
                    "p1": "1",
                    "p2": "2",
                },
            },
        })

    node2Params, node2ParamsExist := succsOf1[2]
    if ! node2ParamsExist {
        t.Fatalf("")
    } else if len(node2Params) != 1 {
        t.Fatalf("")
    }

    succsOf2, err := state.TriggerSuccessors(
        NodeParams{ancList: []int{}, nodeId: 2},
        []TypedParams{
            {
                Strings: map[string]string{
                    "p1": "1",
                },
            },
        })
    PrettyPrint(succsOf2[3][0].params)
}

func TestSthOrOther(t *testing.T) {
    workflow := Workflow{
        Nodes: map[int]WorkflowNode{
            1: {
                Async: false,
                ArgTypes: map[string]WorkflowArgType{
                    "p1": {
                        ArgType: "str",
                    },
                    "p2": {
                        ArgType: "str",
                    },
                },
            },
            2: {
                Async: true,
                ArgTypes: map[string]WorkflowArgType{
                    "p1": {
                        ArgType: "str",
                    },
                },
            },
            3: {
                Async: false,
                ArgTypes: map[string]WorkflowArgType{
                    "p1": {
                        ArgType: "str",
                    },
                },
            },
        },
        Links: []WorkflowLink{
            {
                SourceNodeId:  1,
                SinkNodeId:    2,
                SourceChannel: "p1",
                SinkChannel:   "p1",
            },
            {
                SourceNodeId:  1,
                SinkNodeId:    2,
                SourceChannel: "p2",
                SinkChannel:   "p2",
            },
            {
                SourceNodeId:  1,
                SinkNodeId:    3,
                SourceChannel: "p2",
                SinkChannel:   "p2",
            },
            {
                SourceNodeId:  2,
                SinkNodeId:    3,
                SourceChannel: "p1",
                SinkChannel:   "p1",
            },
        },
    }

    index, err := parseAndValidateWorkflow(&workflow)
    if err != nil {
        t.Fatalf("could not build index")
    }

    state := WorkflowExecutionState{
        workflow: workflow,
        index:    index,
    }
    err = state.addCmdResults(
        NodeParams{ancList: []int{}, nodeId: 1},
        []TypedParams{
            {
                Strings: map[string]string{
                    "p1": "1",
                    "p2": "2",
                },
            },
        })
    succs, err := state.getEligibleSuccessors(1)

    //err = state.addOutputs(
    //    NodeParams{ancList: []int{}, nodeId: 1},
    //    []TypedParams{
    //        {
    //            Strings: map[string]string {
    //                "p1": "1",
    //                "p2": "2",
    //            },
    //        },
    //    })
    succs, err = state.getEligibleSuccessors(1)
    if len(succs) > 0 {
        t.Fatalf("regenerating already generated cmds")
    }

    err = state.addCmdResults(
        NodeParams{ancList: []int{}, nodeId: 2},
        []TypedParams{
            {
                Strings: map[string]string{
                    "p1": "1",
                },
            },
        })
    err = state.addCmdResults(
        NodeParams{ancList: []int{}, nodeId: 2},
        []TypedParams{
            {
                Strings: map[string]string{
                    "p1": "2",
                },
            },
        })
    err = state.addCmdResults(
        NodeParams{ancList: []int{}, nodeId: 2},
        []TypedParams{
            {
                Strings: map[string]string{
                    "p1": "3",
                },
            },
        })
    succs, err = state.getEligibleSuccessors(2)
    for _, succSet := range succs[3] {
        PrettyPrint(succSet.params)
    }
    PrettyPrint(succs[3])
}

func TestSthOrOther2(t *testing.T) {
    workflow := Workflow{
        Nodes: map[int]WorkflowNode{
            1: {
                Async: false,
                ArgTypes: map[string]WorkflowArgType{
                    "p1": {
                        ArgType: "str",
                    },
                    "p2": {
                        ArgType: "str",
                    },
                },
            },
            2: {
                Async: true,
                ArgTypes: map[string]WorkflowArgType{
                    "p1": {
                        ArgType: "str",
                    },
                    "p2": {
                        ArgType: "str",
                    },
                },
            },
            3: {
                Async: false,
                ArgTypes: map[string]WorkflowArgType{
                    "p1": {
                        ArgType: "str",
                    },
                },
            },
        },
        Links: []WorkflowLink{
            {
                SourceNodeId:  1,
                SinkNodeId:    2,
                SourceChannel: "p1",
                SinkChannel:   "p1",
            },
            {
                SourceNodeId:  1,
                SinkNodeId:    2,
                SourceChannel: "p2",
                SinkChannel:   "p2",
            },
            {
                SourceNodeId:  2,
                SinkNodeId:    3,
                SourceChannel: "p1",
                SinkChannel:   "p1",
            },
            {
                SourceNodeId:  2,
                SinkNodeId:    3,
                SourceChannel: "p2",
                SinkChannel:   "p2",
            },
        },
    }

    index, err := parseAndValidateWorkflow(&workflow)
    if err != nil {
        t.Fatalf("could not build index: %s", err)
    }

    PrettyPrint(index)
    state := WorkflowExecutionState{
        workflow: workflow,
        index:    index,
    }
    err = state.addCmdResults(
        NodeParams{ancList: []int{}, nodeId: 1},
        []TypedParams{
            {
                Strings: map[string]string{
                    "p1": "1",
                    "p2": "2",
                },
            },
        })
    succs, err := state.getEligibleSuccessors(1)
    node2input := succs[2][0]

    err = state.addCmdResults(
        node2input,
        []TypedParams{
            {
                Strings: map[string]string{
                    "p1": "1",
                },
            },
        })
    err = state.addCmdResults(
        node2input,
        []TypedParams{
            {
                Strings: map[string]string{
                    "p1": "2",
                },
            },
        })
    err = state.addCmdResults(
        node2input,
        []TypedParams{
            {
                Strings: map[string]string{
                    "p1": "3",
                },
            },
        })
    succs, err = state.getEligibleSuccessors(2)
    for _, succSet := range succs[3] {
        PrettyPrint(succSet.params)
    }
    PrettyPrint(succs[3])
}

func TestMultipleLayersOfAsync(t *testing.T) {
    workflow := Workflow{
        Nodes: map[int]WorkflowNode{
            1: {
                Async: false,
                ArgTypes: map[string]WorkflowArgType{
                    "p1": {
                        ArgType: "str",
                    },
                },
            },
            2: {
                Async: true,
                ArgTypes: map[string]WorkflowArgType{
                    "p2": {
                        ArgType: "str",
                    },
                },
            },
            3: {
                Async: true,
                ArgTypes: map[string]WorkflowArgType{
                    "p3": {
                        ArgType: "str",
                    },
                },
            },
            4: {
                Async:    true,
                ArgTypes: map[string]WorkflowArgType{},
            },
        },
        Links: []WorkflowLink{
            {
                SourceNodeId:  1,
                SinkNodeId:    4,
                SourceChannel: "p1",
                SinkChannel:   "p1",
            },
            {
                SourceNodeId:  1,
                SinkNodeId:    2,
                SourceChannel: "p1",
                SinkChannel:   "p1",
            },
            {
                SourceNodeId:  2,
                SinkNodeId:    3,
                SourceChannel: "p2",
                SinkChannel:   "p2",
            },
            {
                SourceNodeId:  2,
                SinkNodeId:    4,
                SourceChannel: "p2",
                SinkChannel:   "p2",
            },
            {
                SourceNodeId:  3,
                SinkNodeId:    4,
                SourceChannel: "p3",
                SinkChannel:   "p3",
            },
        },
    }

    index, err := parseAndValidateWorkflow(&workflow)
    if err != nil {
        t.Fatalf("could not build index: %s", err)
    }

    state := WorkflowExecutionState{
        workflow: workflow,
        index:    index,
    }

    err = state.addCmdResults(
        NodeParams{ancList: []int{}, nodeId: 1},
        []TypedParams{
            {
                Strings: map[string]string{
                    "p1": "1",
                },
            },
        })

    if err != nil { panic(err) }

    succs, err := state.getEligibleSuccessors(1)
    if err != nil { panic(err) }
    node2input := succs[2][0]
    for i := 1; i < 3; i++ {
        err = state.addCmdResults(
            node2input,
            []TypedParams{
                {
                    Strings: map[string]string{
                        "p2": fmt.Sprintf("%d", i),
                    },
                },
            })
        if err != nil { panic(err) }
    }

    node2Succs, err := state.getEligibleSuccessors(2)
    if err != nil { panic(err) }

    for _, node3Inputs := range node2Succs[3] {
        for i := 1; i < 3; i++ {
            err = state.addCmdResults(
                node3Inputs,
                []TypedParams{
                    {
                        Strings: map[string]string{
                            "p3": fmt.Sprintf("%d", i),
                        },
                    },
                })
            if err != nil { panic(err) }
        }
    }

    fmt.Printf("%v", state)
    succs, err = state.getEligibleSuccessors(3)
    if err != nil { panic(err) }
    PrettyPrint(succs)
    for _, succSet := range succs[3] {
        PrettyPrint(succSet.params)
    }
}

// Test that non-async nodes can trigger backlog of async nodes
// Test propagation of async values to non-immediate descendants of async
// add barrier support
// Test -1

// Test that non-async nodes can trigger backlog of async nodes
// Test propagation of async values to non-immediate descendants of async
// add barrier support
// Test -1
