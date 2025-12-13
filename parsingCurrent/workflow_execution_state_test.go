package parsing

import (
	"fmt"
	"slices"
	"sort"
	"testing"
)

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
	val, ok := params.LookupParam(pname, pnameArgTypes)
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
		output.AddParam(
			fmt.Sprintf("%d", startVal+i),
			pname,
			pnameArgTypes,
		)
		outputs = append(outputs, output)
	}

	return outputs
}

func assertCompletionVal(
	t *testing.T, state WorkflowExecutionState, expVal bool,
) {
	completed := state.IsComplete()

	if completed != expVal {
		if expVal {
			t.Fatalf("workflow should register as completed")
		} else {
			t.Fatalf("workflow prematurely registered as completed")
		}
	}
}

func assertFailureVal(
	t *testing.T, state WorkflowExecutionState, expVal bool,
) {
	completed := state.HasFailed()

	if completed != expVal {
		if expVal {
			t.Fatalf("workflow should register as failed")
		} else {
			t.Fatalf("workflow should not register as failed")
		}
	}
}

func getEligibleSuccessors(
	t *testing.T, state WorkflowExecutionState, inputs []NodeParams, outputs [][]TypedParams,
) map[int][]NodeParams {
	if len(inputs) != len(outputs) {
		t.Fatal("incorrect invocation of getEligibleSuccessors")
	}

	out := make(map[int][]NodeParams)
	for i := 0; i < len(inputs); i++ {
		succParams, err := state.getSuccParams(inputs[i], outputs[i], true)
		if err != nil {
			t.Fatalf("getEligible successors failed w/ err: %s", err)
		}
		for nodeId, paramList := range succParams {
			if _, ok := out[nodeId]; !ok {
				out[nodeId] = make([]NodeParams, 0)
			}
			out[nodeId] = append(out[nodeId], paramList...)
		}
	}
	return out
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

	index, err := ParseAndValidateWorkflow(workflow)
	if err != nil {
		t.Fatalf("could not build index: %s", err)
	}
	if err := PropagateArgTypes(&workflow); err != nil {
		t.Fatalf("error propagating arg types: %s", err)
	}

	state := NewWorkflowExecutionState(workflow, index)
	startNodes, err := state.getInitialNodeParams()
	if err != nil {
		t.Fatalf("could not get start nodes: %s", err)
	} else if _, ok := startNodes[1]; !ok {
		t.Fatalf("no start node params produced for start node 1")
	}

	p1Vals := genArbitraryOutputs(t, "p1", 1, 0, 1, workflow)
	state.getSuccParams(startNodes[1][0], p1Vals, true)

	numIterationsOf2 := 3
	p2Vals := genArbitraryOutputs(t, "p2", numIterationsOf2, 0, 2, workflow)
	succsOf2, err := state.getSuccParams(
		startNodes[2][0], p2Vals, true,
	)
	inputsFor3 := getSuccInputsOrFail(t, succsOf2, err, 2, 3, numIterationsOf2)

	p2ValsSet := make(map[string]struct{})
	for _, valSet := range p2Vals {
		val, _ := valSet.LookupParam("p2", workflow.Nodes[2].ArgTypes["p2"])
		p2ValsSet[val.(string)] = struct{}{}
	}

	for _, inputSet := range inputsFor3 {
		p1 := getKeyOrFail(t, "p1", 3, inputSet.Params, workflow)
		p2 := getKeyOrFail(t, "p2", 3, inputSet.Params, workflow)
		expectedP1Val := getKeyOrFail(t, "p1", 1, p1Vals[0], workflow)
		if p1 != expectedP1Val {
			t.Fatalf(
				"expected parameter p1 from non-async node 1 to stay static: expected %d, got %d",
				expectedP1Val, p1,
			)
		}
		delete(p2ValsSet, p2.(string))
	}

	if len(p2ValsSet) > 0 {
		t.Fatalf("did not generate input set for 3 with p1 vals %s", p2ValsSet)
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

	index, err := ParseAndValidateWorkflow(workflow)
	if err != nil {
		t.Fatalf("could not build index: %s", err)
	}
	if err := PropagateArgTypes(&workflow); err != nil {
		t.Fatalf("error propagating arg types: %s", err)
	}

	state := NewWorkflowExecutionState(workflow, index)
	startNodes, err := state.getInitialNodeParams()
	if err != nil {
		t.Fatalf("could not get start nodes: %s", err)
	} else if _, ok := startNodes[1]; !ok {
		t.Fatalf("no start node params produced for start node 1")
	}

	// Hash set of (p1, p2) vals that should be present in
	// generated parameters for node 3.
	p1ToP2 := make(map[string]map[string]struct{})

	// Generate 3 values for node 1 param p1
	numIterationsOf1 := 3
	p1Vals := genArbitraryOutputs(t, "p1", numIterationsOf1, 0, 1, workflow)
	succsOf1, err := state.getSuccParams(startNodes[1][0], p1Vals, true)
	inputsFor2 := getSuccInputsOrFail(t, succsOf1, err, 1, 2, numIterationsOf1)

	// The idea here is to correlate different values of p2 with each value of p1.
	// I.e. simulate a situation where for different values of p1, async node p2
	// generates different values of output p2. We also have it generate different
	// numbers of outputs for p2 for different inputs to further test robustness.
	expNumSuccsOf2 := 0
	outputsFor2 := make([][]TypedParams, 0)
	for i, inputFor2 := range inputsFor2 {
		expNumSuccsOf2 += (i + 1)
		p1 := getKeyOrFail(t, "p1", 2, inputFor2.Params, workflow).(string)
		p2Vals := genArbitraryOutputs(t, "p2", (i + 1), (i+1)*len(inputsFor2), 2, workflow)
		outputsFor2 = append(outputsFor2, p2Vals)
		if _, innerSetExists := p1ToP2[p1]; !innerSetExists {
			p1ToP2[p1] = make(map[string]struct{})
		}

		for _, val := range p2Vals {
			p2Val := getKeyOrFail(t, "p2", 2, val, workflow).(string)
			p1ToP2[p1][p2Val] = struct{}{}
		}
		if err != nil {
			t.Fatalf("error adding outputs of 2: %s", err)
		}
	}

	succsOf2 := getEligibleSuccessors(t, state, inputsFor2, outputsFor2)
	inputsFor3 := getSuccInputsOrFail(t, succsOf2, err, 2, 3, expNumSuccsOf2)

	for _, inputSet := range inputsFor3 {
		p1 := getKeyOrFail(t, "p1", 3, inputSet.Params, workflow).(string)
		p2 := getKeyOrFail(t, "p2", 3, inputSet.Params, workflow).(string)
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

	index, err := ParseAndValidateWorkflow(workflow)
	if err != nil {
		t.Fatalf("could not build index: %s", err)
	}
	if err := PropagateArgTypes(&workflow); err != nil {
		t.Fatalf("error propagating arg types: %s", err)
	}

	state := NewWorkflowExecutionState(workflow, index)
	startNodes, err := state.getInitialNodeParams()
	if err != nil {
		t.Fatalf("could not get start nodes: %s", err)
	} else if _, ok := startNodes[1]; !ok {
		t.Fatalf("no start node params produced for start node 1")
	}

	// Hash set of (p1, p2) and (p1, p3) vals that should be present in
	// generated parameters for node 3.
	p1ToP2 := make(map[string]map[string]struct{})
	p1ToP3 := make(map[string]string)

	// Generate 3 values for node 1 param p1
	numIterationsOf1 := 3
	p1Vals := genArbitraryOutputs(t, "p1", numIterationsOf1, 0, 1, workflow)
	succsOf1, err := state.getSuccParams(startNodes[1][0], p1Vals, true)
	inputsFor2 := getSuccInputsOrFail(t, succsOf1, err, 1, 2, numIterationsOf1)

	// The idea here is to correlate different values of p2 with each value of p1.
	// I.e. simulate a situation where for different values of p1, async node p2
	// generates different values of output p2. We also have it generate different
	// numbers of outputs for p2 for different inputs to further test robustness.
	expNumSuccsOf2 := 0
	outputsFor2 := make([][]TypedParams, 0)
	for i, inputFor2 := range inputsFor2 {
		expNumSuccsOf2 += (i + 1)
		p1 := getKeyOrFail(t, "p1", 1, inputFor2.Params, workflow).(string)
		p2Vals := genArbitraryOutputs(t, "p2", (i + 1), (i+1)*len(inputsFor2), 2, workflow)
		outputsFor2 = append(outputsFor2, p2Vals)
		if _, innerSetExists := p1ToP2[p1]; !innerSetExists {
			p1ToP2[p1] = make(map[string]struct{})
		}

		for _, val := range p2Vals {
			p2Val := getKeyOrFail(t, "p2", 2, val, workflow).(string)
			p1ToP2[p1][p2Val] = struct{}{}
		}
	}

	inputsFor3 := getSuccInputsOrFail(t, succsOf1, err, 1, 3, numIterationsOf1)
	outputsFor3 := make([][]TypedParams, 0)
	for i, inputsFor3 := range inputsFor3 {
		p1 := getKeyOrFail(t, "p1", 1, inputsFor3.Params, workflow).(string)
		p3Vals := genArbitraryOutputs(t, "p3", 1, i, 3, workflow)
		outputsFor3 = append(outputsFor3, p3Vals)
		p1ToP3[p1] = getKeyOrFail(t, "p3", 3, p3Vals[0], workflow).(string)
	}

	succsOf2 := getEligibleSuccessors(t, state, inputsFor2, outputsFor2)
	if len(succsOf2) > 0 {
		t.Fatalf("prematurely generated successor for 2")
	}

	succsOf3 := getEligibleSuccessors(t, state, inputsFor3, outputsFor3)
	inputsFor4 := getSuccInputsOrFail(t, succsOf3, err, 3, 4, expNumSuccsOf2)

	for _, inputSet := range inputsFor4 {
		p1 := getKeyOrFail(t, "p1", 4, inputSet.Params, workflow).(string)
		p2 := getKeyOrFail(t, "p2", 4, inputSet.Params, workflow).(string)
		p3 := getKeyOrFail(t, "p3", 4, inputSet.Params, workflow).(string)
		if _, validPair := p1ToP2[p1][p2]; !validPair {
			t.Fatalf("got invalid p1, p2 pair %s, %s", p1, p2)
		}
		if expP3, expP3Exists := p1ToP3[p1]; !expP3Exists || p3 != expP3 {
			t.Fatalf(
				"got invalid p1, p3 pair %s, %s: expected %s, %s",
				p1, p3, p1, expP3,
			)
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

func TestNew(t *testing.T) {
	workflow := Workflow{
		Nodes: map[int]WorkflowNode{
			1: {
				Async: false,
				ArgTypes: map[string]WorkflowArgType{
					"p1": {
						ArgType: "str list",
					},
				},
			},
			2: {
				Async: false,
				ArgTypes: map[string]WorkflowArgType{
					"p2": {
						ArgType: "str",
					},
				},
			},
			3: {
				Async: true,
				ArgTypes: map[string]WorkflowArgType{
					"p1": {
						ArgType: "str list",
					},
					"p3": {
						ArgType: "str",
					},
				},
                Iterate: true,
                IterAttrs: []string{"p1"},
                IterGroupSize: map[string]int{"p1": 1},
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

	index, err := ParseAndValidateWorkflow(workflow)
	if err != nil {
		t.Fatalf("could not build index: %s", err)
	}
	if err := PropagateArgTypes(&workflow); err != nil {
		t.Fatalf("error propagating arg types: %s", err)
	}

	state := NewWorkflowExecutionState(workflow, index)
	startNodes, err := state.getInitialNodeParams()
	if err != nil {
		t.Fatalf("could not get start nodes: %s", err)
	} else if _, ok := startNodes[1]; !ok {
		t.Fatalf("no start node params produced for start node 1")
	}

	// Hash set of (p1, p2) and (p1, p3) vals that should be present in
	// generated parameters for node 3.
	p1ToP2 := make(map[string]map[string]struct{})
	p1ToP3 := make(map[string]string)

	// Generate 3 values for node 1 param p1
	numIterationsOf1 := 1
	p1Vals := TypedParams{StrLists: map[string][]string{"p1": []string{"1", "2", "3"}}}
	succsOf1, err := state.getSuccParams(startNodes[1][0], []TypedParams{p1Vals}, true)

	inputsFor2 := getSuccInputsOrFail(t, succsOf1, err, 1, 2, 1)
	outputsFor2 := make([][]TypedParams, 0)
	for i, inputFor2 := range inputsFor2 {
		fmt.Println(inputFor2)
		p1 := getKeyOrFail(t, "p1", 1, inputFor2.Params, workflow).([]string)[0]
		p2Vals := genArbitraryOutputs(t, "p2", 1, (i+1)*len(inputsFor2), 2, workflow)
		outputsFor2 = append(outputsFor2, p2Vals)
		if _, innerSetExists := p1ToP2[p1]; !innerSetExists {
			p1ToP2[p1] = make(map[string]struct{})
		}

		for _, val := range p2Vals {
			p2Val := getKeyOrFail(t, "p2", 2, val, workflow).(string)
			p1ToP2[p1][p2Val] = struct{}{}
		}
	}

	inputsFor3 := getSuccInputsOrFail(t, succsOf1, err, 1, 3, numIterationsOf1)
	outputsFor3 := make([][]TypedParams, 0)
    PrettyPrint(inputsFor3)
	for i, inputsFor3 := range inputsFor3 {
		p1 := getKeyOrFail(t, "p1", 1, inputsFor3.Params, workflow).([]string)[0]
		p3Vals := genArbitraryOutputs(t, "p3", 3, i, 3, workflow)
		outputsFor3 = append(outputsFor3, p3Vals)
		p1ToP3[p1] = getKeyOrFail(t, "p3", 3, p3Vals[0], workflow).(string)
		succsOf3 := getEligibleSuccessors(t, state, []NodeParams{inputsFor3}, [][]TypedParams{p3Vals})
		fmt.Printf("Succs of 3: %d\n", len(succsOf3[4]))

		if i == 2 {
			succsOf2 := getEligibleSuccessors(t, state, inputsFor2, outputsFor2)
			fmt.Printf("Succs of 2: %d\n", len(succsOf2[4]))
		}
	}
}

// If node 1 is async, node 2 descends from node 1, and node 3 descends from
// node 2 (i.e. graph is 1 (async) -> 2 -> 3 with no other links), 3 should
// have one input set for each iteration of its async ancestor 1, even though
// it does not share a link with it.
func TestAsyncPropagationWithoutDirectLink(t *testing.T) {
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
				Async: false,
				ArgTypes: map[string]WorkflowArgType{
					"p2": {
						ArgType: "str",
					},
				},
			},
			3: {
				Async: false,
				ArgTypes: map[string]WorkflowArgType{
					"p2": {
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
				SourceChannel: "p2",
				SinkChannel:   "p2",
			},
		},
	}

	index, err := ParseAndValidateWorkflow(workflow)
	if err != nil {
		t.Fatalf("could not build index: %s", err)
	}
	if err := PropagateArgTypes(&workflow); err != nil {
		t.Fatalf("error propagating arg types: %s", err)
	}

	state := NewWorkflowExecutionState(workflow, index)
	startNodes, err := state.getInitialNodeParams()
	if err != nil {
		t.Fatalf("could not get start nodes: %s", err)
	} else if _, ok := startNodes[1]; !ok {
		t.Fatalf("no start node params produced for start node 1")
	}

	p2Set := make(map[string]struct{})

	// Generate 3 values for node 1 param p1
	numIterationsOf1 := 3
	p1Vals := genArbitraryOutputs(t, "p1", numIterationsOf1, 0, 1, workflow)
	succsOf1, err := state.getSuccParams(startNodes[1][0], p1Vals, true)
	inputsFor2 := getSuccInputsOrFail(t, succsOf1, err, 1, 2, numIterationsOf1)

	outputsFor2 := make([][]TypedParams, 0)
	for i := range inputsFor2 {
		p2Set[fmt.Sprintf("%d", i)] = struct{}{}
		outputsFor2 = append(
			outputsFor2, []TypedParams{{
				Strings: map[string]string{"p2": fmt.Sprintf("%d", i)},
			}},
		)
	}

	succsOf2 := getEligibleSuccessors(t, state, inputsFor2, outputsFor2)
	inputsFor3 := getSuccInputsOrFail(t, succsOf2, err, 2, 3, numIterationsOf1)
	for _, inputsFor3 := range inputsFor3 {
		p2 := getKeyOrFail(t, "p2", 2, inputsFor3.Params, workflow).(string)
		if _, validP2 := p2Set[p2]; !validP2 {
			t.Fatalf("got invalid p2 val %s", p2)
		}
		delete(p2Set, p2)
	}

	if len(p2Set) != 0 {
		t.Fatalf("failed to generate p2 values %v", p2Set)
	}
}

func TestSimpleBarrier(t *testing.T) {
	lvalFor1 := 1
	workflow := Workflow{
		Nodes: map[int]WorkflowNode{
			1: {
				Async: true,
				ArgTypes: map[string]WorkflowArgType{
					"p0": {
						ArgType: "str",
					},
					"p1": {
						ArgType: "str",
					},
				},
			},
			2: {
				Async: false,
				ArgTypes: map[string]WorkflowArgType{
					"p2": {
						ArgType: "str",
					},
				},
			},
			3: {
				Async:      false,
				BarrierFor: &lvalFor1,
				ArgTypes: map[string]WorkflowArgType{
					"p0": {
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
				SourceChannel: "p0",
				SinkChannel:   "p0",
			},
		},
	}

	index, err := ParseAndValidateWorkflow(workflow)
	if err != nil {
		t.Fatalf("could not build index: %s", err)
	}
	if err := PropagateArgTypes(&workflow); err != nil {
		t.Fatalf("error propagating arg types: %s", err)
	}

	state := NewWorkflowExecutionState(workflow, index)
	startNodes, err := state.getInitialNodeParams()
	if err != nil {
		t.Fatalf("could not get start nodes: %s", err)
	} else if _, ok := startNodes[1]; !ok {
		t.Fatalf("no start node params produced for start node 1")
	}

	numOutputsOf1 := 3
	inputsFor1 := startNodes[1][0]
	inputsFor1.Params.AddParam("0", "p0", workflow.Nodes[1].ArgTypes["p0"])
	p1Vals := genArbitraryOutputs(t, "p1", numOutputsOf1, 0, 1, workflow)
	succsOf1, err := state.getSuccParams(startNodes[1][0], p1Vals, true)
	if err != nil {
		t.Fatalf("error getting succs of 1: %s", err)
	}
	inputsFor2 := getSuccInputsOrFail(t, succsOf1, err, 1, 2, numOutputsOf1)

	for i, inputSet := range inputsFor2 {
		p2Vals := genArbitraryOutputs(t, "p2", 1, 0, 2, workflow)
		succsOf1, err := state.getSuccParams(inputSet, p2Vals, true)
		if err != nil {
			t.Fatalf("error generating succs of 1: %s", err)
		}
		if i < len(inputsFor2)-1 && len(succsOf1[3]) > 0 {
			t.Fatalf("generated barrier inputs before async block completed")
		} else if i == len(inputsFor2)-1 && len(succsOf1[3]) != 1 {
			t.Fatalf(
				"expected 1 output for barrier 3 after async block completed, got %d",
				len(succsOf1),
			)
		}
	}
}

func TestBarrierReduction(t *testing.T) {
	lvalFor1 := 1
	workflow := Workflow{
		Nodes: map[int]WorkflowNode{
			1: {
				Async: true,
				ArgTypes: map[string]WorkflowArgType{
					"p0": {
						ArgType: "str",
					},
					"p1": {
						ArgType: "str",
					},
				},
			},
			2: {
				Async: false,
				ArgTypes: map[string]WorkflowArgType{
					"p2": {
						ArgType: "str",
					},
				},
			},
			3: {
				Async:      false,
				BarrierFor: &lvalFor1,
				ArgTypes: map[string]WorkflowArgType{
					"p2": {
						ArgType: "str list",
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
				SourceChannel: "p2",
				SinkChannel:   "p2",
			},
		},
	}

	index, err := ParseAndValidateWorkflow(workflow)
	if err != nil {
		t.Fatalf("could not build index: %s", err)
	}
	if err := PropagateArgTypes(&workflow); err != nil {
		t.Fatalf("error propagating arg types: %s", err)
	}

	state := NewWorkflowExecutionState(workflow, index)
	startNodes, err := state.getInitialNodeParams()
	if err != nil {
		t.Fatalf("could not get start nodes: %s", err)
	} else if _, ok := startNodes[1]; !ok {
		t.Fatalf("no start node params produced for start node 1")
	}

	numOutputsOf1 := 3
	inputsFor1 := startNodes[1][0]
	inputsFor1.Params.AddParam("0", "p0", workflow.Nodes[1].ArgTypes["p0"])
	p1Vals := genArbitraryOutputs(t, "p1", numOutputsOf1, 0, 1, workflow)
	succsOf1, err := state.getSuccParams(startNodes[1][0], p1Vals, true)
	if err != nil {
		t.Fatalf("error getting succs of 1: %s", err)
	}

	inputsFor2 := getSuccInputsOrFail(t, succsOf1, err, 1, 2, numOutputsOf1)
	for i, inputSet := range inputsFor2 {
		p2Vals := genArbitraryOutputs(t, "p2", 1, 0, 2, workflow)
		succsOf1, err := state.getSuccParams(inputSet, p2Vals, true)
		if err != nil {
			t.Fatalf("error generating succs of 1: %s", err)
		}
		if i < len(inputsFor2)-1 && len(succsOf1[3]) > 0 {
			t.Fatalf("generated barrier inputs before async block completed")
		} else if i == len(inputsFor2)-1 {
			if len(succsOf1[3]) != 1 {
				t.Fatalf(
					"expected 1 output for barrier 3 after async block completed, got %d",
					len(succsOf1),
				)
			}

			p2Out, ok := succsOf1[3][0].Params.LookupParam("p2", WorkflowArgType{ArgType: "str list"})
			p2Strs, convOk := p2Out.([]string)
			if !ok || !convOk {
				t.Fatalf(
					"parameters for node 3 should contain str list key `p2`",
				)
			}

			expP2 := []string{"0", "0", "0"}
			if !slices.Equal(p2Strs, expP2) {
				t.Fatalf(
					"expected node 3 to have p2 values %#v, got %#v",
					expP2, p2Out,
				)
			}

		}

	}
}

func TestMultipleBarrier(t *testing.T) {
	lvalFor0 := 0
	lvalFor1 := 1
	workflow := Workflow{
		Nodes: map[int]WorkflowNode{
			0: {
				Async: true,
				ArgTypes: map[string]WorkflowArgType{
					"pInit": {
						ArgType: "str",
					},
					"p0": {
						ArgType: "str",
					},
				},
			},
			1: {
				Async: true,
				ArgTypes: map[string]WorkflowArgType{
					"p0": {
						ArgType: "str",
					},
					"p1": {
						ArgType: "str",
					},
				},
			},
			2: {
				Async: false,
				ArgTypes: map[string]WorkflowArgType{
					"p0": {
						ArgType: "str",
					},
					"p1": {
						ArgType: "str",
					},
				},
			},
			3: {
				Async:      false,
				BarrierFor: &lvalFor1,
				ArgTypes: map[string]WorkflowArgType{
					"pInit": {
						ArgType: "str",
					},
					"p0": {
						ArgType: "str",
					},
				},
			},
			4: {
				Async:      false,
				BarrierFor: &lvalFor0,
				ArgTypes: map[string]WorkflowArgType{
					"pInit": {
						ArgType: "str list",
					},
				},
			},
		},
		Links: []WorkflowLink{
			{
				SourceNodeId:  0,
				SinkNodeId:    1,
				SourceChannel: "p0",
				SinkChannel:   "p0",
			},
			{
				SourceNodeId:  1,
				SinkNodeId:    2,
				SourceChannel: "p0",
				SinkChannel:   "p0",
			},
			{
				SourceNodeId:  1,
				SinkNodeId:    2,
				SourceChannel: "p1",
				SinkChannel:   "p1",
			},
			{
				SourceNodeId:  1,
				SinkNodeId:    3,
				SourceChannel: "p0",
				SinkChannel:   "p0",
			},
			{
				SourceNodeId:  0,
				SinkNodeId:    3,
				SourceChannel: "pInit",
				SinkChannel:   "pInit",
			},
			{
				SourceNodeId:  3,
				SinkNodeId:    4,
				SourceChannel: "pInit",
				SinkChannel:   "pInit",
			},
		},
	}

	index, err := ParseAndValidateWorkflow(workflow)
	if err != nil {
		t.Fatalf("could not build index: %s", err)
	}
	if err := PropagateArgTypes(&workflow); err != nil {
		t.Fatalf("error propagating arg types: %s", err)
	}

	state := NewWorkflowExecutionState(workflow, index)
	startNodes, err := state.getInitialNodeParams()
	if err != nil {
		t.Fatalf("could not get start nodes: %s", err)
	} else if _, ok := startNodes[0]; !ok {
		t.Fatalf("no start node params produced for start node 0")
	}

	numOutputsOf0 := 3
	inputsFor0 := startNodes[0][0]
	inputsFor0.Params.AddParam("0", "pInit", workflow.Nodes[0].ArgTypes["pInit"])
	p0Vals := genArbitraryOutputs(t, "p0", numOutputsOf0, 0, 1, workflow)
	succsOf0, err := state.getSuccParams(inputsFor0, p0Vals, true)

	inputsFor1 := getSuccInputsOrFail(t, succsOf0, err, 0, 1, numOutputsOf0)
	outputsFor1 := make([][]TypedParams, 0)
	numOutputsOf1 := 0
	for i := range inputsFor1 {
		p1Vals := genArbitraryOutputs(t, "p1", 3, i*2, 1, workflow)
		outputsFor1 = append(outputsFor1, p1Vals)
		numOutputsOf1 += 3
	}

	succsOf1 := getEligibleSuccessors(t, state, inputsFor1, outputsFor1)
	inputsFor2 := getSuccInputsOrFail(t, succsOf1, err, 1, 2, numOutputsOf1)

	// Map 0 run ID -> 1 run ID.
	inputsFor3 := make([]NodeParams, 0)
	runIdGroups := make(map[int]map[int]struct{})
	for _, inputSet := range inputsFor2 {
		zeroRunId := inputSet.AncList[1]
		oneRunId := inputSet.AncList[2]

		if runIdGroups[zeroRunId] == nil {
			runIdGroups[zeroRunId] = make(map[int]struct{})
		}
		runIdGroups[zeroRunId][oneRunId] = struct{}{}
	}

	for _, inputSet := range inputsFor2 {
		numOutputsOf1 += 3
		zeroRunId := inputSet.AncList[1]
		oneRunId := inputSet.AncList[2]
		delete(runIdGroups[zeroRunId], oneRunId)

		succsOf1, err = state.getSuccParams(inputSet, []TypedParams{{}}, true)
		if err != nil {
			t.Fatalf("error generating succs of 1: %s", err)
		}

		if len(runIdGroups[zeroRunId]) > 0 && len(succsOf1[3]) > 0 {
			t.Fatalf("generated barrier inputs before async block completed")
		} else if len(runIdGroups[zeroRunId]) == 0 && len(succsOf1[3]) != 1 {
			t.Fatalf(
				"completion of 0 async group %d triggered %d succs, expected 1",
				zeroRunId, len(succsOf1[3]),
			)
		} else if len(succsOf1[3]) == 1 {
			succZeroRunId := succsOf1[3][0].AncList[1]
			if zeroRunId != succZeroRunId {
				t.Fatalf(
					"generated barrier for 0 run ID %d, expected one for %d",
					succZeroRunId, zeroRunId,
				)
			}
			delete(runIdGroups, zeroRunId)
			inputsFor3 = append(inputsFor3, succsOf1[3][0])
		}
	}

	if len(runIdGroups) != 0 {
		t.Fatalf(
			"failed to generate barrier run for some zero run IDs: %v",
			runIdGroups,
		)
	}

	for i, inputSet := range inputsFor3 {
		if err != nil {
			t.Fatalf("error adding results of 3: %s", err)
		}

		succsOf3, err := state.getSuccParams(inputSet, []TypedParams{{}}, true)
		if err != nil {
			t.Fatalf("error getting succs of 3: %s", err)
		}

		if i < len(inputsFor3)-1 && len(succsOf3[4]) > 0 {
			t.Fatalf(
				"produced %d inputs for 4 before all instances of 3 have completed",
				len(inputsFor3),
			)
		} else if i == len(inputsFor3)-1 && len(succsOf3[4]) != 1 {
			t.Fatalf(
				"produced %d inputs for barrier node 4, expected 1",
				len(succsOf3[4]),
			)
		}
	}
}

func TestSimpleWorkflowCompletion(t *testing.T) {
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
		},
	}

	index, err := ParseAndValidateWorkflow(workflow)
	if err != nil {
		t.Fatalf("could not build index: %s", err)
	}
	if err := PropagateArgTypes(&workflow); err != nil {
		t.Fatalf("error propagating arg types: %s", err)
	}

	state := NewWorkflowExecutionState(workflow, index)
	startNodes, err := state.getInitialNodeParams()
	if err != nil {
		t.Fatalf("could not get start nodes: %s", err)
	} else if _, ok := startNodes[1]; !ok {
		t.Fatalf("no start node params produced for start node 1")
	}

	assertCompletionVal(t, state, false)

	p1Vals := genArbitraryOutputs(t, "p1", 1, 0, 1, workflow)
	succsOf1, err := state.getSuccParams(startNodes[1][0], p1Vals, true)
	inputsFor2 := getSuccInputsOrFail(t, succsOf1, err, 1, 2, 1)

	_, err = state.getSuccParams(inputsFor2[0], []TypedParams{{}}, true)
	if err != nil {
		t.Fatalf("error adding outputs for node 2: %s", err)
	}

	assertCompletionVal(t, state, true)
}

func TestAsyncWorkflowCompletion(t *testing.T) {
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

	index, err := ParseAndValidateWorkflow(workflow)
	if err != nil {
		t.Fatalf("could not build index: %s", err)
	}
	if err := PropagateArgTypes(&workflow); err != nil {
		t.Fatalf("error propagating arg types: %s", err)
	}

	state := NewWorkflowExecutionState(workflow, index)
	startNodes, err := state.getInitialNodeParams()
	if err != nil {
		t.Fatalf("could not get start nodes: %s", err)
	} else if _, ok := startNodes[1]; !ok {
		t.Fatalf("no start node params produced for start node 1")
	}

	// Generate 3 values for node 1 param p1
	numIterationsOf1 := 3
	p1Vals := genArbitraryOutputs(t, "p1", numIterationsOf1, 0, 1, workflow)
	succsOf1, err := state.getSuccParams(startNodes[1][0], p1Vals, true)
	inputsFor2 := getSuccInputsOrFail(t, succsOf1, err, 1, 2, numIterationsOf1)

	// The idea here is to correlate different values of p2 with each value of p1.
	// I.e. simulate a situation where for different values of p1, async node p2
	// generates different values of output p2. We also have it generate different
	// numbers of outputs for p2 for different inputs to further test robustness.
	expNumSuccsOf2 := 0
	succsOf2 := make(map[int][]NodeParams)
	for i, inputFor2 := range inputsFor2 {
		expNumSuccsOf2 += (i + 1)
		p2Vals := genArbitraryOutputs(t, "p2", (i + 1), (i+1)*len(inputsFor2), 2, workflow)
		iterSuccsOf2, err := state.getSuccParams(inputFor2, p2Vals, true)
		if err != nil {
			t.Fatalf("error adding outputs of 2: %s", err)
		}

		for succId, succSet := range iterSuccsOf2 {
			succsOf2[succId] = append(succsOf2[succId], succSet...)
		}
		assertCompletionVal(t, state, false)
	}

	inputsFor3 := getSuccInputsOrFail(t, succsOf2, err, 2, 3, expNumSuccsOf2)
	for i, inputFor3 := range inputsFor3 {
		err := state.addCmdResults(inputFor3, []TypedParams{{}})
		if err != nil {
			t.Fatalf("error adding outputs of 2: %s", err)
		}

		if i < len(inputsFor3)-1 {
			assertCompletionVal(t, state, false)
		}
	}
	assertCompletionVal(t, state, true)
}

func TestAsyncWorkflowFailure(t *testing.T) {
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

	index, err := ParseAndValidateWorkflow(workflow)
	if err != nil {
		t.Fatalf("could not build index: %s", err)
	}
	if err := PropagateArgTypes(&workflow); err != nil {
		t.Fatalf("error propagating arg types: %s", err)
	}

	state := NewWorkflowExecutionState(workflow, index)
	startNodes, err := state.getInitialNodeParams()
	if err != nil {
		t.Fatalf("could not get start nodes: %s", err)
	} else if _, ok := startNodes[1]; !ok {
		t.Fatalf("no start node params produced for start node 1")
	}

	// Generate 3 values for node 1 param p1
	numIterationsOf1 := 3
	p1Vals := genArbitraryOutputs(t, "p1", numIterationsOf1, 0, 1, workflow)
	succsOf1, err := state.getSuccParams(startNodes[1][0], p1Vals, true)
	inputsFor2 := getSuccInputsOrFail(t, succsOf1, err, 1, 2, numIterationsOf1)

	// The idea here is to correlate different values of p2 with each value of p1.
	// I.e. simulate a situation where for different values of p1, async node p2
	// generates different values of output p2. We also have it generate different
	// numbers of outputs for p2 for different inputs to further test robustness.
	expNumSuccsOf2 := 0
	succsOf2 := make(map[int][]NodeParams)
	for i, inputFor2 := range inputsFor2 {
		p2Vals := genArbitraryOutputs(t, "p2", (i + 1), (i+1)*len(inputsFor2), 2, workflow)
		failure := i == 0
		iterSuccsOf2, err := state.getSuccParams(inputFor2, p2Vals, !failure)
		if err != nil {
			t.Fatalf("error adding outputs of 2: %s", err)
		}

		if failure && len(iterSuccsOf2[3]) > 0 {
			t.Fatalf("produced inputs for node 3 even after predecessor failed")
		} else if !failure && len(iterSuccsOf2[3]) != i+1 {
			t.Fatalf("failed to produce inputs for node 3 even though predecessor did not fail")
		}

		if !failure {
			expNumSuccsOf2 += (i + 1)
		}

		for succId, succSet := range iterSuccsOf2 {
			succsOf2[succId] = append(succsOf2[succId], succSet...)
		}
		assertCompletionVal(t, state, false)
		assertFailureVal(t, state, true)
	}

	inputsFor3 := getSuccInputsOrFail(t, succsOf2, err, 2, 3, expNumSuccsOf2)
	for i, inputFor3 := range inputsFor3 {
		err := state.addCmdResults(inputFor3, []TypedParams{{}})
		if err != nil {
			t.Fatalf("error adding outputs of 2: %s", err)
		}

		if i < len(inputsFor3)-1 {
			assertCompletionVal(t, state, false)
			assertFailureVal(t, state, true)
		}
	}
	assertCompletionVal(t, state, true)
	assertFailureVal(t, state, true)
}

// Ensure barrier does not run if a predecessor has failed,
// but that workflow registers as complete once all nodes
// prior to the barrier not descended from a failed node run
// have completed. Also ensure that the workflow continually
// registers as failed from the moment of the first failure.
func TestWorkflowFailureWithBarrier(t *testing.T) {
	lvalFor1 := 1
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
				Async: false,
				ArgTypes: map[string]WorkflowArgType{
					"p2": {
						ArgType: "str",
					},
				},
			},
			3: {
				Async: false,
				ArgTypes: map[string]WorkflowArgType{
					"p3": {
						ArgType: "str",
					},
				},
			},
			4: {
				Async:      false,
				BarrierFor: &lvalFor1,
				ArgTypes: map[string]WorkflowArgType{
					"p3": {
						ArgType: "str list",
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

	index, err := ParseAndValidateWorkflow(workflow)
	if err != nil {
		t.Fatalf("could not build index: %s", err)
	}
	if err := PropagateArgTypes(&workflow); err != nil {
		t.Fatalf("error propagating arg types: %s", err)
	}

	state := NewWorkflowExecutionState(workflow, index)
	startNodes, err := state.getInitialNodeParams()
	if err != nil {
		t.Fatalf("could not get start nodes: %s", err)
	} else if _, ok := startNodes[1]; !ok {
		t.Fatalf("no start node params produced for start node 1")
	}

	numOutputsOf1 := 3
	inputsFor1 := startNodes[1][0]
	inputsFor1.Params.AddParam("0", "p0", workflow.Nodes[1].ArgTypes["p0"])
	p1Vals := genArbitraryOutputs(t, "p1", numOutputsOf1, 0, 1, workflow)
	succsOf1, err := state.getSuccParams(startNodes[1][0], p1Vals, true)
	inputsFor2 := getSuccInputsOrFail(t, succsOf1, err, 1, 2, numOutputsOf1)
	for i, inputSet := range inputsFor2 {
		failure := i == 0
		numVals2 := 1
		p2Vals := genArbitraryOutputs(t, "p2", numVals2, 0, 2, workflow)
		succsOf2, err := state.getSuccParams(inputSet, p2Vals, !failure)
		if err != nil {
			t.Fatalf("error generating succs of 2: %s", err)
		}

		if failure && len(succsOf2[3]) != 0 {
			t.Fatalf("produced output for node 3 after failure of node 2: %v", succsOf1[3])
		} else if !failure && len(succsOf2) != numVals2 {
			t.Fatalf("failed to produce output for node 3 after successful run of node 2")
		}

		for _, inputSetFor3 := range succsOf2[3] {
			assertCompletionVal(t, state, false)
			assertFailureVal(t, state, true)
			p3Vals := genArbitraryOutputs(t, "p3", 1, 0, 3, workflow)
			succsOf3, err := state.getSuccParams(inputSetFor3, p3Vals, true)
			if err != nil {
				t.Fatalf("error generating succs of 3: %s", err)
			}

			if len(succsOf3[4]) > 0 {
				t.Fatalf("produced run of barrier 4 even though predecessor node 2 had failed run")
			}
		}
	}
	assertCompletionVal(t, state, true)
	assertFailureVal(t, state, true)
}

// The following tests are for internal methods of WorkflowExecutionState.
// These may need to be changed if the internals of that module ever change.
func TestNodeLookup(t *testing.T) {
	ne := NodeExec{
		NodeId: 1,
		Succs: map[int]map[int]*NodeExec{
			2: {
				0: {
					Id: 1,
				},
				1: {
					Id: 2,
				},
				2: {
					Id: 3,
				},
			},
		},
	}

	nt := WorkflowExecutionState{
		Root: &NodeExec{
			NodeId:   -1,
			Finished: true,
			Succs: map[int]map[int]*NodeExec{
				1: {0: &ne},
			},
		},
		Index: WorkflowIndex{
			AsyncAncestors: map[int][]int{
				2: {-1, 1},
			},
		},
	}

	out := nt.lookupNode(2, []int{0, 2})
	if out == nil {
		t.Fatalf("failed to lookup node 2 w/ anc list [0, 2]")
	} else if out.Id != 3 {
		t.Fatalf("retrieved incorrect node")

	}
}

// Consider lists as equal if they contain the same
// elements, regardless of ordering
func orderAgnosticListEq(ac1, ac2 [][]int) bool {
	if len(ac1) != len(ac2) {
		return false
	}

	ac1ListStrs := make([]string, 0)
	for _, ac := range ac1 {
		acStr := fmt.Sprintf("%v", ac)
		ac1ListStrs = append(ac1ListStrs, acStr)
	}

	ac2ListStrs := make([]string, 0)
	for _, ac := range ac2 {
		acStr := fmt.Sprintf("%v", ac)
		ac2ListStrs = append(ac2ListStrs, acStr)
	}

	sort.Strings(ac1ListStrs)
	sort.Strings(ac2ListStrs)
	return slices.Equal(ac1ListStrs, ac2ListStrs)
}

func TestGetDesc(t *testing.T) {
	ne := NodeExec{
		NodeId: 1,
		Succs: map[int]map[int]*NodeExec{
			2: {
				0: &NodeExec{
					Id:       1,
					Finished: true,
				},
				1: &NodeExec{
					Id: 1,
				},
			},
			3: {
				0: &NodeExec{
					Id:       2,
					Outputs:  []TypedParams{{}, {}},
					Finished: true,
				},
				1: &NodeExec{
					Id:      5,
					Outputs: []TypedParams{{}, {}, {}},
					Succs: map[int]map[int]*NodeExec{
						4: {
							0: &NodeExec{
								Id:      6,
								Outputs: []TypedParams{{}},
							}, 1: &NodeExec{
								Id:      7,
								Outputs: []TypedParams{{}},
							},
						},
					},
				},
			},
		},
	}

	nt := WorkflowExecutionState{
		Root: &NodeExec{
			Succs: map[int]map[int]*NodeExec{
				1: {0: &ne},
			},
		}, Index: WorkflowIndex{
			AsyncAncestors: map[int][]int{
				1: {-1},
				2: {-1, 1},
				3: {-1, 1},
				4: {-1, 1, 3},
			},
			Preds: map[int][]int{
				1: {},
				2: {1},
				3: {1},
				4: {1, 2},
			},
		},
	}

	tests := []struct {
		name      string
		node      int
		prefix    []int
		candidate bool
		expected  [][]int
	}{
		{
			name:      "CandAncListsWithoutOutput",
			node:      4,
			prefix:    []int{0, 0},
			candidate: true,
			expected:  [][]int{{0, 0, 0}, {0, 0, 1}},
		}, {
			name:      "NonCandAncListsWithoutOutput",
			node:      4,
			prefix:    []int{0, 0},
			candidate: false,
			expected:  [][]int{},
		}, {
			name:      "CandAncListsWithOutput",
			node:      4,
			prefix:    []int{0, 1},
			candidate: true,
			expected:  [][]int{{0, 1, 0}, {0, 1, 1}, {0, 1, 2}},
		}, {
			name:      "NonCandAncListsWithOutput",
			node:      4,
			prefix:    []int{0, 1},
			candidate: false,
			expected:  [][]int{{0, 1, 0}, {0, 1, 1}},
		}, {
			name:      "NonImmediateNonCandAncListWithOutput",
			node:      4,
			prefix:    []int{0},
			candidate: false,
			expected:  [][]int{{0, 1, 0}, {0, 1, 1}},
		}, {
			name:      "NonImmediateCandAncListWithOutput",
			node:      4,
			prefix:    []int{0},
			candidate: true,
			expected:  [][]int{{0, 1, 0}, {0, 1, 1}, {0, 1, 2}, {0, 0, 0}, {0, 0, 1}},
		}, {
			name:      "NonImmediateCandAncListWithoutOutput",
			node:      4,
			prefix:    []int{1},
			candidate: true,
			expected:  [][]int{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			descAncLists, err := nt.getAncListsWithPrefix(tt.node, tt.prefix, tt.candidate)
			if err != nil {
				t.Fatalf("getting anc lists failed with error %s", err)
			}
			if len(descAncLists) != len(tt.expected) {
				t.Fatalf(
					"expected %d ancestor lists for %d prefixed by %#v, "+
						"got %d: %#v", len(tt.expected), tt.node, tt.prefix,
					len(descAncLists), descAncLists,
				)
			}

			if !orderAgnosticListEq(descAncLists, tt.expected) {
				t.Fatalf(
					"expected ancestor lists %v for node %d, prefix %v, "+
						"got %#v", tt.expected, tt.node, tt.prefix,
					descAncLists,
				)
			}
		})
	}
}
