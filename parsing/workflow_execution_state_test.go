package parsing

import (
	"fmt"
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
	completed, err := state.IsComplete()
	if err != nil {
		t.Fatalf("failed to get completion val of workflow w/ err: %s", err)
	}

	if completed != expVal {
		if expVal {
			t.Fatalf("workflow should register as completed")
		} else {
			t.Fatalf("workflow prematurely registered as completed")
		}
	}
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

	index, err := ParseAndValidateWorkflow(&workflow)
	if err != nil {
		t.Fatalf("could not build index: %s", err)
	}

	state := NewWorkflowExecutionState(workflow, index)
	startNodes, err := state.getInitialNodeParams()
	if err != nil {
		t.Fatalf("could not get start nodes: %s", err)
	} else if _, ok := startNodes[1]; !ok {
		t.Fatalf("no start node params produced for start node 1")
	}

	p1Vals := genArbitraryOutputs(t, "p1", 1, 0, 1, workflow)
	state.getSuccParams(startNodes[1][0], p1Vals)

	numIterationsOf2 := 3
	p2Vals := genArbitraryOutputs(t, "p2", numIterationsOf2, 0, 2, workflow)
	succsOf2, err := state.getSuccParams(
		NodeParams{ancList: []int{}, nodeId: 2}, p2Vals,
	)
	inputsFor3 := getSuccInputsOrFail(t, succsOf2, err, 2, 3, numIterationsOf2)

	p2ValsSet := make(map[string]struct{})
	for _, valSet := range p2Vals {
		val, _ := valSet.LookupParam("p2", workflow.Nodes[2].ArgTypes["p2"])
		p2ValsSet[val.(string)] = struct{}{}
	}

	for _, inputSet := range inputsFor3 {
		p1 := getKeyOrFail(t, "p1", 3, inputSet.params, workflow)
		p2 := getKeyOrFail(t, "p2", 3, inputSet.params, workflow)
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

	index, err := ParseAndValidateWorkflow(&workflow)
	if err != nil {
		t.Fatalf("could not build index: %s", err)
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
	succsOf1, err := state.getSuccParams(startNodes[1][0], p1Vals)
	inputsFor2 := getSuccInputsOrFail(t, succsOf1, err, 1, 2, numIterationsOf1)

	// The idea here is to correlate different values of p2 with each value of p1.
	// I.e. simulate a situation where for different values of p1, async node p2
	// generates different values of output p2. We also have it generate different
	// numbers of outputs for p2 for different inputs to further test robustness.
	expNumSuccsOf2 := 0
	for i, inputFor2 := range inputsFor2 {
		expNumSuccsOf2 += (i + 1)
		p1 := getKeyOrFail(t, "p1", 2, inputFor2.params, workflow).(string)
		p2Vals := genArbitraryOutputs(t, "p2", (i + 1), (i+1)*len(inputsFor2), 2, workflow)
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

	index, err := ParseAndValidateWorkflow(&workflow)
	if err != nil {
		t.Fatalf("could not build index: %s", err)
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
	succsOf1, err := state.getSuccParams(startNodes[1][0], p1Vals)
	inputsFor2 := getSuccInputsOrFail(t, succsOf1, err, 1, 2, numIterationsOf1)

	// The idea here is to correlate different values of p2 with each value of p1.
	// I.e. simulate a situation where for different values of p1, async node p2
	// generates different values of output p2. We also have it generate different
	// numbers of outputs for p2 for different inputs to further test robustness.
	expNumSuccsOf2 := 0
	for i, inputFor2 := range inputsFor2 {
		expNumSuccsOf2 += (i + 1)
		p1 := getKeyOrFail(t, "p1", 1, inputFor2.params, workflow).(string)
		p2Vals := genArbitraryOutputs(t, "p2", (i + 1), (i+1)*len(inputsFor2), 2, workflow)
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
		err := state.addCmdResults(inputsFor3, p3Vals)
		if err != nil {
			t.Fatalf("error adding outputs of 3: %s", err)
		}
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

		delete(p1ToP2[p1], p2)
		if len(p1ToP2[p1]) == 0 {
			delete(p1ToP2, p1)
		}
	}

	if len(p1ToP2) > 0 {
		t.Fatalf("did not generate all p1, p2 pairs: remaining are %v", p1ToP2)
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

	index, err := ParseAndValidateWorkflow(&workflow)
	if err != nil {
		t.Fatalf("could not build index: %s", err)
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
	succsOf1, err := state.getSuccParams(startNodes[1][0], p1Vals)
	inputsFor2 := getSuccInputsOrFail(t, succsOf1, err, 1, 2, numIterationsOf1)

	for i, inputFor2 := range inputsFor2 {
		p2Set[fmt.Sprintf("%d", i)] = struct{}{}
		err := state.addCmdResults(
			inputFor2,
			[]TypedParams{{
				Strings: map[string]string{"p2": fmt.Sprintf("%d", i)},
			}},
		)
		if err != nil {
			t.Fatalf("error adding outputs of 2: %s", err)
		}
	}

	succsOf2, err := state.getEligibleSuccessors(2)
	inputsFor3 := getSuccInputsOrFail(t, succsOf2, err, 2, 3, numIterationsOf1)
	for _, inputsFor3 := range inputsFor3 {
		p2 := getKeyOrFail(t, "p2", 2, inputsFor3.params, workflow).(string)
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

	index, err := ParseAndValidateWorkflow(&workflow)
	if err != nil {
		t.Fatalf("could not build index: %s", err)
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
	inputsFor1.params.AddParam("0", "p0", workflow.Nodes[1].ArgTypes["p0"])
	p1Vals := genArbitraryOutputs(t, "p1", numOutputsOf1, 0, 1, workflow)
	succsOf1, err := state.getSuccParams(startNodes[1][0], p1Vals)
	inputsFor2 := getSuccInputsOrFail(t, succsOf1, err, 1, 2, numOutputsOf1)
	for i, inputSet := range inputsFor2 {
		p2Vals := genArbitraryOutputs(t, "p2", 1, 0, 2, workflow)
		err := state.addCmdResults(inputSet, p2Vals)
		if err != nil {
			t.Fatalf("error adding outputs of 2: %s", err)
		}

		succsOf1, err := state.getEligibleSuccessors(1)
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
						ArgType: "str",
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

	index, err := ParseAndValidateWorkflow(&workflow)
	if err != nil {
		t.Fatalf("could not build index: %s", err)
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
	inputsFor0.params.AddParam("0", "pInit", workflow.Nodes[0].ArgTypes["pInit"])
	p0Vals := genArbitraryOutputs(t, "p0", numOutputsOf0, 0, 1, workflow)
	succsOf0, err := state.getSuccParams(inputsFor0, p0Vals)

	inputsFor1 := getSuccInputsOrFail(t, succsOf0, err, 0, 1, numOutputsOf0)
	numOutputsOf1 := 0
	for i, inputSet := range inputsFor1 {
		p1Vals := genArbitraryOutputs(t, "p1", 3, i*2, 1, workflow)
		numOutputsOf1 += 3
		err := state.addCmdResults(inputSet, p1Vals)
		if err != nil {
			t.Fatalf("error adding outputs of 1: %s", err)
		}
	}

	succsOf1, err := state.getEligibleSuccessors(1)
	inputsFor2 := getSuccInputsOrFail(t, succsOf1, err, 1, 2, numOutputsOf1)

	// Map 0 run ID -> 1 run ID.
	inputsFor3 := make([]NodeParams, 0)
	runIdGroups := make(map[int]map[int]struct{})
	for _, inputSet := range inputsFor2 {
		zeroRunId := inputSet.ancList[0]
		oneRunId := inputSet.ancList[1]

		if runIdGroups[zeroRunId] == nil {
			runIdGroups[zeroRunId] = make(map[int]struct{})
		}
		runIdGroups[zeroRunId][oneRunId] = struct{}{}
	}

	for _, inputSet := range inputsFor2 {
		numOutputsOf1 += 3
		err := state.addCmdResults(inputSet, []TypedParams{{}})
		if err != nil {
			t.Fatalf("error adding outputs of 2: %s", err)
		}

		zeroRunId := inputSet.ancList[0]
		oneRunId := inputSet.ancList[1]
		delete(runIdGroups[zeroRunId], oneRunId)

		succsOf1, err = state.getEligibleSuccessors(1)
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
			succZeroRunId := succsOf1[3][0].ancList[0]
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
		err := state.addCmdResults(inputSet, []TypedParams{{}})
		if err != nil {
			t.Fatalf("error adding results of 3: %s", err)
		}

		succsOf3, err := state.getEligibleSuccessors(3)
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

	index, err := ParseAndValidateWorkflow(&workflow)
	if err != nil {
		t.Fatalf("could not build index: %s", err)
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
	succsOf1, err := state.getSuccParams(startNodes[1][0], p1Vals)
	inputsFor2 := getSuccInputsOrFail(t, succsOf1, err, 1, 2, 1)

	_, err = state.getSuccParams(inputsFor2[0], []TypedParams{{}})
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

	index, err := ParseAndValidateWorkflow(&workflow)
	if err != nil {
		t.Fatalf("could not build index: %s", err)
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
	succsOf1, err := state.getSuccParams(startNodes[1][0], p1Vals)
	inputsFor2 := getSuccInputsOrFail(t, succsOf1, err, 1, 2, numIterationsOf1)

	// The idea here is to correlate different values of p2 with each value of p1.
	// I.e. simulate a situation where for different values of p1, async node p2
	// generates different values of output p2. We also have it generate different
	// numbers of outputs for p2 for different inputs to further test robustness.
	expNumSuccsOf2 := 0
	for i, inputFor2 := range inputsFor2 {
		expNumSuccsOf2 += (i + 1)
		p2Vals := genArbitraryOutputs(t, "p2", (i + 1), (i+1)*len(inputsFor2), 2, workflow)
		err := state.addCmdResults(inputFor2, p2Vals)
		if err != nil {
			t.Fatalf("error adding outputs of 2: %s", err)
		}
		assertCompletionVal(t, state, false)
	}

	succsOf2, err := state.getEligibleSuccessors(2)
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

//func TestBulkRNA(t *testing.T) {
//	data, err := os.ReadFile("testdata/bulkrna_seq.json")
//	if err != nil {
//		t.Fatalf("failed to read JSON file: %v", err)
//	}
//
//	var workflow Workflow
//	if err := json.Unmarshal(data, &workflow); err != nil {
//		t.Fatalf("failed to unmarshal JSON: %v", err)
//	}
//
//    index, err := ParseAndValidateWorkflow(&workflow)
//    if err != nil {
//        t.Fatalf("failed to build index: %s\n", err)
//    }
//
//    state := NewWorkflowExecutionState(workflow, index)
//    cmds, err := state.GetInitialCmds()
//    for _, cmd := range cmds {
//        cmdStr, envs := FormSingularityCmd(cmd, map[string]string{}, "sif", true)
//        fmt.Printf("%s %s\n", strings.Join(envs, " "),  cmdStr)
//        fmt.Printf("\n")
//    }
//}