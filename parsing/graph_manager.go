package parsing

import (
	"fmt"
)

// Data structure to package all the various data structures for
// looking up database graph structure.
type WorkflowIndex struct {
	preds map[int][]int
	succs map[int][]int
	// Lookup links based on (dstNode, dstPname)
	inLinks map[int]map[string]WorkflowLink
	// Lookup links based on (srcNode, srcPname)
	outLinks       map[int]map[string]WorkflowLink
	asyncAncestors map[int][]int

	// Layered top sort where each inner array of node IDs is
	// nodes that can run independently once all nodes in prior
	// arrays have run.
	layeredTopSort [][]int
	barrierFor     map[int]int
}

func (index *WorkflowIndex) lastSharedAsyncAncestor(id1, id2 int) (int, error) {
	asyncAnc1, id1Exists := index.asyncAncestors[id1]
	if !id1Exists {
		return 0, fmt.Errorf("node %d not in async ancestor array", id1)
	}
	asyncAnc2, id2Exists := index.asyncAncestors[id2]
	if !id2Exists {
		return 0, fmt.Errorf("node %d not in async ancestor array", id2)
	}

	for i := 1; i <= max(len(asyncAnc1), len(asyncAnc2)); i++ {
        if i >= len(asyncAnc1) {
		    if asyncAnc2[len(asyncAnc2)-i] == id1 {
		    	return id1, nil
		    }
        } else if i >= len(asyncAnc2) {
		    if asyncAnc1[len(asyncAnc1)-i] == id2 {
		    	return id2, nil
		    }
        } else if asyncAnc1[len(asyncAnc1)-i] == asyncAnc2[len(asyncAnc2)-i] {
			return asyncAnc1[len(asyncAnc1)-i], nil
		}
	}

	return -1, nil
}

func (index *WorkflowIndex) getAncListValue(
	ownerOfAncListId int, nodeToLookupId int, ancList []int,
) (int, error) {
	if nodeToLookupId == -1 {
		return -1, nil
	}

	asyncAncOfOwner, ownerExists := index.asyncAncestors[ownerOfAncListId]
	if !ownerExists {
		return 0, fmt.Errorf(
			"node %d not in async ancestor array",
			ownerOfAncListId,
		)
	}

	for i := len(asyncAncOfOwner) - 1; i >= 0; i-- {
		if asyncAncOfOwner[i] == nodeToLookupId {
			return ancList[i], nil
		}
	}

	return 0, fmt.Errorf(
		"node %d is not an async ancestor of node %d",
		nodeToLookupId, ownerOfAncListId,
	)
}

type NodeParams struct {
	nodeId  int
	ancList []int
	params  TypedParams
}

type WorkflowExecutionState struct {
	workflow     Workflow
	index        WorkflowIndex
	allocatedCmd map[int]bool
	// node id -> runID of last async ancestor (-1 if none) -> value
	inputs map[int]map[int]NodeParams
	// node id -> run ID -> output list
	outputs           map[int]map[int]NodeParams
	currentMaxId      map[int]int
	unconsumedOutputs map[int]map[int]struct{}
	cmds              map[int][]CmdTemplate
}

func (state *WorkflowExecutionState) consumeAncestorLists(
	nodeId int,
) ([][]int, error) {
	_, nodeExists := state.workflow.Nodes[nodeId]
	if !nodeExists {
		return nil, fmt.Errorf(
			"node %d does not exist",
			nodeId,
		)
	}

	if state.allocatedCmd == nil {
		state.allocatedCmd = map[int]bool{}
	}

	asyncAncestors := state.index.asyncAncestors[nodeId]
	if len(asyncAncestors) == 0 {
		// A node runs once for every run of its topologically last
		// async ancestor. If it does not have any async ancestors,
		// it runs once, so it cannot be run again.
		if state.allocatedCmd[nodeId] {
			return [][]int{}, nil
		}

		// Return a single empty ancestor list corresponding to the
		// one run of this not-run node.
		state.allocatedCmd[nodeId] = true
		return [][]int{{}}, nil
	}

	lastAsyncAnc := asyncAncestors[len(asyncAncestors)-1]
	_, outputsExist := state.outputs[lastAsyncAnc]
	if !outputsExist {
		return nil, fmt.Errorf(
			"node %d does not exist or has no outputs yet",
			lastAsyncAnc,
		)
	}

	unconsumedSet, unconsumedSetExists := state.unconsumedOutputs[lastAsyncAnc]
	if !unconsumedSetExists || unconsumedSet == nil {
		return nil, fmt.Errorf(
			"node %d has no or nil unconsumed set",
			lastAsyncAnc,
		)
	}

	unconsumed := make([][]int, 0)
	for asyncAncPredRunId := range unconsumedSet {
		asyncAncOutputs, asyncAncOutputsExist := state.outputs[lastAsyncAnc][asyncAncPredRunId]

		if !asyncAncOutputsExist {
			return nil, fmt.Errorf(
				"node %d has unconsumed array pointing to non-existent outputs",
				lastAsyncAnc,
			)
		}

		unconsumed = append(unconsumed, asyncAncOutputs.ancList)
		delete(state.unconsumedOutputs[lastAsyncAnc], asyncAncPredRunId)
	}

	if len(unconsumed) > 0 {
		state.allocatedCmd[nodeId] = true
	}

	return unconsumed, nil
}

func (state *WorkflowExecutionState) formInputs(
	nodeId int, ancList []int,
) (NodeParams, error) {
	ret := NodeParams{
		nodeId:  nodeId,
		ancList: ancList,
	}

	// If node's pred shares an async ancestor with it, you should use only
	// the version of this pred's outputs that were descended from this async
	// ancestor.
	// Get LCP between predecessor and node's ancList.
	predRunId := make(map[int]int)
	for _, pred := range state.index.preds[nodeId] {
		var err error
		if err != nil {
			return NodeParams{}, fmt.Errorf(
				"could not find value for %d in ancestor list", pred,
			)
		}

		lastSharedAsyncAncestorId, err := state.index.lastSharedAsyncAncestor(
			pred, nodeId,
		)

		if err != nil {
			return NodeParams{}, fmt.Errorf(
				"could not find last shared async ancestor of %d and %d: %s",
				pred, nodeId, err,
			)
		}

		predRunId[pred], err = state.index.getAncListValue(
			nodeId, lastSharedAsyncAncestorId, ancList,
		)

		if err != nil {
			return NodeParams{}, fmt.Errorf(
				"error looking up val of %d in ancestor list of %d: %s",
				lastSharedAsyncAncestorId, nodeId, err,
			)
		}
	}

	for _, link := range state.index.inLinks[nodeId] {
		srcNode := link.SourceNodeId
		srcChan := link.SourceChannel
		sinkChan := link.SinkChannel
		srcRunId, srcIsPred := predRunId[srcNode]
		if !srcIsPred {
			return NodeParams{}, fmt.Errorf(
				"bad index: %d not listed as pred of %d",
				srcNode, nodeId,
			)
		}

		srcArgType, srcArgTypeExists :=
			state.workflow.Nodes[srcNode].ArgTypes[srcChan]
		if !srcArgTypeExists {
			return NodeParams{}, fmt.Errorf(
				"bad argtype: node %d has no parameter %s",
				srcNode, srcChan,
			)
		}

		sinkArgType, sinkArgTypeExists :=
			state.workflow.Nodes[nodeId].ArgTypes[sinkChan]
		if !sinkArgTypeExists {
			return NodeParams{}, fmt.Errorf(
				"bad argtype: node %d has no parameter %s",
				nodeId, sinkChan,
			)
		}

		srcOutputParams := state.outputs[srcNode][srcRunId]
		srcPval, srcPvalExists := srcOutputParams.params.lookupParam(
			srcChan, srcArgType,
		)

		if !srcPvalExists {
			srcInputParams := state.inputs[srcNode][srcRunId].params
			srcPval, srcPvalExists = srcInputParams.lookupParam(
				srcChan, srcArgType,
			)

			if !srcPvalExists {
				return NodeParams{}, fmt.Errorf(
					"param %s not found in node %d inputs or outputs",
					srcChan, srcNode,
				)
			}
		}

		ret.params.addParam(srcPval, sinkChan, sinkArgType)
	}
	return ret, nil
}

func (state *WorkflowExecutionState) addCmdResults(
	inputs NodeParams,
	outputs []TypedParams,
) error {
	node, nodeExists := state.workflow.Nodes[inputs.nodeId]
	if !nodeExists {
		return fmt.Errorf("node %d of inputs does not exist", inputs.nodeId)
	}

	if len(outputs) > 1 && !node.Async {
		return fmt.Errorf("multiple outputs given for non-async node")
	}

	inputAncList := inputs.ancList
	if len(inputAncList) != len(state.index.asyncAncestors[inputs.nodeId]) {
		return fmt.Errorf("inputs have invalid ancestor list")
	}

	if state.outputs == nil {
		state.outputs = make(map[int]map[int]NodeParams)
	}

	if state.outputs[inputs.nodeId] == nil {
		state.outputs[inputs.nodeId] = make(map[int]NodeParams)
	}

	if state.inputs == nil {
		state.inputs = make(map[int]map[int]NodeParams)
	}

	if state.inputs[inputs.nodeId] == nil {
		state.inputs[inputs.nodeId] = make(map[int]NodeParams)
	}

	if state.unconsumedOutputs == nil {
		state.unconsumedOutputs = make(map[int]map[int]struct{})
	}

	if state.unconsumedOutputs[inputs.nodeId] == nil {
		state.unconsumedOutputs[inputs.nodeId] = make(map[int]struct{})
	}

	if state.currentMaxId == nil {
		state.currentMaxId = make(map[int]int)
	}

	lastAsyncAncestorId := -1
	if len(inputAncList) > 0 {
		lastAsyncAncestorId = inputAncList[len(inputAncList)-1]
	}

	for _, output := range outputs {
		var outputParams NodeParams
		outputParams.nodeId = inputs.nodeId
		outputParams.params = output
        outputParams.ancList = make([]int, len(inputAncList))
		copy(outputParams.ancList, inputAncList)

		if node.Async {
			runId := state.currentMaxId[outputParams.nodeId]
			outputParams.ancList = append(
				outputParams.ancList,
				state.currentMaxId[outputParams.nodeId],
			)
			state.currentMaxId[outputParams.nodeId] += 1
			state.inputs[inputs.nodeId][runId] = inputs
			state.outputs[inputs.nodeId][runId] = outputParams
			state.unconsumedOutputs[inputs.nodeId][runId] = struct{}{}
		} else {
			state.inputs[inputs.nodeId][lastAsyncAncestorId] = inputs
			state.outputs[inputs.nodeId][lastAsyncAncestorId] = outputParams
			state.unconsumedOutputs[inputs.nodeId][lastAsyncAncestorId] = struct{}{}
		}
	}

	return nil
}

func (state *WorkflowExecutionState) getEligibleSuccessors(
	nodeId int,
) (map[int][]NodeParams, error) {
	// ID -> ancestor list
	ret := make(map[int][]NodeParams)
	for _, succ := range state.index.succs[nodeId] {
		succCanRun := true
		for _, predOfSucc := range state.index.preds[succ] {
			if predOfSucc == nodeId {
				continue
			}

			// Could we maybe filter this based on runId? I.e. Completion of
			// node ID with ancestor list will only trigger a successor if
			// other successors of pred have completed an iteration with same
			// ID of last async ancestor as nodeId?
			if len(state.outputs[predOfSucc]) == 0 {
				succCanRun = false
				break
			}
		}

		if succCanRun {
			newAncestorLists, err := state.consumeAncestorLists(succ)
			if err != nil {
				return nil, fmt.Errorf(
					"error generating ancestor lists: %s", err,
				)
			}

			for _, ancList := range newAncestorLists {
				params, err := state.formInputs(succ, ancList)
				if err != nil {
					return nil, fmt.Errorf(
						"error forming inputs for node %d with ancestor list %v: %s",
						succ, ancList, err,
					)
				}
				ret[succ] = append(ret[succ], params)
			}
		}
	}

	return ret, nil
}

func (state *WorkflowExecutionState) TriggerSuccessors(
	inputs NodeParams,
	outputs []TypedParams,
) (map[int][]NodeParams, error) {
    if err := state.addCmdResults(inputs, outputs); err != nil {
        return nil, err
    }

    return state.getEligibleSuccessors(inputs.nodeId)
}


func getInAndOutLinks(
	workflow Workflow,
) (map[int]map[string]WorkflowLink, map[int]map[string]WorkflowLink, error) {
	// dstNode -> dstPname -> srcNode
	inLinks := make(map[int]map[string]WorkflowLink)
	outLinks := make(map[int]map[string]WorkflowLink)

	// NOTE: There is no stipulation in BWB / OWS that a sink channel
	//       corresponds to a parameter entry or ArgType entry of the
	//       sink node, so we can't check that.
	for _, link := range workflow.Links {
		srcNode := link.SourceNodeId
		srcChan := link.SourceChannel
		sinkNode := link.SinkNodeId
		sinkChan := link.SinkChannel
		if _, validSrc := workflow.Nodes[srcNode]; !validSrc {
			return nil, nil, fmt.Errorf(
				"link from %d.%s -> %d.%s has invalid source %d",
				srcNode, srcChan, sinkNode, sinkChan, srcNode,
			)
		}

		if _, validSink := workflow.Nodes[sinkNode]; !validSink {
			return nil, nil, fmt.Errorf(
				"link from %d.%s -> %d.%s has invalid sink %d",
				srcNode, srcChan, sinkNode, sinkChan, sinkNode,
			)
		}

		if _, validSrcChan := workflow.Nodes[srcNode].ArgTypes[srcChan]; !validSrcChan {
			return nil, nil, fmt.Errorf(
				"link from %d.%s -> %d.%s has invalid source channel %s",
				srcNode, srcChan, sinkNode, sinkChan, srcChan,
			)
		}

		if inLinks[link.SinkNodeId] == nil {
			inLinks[link.SinkNodeId] = make(map[string]WorkflowLink)
		}
		if outLinks[link.SourceNodeId] == nil {
			outLinks[link.SourceNodeId] = make(map[string]WorkflowLink)
		}
		inLinks[link.SinkNodeId][link.SinkChannel] = link
		outLinks[link.SourceNodeId][link.SourceChannel] = link
	}

	return inLinks, outLinks, nil
}

func layeredTopSort(workflow Workflow) ([][]int, error) {
	graph := make(map[int][]int)
	indegree := make(map[int]int)
	nodes := make(map[int]bool)

	// Build graph and indegree map
	for nodeId := range workflow.Nodes {
		graph[nodeId] = make([]int, 0)
		indegree[nodeId] = 0
		nodes[nodeId] = true
	}

	for _, link := range workflow.Links {
		u, v := link.SourceNodeId, link.SinkNodeId
		graph[u] = append(graph[u], v)
		indegree[v]++
	}

	// Initialize zero indegree nodes
	layer := []int{}
	for node := range nodes {
		if indegree[node] == 0 {
			layer = append(layer, node)
		}
	}

	var result [][]int

	for len(layer) > 0 {
		result = append(result, layer)
		nextLayer := []int{}
		for _, u := range layer {
			for _, v := range graph[u] {
				indegree[v]--
				if indegree[v] == 0 {
					nextLayer = append(nextLayer, v)
				}
			}
		}
		layer = nextLayer
	}

	for _, v := range indegree {
		if v > 0 {
			return nil, fmt.Errorf("graph has a cycle")
		}
	}

	return result, nil
}

func topSort(workflow Workflow) ([]int, error) {
	layeredTopSort, err := layeredTopSort(workflow)
	if err != nil {
		return nil, err
	}

	if layeredTopSort == nil {
		return nil, nil
	}

	topSort := make([]int, 0, len(workflow.Nodes))
	for _, layer := range layeredTopSort {
		topSort = append(topSort, layer...)
	}

	if len(topSort) != len(workflow.Nodes) {
		return nil, fmt.Errorf("layered top sort has incorrect length")
	}

	return topSort, nil
}

func propagateArgTypes(
	workflow *Workflow,
	layeredTopSort [][]int,
	inLinks map[int]map[string]WorkflowLink,
) error {
	for _, layer := range layeredTopSort {
		for _, nodeId := range layer {
			nodeCopy := workflow.Nodes[nodeId]
			for pname, inLink := range inLinks[nodeId] {
				srcNode, srcNodeExists := workflow.Nodes[inLink.SourceNodeId]
				if !srcNodeExists {
					return fmt.Errorf("src node %d does not exist", inLink.SourceNodeId)
				}

				if _, pnameHasArgtype := nodeCopy.ArgTypes[pname]; !pnameHasArgtype {
					srcArgType, srcArgTypeExists := srcNode.ArgTypes[inLink.SourceChannel]
					if !srcArgTypeExists {
						return fmt.Errorf(
							"src node %d channel %s does not exist",
							inLink.SourceNodeId, inLink.SourceChannel,
						)
					}

					nodeCopy.ArgTypes[pname] = srcArgType
				}
			}
			workflow.Nodes[nodeId] = nodeCopy
		}
	}
	return nil
}

func getAncestorsAndDescendants(
	topSort [][]int,
	inLinks map[int]map[string]WorkflowLink,
	outLinks map[int]map[string]WorkflowLink,
) (map[int]map[int]bool, map[int]map[int]bool) {
	ancestorOf := make(map[int]map[int]bool)
	for i := 0; i < len(topSort); i++ {
		layer := topSort[i]
		for _, sourceNode := range layer {
			for _, outLink := range outLinks[sourceNode] {
				if ancestorOf[outLink.SinkNodeId] == nil {
					ancestorOf[outLink.SinkNodeId] = make(map[int]bool)
				}

				ancestorOf[outLink.SinkNodeId][sourceNode] = true
				for anc := range ancestorOf[sourceNode] {
					ancestorOf[outLink.SinkNodeId][anc] = true
				}
			}
		}
	}

	// (ancestor, descendant) -> T (using this more as hash set than map)
	descendantOf := make(map[int]map[int]bool)
	for i := len(topSort) - 1; i >= 0; i-- {
		layer := topSort[i]
		for _, sinkNode := range layer {
			for _, inLink := range inLinks[sinkNode] {
				if descendantOf[inLink.SourceNodeId] == nil {
					descendantOf[inLink.SourceNodeId] = make(map[int]bool)
				}

				descendantOf[inLink.SourceNodeId][sinkNode] = true
				for desc := range descendantOf[sinkNode] {
					descendantOf[inLink.SourceNodeId][desc] = true
				}
			}
		}
	}

	return ancestorOf, descendantOf
}

func parseAsyncAndBarriers(
	workflow Workflow, topSort [][]int,
	descendantOf map[int]map[int]bool,
) (map[int][]int, map[int]int, error) {
	barrierFor := make(map[int]int)
	for nodeId, node := range workflow.Nodes {
		if node.BarrierFor != nil {
			srcNode, srcNodeExists := workflow.Nodes[*node.BarrierFor]
			if !srcNodeExists {
				return nil, nil, fmt.Errorf(
					"node %d is barrier for non-existent node %d",
					nodeId, *node.BarrierFor,
				)
			}

			if !srcNode.Async {
				return nil, nil, fmt.Errorf(
					"node %d is barrier for non-async node %d",
					nodeId, *node.BarrierFor,
				)
			}

			if barrier, barrierExists := barrierFor[*node.BarrierFor]; barrierExists {
				return nil, nil, fmt.Errorf(
					"node %d is barrier for (at least) two nodes: %d and %d",
					nodeId, *node.BarrierFor, barrier,
				)
			}

			if !descendantOf[*node.BarrierFor][nodeId] {
				return nil, nil, fmt.Errorf(
					"node %d is barrier for node %d but is not its descendant",
					nodeId, *node.BarrierFor,
				)

			}
			barrierFor[*node.BarrierFor] = nodeId
		}
	}

	// Node ID -> Async ancestors IDs ordered topologically.
	// We don't consider barriers yet, because we need this to
	// validate barrier structure in the first place.
	asyncAncestors := make(map[int][]int)
	for _, layer := range topSort {
		for _, asyncNodeId := range layer {
			if !workflow.Nodes[asyncNodeId].Async {
				continue
			}

			descendantsSet, hasDescendants := descendantOf[asyncNodeId]
			if !hasDescendants || descendantsSet == nil {
				continue
			}

			// Consider nodes descended from async node but not from its
			// barrier as the node's async descendants.
			for descendant := range descendantsSet {
				if asyncAncestors[descendant] == nil {
					asyncAncestors[descendant] = make([]int, 0)
				}
				asyncAncestors[descendant] = append(
					asyncAncestors[descendant], asyncNodeId,
				)
			}
		}
	}

	// Verify that barriers are nested. If async node A with barrier B precedes
	// async node A' with barrier B', then it must be the case that B' precedes
	// B.
	for _, layer := range topSort {
		for _, barrierNodeId := range layer {
			barrierNode := workflow.Nodes[barrierNodeId]
			if barrierNode.BarrierFor == nil {
				continue
			}

			asyncNodeId := *barrierNode.BarrierFor
			nodeAsyncAncestors, hasAsyncAncestors := asyncAncestors[asyncNodeId]
			if !hasAsyncAncestors {
				continue
			}

			for _, asyncAncestorId := range nodeAsyncAncestors {
				// Recall that async ancestor list is top sorted.
				if asyncAncestorId == asyncNodeId {
					break
				}

				ancBarrier, ancHasBarrier := barrierFor[asyncAncestorId]
				if ancHasBarrier && !descendantOf[barrierNodeId][ancBarrier] {
					return nil, nil, fmt.Errorf(
						"async nodes %d, %d has non-nested barriers %d and %d",
						asyncAncestorId, asyncNodeId, barrierNodeId, ancBarrier,
					)
				}
			}
		}
	}

	asyncAncestorsBeforeBarrier := make(map[int][]int)
	for nodeId := range workflow.Nodes {
		if asyncAncestorsBeforeBarrier[nodeId] == nil {
			asyncAncestorsBeforeBarrier[nodeId] = make([]int, 0)
		}
		for _, asyncAncestorId := range asyncAncestors[nodeId] {
			if barrier, barrierExists := barrierFor[asyncAncestorId]; barrierExists {
				if descendantOf[barrier][nodeId] {
					continue
				}
			}
			asyncAncestorsBeforeBarrier[nodeId] = append(
				asyncAncestorsBeforeBarrier[nodeId],
				asyncAncestorId,
			)
		}
	}

	for nodeId := range workflow.Nodes {
		asyncAncestors, hasAsyncAncestors := asyncAncestorsBeforeBarrier[nodeId]
		if !hasAsyncAncestors || len(asyncAncestors) == 0 {
			continue
		}

		pred := asyncAncestors[0]
		for i := 1; i < len(asyncAncestors); i++ {
			if !descendantOf[pred][asyncAncestors[i]] {
				return nil, nil, fmt.Errorf(
					"node %d has non-nested async ancestors %d and %d",
					nodeId, pred, asyncAncestors[i],
				)
			}
		}
	}

	return asyncAncestorsBeforeBarrier, barrierFor, nil
}

func getPredsAndSuccs(workflow Workflow) (map[int][]int, map[int][]int) {
	preds := make(map[int][]int)
	succs := make(map[int][]int)
	addedPred := make(map[int]map[int]bool)
	addedSucc := make(map[int]map[int]bool)

	for _, link := range workflow.Links {
		src := link.SourceNodeId
		dst := link.SinkNodeId

		if addedSucc[src] == nil {
			addedSucc[src] = make(map[int]bool)
		}
		if !addedSucc[src][dst] {
			succs[src] = append(succs[src], dst)
			addedSucc[src][dst] = true
		}

		if addedPred[dst] == nil {
			addedPred[dst] = make(map[int]bool)
		}
		if !addedPred[dst][src] {
			preds[dst] = append(preds[dst], src)
			addedPred[dst][src] = true
		}
	}
	return preds, succs
}

// Inputs:
//   - workflow: Workflow whose arg types will be edited so as to create
//     an argtype entry for each incoming link if one does not exist.
//
// Outputs:
//   - A workflow index giving data structures to facilitate easy lookup.
func parseAndValidateWorkflow(workflow *Workflow) (WorkflowIndex, error) {
	var index WorkflowIndex
	index.preds, index.succs = getPredsAndSuccs(*workflow)
	topSort, err := layeredTopSort(*workflow)

	inLinks, outLinks, err := getInAndOutLinks(*workflow)
	if err != nil {
		return WorkflowIndex{}, fmt.Errorf("error parsing links: %s", err)
	}

	index.inLinks = inLinks
	index.outLinks = outLinks

	if err != nil {
		return WorkflowIndex{}, fmt.Errorf("error in top sort: %s", err)
	}
	index.layeredTopSort = topSort

	err = propagateArgTypes(workflow, topSort, inLinks)
	if err != nil {
		return WorkflowIndex{}, fmt.Errorf("error propagating arg types: %s", err)
	}

	_, descendantOf := getAncestorsAndDescendants(topSort, inLinks, outLinks)
	index.asyncAncestors, index.barrierFor, err = parseAsyncAndBarriers(
		*workflow, topSort, descendantOf,
	)
	if err != nil {
		return WorkflowIndex{}, err
	}
	return index, nil
}
