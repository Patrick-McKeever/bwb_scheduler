package parsing

import (
    "fmt"
    "slices"
    "strings"
)

type NodeParams struct {
    NodeId  int
    NodeDef WorkflowNode
    AncList []int
    Params  TypedParams
}

type NodeExec struct {
    id                int
    nodeId            int
    success           bool
    finished          bool
    subtreeFailed     bool
    inputs            TypedParams
    outputs           []TypedParams
    succs             map[int]map[int]*NodeExec
    remainingSuccRuns map[int]map[int]struct{}
    pred              *NodeExec
}

type WorkflowExecutionState struct {
    // roots here is used to mean any nodes without async ancestors.
    root          *NodeExec
    workflow      Workflow
    index         WorkflowIndex
    runsByAncList map[int]map[string]*NodeExec
}

func NewWorkflowExecutionState(
    workflow Workflow, index WorkflowIndex,
) WorkflowExecutionState {
    var state WorkflowExecutionState
    state.workflow = workflow
    state.index = index
    state.root = &NodeExec{
        nodeId:            -1,
        finished:          true,
        succs:             map[int]map[int]*NodeExec{},
        remainingSuccRuns: map[int]map[int]struct{}{},
        success: true,
    }
    state.runsByAncList = make(map[int]map[string]*NodeExec)
    for nodeId := range workflow.Nodes {
        state.runsByAncList[nodeId] = make(map[string]*NodeExec)
        state.root.succs[nodeId] = make(map[int]*NodeExec)
        nodeAsyncAncs := index.AsyncAncestors[nodeId]
        if len(nodeAsyncAncs) == 1 {
            state.root.remainingSuccRuns[nodeId] = map[int]struct{}{
                0: {},
            }
        }
    }

    return state
}

func (tree *WorkflowExecutionState) IsComplete() bool {
    return asyncBlockComplete(*tree.root)
}

func (tree *WorkflowExecutionState) HasFailed() bool {
    return tree.root.subtreeFailed
}

func (tree *WorkflowExecutionState) getInitialNodeParams() (map[int][]NodeParams, error) {
    startNodeIds := tree.index.getStartNodes()
    outParams := make(map[int][]NodeParams)
    for _, startNodeId := range startNodeIds {
        startNode, startNodeExists := tree.workflow.Nodes[startNodeId]
        if !startNodeExists {
            return nil, fmt.Errorf(
                "index error, could not find node %d listed as start node",
                startNodeId,
            )
        }

        tp := tree.index.BaseParams[startNodeId]
        nodeParams := NodeParams{
            NodeId:  startNodeId,
            AncList: []int{0},
            Params:  tp,
            NodeDef: startNode,
        }
        outParams[startNodeId] = []NodeParams{nodeParams}
        tree.createInputNode(nodeParams, []int{0}, false)
    }

    return outParams, nil
}

func asyncBlockComplete(nodeExec NodeExec) bool {
    if !nodeExec.finished {
        return false
    }

    for _, v := range nodeExec.remainingSuccRuns {
        if len(v) > 0 {
            return false
        }
    }

    return true
}

func lookupNodeHelper(
    root *NodeExec, id int, ancNodeIds, ancRunIds []int,
) *NodeExec {
    if len(ancRunIds) == 0 {
        return root
    }

    nextAncNodeId := ancNodeIds[0]
    nextAncRunId := ancRunIds[0]
    if root.succs[nextAncNodeId] == nil {
        return nil
    }

    nextNode := root.succs[nextAncNodeId][nextAncRunId]
    return lookupNodeHelper(
        nextNode, id, ancNodeIds[1:], ancRunIds[1:],
    )
}

func (tree *WorkflowExecutionState) lookupNode(id int, ancRunIds []int) *NodeExec {
    if id == -1 {
        if id == -1 && len(ancRunIds) == 0 {
            return tree.root
        }
        return nil
    }

    ancNodeIds, ok := tree.index.AsyncAncestors[id]
    if !ok {
        return nil
    }

    path := make([]int, len(ancNodeIds)-1)
    copy(path, ancNodeIds[1:])
    path = append(path, id)

    if len(path) != len(ancRunIds) {
        return nil
    }

    return lookupNodeHelper(tree.root, id, path, ancRunIds)
}

func getAncListsWithPrefixHelper(
    nodeExec *NodeExec, baseList []int, intermediateAncIds []int,
    asyncNodeId, succId int, candidate bool,
) [][]int {
    if nodeExec == nil {
        return nil
    }

    out := make([][]int, 0)
    if len(intermediateAncIds) == 0 {
        if candidate {
            for runId := range nodeExec.outputs {
                newList := append(baseList, runId)
                out = append(out, newList)
            }
        } else {
            for runId := range nodeExec.succs[succId] {
                newList := append(baseList, runId)
                out = append(out, newList)
            }
        }
        return out
    }

    nextAncId := intermediateAncIds[0]
    for runId, succRun := range nodeExec.succs[nextAncId] {
        newList := append(baseList, runId)
        descAncLists := getAncListsWithPrefixHelper(
            succRun, newList, intermediateAncIds[1:],
            asyncNodeId, succId, candidate,
        )
        out = append(out, descAncLists...)
    }
    return out
}

func (tree *WorkflowExecutionState) getAncListsWithPrefix(
    succId int, prefix []int, candidate bool,
) ([][]int, error) {
    succAncNodeIds, ok := tree.index.AsyncAncestors[succId]
    if !ok {
        return nil, fmt.Errorf(
            "unable to find async ancestors of desc %d in index", succId,
        )
    }

    if len(succAncNodeIds) == 1 {
        return [][]int{{0}}, nil
    }

    lastAsyncAnc := succAncNodeIds[len(succAncNodeIds)-1]
    if len(prefix) == len(succAncNodeIds) {
        immediateAsyncAnc := tree.lookupNode(lastAsyncAnc, prefix[:len(prefix)-1])
        if immediateAsyncAnc != nil {
            if prefix[len(prefix)-1] < len(immediateAsyncAnc.outputs) {
                return [][]int{prefix}, nil
            }
        }
        return nil, nil
    }

    lastNodeInPrefix := succAncNodeIds[len(prefix)]
    ancAncNodeIds, ok := tree.index.AsyncAncestors[lastNodeInPrefix]
    if !ok {
        return nil, fmt.Errorf(
            "unable to find async ancestors of anc %d in index", lastNodeInPrefix,
        )
    }

    if len(ancAncNodeIds) != len(prefix) {
        return nil, fmt.Errorf(
            "gave ancestor list with %d entries for node %d, while "+
                "this node has only %d async ancestors", len(prefix),
            lastNodeInPrefix, len(ancAncNodeIds),
        )
    }

    notAncErr := fmt.Errorf(
        "node %d (ancs %#v) cannot be descendant of node %d (ancs %#v)",
        lastAsyncAnc, succAncNodeIds, lastNodeInPrefix, ancAncNodeIds,
    )
    if len(succAncNodeIds)-len(ancAncNodeIds) <= 0 {
        return nil, notAncErr
    }

    intermediateAncNodeIds := succAncNodeIds[len(ancAncNodeIds):]
    if intermediateAncNodeIds[0] != lastNodeInPrefix {
        return nil, notAncErr
    }

    ancExecNode := tree.lookupNode(lastNodeInPrefix, prefix)
    return getAncListsWithPrefixHelper(
        ancExecNode, prefix, intermediateAncNodeIds[1:],
        lastAsyncAnc, succId, candidate,
    ), nil
}

func (tree *WorkflowExecutionState) getNodeRunsWithAncPrefix(
    nodeId int, prefix []int,
) ([]*NodeExec, error) {
    nodeAsyncAncs := tree.index.AsyncAncestors[nodeId]
    if len(prefix) > len(nodeAsyncAncs) {
        return nil, fmt.Errorf(
            "got prefix of len %d when node %d has only %d async ancs",
            len(prefix), nodeId, len(nodeAsyncAncs),
        )
    }

    ancLists, err := tree.getAncListsWithPrefix(nodeId, prefix, false)
    if err != nil {
        return nil, err
    }

    out := make([]*NodeExec, 0)
    for _, ancList := range ancLists {
        ancListStr := fmt.Sprintf("%#v", ancList)
        nodeRun, ok := tree.runsByAncList[nodeId][ancListStr]
        if !ok {
            return nil, fmt.Errorf(
                "could not find node %d w/ prefix %#v; this suggests an "+
                    "error in the implementation of getAncListsWithPrefix",
                nodeId, ancList,
            )
        }
        out = append(out, nodeRun)
    }

    return out, nil
}

func markNodeComplete(node *NodeExec, ancList []int, subtreeFailed bool) error {
    currNode := node
    predId := node.nodeId
    predIsComplete := false 

    for i := len(ancList); i >= 0 && currNode != nil; i-- {
        currNode.finished = true
        // A single failure in the subtree means the whole subtree
        // failed, so the value can only ever change from true to 
        // false.
        if subtreeFailed {
            currNode.subtreeFailed = subtreeFailed
        }

        if i < len(ancList) && predIsComplete {
            if _, ok := currNode.remainingSuccRuns[predId]; !ok {
                return fmt.Errorf(
                    "pred of node %d has no entry in remaining succ list for it",
                    predId,
                )
            }
            lastAsyncRunId := ancList[i]
            delete(currNode.remainingSuccRuns[predId], lastAsyncRunId)
        }

        predIsComplete = asyncBlockComplete(*currNode)
        predId = currNode.nodeId
        currNode = currNode.pred

        // If neither of these are true, there's nothing to
        // propagate up the tree.
        if !(subtreeFailed || predIsComplete) {
            break
        }
    }
    return nil
}

func (tree *WorkflowExecutionState) addCmdResults(
    inputs NodeParams, outputs []TypedParams,
) error {
    node, nodeExists := tree.workflow.Nodes[inputs.NodeId]
    if !nodeExists {
        return fmt.Errorf("node %d of inputs does not exist", inputs.NodeId)
    }

    if len(outputs) > 1 && !node.Async {
        return fmt.Errorf("multiple outputs given for non-async node")
    }

    execNodePtr := tree.lookupNode(inputs.NodeId, inputs.AncList)
    if execNodePtr == nil {
        return fmt.Errorf(
            "could not find node %d w/ anc list %#v",
            inputs.NodeId, inputs.AncList,
        )
    }

    outputIndices := make([]int, 0)
    execNodePtr.finished = true
    for _, outputSet := range outputs {
        execNodePtr.outputs = append(execNodePtr.outputs, outputSet)
        outputIndices = append(outputIndices, len(execNodePtr.outputs)-1)
    }

    // If this node is async, we expect each async descendant to
    // run once for every output of this node. We only track immediate
    // async descendants, since we can recursively call asyncBlockFinished
    // on any async blocks contained within the one rooted at this node.
    for succId := range tree.index.AsyncDescendants[inputs.NodeId] {
        succAsyncAncs := tree.index.AsyncAncestors[succId]
        if succAsyncAncs[len(succAsyncAncs)-1] != inputs.NodeId {
            continue
        }

        if execNodePtr.remainingSuccRuns == nil {
            execNodePtr.remainingSuccRuns = map[int]map[int]struct{}{}
        }
        if execNodePtr.remainingSuccRuns[succId] == nil {
            execNodePtr.remainingSuccRuns[succId] = make(map[int]struct{})
        }

        for _, outputInd := range outputIndices {
            execNodePtr.remainingSuccRuns[succId][outputInd] = struct{}{}
        }
    }

    return markNodeComplete(execNodePtr, inputs.AncList, false)
}

func (tree *WorkflowExecutionState) getCandidateAncLists(
    nodeId int, succId int, ancRunIds []int,
) ([][]int, error) {
    if _, nodeExists := tree.workflow.Nodes[nodeId]; !nodeExists {
        return nil, fmt.Errorf("node %d of inputs does not exist", nodeId)
    }

    succAsyncAncs, ok := tree.index.AsyncAncestors[succId]
    if !ok {
        return nil, fmt.Errorf("no anc list found for succ %d", succId)
    }

    // This is a simple optimization; our constraints on the graph form
    // (enforced when building the index) require that a node's async
    // ancestors have a unique topological ordering. So, if two nodes
    // have the same number of async ancestors and one is a successor
    // of the other (as we assume succId is to nodeId), then they must
    // share exactly the same list of async ancestors; hence the ancRunIds
    // of the completed run of nodeId are the only possible values 
    // for an ancestor list of succId.
    if len(ancRunIds) == len(succAsyncAncs) {
        return [][]int{ancRunIds}, nil
    }

    if len(succAsyncAncs) == 1 {
        return [][]int{{0}}, nil
    }

    var err error
    candidateSuccAncLists, err := tree.getAncListsWithPrefix(
        succId, ancRunIds, true,
    )
    if err != nil {
        return nil, fmt.Errorf(
            "error getting anc lists of %d: %s", succId, err,
        )
    }

    return candidateSuccAncLists, nil
}

func (tree *WorkflowExecutionState) getEligibleBarriers(
    completedNodeId int, ancList []int,
) (int, []int, error) {
    asyncAncs := tree.index.AsyncAncestors[completedNodeId]
    if len(asyncAncs) == 0 {
        return 0, nil, nil
    }

    if len(asyncAncs) != len(ancList) {
        return 0, nil, fmt.Errorf(
            "got async anc list of len %d, expected len %d ",
            len(asyncAncs), len(ancList)-1,
        )
    }

    lastAsyncAncNodeId := asyncAncs[len(asyncAncs)-1]
    correspondingBarrier, ok := tree.index.BarrierFor[lastAsyncAncNodeId]
    if !ok {
        return 0, nil, nil
    }

    barrierAncList := ancList[:len(ancList)-1]
    return correspondingBarrier, barrierAncList, nil
}

func (tree *WorkflowExecutionState) barrierCanRun(
    barrierId int, ancList []int,
) bool {
    barrier := tree.workflow.Nodes[barrierId]
    if barrier.BarrierFor == nil {
        return true
    }

    barrierSrc := tree.lookupNode(*barrier.BarrierFor, ancList)
    if barrierSrc == nil || barrierSrc.subtreeFailed {
        return false
    }

    return asyncBlockComplete(*barrierSrc)
}

// STEP 1.
// Find the list of input ancLists to lastAsyncAncestor that share a prefix with
// whatever node just completed. These are the only new ancestor lists that could
// possibly be triggered.
// STEP 2.
// For each such ancestor list A of lastAsyncAnc and each predOfSucc, ensure that there
// is a run of predOfSucc whose ancestor list is a prefix of A.
func (tree *WorkflowExecutionState) triggerSuccs(
    succId int, candidateSuccAncLists [][]int,
) ([][]int, []map[int]TypedParams, []map[int]TypedParams, error) {
    eligibleAncLists := make([][]int, 0)
    predOutputs := make([]map[int]TypedParams, 0)
    predInputs := make([]map[int]TypedParams, 0)
    succ := tree.workflow.Nodes[succId]
    succAsyncAncs := tree.index.AsyncAncestors[succId]

    for _, candidate := range candidateSuccAncLists {
        isEligible := true
        candidatePredInputs := make(map[int]TypedParams)
        candidatePredOutputs := make(map[int]TypedParams)
        for _, predOfSucc := range tree.index.Preds[succId] {
            pred := tree.workflow.Nodes[predOfSucc]
            predAsyncAncs := tree.index.AsyncAncestors[predOfSucc]
            sharedAncIds, err := tree.index.getSharedAsyncAncs(succId, predOfSucc)
            if err != nil {
                return nil, nil, nil, fmt.Errorf(
                    "error getting shared async ancestor: %s", err,
                )
            }

            predIsBarrierSrc := false
            if succ.BarrierFor != nil {
                if !tree.barrierCanRun(succId, candidate) {
                    isEligible = false
                    break
                }
                predIsBarrierSrc = *succ.BarrierFor == predOfSucc
            }

            predAncListPrefix := candidate[:len(sharedAncIds)]
            if len(predAsyncAncs) > len(succAsyncAncs) {
                // When a node within an async block passes a param to a node outside
                // of it, we "reduce" all of its inputs to a list. E.g. If there were
                // four runs of the node inside the async block each producing a string,
                // then these strings will be concatenated to a list to form the input
                // to the node outside the block.
                predRuns, err := tree.getNodeRunsWithAncPrefix(predOfSucc, predAncListPrefix)
                if err != nil {
                    return nil, nil, nil, err
                }

                predInputs := make([]TypedParams, 0)
                predOutputs := make([]TypedParams, 0)
                for _, predRun := range predRuns {
                    predInputs = append(predInputs, predRun.inputs)
                    predOutputs = append(predOutputs, predRun.outputs...)
                }
                candidatePredInputs[predOfSucc] = concatScalarParams(predInputs)
                candidatePredOutputs[predOfSucc] = concatScalarParams(predOutputs)
            } else {
                // The shared prefix of the last async ancestor of succId and the ancestor
                // list of predOfSucc is necessarily an ancestor list of predOfSucc. Otherwise,
                // the last element of predOfSucc's ancestor list would also be an ancestor of
                // succ; this would violate either the assumption that all ancestors of succ
                // appear in its ancestor list or the assumption that there is a strict topological
                // ordering of async ancestors.
                requiredPredRun := tree.lookupNode(predOfSucc, predAncListPrefix)
                if requiredPredRun == nil || !requiredPredRun.finished || !requiredPredRun.success {
                    isEligible = false
                    break
                }

                notFinishedErr := fmt.Errorf(
                    "node %d (anc list %#v) is marked as finished but does not "+
                        "have correct outputs", predOfSucc, predAncListPrefix,
                )
                candidatePredInputs[predOfSucc] = requiredPredRun.inputs

                if predIsBarrierSrc {
                    candidatePredOutputs[predOfSucc] = concatScalarParams(requiredPredRun.outputs)
                } else if pred.Async {
                    lastSharedAncRunId := candidate[len(sharedAncIds)]
                    if len(requiredPredRun.outputs) < lastSharedAncRunId+1 {
                        return nil, nil, nil, notFinishedErr
                    }
                    candidatePredOutputs[predOfSucc] = requiredPredRun.outputs[lastSharedAncRunId]
                } else {
                    if len(requiredPredRun.outputs) < 1 {
                        return nil, nil, nil, notFinishedErr
                    }
                    candidatePredOutputs[predOfSucc] = requiredPredRun.outputs[0]
                }
            }
        }

        if isEligible {
            eligibleAncLists = append(eligibleAncLists, candidate)
            predInputs = append(predInputs, candidatePredInputs)
            predOutputs = append(predOutputs, candidatePredOutputs)
        }
    }
    return eligibleAncLists, predInputs, predOutputs, nil
}

func (tree *WorkflowExecutionState) getLinkParam(
    predInputs map[int]TypedParams,
    predOutputs map[int]TypedParams,
    link WorkflowLink,
) (any, WorkflowArgType, string, error) {
    srcNode := link.SourceNodeId
    sinkNode := link.SinkNodeId
    srcChan := link.SourceChannel
    sinkChan := link.SinkChannel

    srcArgType, srcArgTypeExists :=
        tree.workflow.Nodes[srcNode].ArgTypes[srcChan]
    if !srcArgTypeExists {
        return nil, WorkflowArgType{}, "", fmt.Errorf(
            "bad argtype: node %d has no parameter %s",
            srcNode, srcChan,
        )
    }

    sinkArgType, sinkArgTypeExists :=
        tree.workflow.Nodes[sinkNode].ArgTypes[sinkChan]
    if !sinkArgTypeExists {
        return nil, WorkflowArgType{}, "", fmt.Errorf(
            "bad argtype: node %d has no parameter %s",
            sinkNode, sinkChan,
        )
    }

    srcOutputs, srcOutputsExist := predOutputs[srcNode]
    if !srcOutputsExist {
        return nil, WorkflowArgType{}, "", fmt.Errorf(
            "no node outputs for predecessor node %d of %d",
            srcNode, sinkNode,
        )
    }

    srcPval, srcPvalExists := srcOutputs.LookupParamOptionallyParsed(
        srcChan, sinkArgType,
    )

    if !srcPvalExists {
        srcPval, srcPvalExists = srcOutputs.LookupParamOptionallyParsed(
            srcChan, srcArgType,
        )
    }

    if !srcPvalExists {
        srcInputs, srcInputsExist := predInputs[srcNode]
        if !srcInputsExist {
            return nil, WorkflowArgType{}, "", fmt.Errorf(
                "no node inputs for predecessor node %d of %d",
                srcNode, sinkNode,
            )
        }

        srcPval, srcPvalExists = srcInputs.LookupParamOptionallyParsed(
            srcChan, sinkArgType,
        )

        if !srcPvalExists {
            srcPval, srcPvalExists = srcInputs.LookupParamOptionallyParsed(
                srcChan, srcArgType,
            )
        }

        if !srcPvalExists {
            return nil, WorkflowArgType{}, "", fmt.Errorf(
                "param %s not found in node %d inputs or outputs",
                srcChan, srcNode,
            )
        }
    }

    correctedSrcPval, err := correctArgType(srcPval, srcArgType, sinkArgType)
    if err != nil {
        return nil, WorkflowArgType{}, "", fmt.Errorf(
            "error converting param %s of node %d: %s", srcChan, srcNode, err,
        )
    }

    return correctedSrcPval, sinkArgType, sinkChan, nil
}

func (tree *WorkflowExecutionState) formInputs(
    nodeId int, predInputs map[int]TypedParams,
    predOutputs map[int]TypedParams, ancList []int,
) (NodeParams, error) {
    node, nodeExists := tree.workflow.Nodes[nodeId]
    if !nodeExists {
        return NodeParams{}, fmt.Errorf("non-existent node ID %d", nodeId)
    }

    ret := NodeParams{
        NodeId:  nodeId,
        NodeDef: node,
        AncList: ancList,
    }
    ret.Params = copyTypedParams(tree.index.BaseParams[nodeId])
    for _, link := range tree.index.InLinks[nodeId] {
        srcPval, sinkArgType, sinkChan, err := tree.getLinkParam(
            predInputs, predOutputs, link,
        )

        if err != nil {
            return NodeParams{}, fmt.Errorf(
                "error forming input for node %d: %s",
                nodeId, err,
            )
        }

        err = ret.Params.AddParam(srcPval, sinkChan, sinkArgType)
        if err != nil {
            return NodeParams{}, err
        }
    }

    return ret, nil
}

func (tree *WorkflowExecutionState) createInputNode(
    inputs NodeParams, ancList []int, failed bool,
) (*NodeExec, error) {
    nodeAsyncAncs, ok := tree.index.AsyncAncestors[inputs.NodeId]
    if !ok {
        return nil, fmt.Errorf(
            "no async ancs for node %d; possible malformed index",
            inputs.NodeId,
        )
    }

    if len(nodeAsyncAncs) != len(ancList) {
        return nil, fmt.Errorf(
            "got node %d ancestor list of len %d, expected len %d: %#v",
            inputs.NodeId, len(ancList), len(nodeAsyncAncs), ancList,
        )
    }

    lastAsyncAncNodeId := nodeAsyncAncs[len(nodeAsyncAncs)-1]
    lastAsyncAncRunId := ancList[len(ancList)-1]
    lastAsyncAncAncList := ancList[:len(ancList)-1]
    pred := tree.lookupNode(lastAsyncAncNodeId, lastAsyncAncAncList)
    if pred == nil {
        return nil, fmt.Errorf(
            "no instance of node %d with ancestor list %#v",
            lastAsyncAncNodeId, lastAsyncAncAncList,
        )
    }

    if pred.succs == nil {
        pred.succs = make(map[int]map[int]*NodeExec)
    }
    if pred.succs[inputs.NodeId] == nil {
        pred.succs[inputs.NodeId] = make(map[int]*NodeExec)
    }

    finished := false
    if failed {
        finished = true
    }

    ancListStr := fmt.Sprintf("%#v", ancList)
    insertionVal := &NodeExec{
        nodeId:   inputs.NodeId,
        finished: finished,
        inputs:   inputs.Params,
        pred:     pred,
        success: !failed,
    }
    pred.succs[inputs.NodeId][lastAsyncAncRunId] = insertionVal
    tree.runsByAncList[inputs.NodeId][ancListStr] = insertionVal

    return insertionVal, nil
}

func (tree *WorkflowExecutionState) markRecursiveFailure(
    inputs NodeParams,
) error {
    node := tree.lookupNode(inputs.NodeId, inputs.AncList)
    node.success = false
    if err := markNodeComplete(node, inputs.AncList, true); err != nil {
        return fmt.Errorf(
            "error marking node %d, anc list %v complete: %s",
            inputs.NodeId, inputs.AncList, err,
        )
    }

    nodeAncList := tree.index.AsyncAncestors[inputs.NodeId]
    nodeDescIds := make([]int, 0)
    for descId := range tree.index.Descendants[inputs.NodeId] {
        nodeDescIds = append(nodeDescIds, descId)
    }

    // Process descendants by distance from root of execution tree.
    // This ensures that, as we create dummy nodes for each failed
    // descendant of the failed node, the parent node has always been
    // created before we try to create the child.
    slices.SortFunc(
        nodeDescIds, func(descId1, descId2 int) int {
            desc1AsyncAncs := tree.index.AsyncAncestors[descId1]
            desc2AsyncAncs := tree.index.AsyncAncestors[descId2]
            return len(desc1AsyncAncs) - len(desc2AsyncAncs)
        },
    )

    for _, descId := range nodeDescIds {
        descAsyncAncs := tree.index.AsyncAncestors[descId]
        if len(nodeAncList) >= len(descAsyncAncs) {
            descAncList := inputs.AncList[:len(descAsyncAncs)]
            descNode, err := tree.createInputNode(NodeParams{
                NodeId: descId,
                AncList: descAncList,
            }, descAncList, false)

            if err != nil {
                return fmt.Errorf(
                    "error creating input node %d w/ anc list %v: %s",
                    descId, descAncList, err,
                )
            }

            if err := markNodeComplete(descNode, descAncList, true); err != nil {
                return fmt.Errorf(
                    "error marking node %d, anc list %v complete: %s",
                    descId, descAncList, err,
                )
            }
        }
    }
    return nil
}

func (tree *WorkflowExecutionState) getSuccParams(
    inputs NodeParams, outputs []TypedParams, success bool,
) (map[int][]NodeParams, error) {
    if !success {
        if err := tree.markRecursiveFailure(inputs); err != nil {
            return nil, err
        }
        return map[int][]NodeParams{}, nil
    }

    if err := tree.addCmdResults(inputs, outputs); err != nil {
        return nil, fmt.Errorf("error adding results: %s", err)
    }

    out := make(map[int][]NodeParams)
    for _, succId := range tree.index.Succs[inputs.NodeId] {
        candidateSuccAncLists, err := tree.getCandidateAncLists(
            inputs.NodeId, succId, inputs.AncList,
        )
        if err != nil {
            return nil, err
        }

        succAncLists, predInps, predOuts, err := tree.triggerSuccs(
            succId, candidateSuccAncLists,
        )
        if err != nil {
            return nil, fmt.Errorf(
                "error getting anc lists of succ %d: %s", succId, err,
            )
        }

        if len(succAncLists) > 0 {
            out[succId] = make([]NodeParams, len(succAncLists))
        }
        for i := 0; i < len(succAncLists); i++ {
            succInputs, err := tree.formInputs(
                succId, predInps[i], predOuts[i], succAncLists[i],
            )
            if err != nil {
                return nil, fmt.Errorf(
                    "error forming inputs for node %d with anc list %#v: %s",
                    succId, succAncLists[i], err,
                )
            }
            _, err = tree.createInputNode(succInputs, succAncLists[i], false)
            if err != nil {
                return nil, fmt.Errorf(
                    "error creating input node %d w/ anc list %#v : %s",
                    succInputs.NodeId, succAncLists[i], err,
                )
            }
            out[succId][i] = succInputs
        }
    }

    barrierId, barrierAncList, err := tree.getEligibleBarriers(
        inputs.NodeId, inputs.AncList,
    )
    if err != nil {
        return nil, fmt.Errorf("error getting eligible barriers: %s", err)
    }
    if barrierAncList != nil {
        succAncLists, predInps, predOuts, err := tree.triggerSuccs(
            barrierId, [][]int{barrierAncList},
        )
        if err != nil {
            return nil, fmt.Errorf(
                "error getting anc lists of succ %d: %s", barrierId, err,
            )
        }

        if len(succAncLists) > 0 {
            succInputs, err := tree.formInputs(
                barrierId, predInps[0], predOuts[0], succAncLists[0],
            )
            if err != nil {
                return nil, fmt.Errorf(
                    "error forming inputs for node %d with anc list %#v: %s",
                    barrierId, succAncLists[0], err,
                )
            }

            tree.createInputNode(succInputs, succAncLists[0], false)
            out[barrierId] = []NodeParams{succInputs}
        }
    }
    return out, nil
}

func correctArgType(pValRaw any, srcArgType, sinkArgType WorkflowArgType) (any, error) {
    srcIsList := strings.HasSuffix(srcArgType.ArgType, "list")
    srcIsList = srcIsList || srcArgType.ArgType == "patternQuery"
    sinkIsList := strings.HasSuffix(sinkArgType.ArgType, "list")
    srcBaseType := strings.Split(srcArgType.ArgType, " ")[0]
    sinkBaseType := strings.Split(sinkArgType.ArgType, " ")[0]

    bothStringTypes := argTypeIsStr(srcBaseType) && argTypeIsStr(sinkBaseType)
    if !bothStringTypes && srcBaseType != sinkBaseType {
        return nil, fmt.Errorf(
            "invalid types %s and %s (val %v)",
            srcArgType.ArgType, sinkArgType.ArgType, pValRaw,
        )
    }

    if srcIsList && !sinkIsList {
        switch v := pValRaw.(type) {
        case []any:
            {
                return (pValRaw.([]any))[0], nil
            }
        case []string:
            {
                return (pValRaw.([]string))[0], nil
            }
        case []int:
            {
                return (pValRaw.([]int))[0], nil
            }
        case []float64:
            {
                return (pValRaw.([]float64))[0], nil
            }
        default:
            {
                return nil, fmt.Errorf("unrecognized type %v", v)
            }
        }
    }

    if !srcIsList && sinkIsList {
        switch pValRaw.(type) {
        case []string, []int, []float64:
            {
                return pValRaw, nil
            }
        default:
            {
                return []any{pValRaw}, nil
            }
        }
    }
    return pValRaw, nil
}
