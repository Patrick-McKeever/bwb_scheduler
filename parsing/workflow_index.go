package parsing

import (
	"fmt"
)

// Data structure to package all the various data structures for
// looking up database graph structure.
type WorkflowIndex struct {
	Preds map[int][]int
	Succs map[int][]int
	// Lookup links based on (dstNode, dstPname)
	InLinks map[int]map[string]WorkflowLink
	// Lookup links based on (srcNode, srcPname)
	OutLinks         map[int]map[string]WorkflowLink
	AsyncAncestors   map[int][]int
	AsyncDescendants map[int]map[int]struct{}

    Ancestors   map[int]map[int]struct{}
    Descendants map[int]map[int]struct{}

	// Layered top sort where each inner array of node IDs is
	// nodes that can run independently once all nodes in prior
	// arrays have run.
	LayeredTopSort [][]int
	BarrierFor     map[int]int

	BaseParams map[int]TypedParams
}

func (index *WorkflowIndex) getStartNodes() []int {
	if len(index.LayeredTopSort) == 0 {
		return []int{}
	}
	return index.LayeredTopSort[0]
}

func (index *WorkflowIndex) getSharedAsyncAncs(id1 int, id2 int) ([]int, error) {
	asyncAnc1, id1Exists := index.AsyncAncestors[id1]
	if !id1Exists {
		return nil, fmt.Errorf("node %d not in async ancestor array", id1)
	}
	asyncAnc2, id2Exists := index.AsyncAncestors[id2]
	if !id2Exists {
		return nil, fmt.Errorf("node %d not in async ancestor array", id2)
	}
    shared := make([]int, 0)
	for i := 0; i < min(len(asyncAnc1), len(asyncAnc2)); i++ {
		if asyncAnc1[i] == asyncAnc2[i] {
            shared = append(shared, asyncAnc1[i])
		}
	}

	//if len(asyncAnc1) > len(asyncAnc2) && asyncAnc1[len(asyncAnc2)] == id2 {
	//	shared = append(shared, id2)
	//}
	//if len(asyncAnc2) > len(asyncAnc1) && asyncAnc2[len(asyncAnc1)] == id1 {
	//	shared = append(shared, id1)
	//}

    return shared, nil
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

                if _, sinkHasArgType := nodeCopy.ArgTypes[pname]; sinkHasArgType {
                    continue
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
) (map[int]map[int]struct{}, map[int]map[int]struct{}) {
	ancestorOf := make(map[int]map[int]struct{})
	for i := 0; i < len(topSort); i++ {
		layer := topSort[i]
		for _, sourceNode := range layer {
			for _, outLink := range outLinks[sourceNode] {
				if ancestorOf[outLink.SinkNodeId] == nil {
					ancestorOf[outLink.SinkNodeId] = make(map[int]struct{})
				}

				ancestorOf[outLink.SinkNodeId][sourceNode] = struct{}{}
				for anc := range ancestorOf[sourceNode] {
					ancestorOf[outLink.SinkNodeId][anc] = struct{}{}
				}
			}
		}
	}

	// (ancestor, descendant) -> T (using this more as hash set than map)
	descendantOf := make(map[int]map[int]struct{})
	for i := len(topSort) - 1; i >= 0; i-- {
		layer := topSort[i]
		for _, sinkNode := range layer {
			for _, inLink := range inLinks[sinkNode] {
				if descendantOf[inLink.SourceNodeId] == nil {
					descendantOf[inLink.SourceNodeId] = make(map[int]struct{})
				}

				descendantOf[inLink.SourceNodeId][sinkNode] = struct{}{}
				for desc := range descendantOf[sinkNode] {
					descendantOf[inLink.SourceNodeId][desc] = struct{}{}
				}
			}
		}
	}

	return ancestorOf, descendantOf
}

func parseAsyncAndBarriers(
	workflow Workflow, topSort [][]int,
	descendantOf map[int]map[int]struct{},
) (map[int][]int, map[int]map[int]struct{}, map[int]int, error) {
	barrierFor := make(map[int]int)
	for nodeId, node := range workflow.Nodes {
		if node.BarrierFor != nil {
			srcNode, srcNodeExists := workflow.Nodes[*node.BarrierFor]
			if !srcNodeExists {
				return nil, nil, nil, fmt.Errorf(
					"node %d is barrier for non-existent node %d",
					nodeId, *node.BarrierFor,
				)
			}

			if !srcNode.Async {
				return nil, nil, nil, fmt.Errorf(
					"node %d is barrier for non-async node %d",
					nodeId, *node.BarrierFor,
				)
			}

			if barrier, barrierExists := barrierFor[*node.BarrierFor]; barrierExists {
				return nil, nil, nil, fmt.Errorf(
					"node %d is barrier for (at least) two nodes: %d and %d",
					nodeId, *node.BarrierFor, barrier,
				)
			}

			if _, ok := descendantOf[*node.BarrierFor][nodeId]; !ok {
				return nil, nil, nil, fmt.Errorf(
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
    for nodeId := range workflow.Nodes {
        asyncAncestors[nodeId] = []int{-1}
    }

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
                _, isDesc := descendantOf[barrierNodeId][ancBarrier] 
				if ancHasBarrier && !isDesc {
					return nil, nil, nil, fmt.Errorf(
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
                _, isDesc := descendantOf[barrier][nodeId] 
				if nodeId == barrier || isDesc {
					continue
				}
			}
			asyncAncestorsBeforeBarrier[nodeId] = append(
				asyncAncestorsBeforeBarrier[nodeId],
				asyncAncestorId,
			)
		}
	}
    asyncAncestorsBeforeBarrier[-1] = []int{}

	asyncDescendants := make(map[int]map[int]struct{})
    asyncDescendants[-1] = map[int]struct{}{}
	for nodeId, asyncAncList := range asyncAncestorsBeforeBarrier {
        asyncDescendants[-1][nodeId] = struct{}{}
		if len(asyncAncList) > 0 {
			lastAsyncAnc := asyncAncList[len(asyncAncList)-1]
			if asyncDescendants[lastAsyncAnc] == nil {
				asyncDescendants[lastAsyncAnc] = make(map[int]struct{})
			}
			asyncDescendants[lastAsyncAnc][nodeId] = struct{}{}
		}
	}

	for nodeId := range workflow.Nodes {
		asyncAncestors, hasAsyncAncestors := asyncAncestorsBeforeBarrier[nodeId]
		if !hasAsyncAncestors || len(asyncAncestors) == 0 {
			continue
		}

		pred := asyncAncestors[0]
		for i := 1; i < len(asyncAncestors); i++ {
            _, isDesc := descendantOf[pred][asyncAncestors[i]]
			if !isDesc && pred != -1 {
				return nil, nil, nil, fmt.Errorf(
					"node %d has non-nested async ancestors %d and %d",
					nodeId, pred, asyncAncestors[i],
				)
			}
            pred = asyncAncestors[i]
		}
	}

	return asyncAncestorsBeforeBarrier, asyncDescendants, barrierFor, nil
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

func getBaseParams(workflow Workflow) (map[int]TypedParams, error) {
	ret := make(map[int]TypedParams)
	for nodeId, node := range workflow.Nodes {
		baseProps := workflow.NodeBaseProps[nodeId]

		nodeTp, err := parseTypedParams(node, baseProps)
		if err != nil {
			return nil, fmt.Errorf(
				"error parsing params of node %d: %s",
				nodeId, err,
			)
		}
		ret[nodeId] = nodeTp
	}
	return ret, nil
}

// Inputs:
//   - workflow: Workflow whose arg types will be edited so as to create
//     an argtype entry for each incoming link if one does not exist.
//
// Outputs:
//   - A workflow index giving data structures to facilitate easy lookup.
func ParseAndValidateWorkflow(workflow *Workflow) (WorkflowIndex, error) {
	var index WorkflowIndex
	index.Preds, index.Succs = getPredsAndSuccs(*workflow)
	topSort, err := layeredTopSort(*workflow)
	if err != nil {
		return WorkflowIndex{}, fmt.Errorf("error in top sort: %s", err)
	}
	index.LayeredTopSort = topSort

	inLinks, outLinks, err := getInAndOutLinks(*workflow)
	if err != nil {
		return WorkflowIndex{}, fmt.Errorf("error parsing links: %s", err)
	}

	index.InLinks = inLinks
	index.OutLinks = outLinks

	err = propagateArgTypes(workflow, topSort, inLinks)
	if err != nil {
		return WorkflowIndex{}, fmt.Errorf("error propagating arg types: %s", err)
	}

	index.Ancestors, index.Descendants = getAncestorsAndDescendants(
        topSort, inLinks, outLinks,
    )
	index.AsyncAncestors, index.AsyncDescendants, index.BarrierFor, err = parseAsyncAndBarriers(
		*workflow, topSort, index.Descendants,
	)
	if err != nil {
		return WorkflowIndex{}, err
	}

	index.BaseParams, err = getBaseParams(*workflow)
	if err != nil {
		return WorkflowIndex{}, err
	}

	return index, nil
}
