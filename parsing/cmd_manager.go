package parsing

import (
	"encoding/json"
	"fmt"
)

type CmdManager interface {
	GetImageNames() []string
	GetSuccCmds(
		CmdTemplate, map[string]string, GlobFunc, bool,
	) (map[int][]CmdTemplate, error)
	IsComplete() bool
	HasFailed() bool
	GetInitialCmds(glob GlobFunc) (map[int][]CmdTemplate, error)
}

type CmdManagerV0 struct {
	state           *WorkflowExecutionState
	jobConfig       JobConfig
	currentMaxCmdId int
	cmdIdToParams   map[int]NodeParams
	completedCmds   map[int]struct{}
	remainingIters  map[int]map[string]map[int]struct{}
    nodeStatus      map[int]string
}

func NewCmdManager(
	workflow Workflow, index WorkflowIndex, config JobConfig,
) CmdManagerV0 {
	var cmdMan CmdManagerV0
	cmdMan.cmdIdToParams = map[int]NodeParams{}
	cmdMan.remainingIters = make(map[int]map[string]map[int]struct{})
	cmdMan.completedCmds = make(map[int]struct{})
    cmdMan.nodeStatus = make(map[int]string)
	for _, nodeId := range workflow.GetNodeIds() {
		cmdMan.remainingIters[nodeId] = make(map[string]map[int]struct{})
        cmdMan.nodeStatus[nodeId] = "AWAITING_PREDECESSORS"
	}
	wes := NewWorkflowExecutionState(workflow, index)
	cmdMan.state = &wes
	cmdMan.jobConfig = config
	return cmdMan
}

func (cmdMan *CmdManagerV0) GetImageNames() []string {
	imageNames := make([]string, 0)
	nodes := cmdMan.state.workflow.GetNodes()
	for _, node := range nodes {
		imageName := fmt.Sprintf("%s:%s", node.GetImageName(), node.GetImageTag())
		imageNames = append(imageNames, imageName)
	}
	return imageNames
}

func (cmdMan *CmdManagerV0) GetNodeStatus() map[int]string {
    return cmdMan.nodeStatus
}

func (cmdMan *CmdManagerV0) GetSuccCmds(
	completedCmd CmdTemplate,
	rawOutputs map[string]string,
	glob GlobFunc,
	success bool,
) (map[int][]CmdTemplate, error) {
	inputParams, inputParamsExist := cmdMan.cmdIdToParams[completedCmd.Id]
	if !inputParamsExist {
		return nil, fmt.Errorf("cmd has invalid ID %d", completedCmd.Id)
	}

	// Do not return successors for already completed cmd,
	// these have already been consumed by some prior call.
	if _, ok := cmdMan.completedCmds[completedCmd.Id]; ok {
		return nil, nil
	}

	cmdMan.completedCmds[completedCmd.Id] = struct{}{}
	nodeId := inputParams.NodeId
	node, _ := cmdMan.state.workflow.GetNode(nodeId)
	nodeRunId := fmt.Sprintf("%#v", inputParams.AncList)
	delete(cmdMan.remainingIters[nodeId][nodeRunId], completedCmd.Id)

	outputTp := node.ParseOutputs(rawOutputs)
	if !node.IsAsync() && len(cmdMan.remainingIters[nodeId][nodeRunId]) > 0 {
		return nil, nil
	}

    cmdMan.nodeStatus[completedCmd.NodeId] = "FINISHED"
	succParams, err := cmdMan.state.getSuccParams(
		inputParams, []TypedParams{outputTp}, success,
	)

	if err != nil {
		return nil, err
	}

	cmds, err := cmdMan.getCmdsFromParams(succParams, glob)
	if err != nil {
		return nil, err
	}

	for nodeId := range cmds {
		for i := range cmds[nodeId] {
			RemoveElideableFileXfers(
				&cmds[nodeId][i], cmdMan.state.workflow, cmdMan.state.index,
				cmdMan.jobConfig,
			)
		}
	}

	return cmds, err
}

func (cmdMan *CmdManagerV0) IsComplete() bool {
	return cmdMan.state.IsComplete()
}

func (cmdMan *CmdManagerV0) HasFailed() bool {
	return cmdMan.state.HasFailed()
}

func (cmdMan *CmdManagerV0) GetInitialCmds(glob GlobFunc) (map[int][]CmdTemplate, error) {
	initialParams, err := cmdMan.state.getInitialNodeParams()
	if err != nil {
		return nil, err
	}

	return cmdMan.getCmdsFromParams(initialParams, glob)
}

func resolvePqs(node WorkflowNodeV0, tp *TypedParams, glob GlobFunc) error {
	for pname, argType := range node.ArgTypes {
		if argType.ArgType == "patternQuery" {
			shouldEval := (argType.Flag != nil || argType.Env != nil ||
				(argType.IsArgument != nil && *argType.IsArgument))
			if shouldEval {
				err := tp.ResolvePq(pname, node, glob)
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (cmdMan *CmdManagerV0) getCmdsFromParams(
	nodeParams map[int][]NodeParams, glob GlobFunc,
) (map[int][]CmdTemplate, error) {
	ret := make(map[int][]CmdTemplate, 0)
	for nodeId, paramSets := range nodeParams {
        cmdMan.nodeStatus[nodeId] = "RUNNING"
		node, _ := cmdMan.state.workflow.GetNode(nodeId)
		for _, paramSet := range paramSets {
			if err := node.ResolveGlob(&paramSet.Params, glob); err != nil {
				return nil, fmt.Errorf(
					"error parsing node %d pattern queries: %s",
					nodeId, err,
				)
			}

			nodeCmds, err := node.ParseCmd(paramSet.Params)
			if err != nil {
				return nil, err
			}

			nodeRunId := fmt.Sprintf("%#v", paramSet.AncList)
			if cmdMan.remainingIters[nodeId][nodeRunId] == nil {
				cmdMan.remainingIters[nodeId][nodeRunId] = make(map[int]struct{})
			}

			for i := range nodeCmds {
				cmdId := cmdMan.currentMaxCmdId
				nodeCmds[i].Id = cmdId
				nodeCmds[i].Priority = cmdMan.state.index.MaxDistanceFromSink[nodeId]
				cmdMan.cmdIdToParams[cmdId] = paramSet
				cmdMan.remainingIters[nodeId][nodeRunId][cmdId] = struct{}{}
				cmdMan.currentMaxCmdId += 1
			}

			ret[nodeId] = append(ret[nodeId], nodeCmds...)
			if len(nodeCmds) == 0 {
				marshaledCmd, _ := json.MarshalIndent(paramSet.Params, "", "\t")
				return nil, fmt.Errorf(
					"params for node %d generated 0 commands: %s",
					nodeId, string(marshaledCmd),
				)
			}
		}
	}
	return ret, nil
}
