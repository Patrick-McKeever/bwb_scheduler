package parsing

import (
    "encoding/json"
    "fmt"
)

type CmdManager struct {
    state           *WorkflowExecutionState
    jobConfig       JobConfig
    currentMaxCmdId int
    cmdIdToParams   map[int]NodeParams
    completedCmds   map[int]struct{}
    remainingIters  map[int]map[string]map[int]struct{}
}

func NewCmdManager(
    workflow Workflow, index WorkflowIndex, config JobConfig,
) CmdManager {
    var cmdMan CmdManager
    cmdMan.cmdIdToParams = map[int]NodeParams{}
    cmdMan.remainingIters = make(map[int]map[string]map[int]struct{})
    cmdMan.completedCmds = make(map[int]struct{})
    for nodeId := range workflow.Nodes {
        cmdMan.remainingIters[nodeId] = make(map[string]map[int]struct{})
    }
    wes := NewWorkflowExecutionState(workflow, index)
    cmdMan.state = &wes
    cmdMan.jobConfig = config
    return cmdMan
}

func (cmdMan *CmdManager) GetImageNames() []string {
    imageNames := make([]string, 0)
    for _, node := range cmdMan.state.workflow.Nodes {
        imageName := fmt.Sprintf("%s:%s", node.ImageName, node.ImageTag)
        imageNames = append(imageNames, imageName)
    }
    return imageNames
}
func (cmdMan *CmdManager) GetSuccCmds(
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
    node := cmdMan.state.workflow.Nodes[nodeId]
    nodeRunId := fmt.Sprintf("%#v", inputParams.AncList)
    delete(cmdMan.remainingIters[nodeId][nodeRunId], completedCmd.Id)

    var outputTp TypedParams
    for k, v := range rawOutputs {
        argType, argTypeExists := node.ArgTypes[k]
        if !argTypeExists {
            continue
        }
        outputTp.AddSerializedParam(v, k, argType)
    }

    if !node.Async && len(cmdMan.remainingIters[nodeId][nodeRunId]) > 0 {
        return nil, nil
    }

    succParams, err := cmdMan.state.getSuccParams(
        inputParams, []TypedParams{outputTp}, success,
    )

    if err != nil {
        return nil, err
    }

    cmds, err := cmdMan.getCmdsFromParams(succParams, glob)
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

func (cmdMan *CmdManager) IsComplete() bool {
    return cmdMan.state.IsComplete()
}

func (cmdMan *CmdManager) HasFailed() bool {
    return cmdMan.state.HasFailed()
}

func (cmdMan *CmdManager) GetInitialCmds(glob GlobFunc) (map[int][]CmdTemplate, error) {
    initialParams, err := cmdMan.state.getInitialNodeParams()
    if err != nil {
        return nil, err
    }

    return cmdMan.getCmdsFromParams(initialParams, glob)
}

func (cmdMan *CmdManager) getCmdsFromParams(
    nodeParams map[int][]NodeParams, glob GlobFunc,
) (map[int][]CmdTemplate, error) {
    ret := make(map[int][]CmdTemplate, 0)
    for nodeId, paramSets := range nodeParams {
        node := cmdMan.state.workflow.Nodes[nodeId]
        for _, paramSet := range paramSets {
            nodeCmds, err := ParseNodeCmd(node, paramSet.Params, glob)
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
