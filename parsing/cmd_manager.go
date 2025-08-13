package parsing

import (
	"encoding/json"
	"fmt"
	"os"
	"time"
)

type CmdManager struct {
	state           *WorkflowExecutionState
	currentMaxCmdId int
	cmdIdToParams   map[int]NodeParams
	remainingIters  map[int]map[string]map[int]struct{}
}

func NewCmdManager(
	workflow Workflow, index WorkflowIndex,
) CmdManager {
	var cmdMan CmdManager
	cmdMan.cmdIdToParams = map[int]NodeParams{}
	cmdMan.remainingIters = make(map[int]map[string]map[int]struct{})
	for nodeId := range workflow.Nodes {
		cmdMan.remainingIters[nodeId] = make(map[string]map[int]struct{})
	}
    wes := NewWorkflowExecutionState(workflow, index)
	cmdMan.state = &wes
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
) ([]CmdTemplate, error) {
	inputParams, inputParamsExist := cmdMan.cmdIdToParams[completedCmd.Id]
	if !inputParamsExist {
		return nil, fmt.Errorf("cmd has invalid ID %d", completedCmd.Id)
	}

	nodeId := inputParams.NodeId
	node := cmdMan.state.workflow.Nodes[nodeId]
	nodeRunId := fmt.Sprintf("%#v", inputParams.AncList)
	delete(cmdMan.remainingIters[nodeId][nodeRunId], completedCmd.Id)

    fmt.Printf("Completed node %d, nodeRunId %s, cmd ID %d\n", nodeId, nodeRunId, completedCmd.Id)

	var outputTp TypedParams
	for k, v := range rawOutputs {
		argType, argTypeExists := node.ArgTypes[k]
		if !argTypeExists {
			return nil, fmt.Errorf(
				"node %d output argtype %s does not exist",
				nodeId, k,
			)
		}
		outputTp.AddSerializedParam(v, k, argType)
	}


	if !node.Async && len(cmdMan.remainingIters[nodeId][nodeRunId]) > 0 {
        fmt.Printf("\tCmd %d has %d remaining runs before triggering succs\n", nodeId, len(cmdMan.remainingIters[nodeId][nodeRunId]))
        fmt.Printf("\tRemaining cmds: %#v\n", cmdMan.remainingIters[nodeId][nodeRunId])
		return []CmdTemplate{}, nil
	}

	succParams, err := cmdMan.state.getSuccParams(
		inputParams, []TypedParams{outputTp},
	)

	jsonStr, _ := json.MarshalIndent(inputParams, "", "\t")
	os.WriteFile(
		fmt.Sprintf("/home/patrick/state/input_%s.json", time.Now().Format("2006-01-02__15:04:05")),
		jsonStr, 0644,
	)

	jsonStr, _ = json.MarshalIndent(cmdMan.state, "", "\t")
	os.WriteFile(
		fmt.Sprintf("/home/patrick/state/state_%s.json", time.Now().Format("2006-01-02__15:04:05")),
		jsonStr, 0644,
	)

	jsonStr, _ = json.MarshalIndent(outputTp, "", "\t")
	os.WriteFile(
		fmt.Sprintf("/home/patrick/state/output_%s.json", time.Now().Format("2006-01-02__15:04:05")),
		jsonStr, 0644,
	)

	if err != nil {
		return nil, err
	}

	cmds, err := cmdMan.getCmdsFromParams(succParams, glob)
    if err == nil {
        for _, v := range cmds {
            fmt.Printf("\tGenerated cmd ID %d for node %d\n", v.Id, v.NodeId)
        }
    }
    return cmds, err
}

func (cmdMan *CmdManager) IsComplete() (bool, error) {
	return cmdMan.state.IsComplete()
}

func (cmdMan *CmdManager) GetInitialCmds(glob GlobFunc) ([]CmdTemplate, error) {
	initialParams, err := cmdMan.state.getInitialNodeParams()
	if err != nil {
		return nil, err
	}

	return cmdMan.getCmdsFromParams(initialParams, glob)
}

func (cmdMan *CmdManager) getCmdsFromParams(
	nodeParams map[int][]NodeParams, glob GlobFunc,
) ([]CmdTemplate, error) {
	ret := make([]CmdTemplate, 0)
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
                fmt.Printf("CURRENT MAX ID: %d\n", cmdMan.currentMaxCmdId)
				cmdId := cmdMan.currentMaxCmdId
				nodeCmds[i].Id = cmdId
				cmdMan.cmdIdToParams[cmdId] = paramSet
				cmdMan.remainingIters[nodeId][nodeRunId][cmdId] = struct{}{}
				cmdMan.currentMaxCmdId += 1
			}

			ret = append(ret, nodeCmds...)
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
