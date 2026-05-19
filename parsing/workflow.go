package parsing

import (
	"fmt"
	"strings"
)

type WorkflowLink interface {
    GetSrcId() int
    GetSinkId() int
    GetSrcPname() string
    GetSinkPname() string
    LinkType() string
}

type WorkflowNode interface {
    GetId() int
    GetTitle() string
    ArgIsInputFile(string) bool
    ArgIsOutputFile(string) bool
    GetImageName() string
    GetImageTag() string
    IsAsync() bool
    BarrierSrc() *int
    ParseOutputs(map[string]string) TypedParams
    ParseCmd(TypedParams) ([]CmdTemplate, error)
    ResolveGlob(*TypedParams, GlobFunc) error
}

type Workflow interface {
    GetVersion() string
    GetNumNodes() int
    GetNodeIds() []int
    GetArgType(int, string) (string, error)
    GetParam(int, string) (any, error)
    SetParam(int, string, any) (error)
    GetNodes() map[int]WorkflowNode
    GetLinks() []WorkflowLink
    GetNode(int) (WorkflowNode, bool)
    GetBaseParams() (map[int]TypedParams, error)
    NodeExists(int) bool
    DryRun() ([]string, error)
}

type WorkflowNodeV0 struct {
    Id             int
    ImageName      string
    ImageTag       string
    Title          string
    Command        []string
    ArgTypes       map[string]WorkflowArgType
    //BaseProps      map[string]interface{}
    ArgOrder       []string
    OptionsChecked map[string]bool
    RequiredParams []string
    ResourceReqs   ResourceVector
    Async          bool
    BarrierFor     *int
    Iterate        bool
    IterGroupSize  map[string]int
    IterAttrs      []string
}

func (node *WorkflowNodeV0) GetId() int {
    return node.Id
}

func (node *WorkflowNodeV0) GetTitle() string {
    return node.Title
}

func (node *WorkflowNodeV0) GetImageName() string {
    return node.ImageName
}

func (node *WorkflowNodeV0) GetImageTag() string {
    return node.ImageTag
}

func (node *WorkflowNodeV0) ArgIsInputFile(arg string) bool {
    argType, exists := node.ArgTypes[arg]
    // Pattern queries are evaluated on the same FS as the CMD is run
    // so there is no need to stage files
    if !exists || argType.ArgType == "patternQuery" {
        return false
    }
    return argType.InputFile != nil && *argType.InputFile
}

func (node *WorkflowNodeV0) ArgIsOutputFile(arg string) bool {
    argType, exists := node.ArgTypes[arg]
    if !exists {
        return false
    }
    return argType.OutputFile != nil && *argType.OutputFile 
}

func (node *WorkflowNodeV0) IsAsync() bool {
    return node.Async
}

func (node *WorkflowNodeV0) BarrierSrc() *int {
    return node.BarrierFor
}

func (node *WorkflowNodeV0) ParseOutputs(
    rawOutputs map[string]string,
) TypedParams {
    var outputTp TypedParams
    for k, v := range rawOutputs {
        argType, argTypeExists := node.ArgTypes[k]
        if !argTypeExists {
            continue
        }
        outputTp.AddSerializedParam(v, k, argType.ArgType)
    }
    return outputTp
}

func (node *WorkflowNodeV0) ResolveGlob(tp *TypedParams, glob GlobFunc) error {
    return resolvePqs(*node, tp, glob)
}

func (node *WorkflowNodeV0) ParseCmd(tp TypedParams) ([]CmdTemplate, error) {
    res, err := ParseNodeCmdV0(*node, tp)
    if err != nil {
        return res, err
    }
    for i := range res {
        res[i].Version = 0
    }
    return res, nil
}

type WorkflowLinkV0 struct {
    SourceNodeId  int
    SinkNodeId    int
    SourceChannel string
    SinkChannel   string
}

func (link *WorkflowLinkV0) LinkType() string {
    return "v0"
}

func (link *WorkflowLinkV0) GetSrcId() int {
    return link.SourceNodeId
}

func (link *WorkflowLinkV0) GetSinkId() int {
    return link.SinkNodeId
}

func (link *WorkflowLinkV0) GetSrcPname() string {
    return link.SourceChannel
}

func (link *WorkflowLinkV0) GetSinkPname() string {
    return link.SinkChannel
}

type WorkflowV0 struct {
    Nodes         map[int]WorkflowNodeV0
    NodeBaseProps map[int]map[string]any
    Links         []WorkflowLinkV0
}

func (node *WorkflowV0) GetVersion() string {
    return "biodepot.legacy"
}

func (wf *WorkflowV0) GetNumNodes() int {
    return len(wf.Nodes)
}

func (wf *WorkflowV0) GetNodes() map[int]WorkflowNode {
    nodeIds := make(map[int]WorkflowNode)
    for id, node := range wf.Nodes {
        nodeIds[id] = &node
    }
    return nodeIds
}
func (wf *WorkflowV0) GetParam(nodeId int, pname string) (any, error) {
    _, nodeExists := wf.NodeBaseProps[nodeId]
    if !nodeExists {
        return "", fmt.Errorf("node %d does not exist", nodeId)
    }
    val, valExists := wf.NodeBaseProps[nodeId][pname]
    if !valExists {
        return "", fmt.Errorf("node %d has no value of property %s", nodeId, pname)
    }
    return val, nil
}

func (wf *WorkflowV0) SetParam(nodeId int, pname string, val any) (error) {
    _, nodeExists := wf.NodeBaseProps[nodeId]
    if !nodeExists {
        return fmt.Errorf("node %d does not exist", nodeId)
    }
    wf.NodeBaseProps[nodeId][pname] = val
    return nil
}

func (wf *WorkflowV0) GetNodeIds() []int {
    nodeIds := make([]int, 0)
    for id := range wf.Nodes {
        nodeIds = append(nodeIds, id)
    }
    return nodeIds
}

func (wf *WorkflowV0) DryRun() ([]string, error) {
    return DryRun(*wf)
}

func (wf *WorkflowV0) GetArgType(nodeId int, pname string) (string, error) {
    node, nodeExists := wf.Nodes[nodeId]
    if !nodeExists {
        return "", fmt.Errorf("node %d does not exist", nodeId)
    }
    argType, exists := node.ArgTypes[pname]
    if !exists {
        return "", fmt.Errorf("node %d, argtype %s does not exist", nodeId, pname)
    }
    return argType.ArgType, nil
}

func (wf *WorkflowV0) GetLinks() []WorkflowLink {
    links := make([]WorkflowLink, 0)
    for _, link := range wf.Links {
        links = append(links, &link)
    }
    return links
}

func (wf *WorkflowV0) GetNode(id int) (WorkflowNode, bool) {
    node, exists := wf.Nodes[id]
    return &node, exists
}

func (wf *WorkflowV0) NodeExists(nodeId int) bool {
    _, exists := wf.Nodes[nodeId]
    return exists
}

func (wf *WorkflowV0) GetBaseParams() (map[int]TypedParams, error) {
    ret := make(map[int]TypedParams)
    for nodeId, node := range wf.Nodes {
        baseProps := wf.NodeBaseProps[nodeId]

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

func GetLinkParam(
    wf Workflow, predInputs map[int]TypedParams, 
    predOutputs map[int]TypedParams, link WorkflowLink,
) (any, string, string, error) {
    srcNode := link.GetSrcId()
    sinkNode := link.GetSinkId()
    srcChan := link.GetSrcPname()
    sinkChan := link.GetSinkPname()

    srcArgType, err := wf.GetArgType(srcNode, srcChan)
    if err != nil {
        return nil, "", "", fmt.Errorf(
            "bad argtype: node %d has no parameter %s",
            srcNode, srcChan,
        )
    }

    sinkArgType, err := wf.GetArgType(sinkNode, sinkChan)
    if err != nil {
        return nil, "", "", fmt.Errorf(
            "bad argtype: node %d has no parameter %s",
            sinkNode, sinkChan,
        )
    }

    srcOutputs, srcOutputsExist := predOutputs[srcNode]
    if !srcOutputsExist {
        return nil, "", "", fmt.Errorf(
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
            return nil, "", "", fmt.Errorf(
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
            return nil, "", "", fmt.Errorf(
                "param %s not found in node %d inputs or outputs",
                srcChan, srcNode,
            )
        }
    }

    correctedSrcPval, err := correctArgType(srcPval, srcArgType, sinkArgType)
    if err != nil {
        return nil, "", "", fmt.Errorf(
            "error converting param %s of node %d: %s", srcChan, srcNode, err,
        )
    }

    return correctedSrcPval, sinkArgType, sinkChan, nil
}

func correctArgType(pValRaw any, srcArgType, sinkArgType string) (any, error) {
    srcIsList := strings.HasSuffix(srcArgType, "list")
    srcIsList = srcIsList || srcArgType == "patternQuery"
    sinkIsList := strings.HasSuffix(sinkArgType, "list")
    srcBaseType := strings.Split(srcArgType, " ")[0]
    sinkBaseType := strings.Split(sinkArgType, " ")[0]

    bothStringTypes := argTypeIsStr(srcBaseType) && argTypeIsStr(sinkBaseType)
    if !bothStringTypes && srcBaseType != sinkBaseType {
        return nil, fmt.Errorf(
            "invalid types %s and %s (val %v)",
            srcArgType, sinkArgType, pValRaw,
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
        case PatternQuery:
            {
                cast, _ := pValRaw.(PatternQuery)
                return cast, nil
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
