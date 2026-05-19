package parsing

import (
    "fmt"
    "strings"
)

type ResolvedWorkflow struct {
    RunId           string               `json:"run_id"`
    UseLocalStorage bool                 `json:"use_local_storage,omitempty"`
    Nodes           map[int]ResolvedNode `json:"nodes"`
    Links           []ResolvedLink       `json:"links"`
}

type ResolvedNode struct {
    Id             int                   `json:"id"`
    Title          string                `json:"title"`
    Description    string                `json:"description,omitempty"`
    ImageName      string                `json:"image_name"`
    ImageTag       string                `json:"image_tag"`
    Launch         Launch                `json:"launch"`
    Inputs         map[string]NodeInput  `json:"inputs,omitempty"`
    Outputs        map[string]NodeOutput `json:"outputs,omitempty"`
    Resources      Resources             `json:"resources"`
    SchedulerHints map[string]any        `json:"scheduler_hints,omitempty"`
    Staging        *Staging              `json:"staging,omitempty"`
    Async          bool                  `json:"async,omitempty"`
    BarrierFor     *int                  `json:"barrier_for,omitempty"` // null in JSON → nil
}

type Launch struct {
    Command []string          `json:"command"`
    Env     map[string]string `json:"env,omitempty"`
    Cwd     string            `json:"cwd,omitempty"`
    Shell   bool              `json:"shell,omitempty"`
}

type NodeInput struct {
    Name   string      `json:"name"`
    Kind   string      `json:"kind"` // file | directory | file_list | directory_list
    Source InputSource `json:"source"`
    Mount  *InputMount `json:"mount,omitempty"`
}

type InputSource struct {
    Type    string `json:"type"` // path | node_output | pattern_query
    Path    string `json:"path,omitempty"`
    NodeId  *int   `json:"node_id,omitempty"`
    Output  string `json:"output,omitempty"`
    Root    string `json:"root,omitempty"`
    Pattern string `json:"pattern,omitempty"`
}

type InputMount struct {
    ContainerPath string `json:"container_path"`
    Mode          string `json:"mode,omitempty"` // ro | rw
}

type NodeOutput struct {
    Name string `json:"name"`
    Kind string `json:"kind"`
    Path string `json:"path"`
}

type Resources struct {
    Cores int `json:"cores"`
    MemMb int `json:"mem_mb"`
    Gpus  int `json:"gpus"`
}

type Staging struct {
    Mode string `json:"mode"` // shared_fs | rsync | object_store
}

type ResolvedLink struct {
    Source       int    `json:"source"`
    Sink         int    `json:"sink"`
    SourceOutput string `json:"source_output"`
    SinkInput    string `json:"sink_input"`
}

func (node *ResolvedNode) GetId() int {
    return node.Id
}

func (node *ResolvedNode) GetTitle() string {
    return node.Title
}

func (node *ResolvedNode) GetImageName() string {
    fmt.Println("HERE", node.ImageName, node.ImageTag)
    return node.ImageName
}

func (node *ResolvedNode) GetImageTag() string {
    return node.ImageTag
}

func (node *ResolvedNode) ArgIsInputFile(arg string) bool {
    input, exists := node.Inputs[arg]
    if !exists {
        return false
    }
    return input.Kind == "file" || input.Kind == "directory" ||
        input.Kind == "file list" || input.Kind == "directory list"
}

func (node *ResolvedNode) ArgIsOutputFile(arg string) bool {
    output, exists := node.Outputs[arg]
    if !exists {
        return false
    }
    return output.Kind == "file" || output.Kind == "directory" ||
        output.Kind == "file list" || output.Kind == "directory list"
}

func (node *ResolvedNode) IsAsync() bool {
    return node.Async
}

func (node *ResolvedNode) BarrierSrc() *int {
    return node.BarrierFor
}

func (node *ResolvedNode) ParseOutputs(
    rawOutputs map[string]string,
) TypedParams {
    var outputTp TypedParams
    for k, v := range rawOutputs {
        outputMetadata, outputExists := node.Outputs[k]
        if !outputExists {
            continue
        }
        outputTp.AddSerializedParam(v, k, outputMetadata.Kind)
    }
    return outputTp
}

// No pattern queries in v1 workflow, at least for now
func (node *ResolvedNode) ResolveGlob(tp *TypedParams, glob GlobFunc) error {
    return nil
}

func (node *ResolvedNode) ParseCmd(tp TypedParams) ([]CmdTemplate, error) {
    var template CmdTemplate
    template.Version = 1
    template.NodeId = node.Id
    template.BaseCmd = []string{strings.Join(node.Launch.Command, " ")}
    template.Envs = node.Launch.Env
    template.ImageName = fmt.Sprintf("%s:%s", node.ImageName, node.ImageTag)
    template.ResourceReqs = ResourceVector{
        MemMb: node.Resources.MemMb,
        Cpus:  node.Resources.Cores,
        Gpus:  node.Resources.Gpus,
    }

    template.OverrideFsVolumes = true
    template.InFiles = make(map[string][]string)
    template.VolumeDirs = make(map[string]string)
    template.VolumeFiles = make(map[string]string)
    for inputName, input := range node.Inputs {
        // Param comes from another node, should be in inputs.
        if input.Source.NodeId != nil {
            val, exists := tp.Strings[inputName]
            if !exists {
                return nil, fmt.Errorf(
                    "required input param %s from node %d not present in inputs",
                    inputName, *input.Source.NodeId,
                )
            }
            if input.Kind == "file" {
                template.VolumeFiles[val] = val
            } else if input.Kind == "directory" {
                template.VolumeDirs[val] = val
            }
        } else {
            if node.ArgIsInputFile(inputName) {
                template.InFiles[inputName] = []string{input.Source.Path}
            }
            if input.Mount != nil {
                mnt := *input.Mount
                if input.Kind == "file" {
                    template.VolumeFiles[mnt.ContainerPath] = input.Source.Path
                } else if input.Kind == "directory" {
                    template.VolumeDirs[mnt.ContainerPath] = input.Source.Path
                }
            }
        }
    }
    template.OutFiles = make(map[string][]string)
    for outputName, output := range node.Outputs {
        if node.ArgIsOutputFile(outputName) {
            template.OutFiles[outputName] = []string{output.Path}
        }
        if output.Kind == "file" {
            template.VolumeFiles[output.Path] = output.Path
        } else if output.Kind == "directory" {
            template.VolumeDirs[output.Path] = output.Path
        }
    }
    template.OutFilePnames = make([]string, 0)
    return []CmdTemplate{template}, nil
}

func (link *ResolvedLink) GetSrcId() int {
    return link.Source
}

func (link *ResolvedLink) LinkType() string {
    return "v1"
}

func (link *ResolvedLink) GetSinkId() int {
    return link.Sink
}

func (link *ResolvedLink) GetSrcPname() string {
    return link.SourceOutput
}

func (link *ResolvedLink) GetSinkPname() string {
    return link.SinkInput
}

func (wf *ResolvedWorkflow) GetNumNodes() int {
    return len(wf.Nodes)
}

func (wf *ResolvedWorkflow) GetVersion() string {
    return "biodepot.resolved_workflow/v1"
}

func (wf *ResolvedWorkflow) GetNodes() map[int]WorkflowNode {
    nodeIds := make(map[int]WorkflowNode)
    for id, node := range wf.Nodes {
        nodeIds[id] = &node
    }
    return nodeIds
}

func (wf *ResolvedWorkflow) GetNodeIds() []int {
    nodeIds := make([]int, 0)
    for id := range wf.Nodes {
        nodeIds = append(nodeIds, id)
    }
    return nodeIds
}

func (wf *ResolvedWorkflow) GetArgType(nodeId int, pname string) (string, error) {
    node, nodeExists := wf.Nodes[nodeId]
    if !nodeExists {
        return "", fmt.Errorf("node %d does not exist", nodeId)
    }
    input, exists := node.Inputs[pname]
    if !exists {
        output, exists := node.Outputs[pname]
        if !exists {
            return "", fmt.Errorf("node %d, argtype %s does not exist", nodeId, pname)
        }
        return output.Kind, nil
    }
    return input.Kind, nil
}

func (wf *ResolvedWorkflow) GetParam(nodeId int, pname string) (any, error) {
    node, nodeExists := wf.Nodes[nodeId]
    if !nodeExists {
        return "", fmt.Errorf("node %d does not exist", nodeId)
    }
    val, valExists := node.Inputs[pname]
    if !valExists {
        return "", fmt.Errorf("node %d has no value of property %s", nodeId, pname)
    }
    return val, nil
}

func (wf *ResolvedWorkflow) SetParam(nodeId int, pname string, val any) error {
    node, nodeExists := wf.Nodes[nodeId]
    if !nodeExists {
        return fmt.Errorf("node %d does not exist", nodeId)
    }
    input := node.Inputs[pname]
    s, ok := val.(string)
    if !ok {
        return fmt.Errorf(
            "cannot set node %d, property %s to %v: "+
                "resolved workflow only accepts string-type "+
                "arguments (file paths)", nodeId, pname, val,
        )
    }
    input.Source.Path = s
    node.Inputs[pname] = input
    wf.Nodes[nodeId] = node
    return nil
}

func (wf *ResolvedWorkflow) GetLinks() []WorkflowLink {
    links := make([]WorkflowLink, 0)
    for _, link := range wf.Links {
        links = append(links, &link)
    }
    return links
}

func (wf *ResolvedWorkflow) GetNode(id int) (WorkflowNode, bool) {
    node, exists := wf.Nodes[id]
    return &node, exists
}

func (wf *ResolvedWorkflow) NodeExists(nodeId int) bool {
    _, exists := wf.Nodes[nodeId]
    return exists
}

func (wf *ResolvedWorkflow) GetBaseParams() (map[int]TypedParams, error) {
    ret := make(map[int]TypedParams)
    for nodeId, node := range wf.Nodes {
        nodeTp := TypedParams{}
        for inputName, inputMetadata := range node.Inputs {
            // If input comes from another node, then it's not a "base"
            // param, i.e. it is dynamic.
            if inputMetadata.Source.NodeId != nil {
                err := nodeTp.AddParam(
                    inputMetadata.Source.Path, inputName, inputMetadata.Kind,
                )
                if err != nil {
                    return nil, fmt.Errorf(
                        "error parsing params of node %d: %s",
                        nodeId, err,
                    )
                }
            }
        }
        for outputName, outputMetadata := range node.Outputs {
            err := nodeTp.AddParam(
                outputMetadata.Path, outputName, outputMetadata.Kind,
            )
            if err != nil {
                return nil, fmt.Errorf(
                    "error parsing params of node %d: %s",
                    nodeId, err,
                )
            }
        }
        ret[nodeId] = nodeTp
    }
    return ret, nil
}

func (wf *ResolvedWorkflow) DryRun() ([]string, error) {
    var cmdStrs []string
    topSort, err := topSort(wf)
    if err != nil {
        return nil, fmt.Errorf("failed top sort: %s", err)
    }

    for _, nodeId := range topSort {
        node := wf.Nodes[nodeId]
        cmdStrs = append(cmdStrs, strings.Join(node.Launch.Command, " "))
    }
    return cmdStrs, nil
}
