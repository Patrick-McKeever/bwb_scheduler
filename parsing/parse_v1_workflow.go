package parsing

type ResolvedWorkflowEnvelope struct {
    Schema           string           `json:"schema"`            // must equal "biodepot.resolved_workflow/v1"
    ResolvedWorkflow ResolvedWorkflow `json:"resolved_workflow"`
}

type ResolvedWorkflow struct {
    RunId           string         `json:"run_id"`
    UseLocalStorage bool           `json:"use_local_storage,omitempty"`
    Nodes           []ResolvedNode `json:"nodes"`
    Links           []ResolvedLink `json:"links"`
    Conditions      []any          `json:"conditions,omitempty"`
}

type ResolvedNode struct {
    Id                int                 `json:"id"`
    Title             string              `json:"title"`
    Description       string              `json:"description,omitempty"`
    ImageName         string              `json:"image_name"`
    Launch            Launch              `json:"launch"`
    Inputs            []NodeInput         `json:"inputs,omitempty"`
    Outputs           []NodeOutput        `json:"outputs,omitempty"`
    Resources         Resources           `json:"resources"`
    SchedulerControls *SchedulerControls  `json:"scheduler_controls,omitempty"`
    SchedulerHints    map[string]any      `json:"scheduler_hints,omitempty"`
    Staging           *Staging            `json:"staging,omitempty"`
    Async             bool                `json:"async,omitempty"`
    BarrierFor        *int                `json:"barrier_for,omitempty"` // null in JSON → nil
}

type Launch struct {
    Command []string          `json:"command"`
    Env     map[string]string `json:"env,omitempty"`
    Cwd     string            `json:"cwd,omitempty"`
    Shell   bool              `json:"shell,omitempty"`
}

type NodeInput struct {
    Name   string       `json:"name"`
    Kind   string       `json:"kind"`     // file | directory | file_list | directory_list
    Source InputSource  `json:"source"`
    Mount  *InputMount  `json:"mount,omitempty"`
}

type InputSource struct {
    Type    string `json:"type"`              // path | node_output | pattern_query
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

type SchedulerControls struct {
    UseScheduler  bool            `json:"useScheduler,omitempty"`
    UseGpu        bool            `json:"useGpu,omitempty"`
    Iterate       bool            `json:"iterate,omitempty"`
    NWorkers      int             `json:"nWorkers,omitempty"`
    Slots         int             `json:"slots,omitempty"`
    IterAttrs     []string        `json:"iterAttrs,omitempty"`
    IterGroupSize map[string]int  `json:"iterGroupSize,omitempty"`
}

type Staging struct {
    Mode string `json:"mode"` // shared_fs | rsync | object_store
}

type ResolvedLink struct {
    Source       int    `json:"source"`
    Sink         int    `json:"sink"`
    SourceOutput string `json:"source_output"`
    SinkInput    string `json:"sink_input"`
    ConditionRef string `json:"condition_ref,omitempty"`
}

