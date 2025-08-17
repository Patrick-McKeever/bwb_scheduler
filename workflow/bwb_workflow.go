package workflow

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"go-scheduler/parsing"
    "go-scheduler/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
    "errors"
    "log/slog"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

const (
    SCHEDULER_QUEUE = "bwb_worker"
)

type CmdOutput struct {
    Id          int
    StdOut      string
    StdErr      string
    RawOutputs  map[string]string
    OutputFiles []string
}

type WorkflowState interface {
    Setup() error
    Select()
    Shutdown()
    IsFinished() bool
    AddError(error)
    GetErrors() []error
    SetFinished()
    RequestResourceGrant([]parsing.CmdTemplate)
    ReleaseResourceGrant(ResourceGrant) error
    BuildImages([]string) error
    SetupWorkerFSs() error
    RunCmdWithGrant(parsing.CmdTemplate, ResourceGrant)
    Glob(string, string, bool, bool) ([]string, error)
}

type TemporalBwbWorkflowState struct {
    ctx         workflow.Context
    cmdMan      *parsing.CmdManager
    masterFS    fs.LocalFS
    workers     map[string]WorkerInfo
    softFail    bool
    storageId   string
    selector    workflow.Selector
    schedulerWE workflow.Execution
    cancelChild func()
    workerFSs   map[string]fs.LocalFS
    cmdsById    map[int]parsing.CmdTemplate
    grantsById  map[int]ResourceGrant
    errors      []error
    finished    *bool
}

func NewTemporalBwbWorkflowState(
    ctx workflow.Context, cmdMan *parsing.CmdManager, 
    masterFS fs.LocalFS, workers map[string]WorkerInfo, 
    storageId string, softFail bool,
) TemporalBwbWorkflowState {
    var state TemporalBwbWorkflowState
    lValForFalse := false
    state.finished = &lValForFalse
    state.ctx = ctx
    state.cmdMan = cmdMan
    state.masterFS = masterFS
    state.workers = workers
    state.storageId = storageId
    state.softFail = softFail
    state.workerFSs = make(map[string]fs.LocalFS)
    state.cmdsById = make(map[int]parsing.CmdTemplate)
    state.grantsById = make(map[int]ResourceGrant)
    state.errors = make([]error, 0)
    return state
}

func (state *TemporalBwbWorkflowState) Setup() error {
    childCtx, cancelChild := workflow.WithCancel(state.ctx)
    schedChildWfOptions := workflow.ChildWorkflowOptions{
        WorkflowID: "sched-workflow",
        TaskQueue: SCHEDULER_QUEUE,
    }
    childCtx = workflow.WithChildOptions(childCtx, schedChildWfOptions)
    schedChildWfFuture := workflow.ExecuteChildWorkflow(
        childCtx, ResourceSchedulerWorkflow, SchedWorkflowState{
            Workers: state.workers,
        },
    )
    var schedChildWE workflow.Execution
    err := schedChildWfFuture.GetChildWorkflowExecution().Get(state.ctx, &schedChildWE)
    if err != nil {
        outErr := fmt.Errorf("failed getting child WF execution: %s", err)
        return outErr
    }
    state.cancelChild = cancelChild

    state.selector = workflow.NewSelector(state.ctx)
    rGrantChan := workflow.GetSignalChannel(state.ctx, "allocation-response")
    state.selector.AddReceive(rGrantChan, func(c workflow.ReceiveChannel, _ bool) {
        var grant ResourceGrant
        c.Receive(state.ctx, &grant)
        cmd := state.cmdsById[grant.RequestId]
        state.RunCmdWithGrant(cmd, grant)
    })
    return nil
}

func (state *TemporalBwbWorkflowState) Select() {
    state.selector.Select(state.ctx)
}

func (state *TemporalBwbWorkflowState) Shutdown() {
    state.cancelChild()
}

func (state *TemporalBwbWorkflowState) IsFinished() bool {
    return *state.finished
}

func (state *TemporalBwbWorkflowState) SetFinished() {
    *state.finished = true
}

func (state *TemporalBwbWorkflowState) AddError(err error) {
    state.errors = append(state.errors, err)
}

func (state *TemporalBwbWorkflowState) GetErrors() []error {
    return state.errors
}

func (state *TemporalBwbWorkflowState) RequestResourceGrant(
    cmds []parsing.CmdTemplate,
) {
    workflowId := workflow.GetInfo(state.ctx).WorkflowExecution.ID
    for _, cmd := range cmds {
        state.cmdsById[cmd.Id] = cmd
        req := ResourceRequest{
            Rank: 0,
            Id: cmd.Id,
            Requirements: cmd.ResourceReqs,
            CallerWorkflowId: workflowId,
        }

        workflow.SignalExternalWorkflow(
            state.ctx, state.schedulerWE.ID, state.schedulerWE.RunID,
            "new-request", req,
        )
    }
}

func (state *TemporalBwbWorkflowState) ReleaseResourceGrant(
    grant ResourceGrant,
) error {
    workflow.SignalExternalWorkflow(
        state.ctx, state.schedulerWE.ID, state.schedulerWE.RunID,
        "release-allocation", grant,
    )
    return nil
}

func (state *TemporalBwbWorkflowState) SetupWorkerFSs(
) (error) {
    for queueId := range state.workers {
        ao := workflow.ActivityOptions{
            TaskQueue: queueId,
            StartToCloseTimeout: 1*time.Minute,
            RetryPolicy: &temporal.RetryPolicy{
                MaximumAttempts: 1,
            },
        }
        cmdCtx := workflow.WithActivityOptions(state.ctx, ao)

        var volumes map[string]string
        err := workflow.ExecuteActivity(
            cmdCtx, SetupVolumes, state.storageId,
        ).Get(state.ctx, &volumes)

        if err != nil {
            return fmt.Errorf(
                "error setting up cmd dirs on worker %s: %s",
                queueId, err,
            )
        }

        state.workerFSs[queueId] = fs.LocalFS{ Volumes: volumes }
    }
    return nil
}

func (state *TemporalBwbWorkflowState) BuildImages(imageNames []string) error {
    ao := workflow.ActivityOptions{
        TaskQueue: "bwb_worker",
        StartToCloseTimeout: 10*time.Minute,
        RetryPolicy: &temporal.RetryPolicy{
            MaximumAttempts: 1,
        },
    }
    cmdCtx := workflow.WithActivityOptions(state.ctx, ao)
    for _, imageName := range imageNames {
        err := workflow.ExecuteActivity(
            cmdCtx, BuildSingularitySIF, imageName,
        ).Get(state.ctx, nil)
        if err != nil {
            return err
        }
    }
    return nil
}

func (state *TemporalBwbWorkflowState) RunCmdWithGrant(
    cmd parsing.CmdTemplate, grant ResourceGrant,
) {
    ao := workflow.ActivityOptions{
        TaskQueue: grant.WorkerId,
        StartToCloseTimeout: 3*time.Hour,
        RetryPolicy: &temporal.RetryPolicy{
            MaximumAttempts: 1,
        },
    }

    fs, ok := state.workerFSs[grant.WorkerId]
    if !ok {
        state.errors = append(state.errors, fmt.Errorf(
            "worker %s has no FS", grant.WorkerId,
        ))
        *state.finished = true
        return
    }
    volumes := fs.GetVolumes()

    cmdCtx := workflow.WithActivityOptions(state.ctx, ao)
    cmdFuture := workflow.ExecuteActivity(
        cmdCtx, RunCmd, volumes, cmd,
    )
    state.selector.AddFuture(cmdFuture, func(f workflow.Future) {
        var result CmdOutput
        err := f.Get(state.ctx, &result)
        HandleCompletedCmd(result, err, state.softFail, state.cmdMan, state, cmd, grant)
    })

}

func (state *TemporalBwbWorkflowState) Glob(
    root, pattern string, 
    findFile, findDir bool,
) ([]string, error) {
    ao := workflow.ActivityOptions{
        TaskQueue: "bwb_worker",
        StartToCloseTimeout: 1*time.Minute,
        RetryPolicy: &temporal.RetryPolicy{
            MaximumAttempts: 3,
        },
    }
    cmdCtx := workflow.WithActivityOptions(state.ctx, ao)
    var out []string
    err := workflow.ExecuteActivity(
        cmdCtx, fs.GlobActivity[fs.LocalFS], state.masterFS, 
        root,  pattern, findFile, findDir,
    ).Get(state.ctx, &out)
    return out, err
}

type NonTemporalBwbWorkflowState struct {
    cmdMan              *parsing.CmdManager
    masterFS            fs.LocalFS
    localWorker         WorkerInfo
    storageId           string
    softFail            bool
    logger              slog.Logger
    reqGrantChan        chan ResourceRequest
    releaseGrantChan    chan ResourceGrant
    recvGrantChan       chan ResourceGrant
    doneChan            chan bool
    cmdResChan          chan struct{result CmdOutput; err error}
    cmdsById            map[int]parsing.CmdTemplate
    grantsById          map[int]ResourceGrant
    errors              []error
    finished            bool
}

func NewNonTemporalBwbWorkflowState(
    cmdMan *parsing.CmdManager, masterFS fs.LocalFS,
    worker WorkerInfo, storageId string,
    softFail bool, logger slog.Logger,
) NonTemporalBwbWorkflowState {
    var state NonTemporalBwbWorkflowState
    state.finished = false
    state.cmdMan = cmdMan
    state.masterFS = masterFS
    state.localWorker = worker
    state.storageId = storageId
    state.softFail = softFail
    state.logger = logger
    state.grantsById = make(map[int]ResourceGrant)
    state.cmdsById = make(map[int]parsing.CmdTemplate)
    state.grantsById = make(map[int]ResourceGrant)
    state.errors = make([]error, 0)
    state.reqGrantChan = make(chan ResourceRequest)
    state.releaseGrantChan = make(chan ResourceGrant)
    state.recvGrantChan = make(chan ResourceGrant)
    state.doneChan = make(chan bool)
    state.cmdResChan = make(chan struct{result CmdOutput; err error})
    return state
}

func (state *NonTemporalBwbWorkflowState) Setup() error {
    workers := map[string]WorkerInfo{"local": state.localWorker}
    go LocalResourceScheduler(
        state.logger, workers, state.reqGrantChan, 
        state.releaseGrantChan, state.recvGrantChan, 
        state.doneChan,
    )
    return nil
}

func (state *NonTemporalBwbWorkflowState) Select() {
    select { 
    case grant := <-state.recvGrantChan: {
        state.grantsById[grant.RequestId] = grant
        cmd := state.cmdsById[grant.RequestId]
        state.RunCmdWithGrant(cmd, grant)
    }

    case res := <- state.cmdResChan: {
        cmd, cmdOk := state.cmdsById[res.result.Id]
        grant, grantOk := state.grantsById[res.result.Id]
        if !cmdOk || !grantOk {
            state.logger.Warn(fmt.Sprintf(
                "Received result for non-existent CMD ID %d", res.result.Id,
            ))
        }

        // This absolutely needs to be done in a "select" of the main
        // thread rather than in the finished command's goroutine, since
        // none of these data-structures have been made thread-safe.
        HandleCompletedCmd(
            res.result, res.err, state.softFail, 
            state.cmdMan, state, cmd, grant,
        )
    }
    }
}

func (state *NonTemporalBwbWorkflowState) Shutdown() {
    state.doneChan <- true
}

func (state *NonTemporalBwbWorkflowState) IsFinished() bool {
    return state.finished
}

func (state *NonTemporalBwbWorkflowState) SetFinished() {
    state.finished = true
}

func (state *NonTemporalBwbWorkflowState) AddError(err error) {
    state.errors = append(state.errors, err)
}

func (state *NonTemporalBwbWorkflowState) GetErrors() []error {
    return state.errors
}

func (state *NonTemporalBwbWorkflowState) RequestResourceGrant(
    cmds []parsing.CmdTemplate,
) {
    for _, cmd := range cmds {
        state.logger.Warn("requesting cmd")
        state.cmdsById[cmd.Id] = cmd
        req := ResourceRequest{
            Rank: 0,
            Id: cmd.Id,
            Requirements: cmd.ResourceReqs,
        }

        state.reqGrantChan <- req
    }
}

func (state *NonTemporalBwbWorkflowState) ReleaseResourceGrant(
    grant ResourceGrant,
) error {
    state.releaseGrantChan <- grant
    return nil
}

func (state *NonTemporalBwbWorkflowState) BuildImages(imageNames []string) error {
    for _, imageName := range imageNames {
        state.logger.Info(fmt.Sprintf("Building image %s\n", imageName))
        if err := BuildSingularitySIF(imageName); err != nil {
            return err
        }
    }
    return nil
}

func (state *NonTemporalBwbWorkflowState) SetupWorkerFSs() error {
    volumes, err := SetupVolumes(state.storageId)
    if err != nil {
        return err
    }
    state.masterFS.Volumes = volumes
    return nil
}

func (state *NonTemporalBwbWorkflowState) RunCmdWithGrant(
    cmd parsing.CmdTemplate, grant ResourceGrant,
) {
    volumes := state.masterFS.GetVolumes()

    go func() {
        result, err := RunCmd(volumes, cmd)
        state.cmdResChan <- struct{result CmdOutput; err error}{
            result: result,
            err: err,
        }
    }()
}

func (state *NonTemporalBwbWorkflowState) Glob(
    root, pattern string, 
    findFile, findDir bool,
) ([]string, error) {
    return fs.GlobActivity(
        state.masterFS, root, pattern, findFile, findDir,
    )
}

func getSifName(dockerImage string) string {
    // Replace all `/` in docker image name, since this is going
    // to be a filename
    imgBasename := strings.Replace(dockerImage, "/", ".", -1)
    return fmt.Sprintf("%s.sif", imgBasename)
}

func randomString(length int) string {
    b := make([]byte, length+2)
    rand.Read(b)
    return fmt.Sprintf("%x", b)[2 : length+2]
}

func BuildSingularitySIF(dockerImage string) error {
	if _, err := exec.LookPath("singularity"); err != nil {
		return fmt.Errorf("singularity not found in PATH: %v", err)
	}

    dataDir := os.Getenv("BWB_SCHED_DIR")
    imageDir := filepath.Join(dataDir, "images")
	if err := os.MkdirAll(imageDir, 0755); err != nil {
		return fmt.Errorf(
            "failed to create output directory %s: %v", 
            imageDir, err,
        )
	}

    sifBasename := getSifName(dockerImage)
    outputPath := filepath.Join(imageDir, sifBasename)

    // Do not rebuild existing image.
    if _, err := os.Stat(outputPath); err == nil {
        return nil
    }

	cmd := exec.Command(
		"singularity", 
		"build",
		outputPath,
		"docker://"+dockerImage,
	)

    var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return fmt.Errorf(
            "failed to build singularity image %s: %v\nSTDOUT: %s\nSTDERR: %s", 
            dockerImage, err, stdout.String(), stderr.String(),
        )
	}

	return nil
}

func getCmdOutputs(
    tmpOutputHostPath string, 
    expOutFilePnames []string, 
) (map[string]string, []string, error) {
    if _, err := os.Stat(tmpOutputHostPath); os.IsNotExist(err) {
        return nil, nil, fmt.Errorf(
            "/tmp/output host path %s does not exist", tmpOutputHostPath,
        )
    }

	outPaths, err := os.ReadDir(tmpOutputHostPath)
	if err != nil {
		return nil, nil, fmt.Errorf(
            "error reading output dir %s: %s", tmpOutputHostPath, err,
        )
	}

    outKvs := make(map[string]string)
	for _, outPath := range outPaths {
        pname := outPath.Name()
		if outPath.IsDir() {
			continue
		}

		fullPath := filepath.Join(tmpOutputHostPath, pname)
		data, err := os.ReadFile(fullPath)
		if err != nil {
			fmt.Printf("failed to read %s: %v\n", fullPath, err)
			continue
		}

        outKvs[pname] = string(data)
	}

    outFiles := make([]string, 0)
    for _, expOutFilePname := range expOutFilePnames {
        if outFilesRaw, outFileExists := outKvs[expOutFilePname]; outFileExists {
            outFileVals := strings.Split(outFilesRaw, "\n")
            if len(outFileVals) > 0 {
                if outFileVals[len(outFileVals)-1] == "" {
                    outFileVals = outFileVals[:len(outFileVals)-1]
                }
                outFiles = append(outFiles, outFileVals...)
            }
        }
    }
    
    return outKvs, outFiles, nil
}

func setupCmdDirs() (string, string, error) {
    schedDir := os.Getenv("BWB_SCHED_DIR")
    randStr := randomString(32)
    tmpDir := filepath.Join(schedDir, randStr)
    imageDir := filepath.Join(schedDir, "images")
    
    for _, dir := range []string{tmpDir, imageDir} {
	    if err := os.MkdirAll(dir, 0755); err != nil {
	    	return "", "", fmt.Errorf("failed to create dir %s: %s", dir, err)
	    }
    }

    return tmpDir, imageDir, nil
}

func SetupVolumes(storageId string) (map[string]string, error) {
    schedDir := os.Getenv("BWB_SCHED_DIR")
    dataDir := filepath.Join(schedDir, storageId)

	if err := os.MkdirAll(dataDir, 0755); err != nil {
	    return nil, fmt.Errorf("failed to create dir %s: %s", dataDir, err)
	}

    return map[string]string{"/data": dataDir}, nil
}

func RunCmd(
    volumes map[string]string, cmdTemplate parsing.CmdTemplate, 
) (CmdOutput, error) {
	if _, err := exec.LookPath("singularity"); err != nil {
		return CmdOutput{}, fmt.Errorf("singularity not found in PATH: %v", err)
	}

    tmpDir, imageDir, err := setupCmdDirs()
    if err != nil {
        return CmdOutput{}, fmt.Errorf("error making tmp dir: %s", err)
    }
    volumes["/tmp/output"] = tmpDir
    defer os.RemoveAll(tmpDir)

    sifBasename := getSifName(cmdTemplate.ImageName)
    localSifPath := filepath.Join(imageDir, sifBasename)

    if _, err := os.Stat(localSifPath); os.IsNotExist(err) {
        return CmdOutput{}, fmt.Errorf(
            "SIF image %s not found at expected path %s",
            cmdTemplate.ImageName, localSifPath,
        )
    }

    useGpu := cmdTemplate.ResourceReqs.Gpus > 0
    cmdStr, envs := parsing.FormSingularityCmd(
        cmdTemplate, volumes, localSifPath, useGpu,
    )

    var stdout, stderr bytes.Buffer
    cmd := exec.Command("sh", "-c", cmdStr)
    cmd.Env = envs
    cmd.Stdout = &stdout
    cmd.Stderr = &stderr

    cmdWithEnvStr := fmt.Sprintf("%s %s", strings.Join(envs, " "), cmdStr)
    fmt.Println(cmdWithEnvStr)

    if err = cmd.Run(); err != nil {
		return CmdOutput{}, fmt.Errorf(
            "error running command %s: %v\nSTDOUT: %s\nSTDERR: %s", 
            cmdWithEnvStr, err, stdout.String(), stderr.String(),
        )
    }

    out := CmdOutput {
        Id: cmdTemplate.Id,
        StdOut: stdout.String(),
        StdErr: stderr.String(),
    }
    out.RawOutputs, out.OutputFiles, err = getCmdOutputs(
        tmpDir, cmdTemplate.OutFilePnames,
    )
    if err != nil {
        return CmdOutput{}, fmt.Errorf(
            "error getting outputs of command %s: %s", cmd, err,
        )
    }

    return out, nil
}

func HandleCompletedCmd(
    result CmdOutput, err error, softFail bool,
    cmdMan *parsing.CmdManager, state WorkflowState,
    completedCmd parsing.CmdTemplate,
    grant ResourceGrant,
) {
    fmt.Printf("Completed cmd for node %d\n", completedCmd.NodeId)
    if err != nil {
        state.AddError(err)
        if !softFail {
            state.SetFinished()
            return
        }
    }

    succCmds, err := cmdMan.GetSuccCmds(
        completedCmd, result.RawOutputs, 
        func(root, pattern string, findFile, findDir bool) ([]string, error) {
            return state.Glob(root, pattern, findFile, findDir)
        }, err == nil,
    )

    if err != nil {
        state.SetFinished()
        state.AddError(err)
        return
    }

    if err := state.ReleaseResourceGrant(grant); err != nil {
        state.AddError(err)
        return
    }

    state.RequestResourceGrant(succCmds)

    complete := cmdMan.IsComplete()
    if complete {
        state.SetFinished()
    }
}

func RunBwbWorkflow(
    cmdMan *parsing.CmdManager, state WorkflowState,
) error {
    if cmdMan == nil {
        return errors.New("received nil CMD manager")
    }

    imageNames := cmdMan.GetImageNames()
    if err := state.BuildImages(imageNames); err != nil {
        return err
    }

    if err := state.SetupWorkerFSs(); err != nil {
        return err
    }

    if err := state.Setup(); err != nil {
        return err
    }

    initialCmds, err := cmdMan.GetInitialCmds(
        func(root, pattern string, findFile, findDir bool) ([]string, error) {
            return state.Glob(root, pattern, findFile, findDir)
        },
    )

    if err != nil {
        return fmt.Errorf("error getting initial cmds: %s", err)
    }

    state.RequestResourceGrant(initialCmds)
    for ! state.IsFinished() {
        state.Select()
    }
    state.Shutdown()

    errs := state.GetErrors()
    if len(errs) > 0 {
        errStr := "Workflow encountered multiple errors:\n"
        for _, err := range errs {
            errStr += fmt.Sprintf("\t%s\n", err.Error())
        }
        return errors.New(errStr)
    }

    return nil
}


func RunBwbWorkflowTemporal(
    ctx workflow.Context, 
    storageId string,
    bwbWorkflow parsing.Workflow, 
    index parsing.WorkflowIndex,
    workers map[string]WorkerInfo,
    masterFS fs.LocalFS,
    softFail bool,
) (error) {
    cmdMan := parsing.NewCmdManager(bwbWorkflow, index)
    wfState := NewTemporalBwbWorkflowState(
        ctx, &cmdMan, masterFS, workers, storageId, softFail,
    )
    return RunBwbWorkflow(&cmdMan, &wfState)
}

func RunBwbWorkflowNoTemporal(
    storageId string,
    bwbWorkflow parsing.Workflow, 
    index parsing.WorkflowIndex,
    localWorker WorkerInfo,
    masterFS fs.LocalFS,
    softFail bool,
    logger slog.Logger,
) (error) {
    cmdMan := parsing.NewCmdManager(bwbWorkflow, index)
    wfState := NewNonTemporalBwbWorkflowState(
        &cmdMan, masterFS, localWorker, storageId, softFail, logger,
    )
    return RunBwbWorkflow(&cmdMan, &wfState)
}