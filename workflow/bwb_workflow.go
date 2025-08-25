package workflow

import (
    "bytes"
    "crypto/rand"
    "errors"
    "fmt"
    "go-scheduler/fs"
    "go-scheduler/parsing"
    "log/slog"
    "os"
    "os/exec"
    "path/filepath"
    "strings"
    "time"

    "go.temporal.io/sdk/temporal"
    "go.temporal.io/sdk/workflow"
    "go.temporal.io/sdk/log"
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

type Executor interface {
    Setup() error
    SetCmdHandler(CmdHandler)
    Select()
    Shutdown()
    GetErrors() []error
    RunCmds([]parsing.CmdTemplate)
    BuildImages([]string) error
    Glob(string, string, bool, bool) ([]string, error)
}

type CmdHandler func(CmdOutput, error, Executor, parsing.CmdTemplate)
type TemporalExecutor struct {
    ctx               workflow.Context
    handleFinishedCmd CmdHandler
    masterFS          fs.LocalFS
    workers           map[string]WorkerInfo
    storageId         string
    selector          workflow.Selector
    schedulerWE       workflow.Execution
    cancelChild       func()
    workerFSs         map[string]fs.LocalFS
    cmdsById          map[int]parsing.CmdTemplate
    grantsById        map[int]ResourceGrant
    errors            []error
}

func NewTemporalExecutor(
    ctx workflow.Context, cmdMan *parsing.CmdManager,
    masterFS fs.LocalFS, workers map[string]WorkerInfo,
    storageId string,
) TemporalExecutor {
    var state TemporalExecutor
    state.ctx = ctx
    state.masterFS = masterFS
    state.workers = workers
    state.storageId = storageId
    state.workerFSs = make(map[string]fs.LocalFS)
    state.cmdsById = make(map[int]parsing.CmdTemplate)
    state.grantsById = make(map[int]ResourceGrant)
    state.errors = make([]error, 0)
    return state
}

func (exec *TemporalExecutor) Setup() error {
    childCtx, cancelChild := workflow.WithCancel(exec.ctx)
    schedChildWfOptions := workflow.ChildWorkflowOptions{
        WorkflowID: "sched-workflow",
        TaskQueue:  SCHEDULER_QUEUE,
    }
    childCtx = workflow.WithChildOptions(childCtx, schedChildWfOptions)
    schedChildWfFuture := workflow.ExecuteChildWorkflow(
        childCtx, ResourceSchedulerWorkflow, SchedWorkflowState{
            Workers: exec.workers,
        },
    )
    err := schedChildWfFuture.GetChildWorkflowExecution().Get(exec.ctx, &exec.schedulerWE)
    if err != nil {
        outErr := fmt.Errorf("failed getting child WF execution: %s", err)
        return outErr
    }
    exec.cancelChild = cancelChild

    exec.selector = workflow.NewSelector(exec.ctx)
    rGrantChan := workflow.GetSignalChannel(exec.ctx, "allocation-response")
    exec.selector.AddReceive(rGrantChan, func(c workflow.ReceiveChannel, _ bool) {
        var grant ResourceGrant
        c.Receive(exec.ctx, &grant)
        cmd := exec.cmdsById[grant.RequestId]
        exec.RunCmdWithGrant(cmd, grant)
    })

    // Setup worker FSs.
    for queueId := range exec.workers {
        ao := workflow.ActivityOptions{
            TaskQueue:           queueId,
            StartToCloseTimeout: 1 * time.Minute,
            RetryPolicy: &temporal.RetryPolicy{
                MaximumAttempts: 1,
            },
        }
        cmdCtx := workflow.WithActivityOptions(exec.ctx, ao)

        var volumes map[string]string
        err := workflow.ExecuteActivity(
            cmdCtx, fs.SetupVolumes, exec.storageId,
        ).Get(exec.ctx, &volumes)

        if err != nil {
            return fmt.Errorf(
                "error setting up cmd dirs on worker %s: %s",
                queueId, err,
            )
        }

        exec.workerFSs[queueId] = fs.LocalFS{Volumes: volumes}
    }
    return nil
}

func (exec *TemporalExecutor) SetCmdHandler(handler CmdHandler) {
    exec.handleFinishedCmd = handler
}

func (exec *TemporalExecutor) Select() {
    exec.selector.Select(exec.ctx)
}

func (exec *TemporalExecutor) Shutdown() {
    exec.cancelChild()
}

func (exec *TemporalExecutor) GetErrors() []error {
    return exec.errors
}

func (exec *TemporalExecutor) RunCmds(
    cmds []parsing.CmdTemplate,
) {
    workflowId := workflow.GetInfo(exec.ctx).WorkflowExecution.ID
    for _, cmd := range cmds {
        exec.cmdsById[cmd.Id] = cmd
        req := ResourceRequest{
            Rank:             0,
            Id:               cmd.Id,
            Requirements:     cmd.ResourceReqs,
            CallerWorkflowId: workflowId,
        }

        workflow.SignalExternalWorkflow(
            exec.ctx, exec.schedulerWE.ID, exec.schedulerWE.RunID,
            "new-request", req,
        )
    }
}

func (exec *TemporalExecutor) ReleaseResourceGrant(
    grant ResourceGrant,
) error {
    workflow.SignalExternalWorkflow(
        exec.ctx, exec.schedulerWE.ID, exec.schedulerWE.RunID,
        "release-allocation", grant,
    )
    return nil
}

func (exec *TemporalExecutor) BuildImages(imageNames []string) error {
    ao := workflow.ActivityOptions{
        TaskQueue:           "bwb_worker",
        StartToCloseTimeout: 10 * time.Minute,
        RetryPolicy: &temporal.RetryPolicy{
            MaximumAttempts: 1,
        },
    }
    cmdCtx := workflow.WithActivityOptions(exec.ctx, ao)
    for _, imageName := range imageNames {
        err := workflow.ExecuteActivity(
            cmdCtx, BuildSingularitySIF, imageName,
        ).Get(exec.ctx, nil)
        if err != nil {
            return err
        }
    }
    return nil
}

func (exec *TemporalExecutor) RunCmdWithGrant(
    cmd parsing.CmdTemplate, grant ResourceGrant,
) {
    ao := workflow.ActivityOptions{
        TaskQueue:           grant.WorkerId,
        StartToCloseTimeout: 3 * time.Hour,
        RetryPolicy: &temporal.RetryPolicy{
            MaximumAttempts: 1,
        },
    }

    fs, ok := exec.workerFSs[grant.WorkerId]
    if !ok {
        exec.errors = append(exec.errors, fmt.Errorf(
            "worker %s has no FS", grant.WorkerId,
        ))
        return
    }
    volumes := fs.GetVolumes()

    cmdCtx := workflow.WithActivityOptions(exec.ctx, ao)
    cmdFuture := workflow.ExecuteActivity(
        cmdCtx, RunCmd, volumes, cmd,
    )
    exec.selector.AddFuture(cmdFuture, func(f workflow.Future) {
        var result CmdOutput
        err := f.Get(exec.ctx, &result)
        if err := exec.ReleaseResourceGrant(grant); err != nil {
            exec.errors = append(exec.errors, err)
            return
        }
        exec.handleFinishedCmd(result, err, exec, cmd)
    })

}

func (exec *TemporalExecutor) Glob(
    root, pattern string,
    findFile, findDir bool,
) ([]string, error) {
    ao := workflow.ActivityOptions{
        TaskQueue:           "bwb_worker",
        StartToCloseTimeout: 1 * time.Minute,
        RetryPolicy: &temporal.RetryPolicy{
            MaximumAttempts: 3,
        },
    }
    cmdCtx := workflow.WithActivityOptions(exec.ctx, ao)
    var out []string
    err := workflow.ExecuteActivity(
        cmdCtx, fs.GlobActivity[fs.LocalFS], exec.masterFS,
        root, pattern, findFile, findDir,
    ).Get(exec.ctx, &out)
    return out, err
}

type LocalExecutor struct {
    handleFinishedCmd CmdHandler
    masterFS          fs.LocalFS
    localWorker       WorkerInfo
    storageId         string
    logger            slog.Logger
    reqGrantChan      chan ResourceRequest
    releaseGrantChan  chan ResourceGrant
    recvGrantChan     chan ResourceGrant
    doneChan          chan bool
    cmdResChan        chan struct {
        result CmdOutput
        err    error
    }
    cmdsById   map[int]parsing.CmdTemplate
    grantsById map[int]ResourceGrant
    errors     []error
}

func NewLocalExecutor(
    cmdMan *parsing.CmdManager, masterFS fs.LocalFS,
    worker WorkerInfo, storageId string,
    logger slog.Logger,
) LocalExecutor {
    var state LocalExecutor
    state.masterFS = masterFS
    state.localWorker = worker
    state.storageId = storageId
    state.logger = logger
    state.grantsById = make(map[int]ResourceGrant)
    state.cmdsById = make(map[int]parsing.CmdTemplate)
    state.grantsById = make(map[int]ResourceGrant)
    state.errors = make([]error, 0)
    state.reqGrantChan = make(chan ResourceRequest)
    state.releaseGrantChan = make(chan ResourceGrant)
    state.recvGrantChan = make(chan ResourceGrant)
    state.doneChan = make(chan bool)
    state.cmdResChan = make(chan struct {
        result CmdOutput
        err    error
    })
    return state
}

func (exec *LocalExecutor) Setup() error {
    workers := map[string]WorkerInfo{"local": exec.localWorker}
    go LocalResourceScheduler(
        exec.logger, workers, exec.reqGrantChan,
        exec.releaseGrantChan, exec.recvGrantChan,
        exec.doneChan,
    )

    // Setup worker FS.
    volumes, err := fs.SetupVolumes(exec.storageId)
    if err != nil {
        return err
    }
    exec.masterFS.Volumes = volumes
    return nil
}

func (exec *LocalExecutor) SetCmdHandler(handler CmdHandler) {
    exec.handleFinishedCmd = handler
}

func (exec *LocalExecutor) Select() {
    select {
    case grant := <-exec.recvGrantChan:
        {
            exec.grantsById[grant.RequestId] = grant
            cmd := exec.cmdsById[grant.RequestId]
            exec.RunCmdWithGrant(cmd, grant)
        }

    case res := <-exec.cmdResChan:
        {
            cmd, cmdOk := exec.cmdsById[res.result.Id]
            grant, grantOk := exec.grantsById[res.result.Id]
            if !cmdOk || !grantOk {
                exec.logger.Warn(fmt.Sprintf(
                    "Received result for non-existent CMD ID %d", res.result.Id,
                ))
            }

            if err := exec.ReleaseResourceGrant(grant); err != nil {
                exec.errors = append(exec.errors, err)
                return
            }

            // This absolutely needs to be done in a "select" of the main
            // thread rather than in the finished command's goroutine, since
            // none of these data-structures have been made thread-safe.
            exec.handleFinishedCmd(res.result, res.err, exec, cmd)
        }
    }
}

func (exec *LocalExecutor) Shutdown() {
    exec.doneChan <- true
}

func (exec *LocalExecutor) GetErrors() []error {
    return exec.errors
}

func (exec *LocalExecutor) RunCmds(
    cmds []parsing.CmdTemplate,
) {
    for _, cmd := range cmds {
        exec.logger.Warn("requesting cmd")
        exec.cmdsById[cmd.Id] = cmd
        req := ResourceRequest{
            Rank:         0,
            Id:           cmd.Id,
            Requirements: cmd.ResourceReqs,
        }

        exec.reqGrantChan <- req
    }
}

func (exec *LocalExecutor) ReleaseResourceGrant(
    grant ResourceGrant,
) error {
    exec.releaseGrantChan <- grant
    return nil
}

func (exec *LocalExecutor) BuildImages(imageNames []string) error {
    for _, imageName := range imageNames {
        exec.logger.Info(fmt.Sprintf("Building image %s\n", imageName))
        if _, err := BuildSingularitySIF(imageName); err != nil {
            return err
        }
    }
    return nil
}

func (exec *LocalExecutor) RunCmdWithGrant(
    cmd parsing.CmdTemplate, grant ResourceGrant,
) {
    volumes := exec.masterFS.GetVolumes()

    go func() {
        result, err := RunCmd(volumes, cmd)
        exec.cmdResChan <- struct {
            result CmdOutput
            err    error
        }{
            result: result,
            err:    err,
        }
    }()
}

func (exec *LocalExecutor) Glob(
    root, pattern string,
    findFile, findDir bool,
) ([]string, error) {
    return fs.GlobActivity(
        exec.masterFS, root, pattern, findFile, findDir,
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

func BuildSingularitySIF(dockerImage string) (string, error) {
    if _, err := exec.LookPath("singularity"); err != nil {
        return "", fmt.Errorf("singularity not found in PATH: %v", err)
    }

    dataDir := os.Getenv("BWB_SCHED_DIR")
    imageDir := filepath.Join(dataDir, "images")
    if err := os.MkdirAll(imageDir, 0755); err != nil {
        return "", fmt.Errorf(
            "failed to create output directory %s: %v",
            imageDir, err,
        )
    }

    sifBasename := getSifName(dockerImage)
    outputPath := filepath.Join(imageDir, sifBasename)

    // Do not rebuild existing image.
    if _, err := os.Stat(outputPath); err == nil {
        return outputPath, nil
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
        return "", fmt.Errorf(
            "failed to build singularity image %s: %v\nSTDOUT: %s\nSTDERR: %s",
            dockerImage, err, stdout.String(), stderr.String(),
        )
    }

    return outputPath, nil
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

        outKvs[pname] = strings.TrimSuffix(string(data), "\n")
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

    out := CmdOutput{
        Id:     cmdTemplate.Id,
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
    logger log.Logger, result CmdOutput, err error, softFail bool,
    cmdMan *parsing.CmdManager, executors map[int]Executor,
    completedCmd parsing.CmdTemplate, finalErr *error,
) {
    logger.Info("Finished cmd", "cmdId", completedCmd.Id, "nodeId", completedCmd.NodeId)
    cmdSucceeded := err == nil
    succCmds, err := cmdMan.GetSuccCmds(
        completedCmd, result.RawOutputs,
        func(nodeId int, root, pattern string, findFile, findDir bool) ([]string, error) {
            executor, ok := executors[nodeId]
            if !ok {
                return nil, fmt.Errorf("no executor for node %d", nodeId)
            }
            return executor.Glob(root, pattern, findFile, findDir)
        }, cmdSucceeded,
    )

    if err != nil {
        *finalErr = err
        return
    }

    logger.Debug("Got succ CMDs", "succCmds", succCmds)
    RunCmds(executors, succCmds)
}

func setupExecutors(
    ctx workflow.Context,
    storageId string,
    bwbWorkflow parsing.Workflow,
    cmdMan *parsing.CmdManager,
    workers map[string]WorkerInfo,
    masterFS fs.LocalFS,
    jobConfig parsing.JobConfig,
) (map[int]Executor, []Executor, error) {
    executors := make(map[int]Executor)
    executorList := make([]Executor, 0)
    if len(jobConfig.LocalConfigsByNode) > 0 {
        if len(jobConfig.SlurmConfigsByNode) > 0 || len(jobConfig.TemporalConfigsByNode) > 0 {
            return nil, nil, fmt.Errorf(
                "cannot have temporal / SLURM executor alongside local one",
            )
        }
    }

    if len(jobConfig.TemporalConfigsByNode) > 0 {
        temporalExecutor := NewTemporalExecutor(
            ctx, cmdMan, masterFS, workers, storageId,
        )
        executorList = append(executorList, &temporalExecutor)
        for nodeId := range jobConfig.TemporalConfigsByNode {
            executors[nodeId] = &temporalExecutor
        }
    }

    if len(jobConfig.SlurmConfigsByNode) > 0 {
        slurmExecutor := NewSlurmRemoteExecutor(
            ctx, cmdMan, masterFS, storageId, jobConfig.SlurmConfigsByNode,
            jobConfig.SlurmExecutor,
        )
        executorList = append(executorList, &slurmExecutor)
        for nodeId := range jobConfig.SlurmConfigsByNode {
            executors[nodeId] = &slurmExecutor
        }
    }

    for nodeId := range bwbWorkflow.Nodes {
        if _, execExists := executors[nodeId]; !execExists {
            return nil, nil, fmt.Errorf(
                "no executor set for node %d in config", nodeId,
            )
        }
    }

    return executors, executorList, nil
}

func RunCmds(executors map[int]Executor, cmdsByNode map[int][]parsing.CmdTemplate) error {
    for nodeId, cmdList := range cmdsByNode {
        executor, ok := executors[nodeId]
        if !ok {
            return fmt.Errorf("no executor for node ID %d", nodeId)
        }
        executor.RunCmds(cmdList)
    }
    return nil
}

func RunBwbWorkflowHelper(
    logger log.Logger,
    cmdMan *parsing.CmdManager, 
    executorsByNode map[int]Executor, 
    executors []Executor,
    softFail bool,
) error {
    if cmdMan == nil {
        return errors.New("received nil CMD manager")
    }

    var finalErr error = nil
    imageNames := cmdMan.GetImageNames()
    for _, executor := range executors {
        if err := executor.Setup(); err != nil {
            return err
        }

        if err := executor.BuildImages(imageNames); err != nil {
            return err
        }

        executor.SetCmdHandler(func(res CmdOutput, err error, exec Executor, cmd parsing.CmdTemplate) {
            HandleCompletedCmd(
                logger, res, err, softFail, cmdMan, executorsByNode, cmd, &finalErr,
            )
        })
    }

    initialCmds, err := cmdMan.GetInitialCmds(
        func(nodeId int, root, pattern string, findFile, findDir bool) ([]string, error) {
            executor, ok := executorsByNode[nodeId]
            if !ok {
                return nil, fmt.Errorf("no executor for node %d", nodeId)
            }
            return executor.Glob(root, pattern, findFile, findDir)
        },
    )

    if err != nil {
        return fmt.Errorf("error getting initial cmds: %s", err)
    }

    RunCmds(executorsByNode, initialCmds)
    for !cmdMan.IsComplete() && (!cmdMan.HasFailed() || softFail) && finalErr == nil {
        for _, executor := range executors {
            execErrs := executor.GetErrors()
            if len(execErrs) > 0 {
                errStr := ""
                for _, err := range execErrs {
                    errStr += fmt.Sprintf("\t%s\n", err.Error())
                }
                return errors.New(errStr)
            }

            executor.Select()
        }
    }

    for _, executor := range executors {
        executor.Shutdown()
    }

    if finalErr != nil {
        return finalErr
    }

    return nil
}

func RunBwbWorkflow(
    ctx workflow.Context,
    storageId string,
    jobConfig parsing.JobConfig,
    bwbWorkflow parsing.Workflow,
    index parsing.WorkflowIndex,
    workers map[string]WorkerInfo,
    masterFS fs.LocalFS,
    softFail bool,
) error {
    cmdMan := parsing.NewCmdManager(bwbWorkflow, index, jobConfig)
    executors, executorList, err := setupExecutors(
        ctx, storageId, bwbWorkflow, &cmdMan, workers, masterFS, jobConfig,
    )
    if err != nil {
        return fmt.Errorf("error parsing job config: %s", err)
    }
    logger := workflow.GetLogger(ctx)
    return RunBwbWorkflowHelper(logger, &cmdMan, executors, executorList, softFail)
}

func RunBwbWorkflowNoTemporal(
    storageId string,
    bwbWorkflow parsing.Workflow,
    index parsing.WorkflowIndex,
    localWorker WorkerInfo,
    masterFS fs.LocalFS,
    softFail bool,
    logger slog.Logger,
) error {
    configs := parsing.GetDefaultConfig(bwbWorkflow, true)
    cmdMan := parsing.NewCmdManager(bwbWorkflow, index, configs)
    executors := make(map[int]Executor)
    localExecutor := NewLocalExecutor(
        &cmdMan, masterFS, localWorker, storageId, logger,
    )
    executorList := []Executor{&localExecutor}
    for nodeId := range bwbWorkflow.Nodes {
        executors[nodeId] = &localExecutor
    }
    return RunBwbWorkflowHelper(&logger, &cmdMan, executors, executorList, softFail)
}
