package workflow

import (
    "fmt"
    "time"
    "strings"
    "path/filepath"

    "go-scheduler/fs"
    "go-scheduler/parsing"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)


type SlurmRemoteExecutor struct {
    ctx               workflow.Context
    masterFS          fs.AbstractFileSystem
    storageId         string
    SlurmFS           fs.SshFS
    cmdsById          map[int]parsing.CmdTemplate
    errors            []error
    sshConfig         parsing.SshConfig
    handleFinishedCmd CmdHandler
    configsByNode     map[int]parsing.SlurmJobConfig
    schedDir          string
    selector          *workflow.Selector
    slurmPollerWE     workflow.Execution
    slurmPollerFuture workflow.ChildWorkflowFuture
    cancelChild       func()
}

func NewSlurmRemoteExecutor(
    ctx workflow.Context, selector *workflow.Selector,
    masterFS fs.LocalFS, storageId string, 
    configsByNode map[int]parsing.SlurmJobConfig, 
    sshConfig parsing.SshConfig,
) SlurmRemoteExecutor {
    var state SlurmRemoteExecutor
    state.ctx = ctx
    state.selector = selector
    state.masterFS = masterFS
    state.storageId = storageId
    state.cmdsById = make(map[int]parsing.CmdTemplate)
    state.errors = make([]error, 0)
    state.sshConfig = sshConfig
    state.schedDir = sshConfig.SchedDir
    state.configsByNode = configsByNode
    return state
}

func (exec *SlurmRemoteExecutor) setupFS() (fs.SshFS, error) {
    // Setup container filesystem on SLURM fs.
    var a SlurmActivity
    dataDir := filepath.Join(exec.schedDir, exec.storageId)
    ao := workflow.ActivityOptions{
        TaskQueue:           GetTemporalSshQueueName(exec.sshConfig),
        StartToCloseTimeout: 10 * time.Minute,
        RetryPolicy: &temporal.RetryPolicy{
            MaximumAttempts: 1,
        },
    }
    mkdirCtx := workflow.WithActivityOptions(exec.ctx, ao)

    err := workflow.ExecuteActivity(
        mkdirCtx, a.ExecCmd, fmt.Sprintf("mkdir -p %s", dataDir),
    ).Get(exec.ctx, nil)
    if err != nil {
        return fs.SshFS{}, err
    }
    return fs.SshFS{
        User:          exec.sshConfig.User,
        Endpt:         exec.sshConfig.TransferAddr,
        RemoteVolumes: map[string]string{"/data": dataDir},
    }, nil
}

func (exec *SlurmRemoteExecutor) handleSlurmCompleteSignal(jobRes SlurmResponse) {
    cmd := exec.cmdsById[jobRes.Result.Id]
    if jobRes.Error != nil {
        exec.handleFinishedCmd(jobRes.Result, jobRes.Error, exec, cmd)
        return
    }

    logger := workflow.GetLogger(exec.ctx)
    for outFilePname, outFile := range jobRes.Result.OutputFiles {
        logger.Info(
            "Uploading output", "nodeId", cmd.NodeId, "cmdId", cmd.Id, 
            "outFilePname", outFilePname, "file", outFile,
        )
    }

    // If job completed successfully, upload its outputs to masterFS.
    ao := workflow.ActivityOptions{
        TaskQueue: GetTemporalSshQueueName(exec.sshConfig),
        StartToCloseTimeout: time.Hour * 1,
        RetryPolicy: &temporal.RetryPolicy{
            MaximumAttempts: 1,
        },
    }
    uploadCtx := workflow.WithActivityOptions(exec.ctx, ao)
    uploadFuture := fs.RunTransferActivity(
        uploadCtx, exec.storageId, exec.SlurmFS, exec.masterFS,
        jobRes.Result.OutputFiles,
    )
    (*exec.selector).AddFuture(uploadFuture, func(f workflow.Future) {
        err := f.Get(exec.ctx, nil)
        if err != nil {
            logger.Error(
                "failed to transfer files from SLURM executor", 
                "nodeId", cmd.NodeId, "cmdId", cmd.Id,
                "files", jobRes.Result.OutputFiles,
                "error", err,
            )
            exec.errors = append(exec.errors, err)
            return
        }
        exec.handleFinishedCmd(jobRes.Result, jobRes.Error, exec, cmd)
    })
}

func (exec *SlurmRemoteExecutor) Setup() error {
    // Execute slurm poller child workflow.
    slurmFS, err := exec.setupFS()
    if err != nil {
        return err
    }
    exec.SlurmFS = slurmFS

    childCtx, cancelChild := workflow.WithCancel(exec.ctx)
    queueName := GetTemporalSshQueueName(exec.sshConfig)
    schedChildWfOptions := workflow.ChildWorkflowOptions{
        WorkflowID: fmt.Sprintf("slurm_poller_%s", queueName),
        TaskQueue:  queueName,
    }
    childCtx = workflow.WithChildOptions(childCtx, schedChildWfOptions)

    workflowId := workflow.GetInfo(exec.ctx).WorkflowExecution.ID
    workflowRunId := workflow.GetInfo(exec.ctx).WorkflowExecution.RunID
    exec.slurmPollerFuture = workflow.ExecuteChildWorkflow(
        childCtx, SlurmPollerWorkflow, SlurmState{
            ParentWfId:     workflowId,
            ParentWfRunId:  workflowRunId,
            SlurmConfig:    exec.sshConfig,
            StorageId:      exec.storageId,
            SchedDir:       exec.schedDir,
            SlurmFS:        slurmFS,
        },
    )

    err = exec.slurmPollerFuture.GetChildWorkflowExecution().Get(exec.ctx, &exec.slurmPollerWE)
    if err != nil {
        outErr := fmt.Errorf("failed getting child WF execution: %s", err)
        return outErr
    }
    exec.cancelChild = cancelChild

    slurmJobResChan := workflow.GetSignalChannel(exec.ctx, "slurm-response")
    (*exec.selector).AddReceive(slurmJobResChan, func(c workflow.ReceiveChannel, _ bool) {
        var jobRes SlurmResponse
        c.Receive(exec.ctx, &jobRes)
        exec.handleSlurmCompleteSignal(jobRes)
    })

    var checkForChildFailure func(workflow.Future)
    checkForChildFailure = func (f workflow.Future) {
        if exec.slurmPollerFuture.IsReady() {
            err := exec.slurmPollerFuture.Get(exec.ctx, nil)
            exec.errors = append(exec.errors, fmt.Errorf(
                "slurm poller WF failed w/ err %s", err,
            ))
        }
        timer := workflow.NewTimer(exec.ctx, 1 * time.Minute)
        (*exec.selector).AddFuture(timer, checkForChildFailure)
    }

    timer := workflow.NewTimer(exec.ctx, 1 * time.Minute)
    (*exec.selector).AddFuture(timer, checkForChildFailure)
    return nil
}

func (exec *SlurmRemoteExecutor) RunCmds(cmds []parsing.CmdTemplate) {
    ao := workflow.ActivityOptions{
        TaskQueue: GetTemporalSshQueueName(exec.sshConfig),
        StartToCloseTimeout: time.Hour * 1,
        RetryPolicy: &temporal.RetryPolicy{
            MaximumAttempts: 1,
        },
    }
    downloadCtx := workflow.WithActivityOptions(exec.ctx, ao)

    for _, cmd := range cmds {
        slurmConfig, configExists := exec.configsByNode[cmd.NodeId]
        if !configExists {
            exec.errors = append(exec.errors, fmt.Errorf(
                "no slurm config for node %d", cmd.NodeId,
            ))
        }

        inFiles := make([]string, 0)
        logger := workflow.GetLogger(exec.ctx)
        for inFilePname, inFileList := range cmd.InFiles {
            if len(inFileList) > 0 {
                inFiles = append(inFiles, inFileList...)
                logger.Info(
                    "Downloading input", "nodeId", cmd.NodeId, "cmdId", cmd.Id, 
                    "pname", inFilePname, "files", inFileList,
                )
            }
        }
        logger.Info("Downloading infiles", "infiles", inFiles)

        downloadFuture := fs.RunTransferActivity(
            downloadCtx, exec.storageId, exec.masterFS, exec.SlurmFS,
            inFiles,
        )

        (*exec.selector).AddFuture(downloadFuture, func(f workflow.Future) {
            err := f.Get(exec.ctx, nil)
            if err != nil {
                logger.Error(
                    "failed to transfer files to SLURM executor", 
                    "nodeId", cmd.NodeId, "cmdId", cmd.Id,
                    "files", inFiles, "error", err,
                )
                exec.errors = append(exec.errors, fmt.Errorf(
                    "error uploading inputs for cmd %d: %s", cmd.Id, err,
                ))
                return
            }

            exec.cmdsById[cmd.Id] = cmd
            req := SlurmRequest{
                Cmd:    cmd,
                Config: slurmConfig,
            }
            workflow.SignalExternalWorkflow(
                exec.ctx, exec.slurmPollerWE.ID, exec.slurmPollerWE.RunID,
                "slurm-request", req,
            )
        })
    }
}

func (exec *SlurmRemoteExecutor) SetCmdHandler(handler CmdHandler) {
    exec.handleFinishedCmd = handler
}

func (exec *SlurmRemoteExecutor) Select() {
    (*exec.selector).Select(exec.ctx)
}

func (exec *SlurmRemoteExecutor) Shutdown() {
    exec.cancelChild()
}

func (exec *SlurmRemoteExecutor) GetErrors() []error {
    return exec.errors
}

func (exec *SlurmRemoteExecutor) BuildImages(imageNames []string) error {
    buildAo := workflow.ActivityOptions{
        TaskQueue:           "bwb_worker",
        StartToCloseTimeout: 10 * time.Minute,
        RetryPolicy: &temporal.RetryPolicy{
            MaximumAttempts: 1,
        },
    }
    buildCtx := workflow.WithActivityOptions(exec.ctx, buildAo)
    xferAo := workflow.ActivityOptions{
        TaskQueue:           GetTemporalSshQueueName(exec.sshConfig),
        StartToCloseTimeout: 10 * time.Minute,
        RetryPolicy: &temporal.RetryPolicy{
            MaximumAttempts: 1,
        },
    }
    xferCtx := workflow.WithActivityOptions(exec.ctx, xferAo)
    for _, imageName := range imageNames {
        var outPath string
        err := workflow.ExecuteActivity(
            buildCtx, BuildSingularitySIF, imageName,
        ).Get(exec.ctx, &outPath)
        if err != nil {
            return err
        }

        dstPath := filepath.Join(exec.schedDir, "images", imageName)
        err = workflow.ExecuteActivity(
            xferCtx, fs.SshUploadActivity, exec.SlurmFS, outPath, dstPath,
        ).Get(exec.ctx, nil)
        if err != nil {
            return err
        }
    }
    return nil
}

func (exec *SlurmRemoteExecutor) Glob(
    root string, pattern string, findFile bool, findDir bool,
) ([]string, error) {
    var a SlurmActivity
    dataDir := filepath.Join(exec.schedDir, exec.storageId)
    ao := workflow.ActivityOptions{
        TaskQueue:           GetTemporalSshQueueName(exec.sshConfig),
        StartToCloseTimeout: 1 * time.Minute,
    }
    findCtx := workflow.WithActivityOptions(exec.ctx, ao)

    volumes := map[string]string{"/data": dataDir}
    hostRootPath, ok := fs.GetHostPath(root, volumes)
    if !ok {
        return nil, fmt.Errorf("invalid root path %s", hostRootPath)
    }

    var typeStr string
    if findFile && findDir {
        typeStr = ""
    } else if findFile {
        typeStr = "-type f"
    } else if findDir {
        typeStr = "-type d"
    }

    var findOut CmdOut
    findCmd := fmt.Sprintf("find %s -name \"%s\" %s", hostRootPath, pattern, typeStr)
    err := workflow.ExecuteActivity(
        findCtx, a.ExecCmd, findCmd,
    ).Get(exec.ctx, &findOut)
    if err != nil {
        return nil, err
    }

    out := make([]string, 0)
    lines := strings.Split(findOut.StdOut, "\n")
    for _, line := range lines {
        if line == "" {
            continue
        }
        cntPath, ok := fs.GetCntPath(line, volumes)
        if !ok {
            return nil, fmt.Errorf(
                "couldn't convert host path %s to cnt path with volumes %#v",
                line, volumes,
            )
        }

        out = append(out, cntPath)
    }

    return out, nil
}