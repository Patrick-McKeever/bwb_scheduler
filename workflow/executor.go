package workflow

import (
	"bytes"
	"fmt"
	"go-scheduler/fs"
	"go-scheduler/parsing"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"golang.org/x/crypto/ssh"
)

var JOB_CODES = map[string]struct {
    done   bool
    failed bool
}{
    "RUNNING":       {done: false, failed: false},
    "COMPLETED":     {done: true, failed: false},
    "BOOT_FAIL":     {done: true, failed: true},
    "CANCELLED":     {done: true, failed: true},
    "DEADLINE":      {done: true, failed: true},
    "FAILED":        {done: true, failed: true},
    "NODE_FAIL":     {done: true, failed: true},
    "OUT_OF_MEMORY": {done: true, failed: true},
    "PREEMPTED":     {done: true, failed: true},
}

type SlurmJob struct {
    CmdId             int
    TmpOutputHostPath string
    ExpOutFilePnames  []string
    JobId             string
    SbatchPath        string
    OutPath           string
    ErrPath           string
}

type GetOutputsFuture struct {
    future workflow.Future
    result SacctResult
}

type SlurmState struct {
    FinalErr         error
    SlurmConfig      parsing.SshConfig
    SlurmFS          fs.SshFS
    RunningJobs      map[string]SlurmJob
    CompletedJobs    map[string]SlurmJob
    GetOutputFutures map[string]GetOutputsFuture
    SchedDir         string
    StorageId        string
}

func GetTemporalSshQueueName(config parsing.SshConfig) string {
    return fmt.Sprintf("%s@%s", config.User, config.IpAddr)
}

type SlurmActivity struct {
    Config      parsing.SshConfig
    Mtx         sync.RWMutex
    Client      *ssh.Client
    ConnConfig  *ssh.ClientConfig
}

type CmdOut struct {
    ExitCode int
    StdOut   string
    StdErr   string
}

func (connMan *SlurmActivity) ensureConnected() (*ssh.Client, error) {
    connMan.Mtx.RLock()
    c := connMan.Client
    connMan.Mtx.RUnlock()

    if c != nil {
        return c, nil
    }

    connMan.Mtx.Lock()
    defer connMan.Mtx.Unlock()
    // Handle case where another goroutine already reconnected.
    if connMan.Client != nil {
        return connMan.Client, nil
    }

    client, err := ssh.Dial("tcp", connMan.Config.IpAddr, connMan.ConnConfig)
    if err != nil {
        return nil, err
    }
    connMan.Client = client
    return client, nil
}

func (connMan *SlurmActivity) Close() {
    connMan.Mtx.Lock()
    defer connMan.Mtx.Unlock()
    if connMan.Client != nil {
        connMan.Client.Close()
        connMan.Client = nil
    }
}


type CmdRunner func(string) (CmdOut, error)

func (connMan *SlurmActivity) ExecCmd(cmd string) (CmdOut, error) {
    connMan.Mtx.RLock()
    defer connMan.Mtx.RUnlock()
    if connMan.Client == nil {
        return CmdOut{}, fmt.Errorf(
            "worker does not have established SSH client for %s@%s",
            connMan.Config.User, connMan.Config.IpAddr,
        )
    }

    var stdout, stderr bytes.Buffer
    session, err := connMan.Client.NewSession()
    if err != nil {
        // If we can't create session, try resetting client.
        connMan.Close()
        client, err := connMan.ensureConnected()
        if err != nil {
            return CmdOut{}, fmt.Errorf("error reconnecting to ssh: %s", err)
        }
        
        session, err = client.NewSession()
        if err != nil {
            return CmdOut{}, fmt.Errorf("error getting ssh session: %s", err)
        }
    }

    var fullCmd string
    if connMan.Config.CmdPrefix == nil {
        fullCmd = cmd
    } else {
        fullCmd = fmt.Sprintf("%s %s", *connMan.Config.CmdPrefix, cmd)
    }

    session.Stdout = &stdout
    session.Stderr = &stderr
    err = session.Run(fullCmd)
    exitCode := 0
    if err != nil {
        if exitErr, ok := err.(*ssh.ExitError); ok {
            exitCode = exitErr.ExitStatus()
        }
    }
    return CmdOut{
        ExitCode: exitCode,
        StdOut:   stdout.String(),
        StdErr:   stderr.String(),
    }, err
}

func WriteSbatchFile(
    outStream io.Writer, cmd parsing.CmdTemplate, volumes map[string]string,
    slurmConfig parsing.SshConfig, jobConfig parsing.SlurmJobConfig, 
    storageDir string,
) (string, string, error) {

    outBasePath := fmt.Sprintf("%s.out", randomString(16))
    errBasePath := fmt.Sprintf("%s.err", randomString(16))
    outPath := filepath.Join(storageDir, "slurm", outBasePath)
    errPath := filepath.Join(storageDir, "slurm", errBasePath)
    fmt.Fprintln(outStream, "#!/bin/bash")
    fmt.Fprintf(outStream, "#SBATCH --output=%s\n", outPath)
    fmt.Fprintf(outStream, "#SBATCH --error=%s\n", errPath)

    if jobConfig.Partition != nil {
        fmt.Fprintf(outStream, "#SBATCH --partition=%s\n", *jobConfig.Partition)
    }

    if jobConfig.Time != nil {
        fmt.Fprintf(outStream, "#SBATCH --time=%s\n", *jobConfig.Time)
    }

    if jobConfig.Ntasks != nil {
        fmt.Fprintf(outStream, "#SBATCH --ntasks=%d\n", *jobConfig.Ntasks)
    }

    if jobConfig.Nodes != nil {
        fmt.Fprintf(outStream, "#SBATCH --nodes=%d\n", *jobConfig.Nodes)
    }

    if jobConfig.Gpus != nil {
        fmt.Fprintf(outStream, "#SBATCH --gpus=%s\n", *jobConfig.Gpus)
    }

    if jobConfig.Mem != nil {
        fmt.Fprintf(outStream, "#SBATCH --mem=%s\n", *jobConfig.Mem)
    } else {
        fmt.Fprintf(outStream, "#SBATCH --mem=%dMB\n", cmd.ResourceReqs.MemMb)
    }

    if jobConfig.CpusPerTask != nil {
        fmt.Fprintf(outStream, "#SBATCH --cpus-per-task=%d\n", *jobConfig.CpusPerTask)
    } else {
        fmt.Fprintf(outStream, "#SBATCH --cpus-per-task=%d\n", cmd.ResourceReqs.Cpus)
    }

    if jobConfig.Modules != nil {
        for _, module := range *jobConfig.Modules {
            fmt.Fprintf(outStream, "module load %s\n", module)
        }
    }

    localSifPath := filepath.Join(storageDir, "images", cmd.ImageName)
    useGpu := jobConfig.Gpus != nil || cmd.ResourceReqs.Gpus > 0
    cmdStr, envs := parsing.FormSingularityCmd(
        cmd, volumes, localSifPath, useGpu,
    )
    fmt.Fprintf(outStream, "%s %s", strings.Join(envs, " "), cmdStr)
    fmt.Printf("Writing sbatch w/ cmd %s %s\n", strings.Join(envs, " "), cmdStr)

    return outPath, errPath, nil
}

type SacctResult struct {
    JobId    string
    State    string
    ExitCode string
}

func RunSacct(
    outstandingJobIds []string, runCmd CmdRunner,
) (map[string]SacctResult, error) {
    oustandingJobsStr := ""
    for i, jobId := range outstandingJobIds {
        oustandingJobsStr += jobId
        if i < len(outstandingJobIds)-1 {
            oustandingJobsStr += ","
        }
    }

    sacctCmd := fmt.Sprintf(
        "sacct -j %s -o JobID,State,ExitCode -n -P", oustandingJobsStr,
    )
    sacctOut, err := runCmd(sacctCmd)
    if err != nil {
        return nil, fmt.Errorf(
            "sacct failed with exit code %d, error %s, and stderr %s",
            sacctOut.ExitCode, err, sacctOut.StdErr,
        )
    }

    out := make(map[string]SacctResult, 0)
    jobRecords := strings.Split(sacctOut.StdOut, "\n")
    for _, rawRecord := range jobRecords {
        // Handle trailing / leading newlines.
        if rawRecord == "" {
            continue
        }

        rawFields := strings.Split(rawRecord, "|")
        if len(rawFields) != 3 {
            return nil, fmt.Errorf(
                "sacct returned record \"%s\" with more/fewer than 3 fields; "+
                    "expected fields JobID, State, and ExitCode", rawRecord,
            )
        }

        cleanedJobId := strings.Split(rawFields[0], ".")[0]
        out[cleanedJobId] = SacctResult{
            JobId:    cleanedJobId,
            State:    rawFields[1],
            ExitCode: rawFields[2],
        }
    }

    return out, nil
}

// Filesystems such as LUSTRE can have some latency in propagating
// state, espsecially when you also have to wait for container
// volumes to propagate state. The purpose of this command is to
// add a simple grace period (5 seconds) for this propagation
// to happen.
func AwaitFileExistence(path string, runCmd CmdRunner) bool {
    lsOut, _ := runCmd(fmt.Sprintf("ls %s", path))
    if lsOut.ExitCode == 0 {
        return true
    }

    time.Sleep(time.Second * 5)
    lsOut, _ = runCmd(fmt.Sprintf("ls %s", path))
    return lsOut.ExitCode == 0
}

func (connMan *SlurmActivity) GetRemoteSlurmJobOutputsActivity(
    job SlurmJob,
) (CmdOutput, error) {
    return GetSlurmJobOutputs(job,
        func(cmd string) (CmdOut, error) {
            return connMan.ExecCmd(cmd)
        },
    )
}

func (connMan *SlurmActivity) PollRemoteSlurmActivity(
    outstandingJobIds []string,
) (map[string]SacctResult, error) {
    // Keepalive.
    if len(outstandingJobIds) == 0 {
        _, err := connMan.ExecCmd("echo Keepalive")
        return nil, err
    }

    return RunSacct(outstandingJobIds,
        func(cmd string) (CmdOut, error) {
            return connMan.ExecCmd(cmd)
        },
    )
}

func (connMan *SlurmActivity) StartRemoteSlurmJobActivity(
    cmd parsing.CmdTemplate, jobConfig parsing.SlurmJobConfig, 
    fs fs.SshFS, schedDir string,
) (SlurmJob, error) {
    tmpOutputHostPath := filepath.Join(schedDir, randomString(16))
    mkTmpDirCmd := fmt.Sprintf("mkdir -p %s", tmpOutputHostPath)
    if _, err := connMan.ExecCmd(mkTmpDirCmd); err != nil {
        return SlurmJob{}, fmt.Errorf(
            "failed to make tmp dir %s: %s", tmpOutputHostPath, err,
        )
    }

    sbatchFname := fmt.Sprintf("%s.sbatch", randomString(16))
    sbatchLocalPath := filepath.Join("/tmp", sbatchFname)
    sbatchRemotePath := filepath.Join(schedDir, sbatchFname)
    tmpFile, err := os.Create(sbatchLocalPath)
    if err != nil {
        return SlurmJob{}, fmt.Errorf(
            "failed to make local tmp file %s: %s",
            sbatchLocalPath, err,
        )
    }
    defer tmpFile.Close()

    volumes := fs.GetVolumes()
    volumes["/tmp/output"] = tmpOutputHostPath
    mkTmpDirRemoteCmd := fmt.Sprintf("mkdir -p %s", tmpOutputHostPath)
    if _, err = connMan.ExecCmd(mkTmpDirRemoteCmd); err != nil {
        return SlurmJob{}, fmt.Errorf(
            "unable to make remote tmp dir %s: %s",
            tmpOutputHostPath, err,
        )
    }

    outPath, errPath, err := WriteSbatchFile(
        tmpFile, cmd, volumes, connMan.Config, jobConfig, schedDir,
    )
    if err != nil {
        return SlurmJob{}, fmt.Errorf("error writing sbatch file: %s", err)
    }

    tmpFile.Sync()
    if err := fs.Upload(sbatchLocalPath, sbatchRemotePath); err != nil {
        return SlurmJob{}, fmt.Errorf(
            "unable to upload sbatch file from %s to %s: %s",
            sbatchLocalPath, sbatchRemotePath, err,
        )
    }

    sbatchCmd := fmt.Sprintf("sbatch --parsable %s", sbatchRemotePath)
    sbatchOut, err := connMan.ExecCmd(sbatchCmd)
    if err != nil {
        return SlurmJob{}, fmt.Errorf(
            "\"%s\" failed with exit code %d, error %s, and stderr %s",
            sbatchCmd, sbatchOut.ExitCode, err, sbatchOut.StdErr,
        )
    }

    // Note: sbatch --parsable output has one of two forms.
    // In multi-cluster systems, it gives JOBID;CLUSTER.
    // In single-cluster systems, it's just JOBID.
    jobIdRaw := strings.Split(sbatchOut.StdOut, ";")[0]
    jobId := strings.TrimSuffix(jobIdRaw, "\n")
    return SlurmJob{
        CmdId:             cmd.Id,
        JobId:             jobId,
        TmpOutputHostPath: tmpOutputHostPath,
        ExpOutFilePnames:  cmd.OutFilePnames,
        SbatchPath:        sbatchRemotePath,
        OutPath:           outPath,
        ErrPath:           errPath,
    }, nil
}

func GetSlurmJobOutputs(job SlurmJob, runCmd CmdRunner) (CmdOutput, error) {
    cmdOutput := CmdOutput{
        Id:          job.CmdId,
        RawOutputs:  make(map[string]string),
        OutputFiles: make([]string, 0),
    }

    if !AwaitFileExistence(job.OutPath, runCmd) {
        return CmdOutput{}, fmt.Errorf(
            "job ID %s (cmd ID %d) outfile %s does not exist",
            job.JobId, job.CmdId, job.OutPath,
        )
    }

    catOutFileCmd := fmt.Sprintf("cat %s", job.OutPath)
    catStdoutOut, err := runCmd(catOutFileCmd)
    if err != nil {
        return CmdOutput{}, fmt.Errorf(
            "\"%s\" failed with exit code %d, error %s, and stderr %s",
            catOutFileCmd, catStdoutOut.ExitCode, err, catStdoutOut.StdErr,
        )
    }
    cmdOutput.StdOut = catStdoutOut.StdOut

    if !AwaitFileExistence(job.ErrPath, runCmd) {
        return CmdOutput{}, fmt.Errorf(
            "job ID %s (cmd ID %d) errfile %s does not exist",
            job.JobId, job.CmdId, job.ErrPath,
        )
    }

    catErrFileCmd := fmt.Sprintf("cat %s", job.ErrPath)
    catStderrOut, err := runCmd(catErrFileCmd)
    if err != nil {
        return CmdOutput{}, fmt.Errorf(
            "\"%s\" failed with exit code %d, error %s, and stderr %s",
            catErrFileCmd, catStderrOut.ExitCode, err, catStderrOut.StdErr,
        )
    }
    cmdOutput.StdErr = catStderrOut.StdErr

    // TEMPORARY FIX: Account for time to propagate singularity volumes to 
    // host FS.
    time.Sleep(2*time.Second)
    lsCmd := fmt.Sprintf("ls -1 %s", job.TmpOutputHostPath)
    lsOut, err := runCmd(lsCmd)
    if err != nil {
        return CmdOutput{}, fmt.Errorf(
            "\"%s\" failed with exit code %d, error %s, and stderr %s",
            lsCmd, lsOut.ExitCode, err, lsOut.StdErr,
        )
    }

    files := strings.Split(strings.TrimSpace(string(lsOut.StdOut)), "\n")
    for _, file := range files {
        if file == "" {
            continue
        }

        catCmd := fmt.Sprintf("cat %s", filepath.Join(job.TmpOutputHostPath, file))
        catOutfileOut, err := runCmd(catCmd)
        if err != nil {
            return CmdOutput{}, fmt.Errorf(
                "\"%s\"failed with exit code %d, error %s, and stderr %s",
                catCmd, catOutfileOut.ExitCode, err, catOutfileOut.StdErr,
            )
        }

        cmdOutput.RawOutputs[file] = catOutfileOut.StdOut
    }

    for _, expOutFilePname := range job.ExpOutFilePnames {
        outFilesRaw, outFileExists := cmdOutput.RawOutputs[expOutFilePname]
        if outFileExists {
            outFileVals := strings.Split(outFilesRaw, "\n")
            if len(outFileVals) > 0 {
                if outFileVals[len(outFileVals)-1] == "" {
                    outFileVals = outFileVals[:len(outFileVals)-1]
                }
                cmdOutput.OutputFiles = append(
                    cmdOutput.OutputFiles, outFileVals...,
                )
            }
        }
    }

    return cmdOutput, nil
}

type SlurmRequest struct {
    Cmd    parsing.CmdTemplate
    Config parsing.SlurmJobConfig
}

type SlurmResponse struct {
    Result CmdOutput
    Error  error
}

func ProcessCompletedSlurmCmd(
    ctx workflow.Context, state *SlurmState,
    slurmRes SacctResult, output CmdOutput, err error,
) {
    parentID := workflow.GetInfo(ctx).ParentWorkflowExecution.ID
    parentRunID := workflow.GetInfo(ctx).ParentWorkflowExecution.RunID
    if err == nil {
        workflow.SignalExternalWorkflow(
            ctx, parentID, parentRunID, "slurm-response", SlurmResponse{
                Result: output,
                Error: err,
            },
        )
        return
    }

    var slurmErr error = nil
    jobId := slurmRes.JobId
    job := state.RunningJobs[jobId]
    if JOB_CODES[slurmRes.State].failed {
        slurmErr = fmt.Errorf(
            "job %d failed with code %s, stderr %s", job.CmdId,
            slurmRes.State, output.StdErr,
        )
    }

    workflow.SignalExternalWorkflow(
        ctx, parentID, parentRunID, "slurm-response", SlurmResponse{
            Result: output,
            Error:  slurmErr,
        },
    )
    delete(state.GetOutputFutures, jobId)
    state.CompletedJobs[jobId] = job
}

func ProcessSacctResult(
    ctx workflow.Context, selector workflow.Selector,
    state *SlurmState, result SacctResult,
) {
    var a SlurmActivity
    logger := workflow.GetLogger(ctx)
    jobId := result.JobId
    job, jobExists := state.RunningJobs[jobId]
    if !jobExists {
        logger.Warn(
            "job ID %d (returned by sacct) is not a running job", jobId,
        )
        return
    }

    if JOB_CODES[result.State].done {
        delete(state.RunningJobs, jobId)
        ao := workflow.ActivityOptions{
            TaskQueue:           GetTemporalSshQueueName(state.SlurmConfig),
            StartToCloseTimeout: time.Minute,
            RetryPolicy: &temporal.RetryPolicy{
                MaximumAttempts:    1,
                BackoffCoefficient: 4,
            },
        }
        childCtx := workflow.WithActivityOptions(ctx, ao)
        f := workflow.ExecuteActivity(childCtx, a.GetRemoteSlurmJobOutputsActivity, job)
        state.GetOutputFutures[result.JobId] = GetOutputsFuture{
            future: f,
            result: result,
        }
        selector.AddFuture(f, func(f workflow.Future) {
            var output CmdOutput
            err := f.Get(ctx, &output)
            ProcessCompletedSlurmCmd(ctx, state, result, output, err)
        })
    }
}

func StartSlurmJob(
    req SlurmRequest, ctx workflow.Context,
    state *SlurmState,
) {
    var job SlurmJob
    var a SlurmActivity
    ao := workflow.ActivityOptions{
        TaskQueue:           GetTemporalSshQueueName(state.SlurmConfig),
        StartToCloseTimeout: time.Minute,
        RetryPolicy: &temporal.RetryPolicy{
            MaximumAttempts:    1,
            BackoffCoefficient: 4,
        },
    }
    childCtx := workflow.WithActivityOptions(ctx, ao)
    err := workflow.ExecuteActivity(
        childCtx, a.StartRemoteSlurmJobActivity, req.Cmd,
        req.Config, state.SlurmFS, state.SchedDir,
    ).Get(ctx, &job)

    if err != nil {
        state.FinalErr = err
    }

    state.RunningJobs[job.JobId] = job
}

func PollSlurm(ctx workflow.Context, selector workflow.Selector, state *SlurmState) {
    runningJobIds := make([]string, 0)
    for jobId := range state.RunningJobs {
        runningJobIds = append(runningJobIds, jobId)
    }

    var a SlurmActivity
    var jobRes map[string]SacctResult
    ao := workflow.ActivityOptions{
        TaskQueue:           GetTemporalSshQueueName(state.SlurmConfig),
        StartToCloseTimeout: time.Minute,
        RetryPolicy: &temporal.RetryPolicy{
            MaximumAttempts:    1,
            BackoffCoefficient: 4,
        },
    }
    childCtx := workflow.WithActivityOptions(ctx, ao)
    err := workflow.ExecuteActivity(
        childCtx, a.PollRemoteSlurmActivity, runningJobIds,
    ).Get(ctx, &jobRes)

    if err != nil {
        state.FinalErr = err
    }

    for _, result := range jobRes {
        ProcessSacctResult(ctx, selector, state, result)
    }
}

func SlurmPollerWorkflow(ctx workflow.Context, state SlurmState) error {
    // These should be empty at start of each workflow incarnation.
    state.FinalErr = nil
    state.GetOutputFutures = make(map[string]GetOutputsFuture)

    // These will be emtpy in initial incarnation but full after continue-as-new.
    if state.RunningJobs == nil {
        state.RunningJobs = make(map[string]SlurmJob)
    }
    if state.CompletedJobs == nil {
        state.CompletedJobs = make(map[string]SlurmJob)
    }
    selector := workflow.NewSelector(ctx)

    slurmReqChan := workflow.GetSignalChannel(ctx, "slurm-request")
    selector.AddReceive(slurmReqChan, func(c workflow.ReceiveChannel, _ bool) {
        var pendingReq SlurmRequest
        c.Receive(ctx, &pendingReq)
        StartSlurmJob(pendingReq, ctx, &state)
    })

    durationSecsAsTime := time.Second * 5
    var timerCallback func(workflow.Future)
    timerCallback = func(f workflow.Future) {
        PollSlurm(ctx, selector, &state)
        timer := workflow.NewTimer(ctx, durationSecsAsTime)
        selector.AddFuture(timer, timerCallback)
    }

    timer := workflow.NewTimer(ctx, durationSecsAsTime)
    selector.AddFuture(timer, timerCallback)

    for workflow.GetInfo(ctx).GetCurrentHistoryLength() < 9000 {
        selector.Select(ctx)
        if ctx.Err() != nil {
            return nil
        }

        if state.FinalErr != nil {
            return state.FinalErr
        }
    }

    // It is necessary to drain pending requests before continuing as new,
    // since they won't be delivered to successor workflow (see
    // https://temporal.io/blog/very-long-running-workflows#continue-as-new).
    var pendingReq SlurmRequest
    for {
        ok := slurmReqChan.ReceiveAsync(&pendingReq)
        if !ok {
            break
        }
        StartSlurmJob(pendingReq, ctx, &state)
    }

    // Likewise, make sure that any running GetOutput tasks complete
    // before continuing, since we'll lose their AddFuture handlers along
    // with the selector.
    for _, f := range state.GetOutputFutures {
        var output CmdOutput
        err := f.future.Get(ctx, &output)
        ProcessCompletedSlurmCmd(ctx, &state, f.result, output, err)
    }

    return workflow.NewContinueAsNewError(ctx, SlurmPollerWorkflow, state)
}

// Need to make activities for slurm stuff.

// Need to make child workflow which polls slurm, takes messages from main workflow,
// continues as new.

// Need to make executor which sends signals to that workflow.

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
    selector          workflow.Selector
    slurmPollerWE     workflow.Execution
    slurmPollerFuture workflow.ChildWorkflowFuture
    cancelChild       func()
}

func NewSlurmRemoteExecutor(
    ctx workflow.Context, cmdMan *parsing.CmdManager,
    masterFS fs.LocalFS, storageId string, 
    configsByNode map[int]parsing.SlurmJobConfig, 
    sshConfig parsing.SshConfig,
) SlurmRemoteExecutor {
    var state SlurmRemoteExecutor
    state.ctx = ctx
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
    exec.selector.AddFuture(uploadFuture, func(f workflow.Future) {
        err := f.Get(exec.ctx, nil)
        if err != nil {
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

    exec.slurmPollerFuture = workflow.ExecuteChildWorkflow(
        childCtx, SlurmPollerWorkflow, SlurmState{
            SlurmConfig: exec.sshConfig,
            StorageId:   exec.storageId,
            SchedDir:    exec.schedDir,
            SlurmFS:     slurmFS,
        },
    )

    err = exec.slurmPollerFuture.GetChildWorkflowExecution().Get(exec.ctx, &exec.slurmPollerWE)
    if err != nil {
        outErr := fmt.Errorf("failed getting child WF execution: %s", err)
        return outErr
    }
    exec.cancelChild = cancelChild

    exec.selector = workflow.NewSelector(exec.ctx)
    slurmJobResChan := workflow.GetSignalChannel(exec.ctx, "slurm-response")
    exec.selector.AddReceive(slurmJobResChan, func(c workflow.ReceiveChannel, _ bool) {
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
        exec.selector.AddFuture(timer, checkForChildFailure)
    }

    timer := workflow.NewTimer(exec.ctx, 1 * time.Minute)
    exec.selector.AddFuture(timer, checkForChildFailure)
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
                    "pname", inFilePname, "files", fmt.Sprintf("%#v", inFileList),
                )
            }
        }
        logger.Info("Downloading infiles", "infiles", inFiles)

        downloadFuture := fs.RunTransferActivity(
            downloadCtx, exec.storageId, exec.masterFS, exec.SlurmFS,
            inFiles,
        )

        exec.selector.AddFuture(downloadFuture, func(f workflow.Future) {
            err := f.Get(exec.ctx, nil)
            if err != nil {
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
    exec.selector.Select(exec.ctx)
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
