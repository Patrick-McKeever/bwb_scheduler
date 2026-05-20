package workflow

import (
	"bytes"
	"fmt"
	"go-scheduler/fs"
	"go-scheduler/parsing"
	"io"
	"maps"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"golang.org/x/crypto/ssh"
)

const MAX_SLURM_RETRIES = 2

var JOB_CODES = map[string]struct {
    done   bool
    failed bool
    fatal  bool
}{
    "RUNNING":       {done: false, failed: false, fatal: false},
    "COMPLETED":     {done: true, failed: false, fatal: false},
    "BOOT_FAIL":     {done: true, failed: true, fatal: false},
    "CANCELLED":     {done: true, failed: true, fatal: true},
    "DEADLINE":      {done: true, failed: true, fatal: false},
    "FAILED":        {done: true, failed: true, fatal: true},
    "NODE_FAIL":     {done: true, failed: true, fatal: false},
    "OUT_OF_MEMORY": {done: true, failed: true, fatal: false},
    "PREEMPTED":     {done: true, failed: true, fatal: false},
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
    future      workflow.Future
    results     map[string]SacctResult
    callback    func (workflow.Future)
}

type SlurmState struct {
    ParentWfId              string
    ParentWfRunId           string
    SlurmConfig             parsing.SshConfig
    SlurmFS                 fs.SshFS
    RunningJobs             map[string]SlurmJob
    PossiblyCompletedJobs   map[string]struct{}
    Requests                map[int]SlurmRequest
    NumRetries              map[int]int
    GetOutputFutures        map[int]GetOutputsFuture
    SchedDir                string
    StorageId               string
    MaxSlurmBatchId         int
    FinalErr                error
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

type SlurmRequest struct {
    Cmd    parsing.CmdTemplate
    Config parsing.SlurmJobConfig
}

type SlurmResponse struct {
    Result CmdOutput
    Error  *string
}

func GetTemporalSshQueueName(config parsing.SshConfig) string {
    return fmt.Sprintf("%s@%s", config.User, config.IpAddr)
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

        if strings.HasSuffix(rawFields[0], ".batch") || strings.HasSuffix(rawFields[0], ".extern") {
            continue
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
    jobs []SlurmJob,
) ([]CmdOutput, error) {
    // Allow container volumes time to propagate backward onto host FS.
    time.Sleep(5 * time.Second)
    out := make([]CmdOutput, 0)
    for _, job := range jobs {
        jobOut, err := GetSlurmJobOutputs(job,
            func(cmd string) (CmdOut, error) {
                return connMan.ExecCmd(cmd)
            },
        )

        if err != nil {
            return nil, err
        }
        out = append(out, jobOut)
    }

    return out, nil
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

func (connMan *SlurmActivity) mkdirIfNotExists(dir string) error {
    lsCmd := fmt.Sprintf("ls -1 %s", dir)
    lsOut, err := connMan.ExecCmd(lsCmd)
    if err == nil && lsOut.ExitCode == 0 {
        // CASE 1: Dir exists. (These two conditions should be the same)
        return nil
    } else if lsOut.ExitCode != 2 {
        // CASE 2: Unrelated ls error
        return fmt.Errorf(
            "\"%s\" failed with exit code %d, error %s, and stderr %s",
            lsCmd, lsOut.ExitCode, err, lsOut.StdErr,
        )
    }

    // CASE 3: File does not exist
    mkdirCmd := fmt.Sprintf("mkdir -p %s", dir)
    mkdirOut, err := connMan.ExecCmd(mkdirCmd)
    if err != nil {
        return fmt.Errorf(
            "\"%s\" failed with exit code %d, error %s, and stderr %s",
            mkdirCmd, mkdirOut.ExitCode, err, mkdirOut.StdErr,
        )
    }
    return nil
}

func (connMan *SlurmActivity) getSlurmVolumes(
    cmdTemplate parsing.CmdTemplate, sshFs fs.SshFS,
    baseVolumes map[string]string, tmpOutputHostPath string,
) (map[string]string, error) {
    var volumes map[string]string
    if cmdTemplate.Version == 0 {
        volumes = maps.Clone(baseVolumes)
    } else {
        volumes = make(map[string]string)
    }

    if err := connMan.mkdirIfNotExists(tmpOutputHostPath); err != nil {
        return nil, err
    }
    volumes["/tmp/output"] = tmpOutputHostPath

    if cmdTemplate.Version == 1 {
        for cntPath, hostPath := range cmdTemplate.VolumeDirs {
            remotePath, ok := fs.GetHostPath(hostPath, sshFs.RemoteVolumes)
            volumes[cntPath] = remotePath
            if !ok {
                return nil, fmt.Errorf(
                    "could not translate cnt path %s w/ volumes %v",
                    remotePath, sshFs.RemoteVolumes,
                )
            }
            if err := connMan.mkdirIfNotExists(remotePath); err != nil {
                return nil, err
            }
        }

        for cntPath, hostPath := range cmdTemplate.VolumeFiles {
            remotePath, ok := fs.GetHostPath(hostPath, sshFs.RemoteVolumes)
            volumes[cntPath] = remotePath
            if !ok {
                return nil, fmt.Errorf(
                    "could not translate cnt path %s w/ volumes %v",
                    remotePath, sshFs.RemoteVolumes,
                )
            }
            dir := filepath.Dir(remotePath)
            if err := connMan.mkdirIfNotExists(dir); err != nil {
                return nil, err
            }
        }
    }
    return volumes, nil
}

func (connMan *SlurmActivity) StartRemoteSlurmJobActivity(
    cmd parsing.CmdTemplate, jobConfig parsing.SlurmJobConfig, 
    fs fs.SshFS, schedDir string,
) (SlurmJob, error) {
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

    baseVolumes := fs.GetVolumes()
    tmpOutputHostPath := filepath.Join(schedDir, randomString(16))
    volumes, err := connMan.getSlurmVolumes(cmd, fs, baseVolumes, tmpOutputHostPath)
    if err != nil {
        return SlurmJob{}, fmt.Errorf("error setting up volumes: %s", err)
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

func readRemoteFile(outFile string, runCmd CmdRunner) (string, error) {
    if !AwaitFileExistence(outFile, runCmd) {
        return "", fmt.Errorf("%s does not exist", outFile)
    }

    catOutFileCmd := fmt.Sprintf("cat %s", outFile)
    catStdoutOut, err := runCmd(catOutFileCmd)
    if err != nil {
        return "", fmt.Errorf(
            "\"%s\" failed with exit code %d, error %s, and stderr %s",
            catOutFileCmd, catStdoutOut.ExitCode, err, catStdoutOut.StdErr,
        )
    }
    return catStdoutOut.StdOut, nil
}

func GetSlurmJobOutputs(job SlurmJob, runCmd CmdRunner) (CmdOutput, error) {
    cmdOutput := CmdOutput{
        Id:          job.CmdId,
        RawOutputs:  make(map[string]string),
        OutputFiles: make([]string, 0),
    }

    baseErrStr := fmt.Sprintf(
        "failed getting outputs for job ID %s (cmd ID %d)",
        job.JobId, job.CmdId,
    )
    
    cleanupCmd := fmt.Sprintf(
        "rm -rf %s %s %s", job.OutPath, job.ErrPath, job.TmpOutputHostPath,
    )
    defer runCmd(cleanupCmd)

    var err error
    if cmdOutput.StdOut, err = readRemoteFile(job.OutPath, runCmd); err != nil {
        return CmdOutput{}, fmt.Errorf("%s: %s", baseErrStr, err)
    }

    if cmdOutput.StdErr, err = readRemoteFile(job.ErrPath, runCmd); err != nil {
        return CmdOutput{}, fmt.Errorf("%s: %s", baseErrStr, err)
    }

    lsCmd := fmt.Sprintf("ls -1 %s", job.TmpOutputHostPath)
    lsOut, err := runCmd(lsCmd)
    if err != nil {
        return CmdOutput{}, fmt.Errorf(
            "\"%s\" failed with exit code %d, error %s, and stderr %s",
            lsCmd, lsOut.ExitCode, err, lsOut.StdErr,
        )
    }

    files := strings.Split(strings.TrimSpace(string(lsOut.StdOut)), "\n")
    rawOutputs := make(map[string]string)
    for _, file := range files {
        if file == "" {
            continue
        }

        outFilePath := filepath.Join(job.TmpOutputHostPath, file)
        outVal, err := readRemoteFile(outFilePath, runCmd)
        if err != nil {
            return CmdOutput{}, fmt.Errorf("%s: %s", baseErrStr, err)
        }
        rawOutputs[file] = outVal
    }

    cleanedOutputs, newOutFiles := processRawCmdOutputs(
        rawOutputs, job.ExpOutFilePnames,
    )
    cmdOutput.RawOutputs = cleanedOutputs
    cmdOutput.OutputFiles = append(cmdOutput.OutputFiles, newOutFiles...)
    return cmdOutput, nil
}

func ProcessSacctResult(
    ctx workflow.Context, selector workflow.Selector,
    state *SlurmState, results map[string]SacctResult,
) {
    finishedJobs := make([]SlurmJob, 0)
    resultsByCmdId := make(map[int]SacctResult)
    for jobId, result := range results {
        logger := workflow.GetLogger(ctx)
        job, jobExists := state.RunningJobs[jobId]
        if !jobExists {
            logger.Warn(
                "job ID (returned by sacct) is not a running job", "jobId", jobId,
            )
            return
        }

        if JOB_CODES[result.State].done {
            if JOB_CODES[result.State].failed {
                delete(state.RunningJobs, jobId)
                if JOB_CODES[result.State].fatal {
                    finishedJobs = append(finishedJobs, job)
                } else {
                    req := state.Requests[job.CmdId]
                    maxJobRetries := 0
                    if req.Config.MaxRetries != nil {
                        maxJobRetries = *req.Config.MaxRetries
                    }
                    if state.NumRetries[job.CmdId] < maxJobRetries {
                        state.NumRetries[job.CmdId] += 1
                        StartSlurmJob(req, ctx, state)
                    } else {
                        finishedJobs = append(finishedJobs, job)
                    }
                }
            } else {
                // There is a weird bug in SLURM (possibly unique to the slurm docker
                // setup I use for testing) where a job will register as complete 
                // immediately after submission, switch to PENDING/RUNNING, and then
                // show as complete later. We prevent that by registering a job as
                // possibly complete once it has shown up as COMPLETED once in sacct
                // and treating it as truly complete the next time it shows as 
                // COMPLETED.
                _, possiblyCompleted := state.PossiblyCompletedJobs[jobId]
                if possiblyCompleted {
                    finishedJobs = append(finishedJobs, job)
                    delete(state.RunningJobs, jobId)
                } else {
                    state.PossiblyCompletedJobs[jobId] = struct{}{}
                }
            }
        } else {
        }
        resultsByCmdId[job.CmdId] = result
    }

    if len(finishedJobs) == 0 {
        return
    }

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
    f := workflow.ExecuteActivity(childCtx, a.GetRemoteSlurmJobOutputsActivity, finishedJobs)

    slurmBatchId := state.MaxSlurmBatchId
    callback := func(f workflow.Future) {
        NotifyCmdCompletion(ctx, state, slurmBatchId, resultsByCmdId, f)
    }
    state.GetOutputFutures[slurmBatchId] = GetOutputsFuture{
        future: f,
        results: results,
        callback: callback,
    }
    state.MaxSlurmBatchId++
    selector.AddFuture(f, callback)
}

func NotifyCmdCompletion(
    ctx workflow.Context, state *SlurmState, outputFutInd int,
    resultsByCmdId map[int]SacctResult, f workflow.Future,
) {
    var outputs []CmdOutput
    err := f.Get(ctx, &outputs)
    delete(state.GetOutputFutures, outputFutInd)
    if err != nil {
        state.FinalErr = err
        return
    }

    for _, output := range outputs {
        result, ok := resultsByCmdId[output.Id]
        if !ok {
            state.FinalErr = fmt.Errorf(
                "error notifying of cmd completion; output %v not in " +
                "resultsByCmd map", output,
            )
            return
        }
        if JOB_CODES[result.State].failed {
            jobErr := fmt.Sprintf("job failed with err %s", output.StdErr)
            workflow.SignalExternalWorkflow(
                ctx, state.ParentWfId, "", "slurm-response", 
                SlurmResponse{ Result: output, Error:  &jobErr },
            )
        } else {
            workflow.SignalExternalWorkflow(
                ctx, state.ParentWfId, "", "slurm-response", 
                SlurmResponse{ Result: output, Error: nil },
            )
        }
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

    ProcessSacctResult(ctx, selector, state, jobRes)
}

func SlurmPollerWorkflow(ctx workflow.Context, state SlurmState) error {
    // These should be empty at start of each workflow incarnation.
    state.FinalErr = nil
    state.GetOutputFutures = make(map[int]GetOutputsFuture)

    // These will be emtpy in initial incarnation but full after continue-as-new.
    if state.RunningJobs == nil {
        state.RunningJobs = make(map[string]SlurmJob)
    }
    if state.PossiblyCompletedJobs == nil {
        state.PossiblyCompletedJobs = make(map[string]struct{})
    }
    if state.NumRetries == nil {
        state.NumRetries = make(map[int]int)
    }
    if state.Requests == nil {
        state.Requests = make(map[int]SlurmRequest)
    }

    selector := workflow.NewSelector(ctx)

    slurmReqChan := workflow.GetSignalChannel(ctx, "slurm-request")
    selector.AddReceive(slurmReqChan, func(c workflow.ReceiveChannel, _ bool) {
        var pendingReq SlurmRequest
        c.Receive(ctx, &pendingReq)
        state.Requests[pendingReq.Cmd.Id] = pendingReq
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
    for slurmBatchInd, f := range state.GetOutputFutures {
        f.callback(f.future)
        delete(state.GetOutputFutures, slurmBatchInd)
    }
    state.GetOutputFutures = nil 

    return workflow.NewContinueAsNewError(ctx, SlurmPollerWorkflow, state)
}

