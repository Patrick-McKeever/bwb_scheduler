package workflow

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"go-scheduler/fs"
	"go-scheduler/parsing"
	"log/slog"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/log"
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

type CmdHandler func(CmdOutput, error, Executor, parsing.CmdTemplate)

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


type CmdRunner func(string) (CmdOut, error)

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

func processRawCmdOutputs(
    rawOutputs map[string]string,
    expOutFilePnames []string,
) (map[string]string, []string) {
    outKvs := make(map[string]string)
    for pname, contents := range rawOutputs {
        outKvs[pname] = strings.TrimSuffix(contents, "\n")
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
    return outKvs, outFiles
}

func dockerGetCmdOutputs(
    dockerPrefix string, expOutFilePnames []string,
) (map[string]string, []string, error) {
    var stdout bytes.Buffer
    var stderr bytes.Buffer
    findCmdStr := fmt.Sprintf(
        "%s find /tmp/output -maxdepth 1 -type f", dockerPrefix,
    )
    findCmd := exec.Command("sh", "-c", findCmdStr)
    findCmd.Stdout = &stdout
    findCmd.Stderr = &stderr
    if err := findCmd.Run(); err != nil {
        return nil, nil, fmt.Errorf(
            "cmd %s failed w/ stderr %s and err %s", 
            findCmdStr, stderr.String(), err,
        )
    }

    files := strings.Split(strings.TrimSpace(stdout.String()), "\n")
    rawOutputs := make(map[string]string)
    for _, outFile := range files {
        if outFile == "" {
            continue
        }

        stdout.Reset()
        stderr.Reset()
        catCmdStr := fmt.Sprintf("%s cat %s", dockerPrefix, outFile)
        catCmd := exec.Command("sh", "-c", catCmdStr)
        catCmd.Stdout = &stdout
        catCmd.Stderr = &stderr
        if err := catCmd.Run(); err != nil {
            return nil, nil, fmt.Errorf(
                "cmd %s failed w/ stderr %s and err %s", 
                catCmdStr, stderr.String(), err,
            )
        }

        pname := filepath.Base(outFile)
        rawOutputs[pname] = stdout.String()
    }

    outKvs, outFiles := processRawCmdOutputs(rawOutputs, expOutFilePnames)
    return outKvs, outFiles, nil
}

func singularityGetCmdOutputs(
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

    rawOutputs := make(map[string]string)
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

        rawOutputs[pname] = string(data)
    }

    outKvs, outFiles := processRawCmdOutputs(rawOutputs, expOutFilePnames)
    return outKvs, outFiles, nil
}

func setupTmpDir() (string, error) {
    schedDir := os.Getenv("BWB_SCHED_DIR")
    randStr := randomString(32)
    tmpDir := filepath.Join(schedDir, randStr)

    if err := os.MkdirAll(tmpDir, 0755); err != nil {
        return "", fmt.Errorf("failed to create dir %s: %s", tmpDir, err)
    }

    return tmpDir, nil
}

func setupImageDir() (string, error) {
    schedDir := os.Getenv("BWB_SCHED_DIR")
    imageDir := filepath.Join(schedDir, "images")

    if err := os.MkdirAll(imageDir, 0755); err != nil {
        return "", fmt.Errorf("failed to create dir %s: %s", imageDir, err)
    }

    return imageDir, nil
}

func runCmdDocker(
    ctx context.Context, 
    volumes map[string]string, 
    cmdTemplate parsing.CmdTemplate,
) (CmdOutput, error) {
    useGpu := cmdTemplate.ResourceReqs.Gpus > 0
    cntName := randomString(16)
    cmdStr, envs := parsing.FormDockerCmd(
        cmdTemplate, volumes, useGpu, cntName,
    )

    var stdout, stderr bytes.Buffer
    cmd := exec.Command("sh", "-c", cmdStr)
    cmd.SysProcAttr = &syscall.SysProcAttr{ Setpgid: true }
    cmd.Env = envs
    cmd.Stdout = &stdout
    cmd.Stderr = &stderr

    cmdWithEnvStr := fmt.Sprintf("%s %s", strings.Join(envs, " "), cmdStr)
    fmt.Println(cmdWithEnvStr)

    dockerCmdPrefix := parsing.FormDockerCmdPrefix(
        cmdTemplate, volumes, useGpu, cntName,
    )
    rmTmpDirCmdStr := fmt.Sprintf("%s rm -rf /tmp/output", dockerCmdPrefix)
    rmTmpDirCmd := exec.Command("sh", "-c", rmTmpDirCmdStr)
    defer rmTmpDirCmd.Run()

    errChan := make(chan error)
    go func() {
        errChan <- cmd.Run()
    }()

    select {
    case <-ctx.Done(): {
        rmCntCmdStr := fmt.Sprintf("docker rm -f %s", cntName)
        rmCntCmd := exec.Command("sh", "-c", rmCntCmdStr)
        rmCntCmd.Run()
        return CmdOutput{}, context.Canceled
    }
    case procErr := <-errChan: {
        if procErr != nil {
            return CmdOutput{}, fmt.Errorf(
                "error running command %s: %v\nSTDOUT: %s\nSTDERR: %s",
                cmdWithEnvStr, procErr, stdout.String(), stderr.String(),
            )
        }
    }
    }

    out := CmdOutput{
        Id:     cmdTemplate.Id,
        StdOut: stdout.String(),
        StdErr: stderr.String(),
    }

    var err error
    out.RawOutputs, out.OutputFiles, err = dockerGetCmdOutputs(
        dockerCmdPrefix, cmdTemplate.OutFilePnames,
    )

    if err != nil {
        return CmdOutput{}, fmt.Errorf(
            "error getting outputs of command %s: %s", cmd, err,
        )
    }
    return out, nil
}

func runCmdSingularity(
    ctx context.Context,
    volumes map[string]string,
    cmdTemplate parsing.CmdTemplate,
) (CmdOutput, error) {
    imageDir, err := setupImageDir()
    if err != nil {
        return CmdOutput{}, fmt.Errorf("unable to setup image dir: %s", err)
    }

    tmpDir, ok := volumes["/tmp/output"]
    if !ok {
        return CmdOutput{}, fmt.Errorf("failed to set /tmp/output volume")
    }
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

    // Configure cmd to get killed automatically if ctx is cancelled.
    cmd := exec.CommandContext(ctx, "sh", "-c", cmdStr)
    var stdout, stderr bytes.Buffer
    cmd.Env = envs
    cmd.Stdout = &stdout
    cmd.Stderr = &stderr

    cmdWithEnvStr := fmt.Sprintf("%s %s", strings.Join(envs, " "), cmdStr)
    fmt.Println(cmdWithEnvStr)

    procErr := cmd.Run()
    if ctx.Err() != nil {
        return CmdOutput{}, context.Canceled
    }

    if procErr != nil {
        return CmdOutput{}, fmt.Errorf(
            "error running command %s: %v\nSTDOUT: %s\nSTDERR: %s",
            cmdWithEnvStr, procErr, stdout.String(), stderr.String(),
        )
    }

    out := CmdOutput{
        Id:     cmdTemplate.Id,
        StdOut: stdout.String(),
        StdErr: stderr.String(),
    }
    out.RawOutputs, out.OutputFiles, err = singularityGetCmdOutputs(
        tmpDir, cmdTemplate.OutFilePnames,
    )

    if err != nil {
        return CmdOutput{}, fmt.Errorf(
            "error getting outputs of command %s: %s", cmd, err,
        )
    }
    return out, nil
}

func RunCmd(
    ctx context.Context,
    volumes map[string]string, 
    cmdTemplate parsing.CmdTemplate,
    useDocker bool,
) (CmdOutput, error) {
    tmpDir, err := setupTmpDir()
    if err != nil {
        return CmdOutput{}, fmt.Errorf(
            "unable to setup /tmp/output dir: %s", err,
        )
    }

    volumesCopy := make(map[string]string)
    maps.Copy(volumesCopy, volumes)
    volumesCopy["/tmp/output"] = tmpDir
    if useDocker {
        return runCmdDocker(ctx, volumesCopy, cmdTemplate)
    } else {
        return runCmdSingularity(ctx, volumesCopy, cmdTemplate)
    }
}

func RunCmdActivity(
    ctx context.Context,
    volumes map[string]string, 
    cmdTemplate parsing.CmdTemplate,
    useDocker bool,
) (CmdOutput, error) {
    outChan := make(chan struct{out CmdOutput; err error})
    type outType struct{out CmdOutput; err error}

    go func ()  {
        out, err := RunCmd(ctx, volumes, cmdTemplate, useDocker)
        outChan <- outType{ out: out, err: err }
    }()

    finished := false
    var outVal outType
    for !finished {
        select {
        case <-time.After(1 * time.Second): {
            activity.RecordHeartbeat(ctx, struct{}{})
        }
        case outVal = <-outChan: {
            finished = true
        }
        }
    }
    return outVal.out, outVal.err
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

    selector := workflow.NewSelector(ctx)
    if len(jobConfig.TemporalConfigsByNode) > 0 {
        temporalExecutor := NewTemporalExecutor(
            ctx, &selector, cmdMan, masterFS, workers, storageId,
            jobConfig.TemporalConfigsByNode,
        )
        executorList = append(executorList, &temporalExecutor)
        for nodeId := range jobConfig.TemporalConfigsByNode {
            executors[nodeId] = &temporalExecutor
        }
    }

    if len(jobConfig.SlurmConfigsByNode) > 0 {
        slurmExecutor := NewSlurmRemoteExecutor(
            ctx, &selector, masterFS, storageId, jobConfig.SlurmConfigsByNode,
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
    isDone func() bool,
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
    for !cmdMan.IsComplete() && (!cmdMan.HasFailed() || softFail) && finalErr == nil && !isDone() {
        for _, executor := range executors {
            execErrs := executor.GetErrors()
            if len(execErrs) > 0 {
                errStr := ""
                for _, err := range execErrs {
                    errStr += fmt.Sprintf("\t%s\n", err.Error())
                }
                finalErr = errors.New(errStr)
                break
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
    return RunBwbWorkflowHelper(
        logger, &cmdMan, executors, executorList, softFail, func() bool {
            return ctx.Err() != nil
        })
}

func RunBwbWorkflowNoTemporal(
    ctx context.Context,
    storageId string,
    jobConfig parsing.JobConfig,
    bwbWorkflow parsing.Workflow,
    index parsing.WorkflowIndex,
    localWorker WorkerInfo,
    masterFS fs.LocalFS,
    softFail bool,
    logger slog.Logger,
) error {
    cmdMan := parsing.NewCmdManager(bwbWorkflow, index, jobConfig)
    executors := make(map[int]Executor)
    localExecutor := NewLocalExecutor(
        ctx, &cmdMan, masterFS, localWorker, storageId, 
        jobConfig.LocalConfigsByNode, logger,
    )
    executorList := []Executor{&localExecutor}
    for nodeId := range bwbWorkflow.Nodes {
        executors[nodeId] = &localExecutor
    }
    return RunBwbWorkflowHelper(
        &logger, &cmdMan, executors, executorList, softFail, func() bool {
            return ctx.Err() != nil
        },
    )
}
