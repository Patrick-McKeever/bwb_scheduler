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
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

const (
    SCHEDULER_QUEUE = "bwb_worker"
)

type CmdOutput struct {
    StdOut      string
    StdErr      string
    RawOutputs  map[string]string
    OutputFiles []string
}

type BwbWorkflowState struct {
    ctx         workflow.Context
    selector    workflow.Selector
    schedulerWE workflow.Execution
    cmdMan      *parsing.CmdManager
    masterFS    fs.LocalFS
    workerFSs   map[string]fs.LocalFS
    storageId   string
    cmdsById    map[int]parsing.CmdTemplate
    grantsById  map[int]ResourceGrant
    finalErr    *error
    finished    *bool
    softFail    bool
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

func releaseResourceGrant(state BwbWorkflowState, grant ResourceGrant) error {
    workflow.SignalExternalWorkflow(
        state.ctx, state.schedulerWE.ID, state.schedulerWE.RunID,
        "release-allocation", grant,
    )
    return nil
}

func awaitResourceGrants(state BwbWorkflowState, cmds []parsing.CmdTemplate) {
    // TODO: Implement rank
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

func runCmdWithGrant(
    state BwbWorkflowState,
    cmd parsing.CmdTemplate,
    grant ResourceGrant,
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
        *state.finalErr = fmt.Errorf("worker %s has no FS", grant.WorkerId)
        *state.finished = true
        return
    }
    volumes := fs.GetVolumes()

    cmdCtx := workflow.WithActivityOptions(state.ctx, ao)
    cmdFuture := workflow.ExecuteActivity(
        cmdCtx, RunCmd, volumes, cmd,
    )
    state.selector.AddFuture(cmdFuture, func(f workflow.Future) {
        handleCompletedCmd(cmdFuture, state, cmd, grant)
    })
}

func globWorkerFS(
    state BwbWorkflowState, 
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

func handleCompletedCmd(
    f workflow.Future, 
    state BwbWorkflowState,
    completedCmd parsing.CmdTemplate,
    grant ResourceGrant,
) {
    fmt.Printf("Completed cmd for node %d\n", completedCmd.NodeId)
    var result CmdOutput
    err := f.Get(state.ctx, &result)
    if err != nil {
        *state.finalErr = err
        if !state.softFail {
            *state.finished = true
            return
        }
    }

    succCmds, err := state.cmdMan.GetSuccCmds(
        completedCmd, result.RawOutputs, 
        func(root, pattern string, findFile, findDir bool) ([]string, error) {
            return globWorkerFS(state, root, pattern, findFile, findDir)
        }, err != nil,
    )

    if err != nil {
        *state.finished = true
        *state.finalErr = err
        return
    }

    if err := releaseResourceGrant(state, grant); err != nil {
        *state.finalErr = err
        return
    }

    awaitResourceGrants(state, succCmds)

    failed := state.cmdMan.HasFailed()
    complete := state.cmdMan.IsComplete()
    if failed && !state.softFail {

    }
    if complete {
        *state.finished = true
        state.finalErr = nil
    }
}

func buildImages(ctx workflow.Context, imageNames []string) error {
    ao := workflow.ActivityOptions{
        TaskQueue: "bwb_worker",
        StartToCloseTimeout: 10*time.Minute,
        RetryPolicy: &temporal.RetryPolicy{
            MaximumAttempts: 1,
        },
    }
    cmdCtx := workflow.WithActivityOptions(ctx, ao)
    for _, imageName := range imageNames {
        err := workflow.ExecuteActivity(cmdCtx, BuildSingularitySIF, imageName).Get(ctx, nil)
        if err != nil {
            return err
        }
    }
    return nil
}

func getChildSchedWorkflow(
    ctx workflow.Context, workers map[string]WorkerInfo,
) (workflow.Execution, func(), error) {
    childCtx, cancelChild := workflow.WithCancel(ctx)
    schedChildWfOptions := workflow.ChildWorkflowOptions{
        WorkflowID: "sched-workflow",
        TaskQueue: SCHEDULER_QUEUE,
    }
    childCtx = workflow.WithChildOptions(childCtx, schedChildWfOptions)
    schedChildWfFuture := workflow.ExecuteChildWorkflow(
        childCtx, ResourceSchedulerWorkflow, SchedWorkflowState{
            Workers: workers,
        },
    )
    var schedChildWE workflow.Execution
    err := schedChildWfFuture.GetChildWorkflowExecution().Get(ctx, &schedChildWE)
    if err != nil {
        outErr := fmt.Errorf("failed getting child WF execution: %s", err)
        return workflow.Execution{}, nil, outErr
    }
    return schedChildWE, cancelChild, nil
}

func setupWorkerFSs(
    ctx workflow.Context, storageID string,
    workers map[string]WorkerInfo,
) (map[string]fs.LocalFS, error) {
    ret := make(map[string]fs.LocalFS)
    for queueId := range workers {
        ao := workflow.ActivityOptions{
            TaskQueue: queueId,
            StartToCloseTimeout: 1*time.Minute,
            RetryPolicy: &temporal.RetryPolicy{
                MaximumAttempts: 1,
            },
        }
        cmdCtx := workflow.WithActivityOptions(ctx, ao)

        var volumes map[string]string
        err := workflow.ExecuteActivity(
            cmdCtx, SetupVolumes, storageID,
        ).Get(ctx, &volumes)

        if err != nil {
            return nil, fmt.Errorf(
                "error setting up cmd dirs on worker %s: %s",
                queueId, err,
            )
        }

        ret[queueId] = fs.LocalFS{ Volumes: volumes }
    }
    return ret, nil
}

func RunBwbWorkflow(
    ctx workflow.Context, 
    storageId string,
    bwbWorkflow parsing.Workflow, 
    index parsing.WorkflowIndex,
    workers map[string]WorkerInfo,
    masterFS fs.LocalFS,
    softFail bool,
) (error) {
    logger := workflow.GetLogger(ctx)
    logger.Info("Beginning workflow")

    // It's a good idea to verify that initial commands can be correctly
    // formed before scheduling any activities or IO, so as to fail quickly.
    cmdMan := parsing.NewCmdManager(bwbWorkflow, index)
    imageNames := cmdMan.GetImageNames()
    if err := buildImages(ctx, imageNames); err != nil {
        return err
    }

    workerFSs, err := setupWorkerFSs(ctx, storageId, workers)
    if err != nil {
        return err
    }

    schedChildWE, cancelChild, err := getChildSchedWorkflow(ctx, workers)
    if err != nil {
        return err
    }

    var finished bool
    var finalErr error
    selector := workflow.NewSelector(ctx)

    wfState := BwbWorkflowState{
        ctx: ctx,
        selector: selector,
        schedulerWE: schedChildWE,
        cmdMan: &cmdMan,
        storageId: storageId,
        workerFSs: workerFSs,
        masterFS: masterFS,
        grantsById: make(map[int]ResourceGrant),
        cmdsById: make(map[int]parsing.CmdTemplate),
        finalErr: &finalErr,
        finished: &finished,
    }

    rGrantChan := workflow.GetSignalChannel(ctx, "allocation-response")
    selector.AddReceive(rGrantChan, func(c workflow.ReceiveChannel, _ bool) {
        var grant ResourceGrant
        c.Receive(ctx, &grant)
        cmd := wfState.cmdsById[grant.RequestId]
        runCmdWithGrant(wfState, cmd, grant)
    })

    initialCmds, err := cmdMan.GetInitialCmds(
        func(root, pattern string, findFile, findDir bool) ([]string, error) {
            return globWorkerFS(wfState, root, pattern, findFile, findDir)
        },
    )
    if err != nil {
        return fmt.Errorf("error reading initial cmds: %s", err)
    }
    if len(initialCmds) == 0 {
        return nil
    }

    awaitResourceGrants(wfState, initialCmds)
    for ! *wfState.finished {
        selector.Select(ctx)
    }

    cancelChild()
    return *wfState.finalErr
}