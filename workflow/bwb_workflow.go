package workflow

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"go-scheduler/parsing"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

type CmdOutput struct {
    StdOut      string
    StdErr      string
    RawOutputs  map[string]string
    OutputFiles []string
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
        if outFile, outFileExists := outKvs[expOutFilePname]; outFileExists {
            outFiles = append(outFiles, outFile)
        }
    }
    
    return outKvs, outFiles, nil
}

func setupCmdDirs(storageId string) (map[string]string, string, error) {
    schedDir := os.Getenv("BWB_SCHED_DIR")
    randStr := randomString(32)
    tmpDir := filepath.Join(schedDir, randStr)
    imageDir := filepath.Join(schedDir, "images")
    dataDir := filepath.Join(schedDir, storageId)
    
    for _, dir := range []string{dataDir, tmpDir, imageDir} {
	    if err := os.MkdirAll(dir, 0755); err != nil {
	    	return nil, "", fmt.Errorf("failed to create dir %s: %s", dir, err)
	    }
    }

    volumes := make(map[string]string)
    volumes["/data"] = dataDir
    volumes["/tmp/output"] = tmpDir
    return volumes, imageDir, nil
}

func RunCmd(
    storageId string,
    cmdTemplate parsing.CmdTemplate, 
) (CmdOutput, error) {
	if _, err := exec.LookPath("singularity"); err != nil {
		return CmdOutput{}, fmt.Errorf("singularity not found in PATH: %v", err)
	}

    volumes, imageDir, err := setupCmdDirs(storageId)
    tmpDir := volumes["/tmp/output"]
    defer os.RemoveAll(tmpDir)

    if err != nil {
        return CmdOutput{}, fmt.Errorf("error making volumes: %s", err)
    }
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
    //fmt.Printf("\n%s\n", cmdWithEnvStr)

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

func getResourceGrant(req parsing.ResourceVector) ResourceGrant {
    return ResourceGrant{}
}

func releaseResourceGrant(grant ResourceGrant) error {
    return nil
}

func startCmds(
    ctx workflow.Context,
    selector workflow.Selector,
    state *parsing.WorkflowExecutionState,
    storageId string,
    cmds []parsing.CmdTemplate,
    finalErr *error,
    finished *bool,
) {
    ao := workflow.ActivityOptions{
        TaskQueue: "bwb_worker",
        StartToCloseTimeout: 3*time.Hour,
        RetryPolicy: &temporal.RetryPolicy{
            MaximumAttempts: 1,
        },
    }
    cmdCtx := workflow.WithActivityOptions(ctx, ao)

    for _, cmd := range cmds {
        fmt.Printf("starting cmd for %d\n", cmd.NodeId)
        grant := getResourceGrant(cmd.ResourceReqs)
        cmdFuture := workflow.ExecuteActivity(
            cmdCtx, RunCmd, storageId, cmd,
        )
        selector.AddFuture(cmdFuture, func(f workflow.Future) {
            handleCompletedCmd(
                cmdFuture, cmdCtx, selector, state, 
                storageId, cmd, grant, finalErr, finished,
            )
        })

    }
}

func handleCompletedCmd(
    f workflow.Future, 
    ctx workflow.Context,
    selector workflow.Selector,
    state *parsing.WorkflowExecutionState,
    storageId string,
    completedCmd parsing.CmdTemplate,
    grant ResourceGrant,
    finalErr *error,
    finished *bool,
) {
    fmt.Printf("completed cmd for node %d\n", completedCmd.NodeId)
    var result CmdOutput
    if err := f.Get(ctx, &result); err != nil {
        *finished = true
        *finalErr = err
        return
    }

    // TODO: handle iterables, which produce multiple outputs.
    succCmds, err := state.GetSuccCmds(completedCmd, result.RawOutputs)

    if err != nil {
        *finished = true
        *finalErr = err
        return
    }

    startCmds(ctx, selector, state, storageId, succCmds, finalErr, finished)

    if err := releaseResourceGrant(grant); err != nil {
        *finalErr = err
        return
    }

    complete, err := state.IsComplete()
    if err != nil {
        *finished = true
        *finalErr = err
    } else if complete {
        *finished = true
        finalErr = nil
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

func RunBwbWorkflow(
    ctx workflow.Context, 
    storageId string,
    bwbWorkflow parsing.Workflow, 
    index parsing.WorkflowIndex,
) (error) {
    state := parsing.NewWorkflowExecutionState(bwbWorkflow, index)
    imageNames := state.GetImageNames()
    if err := buildImages(ctx, imageNames); err != nil {
        return err
    }

    initialCmds, err := state.GetInitialCmds()
    if err != nil {
        return fmt.Errorf("error reading initial cmds: %s", err)
    }
    if len(initialCmds) == 0 {
        return nil
    }


    var finished bool
    var finalErr error
    selector := workflow.NewSelector(ctx)
    startCmds(ctx, selector, &state, storageId, initialCmds, &finalErr, &finished)
    for !finished {
        selector.Select(ctx)
    }

    return finalErr
}