package workflow

import (
    "fmt"
    "go-scheduler/fs"
    "go-scheduler/parsing"
    "time"

    "go.temporal.io/sdk/temporal"
    "go.temporal.io/sdk/workflow"
)

type TemporalExecutor struct {
    ctx               workflow.Context
    handleFinishedCmd CmdHandler
    masterFS          fs.LocalFS
    workers           map[string]WorkerInfo
    storageId         string
    selector          *workflow.Selector
    schedulerWE       workflow.Execution
    cancelChild       func()
    workerFSs         map[string]fs.LocalFS
    cmdsById          map[int]parsing.CmdTemplate
    grantsById        map[int]ResourceGrant
    errors            []error
}

func NewTemporalExecutor(
    ctx workflow.Context, selector *workflow.Selector, 
    cmdMan *parsing.CmdManager, masterFS fs.LocalFS, 
    workers map[string]WorkerInfo, storageId string,
) TemporalExecutor {
    var state TemporalExecutor
    state.ctx = ctx
    state.selector = selector
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

    rGrantChan := workflow.GetSignalChannel(exec.ctx, "allocation-response")
    (*exec.selector).AddReceive(rGrantChan, func(c workflow.ReceiveChannel, _ bool) {
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
    (*exec.selector).Select(exec.ctx)
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
    (*exec.selector).AddFuture(cmdFuture, func(f workflow.Future) {
        var result CmdOutput
        err := f.Get(exec.ctx, &result)
        if grantErr := exec.ReleaseResourceGrant(grant); grantErr != nil {
            logger := workflow.GetLogger(exec.ctx)
            logger.Error(
                "failed to release resource grant", "resourceGrant", grant,
                "error", err,
            )
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