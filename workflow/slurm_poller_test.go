package workflow

import (
    "errors"
    "go-scheduler/parsing"
    "testing"
    "time"

    "github.com/stretchr/testify/mock"
    "github.com/stretchr/testify/require"
    "go.temporal.io/sdk/converter"
    "go.temporal.io/sdk/testsuite"
    "go.temporal.io/sdk/workflow"
)

// Test the lifecycle of a successful SLURM job.
//  1. The parent workflow submits a job to the slurm poller child WF.
//  2. The child poller WF submits that job to the SLURM cluster.
//  3. The child poller WF polls SLURM.
//  4. The child poller WF gets the job outputs *only after* the job
//     registers as COMPLETED (not when it is RUNNING or PENDING).
//  5. The child poller WF signals the outputs to the caller WF.
func TestSlurmResponse(t *testing.T) {
    var a SlurmActivity
    testSuite := &testsuite.WorkflowTestSuite{}
    env := testSuite.NewTestWorkflowEnvironment()
    env.RegisterActivity(a.StartRemoteSlurmJobActivity)
    env.RegisterActivity(a.PollRemoteSlurmActivity)
    env.RegisterActivity(a.GetRemoteSlurmJobOutputsActivity)

    jobId := "2718281828459"
    cmdId := 1

    memStr := "40G"
    expConfig := parsing.SlurmJobConfig{Mem: &memStr}
    expCmd := parsing.CmdTemplate{Id: cmdId}
    expSlurmJob := SlurmJob{CmdId: cmdId, JobId: jobId}

    env.OnActivity(
        a.StartRemoteSlurmJobActivity,
        expCmd, expConfig, mock.Anything, mock.Anything,
    ).Return(expSlurmJob, nil).Once()

    // Handle workflow polling before request, which should be empty.
    env.OnActivity(a.PollRemoteSlurmActivity, []string{}).Return(nil, nil)

    stateBeforePoll := ""
    stateAfterPoll := "PENDING"
    env.OnActivity(a.PollRemoteSlurmActivity, []string{jobId}).
        Return(func(jobIds []string) (map[string]SacctResult, error) {
            require.NotEqual(t, stateBeforePoll, "COMPLETED")
            stateBeforePoll = stateAfterPoll
            switch stateAfterPoll {
            case "PENDING":
                stateAfterPoll = "RUNNING"
            case "RUNNING":
                stateAfterPoll = "COMPLETED"
            }
            return map[string]SacctResult{jobId: {JobId: jobId, State: stateBeforePoll}}, nil
        })

    expCmdOutput := CmdOutput{Id: cmdId, StdOut: "stdout", StdErr: "stderr"}
    env.OnActivity(a.GetRemoteSlurmJobOutputsActivity, []SlurmJob{expSlurmJob}).
        Return([]CmdOutput{expCmdOutput}, nil)

    expParentWfId := "parentId"
    expParentWfRunId := "parentRunId"
    env.OnSignalExternalWorkflow(
        mock.Anything, expParentWfId, "", "slurm-response",
        mock.MatchedBy(func(arg interface{}) bool {
            require.True(t, stateBeforePoll == "COMPLETED")
            resp, ok := arg.(SlurmResponse)
            return ok && resp.Result.Id == expCmdOutput.Id && resp.Result.StdOut == expCmdOutput.StdOut && resp.Result.StdErr == expCmdOutput.StdErr
        }),
    ).Return(nil).Once()

    env.RegisterDelayedCallback(func() {
        env.SignalWorkflow("slurm-request", SlurmRequest{Cmd: expCmd, Config: expConfig})
    }, 0)
    env.RegisterDelayedCallback(func() {
        env.SetCurrentHistoryLength(9500)
    }, 30*time.Second)
    env.ExecuteWorkflow(SlurmPollerWorkflow, SlurmState{
        ParentWfId:    expParentWfId,
        ParentWfRunId: expParentWfRunId,
    })

    env.AssertExpectations(t)
    require.True(t, env.IsWorkflowCompleted())
}

// Test that, after workflow continues-as-new, it retains its
// list of outstanding jobs and continues polling for them,
// eventually sending signal back to calling workflow.
func TestSlurmContinueAsNewStateMaintenance(t *testing.T) {
    var a SlurmActivity
    testSuite := &testsuite.WorkflowTestSuite{}
    env := testSuite.NewTestWorkflowEnvironment()
    env.RegisterActivity(a.StartRemoteSlurmJobActivity)
    env.RegisterActivity(a.PollRemoteSlurmActivity)
    env.RegisterActivity(a.GetRemoteSlurmJobOutputsActivity)

    jobId := "2718281828459"
    cmdId := 1

    memStr := "40G"
    expConfig := parsing.SlurmJobConfig{Mem: &memStr}
    expCmd := parsing.CmdTemplate{Id: cmdId}
    expSlurmJob := SlurmJob{CmdId: cmdId, JobId: jobId}

    env.OnActivity(
        a.StartRemoteSlurmJobActivity,
        expCmd, expConfig, mock.Anything, mock.Anything,
    ).Return(expSlurmJob, nil).Once()

    // Handle workflow polling before request, which should be empty.
    env.OnActivity(a.PollRemoteSlurmActivity, []string{}).Return(nil, nil).Maybe()

    env.OnActivity(a.PollRemoteSlurmActivity, []string{jobId}).
        Return(func(jobIds []string) (map[string]SacctResult, error) {
            return map[string]SacctResult{jobId: {JobId: jobId, State: "RUNNING"}}, nil
        })

    expParentWfId := "parentId"

    env.RegisterDelayedCallback(func() {
        env.SignalWorkflow("slurm-request", SlurmRequest{Cmd: expCmd, Config: expConfig})
    }, 0)
    env.RegisterDelayedCallback(func() {
        env.SetCurrentHistoryLength(9500)
    }, 30*time.Second)
    env.ExecuteWorkflow(SlurmPollerWorkflow, SlurmState{
        ParentWfId:    expParentWfId,
        ParentWfRunId: "",
    })
    env.AssertExpectations(t)
    err := env.GetWorkflowError()

    var caeErr *workflow.ContinueAsNewError
    if ok := errors.As(err, &caeErr); !ok {
        t.Fatalf("failed to gen continue-as-new error")
    }

    var continuedWfState SlurmState
    if err := converter.GetDefaultDataConverter().FromPayloads(caeErr.Input, &continuedWfState); err != nil {
        t.Fatalf("failed to convert continue-as-new input")
    }

    contEnv := testSuite.NewTestWorkflowEnvironment()
    contEnv.OnActivity(a.PollRemoteSlurmActivity, []string{jobId}).
        Return(func(jobIds []string) (map[string]SacctResult, error) {
            return map[string]SacctResult{jobId: {JobId: jobId, State: "COMPLETED"}}, nil
        })

    expCmdOutput := CmdOutput{Id: cmdId, StdOut: "stdout", StdErr: "stderr"}
    contEnv.OnActivity(a.PollRemoteSlurmActivity, []string{}).Return(nil, nil)
    contEnv.OnActivity(a.GetRemoteSlurmJobOutputsActivity, []SlurmJob{expSlurmJob}).
        Return([]CmdOutput{expCmdOutput}, nil)

    contEnv.OnSignalExternalWorkflow(
        mock.Anything, expParentWfId, "", "slurm-response",
        mock.MatchedBy(func(arg interface{}) bool {
            resp, ok := arg.(SlurmResponse)
            return ok && resp.Result.Id == expCmdOutput.Id && resp.Result.StdOut == expCmdOutput.StdOut && resp.Result.StdErr == expCmdOutput.StdErr
        }),
    ).Return(nil).Once()
    contEnv.RegisterDelayedCallback(func() {
        contEnv.SetCurrentHistoryLength(9500)
    }, 30*time.Second)
    contEnv.ExecuteWorkflow(SlurmPollerWorkflow, continuedWfState)

    contEnv.AssertExpectations(t)
    require.True(t, env.IsWorkflowCompleted())
}

// If a slurm job fails fatally (i.e. FAILED or CANCELLED), it
// should immediately send notice of the job failure to the
// calling workflow without any retries.
func TestSlurmJobFatalFailure(t *testing.T) {
    var a SlurmActivity
    testSuite := &testsuite.WorkflowTestSuite{}
    env := testSuite.NewTestWorkflowEnvironment()
    env.RegisterActivity(a.StartRemoteSlurmJobActivity)
    env.RegisterActivity(a.PollRemoteSlurmActivity)
    env.RegisterActivity(a.GetRemoteSlurmJobOutputsActivity)

    jobId := "2718281828459"
    cmdId := 1

    memStr := "40G"
    maxRetries := 2
    expConfig := parsing.SlurmJobConfig{Mem: &memStr, MaxRetries: &maxRetries}
    expCmd := parsing.CmdTemplate{Id: cmdId}
    expSlurmJob := SlurmJob{CmdId: cmdId, JobId: jobId}

    env.OnActivity(
        a.StartRemoteSlurmJobActivity,
        expCmd, expConfig, mock.Anything, mock.Anything,
    ).Return(expSlurmJob, nil).Once()

    // Handle workflow polling before request, which should be empty.
    env.OnActivity(a.PollRemoteSlurmActivity, []string{}).Return(nil, nil).Maybe()

    env.OnActivity(a.PollRemoteSlurmActivity, []string{jobId}).
        Return(map[string]SacctResult{jobId: {JobId: jobId, State: "FAILED"}}, nil)

    expCmdOutput := CmdOutput{Id: cmdId, StdOut: "stdout", StdErr: "stderr"}
    env.OnActivity(a.GetRemoteSlurmJobOutputsActivity, []SlurmJob{expSlurmJob}).
        Return([]CmdOutput{expCmdOutput}, nil)

    expParentWfId := "parentId"
    expParentWfRunId := "parentRunId"
    env.OnSignalExternalWorkflow(
        mock.Anything, expParentWfId, "", "slurm-response",
        mock.MatchedBy(func(arg interface{}) bool {
            resp, ok := arg.(SlurmResponse)
            return ok && resp.Error != nil && resp.Result.Id == expCmdOutput.Id &&
                resp.Result.StdOut == expCmdOutput.StdOut &&
                resp.Result.StdErr == expCmdOutput.StdErr
        }),
    ).Return(nil).Once()

    env.RegisterDelayedCallback(func() {
        env.SignalWorkflow("slurm-request", SlurmRequest{Cmd: expCmd, Config: expConfig})
    }, 0)
    env.RegisterDelayedCallback(func() {
        env.SetCurrentHistoryLength(9500)
    }, 90*time.Second)
    env.ExecuteWorkflow(SlurmPollerWorkflow, SlurmState{
        ParentWfId:    expParentWfId,
        ParentWfRunId: expParentWfRunId,
    })

    env.AssertExpectations(t)
    require.True(t, env.IsWorkflowCompleted())
}

// If a SLURM job fails more than the max number of retries, even
// if those failures are non-fatal, it should send a signal to the 
// calling workflow indicating failure.
func TestSlurmJobNonFatalFailure(t *testing.T) {
    var a SlurmActivity
    testSuite := &testsuite.WorkflowTestSuite{}
    env := testSuite.NewTestWorkflowEnvironment()
    env.RegisterActivity(a.StartRemoteSlurmJobActivity)
    env.RegisterActivity(a.PollRemoteSlurmActivity)
    env.RegisterActivity(a.GetRemoteSlurmJobOutputsActivity)

    jobId := "2718281828459"
    cmdId := 1

    memStr := "40G"
    maxRetries := 2
    expConfig := parsing.SlurmJobConfig{Mem: &memStr, MaxRetries: &maxRetries}
    expCmd := parsing.CmdTemplate{Id: cmdId}
    expSlurmJob := SlurmJob{CmdId: cmdId, JobId: jobId}

    env.OnActivity(
        a.StartRemoteSlurmJobActivity,
        expCmd, expConfig, mock.Anything, mock.Anything,
    ).Return(expSlurmJob, nil).Times(maxRetries + 1)

    // Handle workflow polling before request, which should be empty.
    env.OnActivity(a.PollRemoteSlurmActivity, []string{}).Return(nil, nil).Maybe()

    env.OnActivity(a.PollRemoteSlurmActivity, []string{jobId}).
        Return(map[string]SacctResult{jobId: {JobId: jobId, State: "PREEMPTED"}}, nil)

    expCmdOutput := CmdOutput{Id: cmdId, StdOut: "stdout", StdErr: "stderr"}
    env.OnActivity(a.GetRemoteSlurmJobOutputsActivity, []SlurmJob{expSlurmJob}).
        Return([]CmdOutput{expCmdOutput}, nil)

    expParentWfId := "parentId"
    expParentWfRunId := "parentRunId"
    env.OnSignalExternalWorkflow(
        mock.Anything, expParentWfId, "", "slurm-response",
        mock.MatchedBy(func(arg interface{}) bool {
            resp, ok := arg.(SlurmResponse)
            return ok && resp.Error != nil && resp.Result.Id == expCmdOutput.Id &&
                resp.Result.StdOut == expCmdOutput.StdOut &&
                resp.Result.StdErr == expCmdOutput.StdErr
        }),
    ).Return(nil).Once()

    env.RegisterDelayedCallback(func() {
        env.SignalWorkflow("slurm-request", SlurmRequest{Cmd: expCmd, Config: expConfig})
    }, 0)
    env.RegisterDelayedCallback(func() {
        env.SetCurrentHistoryLength(9500)
    }, 90*time.Second)
    env.ExecuteWorkflow(SlurmPollerWorkflow, SlurmState{
        ParentWfId:    expParentWfId,
        ParentWfRunId: expParentWfRunId,
    })

    env.AssertExpectations(t)
    require.True(t, env.IsWorkflowCompleted())
}

// If a SLURM job fails repeatedly but succeeds after retry,
// it should send a successful completion message to the caller
// workflow.
func TestSlurmJobRetryOnNonFatalErr(t *testing.T) {
    var a SlurmActivity
    testSuite := &testsuite.WorkflowTestSuite{}
    env := testSuite.NewTestWorkflowEnvironment()
    env.RegisterActivity(a.StartRemoteSlurmJobActivity)
    env.RegisterActivity(a.PollRemoteSlurmActivity)
    env.RegisterActivity(a.GetRemoteSlurmJobOutputsActivity)

    jobId := "2718281828459"
    cmdId := 1

    memStr := "40G"
    maxRetries := 2
    expConfig := parsing.SlurmJobConfig{Mem: &memStr, MaxRetries: &maxRetries}
    expCmd := parsing.CmdTemplate{Id: cmdId}
    expSlurmJob := SlurmJob{CmdId: cmdId, JobId: jobId}

    env.OnActivity(
        a.StartRemoteSlurmJobActivity,
        expCmd, expConfig, mock.Anything, mock.Anything,
    ).Return(expSlurmJob, nil).Times(maxRetries+1)

    // Handle workflow polling before request, which should be empty.
    env.OnActivity(a.PollRemoteSlurmActivity, []string{}).Return(nil, nil).Maybe()

    currTries := 0
    env.OnActivity(a.PollRemoteSlurmActivity, []string{jobId}).
        Return(func(jobIds []string) (map[string]SacctResult, error) {
            if currTries == maxRetries {
                return map[string]SacctResult{jobId: {JobId: jobId, State: "COMPLETED"}}, nil
            }
            currTries++
            return map[string]SacctResult{jobId: {JobId: jobId, State: "PREEMPTED"}}, nil
        })

    expCmdOutput := CmdOutput{Id: cmdId, StdOut: "stdout", StdErr: "stderr"}
    env.OnActivity(a.GetRemoteSlurmJobOutputsActivity, []SlurmJob{expSlurmJob}).
        Return([]CmdOutput{expCmdOutput}, nil)

    expParentWfId := "parentId"
    expParentWfRunId := "parentRunId"
    env.OnSignalExternalWorkflow(
        mock.Anything, expParentWfId, "", "slurm-response",
        mock.MatchedBy(func(arg interface{}) bool {
            resp, ok := arg.(SlurmResponse)
            return ok && resp.Error == nil && resp.Result.Id == expCmdOutput.Id &&
                resp.Result.StdOut == expCmdOutput.StdOut &&
                resp.Result.StdErr == expCmdOutput.StdErr
        }),
    ).Return(nil).Once()

    env.RegisterDelayedCallback(func() {
        env.SignalWorkflow("slurm-request", SlurmRequest{Cmd: expCmd, Config: expConfig})
    }, 0)
    env.RegisterDelayedCallback(func() {
        env.SetCurrentHistoryLength(9500)
    }, 90*time.Second)
    env.ExecuteWorkflow(SlurmPollerWorkflow, SlurmState{
        ParentWfId:    expParentWfId,
        ParentWfRunId: expParentWfRunId,
    })

    env.AssertExpectations(t)
    require.True(t, env.IsWorkflowCompleted())
}

// Add test for input / output files
// Add tests for sbatch / sacct / output parsing.
// Ideally, add tests for ssh connection.
// Add test for scancel