package workflow

import (
	"go-scheduler/fs"
	"go-scheduler/parsing"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

// Temporal executor should request resource grant from
// scheduling workflow and should schedule the activity to
// run only after receiving a corresponding resource grant
// from the child workflow. Furthermore, it should schedule
// the RunCmd activity to the queue specified in the resource
// grant.
func TestTemporalExecutorGrantRequest(t *testing.T) {
    testSuite := &testsuite.WorkflowTestSuite{}
    env := testSuite.NewTestWorkflowEnvironment()
    env.RegisterActivity(RunCmd)

    assignedQueue := "ASSIGNED_QUEUE"
    readyToRunJob := false
    cmdToRun := parsing.CmdTemplate{Id: 1805, ResourceReqs: parsing.ResourceVector{MemMb: 500}}
    env.RegisterWorkflowWithOptions(func(ctx workflow.Context, _ SchedWorkflowState) error {
        selector := workflow.NewSelector(ctx)
        selector.AddReceive(
            workflow.GetSignalChannel(ctx, "new-request"), 
            func(c workflow.ReceiveChannel, _ bool) {
                var req ResourceRequest
                if ok := c.Receive(ctx, &req); !ok {
                    t.Fatalf("could not convert signal contents to resource req")
                }
                // Ensure request was passed faithfully by executor.
                require.Equal(t, req.Id, cmdToRun.Id)
                require.Equal(t, req.Requirements, cmdToRun.ResourceReqs)
                workflow.SignalExternalWorkflow(
                    ctx, req.CallerWorkflowId, "", "allocation-response", ResourceGrant{
                        RequestId: req.Id,
                        WorkerId: assignedQueue,
                    },
                )
                readyToRunJob = true
            },
        )
        
        // IMPORTANT: Force mocked child workflow to sleep before selecting
        // (and therefore before responding to signal), giving the executor
        // the opportunity to prematurely schedule the job, which would result
        // in the test failing.
        workflow.Sleep(ctx, 900 * time.Second)
        selector.Select(ctx)
        return nil
    }, workflow.RegisterOptions{
        Name: "ResourceSchedulerWorkflow",
    })

    // Handle an activity invoked during setup.
    env.OnActivity(fs.SetupVolumes, mock.Anything, mock.Anything).Return(nil, nil).Maybe()
    env.OnActivity(RunCmd, mock.Anything, cmdToRun).
        Return(func(_ map[string]string, _ parsing.CmdTemplate) (CmdOutput, error) {
            //require.Equal(t, queueName, assignedQueue)
            require.True(t, readyToRunJob)
            return CmdOutput{}, nil
        }).Once()

    env.ExecuteWorkflow(func(ctx workflow.Context) error {
        selector := workflow.NewSelector(ctx)
        temporalExec := NewTemporalExecutor(
            ctx, &selector, nil, fs.LocalFS{}, map[string]WorkerInfo{
                assignedQueue: { QueueId: assignedQueue },
            }, "")
        if err := temporalExec.Setup(); err != nil {
            t.Fatalf("setup failed: %s", err)
        }
        temporalExec.RunCmds([]parsing.CmdTemplate{cmdToRun})
        for i := 0; i < 10; i++ {
            temporalExec.Select()
        }
        return nil
    })
    env.AssertExpectations(t)
}

// NOTE: I am unable to test grant releases because 
// of an unresolved bug in the temporal test env 
// (https://github.com/temporalio/sdk-go/issues/1961).