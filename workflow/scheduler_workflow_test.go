package workflow

import (
	"context"
	"errors"
	"fmt"
	"go-scheduler/parsing"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

// The actual workflow prints a bunch of stuff, so we create
// this type to pass to the mocked workflow during testing
// in order to not clutter the output of the test suite.
type NoopLogger struct{}

func (NoopLogger) Debug(msg string, keyvals ...interface{}) {}
func (NoopLogger) Info(msg string, keyvals ...interface{})  {}
func (NoopLogger) Warn(msg string, keyvals ...interface{})  {}
func (NoopLogger) Error(msg string, keyvals ...interface{}) {}

func TestWorkerRegistration(t *testing.T) {
    testSuite := &testsuite.WorkflowTestSuite{}
    workerQueue := "worker-queue-1"
    workerInfo := WorkerInfo{
        QueueId:        workerQueue,
        TotalResources: parsing.ResourceVector{MemMb: 1024, Cpus: 4, Gpus: 1},
    }

    env := testSuite.NewTestWorkflowEnvironment()

    env.OnActivity(WorkerHeartbeatActivity, mock.Anything, mock.Anything).
        Return(nil)

    env.RegisterDelayedCallback(func() {
        env.SignalWorkflow("worker-register", workerInfo)
    }, 0)

    // To make sure the workflow completes (necessary for the test), we
    // set the history length above the max history length, which should cause
    // it to continue as new.
    env.RegisterDelayedCallback(func() {
        env.SetCurrentHistoryLength(9500)
    }, 10*time.Second)
    env.ExecuteWorkflow(ResourceSchedulerWorkflow, SchedWorkflowState{
        Workers: map[string]WorkerInfo{
            workerQueue: workerInfo,
        },
    })
    env.AssertExpectations(t)
}

func TestWorkflowResourceAllocation(t *testing.T) {
    testSuite := &testsuite.WorkflowTestSuite{}
    workerQueue := "worker-queue-1"
    workerInfo := WorkerInfo{
        QueueId:        workerQueue,
        TotalResources: parsing.ResourceVector{MemMb: 1024, Cpus: 4, Gpus: 1},
    }

    env := testSuite.NewTestWorkflowEnvironment()
    env.RegisterActivity(WorkerHeartbeatActivity)
    env.OnActivity(WorkerHeartbeatActivity, mock.Anything, workerQueue).
        Return(func(ctx context.Context, workerID string) error {
            for i := 0; i < 3; i++ {
                activity.RecordHeartbeat(ctx, workerID)
                time.Sleep(20 * time.Millisecond)
            }
            return nil
        }).Maybe()

    reqId := 1
    request := ResourceRequest{
        Id:               reqId,
        Rank:             1,
        Requirements:     parsing.ResourceVector{MemMb: 512, Cpus: 2, Gpus: 0},
        CallerWorkflowId: "response-signal",
    }

    env.OnSignalExternalWorkflow(
        "default-test-namespace", "response-signal", "", "allocation-response",
        mock.MatchedBy(func(arg interface{}) bool {
            resp, ok := arg.(ResourceGrant)
            return ok && resp.Success &&  resp.RequestId == reqId && 
                resp.WorkerId == workerQueue
        }),
    ).Return(nil).Once()

    env.RegisterDelayedCallback(func() {
        env.SignalWorkflow("new-request", request)
    }, 0)
    env.RegisterDelayedCallback(func() {
        env.SetCurrentHistoryLength(9500)
    }, 10*time.Second)
    env.ExecuteWorkflow(ResourceSchedulerWorkflow, SchedWorkflowState{
        Workers: map[string]WorkerInfo{
            workerQueue: workerInfo,
        },
    })

    env.AssertExpectations(t)
    require.True(t, env.IsWorkflowCompleted())
}

func TestResourceOrdering(t *testing.T) {
    testSuite := &testsuite.WorkflowTestSuite{}
    workerQueue := "worker-queue-1"
    totalWorkerCpus := 4
    workerInfo := WorkerInfo{
        QueueId:        workerQueue,
        TotalResources: parsing.ResourceVector{
            MemMb: 1024, 
            Cpus: totalWorkerCpus, 
            Gpus: 1,
        },
    }

    // Notice how none of these requests can be serviced concurrently
    // because of their CPU requirements, meaning they must be assigned
    // only after the last one has been freed.
    requests := []ResourceRequest{
        {Id: 1, Rank: 2, Requirements: parsing.ResourceVector{Cpus: totalWorkerCpus}, CallerWorkflowId: "sig1"},
        {Id: 2, Rank: 1, Requirements: parsing.ResourceVector{Cpus: totalWorkerCpus-1}, CallerWorkflowId: "sig2"},
        {Id: 3, Rank: 1, Requirements: parsing.ResourceVector{Cpus: totalWorkerCpus}, CallerWorkflowId: "sig3"},
    }
    expectedOrder := []int{1, 3, 2}
    orderReceived := make([]int, 0)

    env := testSuite.NewTestWorkflowEnvironment()
    env.RegisterActivity(WorkerHeartbeatActivity)
    env.OnActivity(WorkerHeartbeatActivity, mock.Anything, workerQueue).Return(nil)
    env.OnSignalExternalWorkflow(
        mock.Anything, mock.Anything, mock.Anything, "allocation-response",
        mock.MatchedBy(func(arg interface{}) bool {
            resp, ok := arg.(ResourceGrant)
            if !ok {
                return false
            }
            return resp.Success
        }),
    ).Return(func(namespace, wfId, runId, sigName string, sigBody any) error {
        env.SignalWorkflow("release-allocation", sigBody)
        resp := sigBody.(ResourceGrant)
        orderReceived = append(orderReceived, resp.RequestId)
        return nil
    }).Times(3)


    for _, req := range requests {
        env.RegisterDelayedCallback(func() {
            env.SignalWorkflow("new-request", req)
        }, 0)
    }
    env.RegisterDelayedCallback(func() {
        env.SetCurrentHistoryLength(9500)
    }, time.Second*100)

    env.ExecuteWorkflow(ResourceSchedulerWorkflow, SchedWorkflowState{
        Workers: map[string]WorkerInfo{
            workerQueue: workerInfo,
        },
    })

    env.AssertExpectations(t)
    for i := range orderReceived {
        if orderReceived[i] != expectedOrder[i] {
            t.Fatalf(
                "Received resource grants for requests %#v " +
                "in order %#v but expected order %#v. Requests should " +
                "be ordered first by rank (ascending), then by GPU, CPU, " +
                "and memory (descending).",
                requests, orderReceived, expectedOrder,
            )
        }
    }
}

func TestSchedulingWorkflowContinueAsNew(t *testing.T) {
    noOpLogger := NoopLogger{}
    testSuite := &testsuite.WorkflowTestSuite{}
    testSuite.SetLogger(noOpLogger)
    workerQueue := "worker-queue-1"

    env := testSuite.NewTestWorkflowEnvironment()
    env.RegisterActivity(WorkerHeartbeatActivity)
    env.OnActivity(WorkerHeartbeatActivity, mock.Anything, workerQueue).Return(nil)
    env.SetCurrentHistoryLength(9500)

    env.ExecuteWorkflow(ResourceSchedulerWorkflow, SchedWorkflowState{})

    require.True(t, env.IsWorkflowCompleted())
    err := env.GetWorkflowError()
    var desiredErr *workflow.ContinueAsNewError
    if !errors.As(err, &desiredErr) {
        t.Fatalf("Workflow did not continue as new")
    }
}

func TestWorkerHeartbeatFailureBlocksRequests(t *testing.T) {
    handler := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
        Level: slog.LevelInfo,
    })
    logger := slog.New(handler)

    testSuite := &testsuite.WorkflowTestSuite{}
    testSuite.SetLogger(logger)
    env := testSuite.NewTestWorkflowEnvironment()

    workerQueue := "worker-queue-1"
    workerInfo := WorkerInfo{
        QueueId:        workerQueue,
        TotalResources: parsing.ResourceVector{MemMb: 1024, Cpus: 4, Gpus: 1},
    }

    requestId := 1
    requests := []ResourceRequest{}
    for i := 0; i < 5; i++ {
        requests = append(requests, ResourceRequest{
            Id:               requestId + i,
            Rank:             1,
            Requirements:     parsing.ResourceVector{MemMb: 512, Cpus: 2, Gpus: 0},
            CallerWorkflowId: fmt.Sprintf("caller-%d", requestId+i),
        })
    }

    heartbeatCount := 0
    env.RegisterActivity(WorkerHeartbeatActivity)
    env.OnActivity(WorkerHeartbeatActivity, mock.Anything, workerQueue).
        Return(func(ctx context.Context, workerID string) error {
            if heartbeatCount == 1 {
                for _, req := range requests {
                    env.SignalWorkflow("new-request", req)
                }
            }

            if heartbeatCount < 3 {
                heartbeatCount++
                logger.Warn("Worker heartbeat timeout", "workerId", workerQueue)
                return temporal.NewTimeoutError(3, fmt.Errorf("heartbeat timeout"))
            }
            activity.RecordHeartbeat(ctx, workerID)
            return nil
        })


    granted := make(map[int]bool)
    for _, req := range requests {
        rid := req.Id
        env.OnSignalExternalWorkflow(
            "default-test-namespace", req.CallerWorkflowId, "", "allocation-response",
            mock.MatchedBy(func(arg interface{}) bool {
                resp, ok := arg.(ResourceGrant)
                if !ok || !resp.Success || resp.RequestId != rid {
                    return false
                }
                granted[rid] = true
                return true
            }),
        ).Return(nil).Maybe()
    }

    env.RegisterDelayedCallback(func() {
        env.SignalWorkflow("worker-register", workerInfo)
    }, 0)

    //for i, req := range requests {
    //    env.RegisterDelayedCallback(func(req ResourceRequest) func() {
    //        return func() {
    //            env.SignalWorkflow("new-request", req)
    //        }
    //    }(req), time.Duration(i)*100*time.Millisecond)
    //}

    env.RegisterDelayedCallback(func() {
        env.SetCurrentHistoryLength(9500)
    }, 20000*time.Second)

    env.ExecuteWorkflow(ResourceSchedulerWorkflow, SchedWorkflowState{
        Workers: map[string]WorkerInfo{
            workerQueue: workerInfo,
        },
    })

    for heartbeatCount < 3 {
        for i := range requests {
            if granted[requestId+i] {
                t.Fatalf("Request %d was granted during heartbeat failure; this should not happen", requestId+i)
            }
        }
    }

    grantedCount := 0
    for _, granted := range granted {
        if granted {
            grantedCount++
        }
    }

    if grantedCount == 0 {
        t.Fatalf("No requests were eventually granted after heartbeat resumed")
    }
    fmt.Printf("Gather cnt %d\n", grantedCount)

    env.AssertExpectations(t)
    require.True(t, env.IsWorkflowCompleted())
}