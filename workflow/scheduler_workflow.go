package workflow

import (
	"context"
	"errors"
	"fmt"
	"go-scheduler/parsing"
	"math"
	"sort"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

type ResourceRequest struct {
	Id               int
	Rank             int
	Requirements     parsing.ResourceVector
	CallerWorkflowId string
}

type ResourceGrant struct {
	RequestId        int
    WorkerId         string
	Requirements     parsing.ResourceVector
    Success          bool
}

type WorkerInfo struct {
	QueueId        string
	IsAlive        bool
	TotalResources parsing.ResourceVector
	UsedResources  parsing.ResourceVector
}

type SchedWorkflowState struct {
	PendingRequests            []ResourceRequest
    ActiveGrants               map[int]bool
	Workers                    map[string]WorkerInfo
	NumRetries                 map[string]int
	SecondsSinceHeartbeatSched map[string]int
	SecondsUntilHeartbeatRetry map[string]int
}

const (
	SCHEDULE_TO_START_TIMEOUT_SECONDS = 30
	HEARTBEAT_RETRY_MAX_SECONDS       = 9000
	HEARTBEAT_RETRY_BASE_SECONDS      = 30
	HEARTBEAT_RETRY_BACKOFF_COEFF     = 4
)

func WorkerHeartbeatActivity(ctx context.Context, workerId string) error {
	for {
		select {
		case <-time.After(10 * time.Second):
			activity.RecordHeartbeat(ctx, workerId)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func registerHeartbeatActivity(
	ctx workflow.Context, selector workflow.Selector,
	state *SchedWorkflowState, queueId string,
) func() {
    logger := workflow.GetLogger(ctx)
    logger.Info(fmt.Sprintf("Starting heartbeat on worker %s", queueId))
	ao := workflow.ActivityOptions{
		TaskQueue:              queueId,
		HeartbeatTimeout:       time.Minute,
		StartToCloseTimeout:    time.Hour * 24,
		ScheduleToStartTimeout: time.Second * SCHEDULE_TO_START_TIMEOUT_SECONDS,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 1,
		},
	}
	heartbeatCtx, cancelFunc := workflow.WithCancel(
		workflow.WithActivityOptions(ctx, ao),
	)
    logger.Info("Registering heartbeat")
	heartbeatFuture := workflow.ExecuteActivity(heartbeatCtx, WorkerHeartbeatActivity, queueId)
	selector.AddFuture(heartbeatFuture, func(f workflow.Future) {
		err := f.Get(ctx, nil)
        //logger.Info(fmt.Sprintf("Got future %#v", err))
		if err != nil {
            logger.Info(fmt.Sprintf("Err: %s\n", err))
			var timeoutErr *temporal.TimeoutError
			if errors.As(err, &timeoutErr) {
				workerRecord := state.Workers[queueId]
				workerRecord.IsAlive = false
				state.Workers[queueId] = workerRecord

				if _, ok := state.NumRetries[queueId]; !ok {
					state.NumRetries[queueId] = 1
				} else {
					state.NumRetries[queueId] += 1
				}

				// Shockingly, go has no built-in integer exponentiation.
				multiple := int(math.Pow(
					HEARTBEAT_RETRY_BACKOFF_COEFF,
					float64(state.NumRetries[queueId]-1),
				))

				state.SecondsUntilHeartbeatRetry[queueId] = min(
					HEARTBEAT_RETRY_BASE_SECONDS*multiple,
					HEARTBEAT_RETRY_MAX_SECONDS,
				)

                delete(state.SecondsSinceHeartbeatSched, queueId)
			}
		}
	})
	return cancelFunc
}

func retryHeartbeats(
	ctx workflow.Context, selector workflow.Selector,
	state *SchedWorkflowState, secondsElapsed int,
) {
    logger := workflow.GetLogger(ctx)
	resurrectedWorkers := make([]string, 0)
	for queueId := range state.SecondsSinceHeartbeatSched {
		state.SecondsSinceHeartbeatSched[queueId] += secondsElapsed
		if state.SecondsSinceHeartbeatSched[queueId] > SCHEDULE_TO_START_TIMEOUT_SECONDS {
			workerRecord := state.Workers[queueId]
			workerRecord.IsAlive = true
			state.Workers[queueId] = workerRecord
			resurrectedWorkers = append(resurrectedWorkers, queueId)
		}
	}

	for _, queueId := range resurrectedWorkers {
		delete(state.SecondsSinceHeartbeatSched, queueId)
	}

	retriedWorkers := make([]string, 0)
	for queueId := range state.SecondsUntilHeartbeatRetry {
		state.SecondsUntilHeartbeatRetry[queueId] -= secondsElapsed
		if state.SecondsUntilHeartbeatRetry[queueId] <= 0 {
            logger.Info("Retrying heartbeat")
			registerHeartbeatActivity(ctx, selector, state, queueId)
			state.SecondsSinceHeartbeatSched[queueId] = 0
			retriedWorkers = append(retriedWorkers, queueId)
		}
	}

	for _, queueId := range retriedWorkers {
		delete(state.SecondsUntilHeartbeatRetry, queueId)
	}
}

func processRequests(ctx workflow.Context, state *SchedWorkflowState) {
	logger := workflow.GetLogger(ctx)

	sort.Slice(state.PendingRequests, func(i, j int) bool {
        reqI := state.PendingRequests[i]
        reqJ := state.PendingRequests[j]
		if reqI.Rank != reqJ.Rank {
			return reqI.Rank < reqJ.Rank
		}
        if reqI.Requirements.Gpus != reqJ.Requirements.Gpus {
            return reqI.Requirements.Gpus > reqJ.Requirements.Gpus
        }
        if reqI.Requirements.Cpus != reqJ.Requirements.Cpus {
            return reqI.Requirements.Cpus > reqJ.Requirements.Cpus
        }
        return reqI.Requirements.MemMb > reqJ.Requirements.MemMb
	})

	for i := 0; i < len(state.PendingRequests); {
		req := state.PendingRequests[i]
		assigned := false

		for queueId, worker := range state.Workers {
			if !worker.IsAlive {
				continue
			}

			available := parsing.ResourceVector{
				MemMb: worker.TotalResources.MemMb - worker.UsedResources.MemMb,
				Cpus:  worker.TotalResources.Cpus - worker.UsedResources.Cpus,
				Gpus:  worker.TotalResources.Gpus - worker.UsedResources.Gpus,
			}

			if available.MemMb >= req.Requirements.MemMb &&
				available.Cpus >= req.Requirements.Cpus &&
				available.Gpus >= req.Requirements.Gpus {

				worker.UsedResources.MemMb += req.Requirements.MemMb
				worker.UsedResources.Cpus += req.Requirements.Cpus
				worker.UsedResources.Gpus += req.Requirements.Gpus
				state.Workers[queueId] = worker

				response := ResourceGrant{
                    RequestId: req.Id,
					WorkerId: queueId,
                    Requirements: req.Requirements,
					Success:  true,
				}
                state.ActiveGrants[req.Id] = true

                //logger.Debug(fmt.Sprintf("Servicing resource request %d (%#v)\n", req.Id, req))
				err := workflow.SignalExternalWorkflow(ctx, req.CallerWorkflowId, "", "allocation-response", response).Get(ctx, nil)
				if err != nil {
					logger.Error("Failed to signal requesting workflow", "Error", err)
				}

				state.PendingRequests = append(state.PendingRequests[:i], state.PendingRequests[i+1:]...)
				assigned = true
				break
			}
		}

		if !assigned {
			i++
		}
	}
}

func freeResourceGrant(state *SchedWorkflowState, grant ResourceGrant) {
    // Temporal can occasionally reschedule the same signal / activity,
    // so we add this check to make the release signal handler idempotent.
    if !state.ActiveGrants[grant.RequestId] {
        return
    }

    worker := state.Workers[grant.WorkerId]
    worker.UsedResources.MemMb -= grant.Requirements.MemMb
    worker.UsedResources.Cpus -= grant.Requirements.Cpus
    worker.UsedResources.Gpus -= grant.Requirements.Gpus
    state.Workers[grant.WorkerId] = worker
    state.ActiveGrants[grant.RequestId] = false
}

func ResourceSchedulerWorkflow(ctx workflow.Context, state SchedWorkflowState) error {
	logger := workflow.GetLogger(ctx)
    logger.Info("Starting sched workflow")
	if state.PendingRequests == nil {
		state.PendingRequests = make([]ResourceRequest, 0)
	}
	if state.Workers == nil {
		state.Workers = make(map[string]WorkerInfo)
	}
	if state.SecondsSinceHeartbeatSched == nil {
		state.SecondsSinceHeartbeatSched = make(map[string]int)
	}
	if state.SecondsUntilHeartbeatRetry == nil {
		state.SecondsUntilHeartbeatRetry = make(map[string]int)
	}
	if state.NumRetries == nil {
		state.NumRetries = make(map[string]int)
	}
    if state.ActiveGrants == nil {
        state.ActiveGrants = make(map[int]bool)
    }

	selector := workflow.NewSelector(ctx)
	heartbeatCancelFuncs := make([]func(), 0)
	for queueId := range state.Workers {
		heartbeatFuture := registerHeartbeatActivity(
			ctx, selector, &state, queueId,
		)
		heartbeatCancelFuncs = append(heartbeatCancelFuncs, heartbeatFuture)
		workerRecord := state.Workers[queueId]
		workerRecord.IsAlive = true
		state.Workers[queueId] = workerRecord
	}

	selector.AddReceive(workflow.GetSignalChannel(ctx, "new-request"), func(c workflow.ReceiveChannel, _ bool) {
		var req ResourceRequest
		c.Receive(ctx, &req)
        fmt.Printf("Got request for ID %d\n", req.Id)
		state.PendingRequests = append(state.PendingRequests, req)
		logger.Info(fmt.Sprintf("Received new resource request %#v\n", req))
	})

	selector.AddReceive(workflow.GetSignalChannel(ctx, "release-allocation"), func(c workflow.ReceiveChannel, _ bool) {
		var grant ResourceGrant
		c.Receive(ctx, &grant)
        freeResourceGrant(&state, grant)
        logger.Info(fmt.Sprintf("Freeing resource grant %d\n", grant.RequestId))
	})

	selector.AddReceive(workflow.GetSignalChannel(ctx, "worker-register"), func(c workflow.ReceiveChannel, _ bool) {
		var worker WorkerInfo
		c.Receive(ctx, &worker)
		if _, exists := state.Workers[worker.QueueId]; !exists {
			worker.IsAlive = true
			state.Workers[worker.QueueId] = worker
			heartbeatFuture := registerHeartbeatActivity(
				ctx, selector, &state, worker.QueueId,
			)
			heartbeatCancelFuncs = append(heartbeatCancelFuncs, heartbeatFuture)
			logger.Info("Registered new worker", "QueueId", worker.QueueId)
		}
	})

    durationSecs := 1
    durationSecsAsTime := time.Second * 1
	var timerCallback func(workflow.Future)
	timerCallback = func(f workflow.Future) {
		processRequests(ctx, &state)
		retryHeartbeats(ctx, selector, &state, durationSecs)
		timer := workflow.NewTimer(ctx, durationSecsAsTime)
		selector.AddFuture(timer, timerCallback)
	}

	timer := workflow.NewTimer(ctx, durationSecsAsTime)
	selector.AddFuture(timer, timerCallback)

    cancelled := false
	for {
		selector.Select(ctx)
        if ctx.Err() != nil{
            cancelled = true
            break
        }

		if workflow.GetInfo(ctx).GetCurrentHistoryLength() > 9000 {
			logger.Info("History approaching limit, continuing as new")
			break
		}
	}

	for _, cancelFunc := range heartbeatCancelFuncs {
		cancelFunc()
	}

    if cancelled {
        return nil
    }

	return workflow.NewContinueAsNewError(ctx, ResourceSchedulerWorkflow, state)
}
