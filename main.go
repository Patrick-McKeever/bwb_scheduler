package main

import (
    "context"
    "encoding/json"
    "fmt"
    "go-scheduler/fs"
    "go-scheduler/parsing"
    "go-scheduler/workflow"
    "log"
    "log/slog"
    "os"
    "os/signal"
    "syscall"
    "time"
    "path/filepath"

    "go.temporal.io/sdk/client"
    temporalLog "go.temporal.io/sdk/log"
    "go.temporal.io/sdk/worker"
)

func startWorker(queueName string, scheduler bool) {
    // Create Temporal client
    logger := temporalLog.NewStructuredLogger(slog.New(slog.NewTextHandler(os.Stdout, 
        &slog.HandlerOptions{
            Level: slog.LevelWarn,
        },
    )))
    c, err := client.NewLazyClient(client.Options{
        Logger: logger,
    })
    if err != nil {
        log.Fatalln("Unable to create Temporal client", err)
    }
    defer c.Close()

    w := worker.New(c, queueName, worker.Options{})

    if scheduler {
        w.RegisterWorkflow(workflow.RunBwbWorkflow)
        w.RegisterWorkflow(workflow.ResourceSchedulerWorkflow)
        w.RegisterActivity(workflow.BuildSingularitySIF)
        w.RegisterActivity(fs.GlobActivity[fs.LocalFS])
    } else {
        w.RegisterActivity(workflow.WorkerHeartbeatActivity)
        w.RegisterActivity(workflow.RunCmd)
        w.RegisterActivity(workflow.SetupVolumes)
    }

    log.Println("Starting worker...")
    err = w.Run(worker.InterruptCh())
    if err != nil {
        log.Fatalln("Unable to start worker", err)
    }

}

func main() {
    data, err := os.ReadFile("parsing/testdata/bulkrna.json")
    if err != nil {
        log.Fatalf("failed to read JSON file: %v\n", err)
    }


    var bwbWorkflow parsing.Workflow
    if err := json.Unmarshal(data, &bwbWorkflow); err != nil {
        log.Fatalf("failed to unmarshal JSON: %v\n", err)
    }

    index, err := parsing.ParseAndValidateWorkflow(&bwbWorkflow)
    if err != nil {
        log.Fatalf("failed to build index: %s\n", err)
    }

    go startWorker("bwb_worker", true)
    go startWorker("abcdefg", false)

    c, err := client.NewLazyClient(client.Options{})

    if err != nil {
        log.Fatalf("Unable to create Temporal client: %s\n", err)
    }
    defer c.Close()

    workflowOptions := client.StartWorkflowOptions{
        ID: "bwb_workflow" + time.Now().Format("20060102150405"),
        TaskQueue: "bwb_worker",
    }

    workers := map[string]workflow.WorkerInfo{
        "abcdefg": {
            QueueId: "abcdefg",
            TotalResources: parsing.ResourceVector{
                MemMb: 8000,
                Cpus: 8,
                Gpus: 0,
            },
        },
    }

    sched_dir := os.Getenv("BWB_SCHED_DIR")
    masterFS := fs.LocalFS{
        Volumes: map[string]string{
            "/data": filepath.Join(sched_dir, "storageID"),
        },
    }

    we, err := c.ExecuteWorkflow(
        context.Background(), workflowOptions, 
        workflow.RunBwbWorkflow, "storageID", 
        bwbWorkflow, index, workers, masterFS,
    )

    if err != nil {
        log.Fatalf("failed to start workflow: %s\n", err)
    }

    sigCh := make(chan os.Signal, 1)
    signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

    go func() {
        <-sigCh
        log.Println("Interrupt received! Canceling workflow...")
        err := c.CancelWorkflow(context.Background(), we.GetID(), we.GetRunID())
        if err != nil {
            log.Println("Failed to cancel workflow:", err)
        } else {
            log.Println("Workflow cancellation requested.")
        }
        os.Exit(0)
    }()

    var resultErr error
    err = we.Get(context.Background(), &resultErr)
    if err != nil {
        log.Fatalf("failed with err: %s\n", err)
    } else {
        fmt.Println("Workflow completed successfully")
    }

}