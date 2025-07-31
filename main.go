package main

import (
	"context"
	"encoding/json"
	"fmt"
	"go-scheduler/parsing"
	"go-scheduler/workflow"
	"os"
	"time"
    "syscall"
    "os/signal"
    "log"
    "log/slog"
	"go.temporal.io/sdk/client"
	temporalLog "go.temporal.io/sdk/log"
	"go.temporal.io/sdk/worker"
)

func startWorker(queueName string) {
	// Create Temporal client
    logger := temporalLog.NewStructuredLogger(slog.New(slog.NewTextHandler(os.Stdout, 
        &slog.HandlerOptions{
            Level: slog.LevelInfo,
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

	w.RegisterWorkflow(workflow.RunBwbWorkflow)
	w.RegisterActivity(workflow.BuildSingularitySIF)
	w.RegisterActivity(workflow.RunCmd)

	log.Println("Starting worker...")
	err = w.Run(worker.InterruptCh())
	if err != nil {
		log.Fatalln("Unable to start worker", err)
	}

}

func main() {
    //envs := []string{"SINGULARITYENV_VERSION=2.7.11a", "SINGULARITYENV_genomeDir=/data/processing/genome"}
    //cmd := "singularity exec  -p -i --cleanenv -B /home/patrick/SCHED_STORAGE/storageID:/data " +
    //    "/home/patrick/SCHED_STORAGE/images/patrickmckeever.star_async:0.4.sif sh -c '[ -n \"$overwrite\" ] " +
    //     "|| [ !  -f  \"$genomeDir/SAindex\" ] ||  exit 0 && /usr/local/bin/$VERSION/Linux_x86_64_static/STAR " +
    //     "--runMode genomeGenerate --genomeDir /data/processing/genome --sjdbGTFfile /data/processing/genome/genome.gtf " +
    //     "--genomeFastaFiles /data/processing/genome/genome.fa --runThreadN 16 '"
    //cmdObj := exec.Command("sh", "-c", cmd)
    //cmdObj.Env = envs

    //var stdout, stderr bytes.Buffer
    //cmdObj.Stdout = &stdout
    //cmdObj.Stderr = &stderr


    
    //if err := cmdObj.Run(); err != nil {
    //    fmt.Printf("STDOUT:\n%s\n", stdout.String())
    //    fmt.Printf("STDERR:\n%s\n", stderr.String())
    //    log.Fatalf("cmd failed: %s\n", err)
    //}
    //os.Exit(1)


	data, err := os.ReadFile("parsing/testdata/bulkrna_seq_chrY.json")
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

    go startWorker("bwb_worker")

    c, err := client.NewLazyClient(client.Options{})

	if err != nil {
		log.Fatalf("Unable to create Temporal client: %s\n", err)
	}
	defer c.Close()

    workflowOptions := client.StartWorkflowOptions{
        ID: "bwb_workflow" + time.Now().Format("20060102150405"),
        TaskQueue: "bwb_worker",
    }

    we, err := c.ExecuteWorkflow(
        context.Background(), workflowOptions, 
        workflow.RunBwbWorkflow, "storageID", 
        bwbWorkflow, index,
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