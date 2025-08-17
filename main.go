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
    "time"
    "path/filepath"
    "strings"
    "strconv"
    "crypto/rand"
    "runtime"
    "math"

    "go.temporal.io/sdk/client"
    temporalLog "go.temporal.io/sdk/log"
    "go.temporal.io/sdk/worker"
    "github.com/spf13/cobra"
    "github.com/shirou/gopsutil/mem"
)

type ByteSize int64

func (b *ByteSize) String() string {
    return fmt.Sprintf("%d", *b)
}

func (b *ByteSize) Type() string {
    return "bytes"
}

func (b *ByteSize) SetBytes(bytes int) {
    *b = ByteSize(bytes)
}

func (b *ByteSize) AsUnit(unit string) int {
    units := map[string]int{
        "KB":  1000,
        "MB":  1000 * 1000,
        "GB":  1000 * 1000 * 1000,
        "KIB": 1024,
        "MIB": 1024 * 1024,
        "GIB": 1024 * 1024 * 1024,
    }

    multiplier, ok := units[unit]
    if !ok {
        return -1
    }
    return int(*b) / multiplier
}

func (b *ByteSize) Set(s string) error {
    s = strings.TrimSpace(s)
    s = strings.ToUpper(s)

    units := map[string]int64{
        "KB":  1000,
        "MB":  1000 * 1000,
        "GB":  1000 * 1000 * 1000,
        "KIB": 1024,
        "MIB": 1024 * 1024,
        "GIB": 1024 * 1024 * 1024,
    }

    for unit, multiplier := range units {
        if strings.HasSuffix(s, unit) {
            numPart := strings.TrimSuffix(s, unit)
            val, err := strconv.ParseFloat(numPart, 64)
            if err != nil {
                return fmt.Errorf("invalid size %q: %w", s, err)
            }
            *b = ByteSize(val * float64(multiplier))
            return nil
        }
    }

    return fmt.Errorf("unknown size unit in %q", s)
}


func startWorker(slogger *slog.Logger, queueName string, scheduler bool) {
    // Create Temporal client
    logger := temporalLog.NewStructuredLogger(slogger)
    c, err := client.NewLazyClient(client.Options{
        Logger: logger,
    })
    if err != nil {
        log.Fatalln("Unable to create Temporal client", err)
    }
    defer c.Close()

    w := worker.New(c, queueName, worker.Options{})

    if scheduler {
        w.RegisterWorkflow(workflow.RunBwbWorkflowTemporal)
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

func randomString(length int) string {
    b := make([]byte, length+2)
    rand.Read(b)
    return fmt.Sprintf("%x", b)[2 : length+2]
}




//var bareword = regexp.MustCompile(`([A-Za-z_][A-Za-z0-9_\-]*)`)
//
//func normalize(input string) string {
//    return bareword.ReplaceAllStringFunc(input, func(s string) string {
//        if s == "true" || s == "false" || s == "null" {
//            return s
//        }
//        return `"` + s + `"`
//    })
//}
//
func overrideParamVals(
    bwbWorkflow *parsing.Workflow, index *parsing.WorkflowIndex, 
    paramVals []string,
) error {
    for _, paramValRaw := range paramVals {
        splitByEq := strings.Split(paramValRaw, "=")
        fmtBaseErr := fmt.Sprintf(
            "could not parse param override %s; --param arguments must be " +
            " of the form [NODE_NAME].[PARAM_NAME]=[VAL], where NODE_NAME is " +
            "the title or numeric ID of a node defined in the workflow JSON, " +
            "PARAM_NAME is one of its parameters, and value is a JSON-parsable " +
            "string giving the desired value of this parameter.", paramValRaw,
        )

        if len(splitByEq) != 2 {
            return fmt.Errorf(fmtBaseErr)
        }
        nodeParamIdStr := splitByEq[0]
        valStr, err := parsing.NormalizeLooseJSON(splitByEq[1])
        if err != nil {
            return fmt.Errorf(fmtBaseErr)
        }

        splitByDot := strings.Split(nodeParamIdStr, ".")
        if len(splitByDot) != 2 {
            return fmt.Errorf(fmtBaseErr)
        }
        nodeNameStr := splitByDot[0]
        pname := splitByDot[1]

        var v any
        dec := json.NewDecoder(strings.NewReader(valStr))
        dec.UseNumber()
        if err := dec.Decode(&v); err != nil {
            return fmt.Errorf(
                "%s %s is not a valid JSON value str", fmtBaseErr, valStr,
            )
        }

        nodeId, nodeNameIsTitle := index.GetIdFromTitle(nodeNameStr)
        invalidNodeNameErr := fmt.Errorf(
            "%s Node name %s is neither the title nor the numeric ID of a node",
             fmtBaseErr, nodeNameStr, 
        )
        if !nodeNameIsTitle {
            var err error
            nodeId, err = strconv.Atoi(nodeNameStr)
            if err != nil {
                return invalidNodeNameErr
            }
        }


        node, nodeExists := bwbWorkflow.Nodes[nodeId]
        if !nodeExists {
            return invalidNodeNameErr
        }

        argType, argTypeExists := node.ArgTypes[pname]
        if !argTypeExists {
            return fmt.Errorf(
                "node %s has no property / no argtype for %s", 
                nodeNameStr, pname,
            )
        }

        if err := index.AddParam(nodeId, pname, argType, v); err != nil {
            return err
        }
        bwbWorkflow.NodeBaseProps[nodeId][pname] = v
    }
    return nil
}

func convertOWSWorkflow(
    owsDirPath string, outPath string,
) error {
    workflow, err := parsing.ParseWorkflow(owsDirPath)
    if err != nil {
        return fmt.Errorf("error parsing OWS: %s", err)
    }

    jsonBytes, err := json.MarshalIndent(&workflow, "", "\t")
    if err != nil {
        return fmt.Errorf("error marshaling bulk rna seq workflow: %s", err)
    }

    if err = os.WriteFile(outPath, jsonBytes, 0644); err != nil {
        return fmt.Errorf("error writing to output file %s: %s", outPath, err)
    }

    return nil
}

func dryRunWorkflow(
    wfPath string, paramVals []string, 
) error {
    data, err := os.ReadFile(wfPath)
    if err != nil {
        return fmt.Errorf("failed to read workflow file %s: %v", wfPath, err)
    }

    var bwbWorkflow parsing.Workflow
    if err := json.Unmarshal(data, &bwbWorkflow); err != nil {
        return fmt.Errorf("failed to unmarshal JSON file %s: %v", wfPath, err)
    }

    index, err := parsing.ParseAndValidateWorkflow(&bwbWorkflow)
    if err != nil {
        return fmt.Errorf("workflow validation error: %s", err)
    }

    if err := overrideParamVals(&bwbWorkflow, &index, paramVals); err != nil {
        return err
    }

    out, err := parsing.DryRun(bwbWorkflow)
    if err != nil {
        return err
    }

    for _, cmd := range out {
        fmt.Println(cmd)
    }
    return nil
}

func runWorkflow(
    wfPath, workerName, temporalWfName, storageId string, paramVals []string, 
    noTemporal, softFail bool, checkptPath string, workerRam ByteSize, 
    workerCpus, workerGpus int,
) error {
    data, err := os.ReadFile(wfPath)
    if err != nil {
        return fmt.Errorf("failed to read workflow file %s: %v", wfPath, err)
    }

    var bwbWorkflow parsing.Workflow
    if err := json.Unmarshal(data, &bwbWorkflow); err != nil {
        return fmt.Errorf("failed to unmarshal JSON file %s: %v", wfPath, err)
    }

    index, err := parsing.ParseAndValidateWorkflow(&bwbWorkflow)
    if err != nil {
        return fmt.Errorf("workflow validation error: %s", err)
    }

    if err := overrideParamVals(&bwbWorkflow, &index, paramVals); err != nil {
        return err
    }

    if _, err := parsing.DryRun(bwbWorkflow); err != nil {
        return fmt.Errorf("dry run failed with error: %s", err)
    }

    queueName := workerName
    if queueName == "" {
        queueName = randomString(16)
    }

    localWorker := workflow.WorkerInfo{
        QueueId: queueName,
        TotalResources: parsing.ResourceVector{
            MemMb: workerRam.AsUnit("MB"),
            Cpus: workerCpus,
            Gpus: workerGpus,
        },
    }

    sched_dir := os.Getenv("BWB_SCHED_DIR")
    revisedStorageID := "storageID"
    if storageId != "" {
        revisedStorageID = storageId
    }

    masterFS := fs.LocalFS{
        Volumes: map[string]string{
            "/data": filepath.Join(sched_dir, revisedStorageID),
        },
    }

    logger := slog.New(slog.NewTextHandler(os.Stdout, 
        &slog.HandlerOptions{
            Level: slog.LevelWarn,
        },
    ))

    if noTemporal {
        return workflow.RunBwbWorkflowNoTemporal(
            storageId, bwbWorkflow, index, localWorker,
            masterFS, softFail, *logger,
        )
    }

    go startWorker(logger, "bwb_worker", true)
    go startWorker(logger, queueName, false)

    c, err := client.NewLazyClient(client.Options{})

    if err != nil {
        return fmt.Errorf("unable to create Temporal client: %s", err)
    }
    defer c.Close()

    wfName := temporalWfName
    if temporalWfName == "" {
        wfName = "bwb_workflow_" + time.Now().Format("20060102150405")
    }
    workflowOptions := client.StartWorkflowOptions{
        ID: wfName,
        TaskQueue: "bwb_worker",
    }

    workers := map[string]workflow.WorkerInfo{ queueName: localWorker }

    we, err := c.ExecuteWorkflow(
        context.Background(), workflowOptions, 
        workflow.RunBwbWorkflowTemporal, revisedStorageID, 
        bwbWorkflow, index, workers, masterFS, softFail,
    )

    if err != nil {
        return fmt.Errorf("failed to start workflow: %s", err)
    }

    var resultErr error
    err = we.Get(context.Background(), &resultErr)
    if err != nil {
        return fmt.Errorf("workflow failed with err: %s", err)
    } else {
        fmt.Println("Workflow completed successfully")
    }
    return nil
}

func main() {
    rootCmd := &cobra.Command{
        Use:   "bwbScheduler",
        Short: "BWB Scheduler",
    }

    var noTemporal bool
    var workerName string
    var storageId string
    var temporalWfName string
    var softFail bool
    var paramOverrides []string
    var checkptPath string
    var workerRam ByteSize
    var workerCpus int
    var workerGpus int
    runCmd := &cobra.Command{
        Use:   "run [WORKFLOW_FILE]",
        Short: "Run JSON-serialized BWB workflow",
        Args: cobra.ExactArgs(1),
        PreRunE: func(cmd *cobra.Command, args []string) error {
            if noTemporal && checkptPath != "" {
                return fmt.Errorf("cannot have --checkpt with --noTemporal")
            }
            if noTemporal && temporalWfName != "" {
                return fmt.Errorf("cannot have --temporalWfName with --noTemporal")
            }
            if noTemporal && workerName != "" {
                return fmt.Errorf("cannot have --workerName with --noTemporal")
            }
            return nil
        },
        RunE: func(cmd *cobra.Command, args []string) error {
            if !cmd.Flags().Changed("ram") {
                vmStat, err := mem.VirtualMemory()
                if err != nil {
                    return fmt.Errorf("failed to get system RAM w/ error: %s", err)
                }

                workerRam.SetBytes(int(math.Floor(
                    float64(vmStat.Total) * float64(0.7),
                )))
                fmt.Printf(
                    "No worker RAM value set, defaulting to 70%% of system " +
                    "total: %d MB\n", workerRam / 1024 / 1024,
                )
            }

            if !cmd.Flags().Changed("cpus") {
                totalCpus := runtime.NumCPU()
                workerCpus = max(totalCpus / 2, 1)
                fmt.Printf(
                    "No worker CPUs value set, defaulting to 50%% of system " +
                    "total: %d\n", workerCpus,
                )
            }

            return runWorkflow(
                args[0], workerName, temporalWfName, storageId,
                paramOverrides, noTemporal, softFail, checkptPath,
                workerRam, workerCpus, workerGpus,
            )
        },
    }
    runCmd.Flags().StringVar(
        &workerName, "workerName", "", "Temporal queue name of local worker. " +
        "Cannot be used in conjunction with --noTemporal.",
    )
    runCmd.Flags().StringVar(
        &temporalWfName, "temporalWfName", "", "Run ID for temporal workflow. " +
        "Cannot be used in conjunction with --noTemporal.",
    )
    runCmd.Flags().StringVar(
        &storageId, "storageId", "", "Storage ID of the workflow. " +
        "Workflows run with the same storage ID (and the same master FS) "+
        "will share filesystems.",
    )
    runCmd.Flags().BoolVar(
        &noTemporal, "noTemporal", false, "Run workflow without temporal",
    )
    runCmd.Flags().BoolVar(
        &softFail, "softFail", false, 
        "Continue running workflow after encountering individual job failures," +
        "until all cmds not dependant on the failed job have run.",
    )
    runCmd.Flags().StringArrayVarP(
        &paramOverrides, "param", "p", []string{}, 
        "String of form [NODE_NAME].[PARAM_NAME]=[VAL], where NODE_NAME is " +
        "the title or numeric ID of a node defined in the workflow JSON, " +
        "PARAM_NAME is one of its parameters, and value is a JSON-parsable " +
        "string giving the desired value of this parameter. This will " +
        "override the default parameters given in the JSON.",
    )
    runCmd.Flags().Var(
        &workerRam, "ram", "Max amount of RAM to use for the local worker " +
        "as a string formed like \"10GB\", \"500MB\", etc. Default is 70% of " +
        "system RAM.",
    )
    runCmd.Flags().IntVar(
        &workerCpus, "cpus", 0, "Max number of CPU cores to use on local worker.",
    )
    runCmd.Flags().IntVar(
        &workerGpus, "gpus", 0, "Max number of GPUs to use on local worker.",
    )


    dryRunCmd := &cobra.Command{
        Use:   "dryRun [WORKFLOW_FILE]",
        Short: "Print dry run of JSON-serialized BWB workflow",
        Args: cobra.ExactArgs(1),
        RunE: func(cmd *cobra.Command, args []string) error {
            return dryRunWorkflow(args[0], paramOverrides)
        },
    }
    dryRunCmd.Flags().StringArrayVarP(
        &paramOverrides, "param", "p", []string{}, 
        "String of form [NODE_NAME].[PARAM_NAME]=[VAL], where NODE_NAME is " +
        "the title or numeric ID of a node defined in the workflow JSON, " +
        "PARAM_NAME is one of its parameters, and value is a JSON-parsable " +
        "string giving the desired value of this parameter. This will " +
        "override the default parameters given in the JSON.",
    )


    var outPath string
    convertOWSCmd := &cobra.Command{
        Use:   "convertOWS [OWS_DIR_PATH]",
        Short: "Convert workflow in BWB's OWS format to scheduler JSON format.",
        Args: cobra.ExactArgs(1),
        RunE: func(cmd *cobra.Command, args []string) error {
            return convertOWSWorkflow(args[0], outPath)
        },
    }
    convertOWSCmd.Flags().StringVarP(
        &outPath, "output", "o", "", 
        "File path where the converted workflow will be stored in JSON format.",
    )
    convertOWSCmd.MarkFlagRequired("output")

    rootCmd.AddCommand(runCmd)
    rootCmd.AddCommand(dryRunCmd)
    rootCmd.AddCommand(convertOWSCmd)
    if err := rootCmd.Execute(); err != nil {
        log.Fatalln(err)
    }
}