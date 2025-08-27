package workflow

import (
	"fmt"
	"go-scheduler/fs"
	"go-scheduler/parsing"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

// When SlurmRemoteExecutor.RunCmds is called on a command with input files,
// require that these files are downloaded *before* a signal is sent to the
// slurm poller workflow to start the SLURM job.
func TestSlurmExecutorFileDownloads(t *testing.T) {
    testSuite := &testsuite.WorkflowTestSuite{}
    env := testSuite.NewTestWorkflowEnvironment()

    // Register activities invoked during setup as NO-OP.
    var a SlurmActivity
    env.RegisterActivity(fs.TransferLocalToSshFS)
    env.RegisterActivity(a.ExecCmd)
    env.OnActivity(a.ExecCmd, mock.Anything).Return(CmdOut{}, nil)
    storageId := "storageId"
    hostDir := "/hostDir"
    cntDir := "/cntDir"
    remoteDir := "/remoteDir"
    xferFileCntPath := filepath.Join(cntDir, "file")
    localFs := fs.LocalFS{Volumes: map[string]string{cntDir: hostDir}}

    finishedTransfer := false
    env.OnActivity(
        fs.TransferLocalToSshFS, storageId, localFs,
        mock.Anything, []string{xferFileCntPath},
    ).Return(func(string, fs.LocalFS, fs.SshFS, []string) error {
        finishedTransfer = true
        return nil
    }).Once()


    cmdToRun := parsing.CmdTemplate{
        Id:     1,
        NodeId: 2,
        InFiles: map[string][]string{
            "file1": {xferFileCntPath},
        },
    }
    memLval := "40G"
    configForCmd := parsing.SlurmJobConfig{
        Mem: &memLval,
    }

    // NOTE: The temporal docs don't say this, but you can't use 
    // OnSignalExternalWorkflow between parent and child workflows
    // in the same testing env, even though the reverse is fine.
    // You have to actually register a mock child workflow.
    receivedSignal := false
    env.RegisterWorkflowWithOptions(func(ctx workflow.Context, _ SlurmState) error {
        selector := workflow.NewSelector(ctx)
        selector.AddReceive(
            workflow.GetSignalChannel(ctx, "slurm-request"), 
            func(c workflow.ReceiveChannel, _ bool) {
                var req SlurmRequest
                if ok := c.Receive(ctx, &req); !ok {
                    t.Fatalf("could not convert signal contents to slurm req")
                }
                receivedSignal = true
                require.Equal(t, req.Cmd, cmdToRun)
                require.Equal(t, req.Config, configForCmd)
                require.True(t, finishedTransfer)
            },
        )
        workflow.Sleep(ctx, 100*time.Second)
        selector.Select(ctx)
        return nil
    }, workflow.RegisterOptions{
        Name: "SlurmPollerWorkflow",
    })


    testCmdRunWf := func(ctx workflow.Context) error {
        slurmExec := NewSlurmRemoteExecutor(
            ctx, localFs, storageId,
            map[int]parsing.SlurmJobConfig{cmdToRun.NodeId: configForCmd},
            parsing.SshConfig{SchedDir: remoteDir},
        )
        
        // Part of the "contract" of executors is that Setup()
        // gets called before trying to run commands.
        if err := slurmExec.Setup(); err != nil {
            return fmt.Errorf("setup failed: %s", err)
        }
        slurmExec.RunCmds([]parsing.CmdTemplate{cmdToRun})

        // Call select a few times to trigger signal-handling logic.
        // I don't know of a better way to test this, but this corresponds
        // to the actual lifecycle of an executor.
        for i := 0; i < 10; i++ {
            slurmExec.Select()
        }
        return nil
    }
    env.RegisterWorkflow(testCmdRunWf)
    env.ExecuteWorkflow(testCmdRunWf)
    err := env.GetWorkflowError()
    if err != nil {
        t.Fatalf("workflow failed w/ err %s", err)
    }

    env.AssertExpectations(t)
    require.True(t, receivedSignal)
}


// When a SLURM command completes, its output files should
// be transferred to the master FS *before* completed CMD 
// handler is invoked.
func TestSlurmExecutorFileUploads(t *testing.T) {
    // Start mock workflow and mock child workflow
    // Have it run CMDs, w/ child handler handling CMD req w/
    // immediate response.
    testSuite := &testsuite.WorkflowTestSuite{}
    env := testSuite.NewTestWorkflowEnvironment()

    var a SlurmActivity
    env.RegisterActivity(fs.TransferLocalToSshFS)
    env.RegisterActivity(a.ExecCmd)
    env.OnActivity(a.ExecCmd, mock.Anything).Return(CmdOut{}, nil)
    storageId := "storageId"
    hostDir := "/hostDir"
    cntDir := "/cntDir"
    remoteDir := "/remoteDir"
    slurmFileToXfer := "/cntDir/file"
    localFs := fs.LocalFS{Volumes: map[string]string{cntDir: hostDir}}

    finishedTransfer := false
    env.OnActivity(
        fs.TransferSshToLocalFS, storageId, mock.Anything,
        localFs, []string{slurmFileToXfer},
    ).Return(func(string, fs.SshFS, fs.LocalFS, []string) error {
        finishedTransfer = true
        return nil
    }).Once()


    // Mock SlurmPollerWorkflow with a stub that immediately reports successful
    // completion of any job requested, along with output files.
    requiredOutfiles := []string{slurmFileToXfer}
    env.RegisterWorkflowWithOptions(func(ctx workflow.Context, state SlurmState) error {
        selector := workflow.NewSelector(ctx)
        selector.AddReceive(
            workflow.GetSignalChannel(ctx, "slurm-request"), 
            func(c workflow.ReceiveChannel, _ bool) {
                var req SlurmRequest
                if ok := c.Receive(ctx, &req); !ok {
                    t.Fatalf("could not convert signal contents to slurm req")
                }
                workflow.SignalExternalWorkflow(
                    ctx, state.ParentWfId, state.ParentWfRunId, "slurm-response",
                    SlurmResponse{
                        Result: CmdOutput{
                            Id: req.Cmd.Id, 
                            OutputFiles: requiredOutfiles,
                        }, Error: nil,
                    },
                )
            },
        )
        workflow.Sleep(ctx, 100*time.Second)
        selector.Select(ctx)
        return nil
    }, workflow.RegisterOptions{
        Name: "SlurmPollerWorkflow",
    })

    // This will be set as handler for finished CMD and will
    // verify that transfer took place prior to invocation. 
    cmdHandlerWasInvoked := false
    mockCmdHandler := func(co CmdOutput, err error, e Executor, ct parsing.CmdTemplate) {
        cmdHandlerWasInvoked = true
        require.True(t, finishedTransfer)
    }

    testCmdRunWf := func(ctx workflow.Context) error {
        cmdToRun := parsing.CmdTemplate{ Id: 1, NodeId: 2 }
        memLval := "40G"
        configForCmd := parsing.SlurmJobConfig{
            Mem: &memLval,
        }

        slurmExec := NewSlurmRemoteExecutor(
            ctx, localFs, storageId,
            map[int]parsing.SlurmJobConfig{cmdToRun.NodeId: configForCmd},
            parsing.SshConfig{SchedDir: remoteDir},
        )
        slurmExec.SetCmdHandler(mockCmdHandler)
        
        // Part of the "contract" of executors is that Setup()
        // gets called before trying to run commands.
        if err := slurmExec.Setup(); err != nil {
            fmt.Printf("setup failed: %s", err)
            return fmt.Errorf("setup failed: %s", err)
        }
        slurmExec.RunCmds([]parsing.CmdTemplate{cmdToRun})

        // Call select a few times to trigger signal-handling logic.
        // I don't know of a better way to test this, but this corresponds
        // to the actual lifecycle of an executor.
        for i := 0; i < 10; i++ {
            slurmExec.Select()
        }
        return nil
    }
    env.RegisterWorkflow(testCmdRunWf)
    env.ExecuteWorkflow(testCmdRunWf)
    err := env.GetWorkflowError()
    if err != nil {
        t.Fatalf("workflow failed w/ err %s", err)
    }

    env.AssertExpectations(t)
    require.True(t, cmdHandlerWasInvoked)
}


// When the underlying slurm workflow fails, SlurmExecutor.GetErrors()
// should eventually return a corresponding error.
func TestSlurmExecutorChildWorkflowFailure(t *testing.T) {
    testSuite := &testsuite.WorkflowTestSuite{}
    env := testSuite.NewTestWorkflowEnvironment()

    var a SlurmActivity
    env.RegisterActivity(fs.TransferLocalToSshFS)
    env.RegisterActivity(a.ExecCmd)
    env.OnActivity(a.ExecCmd, mock.Anything).Return(CmdOut{}, nil)


    // NOTE: The temporal docs don't say this, but you can't use 
    // OnSignalExternalWorkflow between parent and child workflows
    // in the same testing env, even though the reverse is fine.
    // You have to actually register a mock child workflow.
    env.RegisterWorkflowWithOptions(func(ctx workflow.Context, _ SlurmState) error {
        return fmt.Errorf("generic mocked workflow error")
    }, workflow.RegisterOptions{
        Name: "SlurmPollerWorkflow",
    })


    testCmdRunWf := func(ctx workflow.Context) error {
        slurmExec := NewSlurmRemoteExecutor(
            ctx, fs.LocalFS{}, "", nil, parsing.SshConfig{},
        )
        
        // Part of the "contract" of executors is that Setup()
        // gets called before trying to run commands.
        if err := slurmExec.Setup(); err != nil {
            fmt.Printf("setup failed: %s", err)
            return fmt.Errorf("setup failed: %s", err)
        }

        // Call select a few times to trigger signal-handling logic.
        // I don't know of a better way to test this, but this corresponds
        // to the actual lifecycle of an executor.
        var errs []error = nil
        for i := 0; i < 10; i++ {
            slurmExec.Select()
            if errs = slurmExec.GetErrors(); len(errs) > 0 {
                break
            }
        }

        if len(errs) == 0 {
            return nil
        }
        return fmt.Errorf("got errors")
    }
    env.RegisterWorkflow(testCmdRunWf)
    env.ExecuteWorkflow(testCmdRunWf)
    err := env.GetWorkflowError()
    if err == nil {
        t.Fatalf("GetErrors failed to return errors after child WF failure")
    }
}