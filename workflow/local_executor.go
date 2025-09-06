package workflow

import (
	"context"
	"go-scheduler/fs"
	"go-scheduler/parsing"
	"log/slog"
	"sync"
)

type LocalExecutor struct {
    ctx                 context.Context
    waitGroup           *sync.WaitGroup
    handleFinishedCmd   CmdHandler
    masterFS            fs.LocalFS
    localWorker         WorkerInfo
    storageId           string
    logger              slog.Logger
    reqGrantChan        chan ResourceRequest
    releaseGrantChan    chan ResourceGrant
    recvGrantChan       chan ResourceGrant
    doneChan            chan bool
    cmdResChan          chan struct {
        result CmdOutput
        err    error
    }
    cmdsById            map[int]parsing.CmdTemplate
    grantsById          map[int]ResourceGrant
    configsByNode       map[int]parsing.LocalJobConfig
    errors              []error
}

func NewLocalExecutor(
    ctx context.Context, cmdMan *parsing.CmdManager, 
    masterFS fs.LocalFS, worker WorkerInfo, storageId string,
    configsByNode map[int]parsing.LocalJobConfig,
    logger slog.Logger,
) LocalExecutor {
    var state LocalExecutor
    var wg sync.WaitGroup
    state.ctx = ctx
    state.waitGroup = &wg
    state.masterFS = masterFS
    state.localWorker = worker
    state.storageId = storageId
    state.logger = logger
    state.configsByNode = configsByNode
    state.grantsById = make(map[int]ResourceGrant)
    state.cmdsById = make(map[int]parsing.CmdTemplate)
    state.grantsById = make(map[int]ResourceGrant)
    state.errors = make([]error, 0)
    state.reqGrantChan = make(chan ResourceRequest)
    state.releaseGrantChan = make(chan ResourceGrant)
    state.recvGrantChan = make(chan ResourceGrant)
    state.doneChan = make(chan bool)
    state.cmdResChan = make(chan struct {
        result CmdOutput
        err    error
    })
    return state
}

func (exec *LocalExecutor) Setup() error {
    workers := map[string]WorkerInfo{"local": exec.localWorker}
    go LocalResourceScheduler(
        exec.logger, workers, exec.reqGrantChan,
        exec.releaseGrantChan, exec.recvGrantChan,
        exec.doneChan,
    )

    // Setup worker FS.
    volumes, err := fs.SetupVolumes(exec.storageId)
    if err != nil {
        return err
    }
    exec.masterFS.Volumes = volumes
    return nil
}

func (exec *LocalExecutor) SetCmdHandler(handler CmdHandler) {
    exec.handleFinishedCmd = handler
}

func (exec *LocalExecutor) Select() {
    select {
    case grant := <-exec.recvGrantChan: {
        exec.grantsById[grant.RequestId] = grant
        cmd := exec.cmdsById[grant.RequestId]
        exec.RunCmdWithGrant(cmd, grant)
    }
    case res := <-exec.cmdResChan: {
        cmd, cmdOk := exec.cmdsById[res.result.Id]
        grant, grantOk := exec.grantsById[res.result.Id]
        if !cmdOk || !grantOk {
            exec.logger.Warn(
                "Received result for non-existent CMD ID", "cmdId", res.result.Id,
            )
        }

        if res.err != nil {
            exec.errors = append(exec.errors, res.err)
            return
        }

        if err := exec.ReleaseResourceGrant(grant); err != nil {
            exec.errors = append(exec.errors, err)
            return
        }

        // This absolutely needs to be done in a "select" of the main
        // thread rather than in the finished command's goroutine, since
        // none of these data-structures have been made thread-safe.
        exec.handleFinishedCmd(res.result, res.err, exec, cmd)
    }
    }
}

func (exec *LocalExecutor) Shutdown() {
    exec.doneChan <- true
    exec.waitGroup.Wait()
}

func (exec *LocalExecutor) GetErrors() []error {
    return exec.errors
}

func (exec *LocalExecutor) RunCmds(
    cmds []parsing.CmdTemplate,
) {
    for _, cmd := range cmds {
        exec.cmdsById[cmd.Id] = cmd
        req := ResourceRequest{
            Rank:         cmd.Priority,
            Id:           cmd.Id,
            Requirements: cmd.ResourceReqs,
        }

        exec.reqGrantChan <- req
    }
}

func (exec *LocalExecutor) ReleaseResourceGrant(
    grant ResourceGrant,
) error {
    exec.releaseGrantChan <- grant
    return nil
}

func (exec *LocalExecutor) BuildImages(imageNames []string) error {
    for _, imageName := range imageNames {
        exec.logger.Info("Building image", "imageName", imageName)
        if _, err := BuildSingularitySIF(imageName); err != nil {
            return err
        }
    }
    return nil
}

func (exec *LocalExecutor) RunCmdWithGrant(
    cmd parsing.CmdTemplate, grant ResourceGrant,
) {
    volumes := exec.masterFS.GetVolumes()
    useDocker := exec.configsByNode[cmd.NodeId].UseDocker
    exec.waitGroup.Add(1)
    go func() {
        result, err := RunCmd(exec.ctx, volumes, cmd, useDocker)
        exec.waitGroup.Done()
        exec.cmdResChan <- struct {
            result CmdOutput
            err    error
        }{
            result: result,
            err:    err,
        }
    }()
}

func (exec *LocalExecutor) Glob(
    root, pattern string,
    findFile, findDir bool,
) ([]string, error) {
    return fs.GlobActivity(
        exec.masterFS, root, pattern, findFile, findDir,
    )
}
