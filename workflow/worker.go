package workflow

import (
    "fmt"
    "strings"
    "go-scheduler/fs"
    "go-scheduler/parsing"
    "net"
    "log"
    "os"
    "os/exec"
    "os/user"
    "path/filepath"

    "go.temporal.io/sdk/client"
    "go.temporal.io/sdk/worker"
    "golang.org/x/crypto/ssh"
    "golang.org/x/crypto/ssh/knownhosts"
    "golang.org/x/crypto/ssh/agent"
)

func checkDeps() error {
    missingDeps := make([]string, 0)
    if _, err := exec.LookPath("rsync"); err != nil {
        missingDeps = append(missingDeps, "rsync")
    }
    if _, err := exec.LookPath("singularity"); err != nil {
        missingDeps = append(missingDeps, "singularity")
    }

    if len(missingDeps) == 0 {
        return nil
    }
    return fmt.Errorf("missing required binaries %s", strings.Join(missingDeps, ", "))
}

func StartWorkers(
    c client.Client, config parsing.JobConfig,
    queueName string, cancelChan chan any,
) error {
    if err := checkDeps(); err != nil {
        return err
    }

    go StartWorker(c, SCHEDULER_QUEUE, true, cancelChan)
    if len(config.TemporalConfigsByNode) > 0 {
        go StartWorker(c, queueName, false, cancelChan)
    }

    if len(config.SlurmConfigsByNode) > 0 {
        go StartSlurmWorker(c, config.SlurmExecutor, cancelChan)
    }

    return nil
}

func StartWorker(
    c client.Client, queueName string, scheduler bool,
    cancelChan chan any,
) {
    w := worker.New(c, queueName, worker.Options{})

    if scheduler {
        w.RegisterWorkflow(RunBwbWorkflow)
        w.RegisterWorkflow(ResourceSchedulerWorkflow)
        w.RegisterActivity(BuildSingularitySIF)
        w.RegisterActivity(fs.GlobActivity[fs.LocalFS])
    } else {
        w.RegisterActivity(WorkerHeartbeatActivity)
        w.RegisterActivity(RunCmdActivity)
        w.RegisterActivity(fs.SetupVolumes)
    }

    log.Println("Starting worker...")
    err := w.Run(cancelChan)
    if err != nil {
        log.Fatalln("Unable to start worker", err)
    }
}

func StartSlurmWorker(
    c client.Client, config parsing.SshConfig, cancelChan chan any,
) {
    sshClient, connConfig, err := getSshConnection(config)
    if err != nil {
        log.Fatalf(
            "error establishing ssh client to %s@%s: %s\n", 
            config.User, config.IpAddr, err,
        )
    }

    defer sshClient.Close()

    slurmActivityObj := SlurmActivity{
        Config: config,
        Client: sshClient,
        ConnConfig: connConfig,
    }

    w := worker.New(c, GetTemporalSshQueueName(config), worker.Options{
        MaxConcurrentActivityExecutionSize: 3,
    })
    w.RegisterWorkflow(SlurmPollerWorkflow)
    w.RegisterActivity(slurmActivityObj.ExecCmd)
    w.RegisterActivity(slurmActivityObj.GetRemoteSlurmJobOutputsActivity)
    w.RegisterActivity(slurmActivityObj.PollRemoteSlurmActivity)
    w.RegisterActivity(slurmActivityObj.StartRemoteSlurmJobActivity)
    w.RegisterActivity(fs.SshDownloadActivity)
    w.RegisterActivity(fs.SshUploadActivity)
    w.RegisterActivity(fs.TransferLocalToLocalFS)
    w.RegisterActivity(fs.TransferLocalToSshFS)
    w.RegisterActivity(fs.TransferSshToLocalFS)
    w.RegisterActivity(fs.TransferSshToSshFS)
    log.Println("Starting worker...")
    err = w.Run(cancelChan)
    if err != nil {
        log.Fatalln("Unable to start slurm worker", err)
    }
}

func getSshConnection(conf parsing.SshConfig) (*ssh.Client, *ssh.ClientConfig, error) {
    authMethods, err := BuildAuthMethods()
    if err != nil {
        return nil, nil, fmt.Errorf("ssh auth error: %s", err)
    }

    hostKeyCallback, err := getHostKeyCallback()
    if err != nil {
        return nil, nil, fmt.Errorf("error getting host keys: %s", err)
    }

    connConfig := &ssh.ClientConfig{
        User: conf.User,
        Auth: authMethods,
        HostKeyCallback: hostKeyCallback,
    }

    client, err := ssh.Dial("tcp", conf.IpAddr, connConfig)
    if err != nil {
        return nil, nil, fmt.Errorf("error connecting to server: %s", err)
    }
    return client, connConfig, nil
}

// Paramiko-like: try agent first, then all keys in ~/.ssh/
func BuildAuthMethods() ([]ssh.AuthMethod, error) {
    var methods []ssh.AuthMethod
    if am, ok := getAgentAuth(); ok {
        methods = append(methods, am)
    }

    keyAuth, err := getAllKeyAuth()
    if err != nil {
        return nil, err
    }
    if keyAuth != nil {
        methods = append(methods, keyAuth)
    }

    if len(methods) == 0 {
        return nil, fmt.Errorf("no usable SSH auth methods found")
    }
    return methods, nil
}

func getAgentAuth() (ssh.AuthMethod, bool) {
    sock := os.Getenv("SSH_AUTH_SOCK")
    if sock == "" {
        return nil, false
    }
    conn, err := net.Dial("unix", sock)
    if err != nil {
        return nil, false
    }
    ag := agent.NewClient(conn)
    signers, err := ag.Signers()
    if err != nil || len(signers) == 0 {
        return nil, false
    }
    return ssh.PublicKeys(signers...), true
}

// Load all usable keys from ~/.ssh/, error if a key is passphrase-protected
func getAllKeyAuth() (ssh.AuthMethod, error) {
    usr, err := user.Current()
    if err != nil {
        return nil, err
    }
    sshDir := filepath.Join(usr.HomeDir, ".ssh")
    entries, err := os.ReadDir(sshDir)
    if err != nil {
        return nil, err
    }

    var signers []ssh.Signer
    for _, e := range entries {
        if e.IsDir() {
            continue
        }
        path := filepath.Join(sshDir, e.Name())
        data, err := os.ReadFile(path)
        if err != nil {
            continue // unreadable
        }

        signer, err := ssh.ParsePrivateKey(data)
        if err != nil {
            fmt.Printf(
                "unable to read private key %s, possibly password-protected\n", 
                path,
            )
            continue
        }
        signers = append(signers, signer)
    }

    if len(signers) == 0 {
        return nil, nil
    }
    return ssh.PublicKeys(signers...), nil
}


// host key verification from known_hosts
func getHostKeyCallback() (ssh.HostKeyCallback, error) {
    usr, err := user.Current()
    if err != nil {
        return nil, err
    }
    khPath := filepath.Join(usr.HomeDir, ".ssh", "known_hosts")
    return knownhosts.New(khPath)
}