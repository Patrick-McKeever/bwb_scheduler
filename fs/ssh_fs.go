package fs

import (
    "fmt"
    "bytes"
    "os/exec"
)


type CmdOut struct {
    ExitCode    int
    StdOut      string
    StdErr      string
}
type CmdRunner func(string) (CmdOut, error)
type SshFS struct {
    User            string
    Endpt           string
    RemoteVolumes   map[string]string
    LocalVolumes    map[string]string
}

func NewSshFS(
    user, endpt string, remoteVolumes, localVolumes map[string]string,
) SshFS {
    return SshFS{
        User: user,
        Endpt: endpt,
        RemoteVolumes: remoteVolumes,
        LocalVolumes: localVolumes,
    }
}

func LocalRunCmd(cmdStr string) (CmdOut, error) {
    var stdout, stderr bytes.Buffer
    cmd := exec.Command("sh", "-c", cmdStr)
    cmd.Stdout = &stdout
    cmd.Stderr = &stderr
    
    var err error
    exitCode := 0
    if err = cmd.Run(); err != nil {
        if exitErr, ok := err.(*exec.ExitError); ok {
            exitCode = exitErr.ExitCode()
        }
    }

    return CmdOut{
        ExitCode: exitCode,
        StdOut: stdout.String(),
        StdErr: stderr.String(),
    }, err
}

func (fs SshFS) GetVolumes() map[string]string {
    return fs.RemoteVolumes
}

func (fs SshFS) TranslatePath(path string) (string, bool) {
    return GetHostPath(path, fs.RemoteVolumes)
}

func (fs SshFS) Glob(
    root, pattern string, includeFiles, includeDirs bool,
) ([]string, error) {
    return nil, nil
    //hostRootPath, ok := GetHostPath(root, fs.RemoteVolumes)
    //if !ok {
    //    return nil, fmt.Errorf("invalid root path %s", hostRootPath)
    //}

    //fullPattern := filepath.Join(hostRootPath, pattern)
    
    //var typeStr string
    //if includeFiles && includeDirs {
    //    typeStr = ""
    //} else if includeFiles {
    //    typeStr = "-type f"
    //} else if includeDirs {
    //    typeStr = "-type d"
    //}

    //var findOut CmdOut
    //findCmd := fmt.Sprintf("find -name %s %s", fullPattern, typeStr)
    //findOut, err := fs.RemoteRunCmd(findCmd)
    //if err != nil {
    //    return nil, err
    //}

    //out := make([]string, 0)
    //lines := strings.Split(findOut.StdOut, "\n")
    //for _, line := range lines {
    //    if line == "" {
    //        continue
    //    }
    //    cntPath, ok := GetCntPath(line, fs.RemoteVolumes)
    //    if !ok {
    //        return nil, fmt.Errorf(
    //            "couldn't convert host path %s to cnt path with volumes %#v",
    //            line, fs.RemoteVolumes,
    //        )
    //    }

    //    out = append(out, cntPath)
    //}

    //return out, nil
}

func (fs SshFS) Upload(src, dst string) error {
    rsyncCmdStr := fmt.Sprintf(
        "rsync --mkpath -av -e ssh %s %s@%s:%s",
        src, fs.User, fs.Endpt, dst,
    )
    _, err := LocalRunCmd(rsyncCmdStr)
    if err != nil {
        return fmt.Errorf("\"%s\" failed: %s", rsyncCmdStr, err)
    }
    return nil
}

func (fs SshFS) Download(src, dst string) error {
    rsyncCmdStr := fmt.Sprintf(
        "rsync --mkpath -av -e ssh %s@%s:%s %s",
        fs.User, fs.Endpt, src, dst,
    )
    _, err := LocalRunCmd(rsyncCmdStr)
    if err != nil {
        return fmt.Errorf("\"%s\" failed: %s", rsyncCmdStr, err)
    }
    return nil
}

func (fs SshFS) UploadCntFile(localSrcHostPath string, remoteDst string) error {
    remoteDstHostPath, ok := GetHostPath(remoteDst, fs.RemoteVolumes)
    if !ok {
        return fmt.Errorf("invalid remote dest path %s", remoteDst)
    }

    return fs.Upload(localSrcHostPath, remoteDstHostPath)
}

func (fs SshFS) DownloadCntFile(remoteSrc string, localDstHostPath string) error {
    remoteSrcHostPath, ok := GetHostPath(remoteSrc, fs.RemoteVolumes)
    if !ok {
        return fmt.Errorf("invalid remote source path %s", remoteSrc)
    }

    return fs.Download(remoteSrcHostPath, localDstHostPath)
}

func SshUploadActivity(fs SshFS, src, dst string) error {
    return fs.Upload(src, dst)
}

func SshDownloadActivity(fs SshFS, src, dst string) error {
    return fs.Download(src, dst)
}