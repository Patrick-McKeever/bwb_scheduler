package fs

import (
    "fmt"
    "io"
    "os"
    "path/filepath"
    "strings"
)

type LocalFS struct {
    Volumes map[string]string
}

func (fs LocalFS) GetVolumes() map[string]string {
    return fs.Volumes
}

func (fs LocalFS) TranslatePath(path string) (string, bool) {
    return GetHostPath(path, fs.Volumes)
}

func (fs LocalFS) Glob(
    root, pattern string, includeFiles, includeDirs bool,
) ([]string, error) {
    hostRootPath, ok := GetHostPath(root, fs.Volumes)
    if !ok {
        return nil, fmt.Errorf("invalid root path %s", hostRootPath)
    }

    if info, err := os.Stat(hostRootPath); err != nil {
        return nil, fmt.Errorf(
            "error resolving pattern query: root %s (host %s) does not exist",
            root, hostRootPath,
        )
    } else {
        if !info.Mode().IsDir() {
            return nil, fmt.Errorf(
                "root %s (host %s) is not a directory", root, hostRootPath,
            )
        }

    }

    fullPattern := filepath.Join(hostRootPath, pattern)
    matches, err := filepath.Glob(fullPattern)
    if err != nil {
        return nil, fmt.Errorf("error glob-ing %s: %s", fullPattern, err)
    }

    // Python BWB uses glob.glob with recursive=True, which considers
    // root/file to be a match for root/**/file, unlike golang glob.
    if strings.HasPrefix(pattern, "**/") {
        depth1Pattern := filepath.Join(hostRootPath, pattern[3:])
        depth1Matches, err := filepath.Glob(depth1Pattern)
        if err != nil {
            return nil, fmt.Errorf("error glob-ing %s: %s", depth1Pattern, err)
        }
        matches = append(matches, depth1Matches...)
    }

    ret := make([]string, 0)
    for _, match := range matches {
        stat, err := os.Stat(match)
        if err != nil {
            return nil, fmt.Errorf("error stat-ing %s: %s", stat, err)
        }

        isFile := stat.Mode().IsRegular()
        if (stat.IsDir() && includeDirs) || (isFile && includeFiles) {
            matchCntPath, ok := GetCntPath(match, fs.Volumes)
            if !ok {
                return nil, fmt.Errorf(
                    "couldn't convert host path %s to cnt path with volumes %#v",
                    match, fs.Volumes,
                )
            }
            ret = append(ret, matchCntPath)
        }
    }

    return ret, nil
}

func (fs LocalFS) Copy(src string, dst string) error {
    parentDstDir := filepath.Dir(dst)
    if err := os.MkdirAll(parentDstDir, 0o755); err != nil {
        return fmt.Errorf(
            "unable to make parent dir %s of dst %s: %s",
            parentDstDir, dst, err,
        )
    }

    srcFile, err := os.Open(src)
    if err != nil {
        return fmt.Errorf(
            "unable to create source file %s", src,
        )
    }
    defer srcFile.Close()

    dstFile, err := os.Open(src)
    if err != nil {
        return fmt.Errorf(
            "unable to create dst file %s", dst,
        )
    }
    defer dstFile.Close()

    if _, err = io.Copy(dstFile, srcFile); err != nil {
        return fmt.Errorf(
            "error copying from src %s to dst %s: %s",
            src, dst, err,
        )
    }

    if err = dstFile.Sync(); err != nil {
        return fmt.Errorf(
            "error syncing dst %s to disk: %s",
            dst, err,
        )
    }

    return nil
}

// src is taken to be an absolute path on the current
// filesystem; dst is a container path which will be
// translated according to LocalFS.Volumes.
func (fs LocalFS) UploadCntFile(src string, dst string) error {
    hostDstPath, ok := GetHostPath(dst, fs.Volumes)
    if !ok {
        return fmt.Errorf("invalid dst path %s", dst)
    }
    return fs.Copy(src, hostDstPath)
}

// src is taken to be a container path on fs, which will
// be translated according to fs.Volumes, wehereas dst
// is taken to be a 
func (fs LocalFS) DownloadCntFile(src string, dst string) error {
    hostSrcPath, ok := GetHostPath(src, fs.Volumes)
    if !ok {
        return fmt.Errorf("invalid source path %s", src)
    }

    return fs.Copy(hostSrcPath, dst)
}

func (fs LocalFS) Upload(src string, dst string) error {
    return fs.Copy(src, dst)
}

func (fs LocalFS) Download(src string, dst string) error {
    return fs.Copy(src, dst)
}
