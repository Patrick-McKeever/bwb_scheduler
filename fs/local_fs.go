package fs

import (
	"fmt"
	"os"
	"path/filepath"
)

type LocalFS struct {
    Volumes     map[string]string
}

func (fs LocalFS) GetVolumes() map[string]string {
    return fs.Volumes
}

func (fs LocalFS) Glob(
    root, pattern string, includeFiles, includeDirs bool,
) ([]string, error) {
    hostRootPath, ok := getHostPath(root, fs.Volumes)
    if !ok {
        return nil, fmt.Errorf("invalid root path %s", hostRootPath)
    }

    fullPattern := filepath.Join(hostRootPath, pattern)
    matches, err := filepath.Glob(fullPattern)
    if err != nil {
        return nil, fmt.Errorf("error glob-ing %s: %s", fullPattern, err)
    }

    ret := make([]string, 0)
    for _, match := range matches {
        stat, err := os.Stat(match)
        if err != nil {
            return nil, fmt.Errorf("error stat-ing %s: %s", stat, err)
        }
        
        isFile := stat.Mode().IsRegular()
        if (stat.IsDir() && includeDirs) || (isFile && includeFiles) {
            matchCntPath, ok := getCntPath(match, fs.Volumes)
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