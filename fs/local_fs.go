// local_fs.go
package fs

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

type LocalFS struct {
	RootDir string
}

func (fs LocalFS) GetRootDir() string {
	// TODO: This is a hacky way to maintain compatability between v0 and v1,
	// but fix.
	return fs.RootDir
}

func (fs LocalFS) Glob(
	root, pattern string, includeFiles, includeDirs bool,
) ([]string, error) {
	hostRootPath, ok := GetV0HostPath(root, fs.RootDir)
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
			matchCntPath, ok := GetV0CntPath(match, fs.RootDir)
			if !ok {
				return nil, fmt.Errorf(
					"couldn't convert host path %s to cnt path with root dir %s",
					match, fs.RootDir,
				)
			}
			ret = append(ret, matchCntPath)
		}
	}

	return ret, nil
}

func (fs LocalFS) Copy(src, dst string) error {
    if src == dst {
        return nil
    }

    if err := os.MkdirAll(filepath.Dir(dst), 0755); err != nil {
        return fmt.Errorf("creating parent directory: %w", err)
    }

    srcFile, err := os.Open(src)
    if err != nil {
        return fmt.Errorf("opening source %q: %w", src, err)
    }
    defer srcFile.Close()

    dstFile, err := os.Create(dst)
    if err != nil {
        return fmt.Errorf("creating destination %q: %w", dst, err)
    }
    defer dstFile.Close()

    if _, err := io.Copy(dstFile, srcFile); err != nil {
        return fmt.Errorf("copying %q -> %q: %w", src, dst, err)
    }

    if err := dstFile.Sync(); err != nil {
        return fmt.Errorf("syncing %q: %w", dst, err)
    }

    return nil
}

func (fs LocalFS) Upload(src string, dst string) error {
	return fs.Copy(src, dst)
}

func (fs LocalFS) Download(src string, dst string) error {
	return fs.Copy(src, dst)
}
