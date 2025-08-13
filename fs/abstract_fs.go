package fs

import (
	"path/filepath"
)

type AbstractFileSystem interface {
    GetVolumes() (map[string]string)
	// Glob impl with string include dirs, include files as args.
    Glob(string, string, bool, bool) ([]string, error)
}

type DummyFS struct{}

func (_ DummyFS) GetVolumes() map[string]string { return nil }
func (_ DummyFS) Glob(string, _ string, _, _ bool) ([]string, error)  { return nil, nil }

// Returns: In-container path of volume to which the longest
// matching host path is mounted, plus the portion of path which
// overlaps with a mounted dir.
func getMntPtPrefix(
    path string, volumes map[string]string, isCntPath bool,
) (string, string, bool) {
	longestMatch := -1
	longestSharedPrefix := ""
    longestCorrespondingPath := ""

    for volCntPath := range volumes {
        var pathToCompare string
        if isCntPath {
            pathToCompare = filepath.Clean(volCntPath)
        } else {
            pathToCompare = filepath.Clean(volumes[volCntPath])
        }

        if len(path) < len(pathToCompare) {
            continue
        }
        
        matches := path[:len(pathToCompare)] == pathToCompare
        if matches && len(pathToCompare) > longestMatch {
            longestMatch = len(pathToCompare)
            longestSharedPrefix = pathToCompare

            if isCntPath {
                longestCorrespondingPath = filepath.Clean(volumes[volCntPath])
            } else {
                longestCorrespondingPath = filepath.Clean(volCntPath)
            }
        }
    }

    return longestSharedPrefix, longestCorrespondingPath, longestMatch > 0
}

func getHostPath(
    cntPath string, volumes map[string]string,
) (string, bool) {
    cntPathPrefix, mntPt, ok := getMntPtPrefix(cntPath, volumes, true)
    if !ok {
        return "", false
    }

    hostPath := filepath.Join(mntPt, cntPath[len(cntPathPrefix):])
    return hostPath, true
}

func getCntPath(
    hostPath string, volumes map[string]string,
) (string, bool) {
    hostPathPrefix, cntPathPrefix, ok := getMntPtPrefix(hostPath, volumes, false)
    if !ok {
        return "", false
    }

    cntPath := filepath.Join(cntPathPrefix, hostPath[len(hostPathPrefix):])
    return cntPath, true
}

// If fullMathc is set, a string from candidates will be returned
// only if the whole string is a prefix of strWithPrefix.
func longestPrefix(
    strWithPrefix string, candidates []string, fullMatch bool,
) (string, int) {
	longestMatch := 0
	longestMatchingStr := ""

	for _, candidate := range candidates {
		if fullMatch && len(strWithPrefix) < len(candidate) {
			continue
		}

		currentMatch := 0
		for i := 0; i < min(len(strWithPrefix), len(candidate)); i++ {
			if candidate[i] != strWithPrefix[i] {
				break
			}
			currentMatch += 1
		}

		if currentMatch > longestMatch {
			longestMatch = currentMatch
			longestMatchingStr = candidate
		}
	}

	return longestMatchingStr, longestMatch
}

func GlobActivity[T AbstractFileSystem](
    fs T, root, pattern string, findFile, findDir bool,
) ([]string, error) {
    return fs.Glob(root, pattern, findFile, findDir)
}
