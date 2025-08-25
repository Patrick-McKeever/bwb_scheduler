package fs

import (
	"fmt"
	"os"
	"path/filepath"

	"go.temporal.io/sdk/workflow"
)

type AbstractFileSystem interface {
	GetVolumes() map[string]string
	// Glob impl with string include dirs, include files as args.
	Glob(string, string, bool, bool) ([]string, error)
	Upload(string, string) error
	Download(string, string) error
	UploadCntFile(string, string) error
	DownloadCntFile(string, string) error
    TranslatePath(string) (string, bool)
}

type DummyFS struct{}

func (DummyFS) GetVolumes() map[string]string                 { return nil }
func (DummyFS) Glob(_, _ string, _, _ bool) ([]string, error) { return nil, nil }
func (DummyFS) Upload(_, _ string) error                      { return nil }
func (DummyFS) Download(_, _ string) error                    { return nil }
func (DummyFS) UploadCntFile(_, _ string) error                      { return nil }
func (DummyFS) DownloadCntFile(_, _ string) error                    { return nil }
func (DummyFS) TranslatePath(string) (string, bool)           { return "", true }

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

func GetHostPath(
	cntPath string, volumes map[string]string,
) (string, bool) {
	cntPathPrefix, mntPt, ok := getMntPtPrefix(cntPath, volumes, true)
	if !ok {
		return "", false
	}

	hostPath := filepath.Join(mntPt, cntPath[len(cntPathPrefix):])
	return hostPath, true
}

func GetCntPath(
	hostPath string, volumes map[string]string,
) (string, bool) {
	hostPathPrefix, cntPathPrefix, ok := getMntPtPrefix(hostPath, volumes, false)
	if !ok {
		return "", false
	}

	cntPath := filepath.Join(cntPathPrefix, hostPath[len(hostPathPrefix):])
	return cntPath, true
}

func TransferViaIntermediary(storageId string, files []string, srcFS, dstFS AbstractFileSystem) error {
    localVolumes, err := SetupVolumes(storageId)
    if err != nil {
        return fmt.Errorf("error establishing intermediate FS: %s", err)
    }
    localIntermediateFS := LocalFS{Volumes: localVolumes}

    for _, file := range files {
        localPath, ok := localIntermediateFS.TranslatePath(file)
        if !ok {
            return fmt.Errorf(
                "could not translate container path %s w/ volumes %#v; files %#v", 
                file, localVolumes, files,
            )
        }

        // Download file to intermediate FS if it does not exist
        if _, statErr := os.Stat(localPath); statErr != nil {
            if downloadErr := srcFS.Download(file, localPath); downloadErr != nil {
                return fmt.Errorf(
                    "error downloading %s to local path %s: %s",
                    file, localPath, downloadErr,
                )
            }
        }

        if uploadErr := dstFS.Upload(localPath, file); uploadErr != nil {
            return fmt.Errorf(
                "error uploading local path %s to %s: %s",
                localPath, file, uploadErr,
            )
        }
    }
    
    return nil
}

func SetupVolumes(storageId string) (map[string]string, error) {
    schedDir := os.Getenv("BWB_SCHED_DIR")
    dataDir := filepath.Join(schedDir, storageId)

	if err := os.MkdirAll(dataDir, 0755); err != nil {
	    return nil, fmt.Errorf("failed to create dir %s: %s", dataDir, err)
	}

    return map[string]string{"/data": dataDir}, nil
}

func GlobActivity[T AbstractFileSystem](
	fs T, root, pattern string, findFile, findDir bool,
) ([]string, error) {
	return fs.Glob(root, pattern, findFile, findDir)
}

func TransferLocalToSshFS(storageId string, srcFS LocalFS, dstFS SshFS, files []string) error {
    return TransferViaIntermediary(storageId, files, srcFS, dstFS)
}

func TransferLocalToLocalFS(storageId string, srcFS LocalFS, dstFS LocalFS, files []string) error {
    return TransferViaIntermediary(storageId, files, srcFS, dstFS)
}

func TransferSshToLocalFS(storageId string, srcFS SshFS, dstFS LocalFS, files []string) error {
    return TransferViaIntermediary(storageId, files, srcFS, dstFS)
}

func TransferSshToSshFS(storageId string, srcFS, dstFS LocalFS, files []string) error {
    return TransferViaIntermediary(storageId, files, srcFS, dstFS)
}

// It's extremely regrettable that we need to do this, but temporal activities need to know concrete types.
func RunTransferActivity(ctx workflow.Context, storageId string, srcFS, dstFS AbstractFileSystem, files []string) (workflow.Future) {
    fmt.Printf("Transfering %#v\n", files)
    switch srcFS.(type) {
    case LocalFS: {
        switch dstFS.(type) {
        case LocalFS: {
            return workflow.ExecuteActivity(
                ctx, TransferLocalToLocalFS, storageId, srcFS.(LocalFS), dstFS.(LocalFS), files,
            )
        }
        case SshFS: {
            return workflow.ExecuteActivity(
                ctx, TransferLocalToSshFS, storageId, srcFS.(LocalFS), dstFS.(SshFS), files,
            )
        }
        }
    }
    case SshFS: {
        switch dstFS.(type) {
        case LocalFS: {
            return workflow.ExecuteActivity(
                ctx, TransferSshToLocalFS, storageId, srcFS.(SshFS), dstFS.(LocalFS), files,
            )

        }
        case SshFS: {
            return workflow.ExecuteActivity(
                ctx, TransferSshToSshFS, storageId, srcFS.(SshFS), dstFS.(SshFS), files,
            )
        }
        }
    }
    }
    return nil
}