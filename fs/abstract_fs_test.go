package fs

import ("testing")

func TestGetVolumesValidCntPath(t *testing.T) {
    volumes := map[string]string{
        "/data": "A",
        "/data/123": "B",
    }
    cntPath, mntPt, ok := getMntPtPrefix("/data/123/C", volumes, true)
    if !ok {
        t.Fatalf("failed to find mountpoint /data/123 in volumes")
    }

    if cntPath != "/data/123" || mntPt != "B" {
        t.Fatalf(
            "expected cnt path /data/123, mnt pnt B, got %s and %s",
            cntPath, mntPt,
        )
    }
}

func TestGetVolumesInvalidCntPath(t *testing.T) {
    volumes := map[string]string{
        "/data": "A",
        "/data/123": "B",
    }
    _, _, ok := getMntPtPrefix("/dat/abcd", volumes, true)
    if ok {
        t.Fatalf("gave cndPath for non-existent path /dat/abcd")
    }
}

func TestGetVolumesValidHostPath(t *testing.T) {
    volumes := map[string]string{
        "/data/123/1234": "/A/B/C",
        "/data/123": "/A/B/C/D/E",
    }
    hostPathPrefix, cntPath, ok := getMntPtPrefix("/A/B/C/D/E/F", volumes, false)
    if !ok {
        t.Fatalf("failed to find mountpoint /A/B/C/D/E volumes")
    }

    if hostPathPrefix != "/A/B/C/D/E" || cntPath != "/data/123" {
        t.Fatalf(
            "expected host path prefix '/A/B/C/D/E', cnt path '/data/123', "+
            "got %s and %s", hostPathPrefix, cntPath,
        )
    }
}

func TestGetVolumesInvalidHostPath(t *testing.T) {
    volumes := map[string]string{
        "/data": "/A/B/C",
        "/data/123": "A/B/C/D",
    }
    _, _, ok := getMntPtPrefix("C/D/E", volumes, false)
    if ok {
        t.Fatalf("gave cntPath for non-existent path /C/D/E")
    }
}

func TestGetHostPath(t *testing.T) {
    var testCases = []struct{
        name string
        volumes map[string]string
        path string
    }{
        {
            name: "No backslashes after paths",
            volumes: map[string]string{"/data/123": "B"},
            path: "/data/123/C",
        },
        {
            name: "Backslash after cnt path",
            volumes: map[string]string{"/data/123/": "B"},
            path: "/data/123/C",
        },
        {
            name: "Backslash after query path",
            volumes: map[string]string{"/data/123": "B"},
            path: "/data/123/C/",
        },
        {
            name: "Backslash after cnt and query path",
            volumes: map[string]string{"/data/123/": "B"},
            path: "/data/123/C/",
        },
    }

    expectedOutputForAllCases := "B/C"
    for _, tt := range testCases {
        t.Run(tt.name, func(t *testing.T) {
            hostPath, ok := getHostPath(tt.path, tt.volumes)
            if !ok {
                t.Fatalf("failed to generate host path for path /data/123/C")
            }
            if hostPath !=  expectedOutputForAllCases {
                t.Fatalf(
                    "got host path '%s', expected '%s'", 
                    hostPath, expectedOutputForAllCases,
                )
            }
        })
    }
}