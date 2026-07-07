package parsing

import (
	"path/filepath"
	"testing"
)

func GlobNoOp(i int, s1, s2 string, b1, b2 bool) ([]string, error) { 
	return nil, nil
}

// v0 mounts data dir
func TestV0MountsDataDir(t *testing.T) {
	wf := WorkflowV0 {
		Nodes: map[int]WorkflowNodeV0{
			1: WorkflowNodeV0{
				Id: 1,
			},
		},
	}

	index, err := ParseAndValidateWorkflow(&wf)
	if err != nil {
		t.Fatalf("failed to build wf index: %s", err)
	}

	hybridConfig := JobConfig{
		ExecTypeByNode: map[int]ExecType{
			1: EXEC_LOCAL,
		},
	}

	localRoot := "/local_root"
	cmdMan := NewCmdManager(&wf, index, hybridConfig, map[ExecType]string{
		EXEC_LOCAL: localRoot,
	})

	initCmds, err := cmdMan.GetInitialCmds(GlobNoOp)
	PrettyPrint(initCmds)
	if len(initCmds[1]) != 1 { 
		t.Fatalf("expected 1 cmd for node 1, got %d", len(initCmds[1])) 
	}

	cmd := initCmds[1][0]
	if len(cmd.Volumes) != 1 {
		t.Fatalf("expected 1 volume for node 1, got %d", len(cmd.Volumes))
	}

	for _, vol := range cmd.Volumes {
		if vol.Executor != EXEC_LOCAL {
			t.Fatal("vol has wrong executor")
		} else if vol.CntPath != "/data" && vol.HostPath != localRoot {
			t.Fatalf(
				"expected vol %s -> /data, got %s -> %s", 
				localRoot, vol.HostPath, vol.CntPath,
			)
		}
	}
}


func TestV0HybridTransfer(t *testing.T) {
	node1OutputCntPath := "/data/output"
	lvalForTrue := true
	wf := WorkflowV0 {
		Nodes: map[int]WorkflowNodeV0{
			1: WorkflowNodeV0{
				Id: 1,
				ArgTypes: map[string]WorkflowArgType{
					"node1Output": WorkflowArgType{
						ArgType: "str",
						OutputFile: &lvalForTrue,
					},
				},
			},
			2: WorkflowNodeV0{
				Id: 2,
				ArgTypes: map[string]WorkflowArgType{
					"node2Input": WorkflowArgType{
						ArgType: "str",
						InputFile: &lvalForTrue,
					},
				},
			},
		},
		Links: []WorkflowLinkV0{
			WorkflowLinkV0{
				SourceNodeId: 1,
				SinkNodeId: 2,
				SourceChannel: "node1Output",
				SinkChannel: "node2Input",
			},
		},
	}

	index, err := ParseAndValidateWorkflow(&wf)
	if err != nil {
		t.Fatalf("failed to build wf index: %s", err)
	}

	hybridConfig := JobConfig{
		ExecTypeByNode: map[int]ExecType{
			1: EXEC_LOCAL,
			2: EXEC_SLURM,
		},
	}

	slurmRoot := "/slurm_root"
	localRoot := "/local_root"
	cmdMan := NewCmdManager(&wf, index, hybridConfig, map[ExecType]string{
		EXEC_LOCAL: localRoot,
		EXEC_SLURM: slurmRoot,
	})

	initCmds, err := cmdMan.GetInitialCmds(GlobNoOp)
	if len(initCmds[1]) != 1 { 
		t.Fatalf("expected 1 cmd for node 1, got %d", len(initCmds[1])) 
	}
	
	node1Outputs := map[string]string{ "node1Output": node1OutputCntPath }
	succCmds, err := cmdMan.GetSuccCmds(initCmds[1][0].Cmd, node1Outputs, GlobNoOp, true)
	if err != nil {
		t.Fatalf("failed generating succs of node 1 w/ err %s", err)
	}
	if len(succCmds[2]) != 1 { 
		t.Fatalf("expected 1 cmd for node 2, got %d", len(succCmds[2])) 
	}

	cmd2 := succCmds[2][0]
	if len(cmd2.Xfers) != 1 {
		t.Fatalf("expected 1 xfer of node 1 outputs to node 2 executor, got %d", len(cmd2.Xfers))
	}

	xfer := cmd2.Xfers[0]
	expectedXfer := ObligatoryXfer{
		SrcExecutor: EXEC_LOCAL,
		DstExecutor: EXEC_SLURM,
		SrcHostPath: filepath.Join(localRoot, "output"),
		DstHostPath: filepath.Join(slurmRoot, "output"),
	}
	if xfer != expectedXfer {
		t.Fatalf("wanted xfer %v, got %v", expectedXfer, xfer)
	}
}

func TestV1OutputFileMnt(t *testing.T) {
	node1OutputCntPath := "/data/output"
	wf := ResolvedWorkflow {
		Nodes: map[int]ResolvedNode{
			1: ResolvedNode{
				Outputs: map[string]NodeOutput{
					"node1Output": NodeOutput{
						Kind: "file",
						Path: node1OutputCntPath,
					},
				},
			},
		},
	}

	index, err := ParseAndValidateWorkflow(&wf)
	if err != nil {
		t.Fatalf("failed to build wf index: %s", err)
	}

	hybridConfig := JobConfig{
		ExecTypeByNode: map[int]ExecType{
			1: EXEC_LOCAL,
		},
	}

	localRoot := "/local_root"
	cmdMan := NewCmdManager(&wf, index, hybridConfig, map[ExecType]string{
		EXEC_LOCAL: localRoot,
	})

	initCmds, err := cmdMan.GetInitialCmds(GlobNoOp)
	if len(initCmds[1]) != 1 { 
		t.Fatalf("expected 1 cmd for node 1, got %d", len(initCmds[1])) 
	}

	cmd := initCmds[1][0]
	if len(cmd.Volumes) != 1 {
		t.Fatalf("expected 1 volume for node 1, got %d", len(cmd.Volumes))
	}

	expOutputHostPath := filepath.Join(localRoot, filepath.Dir(node1OutputCntPath))
	for _, vol := range cmd.Volumes {
		if vol.Executor != EXEC_LOCAL {
			t.Fatal("vol has wrong executor")
		} else if vol.CntPath != node1OutputCntPath && vol.HostPath != expOutputHostPath {
			t.Fatalf(
				"expected vol %s -> %s, got %s -> %s", 
				expOutputHostPath, node1OutputCntPath, 
				vol.HostPath, vol.CntPath,
			)
		}
	}
}

// TEST 1: Job1 executes locally, job2 executes on SLURM.
// There should be a transfer of the output file from job1
// to SLURM.
func TestV1HybridTransfer(t *testing.T) {
	node1OutputCntPath := "/data/output"
	node2InputCntPath := "/data/input"
	lvalFor1 := 1
	wf := ResolvedWorkflow {
		Nodes: map[int]ResolvedNode{
			1: ResolvedNode{
				Outputs: map[string]NodeOutput{
					"node1Output": NodeOutput{
						Kind: "file",
						Path: node1OutputCntPath,
					},
				},
			},
			2: ResolvedNode{
				Inputs: map[string]NodeInput{
					"node2Input": NodeInput{
						Kind: "file",
						Source: InputSource{
							Type: "node_output",
							NodeId: &lvalFor1,
							Output: "node1Output",
						},
						Mount: &InputMount{
							ContainerPath: node2InputCntPath,
						},
					},
				},
			},
		},
		Links: []ResolvedLink{
			ResolvedLink{
				Source: 1,
				Sink: 2,
				SourceOutput: "node1Output",
				SinkInput: "node2Input",
			},
		},
	}

	index, err := ParseAndValidateWorkflow(&wf)
	if err != nil {
		t.Fatalf("failed to build wf index: %s", err)
	}

	hybridConfig := JobConfig{
		ExecTypeByNode: map[int]ExecType{
			1: EXEC_LOCAL,
			2: EXEC_SLURM,
		},
	}

	slurmRoot := "/slurm_root"
	localRoot := "/local_root"
	cmdMan := NewCmdManager(&wf, index, hybridConfig, map[ExecType]string{
		EXEC_LOCAL: localRoot,
		EXEC_SLURM: slurmRoot,
	})

	initCmds, err := cmdMan.GetInitialCmds(GlobNoOp)
	if len(initCmds[1]) != 1 { 
		t.Fatalf("expected 1 cmd for node 1, got %d", len(initCmds[1])) 
	}
	
	succCmds, err := cmdMan.GetSuccCmds(initCmds[1][0].Cmd, nil, GlobNoOp, true)
	if err != nil {
		t.Fatalf("failed generating succs of node 1 w/ err %s", err)
	}
	if len(succCmds[2]) != 1 { 
		t.Fatalf("expected 1 cmd for node 2, got %d", len(succCmds[2])) 
	}

	cmd2 := succCmds[2][0]
	if len(cmd2.Xfers) != 1 {
		t.Fatalf("expected 1 xfer of node 1 outputs to node 2 executor, got %d", len(cmd2.Xfers))
	}

	xfer := cmd2.Xfers[0]
	expectedXfer := ObligatoryXfer{
		SrcExecutor: EXEC_LOCAL,
		DstExecutor: EXEC_SLURM,
		SrcHostPath: filepath.Join(localRoot, node1OutputCntPath),
		DstHostPath: filepath.Join(slurmRoot, node2InputCntPath),
	}
	if xfer != expectedXfer {
		t.Fatalf("wanted xfer %v, got %v", expectedXfer, xfer)
	}
}

// TEST 1: Job1 executes locally, job2 executes on SLURM.
// There should be a transfer of the output file from job1
// to SLURM.
func TestV1ExplicitVolumePropagation(t *testing.T) {
	node1InputHostPath := "/local_mnt/input1"
	node1InputCntPath := "/data/input1"
	node2InputCntPath := "/data/input2"
	lvalFor1 := 1
	wf := ResolvedWorkflow {
		Nodes: map[int]ResolvedNode{
			1: ResolvedNode{
				Id: 1,
				Inputs: map[string]NodeInput{
					"node1Input": NodeInput{
						Kind: "file",
						Source: InputSource{
							Type: "path",
							Path: node1InputCntPath,
						},
						Mount: &InputMount{
							ContainerPath: node1InputHostPath,
						},
					},
				},
			},
			2: ResolvedNode{
				Id: 2,
				Inputs: map[string]NodeInput{
					"node2Input": NodeInput{
						Kind: "file",
						Source: InputSource{
							Type: "node_output",
							NodeId: &lvalFor1,
							Output: "node1Input",
						},
						Mount: &InputMount{
							ContainerPath: node2InputCntPath,
						},
					},
				},
			},
		},
		Links: []ResolvedLink{
			ResolvedLink{
				Source: 1,
				Sink: 2,
				SourceOutput: "node1Input",
				SinkInput: "node2Input",
			},
		},
	}

	index, err := ParseAndValidateWorkflow(&wf)
	if err != nil {
		t.Fatalf("failed to build wf index: %s", err)
	}

	localConfig := JobConfig{
		ExecTypeByNode: map[int]ExecType{
			1: EXEC_LOCAL,
			2: EXEC_LOCAL,
		},
	}

	localRoot := "/localRoot"
	cmdMan := NewCmdManager(&wf, index, localConfig, map[ExecType]string{
		EXEC_LOCAL: localRoot,
	})

	initCmds, err := cmdMan.GetInitialCmds(GlobNoOp)
	if len(initCmds[1]) != 1 { 
		t.Fatalf("expected 1 cmd for node 1, got %d", len(initCmds[1])) 
	}
	
	succCmds, err := cmdMan.GetSuccCmds(initCmds[1][0].Cmd, nil, GlobNoOp, true)
	if err != nil {
		t.Fatalf("failed generating succs of node 1 w/ err %s", err)
	}
	if len(succCmds[2]) != 1 { 
		t.Fatalf("expected 1 cmd for node 2, got %d", len(succCmds[2])) 
	}

	PrettyPrint(succCmds[2][0])
}
