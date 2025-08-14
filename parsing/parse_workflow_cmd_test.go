package parsing

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"testing"
)

func TestEnvScalarSerialization(t *testing.T) {
	tests := []struct {
		ptype    string
		val      TypedParams
		expected string
	}{
		{
			ptype: "str",
			val: TypedParams{
				Strings: map[string]string{
					"str": "test_string",
				},
			},
			expected: "test_string",
		}, {
			ptype: "int",
			val: TypedParams{
				Ints: map[string]int{
					"int": 123456789,
				},
			},
			expected: "123456789",
		}, {
			ptype: "double",
			val: TypedParams{
				Doubles: map[string]float64{
					"double": 2.718281828459,
				},
			},
			expected: "2.718281828459",
		},
	}

	for _, tt := range tests {
		t.Run(tt.ptype, func(t *testing.T) {
			actual, err := getEnvValStr(tt.ptype, tt.ptype, tt.val)
			if err != nil {
				t.Fatalf("env serialization failed with error %s", err)
			} else if actual != tt.expected {
				t.Fatalf("expected %v, got %v", tt.expected, actual)
			}
		})
	}
}

// Default BWB behavior is to have all lists, regardless of type,
// as [\"val_1\",\"val_2\",...,\"val_n"].
func TestEnvListSerialization(t *testing.T) {
	tests := []struct {
		ptype    string
		pname    string
		val      TypedParams
		expected string
	}{
		{
			ptype: "str list",
			pname: "strList",
			val: TypedParams{
				StrLists: map[string][]string{
					"strList": {"test_string1", "test_string2"},
				},
			},
			expected: "[\"test_string1\",\"test_string2\"]",
		}, {
			ptype: "int list",
			pname: "intList",
			val: TypedParams{
				IntLists: map[string][]int{
					"intList": {1, 2},
				},
			},
			expected: "[\"1\",\"2\"]",
		}, {
			ptype: "double list",
			pname: "doubleList",
			val: TypedParams{
				DoubleLists: map[string][]float64{
					"doubleList": {2.718281828459, 3.1415927},
				},
			},
			expected: "[\"2.718281828459\",\"3.1415927\"]",
		},
	}

	for _, tt := range tests {
		t.Run(tt.ptype, func(t *testing.T) {
			actual, err := getEnvValStr(tt.pname, tt.ptype, tt.val)
			if err != nil {
				t.Fatalf("env serialization failed with error %s", err)
			} else if actual != tt.expected {
				t.Fatalf("expected %v, got %v", tt.expected, actual)
			}
		})
	}
}

func TestScalarCmdSubBasic(t *testing.T) {
	var node WorkflowNode
	node.ArgTypes = map[string]WorkflowArgType{
		"scalarParam": {
			ArgType: "text",
		},
	}

	baseProps := map[string]any{
		"scalarParam": "subText",
	}

	node.Command = []string{"command _bwb{scalarParam}"}

	tp, err := parseTypedParams(node, baseProps)
	if err != nil {
		t.Fatalf("failed to parse typed params: %s", err)
	}

	var template CmdTemplate
	err = performCmdSubs(node, &template, tp, false)
	if err != nil {
		t.Fatalf("input error: %s", err)
	}

	if template.BaseCmd[0] != "command subText" {
		t.Fatalf("expected \"command subText\", got \"%s\"", template.BaseCmd[0])
	}
}

func TestScalarCmdSubTypeErr(t *testing.T) {
	var node WorkflowNode
	node.ArgTypes = map[string]WorkflowArgType{
		"scalarParam": {
			ArgType: "text",
		},
	}

	baseProps := map[string]any{
		"scalarParam": nil,
	}
	node.Command = []string{"command _bwb{scalarParam}"}

	tp, err := parseTypedParams(node, baseProps)
	if err != nil {
		t.Fatalf("failed to parse typed params: %s", err)
	}

	var template CmdTemplate
	err = performCmdSubs(node, &template, tp, false)
	if err == nil {
		t.Fatalf(
			"expected type error when trying to substitute null variable 'scalarParam'",
		)
	}
}

func TestScalarCmdSubMissingErr(t *testing.T) {
	var node WorkflowNode
	node.ArgTypes = map[string]WorkflowArgType{
		"scalarParam": {
			ArgType: "text",
		},
	}

	baseProps := map[string]any{
		"scalarParam": nil,
	}
	node.Command = []string{"command _bwb{nonExistentParam}"}

	tp, err := parseTypedParams(node, baseProps)
	if err != nil {
		t.Fatalf("failed to parse typed params: %s", err)
	}

	var template CmdTemplate
	err = performCmdSubs(node, &template, tp, false)
	if err == nil {
		t.Fatalf(
			"expected type error when trying to substitute missing variable `nonExistentParam`",
		)
	}
}

func TestScalarInFile(t *testing.T) {
	var node WorkflowNode
	lvalForTrue := true
	node.ArgTypes = map[string]WorkflowArgType{
		"scalarInParam": {
			ArgType:   "file",
			InputFile: &lvalForTrue,
		},
	}
	baseProps := map[string]any{
		"scalarInParam": "/path",
	}

	tp, err := parseTypedParams(node, baseProps)
	if err != nil {
		t.Fatalf("failed to parse typed params: %s", err)
	}

	var template CmdTemplate
	iterAttrs := getIterAttrs(node)
	err = evaluateInOutFiles(node, &template, tp, iterAttrs)
	if err != nil {
		t.Fatalf("error parsing in files: %s", err)
	}

	if len(template.InFiles) != 1 {
		t.Fatalf("got %d infiles, expected 1", len(template.InFiles))
	}

	expVal := baseProps["scalarInParam"].(string)
	if template.InFiles[0] != expVal {
		t.Fatalf(
			"got incorrect value for input file, got %s, expected %s",
			template.InFiles[0], expVal,
		)
	}
}

func TestScalarOutFile(t *testing.T) {
	var node WorkflowNode
	lvalForTrue := true
	node.ArgTypes = map[string]WorkflowArgType{
		"scalarOutParamAbsent": {
			ArgType:    "file",
			OutputFile: &lvalForTrue,
		},
		"scalarOutParamPresent": {
			ArgType:    "file",
			OutputFile: &lvalForTrue,
		},
	}
	baseProps := map[string]any{
		"scalarOutParamPresent": "/path",
	}

	tp, err := parseTypedParams(node, baseProps)
	if err != nil {
		t.Fatalf("failed to parse typed params: %s", err)
	}

	var template CmdTemplate
	iterAttrs := getIterAttrs(node)
	err = evaluateInOutFiles(node, &template, tp, iterAttrs)
	if err != nil {
		t.Fatalf("error parsing in files: %s", err)
	}

	if len(template.OutFilePnames) != 1 {
		t.Fatalf("got %d outfile pnames, expected 1", len(template.OutFilePnames))
	}

	if template.OutFilePnames[0] != "scalarOutParamAbsent" {
		t.Fatalf(
			"got incorrect value for output file, got %s, expected %s",
			template.OutFilePnames[0], "scalarOutParamAbsent",
		)
	}

    if len(template.OutFiles) != 1 {
        t.Fatalf("got %d outfile paths, expected 1", len(template.OutFiles))
    }

    if template.OutFiles[0] != "/path" {
        t.Fatalf(
            "got incorrect value for output file, got %s, expected %s",
            template.OutFiles[0], "/path",
        )
    }
}

// Helper function to catch certain basic issues with iterated
// parameter parsing that are common to all subsequent tests.
// For simplicity's sake, we just assume that the parameter being
// tested is a string list.
func basicIterValidation(
	node WorkflowNode, baseProps map[string]any,
	iters []CmdTemplate, param string,
) error {
	// These are panics rather than errors, since this is an
	// issue with the testing code rather than the thing being tested.
	if _, ok := baseProps[param]; !ok {
		panic(fmt.Sprintf("non-existent param %s", param))
	}

	actualVals, ok := baseProps[param].([]any)
	listParamLen := len(actualVals)
	if !ok {
		panic(fmt.Sprintf("failed converting %s to list", param))
	}

	groupSize, ok := node.IterGroupSize[param]
	if !ok {
		panic(fmt.Sprintf(
			"trying to test non-iterable param %s in iterable tests",
			param,
		))
	}

	expectedSize := math.Ceil(float64(listParamLen) / float64(groupSize))
	if len(iters) != int(expectedSize) {
		return fmt.Errorf("expected %d iterations, got %d", int(expectedSize), len(iters))
	}

	for i, iter := range iters {
		listParamVal, ok := iter.IterVals.StrLists[param]
		if !ok {
			return fmt.Errorf("iterable vals has no entry for \"%s\"", param)
		}
		if len(listParamVal) != groupSize {
			return fmt.Errorf(
				"expected iterable vals to have len groupsize (%d), got %d",
				groupSize, len(listParamVal),
			)
		}

		for j := 0; j < groupSize; j++ {
			var expected string
			if i*groupSize+j < len(actualVals) {
				expected = actualVals[i*groupSize+j].(string)
			} else {
				// Pad with last value until reaching even multiple of group size,
				// as in BWB.
				expected = actualVals[len(actualVals)-1].(string)
			}

			if listParamVal[j] != expected {
				return fmt.Errorf(
					"iterable vals index %d differs from input: expected %s, got %s",
					j, expected, listParamVal[j],
				)
			}
		}
	}
	return nil
}

// It should work with substitutions
func TestIterValBasic(t *testing.T) {
	var node WorkflowNode
	isArg := true
	node.ArgTypes = map[string]WorkflowArgType{
		"listParam": {
			ArgType:    "text list",
			IsArgument: &isArg,
		},
	}

	baseProps := map[string]any{
		"listParam": []any{"1", "2", "3", "4"},
	}
	node.Iterate = true
	node.IterAttrs = []string{"listParam"}
	node.IterGroupSize = map[string]int{
		"listParam": 1,
	}
	node.Command = []string{"command"}

	tp, err := parseTypedParams(node, baseProps)
	if err != nil {
		t.Fatalf("failed to parse typed params: %s", err)
	}

	reqParams := getRequiredParams(node)

	var template CmdTemplate
	iters, err := evaluateIterables(node, template, tp, reqParams)
	if err != nil {
		t.Fatalf("failed to parse iterable: %s", err)
	}

	err = basicIterValidation(node, baseProps, iters, "listParam")
	if err != nil {
		t.Fatalf("incorrectly parsed iterable: %s", err)
	}
}

// Test that things are correctly split if the group size is larger than one.
func TestNonOneGroupSize(t *testing.T) {
	var node WorkflowNode
	isArg := true
	node.ArgTypes = map[string]WorkflowArgType{
		"listParam": {
			ArgType:    "text list",
			IsArgument: &isArg,
		},
	}

	baseProps := map[string]any{
		"listParam": []any{"1", "2", "3", "4"},
	}
	node.Iterate = true
	node.IterAttrs = []string{"listParam"}
	node.IterGroupSize = map[string]int{
		"listParam": 2,
	}
	node.Command = []string{"command"}

	tp, err := parseTypedParams(node, baseProps)
	if err != nil {
		t.Fatalf("failed to parse typed params: %s", err)
	}

	reqParams := getRequiredParams(node)
	var template CmdTemplate
	iters, err := evaluateIterables(node, template, tp, reqParams)
	if err != nil {
		t.Fatalf("failed to parse iterable: %s", err)
	}

	// This actually already tests what we want, and will give a distinct
	// error if it fails.
	err = basicIterValidation(node, baseProps, iters, "listParam")
	if err != nil {
		t.Fatalf("incorrectly parsed iterable: %s", err)
	}
}

// Test that if the size of an iterable param's list is not evenly divisible by
// the group size that the array is padded with the last value.
func TestIterValPadding(t *testing.T) {
	var node WorkflowNode
	isArg := true
	node.ArgTypes = map[string]WorkflowArgType{
		"listParam": {
			ArgType:    "text list",
			IsArgument: &isArg,
		},
	}

	baseProps := map[string]any{
		"listParam": []any{"1", "2", "3", "4", "5"},
	}
	node.Iterate = true
	node.IterAttrs = []string{"listParam"}
	node.IterGroupSize = map[string]int{
		"listParam": 2,
	}
	node.Command = []string{"command"}

	tp, err := parseTypedParams(node, baseProps)
	if err != nil {
		t.Fatalf("failed to parse typed params: %s", err)
	}

	reqParams := getRequiredParams(node)

	var template CmdTemplate
	iters, err := evaluateIterables(node, template, tp, reqParams)
	if err != nil {
		t.Fatalf("failed to parse iterable: %s", err)
	}

	// This actually already tests what we want, and will give a distinct
	// error if it fails.
	err = basicIterValidation(node, baseProps, iters, "listParam")
	if err != nil {
		t.Fatalf("incorrectly parsed iterable: %s", err)
	}
}

func validateIterGroup(
	iterTemplate CmdTemplate, iterNo int, pname string,
	lp []any, lpGroupSize int, lpExpSize int,
) error {
	var startInd int
	if len(lp) == 0 {
		if iterTemplate.IterVals.StrLists[pname] != nil {
			return fmt.Errorf("%s is emtpy, so its iterVals entry should be nil", pname)
		}
		return nil
	}

	startInd = (iterNo * lpGroupSize) % (int(lpExpSize) * lpGroupSize)
	for j := 0; j < lpGroupSize; j++ {
		ind := min(startInd+j, len(lp)-1)
		got := iterTemplate.IterVals.StrLists[pname][j]
		want := lp[ind]
		if got != want {
			return fmt.Errorf(
				"iter %d, %s, index %d: got %v, want %v",
				iterNo, pname, j, got, want,
			)
		}
	}
	return nil
}

// This test ensures that multiple iter vals in the same command are handled
// the same as in BWB; they are broken into groups of their respective group size,
// one command is generated for each group of the iter val with the most groups,
// and if we reach the end of anotehr iterval's list of groups is reached when
// iterating over groups of the longest, we loop back to the start of this shorter
// val's array. E.g. iterVal1 = {1,2,3,4}, iterVal2 = {1, 2} with group size one
// should yield 4 commands with values
//   - {iterVal1=1, iterVal2=1}
//   - {iterVal1=2, iterVal2=2}
//   - {iterVal1=3, iterVal2=1}
//   - {iterVal1=4, iterVal2=2}
//
// I am unaware of this behavior being used in any actual BWB workflows, so we
// could probably ditch it without breaking compatibility with any existing
// worfklows. Or at least we could enforce constraints that the number of groups
// of the iter val with the most groups is divided evenly by the number of groups
// for other iter vals. At the moment, however, we do behave exactly like BWB.
func TestMultipleIterVals(t *testing.T) {
	tests := []struct {
		name       string
		listParam1 []any
		listParam2 []any
		group1     int
		group2     int
	}{
		{
			name:       "Group size 1",
			listParam1: []any{"1", "2", "3", "4", "5", "6", "7", "8"},
			listParam2: []any{"1", "2", "3", "4", "5", "6", "7", "8"},
			group1:     1,
			group2:     1,
		},
		{
			name:       "Varying group sizes",
			listParam1: []any{"1", "2", "3", "4", "5", "6", "7", "8"},
			listParam2: []any{"1", "2", "3", "4", "5", "6", "7", "8"},
			group1:     3,
			group2:     2,
		},
		{
			name:       "Uneven list lengths",
			listParam1: []any{"a", "b", "c", "d", "e"},
			listParam2: []any{"x", "y", "z"},
			group1:     2,
			group2:     1,
		},
		{
			name:       "Empty list",
			listParam1: []any{"1", "2", "3", "4", "5", "6"},
			listParam2: []any{},
			group1:     2,
			group2:     2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			node := WorkflowNode{
				ArgTypes: map[string]WorkflowArgType{
					"listParam1": {
						ArgType: "text list",
					},
					"listParam2": {
						ArgType: "text list",
					},
				},
				Iterate:   true,
				IterAttrs: []string{"listParam1", "listParam2"},
				IterGroupSize: map[string]int{
					"listParam1": tt.group1,
					"listParam2": tt.group2,
				},
				Command: []string{"command"},
			}

			baseProps := map[string]any{
				"listParam1": tt.listParam1,
				"listParam2": tt.listParam2,
			}

			tp, err := parseTypedParams(node, baseProps)
			if err != nil {
				t.Fatalf("failed to parse typed params: %s", err)
			}

			reqParams := getRequiredParams(node)

			var template CmdTemplate
			iters, err := evaluateIterables(node, template, tp, reqParams)
			if err != nil {
				t.Fatalf("failed to parse iterable: %s", err)
			}

			lp1 := tt.listParam1
			lp1GroupSize := tt.group1
			lp1ExpSize := math.Ceil(float64(len(lp1)) / float64(lp1GroupSize))

			lp2 := tt.listParam2
			lp2GroupSize := tt.group2
			lp2ExpSize := math.Ceil(float64(len(lp2)) / float64(lp2GroupSize))

			itersExpLen := int(max(lp1ExpSize, lp2ExpSize))
			if len(iters) != itersExpLen {
				t.Fatalf("expected %d iterations, got %d", itersExpLen, len(iters))
			}

			for i := 0; i < itersExpLen; i++ {
				lp1GroupErr := validateIterGroup(
					iters[i], i, "listParam1", lp1, lp1GroupSize, int(lp1ExpSize),
				)
				if lp1GroupErr != nil {
					t.Fatal(lp1GroupErr.Error())
				}

				lp2GroupErr := validateIterGroup(
					iters[i], i, "listParam2", lp2, lp2GroupSize, int(lp2ExpSize),
				)
				if lp2GroupErr != nil {
					t.Fatal(lp2GroupErr.Error())
				}
			}
		})
	}
}

func TestIterSubstitutions(t *testing.T) {
	node := WorkflowNode{
		ArgTypes: map[string]WorkflowArgType{
			"listParam": {
				ArgType: "text list",
			},
		},
		Iterate:   true,
		IterAttrs: []string{"listParam"},
		IterGroupSize: map[string]int{
			"listParam": 1,
		},
		Command: []string{"_bwb{listParam}"},
	}

	baseProps := map[string]any{
		"listParam": []any{"1", "2", "3", "4", "5", "6", "7", "8"},
	}

	tp, err := parseTypedParams(node, baseProps)
	if err != nil {
		t.Fatalf("failed to parse typed params: %s", err)
	}
	reqParams := getRequiredParams(node)

	var template CmdTemplate
	iters, err := evaluateIterables(node, template, tp, reqParams)
	if err != nil {
		t.Fatalf("failed to parse iterable: %s", err)
	}

	// Verify that all values of listParam were assigned to correctly-sized and
	// correctly ordered groups.
	if err = basicIterValidation(node, baseProps, iters, "listParam"); err != nil {
		t.Fatalf("iteration grouping error: %s", err)
	}

	for i := range iters {
		performCmdSubs(node, &iters[i], tp, false)
		actual := iters[i].BaseCmd[0]
		expected := iters[i].IterVals.StrLists["listParam"][0]
		if actual != expected {
			t.Fatalf(
				"el %d has incorrect value, got %s, expected %s",
				i, actual, expected,
			)
		}
	}
}

func TestIterInputFiles(t *testing.T) {
	lvalForTrue := true
	node := WorkflowNode{
		ArgTypes: map[string]WorkflowArgType{
			"fListParam": {
				ArgType:   "text list",
				InputFile: &lvalForTrue,
			},
		},
		Iterate: true,
		IterGroupSize: map[string]int{
			"fListParam": 1,
		},
		IterAttrs: []string{"fListParam"},
		Command:   []string{""},
	}

	baseProps := map[string]any{
		"fListParam": []any{"/path1", "/path2", "/path3"},
	}

	tp, err := parseTypedParams(node, baseProps)
	if err != nil {
		t.Fatalf("failed to parse typed params: %s", err)
	}

	reqParams := getRequiredParams(node)

	var template CmdTemplate
	iters, err := evaluateIterables(node, template, tp, reqParams)
	if err != nil {
		t.Fatalf("failed to parse iterable: %s", err)
	}

	// Verify that all values of listParam were assigned to correctly-sized and
	// correctly ordered groups.
	err = basicIterValidation(node, baseProps, iters, "fListParam")
	if err != nil {
		t.Fatalf("iteration grouping error: %s", err)
	}

	fListParamVals := baseProps["fListParam"].([]any)
	for i := range iters {
		expFname := fListParamVals[i].(string)
		if len(iters[i].InFiles) != 1 && iters[i].InFiles[0] != expFname {
			t.Fatalf(
				"error with iterable infile assignment: xpected %s, got %s",
				iters[i].InFiles[0], expFname,
			)
		}
	}
}

func TestIterValNonListErr(t *testing.T) {
	var node WorkflowNode
	node.ArgTypes = map[string]WorkflowArgType{
		"listParam": {
			ArgType: "text",
		},
	}

	baseProps := map[string]any{
		"listParam": "1",
	}
	node.IterGroupSize = map[string]int{
		"listParam": 1,
	}
	node.Iterate = true
	node.IterAttrs = []string{"listParam"}
	node.Command = []string{"command"}

	tp, err := parseTypedParams(node, baseProps)
	if err != nil {
		t.Fatalf("failed to parse typed params: %s", err)
	}

	reqParams := getRequiredParams(node)

	var template CmdTemplate
	_, err = evaluateIterables(node, template, tp, reqParams)
	if err == nil {
		t.Fatalf("should give error on non-list iterable")
	}
}

func TestDryRun(t *testing.T) {
	data, err := os.ReadFile("testdata/bulkrna_seq.json")
	if err != nil {
		t.Fatalf("failed to read JSON file: %v", err)
	}

	var workflow Workflow
	if err := json.Unmarshal(data, &workflow); err != nil {
		t.Fatalf("failed to unmarshal JSON: %v", err)
	}

	_, err = layeredTopSort(workflow)
	if err != nil {
		t.Fatalf("error: %s", err)
	}
	_, cmdStrErr := DryRun(workflow)
	if cmdStrErr != nil {
		t.Fatalf("failed to execute dry run: %s", cmdStrErr)
	}
}