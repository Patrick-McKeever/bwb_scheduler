package parsing

import (
	"bytes"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"runtime"
	"strings"
)

type OwsScheme struct {
	XMLName     xml.Name     `xml:"scheme"`
	Description string       `xml:"description,attr"`
	Title       string       `xml:"title,attr"`
	Version     string       `xml:"version,attr"`
	Nodes       OwsNodes     `xml:"nodes"`
	Links       OwsLinks     `xml:"links"`
	NodeProps   OwsNodeProps `xml:"node_properties"`
}

type OwsNodes struct {
	XMLName xml.Name  `xml:"nodes"`
	Nodes   []OwsNode `xml:"node"`
}

type OwsNode struct {
	XMLName xml.Name `xml:"node"`
	Id      int      `xml:"id,attr"`
	Name    string   `xml:"name,attr"`
	Title   string   `xml:"title,attr"`
}

type OwsLinks struct {
	XMLName xml.Name  `xml:"links"`
	Links   []OwsLink `xml:"link"`
}

type OwsLink struct {
	XMLName       xml.Name `xml:"link"`
	Id            int      `xml:"id,attr"`
	SourceNodeId  int      `xml:"source_node_id,attr"`
	SinkNodeId    int      `xml:"sink_node_id,attr"`
	SourceChannel string   `xml:"source_channel,attr"`
	SinkChannel   string   `xml:"sink_channel,attr"`
}

type OwsNodeProps struct {
	XMLName   xml.Name          `xml:"node_properties"`
	NodeProps []OwsRawNodeProps `xml:"properties"`
}

type OwsRawNodeProps struct {
	XMLName xml.Name `xml:"properties"`
	Format  string   `xml:"format,attr"`
	NodeId  int      `xml:"node_id,attr"`
	RawStr  []byte   `xml:",chardata"`
}

type WorkflowLink struct {
	SourceNodeId  int
	SinkNodeId    int
	SourceChannel string
	SinkChannel   string
}

type WorkflowArgType struct {
	ArgType    string      `json:"type"`
	IsArgument *bool       `json:"argument"`
	Flag       *string     `json:"flag"`
	Env        *string     `json:"env"`
	Label      *string     `json:"label"`
	DefaultVal interface{} `json:"default"`
	InputFile  *bool       `json:"input_file"`
	OutputFile *bool       `json:"output_file"`
}

type IterateSetting struct {
	GroupSize int `json:"groupSize"`
}

type IterateSettings struct {
	IterableAttrs []string                  `json:"iterableAttrs"`
	IteratedAttrs []string                  `json:"iteratedAttrs"`
	Settings      map[string]IterateSetting `json:"data"`
}

type BwbJsonWorkflowNode struct {
	Title          string
	Command        []string
	ArgTypes       map[string]WorkflowArgType
	RequiredParams []string
	ImageName      string
	ImageTag       string
}

type XmlNodeInfo struct {
	Props          map[string]interface{}
	Iterate        bool
	IterAttrs      []string
	IterGroupSize  map[string]int
	OptionsChecked map[string]bool
}

type ResourceVector struct {
	MemMb int
	Cpus  int
	Gpus  int
}

type WorkflowNode struct {
	Id        int
	ImageName string
	ImageTag  string
	Title     string
	Command   []string
	ArgTypes  map[string]WorkflowArgType
	//BaseProps      map[string]interface{}
	ArgOrder       []string
	OptionsChecked map[string]bool
	RequiredParams []string
	ResourceReqs   ResourceVector
	Async          bool
	BarrierFor     *int
	Iterate       bool
	IterGroupSize map[string]int
	IterAttrs     []string
}

type Workflow struct {
	Nodes         map[int]WorkflowNode
	NodeBaseProps map[int]map[string]any
	Links         []WorkflowLink
}

func copyWorkflowNode(node WorkflowNode) WorkflowNode {
	nodeCopy := WorkflowNode{
		Id:           node.Id,
		ImageName:    node.ImageName,
		ImageTag:     node.ImageTag,
		Title:        node.Title,
		ResourceReqs: node.ResourceReqs,
		Async:        node.Async,
		BarrierFor:   node.BarrierFor,
	}
	nodeCopy.ArgTypes = copyMapOfScalars(node.ArgTypes)
	nodeCopy.OptionsChecked = copyMapOfScalars(node.OptionsChecked)
	nodeCopy.RequiredParams = make([]string, len(node.RequiredParams))

	nodeCopy.Command = make([]string, len(node.Command))
	copy(nodeCopy.Command, node.Command)
	nodeCopy.RequiredParams = make([]string, len(node.RequiredParams))
	copy(nodeCopy.RequiredParams, node.RequiredParams)
	nodeCopy.ArgOrder = make([]string, len(node.ArgOrder))
	copy(nodeCopy.ArgOrder, node.ArgOrder)
	nodeCopy.IterGroupSize = copyMapOfScalars(node.IterGroupSize)

	return nodeCopy
}

func (node *BwbJsonWorkflowNode) UnmarshalJSON(data []byte) error {
	var raw struct {
		Name           string   `json:"name"`
		ImageName      string   `json:"docker_image_name"`
		ImageTag       string   `json:"docker_image_tag"`
		Command        []string `json:"command"`
		RequiredParams []string `json:"requiredParameters"`
		Parameters     struct {
			PyReduce []interface{} `json:"py/reduce"`
		} `json:"parameters"`
	}

	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}

	node.Command = raw.Command
	node.ImageName = raw.ImageName
	node.ImageTag = raw.ImageTag
	node.RequiredParams = raw.RequiredParams
	node.Title = raw.Name

	if len(raw.Parameters.PyReduce) < 5 {
		return fmt.Errorf("unexpected format: too few elements in py/reduce")
	}

	innerRaw, err := json.Marshal(raw.Parameters.PyReduce[4])
	if err != nil {
		return err
	}

	var tupleBlock struct {
		PyTuple []struct {
			PyTuple []json.RawMessage `json:"py/tuple"`
		} `json:"py/tuple"`
	}

	if err := json.Unmarshal(innerRaw, &tupleBlock); err != nil {
		return err
	}

	node.ArgTypes = make(map[string]WorkflowArgType)
	for _, entry := range tupleBlock.PyTuple {
		if len(entry.PyTuple) != 2 {
			return fmt.Errorf("expected key-value pair tuple")
		}

		var key string
		if err := json.Unmarshal(entry.PyTuple[0], &key); err != nil {
			return err
		}

		var val WorkflowArgType
		if err := json.Unmarshal(entry.PyTuple[1], &val); err != nil {
			return err
		}

		// Sometimes argType key has a space after it as a consequence
		// of how BWB UI parses input.
		trimmedKey := strings.TrimSpace(key)
		node.ArgTypes[trimmedKey] = val
	}

	return nil
}

func parseNodeProps(node_props OwsRawNodeProps) (XmlNodeInfo, error) {
	var xmlNodeInfo XmlNodeInfo

	_, currentFilename, _, ok := runtime.Caller(0)
	if !ok {
		return xmlNodeInfo, fmt.Errorf("unable to find pickle_to_json.py script")
	}
	pythonScriptPath := filepath.Join(filepath.Dir(currentFilename), "pickle_to_json.py")

	cmd := exec.Command("python3", pythonScriptPath, node_props.Format)
	cmd.Stdin = bytes.NewReader(node_props.RawStr)
	out_json, err := cmd.CombinedOutput()
	if err != nil {
		return xmlNodeInfo, fmt.Errorf("pickle_to_json.py failed with: %s", out_json)
	}

	var data map[string]interface{}
	if err := json.Unmarshal(out_json, &data); err != nil {
		return xmlNodeInfo, fmt.Errorf("unable to decode JSON %s", out_json)
	}

	// Delete various unnecessary keys. BWB has the annoying habit
	// of lumping graphics information and scheduling info with the
	// actual user-input parameters of jobs.
	delete(data, "__version__")
	delete(data, "controlAreaVisible")
	delete(data, "inputConnectionsStore")
	delete(data, "useScheduler")
	delete(data, "runMode")
	delete(data, "exportGraphics")
	delete(data, "nThreads")
	delete(data, "nWorkers")
	delete(data, "triggerReady")
	delete(data, "runTriggers")

	iterate, ok := data["iterate"].(bool)
	if !ok {
		return xmlNodeInfo, fmt.Errorf(
			"could not convert `iterate` (val %v) to bool: %s",
			data["iterate"], err,
		)
	}
	xmlNodeInfo.Iterate = iterate
	delete(data, "iterate")

	iterateSettingsRaw, ok := data["iterateSettings"]
	if !ok {
		return xmlNodeInfo, fmt.Errorf("`iterateSettings` key not found")
	}

	iterateSettings, ok := iterateSettingsRaw.(map[string]interface{})
	if !ok {
		return xmlNodeInfo, fmt.Errorf("`iterateSettings` is not a map")
	}

	iterableAttrsRaw, ok := iterateSettings["iterableAttrs"]
	if !ok {
		return xmlNodeInfo, fmt.Errorf("`iterateSettings->iterableAttrs` key not found")
	}

	iterableAttrs, ok := iterableAttrsRaw.([]interface{})
	if !ok {
		return xmlNodeInfo, fmt.Errorf("`iterableAttrs` is not a list")
	}

	dataRaw, hasDataEntry := iterateSettings["data"]
	dataMap := map[string]interface{}{}
	if hasDataEntry {
		dataMap, _ = dataRaw.(map[string]interface{})
	}

	xmlNodeInfo.IterGroupSize = make(map[string]int)
	for _, attrRaw := range iterableAttrs {
		attr, ok := attrRaw.(string)
		if !ok {
			return xmlNodeInfo, fmt.Errorf("non-string attribute in `iterableAttrs`")
		}

		groupSize := 1
		if hasDataEntry {
			attrEntryRaw, exists := dataMap[attr]
			if exists {
				attrEntryMap, ok := attrEntryRaw.(map[string]interface{})
				if ok {
					if gsRaw, ok := attrEntryMap["groupSize"]; ok {
						if gsFloat, ok := gsRaw.(float64); ok {
							groupSize = int(gsFloat)
						}
					}
				}
			}
		}

		trimmedAttr := strings.TrimSpace(attr)
		xmlNodeInfo.IterGroupSize[trimmedAttr] = groupSize
	}

    // iterateSettings->iteratedAttrs is optional
    xmlNodeInfo.IterAttrs = make([]string, 0)
	iteratedAttrsRaw, iteratedAttrsExist := iterateSettings["iteratedAttrs"]
	if iteratedAttrsExist {
	    iteratedAttrs, ok := iteratedAttrsRaw.([]interface{})
	    if !ok {
	    	return xmlNodeInfo, fmt.Errorf("`iteratedAttrs` is not a list")
	    }

        for _, pnameRaw := range iteratedAttrs {
            pnameStr, ok := pnameRaw.(string)
            if !ok {
                return xmlNodeInfo, fmt.Errorf("non-string attribute in `iteratedAttrs`")
            }
            xmlNodeInfo.IterAttrs = append(xmlNodeInfo.IterAttrs, pnameStr)
        }
	}
	delete(data, "iterateSettings")

	optionsCheckedRaw, ok := data["optionsChecked"]
	if !ok {
		return xmlNodeInfo, fmt.Errorf("`optionsChecked` key not found")
	}

	optionsChecked, ok := optionsCheckedRaw.(map[string]interface{})
	if !ok {
		return xmlNodeInfo, fmt.Errorf("`optionsChecked` is not a map")
	}
	xmlNodeInfo.OptionsChecked = make(map[string]bool)
	for attr, checkedRaw := range optionsChecked {
		checked, ok := checkedRaw.(bool)
		if ok {
			xmlNodeInfo.OptionsChecked[attr] = checked
		}
	}
	delete(data, "optionsChecked")
	xmlNodeInfo.Props = data

	return xmlNodeInfo, nil
}

func parseWorkflowNode(
	jsonWorkflowNode BwbJsonWorkflowNode, props OwsRawNodeProps,
) (WorkflowNode, map[string]any, error) {
	nodeId := props.NodeId
	var workflowNode WorkflowNode

	workflowNode.Id = props.NodeId
	workflowNode.Title = jsonWorkflowNode.Title
	workflowNode.ImageName = jsonWorkflowNode.ImageName
	workflowNode.ImageTag = jsonWorkflowNode.ImageTag
	workflowNode.Command = jsonWorkflowNode.Command
	workflowNode.ArgTypes = jsonWorkflowNode.ArgTypes
	workflowNode.RequiredParams = jsonWorkflowNode.RequiredParams

	workflowNode.ArgOrder = make([]string, 0)
	for arg, argType := range jsonWorkflowNode.ArgTypes {
		if argType.IsArgument != nil && *argType.IsArgument {
			workflowNode.ArgOrder = append(workflowNode.ArgOrder, arg)
		}
	}

	parsedXml, err := parseNodeProps(props)
	if err != nil {
		return workflowNode, nil, fmt.Errorf(
			"error parsing node %d XML: %s", nodeId, err,
		)
	}

	baseProps := parsedXml.Props
	workflowNode.Iterate = parsedXml.Iterate
    workflowNode.IterAttrs = parsedXml.IterAttrs
	workflowNode.IterGroupSize = parsedXml.IterGroupSize
	workflowNode.OptionsChecked = parsedXml.OptionsChecked

	//fmt.Printf("WARNING: Adding default resource vals for node %d\n", nodeId)

	var useGpu bool
	if useGpuRaw, ok := baseProps["useGpu"]; ok {
		if bVal, ok := useGpuRaw.(bool); ok {
			useGpu = bVal
		} else {
			return workflowNode, nil, fmt.Errorf(
				"node %d has non-bool for parameters->useGpu", nodeId,
			)
		}
	} else {
		return workflowNode, nil, fmt.Errorf(
			"node %d has no key parameters->useGpu", nodeId,
		)
	}
	delete(baseProps, "useGpu")

	workflowNode.ResourceReqs.Cpus = 8
	workflowNode.ResourceReqs.MemMb = 8000
	if useGpu {
		workflowNode.ResourceReqs.Gpus = 1
	}

	return workflowNode, baseProps, nil
}

func ParseWorkflow(workflowDir string) (Workflow, error) {
	var workflow Workflow
	var ows OwsScheme

	workflowName := filepath.Base(workflowDir)
	owsBasename := fmt.Sprintf("%s.ows", workflowName)
	owsPath := filepath.Join(workflowDir, owsBasename)
	if _, err := os.Stat(owsPath); errors.Is(err, os.ErrNotExist) {
		return workflow, fmt.Errorf("could not find %s", owsPath)
	}

	owsHandle, owsOpenErr := os.Open(owsPath)
	if owsOpenErr != nil {
		panic(owsOpenErr)
	}
	defer owsHandle.Close()

	decoder := xml.NewDecoder(owsHandle)
	xmlErr := decoder.Decode(&ows)
	if xmlErr != nil {
		panic(xmlErr)
	}

	nodeIdToName := make(map[int]string)
	for _, node := range ows.Nodes.Nodes {
		nodeIdToName[node.Id] = node.Name
	}

	workflow.Nodes = make(map[int]WorkflowNode)
	workflow.NodeBaseProps = make(map[int]map[string]any)
	for _, nodeProps := range ows.NodeProps.NodeProps {
		nodeId := nodeProps.NodeId
		nodeName := nodeIdToName[nodeId]
		nodeJsonBasename := fmt.Sprintf("%s.json", nodeName)
		nodeJsonDir := path.Join(workflowDir, "widgets", workflowName, nodeName)
		nodeJsonPath := path.Join(nodeJsonDir, nodeJsonBasename)

		if _, err := os.Stat(owsPath); errors.Is(err, os.ErrNotExist) {
			return workflow, fmt.Errorf("could not find %s", nodeJsonPath)
		}

		jsonFile, err := os.Open(nodeJsonPath)
		if err != nil {
			return workflow, fmt.Errorf("error reading %s: %s", nodeJsonPath, err)
		}
		defer jsonFile.Close()

		var jsonWorkflowNode BwbJsonWorkflowNode
		jsonDecoder := json.NewDecoder(jsonFile)
		err = jsonDecoder.Decode(&jsonWorkflowNode)
		if err != nil {
			return workflow, fmt.Errorf("error parsing node %d JSON: %s", nodeProps.NodeId, err)
		}

		parsedNode, baseProps, err := parseWorkflowNode(jsonWorkflowNode, nodeProps)
		if err != nil {
			panic(err)
		}
		workflow.Nodes[nodeId] = parsedNode
		workflow.NodeBaseProps[nodeId] = baseProps
	}

	for _, owsLink := range ows.Links.Links {
		workflow.Links = append(workflow.Links, WorkflowLink{
			SourceNodeId:  owsLink.SourceNodeId,
			SinkNodeId:    owsLink.SinkNodeId,
			SourceChannel: owsLink.SourceChannel,
			SinkChannel:   owsLink.SinkChannel,
		})
	}

	return workflow, nil
}

func PrettyPrint(v interface{}) {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		fmt.Printf("PrettyPrint error: %v\n", err)
		return
	}
	fmt.Println(string(b))
}

func PrettyStr(v interface{}) string {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return fmt.Sprintf("PrettyPrint error: %v\n", err)
	}
	return string(b)
}
