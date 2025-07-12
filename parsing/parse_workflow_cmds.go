package parsing

import (
    "encoding/json"
    "fmt"
    "os"
    "regexp"
    "sort"
    "strconv"
    "strings"
)

type PatternQuery struct {
    Root     string
    Pattern  string
    FindFile bool
    FindDir  bool
}

type CmdSub struct {
    Start int
    End   int
    Pname string
}

type CmdTemplate struct {
    BaseCmd    []string
    Args       []string
    ImageName  string
    Flags      []string
    Envs       map[string]string
    InputFiles []string
    // For iterable vars, we need to keep track of which version
    // of the variable was used to generate a particular command.
    IterVals TypedParams
}

type TypedParams struct {
    Bools          map[string]bool
    Ints           map[string]int
    Doubles        map[string]float64
    Strings        map[string]string
    StrLists       map[string][]string
    IntLists       map[string][]int
    DoubleLists    map[string][]float64
    PatternQueries map[string]PatternQuery
}

func copyMapOfScalars[K comparable, V any](srcMap map[K]V) map[K]V {
    dstMap := make(map[K]V)
    for k, v := range srcMap {
        dstMap[k] = v
    }
    return dstMap
}

func copyMapOfLists[K comparable, V any](srcMap map[K][]V) map[K][]V {
    dstMap := make(map[K][]V)
    for k, v := range srcMap {
        dstMap[k] = make([]V, len(v))
        copy(dstMap[k], v)
    }
    return dstMap
}

func copyTypedParams(tp TypedParams) TypedParams {
    var newTp TypedParams
    newTp.Bools = copyMapOfScalars(tp.Bools)
    newTp.Ints = copyMapOfScalars(tp.Ints)
    newTp.Doubles = copyMapOfScalars(tp.Doubles)
    newTp.Strings = copyMapOfScalars(tp.Strings)
    newTp.PatternQueries = copyMapOfScalars(tp.PatternQueries)
    newTp.StrLists = copyMapOfLists(tp.StrLists)
    newTp.IntLists = copyMapOfLists(tp.IntLists)
    newTp.DoubleLists = copyMapOfLists(tp.DoubleLists)
    return newTp
}

func copyCmdTemplate(template CmdTemplate) CmdTemplate {
    var newTemplate CmdTemplate
    newTemplate.ImageName = template.ImageName
    newTemplate.BaseCmd = make([]string, len(template.BaseCmd))
    newTemplate.Args = make([]string, len(template.Args))
    newTemplate.Flags = make([]string, len(template.Flags))
    newTemplate.InputFiles = make([]string, len(template.InputFiles))

    copy(newTemplate.BaseCmd, template.BaseCmd)
    copy(newTemplate.Args, template.Args)
    copy(newTemplate.Flags, template.Flags)
    copy(newTemplate.InputFiles, template.InputFiles)

    newTemplate.Envs = copyMapOfScalars(template.Envs)
    newTemplate.IterVals = copyTypedParams(template.IterVals)

    return newTemplate
}

func getRequiredParams(node WorkflowNode) map[string]bool {
    reqParams := make(map[string]bool)
    for _, pname := range node.RequiredParams {
        reqParams[pname] = true
    }
    return reqParams
}

func argTypeIsStr(argType string) bool {
    return argType == "text" || argType == "str" ||
        argType == "directory" || argType == "file" ||
        argType == "dryRunStr"
}

func parseTypedParams(node WorkflowNode) (TypedParams, error) {
    var tp TypedParams
    tp.Bools = make(map[string]bool)
    tp.Ints = make(map[string]int)
    tp.Doubles = make(map[string]float64)
    tp.Strings = make(map[string]string)
    tp.StrLists = make(map[string][]string)
    tp.IntLists = make(map[string][]int)
    tp.DoubleLists = make(map[string][]float64)
    tp.PatternQueries = make(map[string]PatternQuery)

    for pname, argType := range node.ArgTypes {
        propValRaw, ok := node.Props[pname]
        if !ok || propValRaw == nil {
            continue
        }

        castErr := fmt.Errorf("node %d, var %s cannot be cast to %s", node.Id, pname, argType.ArgType)
        if argType.ArgType == "bool" {
            pValBool, ok := propValRaw.(bool)
            if !ok {
                return tp, castErr
            }
            tp.Bools[pname] = pValBool
        } else if argType.ArgType == "int" {
            pValDouble, ok := propValRaw.(float64)
            if !ok {
                return tp, castErr
            }
            tp.Ints[pname] = int(pValDouble)
        } else if argType.ArgType == "double" {
            pValDouble, ok := propValRaw.(float64)
            if !ok {
                return tp, castErr
            }
            tp.Doubles[pname] = pValDouble
        } else if argTypeIsStr(argType.ArgType) {
            pValStr, ok := propValRaw.(string)
            if !ok {
                return tp, castErr
            }
            tp.Strings[pname] = pValStr
        } else if strings.HasSuffix(argType.ArgType, "list") {
            pValList, ok := propValRaw.([]any)
            if !ok {
                return tp, castErr
            }
            listType := strings.Split(argType.ArgType, " ")[0]

            
            if argTypeIsStr(listType) {
                tp.StrLists[pname] = make([]string, len(pValList))
                for i, v := range pValList {
                    pValStr, ok := v.(string)
                    if !ok {
                        return tp, castErr
                    }
                    tp.StrLists[pname][i] = pValStr
                }
            } else if listType == "int" {
                tp.IntLists[pname] = make([]int, len(pValList))
                for i, v := range pValList {
                    // Casting JSON to interface{} makes numbers into floats by default,
                    // so v.(int) would fail.
                    pValDouble, ok := v.(float64)
                    intError := fmt.Errorf("node %d, var %s cannot be cast to int", node.Id, pname)
                    var pValInt int
                    if ok {
                        pValInt = int(pValDouble)
                    } else {
                        // The default behavior of BWB seems to be storing int / double lists as JSON
                        // lists of strs. This seems counterintuitive, so I want to support both this
                        // and the more natural format of using JSON ints / numbers.
                        pValStr, strOk := v.(string)
                        if !strOk {
                            return tp, intError
                        }

                        var convErr error
                        pValInt, convErr = strconv.Atoi(pValStr)
                        if convErr != nil {
                            return tp, intError
                        }
                    }

                    tp.IntLists[pname][i] = pValInt
                }
            } else if listType == "double" {
                tp.DoubleLists[pname] = make([]float64, len(pValList))
                for i, v := range pValList {
                    doubleError := fmt.Errorf("node %d, var %s cannot be cast to double", node.Id, pname)
                    pValDouble, ok := v.(float64)
                    if !ok {
                        pValStr, strOk := v.(string)
                        if !strOk {
                            return tp, doubleError
                        }

                        var convErr error
                        pValDouble, convErr = strconv.ParseFloat(pValStr, 64)
                        if convErr != nil {
                            return tp, doubleError
                        }
                    }

                    tp.DoubleLists[pname][i] = pValDouble
                }
            }
        } else if argType.ArgType == "patternQuery" {
            pqError := fmt.Errorf("node %d, var %s cannot be parsed as pattern query", node.Id, pname)
            pValMap, mapOk := propValRaw.(map[string]any)
            if !mapOk {
                return tp, pqError
            }

            root, rootOk := pValMap["root"].(string)
            pattern, patternOk := pValMap["pattern"].(string)
            findFile, findFileOk := pValMap["findFile"].(bool)
            findDir, findDirOk := pValMap["findDir"].(bool)
            if !(rootOk && patternOk && findFileOk && findDirOk) {
                return tp, pqError
            }

            tp.PatternQueries[pname] = PatternQuery{
                Root:     root,
                Pattern:  pattern,
                FindFile: findFile,
                FindDir:  findDir,
            }
        }
    }
    return tp, nil
}

func strRemoveQuotes(str string) string {
    if strings.HasPrefix(str, "'") && strings.HasSuffix(str, "'") {
        return str[1 : len(str)-1]
    }
    return str
}

func getEnvValStrFromList[T any](list []T) string {
    out := "["
    for i, elRaw := range list {
        out += fmt.Sprintf("\\\"%v\\\"", elRaw)

        if i < len(list)-1 {
            out += ","
        }
    }
    out += "]"
    return out
}

func getEnvValStr(pname string, argType string, tp TypedParams) (string, error) {
    notFoundError := fmt.Errorf("param %s not found", pname)
    if argType == "bool" {
        if pVal, ok := tp.Bools[pname]; ok {
            if pVal {
                return "True", nil
            } else {
                return "False", nil
            }
        }
        return "", notFoundError
    } else if argType == "int" {
        if pVal, ok := tp.Ints[pname]; ok {
            return strconv.Itoa(pVal), nil
        }
        return "", notFoundError
    } else if argType == "double" {
        if pVal, ok := tp.Doubles[pname]; ok {
            return strconv.FormatFloat(pVal, 'f', -1, 64), nil
        }
        return "", notFoundError
    } else if argTypeIsStr(argType) {
        if pVal, ok := tp.Strings[pname]; ok {
            return strRemoveQuotes(pVal), nil
        }
        return "", notFoundError
    } else if strings.HasSuffix(argType, "list") {
        listType := strings.Split(argType, " ")[0]

        if argTypeIsStr(listType) {
            if _, ok := tp.StrLists[pname]; !ok {
                return "", notFoundError
            }
            return getEnvValStrFromList(tp.StrLists[pname]), nil
        } else if listType == "int" {
            if _, ok := tp.IntLists[pname]; !ok {
                return "", notFoundError
            }
            return getEnvValStrFromList(tp.IntLists[pname]), nil
        } else if listType == "double" {
            if _, ok := tp.DoubleLists[pname]; !ok {
                return "", notFoundError
            }
            return getEnvValStrFromList(tp.DoubleLists[pname]), nil
        }
    } else if argType == "patternQuery" {
        return "", fmt.Errorf("pattern queries not supported for env")
    }

    return "", fmt.Errorf("unknown argtype %s for %s", argType, pname)
}

// Evaluate non-iterable, non-pattern query env vars, populate template.Envs with
// serialized env var values.
func evaluateEnvVars(node WorkflowNode, template *CmdTemplate, tp TypedParams) error {
    template.Envs = make(map[string]string)
    for pname, argType := range node.ArgTypes {
        if argType.Env == nil {
            continue
        }
        envVar := *argType.Env

        propValRaw, ok := node.Props[pname]
        if !ok || propValRaw == nil {
            continue
        }

        if argType.ArgType == "bool" {
            if !ok {
                return fmt.Errorf("could node find node %d var %s", node.Id, pname)
            }

            if valStr, err := getEnvValStr(pname, argType.ArgType, tp); err != nil {
                return err
            } else {
                template.Envs[envVar] = valStr
            }
        } else {
            if !node.OptionsChecked[pname] {
                continue
            }

            if _, isIterable := node.IterAttrs[pname]; isIterable {
                continue
            }

            if valStr, err := getEnvValStr(pname, argType.ArgType, tp); err != nil {
                return err
            } else {
                template.Envs[envVar] = valStr
            }
        }

    }
    return nil
}

func joinFlagVal(flag string, val string) string {
    trimmedFlag := strings.TrimSpace(flag)
    trimmedVal := strings.TrimSpace(val)
    if trimmedFlag[len(trimmedFlag)-1] == '=' {
        return fmt.Sprintf("%s%s", trimmedFlag, trimmedVal)
    }
    return fmt.Sprintf("%s %s", trimmedFlag, trimmedVal)
}

func joinFlagValList(flag string, list []string) []string {
    result := make([]string, len(list))
    for i, val := range list {
        result[i] = joinFlagVal(flag, val)
    }
    return result
}

func getScalarArgStr[T any](
    typedProps map[string]T, argType WorkflowArgType,
    pname string, isFlag bool,
) (string, error) {
    if flagVal, ok := typedProps[pname]; ok {
        flagValStr := fmt.Sprintf("%v", flagVal)
        if isFlag {
            return joinFlagVal(*argType.Flag, flagValStr), nil
        }
        return flagValStr, nil
    }

    return "", fmt.Errorf("could not find parameter %s", pname)
}

func getListArgStr[T any](
    typedProps map[string][]T, argType WorkflowArgType,
    pname string, isFlag bool,
) (string, error) {
    flagVal := ""
    if _, ok := typedProps[pname]; ok {
        for i, val := range typedProps[pname] {
            flagVal += fmt.Sprintf("%v", val)
            if i < len(typedProps[pname]) - 1 {
                flagVal += " "
            }
        }

        if isFlag {
            return joinFlagVal(*argType.Flag, flagVal), nil
        }
        return flagVal, nil
    }

    return "", fmt.Errorf("could not find parameter %s", pname)
}

func getArgStr(pname string, argType WorkflowArgType, tp TypedParams, isFlag bool) (string, error) {
    if argType.ArgType == "bool" {
        if tp.Bools[pname] {
            return *argType.Flag, nil
        }
        return "", nil
    }

    if argTypeIsStr(argType.ArgType) {
        return getScalarArgStr(tp.Strings, argType, pname, isFlag)
    }

    if argType.ArgType == "int" {
        return getScalarArgStr(tp.Ints, argType, pname, isFlag)
    }

    if argType.ArgType == "double" {
        return getScalarArgStr(tp.Doubles, argType, pname, isFlag)
    }

    if strings.HasSuffix(argType.ArgType, "list") {
        flagVal := ""
        listType := strings.Split(argType.ArgType, " ")[0]
        if argTypeIsStr(listType) {
            return getListArgStr(tp.StrLists, argType, pname, isFlag)
        } else if listType == "int" {
            return getListArgStr(tp.IntLists, argType, pname, isFlag)
        } else if listType == "double" {
            return getListArgStr(tp.DoubleLists, argType, pname, isFlag)
        }

        if isFlag {
            return joinFlagVal(*argType.Flag, flagVal), nil
        }
        return flagVal, nil
    }

    return "", fmt.Errorf("cannot gen flag value for pname %s, type %s", pname, argType.ArgType)
}

func evaluateFlags(node WorkflowNode, template *CmdTemplate, tp TypedParams) error {
    reqParams := getRequiredParams(node)

    for pname, argType := range node.ArgTypes {
        if argType.Flag == nil {
            continue
        }

        if _, isIterable := node.IterAttrs[pname]; isIterable {
            continue
        }

        addParam := false
        addParam = addParam || node.OptionsChecked[pname]
        addParam = addParam || reqParams[pname]
        if argType.ArgType == "bool" {
            addParam = addParam && tp.Bools[pname]
        }

        if addParam {
            flagStr, err := getArgStr(pname, argType, tp, true)
            if err != nil {
                return err
            }
            template.Flags = append(template.Flags, flagStr)
        }
    }
    return nil
}

func evaluateArgs(node WorkflowNode, template *CmdTemplate, tp TypedParams) error {
    for pname, argType := range node.ArgTypes {
        if argType.IsArgument == nil || !*argType.IsArgument {
            continue
        }

        if _, isIterable := node.IterAttrs[pname]; isIterable {
            continue
        }

        argStr, err := getArgStr(pname, argType, tp, false)
        if err != nil {
            return err
        }
        template.Args = append(template.Args, argStr)
    }

    return nil
}

/**
 * Parameters:
 *  - list: Some list to be broken up into chunks of group size.
 *  - groupSize: The size of each chunk.
 * Returns:
 *  - slice of serialzied vals, where nth el is serialized nth group of "list".
**/
func getIterableGroups[T any](list []T, groupSize int) [][]T {
    var resultLen int
    if len(list)%groupSize == 0 {
        resultLen = len(list) / groupSize
    } else {
        resultLen = (len(list) / groupSize) + 1
    }

    iterVals := make([][]T, resultLen)
    for i := 0; i < len(list); i += groupSize {
        resultInd := i / groupSize
        group := make([]T, 0, groupSize)

        for j := 0; j < groupSize; j++ {
            if i+j < len(list) {
                group = append(group, list[i+j])
            } else {
                group = append(group, list[len(list)-1])
            }
        }

        iterVals[resultInd] = group
    }

    return iterVals
}

func getIterableGroupStr[T any](groups [][]T, isEnv bool) []string {
    iterStrs := make([]string, len(groups))
    for i, group := range groups {
        if isEnv {
            iterStrs[i] = getEnvValStrFromList(group)
        } else {
            iterStrs[i] = ""
            for j, val := range group {
                iterStrs[i] += fmt.Sprintf("%v", val)
                if j < len(group) - 1 {
                    iterStrs[i] += " "
                }
            }
        }
    }
    return iterStrs
}

/** Parameters
*   - typedProps: map of node pname to the value corresponding to that pname.
*           As this is an iterable attr, the value will always be a list.
*   - iterTps: A typedParams structure to fill with the
*
**/
func processIterableAttr[T any](
    typedProps map[string][]T, node WorkflowNode, reqParams map[string]bool,
    argType WorkflowArgType, pname string, groupSize int,
    envStrs, argStrs, flagStrs map[string][]string,
) ([][]T, error) {
    addEnv := false
    if argType.Env != nil && *argType.Env != "" {
        addEnv = true
    }

    addArg := false
    addFlag := false
    if argType.IsArgument != nil && *argType.IsArgument {
        addArg = true
    } else {
        addFlag = addFlag || node.OptionsChecked[pname]
        addFlag = addFlag || reqParams[pname]
        if addFlag && argType.Flag == nil {
            return nil, fmt.Errorf("nil flag for %s", pname)
        }
    }

    if list, ok := typedProps[pname]; !ok || list == nil {
        notFoundErr := fmt.Errorf(
            "iterable key %s of type %s not found or null",
            pname, argType.ArgType,
        )
        return nil, notFoundErr
    }

    groups := getIterableGroups(typedProps[pname], groupSize)
    if addEnv {
        envStrs[pname] = getIterableGroupStr(groups, true)
    }

    if addArg {
        argStrs[pname] = getIterableGroupStr(groups, false)
    } else if addFlag {
        flagVal := getIterableGroupStr(groups, false)
        flagStrs[pname] = joinFlagValList(*argType.Flag, flagVal)
    }

    return groups, nil
}

func evaluateIterables(node WorkflowNode, template CmdTemplate, tp TypedParams) ([]CmdTemplate, error) {
    if len(node.IterAttrs) == 0 {
        return []CmdTemplate{template}, nil
    }

    strGroups := make(map[string][][]string)
    intGroups := make(map[string][][]int)
    doubleGroups := make(map[string][][]float64)

    argStrs := make(map[string][]string)
    flagStrs := make(map[string][]string)
    envStrs := make(map[string][]string)

    reqParams := getRequiredParams(node)

    maxSize := 0
    groupsPerPname := make(map[string]int)
    for pname, groupSize := range node.IterAttrs {
        argType, ok := node.ArgTypes[pname]
        if !ok {
            return nil, fmt.Errorf("iterable key %s is not a parameter", pname)
        }

        if !strings.HasSuffix(argType.ArgType, "list") {
            return nil, fmt.Errorf("iterable key %s is not a list", pname)
        }

        var err error
        listType := strings.Split(argType.ArgType, " ")[0]
        if argTypeIsStr(listType) {
            strGroups[pname], err = processIterableAttr(
                tp.StrLists, node, reqParams, argType,
                pname, groupSize, envStrs, argStrs, flagStrs,
            )
            groupsPerPname[pname] = len(strGroups[pname])
        } else if listType == "int" {
            intGroups[pname], err = processIterableAttr(
                tp.IntLists, node, reqParams, argType,
                pname, groupSize, envStrs, argStrs, flagStrs,
            )
            groupsPerPname[pname] = len(intGroups[pname])
        } else if listType == "double" {
            doubleGroups[pname], err = processIterableAttr(
                tp.DoubleLists, node, reqParams, argType,
                pname, groupSize, envStrs, argStrs, flagStrs,
            )
            groupsPerPname[pname] = len(doubleGroups[pname])
        } else {
            return nil, fmt.Errorf("unsupported list type %s for param %s", argType.ArgType, pname)
        }

        if err != nil {
            return nil, fmt.Errorf("error processing attr %s: %s", pname, err)
        }

        maxSize = max(groupsPerPname[pname], maxSize)
    }

    ret := make([]CmdTemplate, 0, maxSize)
    for i := 0; i < maxSize; i++ {
        ithIteration := copyCmdTemplate(template)
        for pname := range node.IterAttrs {
            numGroups := groupsPerPname[pname]

            if envStrList, pnameIsEnv := envStrs[pname]; pnameIsEnv {
                numGroups = len(envStrList)
                if envKey := node.ArgTypes[pname].Env; envKey != nil {
                    ithIteration.Envs[*envKey] = envStrList[i%len(envStrList)]
                } else {
                    return ret, fmt.Errorf("could not find env key for key %s", pname)
                }
            }

            if argStrList, pnameIsArg := argStrs[pname]; pnameIsArg {
                numGroups = len(argStrList)
                ithIteration.Args = append(
                    ithIteration.Args, argStrList[i%len(argStrList)],
                )
            } else if flagStrList, pnameIsFlag := flagStrs[pname]; pnameIsFlag {
                numGroups = len(flagStrList)
                ithIteration.Flags = append(
                    ithIteration.Flags, flagStrList[i%len(flagStrList)],
                )
            }

            if numGroups > 0 {
                listType := strings.Split(node.ArgTypes[pname].ArgType, " ")[0]
                if argTypeIsStr(listType) {
                    ithIteration.IterVals.StrLists[pname] = strGroups[pname][i%numGroups]
                } else if node.ArgTypes[pname].ArgType == "int" {
                    ithIteration.IterVals.IntLists[pname] = intGroups[pname][i%numGroups]
                } else if node.ArgTypes[pname].ArgType == "double" {
                    ithIteration.IterVals.DoubleLists[pname] = doubleGroups[pname][i%numGroups]
                }
            }
        }

        ret = append(ret, ithIteration)
    }
    return ret, nil
}

// Return map of starting location of each substitution to
// location. Doing it as a map lets us easily iterate over
// substitutions in order when building cmd.
func getCmdSubs(cmd string) map[int]CmdSub {
    // Regex for strings of form "_bwb{VAR_NAME}"
    ret := make(map[int]CmdSub)
    r := regexp.MustCompile(`_bwb\{([^\}]*)\}`)
    subStrMatches := r.FindAllStringSubmatchIndex(cmd, -1)

    for _, match := range subStrMatches {
        ret[match[0]] = CmdSub{
            // start and end give start and end positions of _bwb{...}
            // in command.
            Start: match[0],
            End:   match[1],
            // pname is whatever is inside _bwb{...} brackets
            Pname: cmd[match[2]:match[3]],
        }
    }

    return ret
}

func performCmdSubs(
    node WorkflowNode, template *CmdTemplate, nonIterVals TypedParams, skipIters bool,
) error {
    revisedCmds := make([]string, len(node.Command))
    for i, cmdClause := range node.Command {
        revisedCmds[i] = ""
        cmdSubs := getCmdSubs(cmdClause)

        var startLocs []int
        for startLoc := range cmdSubs {
            startLocs = append(startLocs, startLoc)
        }
        sort.Ints(startLocs)

        previousEnd := 0
        for _, startLoc := range startLocs {
            pname := cmdSubs[startLoc].Pname
            subIdStr := fmt.Sprintf(
                "node %d, cmd %d, pos %d-%d",
                node.Id, i, startLoc, cmdSubs[startLoc].End,
            )

            _, pnameIsIterable := node.IterAttrs[pname]
            if pnameIsIterable && skipIters {
                continue
            }

            if argType, validPname := node.ArgTypes[pname]; validPname {
                var serializedPval string
                var serializationErr error

                if pnameIsIterable {
                    serializedPval, serializationErr = getArgStr(
                        pname, argType, template.IterVals, false,
                    )
                } else {
                    serializedPval, serializationErr = getArgStr(
                        pname, argType, nonIterVals, false,
                    )
                }

                if serializationErr != nil {
                    return fmt.Errorf("error substituting %s: %s", subIdStr, serializationErr)
                }
                revisedCmds[i] += cmdClause[previousEnd:startLoc] + serializedPval
            } else {
                return fmt.Errorf(
                    "%s has invalid substitution variable %s", subIdStr, pname,
                )
            }

            previousEnd = cmdSubs[startLoc].End
        }

        revisedCmds[i] += cmdClause[previousEnd:]
    }

    template.BaseCmd = revisedCmds
    return nil
}

func parseNodeCmd(node WorkflowNode) ([]CmdTemplate, error) {
    nodeId := node.Id
    var template CmdTemplate
    template.BaseCmd = node.Command
    template.ImageName = fmt.Sprintf("%s:%s", node.ImageName, node.ImageTag)
    tp, err := parseTypedParams(node)
    if err != nil {
        return []CmdTemplate{}, err
    }
    err = evaluateEnvVars(node, &template, tp)
    if err != nil {
        panic(fmt.Errorf("error parsing node %d env vars: %s", nodeId, err))
    }

    err = evaluateFlags(node, &template, tp)
    if err != nil {
        panic(fmt.Errorf("error parsing node %d flags: %s", nodeId, err))
    }

    err = evaluateArgs(node, &template, tp)
    if err != nil {
        panic(fmt.Errorf("error parsing node %d args: %s", nodeId, err))
    }

    err = performCmdSubs(node, &template, tp, true)
    if err != nil {
        panic(fmt.Errorf("error parsing node %d substitutions: %s", nodeId, err))
    }

    var iterations []CmdTemplate
    iterations, err = evaluateIterables(node, template, tp)
    if err != nil {
        panic(fmt.Errorf("error parsing node %d iterables: %s", nodeId, err))
    }

    for i := range iterations {
        err := performCmdSubs(node, &iterations[i], tp, false)
        if err != nil {
            panic(fmt.Errorf(
                "error parsing node %d, iteration %d substitutions: %s",
                nodeId, i, err,
            ))
        }
    }

    return iterations, nil
}

func getInAndOutLinks(
    workflow Workflow,
) (map[int]map[string]WorkflowLink, map[int]map[string]WorkflowLink, error) {
    // dstNode -> dstPname -> srcNode
    inLinks := make(map[int]map[string]WorkflowLink)
    outLinks := make(map[int]map[string]WorkflowLink)

    // NOTE: There is no stipulation in BWB / OWS that a sink channel
    //       corresponds to a parameter entry or ArgType entry of the
    //       sink node, so we can't check that.
    for _, link := range workflow.Links {
        srcNode := link.SourceNodeId
        srcChan := link.SourceChannel
        sinkNode := link.SinkNodeId
        sinkChan := link.SinkChannel
        if _, validSrc := workflow.Nodes[srcNode]; !validSrc {
            return nil, nil, fmt.Errorf(
                "link from %d.%s -> %d.%s has invalid source %d",
                srcNode, srcChan, sinkNode, sinkChan, srcNode,
            )
        }

        if _, validSink := workflow.Nodes[sinkNode]; !validSink {
            return nil, nil, fmt.Errorf(
                "link from %d.%s -> %d.%s has invalid sink %d",
                srcNode, srcChan, sinkNode, sinkChan, sinkNode,
            )
        }

        if _, validSrcChan := workflow.Nodes[srcNode].ArgTypes[srcChan]; !validSrcChan {
            return nil, nil, fmt.Errorf(
                "link from %d.%s -> %d.%s has invalid source channel %s",
                srcNode, srcChan, sinkNode, sinkChan, srcChan,
            )
        }

        if inLinks[link.SinkNodeId] == nil {
            inLinks[link.SinkNodeId] = make(map[string]WorkflowLink)
        }
        if outLinks[link.SourceNodeId] == nil {
            outLinks[link.SourceNodeId] = make(map[string]WorkflowLink)
        }
        inLinks[link.SinkNodeId][link.SinkChannel] = link
        outLinks[link.SourceNodeId][link.SourceChannel] = link
    }

    return inLinks, outLinks, nil
}

func cmdToStr(template CmdTemplate) string {
    out := ""
    for envK, envV := range template.Envs {
        out += fmt.Sprintf("%s=%s ", envK, envV)
    }


    out += strings.Join(template.BaseCmd, " && ") + " "
    out += strings.Join(template.Flags, " ") + " "
    out += strings.Join(template.Args, " ") + " "

    return out
}

func dryRun(workflow Workflow) ([]string, error) {
    inLinks, _, err := getInAndOutLinks(workflow)
    if err != nil {
        return nil, err
    }
        
    var cmdStrs []string
    for nodeId, node := range workflow.Nodes {
        revisedNode := copyWorkflowNode(node)
        reqParams := getRequiredParams(node)

        // Verify that all properties have corresponding argTypes.
        for pname := range node.Props {
            if _, pnameHasArgType := node.ArgTypes[pname]; !pnameHasArgType {
                return nil, fmt.Errorf(
                    "error parsing node %d parameters: attr %s has no arg type entry",
                    nodeId, pname,
                )
            }

            _, pnameIsIterable := node.IterAttrs[pname]

            // Replace all incoming links with strings indicating their origin
            // node and origin channel. These strings will be treated as the values
            // of those paramters, so the paramter parsing code can otherwise work
            // as normal.
            inLink, hasIncomingLink := inLinks[nodeId][pname]
            if hasIncomingLink {
                if pnameIsIterable {
                    revisedNode.Props[pname] = fmt.Sprintf(
                        "ITERABLE{%d.%s}", inLink.SourceNodeId, inLink.SourceChannel,
                    )
                } else {
                    revisedNode.Props[pname] = fmt.Sprintf(
                        "{%d.%s}", inLink.SourceNodeId, inLink.SourceChannel,
                    )
                }
                argTypeEntry := revisedNode.ArgTypes[pname]
                argTypeEntry.ArgType = "dryRunStr"
                revisedNode.ArgTypes[pname] = argTypeEntry
            } 
            
            if reqParams[pname]  && revisedNode.Props[pname] == nil {
                return nil, fmt.Errorf(
                    "required parameter %s of node %d is nil",
                    pname, nodeId,
                )
            }
        }

        cmdTemplates, cmdErrs := parseNodeCmd(revisedNode)
        if cmdErrs != nil {
            return nil, fmt.Errorf("Error parsing cmds: %s", cmdErrs)
        }

        for _, template := range cmdTemplates {
            //fmt.Println("Node", nodeId)
            //PrettyPrint(cmdTemplates)
            //fmt.Println(cmdToStr(template))
            cmdStrs = append(cmdStrs, cmdToStr(template))
        }
    }

    return cmdStrs, nil
}


func validateWorkflow(workflow Workflow) error {
    // dstNode -> dstPname -> srcNode
    idToNode := make(map[int]WorkflowNode)
    for _, node := range workflow.Nodes {
        idToNode[node.Id] = node
    }

    // Verify that link values names correspond to actual nodes parameters.
    // Populate inLinks map for easy lookup of incoming links.
    inLinks, _, err := getInAndOutLinks(workflow)
    if err != nil {
        return err
    }

    for nodeId, node := range workflow.Nodes {
        reqParams := getRequiredParams(node)

        // Verify that all properties have corresponding argTypes.
        for pname := range node.Props {
            if _, pnameHasArgType := node.ArgTypes[pname]; !pnameHasArgType {
                return fmt.Errorf(
                    "error parsing node %d parameters: attr %s has no arg type entry",
                    nodeId, pname,
                )
            }

            // Verify that required params have non-null values,
            // unless there is an incoming link to this node that
            // will set them to a non-null value during workflow
            // runtime.
            if reqParams[pname] {
                _, hasIncomingLink := inLinks[nodeId][pname]
                if node.Props[pname] == nil && !hasIncomingLink {
                    return fmt.Errorf(
                        "required parameter %s of node %d is nil",
                        pname, nodeId,
                    )
                }
            }

        }

        // Verify that all node params listed as iterable have iterable type
        // (list or patternQuery) and that they have corresponding values /
        // argTypes.
        for iterKey := range node.IterAttrs {
            if _, iterKeyHasProp := node.Props[iterKey]; !iterKeyHasProp {
                return fmt.Errorf(
                    "iterable key %s of node %d has no corresponding parameter",
                    iterKey, nodeId,
                )
            }

            if _, iterKeyHasArgType := node.ArgTypes[iterKey]; iterKeyHasArgType {
                argType := node.ArgTypes[iterKey].ArgType
                if !strings.HasSuffix(argType, "list") && argType != "patternQuery" {
                    return fmt.Errorf(
                        "iterable key %s of node %d has non-list arg type %s",
                        iterKey, nodeId, argType,
                    )
                }
            } else {
                return fmt.Errorf(
                    "iterable key %s of node %d has no corresponding arg type",
                    iterKey, nodeId,
                )
            }
        }

        // Verify that there are no type errors inside the node's parameter values.
        _, paramTypeErr := parseTypedParams(node)
        if paramTypeErr != nil {
            return fmt.Errorf(
                "error parsing node %d parameters: %s",
                nodeId, paramTypeErr,
            )
        }

        // Make sure all substitutions using _bwb{...} in command body refer
        // to valid variables.
        // Clauses of cmd will be conjoined as [clause1] && [clause2] && ...
        for cmdInd, cmdClause := range node.Command {
            cmdSubKeys := getCmdSubs(cmdClause)
            for _, cmdSub := range cmdSubKeys {
                if _, validPname := node.Props[cmdSub.Pname]; !validPname {
                    errFmt := "substitution for non-existent var %s at " +
                        "positions %d-%d of cmd %d of node %d"
                    return fmt.Errorf(
                        errFmt, cmdSub.Pname, cmdSub.Start, cmdSub.End,
                        cmdInd, nodeId,
                    )
                }
            }
        }
    }

    return nil
}

func main() {
    workflow, err := ParseWorkflow("tests/OWS/Bulk-RNA-seq/star_salmon_aws")
    if err != nil {
        panic(err)
    }

    validateErr := validateWorkflow(workflow)
    if validateErr != nil {
        panic(fmt.Errorf("workflow validation error: %s", validateErr))
    }
    fmt.Println("Successfully validated workflow")

    wfSerialized, _ := json.MarshalIndent(workflow, "", "  ")
    err = os.WriteFile("bulk_rna.json", wfSerialized, 0644)

    for _, node := range workflow.Nodes {
        templates, err := parseNodeCmd(node)
        if err != nil {
            panic(err)
        }
        PrettyPrint(templates)
    }

    if err != nil {
        panic(err)
    }
}
