import json

with open("bulkrna_v1_ORIG.json", "r") as f:
    wf = json.load(f)

newNodes = {}
for nodeId, node in wf["resolved_workflow"]["nodes"].items():
    newParams = {}
    for param in node["inputs"]:
        newParams[param["name"]] = param
    newNodes[nodeId] = node
    newNodes[nodeId]["inputs"] = newParams
    newParams = {}
    for param in node["outputs"]:
        newParams[param["name"]] = param
    newNodes[nodeId] = node
    newNodes[nodeId]["outputs"] = newParams
    command = node["launch"]["command"]
    newCommand = command[-1].split("&&")
    newNodes[nodeId]["launch"]["command"] = newCommand


wf["resolved_workflow"]["nodes"] = newNodes
with open("bulkrna_v1_NEW.json", "w+") as f:
    json.dump(wf, f, indent=4)
