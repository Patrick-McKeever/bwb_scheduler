import sys, pickle, json, base64, ast

if __name__ == "__main__":
    inp_format = sys.argv[1]
    assert inp_format in ["literal", "pickle"]
    inp = sys.stdin.read()
    if inp_format == "pickle":
        pickled_bytes = base64.b64decode(inp)
        obj = pickle.loads(pickled_bytes)
    else:
        obj = ast.literal_eval(inp)

    
    for k in list(obj.keys()):
        if type(obj[k]) is bytes:
            del obj[k]
    print(json.dumps(obj))
