import os
import glob
import sys
import functools
import jsonpickle
from collections import OrderedDict
from Orange.widgets import widget, gui, settings
import Orange.data
from Orange.data.io import FileFormat
from DockerClient import DockerClient
from BwBase import OWBwBWidget, ConnectionDict, BwbGuiElements, getIconName, getJsonName
from PyQt5 import QtWidgets, QtGui

class OWScanDirectory(OWBwBWidget):
    name = "ScanDirectory"
    description = "Enter and output a file"
    priority = 10
    icon = getIconName(__file__,"scanfile.png")
    want_main_area = False
    docker_image_name = "biodepot/scanandcopy"
    docker_image_tag = "latest"
    inputs = [("File",str,"handleInputsFile")]
    outputs = [("local_dir",str)]
    pset=functools.partial(settings.Setting,schema_only=True)
    runMode=pset(0)
    exportGraphics=pset(False)
    runTriggers=pset([])
    triggerReady=pset({})
    inputConnectionsStore=pset({})
    optionsChecked=pset({})
    serverip=pset("127.0.0.1")
    remote_dir=pset(None)
    local_dir=pset(None)
    working_dir=pset(None)
    sshUser=pset(None)
    nthreads=pset(1)
    sshDir=pset(None)
    done_dir=pset(None)
    inputExt=pset("fast5")
    maxFiles=pset(None)
    uselocal=pset(False)
    localscandir=pset(None)
    def __init__(self):
        super().__init__(self.docker_image_name, self.docker_image_tag)
        with open(getJsonName(__file__,"ScanDirectory")) as f:
            self.data=jsonpickle.decode(f.read())
            f.close()
        self.initVolumes()
        self.inputConnections = ConnectionDict(self.inputConnectionsStore)
        self.drawGUI()
    def handleInputsFile(self, value, *args):
        if args and len(args) > 0: 
            self.handleInputs("File", value, args[0][0], test=args[0][3])
        else:
            self.handleInputs("inputFile", value, None, False)
    def handleOutputs(self):
        outputValue=None
        if hasattr(self,"local_dir"):
            outputValue=getattr(self,"local_dir")
        self.send("local_dir", outputValue)
