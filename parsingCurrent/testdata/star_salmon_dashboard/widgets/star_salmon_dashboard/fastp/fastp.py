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

class OWfastp(OWBwBWidget):
    name = "fastp"
    description = "Enter and output a file"
    priority = 10
    icon = getIconName(__file__,"fastp.png")
    want_main_area = False
    docker_image_name = "biodepot/fastp"
    docker_image_tag = "0.23.4__bookworm-slim__amd64"
    inputs = [("inputFiles",str,"handleInputsinputFiles")]
    outputs = [("outputfastq",str)]
    pset=functools.partial(settings.Setting,schema_only=True)
    runMode=pset(0)
    exportGraphics=pset(False)
    runTriggers=pset([])
    triggerReady=pset({})
    inputConnectionsStore=pset({})
    optionsChecked=pset({})
    htmlfile=pset(None)
    jsonfile=pset(None)
    inputfiles=pset([])
    outputfastq=pset(None)
    pairedends=pset(False)
    def __init__(self):
        super().__init__(self.docker_image_name, self.docker_image_tag)
        with open(getJsonName(__file__,"fastp")) as f:
            self.data=jsonpickle.decode(f.read())
            f.close()
        self.initVolumes()
        self.inputConnections = ConnectionDict(self.inputConnectionsStore)
        self.drawGUI()
    def handleInputsinputFiles(self, value, *args):
        if args and len(args) > 0: 
            self.handleInputs("inputFiles", value, args[0][0], test=args[0][3])
        else:
            self.handleInputs("inputFile", value, None, False)
    def handleOutputs(self):
        outputValue=None
        if hasattr(self,"outputfastq"):
            outputValue=getattr(self,"outputfastq")
        self.send("outputfastq", outputValue)
