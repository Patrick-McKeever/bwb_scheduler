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

class OWgnumeric(OWBwBWidget):
    name = "gnumeric"
    description = "Open source spreadsheet"
    priority = 2
    icon = getIconName(__file__,"gnumeric.png")
    want_main_area = False
    docker_image_name = "biodepot/gnumeric"
    docker_image_tag = "latest"
    inputs = [("inputDirectory",str,"handleInputsinputDirectory"),("Trigger",str,"handleInputsTrigger")]
    outputs = [("inputDirectory",str)]
    pset=functools.partial(settings.Setting,schema_only=True)
    runMode=pset(0)
    exportGraphics=pset(False)
    runTriggers=pset([])
    triggerReady=pset({})
    inputConnectionsStore=pset({})
    optionsChecked=pset({})
    inputDirectory=pset("")
    inputFile=pset("summary")
    def __init__(self):
        super().__init__(self.docker_image_name, self.docker_image_tag)
        with open(getJsonName(__file__,"gnumeric")) as f:
            self.data=jsonpickle.decode(f.read())
            f.close()
        self.initVolumes()
        self.inputConnections = ConnectionDict(self.inputConnectionsStore)
        self.drawGUI()
    def handleInputsinputDirectory(self, value, *args):
        if args and len(args) > 0: 
            self.handleInputs("inputDirectory", value, args[0][0], test=args[0][3])
        else:
            self.handleInputs("inputFile", value, None, False)
    def handleInputsTrigger(self, value, *args):
        if args and len(args) > 0: 
            self.handleInputs("Trigger", value, args[0][0], test=args[0][3])
        else:
            self.handleInputs("inputFile", value, None, False)
    def handleOutputs(self):
        outputValue=None
        if hasattr(self,"inputDirectory"):
            outputValue=getattr(self,"inputDirectory")
        self.send("inputDirectory", outputValue)
