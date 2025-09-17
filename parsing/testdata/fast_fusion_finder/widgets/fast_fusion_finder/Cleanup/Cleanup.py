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

class OWCleanup(OWBwBWidget):
    name = "Cleanup"
    description = "Setup and launch lambda functions"
    priority = 10
    icon = getIconName(__file__,"Recycling_symbol2.svg.png")
    want_main_area = False
    docker_image_name = "biodepot/fast-fusion-finder-cleanup"
    docker_image_tag = "latest"
    inputs = [("inputDir",str,"handleInputsinputDir"),("outputDir",str,"handleInputsoutputDir"),("modelDir",str,"handleInputsmodelDir"),("referenceDir",str,"handleInputsreferenceDir"),("trigger",str,"handleInputstrigger")]
    outputs = [("credentials_dir",str)]
    pset=functools.partial(settings.Setting,schema_only=True)
    runMode=pset(0)
    exportGraphics=pset(False)
    runTriggers=pset([])
    triggerReady=pset({})
    inputConnectionsStore=pset({})
    optionsChecked=pset({})
    inputDir=pset(None)
    outputDir=pset(None)
    modelDir=pset(None)
    referenceDir=pset(None)
    fastqext=pset("fastq")
    basecallext=pset("fast5")
    def __init__(self):
        super().__init__(self.docker_image_name, self.docker_image_tag)
        with open(getJsonName(__file__,"Cleanup")) as f:
            self.data=jsonpickle.decode(f.read())
            f.close()
        self.initVolumes()
        self.inputConnections = ConnectionDict(self.inputConnectionsStore)
        self.drawGUI()
    def handleInputsinputDir(self, value, *args):
        if args and len(args) > 0: 
            self.handleInputs("inputDir", value, args[0][0], test=args[0][3])
        else:
            self.handleInputs("inputFile", value, None, False)
    def handleInputsoutputDir(self, value, *args):
        if args and len(args) > 0: 
            self.handleInputs("outputDir", value, args[0][0], test=args[0][3])
        else:
            self.handleInputs("inputFile", value, None, False)
    def handleInputsmodelDir(self, value, *args):
        if args and len(args) > 0: 
            self.handleInputs("modelDir", value, args[0][0], test=args[0][3])
        else:
            self.handleInputs("inputFile", value, None, False)
    def handleInputsreferenceDir(self, value, *args):
        if args and len(args) > 0: 
            self.handleInputs("referenceDir", value, args[0][0], test=args[0][3])
        else:
            self.handleInputs("inputFile", value, None, False)
    def handleInputstrigger(self, value, *args):
        if args and len(args) > 0: 
            self.handleInputs("trigger", value, args[0][0], test=args[0][3])
        else:
            self.handleInputs("inputFile", value, None, False)
    def handleOutputs(self):
        outputValue=None
        if hasattr(self,"credentials_dir"):
            outputValue=getattr(self,"credentials_dir")
        self.send("credentials_dir", outputValue)
