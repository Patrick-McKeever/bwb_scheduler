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

class OWfast_dorado(OWBwBWidget):
    name = "fast_dorado"
    description = "Dorado Basecaller"
    priority = 3
    icon = getIconName(__file__,"dorado.png")
    want_main_area = False
    docker_image_name = "biodepot/fast-dorado"
    docker_image_tag = "latest"
    inputs = [("inputDir",str,"handleInputsinputDir"),("trigger",str,"handleInputstrigger"),("trigger2",str,"handleInputstrigger2"),("reference",str,"handleInputsreference"),("outputDir",str,"handleInputsoutputDir"),("modelFile",str,"handleInputsmodelFile")]
    outputs = [("outputDir",str),("fastqfilename",str),("inputDir",str)]
    pset=functools.partial(settings.Setting,schema_only=True)
    runMode=pset(0)
    exportGraphics=pset(False)
    runTriggers=pset([])
    triggerReady=pset({})
    inputConnectionsStore=pset({})
    optionsChecked=pset({})
    inputDir=pset(None)
    outputDir=pset(None)
    modeldir=pset(None)
    reference=pset(None)
    device=pset("cuda:all")
    nameSort=pset(False)
    doradocmd=pset("dorado")
    chunksize=pset(2000)
    filterlist=pset(None)
    fastqfilename=pset("output.fastq")
    usecpu=pset(False)
    def __init__(self):
        super().__init__(self.docker_image_name, self.docker_image_tag)
        with open(getJsonName(__file__,"fast_dorado")) as f:
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
    def handleInputstrigger(self, value, *args):
        if args and len(args) > 0: 
            self.handleInputs("trigger", value, args[0][0], test=args[0][3])
        else:
            self.handleInputs("inputFile", value, None, False)
    def handleInputstrigger2(self, value, *args):
        if args and len(args) > 0: 
            self.handleInputs("trigger2", value, args[0][0], test=args[0][3])
        else:
            self.handleInputs("inputFile", value, None, False)
    def handleInputsreference(self, value, *args):
        if args and len(args) > 0: 
            self.handleInputs("reference", value, args[0][0], test=args[0][3])
        else:
            self.handleInputs("inputFile", value, None, False)
    def handleInputsoutputDir(self, value, *args):
        if args and len(args) > 0: 
            self.handleInputs("outputDir", value, args[0][0], test=args[0][3])
        else:
            self.handleInputs("inputFile", value, None, False)
    def handleInputsmodelFile(self, value, *args):
        if args and len(args) > 0: 
            self.handleInputs("modelFile", value, args[0][0], test=args[0][3])
        else:
            self.handleInputs("inputFile", value, None, False)
    def handleOutputs(self):
        outputValue=None
        if hasattr(self,"outputDir"):
            outputValue=getattr(self,"outputDir")
        self.send("outputDir", outputValue)
        outputValue=None
        if hasattr(self,"fastqfilename"):
            outputValue=getattr(self,"fastqfilename")
        self.send("fastqfilename", outputValue)
        outputValue=None
        if hasattr(self,"inputDir"):
            outputValue=getattr(self,"inputDir")
        self.send("inputDir", outputValue)
