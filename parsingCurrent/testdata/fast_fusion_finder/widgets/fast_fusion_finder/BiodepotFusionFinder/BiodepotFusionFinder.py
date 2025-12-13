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

class OWBiodepotFusionFinder(OWBwBWidget):
    name = "BiodepotFusionFinder"
    description = "Minimum perl container"
    priority = 20
    icon = getIconName(__file__,"bff.png")
    want_main_area = False
    docker_image_name = "biodepot/fusionfinder"
    docker_image_tag = "latest"
    inputs = [("inputFile",str,"handleInputsinputFile"),("Trigger",str,"handleInputsTrigger"),("breakpointfile",str,"handleInputsbreakpointfile"),("guidefile",str,"handleInputsguidefile"),("outputdir",str,"handleInputsoutputdir")]
    outputs = [("outputdir",str)]
    pset=functools.partial(settings.Setting,schema_only=True)
    runMode=pset(0)
    exportGraphics=pset(False)
    runTriggers=pset([])
    triggerReady=pset({})
    inputConnectionsStore=pset({})
    optionsChecked=pset({})
    breakpointfile=pset(None)
    outputdir=pset("/data/breakpoint_files")
    breakpoints=pset([])
    bamfile=pset(False)
    minalignqual=pset(50)
    maxoverlap=pset(20)
    maxgap=pset(80)
    filterconsensus=pset(100)
    guidefile=pset(None)
    guiderange=pset(50)
    enrichment=pset(True)
    isunsorted=pset(False)
    support=pset(3)
    outputreadslist=pset(False)
    outputpoorreads=pset(False)
    filterends=pset(None)
    keepfirst=pset(False)
    keeplast=pset(False)
    inputfile=pset(None)
    first=pset(None)
    last=pset(None)
    def __init__(self):
        super().__init__(self.docker_image_name, self.docker_image_tag)
        with open(getJsonName(__file__,"BiodepotFusionFinder")) as f:
            self.data=jsonpickle.decode(f.read())
            f.close()
        self.initVolumes()
        self.inputConnections = ConnectionDict(self.inputConnectionsStore)
        self.drawGUI()
    def handleInputsinputFile(self, value, *args):
        if args and len(args) > 0: 
            self.handleInputs("inputFile", value, args[0][0], test=args[0][3])
        else:
            self.handleInputs("inputFile", value, None, False)
    def handleInputsTrigger(self, value, *args):
        if args and len(args) > 0: 
            self.handleInputs("Trigger", value, args[0][0], test=args[0][3])
        else:
            self.handleInputs("inputFile", value, None, False)
    def handleInputsbreakpointfile(self, value, *args):
        if args and len(args) > 0: 
            self.handleInputs("breakpointfile", value, args[0][0], test=args[0][3])
        else:
            self.handleInputs("inputFile", value, None, False)
    def handleInputsguidefile(self, value, *args):
        if args and len(args) > 0: 
            self.handleInputs("guidefile", value, args[0][0], test=args[0][3])
        else:
            self.handleInputs("inputFile", value, None, False)
    def handleInputsoutputdir(self, value, *args):
        if args and len(args) > 0: 
            self.handleInputs("outputdir", value, args[0][0], test=args[0][3])
        else:
            self.handleInputs("inputFile", value, None, False)
    def handleOutputs(self):
        outputValue=None
        if hasattr(self,"outputdir"):
            outputValue=getattr(self,"outputdir")
        self.send("outputdir", outputValue)
