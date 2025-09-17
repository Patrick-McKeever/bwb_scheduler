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

class OWStart(OWBwBWidget):
    name = "Start"
    description = "Enter workflow parameters and start"
    priority = 1
    icon = getIconName(__file__,"start.png")
    want_main_area = False
    docker_image_name = "biodepot/nanopore-start"
    docker_image_tag = "alpine_3.12"
    outputs = [("data_dir",str),("genome_dir",str),("genome_file",str),("bam_dir",str),("bam_file",str),("fast5_dir",str),("model_dir",str)]
    pset=functools.partial(settings.Setting,schema_only=True)
    runMode=pset(0)
    exportGraphics=pset(False)
    runTriggers=pset([])
    triggerReady=pset({})
    inputConnectionsStore=pset({})
    optionsChecked=pset({})
    data_dir=pset(None)
    fast5_dir=pset(None)
    genome_dir=pset(None)
    genome_file=pset(None)
    bam_dir=pset(None)
    bam_file=pset(None)
    model_dir=pset(None)
    def __init__(self):
        super().__init__(self.docker_image_name, self.docker_image_tag)
        with open(getJsonName(__file__,"Start")) as f:
            self.data=jsonpickle.decode(f.read())
            f.close()
        self.initVolumes()
        self.inputConnections = ConnectionDict(self.inputConnectionsStore)
        self.drawGUI()
    def handleOutputs(self):
        outputValue=None
        if hasattr(self,"data_dir"):
            outputValue=getattr(self,"data_dir")
        self.send("data_dir", outputValue)
        outputValue=None
        if hasattr(self,"genome_dir"):
            outputValue=getattr(self,"genome_dir")
        self.send("genome_dir", outputValue)
        outputValue=None
        if hasattr(self,"genome_file"):
            outputValue=getattr(self,"genome_file")
        self.send("genome_file", outputValue)
        outputValue=None
        if hasattr(self,"bam_dir"):
            outputValue=getattr(self,"bam_dir")
        self.send("bam_dir", outputValue)
        outputValue=None
        if hasattr(self,"bam_file"):
            outputValue=getattr(self,"bam_file")
        self.send("bam_file", outputValue)
        outputValue=None
        if hasattr(self,"fast5_dir"):
            outputValue=getattr(self,"fast5_dir")
        self.send("fast5_dir", outputValue)
        outputValue=None
        if hasattr(self,"model_dir"):
            outputValue=getattr(self,"model_dir")
        self.send("model_dir", outputValue)
