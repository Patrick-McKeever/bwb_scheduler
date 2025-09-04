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
    priority = 10
    icon = getIconName(__file__,"start.png")
    want_main_area = False
    docker_image_name = "brycenofu/star-salmon-dashboard-start"
    docker_image_tag = "latest"
    outputs = [("work_dir",str),("genome_dir",str),("genome_file",str),("annotation_file",str),("genomegtfURLs",str),("bypass_star_index",str),("starversion",str),("transcriptomefile",str),("raw_genome_file",str),("raw_annotation_file",str),("download_dir",str),("trimmedfastqfiles",str),("trimmeddir",str),("fastqfiles",str),("s3bucket",str),("s3directory",str),("s3downloaddir",str),("alignedfiles",str),("aligneddir",str),("trimmedfiles",str),("fastqdir",str),("tablesDir",str),("salmondir",str),("srridsAll",str),("jsonFile",str),("paired",str),("notebookOutput",str)]
    pset=functools.partial(settings.Setting,schema_only=True)
    runMode=pset(0)
    exportGraphics=pset(False)
    runTriggers=pset([])
    triggerReady=pset({})
    inputConnectionsStore=pset({})
    optionsChecked=pset({})
    work_dir=pset(None)
    genome_dir=pset(None)
    genome_file=pset(None)
    annotation_file=pset(None)
    bypass_star_index=pset(True)
    downloadindexlink=pset(False)
    transcriptomefile=pset(None)
    download_dir=pset(None)
    genomegtfURLs=pset([])
    starversion=pset("2.7.11a")
    raw_genome_file=pset(None)
    raw_annotation_file=pset(None)
    s3directory=pset("bucket-dir/")
    s3bucket=pset(None)
    s3downloaddir=pset(None)
    trimmedfastqfiles=pset([])
    trimmeddir=pset(None)
    alignedfiles=pset({})
    aligneddir=pset(None)
    trimmedfiles=pset({})
    fastqdir=pset(None)
    fastqfiles=pset({})
    tablesDir=pset(None)
    srridsAll=pset([])
    salmondir=pset(None)
    srridsNormal=pset([])
    srridsDisease=pset([])
    jsonFile=pset(None)
    paired=pset(False)
    notebookOutput=pset(None)
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
        if hasattr(self,"work_dir"):
            outputValue=getattr(self,"work_dir")
        self.send("work_dir", outputValue)
        outputValue=None
        if hasattr(self,"genome_dir"):
            outputValue=getattr(self,"genome_dir")
        self.send("genome_dir", outputValue)
        outputValue=None
        if hasattr(self,"genome_file"):
            outputValue=getattr(self,"genome_file")
        self.send("genome_file", outputValue)
        outputValue=None
        if hasattr(self,"annotation_file"):
            outputValue=getattr(self,"annotation_file")
        self.send("annotation_file", outputValue)
        outputValue=None
        if hasattr(self,"genomegtfURLs"):
            outputValue=getattr(self,"genomegtfURLs")
        self.send("genomegtfURLs", outputValue)
        outputValue=None
        if hasattr(self,"bypass_star_index"):
            outputValue=getattr(self,"bypass_star_index")
        self.send("bypass_star_index", outputValue)
        outputValue=None
        if hasattr(self,"starversion"):
            outputValue=getattr(self,"starversion")
        self.send("starversion", outputValue)
        outputValue=None
        if hasattr(self,"transcriptomefile"):
            outputValue=getattr(self,"transcriptomefile")
        self.send("transcriptomefile", outputValue)
        outputValue=None
        if hasattr(self,"raw_genome_file"):
            outputValue=getattr(self,"raw_genome_file")
        self.send("raw_genome_file", outputValue)
        outputValue=None
        if hasattr(self,"raw_annotation_file"):
            outputValue=getattr(self,"raw_annotation_file")
        self.send("raw_annotation_file", outputValue)
        outputValue=None
        if hasattr(self,"download_dir"):
            outputValue=getattr(self,"download_dir")
        self.send("download_dir", outputValue)
        outputValue=None
        if hasattr(self,"trimmedfastqfiles"):
            outputValue=getattr(self,"trimmedfastqfiles")
        self.send("trimmedfastqfiles", outputValue)
        outputValue=None
        if hasattr(self,"trimmeddir"):
            outputValue=getattr(self,"trimmeddir")
        self.send("trimmeddir", outputValue)
        outputValue=None
        if hasattr(self,"fastqfiles"):
            outputValue=getattr(self,"fastqfiles")
        self.send("fastqfiles", outputValue)
        outputValue=None
        if hasattr(self,"s3bucket"):
            outputValue=getattr(self,"s3bucket")
        self.send("s3bucket", outputValue)
        outputValue=None
        if hasattr(self,"s3directory"):
            outputValue=getattr(self,"s3directory")
        self.send("s3directory", outputValue)
        outputValue=None
        if hasattr(self,"s3downloaddir"):
            outputValue=getattr(self,"s3downloaddir")
        self.send("s3downloaddir", outputValue)
        outputValue=None
        if hasattr(self,"alignedfiles"):
            outputValue=getattr(self,"alignedfiles")
        self.send("alignedfiles", outputValue)
        outputValue=None
        if hasattr(self,"aligneddir"):
            outputValue=getattr(self,"aligneddir")
        self.send("aligneddir", outputValue)
        outputValue=None
        if hasattr(self,"trimmedfiles"):
            outputValue=getattr(self,"trimmedfiles")
        self.send("trimmedfiles", outputValue)
        outputValue=None
        if hasattr(self,"fastqdir"):
            outputValue=getattr(self,"fastqdir")
        self.send("fastqdir", outputValue)
        outputValue=None
        if hasattr(self,"tablesDir"):
            outputValue=getattr(self,"tablesDir")
        self.send("tablesDir", outputValue)
        outputValue=None
        if hasattr(self,"salmondir"):
            outputValue=getattr(self,"salmondir")
        self.send("salmondir", outputValue)
        outputValue=None
        if hasattr(self,"srridsAll"):
            outputValue=getattr(self,"srridsAll")
        self.send("srridsAll", outputValue)
        outputValue=None
        if hasattr(self,"jsonFile"):
            outputValue=getattr(self,"jsonFile")
        self.send("jsonFile", outputValue)
        outputValue=None
        if hasattr(self,"paired"):
            outputValue=getattr(self,"paired")
        self.send("paired", outputValue)
        outputValue=None
        if hasattr(self,"notebookOutput"):
            outputValue=getattr(self,"notebookOutput")
        self.send("notebookOutput", outputValue)
