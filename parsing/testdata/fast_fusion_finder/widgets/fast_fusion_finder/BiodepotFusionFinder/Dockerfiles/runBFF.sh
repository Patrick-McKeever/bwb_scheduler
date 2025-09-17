#!/bin/bash
shopt -s extglob
inputArgs=("$@")
printenv
if [ -n "$filterends" ]; then
  echo " filterEnds.pl  ${inputArgs[@]} > $filterends"
  eval " filterEnds.pl  ${inputArgs[@]} > $filterends"
else
  echo "/data/debug/filterSam.pl ${inputArgs[@]}"
  eval "/data/debug/filterSam.pl ${inputArgs[@]}"
fi
