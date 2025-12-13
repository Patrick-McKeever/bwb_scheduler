#!/bin/bash
echo "Cleaning up..."
[ -z "$basecallext" ] && basecallext="fast5"
[ -z "$fastqext" ] && fastqext="fastq"

if [ -n "$inputDir" ]; then 
  echo "moving $inputDir/*.$basecallext"
  if compgen -G "$inputDir/*.$basecallext" > /dev/null; then
     mkdir -p "$inputDir.done/" && eval "mv -f $inputDir/*.$basecallext $inputDir.done/."
  else
    echo "no matches"
  fi
fi
if [ -n "$outputDir" ]; then 
  echo "moving $outputDir/*.$fastqext"
  if compgen -G "$outputDir/*.$fastqext" > /dev/null; then
     mkdir -p "$outputDir.done/" && eval "mv -f $outputDir/*.$fastqext $outputDir.done/."
  else
    echo "no matches"
  fi
fi
[ -n "$modelDir" ] && echo "removing $modelDir/*" && rm -rf "$modelDir/*"
[ -n "$referenceDir" ] && echo "removing $referenceDir/*" && rm -rf "$referencedir/*"
exit 0;