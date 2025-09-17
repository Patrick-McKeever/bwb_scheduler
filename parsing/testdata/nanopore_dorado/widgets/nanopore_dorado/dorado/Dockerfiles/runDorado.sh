#!/bin/bash
shopt -s extglob
inputArgs=("$@")


#Parse command variable

[ -z "$command" ] && command="dorado basecaller "
#check the second field of the command variable
command2=$(echo $command | awk '{print $2}')
#sort files by default
if [ $command2 == "basecaller" ]; then
  #if outputFile is not set give some reasonable value to it
    [ -z "$outputDir" ] && outputDir="$inputDir"
    [ -z "$outputDir" ] && outputDir=$(dirname "$inputFile")
    [ -z "$outputFile" ] && [ -n "$emitFastq" ] &&  outputFile="$outputDir/calls.fastq"
    [ -z "$outputFile" ] && [ -n "$emitSam" ] &&  outputFile="$outputDir/calls.sam"
    [ -z "$outputFile" ] && outputFile="$outputDir/calls.bam"
  # if reference is set and sortindex is set then we use samtools sort
    if [ -n "$reference" ] && [ -n "$sortIndex" ]; then
	  echo "$command $modelString $modelFile $inputDir $inputFile ${inputArgs[@]} | samtools sort > $outputFile"
	  eval "$command $modelString $modelFile $inputDir $inputFile ${inputArgs[@]} | samtools sort > $outputFile"
	  samtools index "$outputFile"
    else
	    echo "$command $modelString $modelFile $inputDir $inputFile ${inputArgs[@]} > $outputFile"
	    eval "$command $modelString $modelFile $inputDir $inputFile ${inputArgs[@]} > $outputFile" 
    fi	
    if [ -n "$nameSort" ]; then
	    echo "Generating name sorted bam/sam"
        basename=$(basename "$outputFile")
        dirname=$(dirname "$outputFile")
        extension="${basename##*.}"
	    samtools sort -n "$outputFile" > "$dirname/$basename.nameSorted.$extension"
    fi
else
	if [ -n "$outputFile" ]; then
		echo "$command" "${inputArgs[@]}" > "$outputFile"
    eval "$command" "${inputArgs[@]}" > "$outputFile"
	else
		echo "$command" "${inputArgs[@]}"
    eval "$command" "${inputArgs[@]}"
	fi
fi