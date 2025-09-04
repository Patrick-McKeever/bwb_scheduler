#!/bin/bash

#This wrapper around fastp allows for a list of multiple files to be executed

#make sure everything gets killed upon exit
trap "exit" INT TERM
trap "kill 0" EXIT

removequotes(){
    local quoted=$1
    quoted="${quoted%\"}"
    quoted="${quoted#\"}"
    echo "$quoted"
}

addMiddleExtension() {
    # The input is the full path to a file
    local filepath="$1"
    local label="$2"

    #must remove quotes for this to work 
    filepath=$(removequotes $filepath)
    label=$(removequotes $label) 

    # Extract only the filename
    local filename=$(basename "$filepath")

    # List of compression extensions
    local comp_exts="(tar|gz|zip|tgz|bzip)"

    # Keep on stripping the compression extensions
    while [[ $filename =~ \.$comp_exts$ ]]; do
        filename="${filename%.*}"
    done

    # If the file had a non-compression extension, save it
    local final_ext=""
    if [[ $filename =~ \..+$ ]]; then
        final_ext=".${filename##*.}"
        filename="${filename%.*}"
    fi

    # Append "$label" to the filename
    filename="${filename}_${label}"

    # Append the non-compression extension back if it was found
    if [ ! -z "$final_ext" ]; then
        filename="${filename}${final_ext}"
    fi
    #add quotes back
    echo "$filename"
}
addEndExtension() {
    # The input is the full path to a file
    local filepath="$1"
    local extension="$2"
    # Extract only the filename
    
    filepath=$(removequotes $filepath)
    extension=$(removequotes $extension)
    
    local filename=$(basename "$filepath")
    # List of compression extensions
    local comp_exts="(tar|gz|zip|tgz|bzip)"

    # Keep on stripping the compression extensions
    while [[ $filename =~ \.$comp_exts$ ]]; do
        filename="${filename%.*}"
    done
    # If the file had a non-compression extension, save it
    local final_ext=""
    if [[ $filename =~ \..+$ ]]; then
        final_ext=".${filename##*.}"
        filename="${filename%.*}"
    fi

    # Append $extension to the filename
    filename="${filename}.${extension}"
    echo "$filename"
}
fastpSE() {
  echo "running fastp on $1"
  local inputFile outputfastq htmlname jsonname
  inputFile=$(removequotes $1)

  outputfastq="$outputDir/$(addMiddleExtension "$inputFile" "$trimsuffix")"
  htmlname="$reportDir/$(addEndExtension "$inputFile" "html")"
  jsonname="$reportDir/$(addEndExtension "$inputFile" "json")"
  echo "fastp -i $inputFile -o $outputfastq -h $htmlname -j $jsonname"
  fastp -i $inputFile -o $outputfastq -h $htmlname -j $jsonname
}

fastpPE() {
  echo "running fastp on paired ends $1 $2"
  local inputFile1 inputFile2 outputfastq1 outputfastq2 htmlname jsonname
  inputFile1=$(removequotes $1)
  inputFile2=$(removequotes $2)
  outputfastq1="$outputDir/$(addMiddleExtension "$inputFile1" "$trimsuffix")"
  outputfastq2="$outputDir/$(addMiddleExtension "$inputFile2" "$trimsuffix")"
  htmlname="$reportDir/$(addEndExtension "$inputFile1" "html")"
  jsonname="$reportDir/$(addEndExtension "$inputFile2" "json")"
  echo "fastp -i $inputFile1 -I $inputFile1 -o $outputfastq1 -O $outputfastq2 -h $htmlname -j $jsonname "
  fastp -i $inputFile1 -I $inputFile1 -o $outputfastq1 -O $outputfastq2 -h $htmlname -j $jsonname
}


#check if there is one file
echo "$files"
[ -z "$files" ] && echo "missing files" && exit 1

#remove brackets and convert to bash array
filestring="${files:1:${#files}-2}"

readarray -td, files <<<"$filestring,"; unset 'files[-1]'; declare -p files;
nFiles=${#files[@]}

echo "${files[@]}"
echo "number of files is ${nFiles}"

#defaults for outputDirectory trimsuffix

[ -z "$trimsuffix" ] && trimsuffix="trimmed"
[ -z "$reportDir" ] && reportDir=$(dirname ${files[0]})/reports
[ -z "$outputDir" ] && outputDir=$(dirname ${files[0]})/trimmed

#make the directories
mkdir -p "$outputDir"


if [ ${nFiles} -le 1 ]; then
   [ -n "${pairedEnds}" ] && echo "missing R2 file" && exit 1
   fastpSE ${files[0]}
elif [ -n "${pairedEnds}" ]; then
    echo "Working on paired ends"
    R1files=(${files[@]:0:$(( nFiles / 2 ))})
    R2files=(${files[@]:$(( nFiles / 2 ))})
    (( ${#R1files[@]} !=  ${#R2files[@]} )) && echo "uneven number of PE files" && exit 1
    for i in "${!R1files[@]}"; do
        file1=${R1files[i]} 
        file2=${R2files[i]}
        fastpPE $file1 $file2
    done
else
    for file in "${files[@]}"; do
        fastpSE $file
    done
fi

