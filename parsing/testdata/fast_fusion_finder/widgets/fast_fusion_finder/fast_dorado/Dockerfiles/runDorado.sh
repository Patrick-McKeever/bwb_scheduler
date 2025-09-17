#!/bin/bash

shopt -s extglob

#Get the filter file if it exists
#!/bin/bash

# Check if the $filterlist variable is set, is a file and is empty - then just write an empty fastq file
printenv
if [[ -n "$filterlist" && -f "$filterlist" && ! -s "$filterlist" ]]; then
    # Check if $fastqfilename variable is set
    echo "filterlist is empty"
    if [[ -n "$fastqfilename" ]]; then
        echo "writing empty file to $outputDir/$fastqfilename"
        # Write/replace with an empty file
        : > "$outputDir/$fastqfilename"
        exit 0
    else
        echo "Error: fastqfilename environment variable is not set."
        exit 1
    fi
fi
inputArgs=("$@")

echo "$doradocmd $modeldir $inputDir ${inputArgs[@]} > $outputDir/$fastqfilename"
eval "$doradocmd $modeldir $inputDir ${inputArgs[@]}" > "$outputDir/$fastqfilename"
