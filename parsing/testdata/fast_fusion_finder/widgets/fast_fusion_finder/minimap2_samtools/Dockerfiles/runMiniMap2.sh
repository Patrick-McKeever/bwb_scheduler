#!/bin/bash
makeArrayString() {
	echo $1 | sed 's/[][]//g' | sed 's/\,/ /g'
}
unquotedFile() {
	echo $* | sed 's/\"//g'
}
shopt -s extglob
inputArgs=("$@")
set -ef -o pipefail
mkdir -p $outputdir
files=($(makeArrayString $inputfiles))
for file in "${files[@]}"; do
	echo "working on $file"
	unquoted=$(unquotedFile $file)
    filename=$(basename -- "$unquoted")
    bambase="$outputdir/${filename%%.*}"
	#check for case of empty file
	if [ ! -s "$unquoted" ]; then
		echo "file $unquoted is empty - skipping"
		continue
	fi
	if [ -n "$saveassam" ]; then
		outputfile="$bambase.sam"
	    echo "minimap2 -ax map-ont $optionalflags ${inputArgs[@]} $indexfile $unquoted > $outputfile" 
		eval "minimap2 -ax map-ont $optionalflags ${inputArgs[@]} $indexfile $unquoted > $outputfile" || exit $?
		if [ -n "$mergesam" ]; then
			if [ -f "$outputdir/$mergesam" ]; then
				grep -v '^[#@]' "$outputfile" >> "$outputdir/$mergesam"
			elif [ -f "$outputfile" -a "$outputfile" != "$outputdir/$mergesam" ]; then
				cp "$outputfile" "$outputdir/$mergesam"
			elif [ ! -f "$outputfile" -a ! -f "$outputdir/$mergesam" ]; then
				touch "$outputdir/$mergesam"
			fi
		fi
	else
		echo "minimap2 -ax map-ont $optionalflags ${inputArgs[@]} $indexfile $unquoted | samtools view -bS | samtools sort -O BAM | tee $bambase.bam | samtools index - $bambase.bai"  	
	    minimap2 -ax map-ont $optionalflags ${inputArgs[@]} $indexfile $unquoted | samtools view -bS | samtools sort -O BAM | tee "$bambase.bam" | samtools index - "$bambase.bai"

	fi
done
#check if the output file exists - otherwise write an empty file
[ -n "$saveassam" ] && [ -n "$mergesam" ] && [ ! -f "$outputdir/$mergesam" ] && echo "writing empty file to $outputdir/$mergesam" && touch "$outputdir/$mergesam"
exit 0;