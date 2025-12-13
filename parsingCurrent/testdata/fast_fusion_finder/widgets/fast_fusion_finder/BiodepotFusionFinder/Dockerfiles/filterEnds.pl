#!/usr/bin/perl

#Based on BFF script filterSam.pl adapted to be used to filter on the two ends of the read
#

#command to use

#./filterSam.pl --first <sam/bam.first> --last <sam/bam.last> -i <sam/bam.all control> -b breakpoints.txt -G guides.txt

#breakpoints are stored as two positions - one breakpoint per line
#each position is in the form of chr:<start>:<end> and the two positions are separated by a semi colon 
#you can comment out a line by using # 
#eg
#  chr15:74318559:74336132;chr17:38428464:38512385
#  chr15:74318559:;chr17:38428464:38512385
#  chr15::74336132;chr17::
#  chr15::74336132;
# You can skip start, end and chr values if you want - the script will find all reads that match any existing criteria
use warnings;
use List::Util qw< sum >;
use Getopt::Long;

#global variables
$gminAlignQual  =  20;  #minimum alignment quality 60 is max for minimap
$gpoorAlignQual  = 20;
$gmaxOverlapRegion = 20; #maximum length region where the two parts of fusion can overlap
$gminUniqueAlignLength = 20; #minimum length for each part of the fusion to align
$gmaxGap = 30; #maximum unaligned gap between two parts of fusion
$gfilterConsensusLength=50; #For consensus count only count the breakpoints that are within this number of basepairs of the most common breakpoint (-1 means count all)
$gverbose=""; #verbose
$gkeepFirstUnmapped="";
$gkeepLastUnmapped="";


undef %gpos1;
undef %gpos2;
#these will store hashes to names that match
undef %gbreakpoints;
undef %greads;
undef %gunmapped;


my $breakpointFile;
my $inputSamFirstFile;
my $inputSamLastFile;
my %breakpoints;
$gstandardChrs={"chrX" => 23, "chrY" => 24 };

foreach my $i (1..22){
        $gstandardChrs{"chr$i"}=$i;
}


GetOptions ("minAlignQual|q=i" => \$gminAlignQual,
			"poorAlignQual|q=i" => \$gpoorAlignQual,
            "maxOverlapRegion|o=i"   => \$gmaxOverlapRegion,
            "minUniqueAlignLength|l=i"   => \$gminUniqueAlignLength,
            "maxGap|m=i"  => \$gmaxGap,
            "filterConsensusLength|c=i"   => \$gfilterConsensusLength,
			"first=s"   => \$inputSamFirstFile,
			"last=s"   => \$inputSamLastFile,
			"keepFirst" => \$gkeepFirstUnmapped,
			"keepLast" => \$gkeepLastUnmapped,
            "breakpointFile|b=s"   => \$breakpointFile,    
            "verbose"  => \$gverbose)  
or die("Error in command line arguments\n");




#for now require breakpoint file
#otherwise we check the best map on both sides for different chromosomes
%gbreakpoints=readPositionFile($breakpointFile);

if ($inputSamLastFile){
	readSamBamFile($inputSamFirstFile,'First',0);
	readSamBamFile($inputSamLastFile,'Last',1);
}
else{

    filterSamBamFile($inputSamFirstFile);
}
#print out filtered list
foreach my $name (keys %greads){
	print "$name\n";
}
sub findSamBamFromExtension{
	my($file)=@_;
	my $extension=substr($file,-4);
	if ($extension eq ".bam"){return 'bam'}
	if ($extension eq ".sam"){return 'sam'}
	return ""
}
sub readSamBamFile{
    my ( $file, $endtype, $checkbp ) = @_;
    my $samBam = findSamBamFromExtension($file);
    $samBam || die "$file does not end in .sam or .bam\n";
    if ( $samBam eq 'bam' ) {
		$gverbose && print STDERR "opening bam file $file\n";
        open( $fp, "samtools view -h $file |" )
          || die "can't open $file \n";
    }
    else {
		$gverbose && print STDERR "opening sam file $file\n";
        open( $fp, "$file" ) || die "can't open $file \n";
    }
    #read in sam/bam file

    while ( my $line = <$fp> ) {
        unless ( substr( $line, 0, 1 ) eq "@" ) {
			if ($checkbp){compareSAMLine($line);}
			else{parseSAMLine($line);}
	
        }
    }
    close($fp);
}




#breakpoints are stored in two arrays pos1 with elements chr,leftCoord,rightCoord repeated and undefs for missing values
#To access multiply index by 3
sub readPositionFile{
	my ($file)=@_;
	my %bps;
	@{$bps{pos1}}=();
	@{$bps{pos2}}=();
	open(my $fp,'<',$file) || die "can't open $file";
	while (defined(my $line=<$fp>)){
		unless(substr($line,0,1) eq '#'){
			parsePositionLine($line,\%bps);
		}
	}
	return %bps;
}
sub parsePositionLine{
	my ($line)=@_;
	chomp($line);
	my ($brk1Str,$brk2Str)=split(/\;/,$line);
	my(@pos1)=split(/\:/,$brk1Str);
	my(@pos2)=split(/\:/,$brk2Str);
	foreach my $i (0..2){
	  if (defined($pos1[$i])){push(@{$gbreakpoints->{pos1}},$pos1[$i])}
	  else {push(@{$gbreakpoints->{pos1}},undef)}
	  if (defined($pos2[$i])){push(@{$gbreakpoints->{pos2}},$pos2[$i])}
	  else {push(@{$gbreakpoints->{pos2}},undef)}
	}
}


sub findSAZTag{
	my (@tags)=@_;
	foreach my $tag (@tags){
	  if(substr($tag,0,5) eq "SA:Z:"){
		return substr($tag,5);  
	  }	
	}
	return "";
}
sub parseSAMLine{
  #for the rapid filter check the main assignment and tags for pos1 and pos2
  my ($line)=@_;
  chomp($line);
  my (@parts)=split(/\t/,$line);
  my ($qname,$flag,$chrName,$pos,$mapq,$cigar,$rnext,$pnext,$tlen,$seq,$qual,@tags)=@parts;
  if ($#parts < 4){
	die "bad line:\n$line\n";  
  }
  my $poorAlignment;
  if ($flag == 4  || $mapq < $gpoorAlignQual){
	$poorAlignment++;
  }
  if ($poorAlignment && $gkeepFirstUnmapped){$gunmapped{$qname}++}	
  foreach my $i (0..$#{$gbreakpoints->{pos1}}/3){
	unless ( exists($gpos1{$i}) && $gpos1{$i}{$qname} && exists($gpos2{$i}) && $gpos2{$i}{$qname}){
		my (@pos1)=@{$gbreakpoints->{pos1}}[$i*3..$i*3+2];
		my (@pos2)=@{$gbreakpoints->{pos2}}[$i*3..$i*3+2];
		#check for posMatch in position 1
		my $printFlag;
		if (posMatch(\@pos1,$chrName,$pos,$mapq)){$gpos1{$i}{$qname}++;$printFlag++}
		if (posMatch(\@pos2,$chrName,$pos,$mapq)){$gpos2{$i}{$qname}++;$printFlag++}
			#if ($printFlag){print STDERR "$line\n"}

	}
  }	

  return ();
}
sub compareSAMLine{
  #use after found set of potential matches
  my ($line)=@_;
  chomp($line);
  
  my (@parts)=split(/\t/,$line);
  my ($qname,$flag,$chrName,$pos,$mapq,$cigar,$rnext,$pnext,$tlen,$seq,$qual,@tags)=@parts;

  if ($#parts < 4){
	die "bad line:\n$line\n";  
  }
  my $poorAlignment;
  if ($flag == 4  || $mapq < $gpoorAlignQual){
	$poorAlignment++;
  }
  my $keepPoorAlignment;
  if ($poorAlignment && $gkeepLastUnmapped ){
	 $keepPoorAlignment++; 
  }
  my $keepRead;
  foreach my $i (0..$#{$gbreakpoints->{pos1}}/3){
	my (@pos1)=@{$gbreakpoints->{pos1}}[$i*3..$i*3+2];
	my (@pos2)=@{$gbreakpoints->{pos2}}[$i*3..$i*3+2];
		#check for first match and last match or last unmapped
	if( exists($gpos1{$i}) && $gpos1{$i}{$qname}){
	  if ($keepPoorAlignment || posMatch(\@pos2,$chrName,$pos,$mapq)){$gpos2{$i}{$qname}++;$keepRead++;last}
	}
	if( exists($gpos2{$i}) && $gpos2{$i}{$qname}){
	  if ($keepPoorAlignment || posMatch(\@pos1,$chrName,$pos,$mapq)){$gpos1{$i}{$qname}++;$keepRead++;last}
	}
		#check for first unmapped and keep if there it maps from right 
	if(!exists($gunmapped{$qname})){
		if (posMatch(\@pos1,$chrName,$pos,$mapq) || posMatch(\@pos2,$chrName,$pos,$mapq)){
			$keepRead++; last;
		}
	}	
  }
	if($keepRead){
		#get rid of size field
		my ($readName)=split(/\_/,$qname);
		$greads{$readName}++
	}
  
  return ();
}
sub filterSamBamFile{
    my ( $file) = @_;
    my $samBam = findSamBamFromExtension($file);
    $samBam || die "$file does not end in .sam or .bam\n";
    if ( $samBam eq 'bam' ) {
		$gverbose && print STDERR "opening bam file $file\n";
        open( $fp, "samtools view -h $file |" )
          || die "can't open $file \n";
    }
    else {
		$gverbose && print STDERR "opening sam file $file\n";
        open( $fp, "$file" ) || die "can't open $file \n";
    }
    #read in sam/bam file
    while ( my $line = <$fp> ) {
        unless ( substr( $line, 0, 1 ) eq "@" ) {
			filterSAMLine($line);
        }
    }
    close($fp);	
}
sub filterSAMLine{
  #use after found set of potential matches
  my ($line)=@_;
  chomp($line);
  my (@parts)=split(/\t/,$line);
  my ($qname,$flag,$chrName,$pos,$mapq,$cigar,$rnext,$pnext,$tlen,$seq,$qual,@tags)=@parts;
  if ($#parts < 4){
	die "bad line:\n$line\n";  
  }
  my $poorAlignment;
  if ($flag == 4  || $mapq < $gpoorAlignQual){
	$poorAlignment++;
  }

  my $keepRead;
  if ($poorAlignment && $gkeepFirstUnmapped){
	 $keepRead++
  }
  elsif($mapq > $gminAlignQual){
	foreach my $i (0..$#{$gbreakpoints->{pos1}}/3){
		my (@pos1)=@{$gbreakpoints->{pos1}}[$i*3..$i*3+2];
		my (@pos2)=@{$gbreakpoints->{pos2}}[$i*3..$i*3+2];
			#check for first match and last match or last unmapped
		if(posMatch(\@pos1,$chrName,$pos)){$keepRead++;last}
		if(posMatch(\@pos2,$chrName,$pos)){$keepRead++;last}
	}
  }
  if($keepRead){

		#get rid of size field
		my ($readName)=split(/\_/,$qname);
		$greads{$readName}++
  }
  
  return ();
}
sub findBreakPoints{
	my ($mainPos,$flag,$qname,$pos,$cigar,$alts)=@_;
	my $mainReversed=isReversed($flag);
	my $mainOrient='+';
	if ($mainReversed){$mainOrient='-'}
	my $returnString="";
    my ($lmain,$rmain,$mainRefLength)=findMatchingIntervals($cigar,$mainReversed);
	foreach my $alt (@{$alts}){
		my ($altChr,$altPos,$altOrient,$altCigar,$altConf,$altMisMatch)=split(/\,/,$alt);  
		if ($altConf < $gminAlignQual){
			next;
		}
		my $altReversed=0;
		my ($breakpoint,$fusion);
		if ($altOrient eq "-"){$altReversed=1}
        my ($lalt,$ralt,$altRefLength)=findMatchingIntervals($altCigar,$altReversed);
        my $score;
        my $goodOverlap=checkOverlap($lmain,$rmain,$lalt,$ralt,\$score);
        if ($goodOverlap){
			my @mainRefPos=($pos,$pos+$mainRefLength-1);
			my @altRefPos=($altPos,$altPos+$altRefLength-1);
			if ($mainReversed){
			   @mainRefPos[0,1]=@mainRefPos[1,0];
			}
			if ($altReversed){
			   @altRefPos[0,1]=@altRefPos[1,0];
			}
			if ($goodOverlap < 0){
				#alt goes before main
				if ($mainPos == 0){
					#the main match must be written first so reverse the fusion orientation
				   	$breakpoint="$qname:$mainRefPos[0];$altChr:$altRefPos[1]";
					$fusion="$qname:$mainRefPos[1]:$mainRefPos[0];$altChr:$altRefPos[1]:$altRefPos[0]";
				}
				else{
				   	$breakpoint="$altChr:$altRefPos[1];$qname:$mainRefPos[0]";
					$fusion="$altChr:$altRefPos[0]:$altRefPos[1];$qname:$mainRefPos[0]:$mainRefPos[1]";	
				}
			}
			else{
				#main goes before alt
				if ($mainPos == 0){
				   	$breakpoint="$qname:$mainRefPos[1];$altChr:$altRefPos[0]";
					$fusion="$qname:$mainRefPos[0]:$mainRefPos[1];$altChr:$altRefPos[0]:$altRefPos[1]";
				}
				else{
					#the alt match must be written first so reverse the fusion orientation
				   	$breakpoint="$altChr:$altRefPos[0];$qname:$mainRefPos[1]";
					$fusion="$altChr:$altRefPos[1]:$altRefPos[0];$qname:$mainRefPos[1]:$mainRefPos[0]";	
				}				
			}
			$returnString="$returnString$fusion\t$breakpoint\t$score\n";
		}	
    }
	return $returnString;
}
sub checkOverlap{
	my($lmain,$rmain,$lalt,$ralt,$score)=@_;
	#minimum alignment
	if ($ralt - $lalt < $gminUniqueAlignLength) {return 0}
	#fusion happens before main sequence
	if ($lalt < $lmain && $lmain - $lalt >  $gminUniqueAlignLength && $ralt < $rmain && $ralt-$lmain < $gmaxOverlapRegion && $lmain-$ralt < $gmaxGap){
		$$score=abs($lmain-$ralt-1);
		return -1;
	}
    #fusion happens after main sequence
	if ($ralt > $rmain && $ralt - $rmain >  $gminUniqueAlignLength && $lalt > $lmain && $rmain-$lalt < $gmaxOverlapRegion && $lalt-$rmain < $gmaxGap){
		$$score=abs($lalt-$rmain-1);
		return 1;
	}
	return 0;
}
sub posMatch{
	my($query,$qname,$pos,$mapq)=@_;
	if(defined($mapq) && $mapq < $gminAlignQual){ return 0}
	if ($query->[0] && $query->[0] ne $qname){return 0;}
	#find which position is lowRefCoord and which is highRefCoord - first number is not always lowest value
	my $lowRefCoord=$query->[1];
	my $highRefCoord=$query->[2];
	if ($query->[1] && $query->[2] && $query->[1] > $query->[2]){
		#swap values
		$lowRefCoord=$query->[2];
		$highRefCoord=$query->[1];
	}
    if ($lowRefCoord && $lowRefCoord > $pos){return 0;}
    if ($highRefCoord && $highRefCoord < $pos){return 0;}
    return 1;
}

sub findOtherAlt{
	my ($altTag,$chr,$matches1,$matches2)=@_;
	my $nmatches=0;
	my $chrNum=$gstandardChrs{$chr};
	my (@alts)=split(/\;/,$altTag);
	foreach my $alt (@alts){
		my ($altChr)=split(/\,/,$alt);
 #only check for non mitochondrial and non
		if (exists($gstandardChrs{$altChr})){
			
			if ($gstandardChrs{$altChr} > $chrNum){
				push(@{$matches1},$alt);
				$nmatches++;
			}
			elsif ($gstandardChrs{$altChr} < $chrNum){
				push(@{$matches2},$alt);
				$nmatches++;
			}
		}
	}
 return $nmatches;
}

sub findAlt{
	my ($altTag,$altPos)=@_;
	my @matches;
	my (@alts)=split(/\;/,$altTag);
	foreach my $alt (@alts){
		my @parts=split(/\,/,$alt);
		if (posMatch($altPos,@parts)){
			push(@matches,$alt);
		}
	}
	return (@matches);
}
sub checkMatches{
  my($orient)=@_;
   my ($altChr,$altPos,$altOrient,$altCigar,$altConf,$altMisMatch);
   my ($refLength,$seqLength)=findLengths($altCigar);
   my ($lmismatch,$rmismatch)=findCigarMismatches($altCigar);
   my ($lseq,$rseq);
   if ($altOrient eq '+'){
	  $lseq=$lmismatch;
	  $rseq=$lmismatch+$seqLength-$rmismatch;
   }
   else{
	  $lseq=$rmismatch;
	  $rseq=$rmismatch+$seqLength-$lmismatch		      
   }
   return 1;	
	
}
sub findMatchingIntervals{
  my ($cigar,$reversed)=@_;
  my ($refLength,$seqLength)=findLengths($cigar);
  my ($lmismatch,$rmismatch)=findCigarMismatches($cigar);
  if ($reversed){($lmismatch,$rmismatch)=($rmismatch,$lmismatch)}	
  return ($lmismatch+1,$seqLength-$rmismatch,$refLength);
}
sub findLengths{
 my ($cigar)=@_;
 my @outputRef=($cigar =~/(\d*)[MDN=X]/ga);
 my @outputSeq=($cigar =~/(\d*)[MI=XSH]/ga);
 return(sum(@outputRef),sum(@outputSeq));
}
sub findRightRefPosition{
  my ($pos,$cigar)=@_;	
  my @outputRef=($cigar =~/(\d*)[MDN=X]/ga);
  return(sum(@outputRef,$pos));
}
sub isReversed{
  my ($flag)=@_;
  if ($flag & 16) {return 1;}
  return 0;
}
sub findCigarMismatches{
   my ($cigar)=@_;
   my (@fields)=($cigar =~/(\d*[MIDNSHPX=])/ga);
   my (@rfields)=reverse(@fields);
   my ($lmismatch,$rmismatch)=(0,0);
   foreach my $field (@fields){
	if(substr($field,-1) !~ /[HS]/){
		last;
	}
	$lmismatch+=int(substr($field,0,-1));
   }
   foreach my $field (@rfields){
	if(substr($field,-1) !~ /[HS]/){
		last;
	}
	$rmismatch+=int(substr($field,0,-1));
   }
   return($lmismatch,$rmismatch);
 }


