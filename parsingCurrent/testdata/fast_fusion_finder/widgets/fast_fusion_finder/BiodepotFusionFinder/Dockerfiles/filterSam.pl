#!/usr/bin/perl

#command to use

#cat test.sam | ./filterSam.pl -b breakpoints.txt -G guides.txt

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
$gminAlignQual  =  50;  #minimum alignment quality 60 is max for minimap
$gmaxOverlapRegion = 20; #maximum length region where the two parts of fusion can overlap
$gminUniqueAlignLength = 20; #minimum length for each part of the fusion to align
$gmaxGap = 30; #maximum unaligned gap between two parts of fusion
$gfilterConsensusLength=50; #For consensus count only count the breakpoints that are within this number of basepairs of the most common breakpoint (-1 means count all)
$gverbose=""; #verbose
undef %gguideLCoords;
undef %gguideRCoords;
undef %genrichSeen;
undef %gonTarget;
undef %gonNearbyTarget;
undef %gonChr;

$gMinInversionSize=1000;
$gtotalEnrichReads=0;
$gguideRange=50;
$gcoverage=0;

my $breakpointFile;
my $inputSamFile;
my %breakpoints;
my $guideFile;
my $outputDir=breakpoint_files;
$ggetEnrichment=0;
$gbam=0;
$gisUnsorted=0;
$gsupport=3;
$gnoBreakPointFilter=0;
$gstandardChrs={"chrX" => 23, "chrY" => 24 };
foreach my $i (1..22){
        $gstandardChrs{"chr$i"}=$i;
}


GetOptions ("minAlignQual|q=i" => \$gminAlignQual,
            "maxOverlapRegion|o=i"   => \$gmaxOverlapRegion,
            "minUniqueAlignLength|l=i"   => \$gminUniqueAlignLength,
            "maxGap|m=i"  => \$gmaxGap,
            "filterConsensusLength|c=i"   => \$gfilterConsensusLength,
            "inputSamFile|i=s"   => \$inputSamFile,
            "outputDir|d=s"   => \$outputDir,
            "breakpointFile|b=s"   => \$breakpointFile,    
            "guideFile|G=s"   => \$guideFile,
            "guideRange|R=i" => \$gguideRange,
            "enrichment" => \$ggetEnrichment,
            "bam" =>\$gbam,
            "isUnsorted" =>\$gisUnsorted,
            "support=i" =>\$gsupport,
            "verbose"  => \$gverbose)  
or die("Error in command line arguments\n");

if (-d $outputDir){
	system("rm -rf $outputDir/chr*");
}
if (!$breakpointFile){
	foreach my $bp (@ARGV){
		parsePositionLine($bp,\%breakpoints);
	}
}
else{
	%breakpoints=readPositionFile($breakpointFile);
}
if (!keys %breakpoints){
	$gnoBreakPointFilter=1;
}
#print STDERR "$gnoBreakPointFilter\n";
#exit 0;
if ($guideFile){
	print STDERR "reading guides file $guideFile\n";
	readGuideFile($guideFile);
	open($gguideReads,">$outputDir/guides.sam") || die;
}
#get coverage if necessary
my $determineEnrichmentFlag=0;
my $coverageFile;
if ($guideFile || $ggetEnrichment){
	my $dateString=`date +%d:%m:%Y:%H:%M:%S`;
	chomp($dateString);
	$coverageFile="/tmp/coverage_$dateString.txt";
    if ($gisUnsorted){
		print STDERR "sorting sam/bam file\n";
		open ($sfp, "| samtools sort -O sam | samtools coverage /dev/stdin > $coverageFile") || die;
	}
	else{ 
		open ($sfp, "| samtools coverage /dev/stdin > $coverageFile") || die;
	}	
} 
if ($gbam){
	open($fp,"samtools view -h $inputSamFile |") || die "can't open $inputSamFile\n";
}
else{
	open($fp,"$inputSamFile") || die "can't open $inputSamFile\n";
}
my %reads;
my %bpStrings;
my %coveringReads;
my %nearbyCoveringReads;

#read in sam/bam file

while (my $line=<$fp>){
	if($coverageFile){print $sfp "$line";}
	unless(substr($line,0,1) eq "@"){
		my @results;
		if ($coverageFile){
			determineEnrichmentFromSamLine($line);
		}
		if (@results=parseSAMLine($line,\%breakpoints)){
			my @parts=split(' ',$results[2]);
			push(@{$reads{$results[0]}},$results[1]);
			my (@bps)=split(/\n/,$results[2]);
			foreach my $bp (@bps){
				push(@{$bpStrings{$results[0]}},$bp);
			}
		}
	}
}

close($fp);
if ($coverageFile){
  close($sfp);
  $gcoverage=findCoverage($coverageFile);
}
my @names= keys %reads;
my @outputSamLines;
my( @matchedNames,@matchedFusions,@matchedBreakpoints,@matchedScores);
foreach my $name (@names){
	
 my %bpHash;
 my (@matchedBps);
  foreach my $bp (@{$bpStrings{$name}}){
	my	$bpReverse=reverseBreakpointString($bp);
	if ($bpReverse){
		my $canonical=bpCanonical($bp,$bpReverse);
		$bpHash{$canonical}++;
	}
	else{
		$bpHash{$bp}++;
	}
	if (exists($bpHash{$bp})){
		#print STDERR "matched bp $bp\n";
		$bpHash{$bp}++;
	}
	else{
		my	$bpReverse=reverseBreakpointString($bp);
		if ($bpReverse){
		    my $canonical=bpCanonical($bp,$bpReverse);
			$bpHash{$canonical}++;
		}
		else{
			$bpHash{$bp}=1;
		}
	}
  }
  my @bps =  keys %bpHash;
  foreach my $bp (@bps){
	if($bpHash{$bp} > 1 && checkSameChr($bp)){
		push (@matchedBps,$bp); 
	}
  }
  if(@matchedBps){
	#possible that there are multiple matches - pick one with lowest gap/overlap
	my @indices=(0..$#matchedBps);
	my @scores;
	foreach my $bp (@matchedBps){
		my(@parts)=split(/\t/,$bp);
		push(@scores,$parts[-1]); 
	}
	my @sortedIndices=sort{$scores[$a]<=>$scores[$b]}@indices;
	push(@matchedNames,$name);
	my @parts=split(/\t/,$matchedBps[$sortedIndices[0]]);
    push(@matchedFusions,$parts[0]);
    push(@matchedBreakpoints,$parts[1]);
    push(@matchedScores,$parts[2]);
  }
}
#collate the matches
my %mbpHash;
my %mbpScores;
my %nearby;
my %nearbyNames;
my %readThru;
my %nearbyReadThru;
my %mbpNames;
my %mbpFusions;
my %nearbyFusions;

#make an output file with all the read names - must write an empty file if there are no matches for downstream processing
open (OUT,">$outputDir/readnames") || die "can't open $outputDir/readnames\n";
foreach my $name (@matchedNames){
	print OUT "$name\n";
}		
close(OUT);

foreach my $i (0..$#matchedNames){
	my $mbp=$matchedBreakpoints[$i];
	my $mscore=$matchedScores[$i];
	my $mname=$matchedNames[$i];
	my $mfusions=$matchedFusions[$i];
	$mbpHash{$mbp}++;
	if (!exists($mbpNames{$mbp})){@{$mbpNames{$mbp}}=()}
    push(@{$mbpNames{$mbp}},$mname);
    push(@{$mbpFusions{$mbp}},$mfusions);
	if (!exists($mbpScores{$mbp}) || $mscore < $mbpScores{$mbp}){$mbpScores{$mbp}=$mscore}
}
my @mbps=keys %mbpHash;
foreach my $mbp (@mbps){
	$nearby{$mbp}=$mbpHash{$mbp};
	@{$nearbyNames{$mbp}}=@{$mbpNames{$mbp}};
	@{$nearbyFusions{$mbp}}=@{$mbpFusions{$mbp}};
	@{$coveringReads{$mbp}}=();
}

foreach my $mbp (@mbps){
	@{$nearbyCoveringReads{$mbp}}=@{$coveringReads{$mbp}};
}
if ($guideFile){
	#find reads that span each mbp
	foreach $readkey (keys %gonChr){
		my $read=$gonChr{$readkey};
		foreach my $i (0..$#{$gonChr{$readkey}{lpos}}){
			my $lpos=$gonChr{$readkey}{lpos}[$i];
			my $rpos=$gonChr{$readkey}{rpos}[$i];
			my $chr=$gonChr{$readkey}{chr}[$i];
			foreach my $mbp (@mbps){
				my ($chr1,$bp1,$chr2,$bp2)=$mbp =~/([^:^;]+)/g;
				if( ($chr eq $chr1 && $lpos <= $bp1 && $rpos >= $bp1) || ($chr eq $chr2 && $lpos <= $bp2 && $rpos >=$bp2) ){
					push(@{$coveringReads{$mbp}},$read);
					push(@{$nearbyCoveringReads{$mbp}},$read);
				}
			}

		}
	}

}
my (@filteredMbps)=();
foreach my $i (0..$#mbps-1){
	my $mbpi=$mbps[$i];
	my ($chr1,$left,$chr2,$right)=$mbpi =~/([^:^;]+)/g;
	foreach my $j ($i+1..$#mbps){
		my $mbpj=$mbps[$j];
		my ($chr1j,$leftj,$chr2j,$rightj) = $mbpj =~/([^:^;]+)/g;
		if ($chr1 eq $chr1j && $chr2 eq $chr2j && abs($left-$leftj) < $gfilterConsensusLength && abs($right-$rightj) < $gfilterConsensusLength){
			#join the arrays and then unique them later
		   	$nearby{$mbpi}+=$mbpHash{$mbpj};
		   	$nearby{$mbpj}+=$mbpHash{$mbpi};
		   	@{$nearbyNames{$mbpi}}=(@{$nearbyNames{$mbpi}},@{$mbpNames{$mbpj}});
		   	@{$nearbyNames{$mbpj}}=(@{$nearbyNames{$mbpj}},@{$mbpNames{$mbpi}});
		    @{$nearbyFusions{$mbpi}}=(@{$nearbyFusions{$mbpi}},@{$mbpFusions{$mbpj}});
		   	@{$nearbyFusions{$mbpj}}=(@{$nearbyFusions{$mbpj}},@{$mbpFusions{$mbpi}});
		    @{$nearbyCoveringReads{$mbpi}}=(@{$nearbyCoveringReads{$mbpi}},@{$coveringReads{$mbpj}});
		   	@{$nearbyCoveringReads{$mbpj}}=(@{$nearbyCoveringReads{$mbpj}},@{$coveringReads{$mbpi}});
		}	
	}
}
foreach my $mbp (@mbps){
	if ($nearby{$mbp} >= $gsupport){
		push(@filteredMbps,$mbp);
	}
}
my (@sortedMbps) = sort {($mbpScores{$a} <=> $mbpScores{$b}) || ($mbpHash{$b} <=> $mbpHash{$a}) || ($a cmp $b)}@filteredMbps;

system("mkdir -p $outputDir");
open (SUM,">$outputDir/summary") || die;
#print STDERR "@sortedMbps\n";

if ($guideFile){
	print "Breakpoint\tGap/Overlap\tCount\tNearby-count\tReadThru\tNearbyReadThru\tBreakpoint enrichment\tBreakpoint nearby enrichment\tFraction on target\tFraction on target nearby\n";
	print  SUM "Breakpoint\tGap/Overlap\tCount\tNearby-count\tReadThru\tNearbyReadThru\tBreakpoint enrichment\tBreakpoint nearby enrichment\tFraction on target\tFraction on target nearby\n";
	close($gguideReads);
	my (@guideReads)=(keys %genrichSeen);
	foreach my $mbp (@sortedMbps){
		my %guideHash = map { $_ => 1 } @{$mbpNames{$mbp}};
		my %guideNearbyHash = map { $_ => 1 } @{$nearbyNames{$mbp}};
		my %nearbyCoveringReadsHash = map { $_ => 1 } @{$nearbyCoveringReads{$mbp}};
		my %coveringReadsHash = map { $_ => 1 } @{$coveringReads{$mbp}};
		$readThru{$mbp}=keys %coveringReadsHash;
		$nearbyReadThru{$mbp}= keys %nearbyCoveringReadsHash;
		$gonTarget{$mbp}=0;
		$gonNearbyTarget{$mbp}=0;	
		foreach my $guideRead (@guideReads){
			if (exists($guideHash{$guideRead})){$gonTarget{$mbp}++}
			if (exists($guideNearbyHash{$guideRead})){$gonNearbyTarget{$mbp}++}
	    }	
	}	
}
else{
	print "Breakpoint\tGap/Overlap\tCount\tNearby-count\tBreakpoint enrichment\tBreakpoint nearby enrichment\n";
	print  SUM "Breakpoint\tGap/Overlap\tCount\tNearby-count\tBreakpoint enrichment\tBreakpoint nearby enrichment\n";
#close the guides sam file which is needed for stats
}
foreach my $mbp (@sortedMbps){
	my @parts=$mbp =~/([^:^;]+)/g;
	my $fileBase=join("-",@parts);
	my $breakpointEnrichment='-';
	my $nearbyEnrichment='-';
	if ($gcoverage){
		$breakpointEnrichment=sprintf "%8.5f",$mbpHash{$mbp}/$gcoverage;
		$nearbyEnrichment=sprintf "%8.5f",$nearby{$mbp}/$gcoverage;
	}
	if ($guideFile){
		my $onTargetFraction='-';
		my $onNearbyTargetFraction='-';
		if ($readThru{$mbp}){
			$onTargetFraction=sprintf "%8.5f",$mbpHash{$mbp}/$readThru{$mbp};
		}
		if ($nearbyReadThru{$mbp}){
			$onNearbyTargetFraction=sprintf "%8.5f",$nearby{$mbp}/$nearbyReadThru{$mbp};
		}
		
		print "$mbp\t$mbpScores{$mbp}\t$mbpHash{$mbp}\t$nearby{$mbp}\t$readThru{$mbp}\t$nearbyReadThru{$mbp}\t$breakpointEnrichment\t$nearbyEnrichment\t$onTargetFraction\t$onNearbyTargetFraction\n";
		print SUM "$mbp\t$mbpScores{$mbp}\t$mbpHash{$mbp}\t$nearby{$mbp}\t$readThru{$mbp}\t$nearbyReadThru{$mbp}\t$breakpointEnrichment\t$nearbyEnrichment\t$onTargetFraction\t$onNearbyTargetFraction\n";
	}
	else{
		print "$mbp\t$mbpScores{$mbp}\t$mbpHash{$mbp}\t$nearby{$mbp}\t$breakpointEnrichment\t$nearbyEnrichment\n";
		print SUM "$mbp\t$mbpScores{$mbp}\t$mbpHash{$mbp}\t$nearby{$mbp}\t$breakpointEnrichment\t$nearbyEnrichment\n";
		
	}
	open (OUT,">$outputDir/$fileBase.readnamescoords") || die;
	foreach my $i (0..$#{$mbpNames{$mbp}}){
	  my $name=$mbpNames{$mbp}[$i];
	  my $fusion=$mbpFusions{$mbp}[$i];
	  print OUT "$name\t$fusion\n";	
	}
	open (OUT,">$outputDir/$fileBase.nearby.readnamescoords") || die;
	foreach my $i (0..$#{$nearbyNames{$mbp}}){
	  my $name=$nearbyNames{$mbp}[$i];
	  my $fusion=$nearbyFusions{$mbp}[$i];
	  print OUT "$name\t$fusion\n";	
	}
	open (OUT,">$outputDir/$fileBase.sam") || die;
	foreach my $name (@{$mbpNames{$mbp}}){
	  foreach my $read (@{$reads{$name}}){
		print OUT "$read\n";
	  }	
	}
	open (OUT,">$outputDir/$fileBase.nearby.sam") || die;
	foreach my $name (@{$nearbyNames{$mbp}}){
	  foreach my $read (@{$reads{$name}}){
		print OUT "$read\n";
	  }	
	}
}
if($gcoverage){
	printf "Coverage\tMaxBpFromEnd\tGuideReads\tEnrichment\n%10.4f\t%d\t%d\t%10.4f\n",$gcoverage,$gguideRange, $gtotalEnrichReads,$gtotalEnrichReads/$gcoverage;
	printf SUM "Coverage\tMaxBpFromEnd\tGuideReads\tEnrichment\n%10.4f\t%d\t%d\t%10.4f\n",$gcoverage,$gguideRange, $gtotalEnrichReads,$gtotalEnrichReads/$gcoverage;
}
sub readGuideFile{
	my ($file)=@_;
	open(my $fp,'<',$file) || die "can't open $file";
	while (defined(my $line=<$fp>)){
		unless(substr($line,0,1) eq '#'){
			chomp($line);	
			my($chr,$lcoords,$rcoords)=split(/\:/,$line);
			if (!exists($gguideLCoords{$chr})){
				@{$gguideLCoords{$chr}}=($lcoords);
				@{$gguideRCoords{$chr}}=($rcoords);
			}
			else{
				push(@{$gguideLCoords{$chr}},$lcoords);
				push(@{$gguideRCoords{$chr}},$rcoords);
			}
		}
	}
	close($fp);
}
sub checkSameChr{
	my ($bp)=@_;
	my ($chrCoords1,$chrCoords2)=split(/\;/,$bp);
	my ($chr1,$left1,$right1)=split(/\:/,$chrCoords1);
	my ($chr2,$left2,$right2)=split(/\:/,$chrCoords2);
	if ($chr1 ne $chr2){
		return 1;
	}
	if (abs($right1-$left2) > $gMinInversionSize){
		return 1;
	}	
	return 0;
}
sub reverseBreakpointString{
	my ($bpString)=@_;
	my ($breakpoint,$bp,$score)=split(/\t/,$bpString);
	my ($chr1,$chr2)=split(/\;/,$breakpoint);
    my ($chr,$left,$right)=split(/\:/,$chr1);
	my ($chrj,$leftj,$rightj)=split(/\:/,$chr2);
    if ($chr ne $chrj || abs($right - $leftj) < $gMinInversionSize){
		return "";
	}
	my $newChr1=reverseChrCoords($chr1);
	my $newChr2=reverseChrCoords($chr2);
	my $newBreakpoint=join(";",($newChr2,$newChr1));
	my $newBp=reverseBpCoords($bp);
	return join("\t",($newBreakpoint,$newBp,$score));
}
sub bpCanonical{
	my ($bp1,$bp2)=@_;
	my ($breakpoint1,$alt1,$score1)=split(/\t/,$bp1);
	my ($breakpoint2,$alt2,$score2)=split(/\t/,$bp2);
	my ($chr1,$chr2)=split(/\;/,$breakpoint1);
	my ($chr1j,$chr2j)=split(/\;/,$breakpoint2);
	my ($chr,$left,$right)=split(/\:/,$chr1);
	my ($chrj,$leftj,$rightj)=split(/\:/,$chr1j);
	if($right <$rightj){
		return $bp1;
	}
	else{
		return $bp2;
	}	
}
sub reverseChrCoords{
	my ($chrCoords)=@_;
	my ($chr,$left,$right)=split(/\:/,$chrCoords);
	return join(":",$chr,$right,$left);
}
sub reverseBpCoords{
	my ($bp)=@_;
	my ($chr1,$chr2)=split(/\;/,$bp);
	return join(";",$chr2,$chr1);
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
	my ($line,$breakpoints)=@_;
	chomp($line);
	my ($brk1Str,$brk2Str)=split(/\;/,$line);
	my(@pos1)=split(/\:/,$brk1Str);
	my(@pos2)=split(/\:/,$brk2Str);
	foreach my $i (0..2){
	  if (defined($pos1[$i])){push(@{$breakpoints->{pos1}},$pos1[$i])}
	  else {push(@{$breakpoints->{pos1}},undef)}
	  if (defined($pos2[$i])){push(@{$breakpoints->{pos2}},$pos2[$i])}
	  else {push(@{$breakpoints->{pos2}},undef)}
	}
}
sub updateOnChromosomeReads{
	my($read,$chr,$lpos,$rpos)=@_;
	if (!exists($gonChr{$read})){
		$gonChr{$read}={};
		@{$gonChr{$read}{lpos}}=();
		@{$gonChr{$read}{rpos}}=();
		@{$gonChr{$read}{chr}}=();
	}
	push(@{$gonChr{$read}{lpos}},$lpos);
	push(@{$gonChr{$read}{rpos}},$rpos);
	push(@{$gonChr{$read}{chr}},$chr);
}
sub determineEnrichmentFromSamLine{
  my ($line)=@_;
  chomp($line);
  my (@parts)=split(/\t/,$line);
  my ($qname,$flag,$rname,$pos,$mapq,$cigar,$rnext,$pnext,$tlen,$seq,$qual,@tags)=@parts;
  if ($gguideLCoords{$rname}){
	#rpos is needed and calculated here and later passed to the guideMatches routine if needed
	my $rpos=findRightRefPosition($pos,$cigar);
	updateOnChromosomeReads($qname,$rname,$pos,$rpos);
	if(!$genrichSeen{$qname}){
		#find the position of left and right matches in the read
		if(guideMatches($rname,$pos,$cigar,$rpos)){
			print $gguideReads "$line\n";
			$genrichSeen{$qname}++;
			$gtotalEnrichReads++ 
		} 
	}
  }
}
sub guideMatches{
  my($chr,$lpos,$cigar,$rpos)=@_;  
  #check leftCigar and rightCigar
  my ($lmismatch,$rmismatch)=findCigarMismatches($cigar);
  if ($lmismatch < $gguideRange){
	my $leftEndPos=$lpos-$lmismatch;
	if ($leftEndPos < 1) {$leftEndPos = 1}
	foreach my $pos (@{$gguideLCoords{$chr}}){
		if(abs($leftEndPos-$pos) < $gguideRange){
			return 1;
		}
	}		  
  }
  if ($rmismatch < $gguideRange){ 
	 my $rightEndPos=$rpos+$rmismatch;
	 foreach my $pos (@{$gguideRCoords{$chr}}){
		if(abs($rightEndPos-$pos) < $gguideRange){
			return 1;
		}
	 }
  }
  return 0;

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
  my ($line,$breakpoints)=@_;
  chomp($line);
  my (@parts)=split(/\t/,$line);
  my ($qname,$flag,$rname,$pos,$mapq,$cigar,$rnext,$pnext,$tlen,$seq,$qual,@tags)=@parts;
  if ($#parts < 4){
	die "bad line:\n$line\n";  
  }
  if ($mapq < $gminAlignQual){
	  return();
  }
  if ($gnoBreakPointFilter){
		my $altTag;
        if (@tags && exists($gstandardChrs{$rname}) && ($altTag=findSAZTag(@tags))){
                my (@alts1,@alts2,$rs1,$rs2);
                if( findOtherAlt($altTag,$rname,\@alts1,\@alts2)){
                        if (@alts1){
                                $rs1=findBreakPoints(0,$flag,$rname,$pos,$cigar,\@alts1);
                        }
                        if (@alts2){
                                $rs2=findBreakPoints(1,$flag,$rname,$pos,$cigar,\@alts2);
                        }
                        if ($rs1){
                                if($rs2){
                                        return ($qname,$line,"$rs1$rs2");
                                }
                            return ($qname,$line,$rs1)
                        }
                        elsif($rs2){
                                return ($qname,$line,$rs2)
                        }
                }
        }       
  }
  else{
	foreach my $i (0..$#{$breakpoints->{pos1}}/3){
		my (@pos1)=@{$breakpoints->{pos1}}[$i*3..$i*3+2];
		my (@pos2)=@{$breakpoints->{pos2}}[$i*3..$i*3+2];
		if (posMatch(\@pos1,$rname,$pos) && @tags && ($altTag=findSAZTag(@tags))) {
			my $mainPos=0;#let findBreakPoints know that main has matched pos1
			my @alts=findAlt($altTag,\@pos2);
			if (@alts){
				my $rs=findBreakPoints($mainPos,$flag,$rname,$pos,$cigar,\@alts);
				if($rs){
					return ($qname,$line,$rs)
				}
			}
		}
		if (posMatch(\@pos2,$rname,$pos) && @tags && ($altTag=findSAZTag(@tags))){
			my @alts=findAlt($altTag,\@pos1);
			my $mainPos=1;#let findBreakPoints know thatmain has matched pos2
			if (@alts){
				my $rs=findBreakPoints($mainPos,$flag,$rname,$pos,$cigar,\@alts);
				if($rs){
					return ($qname,$line,$rs)
				}
			}
		}
	  }
  }
  return ();
}
sub findBreakPoints{
	my ($mainPos,$flag,$rname,$pos,$cigar,$alts)=@_;
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
				   	$breakpoint="$rname:$mainRefPos[0];$altChr:$altRefPos[1]";
					$fusion="$rname:$mainRefPos[1]:$mainRefPos[0];$altChr:$altRefPos[1]:$altRefPos[0]";
				}
				else{
				   	$breakpoint="$altChr:$altRefPos[1];$rname:$mainRefPos[0]";
					$fusion="$altChr:$altRefPos[0]:$altRefPos[1];$rname:$mainRefPos[0]:$mainRefPos[1]";	
				}
			}
			else{
				#main goes before alt
				if ($mainPos == 0){
				   	$breakpoint="$rname:$mainRefPos[1];$altChr:$altRefPos[0]";
					$fusion="$rname:$mainRefPos[0]:$mainRefPos[1];$altChr:$altRefPos[0]:$altRefPos[1]";
				}
				else{
					#the alt match must be written first so reverse the fusion orientation
				   	$breakpoint="$altChr:$altRefPos[0];$rname:$mainRefPos[1]";
					$fusion="$altChr:$altRefPos[1]:$altRefPos[0];$rname:$mainRefPos[1]:$mainRefPos[0]";	
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
	my($query,$rname,$pos)=@_;
	if ($query->[0] && $query->[0] ne $rname){return 0;}
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
sub findCoverage{
    my ($file)=@_;
    open (my $fp, "$file") || die;
	my $totalbp=0;
	my $totalcov=0;
	while (defined(my $line=<$fp>)){
		if(substr($line,0,3) eq "chr" && substr($line,0,4) ne "chrM"){
			my (@parts)= split(' ',$line);
			$totalbp+=$parts[2];
			$totalcov+=$parts[4];
		}
	}
	if ($totalbp){return (sprintf "%f",$totalcov/$totalbp)}
	return 0;		
} 

