#!/usr/bin/perl
my ($guidesFile) = @ARGV;
if (!defined $guidesFile) {
    die "Usage: $0 <guidesFile> \n";
}
open (my $guides, "<", $guidesFile) or die "Cannot open $guidesFile: $!\n";
my @guides;
my %guides_dict;
while (my $line = <$guides>) {
    chomp $line;
    my ($group, $chr, $direction) = split /\t/, $line;
    my $chrCoordDir=$chr."$direction";
    if (!exists $guides_dict{$group}) {
        @{$guides_dict{$group}}= ($chrCoordDir);
    }
    else{
        push @{$guides_dict{$group}}, $chrCoordDir;
    }
}
close $guides;
foreach my $group (keys %guides_dict) {
    my @guides = @{$guides_dict{$group}};
    print "Group: $group\n";
    foreach my $guide (@guides) {
        print "$guide\n";
    }
}
