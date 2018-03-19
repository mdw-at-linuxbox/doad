#!/usr/bin/perl

#
#	read in a list of files.
#	write out:
#		ADD file-1
#		DEL file-1
#	randomized, but for each file, ADD is always before DEL
#

use common::sense;
use Geography::Countries;

#my $oflag;
#my $kflag;
my $rc;

sub process_opts
{
	my @r;
	my $f;
	for my $j ( @_ ) {
		if (defined($f)) {
			&$f($j);
			undef $f;
			next;
		}
#		if ($j eq "-k") {
#			++$kflag;
#			next;
#		}
#		if ($j eq "-o") {
#			$f = sub {
#				my ($f) = @_;
#				$oflag = $f;
#			};
#			next;
#		}
#		if ($j eq "-d") {
#			++$dflag;
#			next;
#		}
		push @r, $j;
	}
	return @r;
}

sub readin_files
{
	my @r;
	while (my $l = <>) {
		chomp $l;
		push @r, $l;
	}
	return \@r;
}

sub permute_files
{
	my ($i) = @_;
	my @work;
	my @results;
	for my $q ( @$i ) {
		my $e = {};
		$e->{op} = "ADD";
		$e->{fn} = $q;
		push @work, $e;
	}
	while ($#work >= 0) {
		my $n = 1+$#work;
		my $j = int(rand($n));
		my $e = splice(@work, $j, 1);
		push @results, $e;
		if ($e->{op} eq "ADD") {
			my $f = {};
			$f->{op} = "DEL";
			$f->{fn} = $e->{fn};
			push @work, $f;
		}
	}
	return \@results;
}

sub writeout_files
{
	my ($x) = @_;
	for my $j ( @$x ) {
		print join(" ", $j->{op}, $j->{fn})."\n";
	}
}

@ARGV = process_opts(@ARGV);

my $list = readin_files();
my $list2 = permute_files($list);
writeout_files($list2);
exit($rc);
