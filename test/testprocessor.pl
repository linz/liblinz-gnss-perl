#!/usr/bin/perl
#
#  Script to run a daily processor.  This just creates and runs a LINZ::GNSS::DailyProcessor 
#  instance using the specified configuration
#

use strict;
use lib '../lib';
use LINZ::GNSS::DailyProcessor;
use File::Path qw/make_path remove_tree/;
use File::Find;
use LINZ::RunBatch;
use Getopt::Std;
use Cwd qw(cwd);

my %opts;
getopts('hHcCfs:e:t:T:m:',\%opts);

if( $opts{c} || $opts{C} )
{
    my $config=LINZ::GNSS::DailyProcessor::ExampleConfig;
    if( $opts{C})
    {
        $config=~s/^\#[^\n]*\n/\n/mg;
        $config=~s/\n\s*\n/\n/g;
    }
    print $config;
    exit();
}

@ARGV || die <<EOD;

syntax: run_daily_processor [options] configuration_file [stop|restart] [option=value option=value ...]

Options can include
   -b          Run in batch mode (using at now)
   -B logfile  Run in batch mode and specify the log file
   -c          Just print an example configuration file and exit
   -C          Print example configuration more briefly
   -f          Force retry of all failures 
               (retry_max_age_days=0, retry_interval_days=0)
   -s yyyy-ddd Start date
   -e yyyy-ddd End date
   -t yyyy-ddd Test specific day - outputs to current directory, outputs debug info
   -T dir      Test target directory - also copies bernese processing directory to dir/bernese
   -m #        Maximum number of days to process
   -h          Halt running processes by creating a "stop file" (same as "stop")
   -H          Delete the stop file and restart (same as "restart")

EOD

my $configfile=shift @ARGV;
my @params;
foreach my $prm (@ARGV)
{
    if( lc($prm) eq 'stop' )
    {
       $opts{h}=1;
    } 
    elsif(lc($prm) eq 'restart')
    {
        $opts{H}=1;
    }
    elsif($prm !~ /^\w+\=/)
    {
        print "Invalid option $prm: options must be entered as option=value\n";
        exit();
    }
    else
    {
        push(@params,$prm) if $prm =~ /\=/;
    }
}
 
# Fix up parameters accidentally entered without '='


if( $opts{f} )
{
    push(@params,'retry_max_age_days=0','retry_interval_days=0');
}
my $startdate=$opts{t} || $opts{s};
my $enddate=$opts{t} || $opts{e};
my $targetdir=$opts{T} || ($opts{t} ? '.' : '');
die "Invalid test target directory $targetdir specified\n" if $targetdir && ! -d $targetdir;

push(@params,'start_date='.$startdate) if $startdate;
push(@params,'end_date='.$enddate) if $enddate;
push(@params,'target_directory='.$targetdir) if $targetdir;
push(@params,'logsettings=debug') if $opts{t};
push(@params,'rerun=1') if $opts{t};
push(@params,'pcf_copy_dir=bernese') if $opts{T};
push(@params,'pcf_fail_copy_dir=bernese') if $opts{T};
push(@params,'max_days_processed_per_run='.$opts{m}) if $opts{m};

eval
{
    die "$configfile is missing\n" if ! -e $configfile;
    my $processor=new LINZ::GNSS::DailyProcessor($configfile,@params);
    my $stop_file=$processor->get('stop_file','');
    if( $opts{h} )
    {
        if( $stop_file )
        {
            open(my $sf,">",$stop_file) || die "Cannot create stop file $stop_file\n";
            print $sf "Delete this file to allow the daily processor to run\n";
            close($sf);
            print "Create stop file $stop_file\n";
            print "This will stop the daily processor at the end of the current job\n";
            print "Delete this file to allow the processor to run again.\n";
        }
        else
        {
            print "The configuration does not define a stop_file .. processor cannot be halted\n";
        }
        exit();
    }
    unlink($stop_file) if $stop_file && $opts{H};
    if( $stop_file && -e $stop_file )
    {
        print "Processor not started because stop file $stop_file is present\n".
        print "Use -S option to restart.\n";
    }
    else
    {
        $processor->runProcessor(\&testProcess);
    }
};
if( $@ )
{
    print "Processing failed: $@\n";
}

sub testProcess
{
    my($self)=@_;
    my $vars=$self->{vars};
    $self->info('Running testProcess in '.cwd);
    foreach my $k (sort keys %$vars)
    {
        my $varstr=sprintf("  %s=%s",$k,$vars->{$k});
        $self->info($varstr);
    }
    my $targetdir=$self->targetdir;
    my $dd=$self->get('dd');
    my $prerequisite_files=$self->get('prerequisite_files','');
    my $config=$self->get('configname','unnamed');
    open(my $fh,">$targetdir/output-$config-$dd-1.txt");
    print $fh "Here is some work done at ".time()."\n";
    print $fh "Prerequisite files: $prerequisite_files\n";
    close($fh);
    if( $self->get('pcf_campaign_files'))
    {
        my $campdir=$targetdir.'/campaign';
        make_path($campdir.'/SOL');
        make_path($campdir.'/RAW');
        $self->installPcfCampaignFiles($campdir);
    }
    my @files=();
    my $lendir=length($targetdir);
    find(
        sub 
        {
            my $name=$File::Find::name;
            $name=substr($name,$lendir) if substr($name,0,$lendir) eq $targetdir;
            push(@files,$name);
        },
    $targetdir);

    open(my $fh2,">$targetdir/output-$config-$dd-2.txt");
    print $fh2 "Here is some more work done at ".time()."\nFiles are:\n";
    foreach my $f (@files){ print $fh2 "  $f\n"; }
    close($fh2);
    return 1;
}
