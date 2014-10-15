=head1 LINZ::GNSS::DailyProcessor

Module to manage running daily processing routines into a directory 
structure of results.  Key files in the daily directories are used to
track which directories have been processed and the results of the processing,
as well as to lock directories to prevent multiple processors accessing it at
the same time.

The processing is based on a configuration file as per example below.


=cut

use strict;

package LINZ::GNSS::DailyProcessor;
use Carp;
use File::Path qw/make_path remove_tree/;
use File::Which;
use LINZ::GNSS::Config;
use LINZ::GNSS::Time qw/
    $SECS_PER_DAY
    ymdhms_seconds
    seconds_ymdhms
    yearday_seconds
    seconds_yearday
    /;
use Log::Log4perl qw(:easy);

use vars qw/$processor/;

=head2 $processor=LINZ::GNSS::DailyProcessor->new($cfgfile,@args)

Creates a new daily processor based on the configuration file, and 
also any "name=value" items in the list of arguments.  In particular

 config=xxx

may be used to identify an alternative configuration (see LINZ::GNSS::Config)

=cut

sub new
{
    my($class,$cfgfile,@args)=@_;
    my $cfg=LINZ::GNSS::Config->new($cfgfile,@args);
    my $self={cfg=>$cfg,vars=>{},loggers=>{}};
    return bless $self,$class;
}

=head2 $processor->runProcessor( $func )

The processor main routine.  Runs the processing for each year as
specified by the configuration, calling the supplied function to 
implement the process.

=cut

sub runProcessor
{
    my ($self,$func) = @_;
    my $start_date=$self->getDate('start_date');
    my $end_date=$self->getDate('end_date');
    my $runtime=time();
    my $maxruntime=$self->get('max_runtime_seconds','0');
    # Set max run time to 1000 days if not specified or 0
    $maxruntime=1000*$SECS_PER_DAY if $maxruntime==0;
    my $endtime=$runtime+$maxruntime;

    my $completefile=$self->get('complete_file');
    my $failfile=$self->get('fail_file');
    my $retry_max_age=$runtime-$self->get('retry_max_age_days')*$SECS_PER_DAY;
    my $retry_interval_days=$self->get('retry_interval_days');

    my $rerun=$self->get('rerun','0');

    for( my $date=$end_date; $date >= $start_date; $date -= $SECS_PER_DAY )
    {
        # Have we run out of time
        if( time() > $endtime )
        {
            $self->writeLog("Daily processor cancelled: max run time expired");
            last;
        }
        my($year,$day)= $self->setYearDay($date);

        my $targetdir=$self->get('target_directory');
        $self->makePath($targetdir);
        $self->set('target',$targetdir);

        # Set up the processor enviromnent
        $ENV{PROCESSOR_TARGET_DIR}=$targetdir;
        $ENV{PROCESSOR_YEAR}=$year;
        $ENV{PROCESSOR_DOY}=$day;

        $self->deleteMarkerFiles($completefile,$failfile) if $rerun;

        # Has this directory already been processed?
        next if $self->markerFileExists($completefile);

        # Did it fail but is not ready to rerun
        if( $self->markerFileExists($failfile) )
        {
            next if $date < $retry_max_age;
            next if -M $targetdir.'/'.$failfile < $retry_interval_days;
            $self->deleteMarkerFile($failfile);
        }

        # Can we get a lock on the file.
        next if ! $self->lock();
        $self->cleanTarget();
        $self->info("Processing $year $day");
        eval
        {
            $func->($self);
            $self->createMarkerFile($completefile);
            $self->info("Processing completed");
        };
        if( $@ )
        {
            my $message=$@;
            $self->warn("Processing failed: $message");
            $self->createMarkerFile($failfile);
        }
        $self->unlock();
    }
}

sub setPcfParams
{
    my($self,$params,$varhash)=@_;
    while ($params =~ /\b(\w+)\=(\S+)/g)
    {
        $varhash->{$1}=$2;
    }
}

=head2 $self->runBernesePcf($pcf,$pcf_params)

Utility routine for running a Bernese PCF .  Accepts the name of a PCF file 
and PCF parameters (formatted as a string "var=value var=value ..."). If
either is not supplied then values are taken from the processor configuration.

The routine creates a temporary runtime environment for the Bernese run, using
the system installed options directories and scripts.  It then runs the Bernese
script and determines the success or failure status.

=cut

sub runBernesePcf
{
    my($self,$pcf,$pcf_params)=@_;
    # If pcf is blank then need a name
    $pcf ||= $self->get('pcf');
    # If params is defined then use (which allows overriding default params with none)
    $pcf_params //= $self->get('pcf_params');

    require LINZ::BERN::BernUtil;

    # Create a Bernese environment.  Ensrue that the SAVEDISK area is redirected
    # to the target directory for the daily processing.

    my $environment=LINZ::BERN::BernUtil::CreateRuntimeEnvironment(
        CanOverwrite=>1,
        EnvironmentVariables=>{S=>$self->target}
        );

    my $start=$self->timestamp;
    my $end=$start+$SECS_PER_DAY-1;
    my $campaign=LINZ::BERN::BernUtil::CreateCampaign(
        $pcf,
        CanOverwrite=>1,
        SetSession=>[$start,$end],
        MakeSessionFile=>1,  # Daily session file
        UseStandardSessions=>1,
        );
    $ENV{PROCESSOR_CAMPAIGN}=$campaign->{JOBID};
    $ENV{PROCESSOR_CAMPAIGN_DIR}=$campaign->{CAMPAIGN};
    $self->setPcfParams($pcf_params,$campaign->{variables});
    my $result=LINZ::BERN::BernUtil::RunPcf($campaign,$pcf,%$environment);
    my $status=LINZ::BERN::BernUtil::RunPcfStatus($campaign);
    $self->{pcfstatus}=$status;
    my $return=1;
    if( $status->{status} eq 'OK' )
    {
        $self->info('Bernese PCF $pcf successfully run');
    }
    else
    {
        $self->warn(join(': ','Bernese PCF $pcf failed',
            $status->{fail_pid},
            $status->{fail_script},
            $status->{fail_prog},
            $status->{fail_message}
        ));
        $return=0;
    }
    $ENV{PROCESSOR_BERNESE_STATUS}=1;
    return $return;
}

=head2 $processor->runProcessorScript($scriptname,$param,$param)

Utility routine for running a script.  This looks for an executable script
matching the supplied name in the configuration directory, then as an
absolute path name, then in the system path.

=cut

sub runProcessorScript
{
    my($self,$script,@params)=@_;
    
    my $cfgdir=$self->cfg->get('configdir');
    my $exe;
    
    if( -x "$cfgdir/$script" )
    {
        $exe="$cfgdir/$script";
    }
    elsif( -x $script )
    {
        $exe=$script;
    }
    else
    {
        $exe=File::Which::which($script);
    }
    my $result;
    if( -x $exe )
    {
        require IPC::Run;
        my ($in,$out,$err);
        $self->info("Running script $script\n");
        my @cmd=($exe,@params);
        IPC::Run::run(\@cmd,\$in,\$out,\$err);
        $self->info($out) if $in;
        $self->error($err) if $err;
        $result=1;
    }
    else
    {
        $self->error("Cannot find script $script\n");
        $result=0;
    }
    return $result;
}

=head2 $processor->runPerlScript($scriptname,$param,$param)

Utility routine for running a perl script.  The script is executed using
the perl 'do' function.  The routine first looks for the script in the 
the configuration file directory, then as an absolute path.

Within the perl script the variable $processor can be used to refer to this
instance of the processor. The @ARGV variable will contain parameters passed
to this routine.

=cut

sub runPerlScript
{
    my($self,$script,@params)=@_;
    
    my $cfgdir=$self->cfg->get('configdir');
    my $scriptfile;
    
    if( -e "$cfgdir/$script" )
    {
        $scriptfile="$cfgdir/$script";
    }
    elsif( -e $script )
    {
        $scriptfile=$script;
    }
    my $result;
    if( -e $scriptfile )
    {
        $self->info("Running perl script $script\n");
        $processor=$self;
        @ARGV=@params;
        do $scriptfile;
        if( $@ )
        {
            $self->error($@);
        }
        $result=1;
    }
    else
    {
        $self->error("Cannot find perl script $script\n");
        $result=0;
    }
    return $result;
}

=head2 $processor->runScripts( @scripts )

Runs one or more scripts.  Scripts may be supplied in a newline separated
string (each line containing a script name followed by a list of space
separated parameters, or as multiple arguments.  Scripts prefixed perl:
are run as perl scripts, otherwise as system commands.

Will return 0 if running any script returns 0 (script cannot be found), 
otherwise will return 1.

=cut

sub runScripts
{
    my ($self,@scripts)=@_;
    @scripts=map { split(/\n/,$_) } @scripts;
    my $result=1;
    foreach my $scriptdef (@scripts)
    {
        my($scriptname,@params)=split(' ',$scriptdef);
        next if ! $scriptname;
        if( $scriptname =~ /^perl\:/ )
        {
            $scriptname=$';
            my $result=$self->runPerlScript($scriptname,@params);
            last if ! $result;
        }
        else
        {
            my $result=$self->runProcessorScript($scriptname,@params);
            last if ! $result;
        }
    }
    return $result;
}

=head2 $processor->cfg

Returns the processor configuration file used by the script

=cut

sub cfg
{
    my ($self)=@_;
    return $self->{cfg};
}

=head2 my $value=$processor->get($key,$default)

Get a processor variable, either one define using set(), or 
one from the configuration

=cut

sub get
{
    my( $self, $key, $default ) = @_;
    return exists $self->{vars}->{$key} ?
           $self->{vars}->{$key} :
           $self->cfg->get($key,$default);
}

=head2 $processor->set($key,$value)

Set a value that may be used by the processor (or scripts it runs)

=cut

sub set
{
    my( $self, $key, $value ) = @_;
    $self->{vars}->{$key}=$value;
    return $self;
}

=head2 $processor->getDate($key)

Returns a date defined by the configuration file as a timestamp

=cut

sub getDate
{
    my ($self,$key)=@_;
    return $self->cfg->getDate($key);
}

=head2 $processor->setYearDay($timestamp)

Sets the time used to interpret configuration items (eg directory names)
and returns the year/day (which are also available as $processor->get('yyyy'), 
and $processor->get('ddd').

=cut

sub setYearDay
{
    my($self,$timestamp)=@_;
    $self->set('timestamp',$timestamp);
    $self->cfg->setTime($timestamp);
    return ($self->get('yyyy'),$self->get('ddd'));
}

=head2 $processor->logger

Returns a Log::Log4perl logger associated with the processor.

=cut

sub logger
{
    my ($self, $loggerid) = @_;
    $self->{loggers}->{$loggerid} = $self->cfg->logger($loggerid)
        if ! exists $self->{loggers}->{$loggerid};
    return $self->{loggers}->{$loggerid};
};


=head2 $processor->makePath

Creates a path, including all components to it, if it does not exist

=cut

sub makePath
{
    my($self,$path)=@_;
    return 1 if -d $path;
    eval
    {
        my $errors;
        make_path($path,{error=>\$errors});
    };
    return -d $path ? 1 : 0;
}

=head2 $processor->createMarkerFile($file,$message)

Creates a marker file in the current target directory

=cut

sub createMarkerFile
{
    my($self,$file,$message)=@_;
    my $marker=$self->target.'/'.$file;
    open(my $mf,">",$marker);
    print $mf $message;
    close($mf);
}

=head2 $processor->markerFileExists($file)

Tests if a marker file exists in the current target directory

=cut

sub markerFileExists
{
    my($self,$file)=@_;
    my $marker=$self->target.'/'.$file;
    return -e $marker;
}

=head2 $processor->deletemarkerFiles($file1,$file2,...)

Delete one or more marker files

=cut

sub deleteMarkerFiles
{
    my ($self,@files)=@_;
    my $target=$self->target;
    foreach my $file (@files)
    {
        unlink($target.'/'.$file);
    }
}


=head2 $processor->lock

Attempt to create a lock file in the current target directory.
Returns 1 if successful or 0 otherwise.

=cut

sub lock
{
    my($self)=@_;
    my $targetdir=$self->target;
    my $lockfile=$self->get('lock_file');
    $lockfile="$targetdir/$lockfile";
    my $lockexpiry=$self->get('lock_expiry_days');
    return 0 if -e $lockfile && -M $lockfile < $lockexpiry;
    $self->writeLog("Overriding expired lock") if -e $lockfile;
    return 0 if ! -d $targetdir && ! $self->makePath($targetdir);
    open(my $lf,">",$lockfile);
    if( ! $lf )
    {
        $self->writeLog("Cannot create lockfile $lockfile\n");
        return 0;
    }
    my $lockmessage="PID:$$";
    print $lf $lockmessage;
    close($lf);

    # Check we actually got the lock
    my $check;
    open($lf,"<",$lockfile);
    if( $lf )
    {
        $check=<$lf>;
        close($lf);
        $check =~ s/\s+//g;
    }
    if( $check ne $lockmessage )
    {
        $self->writeLog("Beaten to lockfile $lockfile\n");
        return 0;
    }
    return 1;
}

=head2 $processor->unlock

Release the lock on the current target

=cut

sub unlock
{
    my($self)=@_;
    my $targetdir=$self->target;
    my $lockfile=$self->get('lock_file');
    $lockfile="$targetdir/$lockfile";
    unlink($lockfile);
}

=head2 $processor->cleanTarget

Cleans the files from the current target directory if clean_on_start
is defined.

=cut

sub cleanTarget
{
    my($self)=@_;
    my $cleanfiles=$self->get('clean_on_start');
    return if $cleanfiles eq '' || lc($cleanfiles) eq 'none';
    $cleanfiles='*' if lc($cleanfiles) eq 'all';

    my $targetdir=$self->target;
    my $lockfile=$self->get('lock_file');
    $lockfile="$targetdir/$lockfile";

    foreach my $spec (split(' ',$cleanfiles))
    {
        foreach my $rmf (glob("$targetdir/$spec"))
        {
            next if $rmf eq $lockfile;
            if( -f $rmf )
            {
                unlink($rmf);
            }
            elsif( -d $rmf )
            {
                my $errors;
                remove_tree($rmf,{error=>\$errors});
            }
        }
    }
}

sub _logMessage
{
    my($self,$message)=@_;
    return $self->year.':'.$self->day.': '.$message;
}

=head2 $processor->info( $message )

Writes an info message to the log

=cut

sub info
{
    my($self,$message)=@_;
    $self->logger->info($self->_logMessage($message));
}

=head2 $processor->warn( $message )

Writes an warning message to the log

=cut

sub warn
{
    my($self,$message)=@_;
    $self->logger->warn($self->_logMessage($message));
}

=head2 $processor->error( $message )

Writes an error message to the log

=cut

sub error
{
    my($self,$message)=@_;
    $self->logger->error($self->_logMessage($message));
}

=head2 $processor->year

Returns the current year being processed

=cut

sub year { return $_[0]->get('yyyy'); }

=head2 $processor->day

Returns the current day of year being processed

=cut

sub day { return $_[0]->get('ddd'); }

=head2 $processor->timestamp

Returns unix timstamp of the start of the day being processed

=cut

sub timestamp { return $_[0]->get('timestamp'); }

=head2 $processor->target

Returns the current target directory

=cut

sub target { return $_[0]->get('target'); }

          
1;

__END__


=head2 Example configuration file

 # Configuration file for the daily processing code
 #
 # This is read and processed using the LINZ::GNSS::Config module.
 #
 # Configuration items may include ${xxx} that are replaced by (in order of
 # preference) 
 #    command line parameters xxx=yyy
 #    other configuration items
 #    environment variable xxx
 #
 # ${configdir} is defined as the directory of this configuration file unless
 # overridden by another option
 # ${pid_#} expands to the process id padded with 0or left trimmed to # 
 # characters 
 # ${yyyy} and ${ddd} expand to the currently processing day and year, and
 # otherwise are invalid.
 
 # Parameters can have alternative configuration specified by suffix -cfg.
 #
 # For example
 #   start_date -14
 #   start_date-rapid -2
 #
 # Will use -14 for start_date by default, and -2 if the script is run
 # with cfg=rapid on the command line.
 
 # Working directories.  Paths are relative to the location of this
 # configuration file.  
 
 # Location of results files, status files, lock files for daily processing.
 
 target_directory ${configdir}/${yyyy}/${ddd}
 
 # Lock file - used to prevent two jobs trying to work on the same job
 # Lock expiry is the time out for the lock - a lock that is older than this
 # is ignored.
 
 lock_file daily_processing.lock
 lock_expiry_days 0.9
 
 # Completed file - used to flag that the daily processing has run for 
 # the directory successfully
 # The script will not run on a directory containing this flag.
 
 complete_file daily_processing.complete
 
 # Fail file - used to flag that the processing has run for the directory
 # but not succeeded.  The script may run in that directory again subject
 # to the retry parameters.
 
 fail_file daily_processing.failed
 
 # Failed jobs may be retried after retry_interval, but will never rerun 
 # for jobs greater than retry_max_age days.  (Assume that nothing will
 # change to fix them after that)
 
 retry_max_age_days  30
 retry_interval_days 0.9
 
 # Start and end dates.  
 # Processing is done starting at the end date (most recent) back to 
 # the start date.  Dates may either by dd-mm-yyyy, yyyy-ddd, 
 # or -n (for n days before today).
 
 start_date 2000/001
 end_date -18
 
 start_date-rapid -17
 end_date-rapid -2
 
 # Limits on number of jobs.  0 = unlimited
 # Maximum number of days is the maximum number of days that will be processed,
 # not including days that don't need processing.
 # Maximum run time defines the latest time after the script is initiated that
 # it will start a new job.
 
 max_days_per_run 0
 max_runtime_seconds 0
 
 # Clean on start setting controls which files are removed from the
 # result directory when the jobs starts.  The default is just to remove
 # the job management files (above).
 # 
 # Use "all" to remove all files (except the lock file)
 # Use "none" to leave all other files unaltered
 # Use file names (possibly wildcarded) for specific files.
 
 clean_on_start all
 
 # =======================================================================
 # The following items are used by the run_daily_processing script
 #
 # The name of the Bernese PCF to run
 
 pcf          POSDAILY
 
 # PCF parameters to override, written as 
 #  xxx1=yyy1 xxx2=yyy2 ...
 
 pcf_params         V_ORBTYP=FINAL V_O=s
 pcf_params-rapid   V_ORBTYP=RAPID V_O=r
 
 # Pre-run and post run scripts.  These are run before and after the 
 # bernese job.  When these are run the Bernese environment is configured,
 # including the Bernese environment variables.  Three additional variables
 # are defined
 #
 #    PROCESSOR_CAMPAIGN      The Bernese campaign id
 #    PROCESSOR_CAMPAIGN_DIR  The Bernese campaign id
 #    PROCESSOR_STATUS        (Only applies to the post_run script) either 0 or 1 
 #                            depending on whether the Bernese BPE script ran 
 #                            successfully or not.
 #
 # If a directory is not specified these are assumed to be in the same directory
 # as the configuration file
 #
 # If "none" then no script is run.
 # If more than one script is to be run these can included using a <<EOD heredoc.
 # Each line can specify a script name and parameters.
 # If the script name is prefixed with perl: then it will be run as a perl script 
 # in the context of the processor using the perl "do" function, otherwise it 
 # will run as a normal command.
 
 prerun_script  none
 postrun_script none
 
=cut
