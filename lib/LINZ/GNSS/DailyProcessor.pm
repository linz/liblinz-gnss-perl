
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
use Archive::Zip qw/ :ERROR_CODES /;
use Cwd qw/abs_path/;
use File::Path qw/make_path remove_tree/;
use File::Which;
use File::Copy;
use File::Copy::Recursive;
use File::Temp;
use LINZ::GNSS::Config;
use LINZ::GNSS::AwsS3Bucket;
use LINZ::GNSS::Time qw/
  $SECS_PER_DAY
  ymdhms_seconds
  seconds_ymdhms
  yearday_seconds
  seconds_yearday
  /;
use LINZ::GNSS::Variables qw/ExpandEnv/;
use List::Util;
use POSIX qw(strftime);
use Time::Local;
use Log::Log4perl qw(:easy);

use vars qw/$processor/;

our $UNLOCKED    = 0;
our $LOCKEXPIRED = 1;
our $LOCKED      = 2;

=head2 $processor=LINZ::GNSS::DailyProcessor->new($cfgfile,@args)

Creates a new daily processor based on the configuration file, and 
also any "name=value" items in the list of arguments.  In particular

 config=xxx

may be used to identify an alternative configuration (see LINZ::GNSS::Config)

=cut

sub new {
    my ( $class, $cfgfile, @args ) = @_;
    $cfgfile = abs_path($cfgfile);
    my $cfg = LINZ::GNSS::Config->new( $cfgfile, @args );
    my $s3bucket;
    if ( $cfg->get( 's3_bucket', '' ) ) {
        $s3bucket = new LINZ::GNSS::AwsS3Bucket( config => $cfg );
    }
    $cfg->initLogger();
    my $logger=Log::Log4perl->get_logger('LINZ::GNSS::DailyProcessor');
    my $self =
      { cfg => $cfg, vars => {}, logger => $logger, s3bucket => $s3bucket };
    return bless $self, $class;
}

=head2 $processor->runProcessor( $func )

The processor main routine.  Runs the processing for each date as
specified by the configuration, calling the supplied function to 
implement the process.

If $func is not defined then runs the defaultProcess function.

=cut

sub runProcessor {
    my ( $self, $func ) = @_;
    $func ||= \&defaultProcess;
    $func = $func eq 'test' ? \&testProcess : $func;
    my $start_date = $self->getDate('start_date');
    my $end_date   = $self->getDate('end_date');
    my $increment  = int( $self->get( 'date_increment', 1 ) + 0 );
    $increment > 0 || die "date_increment must be greater than 0\n";
    my $runtime           = time();
    my $maxruntime        = $self->get('max_runtime');
    my $maxdaysprocperrun = $self->get( 'max_days_processed_per_run', '0' );
    $maxruntime = ( $1 * 60 + $2 ) * 60 if $maxruntime =~ /^(\d\d?)\:(\d\d)$/;

    # Set max run time to 1000 days if not specified or 0
    $maxruntime = 1000 * $SECS_PER_DAY if $maxruntime == 0;
    my $endtime = $runtime + $maxruntime;

    my $completefile = $self->get('complete_file');
    my @skipfiles = split( ' ', $self->get( 'skip_files', '' ) );

    my $failfile      = $self->get('fail_file');
    my $retry_max_age_days = $self->get('retry_max_age_days');
    my $retry_max_age=0;
    $retry_max_age = $runtime - $retry_max_age_days*$SECS_PER_DAY if $retry_max_age_days;
    my $retry_interval_days = $self->get('retry_interval_days');

    my $rerun = $self->get( 'rerun', '0' );
    my $procorder = lc( $self->get( 'processing_order', 'backwards' ) );
    my $stopfile            = $self->get( 'stop_file',             '' );
    my $maxconsecutivefails = $self->get( 'max_consecutive_fails', '0' ) + 0;
    my $maxconsecutiveskip =
      $self->get( 'max_consecutive_prerequisite_fails', '0' ) + 0;
    my $faillist = [];
    my $nskip;

    my $runno     = 0;
    my $terminate = 0;

    my @rundates = ();
    for (
        my $date = $end_date ;
        $date >= $start_date ;
        $date -= $SECS_PER_DAY * $increment
      )
    {
        push( @rundates, $date );
    }

    if ( $procorder eq 'forwards' ) {
        @rundates = reverse(@rundates);
    }
    elsif ( $procorder eq 'random' ) {
        @rundates = List::Util::shuffle(@rundates);
    }
    elsif ( $procorder eq 'binary_fill' ) {
        my @dateb = ();
        foreach my $r (@rundates) {
            my $bin =
              unpack( 'B10',
                pack( 'c', int( ( $r - $start_date ) / $SECS_PER_DAY ) ) );
            $bin = reverse($bin);
            push( @dateb, [ $r, $bin ] );
        }
        @dateb    = sort { $a->[1] cmp $b->[1] } @dateb;
        @rundates = map  { $_->[0] } @dateb;
    }

    my $basedir = $self->get('base_directory');
    $self->set( 'basedir', $basedir );

    foreach my $date (@rundates) {
        last if $terminate;

        # Test if have already done maximum number of runs..

        if ( $maxdaysprocperrun > 0 && $runno >= $maxdaysprocperrun ) {
            $self->warn(
                "Daily processor cancelled: max_days_processed_per_run exceeded"
            );
            last;
        }

        # Test for a stop file ..

        if ( $stopfile && -e $stopfile ) {
            $self->error("Stopped by \"stop file\" $stopfile");
            last;
        }

        # Have we run out of time
        if ( time() > $endtime ) {
            $self->warn("Daily processor cancelled: max_runtime expired");
            last;
        }

        my ( $year, $day ) = $self->setYearDay($date);

        my $target = $self->get('target_directory');
        my $targetdir = $basedir ne '' ? "$basedir/$target" : $target;
        $self->set( 'target',    $target );
        $self->set( 'targetdir', $targetdir );

        my $started = 0;
        eval {
            # Set up the processor enviromnent
            $ENV{PROCESSOR_TARGET_DIR} = $targetdir;
            $ENV{PROCESSOR_YEAR}       = $year;
            $ENV{PROCESSOR_DOY}        = $day;

            # Are we skipping this directory?

            my $skipping = 0;
            foreach my $skipfile (@skipfiles) {
                next if !$self->markerFileExists($skipfile);
                $skipping = 1;
                last;
            }
            next if $skipping;

            $self->deleteMarkerFiles( $completefile, $failfile ) if $rerun;

            # Has this directory already been processed?
            next if $self->markerFileExists($completefile);

            # Did it fail but is not ready to rerun
            my $failmtime = $self->markerFileExists($failfile);
            if ($failmtime) {
                if( $date < $retry_max_age )
                {
                    $self->info(sprintf("Not processing $year:$day as failed more than %d days ago (retry_max_age_days)",
                       $retry_max_age_days));
                    next
                }
                if (( time() - $failmtime ) < ( $retry_interval_days * $SECS_PER_DAY ))
                {
                    $self->info(sprintf("Not processing $year:$day as last failed less than %d days ago (retry_interval_days)",
                       $retry_interval_days));
                    next;
                };
                $self->deleteMarkerFiles($failfile);
            }

            # Do we have prerequisite files
            my $skip = 0;
            eval {
                foreach my $prerequisite (
                    split( ' ', $self->get( 'prerequisite_files', '' ) ) )
                {
                    my $pfile = $prerequisite;
                    $pfile =~ s/^\~\//$target\//;
                    my $available = 0;
                    if ( $self->bucket ) {
                        $available = $self->bucket->fileExists($pfile);
                    }
                    else {
                        $pfile = $self->basedir . '/' . $pfile if $pfile;
                        $available = 1 if -e $pfile;
                    }
                    if ( !$available ) {
                        $self->info(
                            "Skipping $year $day as $prerequisite not found");
                        $skip = 1;
                        last;
                    }
                }
            };
            if ($@) {
                my $msg = $@;
                $msg =~ s/\s*$//;
                $self->info("Skipping $year $day: $msg");
                $skip = 1;
            }
            if ($skip) {
                $nskip++;
                if ( $maxconsecutiveskip && $nskip >= $maxconsecutiveskip ) {
                    $self->warn(
'Processing stopped as maximum number of consecutive prerequisite files missing reached'
                    );
                    last;
                }
                next;
            }
            $nskip = 0;

            # Can we get a lock on the file.

            next if $self->locked() == $LOCKED;
            $started = 1;
            $runno++;

            $self->makePath($targetdir);
            next if !$self->lock();
            if ( $self->bucket ) {
                my $syncok =
                  $self->bucket->syncFromBucket( $target, $targetdir );
                if ( !$syncok ) {
                    $self->error("Failed to synchronize data from S3 bucket");
                }
            }
            $self->cleanTarget();
            $self->clearLogBuffers();
            $self->info("Processing $year $day");
            eval {
                my $result = $func->($self);
                if ( !$result ) {
                    die "Daily processing failed\n";
                }
                my $successfile = $self->get( 'test_success_file', '' );
                if ( $successfile && !-d $self->targetdir . '/' . $successfile )
                {
                    die "Test file $successfile not created\n";
                }
                $self->createMarkerFile( $completefile, 1 );
                $self->info("Processing completed successfully");
                $faillist = [];
            };
            my $error = $@;
            if ( $self->bucket ) {
                eval {
                    if ( !$self->bucket->syncToBucket( $targetdir, $target ) ) {
                        $error = "Failed to synchronize data back to S3";
                    }
                };
                if ($@) {
                    $error = "Failed to synchronize back to S3: $@";
                }
            }
            if ($error) {
                $self->warn("Processing failed: $error");
                push( @$faillist, $self->createMarkerFile( $failfile, 1 ) );
                $self->deleteMarkerFiles($completefile)
                  ;    # Just in case failed in sync
                if ( $maxconsecutivefails
                    && scalar(@$faillist) >= $maxconsecutivefails )
                {
                    unlink(@$faillist);
                    $self->warn('Processing stopped as maximum number of consecutive failures reached'
                    );
                    $terminate = 1;
                }
            }
        };
        if ($@) {
            my $msg = $@;
            $msg =~ s/\s*$//;
            $self->warn("Failed $year $day: $msg");
        }

        if( $started )
        {
            # Cleaning out working directories if storing to S3 bucket
            if ( $self->bucket ) {
                remove_tree( $self->targetdir );
            }
            $self->unlock();
        }
    }
}

sub setPcfParams {
    my ( $self, $params, $varhash ) = @_;
    while ( $params =~ /\b(\w+)\=(\S+)/g ) {
        $varhash->{$1} = $2;
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

sub runBernesePcf {
    my ( $self, $pcf, $pcf_params ) = @_;

    # If pcf is blank then need a name
    $pcf ||= $self->get('pcf');

    # Skip if specified as blank or none.
    return 1 if $pcf eq '' || lc($pcf) eq 'none';

# If params is defined then use (which allows overriding default params with none)
    $pcf_params //= $self->get( 'pcf_params', '' );
    my $pcf_cpu = $self->get( 'pcf_cpufile', 'UNIX' );
    my $return = 1;

    require LINZ::BERN::BernUtil;

    # Create a Bernese environment.  Ensure that the SAVEDISK area is redirected
    # to the target directory for the daily processing.

    my ( $targetdir, $environment );
    eval {
        my $zipfiles = $self->get( 'pcf_user_zip_file', '' );
        $targetdir   = File::Spec->rel2abs( $self->targetdir );
        $environment = LINZ::BERN::BernUtil::CreateRuntimeEnvironment(
            CanOverwrite    => 1,
            CustomGenDir    => 1,
            SaveDir         => $targetdir,
            CustomUserFiles => $zipfiles,
            CpuFile         => $pcf_cpu,
        );
        $self->info("Created Bernese runtime environment");
        foreach my $e ( sort( keys(%$environment) ) ) {
            next if $e =~ /^_/;
            $self->info( sprintf( "%s: %s", $e, $environment->{$e} ) );
        }
        my $envfile = $environment->{'CLIENT_ENV'};
        if ( open( my $bh, $envfile ) ) {
            $self->info("Environment");
            while ( my $line = <$bh> ) {
                chomp($line);
                $self->info($line);
            }
            close($bh);
        }
        else {
            $self->fail("Cannot open Bernese environment file");
            return 0;
        }
    };

    if ($@) {
        $self->fail($@);
        $return = 0;
    }

    # If OK, then create a campaign and run the PCF

    my ( $start, $end, $campaign, $campdir );
    if ($return) {
        $start    = $self->timestamp;
        $end      = $start + $SECS_PER_DAY - 1;
        $campaign = LINZ::BERN::BernUtil::CreateCampaign(
            $pcf,
            CanOverwrite        => 1,
            SetSession          => [ $start, $end ],
            MakeSessionFile     => 1,                  # Daily session file
            UseStandardSessions => 1,
        );
        $ENV{PROCESSOR_CAMPAIGN} = $campaign->{JOBID};
        $campdir = ExpandEnv( $campaign->{CAMPAIGN}, 'for campaign directory' );
        $ENV{PROCESSOR_CAMPAIGN_DIR} = $campdir;
        $self->setPcfParams( $pcf_params, $campaign->{variables} );
        $self->info("Campaign dir: $campdir");
        $self->info("Target dir: $ENV{S}");
    }

    if ($return) {
        $return = $self->installPcfCampaignFiles($campaign);
    }

    if ($return) {
        my $result =
          LINZ::BERN::BernUtil::RunPcf( $campaign, $pcf, $environment );
        my $status = LINZ::BERN::BernUtil::RunPcfStatus($campaign);
        $self->info( "Bernese result status: " . $status->{status} );
        $self->{pcfstatus} = $status;

        my $testfile = $self->get( 'pcf_test_success_file', '' );

        if (   $status->{status} eq 'OK'
            && $testfile
            && !-e "$campdir/$testfile" )
        {
            $status->{status} = 'FAIL';
            $self->warn(
                "PCF required output file $testfile not built - run failed");
            $return = 0;
        }
        elsif ( $status->{status} ne 'OK' ) {
            $self->warn(
                join( ': ',
                    "Bernese PCF $pcf failed", $status->{fail_pid},
                    $status->{fail_script},    $status->{fail_prog},
                    $status->{fail_message} )
            );
            $return = 0;
        }
        else {
            $self->info("Bernese PCF $pcf successfully run");
        }

        my $fail = $return ? '' : '_fail';
        my $copydir   = $self->get( 'pcf' . $fail . '_copy_dir',   '' );
        my $copyfiles = $self->get( 'pcf' . $fail . '_save_files', '' );

        if ($copydir) {
            my $copytarget = $targetdir . '/' . $copydir;
            my $copysource = $campdir;
            if ( !File::Copy::Recursive::dircopy( $copysource, $copytarget ) ) {
                $self->error("Failed to copy $copysource to $copytarget");
            }
        }

        if ($copyfiles) {
            foreach my $file ( split( ' ', $copyfiles ) ) {
                $file =~ /(.*?)(?:\:(gzip|compress))?$/i;
                my ( $filename, $compress ) = ( $1, $2 );
                my $src    = "$campdir/$filename";
                my $target = $filename;
                $target =~ s/.*[\\\/]//;
                $target = "$targetdir/$target";
                File::Copy::copy( $src, $target ) if -e $src;
                if ($compress) {
                    my $prog = lc($compress) eq 'gzip' ? 'gzip' : 'compress';
                    my $progexe = File::Which::which($prog);
                    if ($progexe) {
                        system( $progexe, $target );
                    }
                }
            }
        }
    }
    my $save_campaign = $self->get( 'pcf_save_campaign_dir', '' );
    if ( !$save_campaign ) {
        LINZ::BERN::BernUtil::DeleteRuntimeEnvironment($environment);
    }
    $ENV{PROCESSOR_BERNESE_STATUS} = $return;
    return $return;
}

=head2 $processor->installPcfCampaignFiles($campaign_dir)

Install PCF campaign files, installs files into the campaign directory.  

=cut

sub installPcfCampaignFiles {
    my ( $self, $campaign ) = @_;
    my $return = 1;

    my $campfiles = $self->get( 'pcf_campaign_files', '' );
    return if $campfiles eq '';

    # Install campaign files
    my @filespecs = ();
    my $srcdir    = $self->targetdir;

    # If using an S3 bucket then copy any required files from the bucket to
    # a temporary directory

    my $tmpdir;
    eval {
        if ( $self->bucket ) {
            foreach my $cfdef ( split( /\n/, $campfiles ) ) {
                next if $cfdef =~ /^\s*$/;
                $cfdef =~ /^\s*([A-Z]+(?:\s+uncompress)?\s+)(.*?)\s*$/;
                my $prefix    = $1;
                my $filenames = $2;
                die "Invalid campaign file definition $cfdef\n" if ! $prefix;

                # If the filename is not in
                foreach my $filename ( split( ' ', $filenames ) ) {
                    if (   $filename !~ /^\~\//
                        && $filename !~ /[\?\*]/
                        && !-e $filename )
                    {
                        $tmpdir = File::Temp->newdir();
                        my $tmpfile = $filename;
                        $tmpfile =~ s/^.*[\\\/]//;
                        $tmpfile = "$tmpdir/$tmpfile";
                        if ( !$self->bucket->getFile( $filename, $tmpfile ) ) {
                            die "Cannot retrieve pcf_campaign_file $filename from S3\n";
                        }
                        $filename = $tmpfile;
                    }
                    push( @filespecs, $prefix . $filename );
                }
            }
        }
        else {
            @filespecs = split( /\n/, $campfiles );
        }
        LINZ::BERN::BernUtil::InstallCampaignFiles( $campaign, \@filespecs,
            SourceDirectory => $srcdir );
    };
    if( $@ )
    {
        $self->error("Cannot install campaign files: $@");
        $return = 0;
    }
    return $return;
}

=head2 $processor->runProcessorScript($scriptname,$param,$param)

Utility routine for running a script.  This looks for an executable script
matching the supplied name in the configuration directory, then as an
absolute path name, then in the system path.

=cut

sub runProcessorScript {
    my ( $self, $script, @params ) = @_;

    my $cfgdir = $self->cfg->get('configdir');
    my $exe;

    if ( -x "$cfgdir/$script" ) {
        $exe = "$cfgdir/$script";
    }
    elsif ( -x $script ) {
        $exe = $script;
    }
    else {
        $exe = File::Which::which($script);
    }
    my $result;
    if ( -x $exe ) {
        require IPC::Run;
        my ( $in, $out, $err );
        $self->info("Running script $script\n");
        my @cmd = ( $exe, @params );
        IPC::Run::run( \@cmd, \$in, \$out, \$err );
        $self->info($out)  if $out;
        $self->error($err) if $err;
        $result = 1;
    }
    else {
        $self->error("Cannot find script $script\n");
        $result = 0;
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

sub runPerlScript {
    my ( $self, $script, @params ) = @_;

    my $cfgdir = $self->cfg->get('configdir');
    my $scriptfile;

    if ( -e "$cfgdir/$script" ) {
        $scriptfile = "$cfgdir/$script";
    }
    elsif ( -e $script ) {
        $scriptfile = $script;
    }
    my $result;
    if ( -e $scriptfile ) {
        $self->info("Running perl script $script\n");
        $processor = $self;
        @ARGV      = @params;
        do $scriptfile;
        if ($@) {
            $self->error($@);
        }
        $result = 1;
    }
    else {
        $self->error("Cannot find perl script $script\n");
        $result = 0;
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

sub runScripts {
    my ( $self, @scripts ) = @_;
    @scripts = map { split( /\n/, $_ ) } @scripts;
    my $result = 1;
    foreach my $scriptdef (@scripts) {
        my ( $scriptname, @params ) = split( ' ', $scriptdef );
        next if !$scriptname;
        $self->info("Running script: $scriptdef");
        if ( $scriptname =~ /^perl\:/ ) {
            $scriptname = $';
            my $result = $self->runPerlScript( $scriptname, @params );
            last if !$result;
        }
        else {
            my $result = $self->runProcessorScript( $scriptname, @params );
            last if !$result;
        }
    }
    return $result;
}

=head2 $processor->sendNotification( $success, $text )

Sends an email notification reporting the status of the daily processing.  Generally
will be sent at the end of completed days processing.  Depending on the configuration
settings may be sent on success, failure, or both (The message will be sent if there
is a corresponding subject line defined).  

=cut

sub sendNotification {
    my ( $self, $success, $text ) = @_;
    return if $self->get('notifications') eq 'off';
    my $server    = $self->get( 'notification_smtp_server',   '' );
    my $auth_file = $self->get( 'notification_auth_file',     '' );
    my $timeout   = $self->get( 'notification_smtp_timeout',  '30' );
    my $email     = $self->get( 'notification_email_address', '' );
    my $from =
      $self->get( 'notification_from_address', 'daily_processor@linz.govt.nz' );
    my $subject =
      $self->get( 'notification_subject_' . ( $success ? 'success' : 'fail' ),
        $self->get( 'notification_subject', '' ) );
    my $emailtext =
      $self->get( 'notification_text_' . ( $success ? 'success' : 'fail' ),
        $self->get( 'notification_text', '' ) );

    return if $subject eq '';

    # Construct the email message

    my $emailheader = "To: $email\nFrom: $from\nSubject: $subject\n\n";

    $emailtext =~ s/\[text\]/$text/;
    my $info  = join( "\n", @{ $self->{info_buffer} } );
    my $warn  = join( "\n", @{ $self->{warn_buffer} } );
    my $error = join( "\n", @{ $self->{error_buffer} } );

    $emailtext =~ s/\[info\]/$info/;
    $emailtext =~ s/\[warning\]/$warn/;
    $emailtext =~ s/\[error\]/$error/;

    # Read authentication..
    my $user = '';
    my $pwd  = '';
    if ($auth_file) {
        my $af;
        if ( !open( $af, "<", $auth_file ) ) {
            $self->warn("Cannot open notification_auth_file $auth_file");
        }
        else {
            while ( my $line = <$af> ) {
                my @parts = split( ' ', $line );
                my $item = lc( $parts[0] );
                $server = $parts[1] if $item eq 'server';
                $user   = $parts[1] if $item eq 'user';
                $pwd    = $parts[1] if $item eq 'password';
                last if $user ne '' && $pwd ne '';
            }
            close($af);
        }
    }

    # Split out the port from the server name

    my $port;
    ( $server, $port ) = split( /\:/, $server );
    $port //= '25';

    # Have we got a server defined ...

    if ( !$server || lc($server) eq 'none' ) {
        $self->error(
"Cannot send notification as notification_smtp_server is not defined\n"
              . "$emailheader$emailtext" );
        return;
    }

    # Split out recipients

    my @recipients = split( /\,/, $email );
    foreach my $r (@recipients) { $r =~ s/^\s+//; $r =~ s/\s+$//; }

    # Attempt to connect to the server

    eval {
        require Net::SMTP;
        my $smtp = Net::SMTP->new(
            Host    => $server,
            Port    => $port,
            Timeout => $timeout,
        );
        die "Cannot connect to SMTP server $server: $@\n" if !$smtp;
        $smtp->auth( $user, $pwd ) if $user ne '';

        # Set the recipient(s)

        $smtp->mail($from);
        $smtp->to( @recipients, { SkipBad => 1 } );
        $smtp->data();
        $smtp->datasend($emailheader);
        $smtp->datasend($emailtext);
        $smtp->dataend();
        $smtp->quit();
    };
    if ($@) {
        $self->error("Error in sendNotification: $@\n");
        $self->error("Failed to send:\n$emailheader$emailtext");
    }
}

=head2 $processor->defaultProcess

Default routine run when the runProcessor function is called without specifying
a subroutine.  Carries out the following steps:

=over

=item Run prescripts

=item Run bernese PCF

=item Run post scripts

=item Send notifications

=back

If the prescripts fail then the PCF and post scripts are not run.

=cut

sub defaultProcess {
    my ($self) = @_;
    my $ok = 0;
    eval {
        $ok = 1;
        my $prescripts = $self->get( 'prerun_script', 'none' );
        $ok = $self->runScripts($prescripts) if $prescripts !~ /^\s*none\s*$/i;

        if ($ok) {
            my $bernok = 1;
            if ( $self->get('pcf') ) {
                $bernok = $self->runBernesePcf();
            }
            my $status = $bernok ? '_success' : '_fail';
            my $postscripts =
              $self->get( "postrun_script$status",
                $self->get( "postrun_script", "none" ) );
            $ok = $self->runScripts($postscripts)
              if $postscripts !~ /^\s*(none\s*)?$/i;
            $ok = $bernok && $ok;
        }
    };
    if ($@) {
        $ok = 0;
    }
    $self->sendNotification($ok);

    return $ok;
}

=head2 $processor->testProcess

Test process - just writes log the current variables

=cut

sub testProcess {
    my ($self) = @_;
    my $vars = $self->{vars};
    $self->info('Running testProcess');
    foreach my $k ( sort keys %$vars ) {
        my $varstr = sprintf( "  %s=%s", $k, $vars->{$k} );
        $self->info($varstr);
    }
    return 1;
}

=head2 $processor->cfg

Returns the processor configuration file used by the script

=cut

sub cfg {
    my ($self) = @_;
    return $self->{cfg};
}

=head2 my $value=$processor->get($key,$default)

Get a processor variable, either one define using set(), or 
one from the configuration

Also expands results that evaluate to 

  for ## to ## [step #][if exists][need #] 

to values for the range of date offsets from ## to ##

=cut

sub getRaw {
    my ( $self, $key, $default ) = @_;
    my $result =
      exists $self->{vars}->{$key}
      ? $self->{vars}->{$key}
      : $self->cfg->get( $key, $default );
    $result //= '';

    if ( $result =~
/(for\s+(\-?\d+)\s+to\s+(\-?\d+)(?:\s+step\s+(\d+))?(?:\s+if\s+(exists))?(?:\s+need\s+(\d+))?\s+)\S/
      )
    {
        my ( $prefix, $start, $end, $step, $check, $need ) =
          ( $1, $2, $3, $4 // 1, $5 // 0, $6 // 0 );
        my @values    = ();
        my $pfxlen    = length($prefix);
        my $timestamp = $self->get('timestamp');
        $step = 1 if $step < 1;
        eval {
            while ( $start <= $end ) {
                $self->setYearDay( $timestamp + $start * $SECS_PER_DAY );
                $start += $step;
                my $value = $self->cfg->get($key);
                $value = substr( $value, $pfxlen );
                if ($check) {
                    if ( $self->bucket ) {
                        next if !$self->bucket->fileExists($value);
                    }
                    else {
                        my $file = $value;
                        $file = $self->basedir . '/' . $file if $self->basedir;
                        next if !-e $value;
                    }
                }
                push( @values, $value );
            }
        };
        $self->setYearDay($timestamp);
        if ( $need > 0 && scalar(@values) < $need ) {
            die "Not enough files found for $key\n";
        }
        $result = join( ' ', @values );
    }
    return $result;
}

sub get {
    my ( $self, $key, $default ) = @_;
    my $value = $self->getRaw( $key, $default );
    my $maxexpand = 5;
    while ( $value =~ /\$\[\w+\]/ && $maxexpand-- > 0 ) {
        $value =~ s/\$\[(\w+)\]/$self->getRaw($1)/eg;
    }
    return $value;
}

=head2 $processor->set($key,$value)

Set a value that may be used by the processor (or scripts it runs)

=cut

sub set {
    my ( $self, $key, $value ) = @_;
    $self->{vars}->{$key} = $value;
    return $self;
}

=head2 $processor->getDate($key)

Returns a date defined by the configuration file as a timestamp

=cut

sub getDate {
    my ( $self, $key ) = @_;
    return $self->cfg->getDate($key);
}

=head2 $processor->setYearDay($timestamp)

Sets the time used to interpret configuration items (eg directory names)
and returns the year/day (which are also available as $processor->get('yyyy'), 
and $processor->get('ddd').

=cut

sub setYearDay {
    my ( $self, $timestamp ) = @_;
    $self->set( 'timestamp', $timestamp );
    $self->cfg->setTime($timestamp);
    return ( $self->get('yyyy'), $self->get('ddd') );
}

=head2 $processor->logger

Returns a Log::Log4perl logger associated with the processor.

=cut

sub logger {
    my($self) = @_;
    return $self->{'logger'};
}

=head2 $processor->makePath

Creates a path, including all components to it, if it does not exist

=cut

sub makePath {
    my ( $self, $path ) = @_;
    return 1 if -d $path;
    eval {
        my $errors;
        make_path( $path, { error => \$errors } );
    };
    return -d $path ? 1 : 0;
}

=head2 $processor->createMarkerFile($file)

Creates a marker file in the current target directory
Writes any messages from the message buffers to the file

=cut

sub createMarkerFile {
    my ( $self, $file ) = @_;
    my $marker     = $self->target . '/' . $file;
    my $markerfile = $self->targetdir . '/' . $file;
    open( my $mf, ">", $markerfile );

    if ( scalar( @{ $self->{error_buffer} } ) ) {
        print $mf "\nErrors:\n  ";
        print $mf join( "\n  ", @{ $self->{error_buffer} } );
        print $mf "\n";
    }

    if ( scalar( @{ $self->{warn_buffer} } ) ) {
        print $mf "\nWarnings:\n  ";
        print $mf join( "\n  ", @{ $self->{warn_buffer} } );
        print $mf "\n";
    }

    if ( scalar( @{ $self->{info_buffer} } ) ) {
        print $mf "\nProcessing notes:\n  ";
        print $mf join( "\n  ", @{ $self->{info_buffer} } );
        print $mf "\n";
    }

    close($mf);
    if ( $self->bucket ) {
        $self->bucket->putFile( $markerfile, $marker );
    }
    return $marker;
}

=head2 $processor->markerFileExists($file)

Tests if a marker file exists in the current target directory.  Returns
its mtime if it exists, else undef

=cut

sub markerFileExists {
    my ( $self, $file ) = @_;
    my $ok;
    if ( $self->bucket ) {
        my $marker   = $self->target . '/' . $file;
        my $filestat = $self->bucket->fileStats($marker);
        $ok = $filestat->{mtime} if $filestat;
    }
    else {
        my $marker = $self->targetdir . '/' . $file;
        $ok = ( stat($marker) )[9];
    }
    return $ok;
}

=head2 $processor->deletemarkerFiles($file1,$file2,...)

Delete one or more marker files

=cut

sub deleteMarkerFiles {
    my ( $self, @files ) = @_;
    my $target    = $self->target;
    my $targetdir = $self->targetdir;
    foreach my $file (@files) {
        unlink("$targetdir/$file");
        if ( $self->bucket ) {
            $self->bucket->deleteFile("$target/$file");
        }
    }
}

=head2 $processor->lock

Attempt to create a lock file in the current target directory.
Returns 1 if successful or 0 otherwise.

=cut

sub lock {
    my ($self)       = @_;
    my $target       = $self->target;
    my $lockfilename = $self->get('lock_file');
    my $lockfile     = $self->targetdir . '/' . $lockfilename;
    my $lockkey      = "$target/$lockfilename";
    my $lockexpiry   = $self->get('lock_expiry_days');
    my $locked       = $self->locked;
    return 0 if $locked == $LOCKED;
    $self->warn("Overriding expired lock") if $locked == $LOCKEXPIRED;

    # Making local lockfile
    return 0 if !-d $self->targetdir && !$self->makePath( $self->targetdir );
    open( my $lf, ">", $lockfile );
    if ( !$lf ) {
        $self->error("Cannot create lockfile $lockfile\n");
        return 0;
    }
    my $time        = time();
    my $lockmessage = "PID:$$:$time";
    print $lf $lockmessage;
    close($lf);
    $self->bucket->putFile( $lockfile, $lockkey ) if $self->bucket;

    # # Check we actually got the lock - disabled as not really working,
    # # even local file system let alone S3
    # my $check;
    # open($lf,"<",$lockfile);
    # if( $lf )
    # {
    #     $check=<$lf>;
    #     close($lf);
    #     $check =~ s/\s+//g;
    # }
    # if( $check ne $lockmessage )
    # {
    #     $self->warn("Beaten to lockfile $lockfile\n");
    #     return 0;
    # }
    return 1;
}

=head2 $processor->locked

Check if there is a lock on the current target

=cut

sub locked {
    my ($self)       = @_;
    my $target       = $self->target;
    my $lockfilename = $self->get('lock_file');
    my $lockfile     = $self->targetdir . '/' . $lockfilename;
    my $lockkey      = "$target/$lockfilename";
    my $lockexpiry   = $self->get('lock_expiry_days');
    my $time         = time();
    if ( $self->bucket ) {
        my $filestats = $self->bucket->fileStats($lockkey);
        if ($filestats) {
            return $LOCKEXPIRED
              if ( $time - $filestats->{mtime} ) > $lockexpiry * 60 * 60 * 24;
            return $LOCKEXPIRED if $self->get('override_lock','');
            return $LOCKED;
        }
    }
    elsif ( -e $lockfile ) {
        return $LOCKEXPIRED if -M $lockfile > $lockexpiry;
        return $LOCKED;
    }
    return $UNLOCKED;
}

=head2 $processor->unlock

Release the lock on the current target

=cut

sub unlock {
    my ($self)       = @_;
    my $targetdir    = $self->targetdir;
    my $lockfilename = $self->get('lock_file');
    my $lockfile     = "$targetdir/$lockfilename";
    unlink($lockfile);
    if ( $self->bucket ) {
        my $target  = $self->target;
        my $lockkey = "$target/$lockfilename";
        $self->bucket->deleteFile($lockkey);
    }
}

=head2 $processor->cleanTarget

Cleans the files from the current target directory if clean_on_start
is defined.

=cut

sub cleanTarget {
    my ($self) = @_;
    my $cleanfiles = $self->get('clean_on_start');
    return if $cleanfiles eq '' || lc($cleanfiles) eq 'none';
    $cleanfiles = '*' if lc($cleanfiles) eq 'all';

    my $targetdir = $self->target;
    my $lockfile  = $self->get('lock_file');
    $lockfile = "$targetdir/$lockfile";

    foreach my $spec ( split( ' ', $cleanfiles ) ) {
        foreach my $rmf ( glob("$targetdir/$spec") ) {
            next if $rmf eq $lockfile;
            if ( -f $rmf ) {
                unlink($rmf);
            }
            elsif ( -d $rmf ) {
                my $errors;
                remove_tree( $rmf, { error => \$errors } );
            }
        }
    }
}

=head2 $processor->clearLogBuffers

Clears log message buffers which record messages for including in
emails, etc.

=cut

sub clearLogBuffers {
    my ($self) = @_;
    $self->{info_buffer}  = [];
    $self->{warn_buffer}  = [];
    $self->{error_buffer} = [];
}

sub _logMessage {
    my ( $self, $message ) = @_;
    my $prefix=$self->year.': '.$self->day.': '.$self->cfg->name.': ';
    $message = $prefix.$message;
    $prefix .= 'cont) ';
    $message =~ s/\n(.)/\n$prefix$1/g;
    return $message;
}

=head2 $processor->info( $message )

Writes an info message to the log

=cut

sub info {
    my ( $self, $message ) = @_;
    $message = $self->_logMessage($message);
    $self->logger->info($message);
    push( @{ $self->{info_buffer} }, $message );
}

=head2 $processor->warn( $message )

Writes an warning message to the log

=cut

sub warn {
    my ( $self, $message ) = @_;
    $message = $self->_logMessage($message);
    $self->logger->warn($message);
    push( @{ $self->{warn_buffer} }, $message );
}

=head2 $processor->error( $message )

Writes an error message to the log

=cut

sub error {
    my ( $self, $message ) = @_;
    $message = $self->_logMessage($message);
    $self->logger->error($message);
    push( @{ $self->{error_buffer} }, $message );
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

=head $processor->basedir

Returns the current base directory

=cut

sub basedir { return $_[0]->get('basedir'); }

=head2 $processor->target

Returns the current target relative to the base directory

=cut

sub target { return $_[0]->get('target'); }

=head2 $processor->targetdir

Returns the current target directory

=cut

sub targetdir { return $_[0]->get('targetdir'); }

=head2 $processor->bucket

Returns the current S3 bucket

=cut

sub bucket { return $_[0]->{'s3bucket'}; }

=head2 LINZ::GNSS::DailyProcessor::ExampleConfig

Returns an example configuration file as a string.

=cut

sub ExampleConfig {
    open( my $f, __FILE__ ) || return '';
    my $config;
    my $copy = 0;
    while ( my $line = <$f> ) {
        if ( !$copy ) {
            $copy = $line =~ /^\s*\#start_config\s*$/;
        }
        else {
            last if $line =~ /^\s*\#end_config\s*$/;
            $line =~ s/^\s*//;
            $line =~ s/\s*$/\n/;
            $config .= $line;
        }
    }
    close($f);
    return $config;
}

1;

__END__


=head2 Example configuration file

 #start_config
 # Configuration file for the daily processing code
 #
 # This is read and processed using the LINZ::GNSS::Config module.
 # See 'man LINZ::GNSS::Config' for information about string substitution
 #
 # Configuration items may include ${xxx} that are replaced by (in order of
 # preference) 
 #    command line parameters xxx=yyy
 #    other configuration items
 #    environment variable xxx
 #
 # ${configdir} is defined as the directory of this configuration file unless
 # overridden by another option
 # ${pid_#} expands to the process id padded with 0 or left trimmed to # 
 # characters 
 # ${yyyy} and ${ddd} expand to the currently processing day and year, and
 # otherwise are invalid. Also ${mm} and ${dd} for month and day number.
 # Date variables can be offset by a number of days, eg ${yyyy+14}
 #
 # A variable can be substituted with results for multiple days by 
 # the syntax
 #
 # value for -14 to 0 [step #] [if exists] [need #] xxxxxxxxx
 #
 # which will return value with expanded values for the current day -14 to the
 # current day. Note that to use value in another configuration item it must
 # be specified as $[value] rather than with ${value}
 
 # Parameters can have alternative configuration specified by suffix -cfg.
 #
 # For example
 #   start_date -14
 #   start_date-rapid -2
 #
 # Will use -14 for start_date by default, and -2 if the script is run
 # with config=rapid as a suffix on the command line.
 
 # Working directories.  Paths are relative to the location of this
 # configuration file.  
 
 # Location of results files, status files for daily processing.
 # Allows defining a base directory with a subdirectory.  Filename is 
 # relative to base directory (on file system), or relative to S3 prefix
 # Filenames below are relative to the target_directory, unless stated 
 # otherwise.
 
 subdir ${configname}
 base_directory ${configdir}${subdir?/}${subdir}
 target_directory ${yyyy}/${ddd}

 # If files are to be stored on AWS S3, then s3 bucket and s3_dir can be
 # defined.  Otherwise storage is in the local file system.  status files
 # (lock, success, fail) are tested in S3 if it being used)

 # s3_bucket my_bucket
 # s3_prefix ${subdir}${subdir?/}
 
 # Lock file - used to prevent two jobs trying to work on the same task.
 # Lock expiry is the time out for the lock - a lock that is older than this
 # is ignored.
 
 lock_file daily_processing.lock
 lock_expiry_days 0.9
 
 # Completed file - used to flag that the daily processing has run for 
 # the directory successfully.
 # The script will not run on a directory containing this flag.
 
 complete_file daily_processing.complete
 
 # Fail file - used to flag that the processing has run for the directory
 # but not succeeded.  The script may run in that directory again subject
 # to the retry parameters.
 
 fail_file daily_processing.failed

 # Skip files - names of one or more files which signal that the script is not
 # to run.
 
 skip_files
 
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

 # Number of days to subtract for each day processed

 date_increment 1

 # Order of processing.  Options are forwards (from earliest date),
 # backwards (from latest date), binary_fill (fills with scheme that 
 # aims to provide uniform coverage while filling), and random
 # Default is backwards
 
 processing_order backwards
 
 # Limits on number of jobs.  0 = unlimited
 # Maximum number of days is the maximum number of days that will be processed,
 # not including days that don't need processing.
 # Maximum run time defines the latest time after the script is initiated that
 # it will start a new job. It is formatted as hh:mm
 
 max_days_processed_per_run 0
 max_runtime 0:00

 # Maximum consecutive failures.  If this number of consecutive failures occurs then
 # the processing is aborted and the failed status files are removed.  This assumes 
 # that there is some system failure rather than problems with individual days, so
 # leaves the unprocessed days ready to run again. Missing or 0 will accept any number
 # of failures.
 
 max_consecutive_fails 0
 
 # Prerequisite file(s).  If specified then days will be skipped if the
 # specified files do not exist.  Prerequisite files can start be specified
 # as ~/filename to specify a file in the target directory.  Otherwise they
 # are assumed to be relative to the base directory.
 
 prerequisite_files

 max_consecutive_prerequisite_fails

 # Clean on start setting controls which files are removed from the
 # result directory when the jobs starts.  The default is just to remove
 # the job management files (above).
 # 
 # Use "all" to remove all files (except the lock file)
 # Use "none" to leave all other files unaltered
 # Use file names (possibly wildcarded) for specific files.
 
 clean_on_start all

 # Optional name of a file to test for success of the processing run

 test_success_file 

 # File that will stop the script if it exists
 
 stop_file ${base_directory}/${configname}.stop
 
 # =======================================================================
 # The following items are used by the runBernesePcf function
 #
 # Note that when a PCF is run the SAVEDISK environment variable ($S) is set
 # to point to the daily processor target directory.
 #
 # The name of the Bernese PCF to run (use NONE to skip bernese processing)
 
 pcf          PNZDAILY

 # ZIP file(s) containing files to unpack into user directory of the 
 # bernese environment of the job.  (eg compiled with get_pcf_files -z)
 
 pcf_user_zip_file 

 # Campaign files copied before the PCF is run.  These are formatted as
 # one file specification per line (use << EOD for multiple lines).  
 # Each line consists of the campaign directory (eg STA), the optional
 # keyword "uncompress", and the name of one or more files separated by 
 # space characters.  
 # Files names are relative to the base directory unless they are prefixed
 # ~/, in which case they are relative to the target directory.
 # Filenames in the target directory can contain the * and ? wildcards to 
 # copy multiple files.  Wildcards are not supported for files in other
 # directories if S3 is being used for storage.
 #
 # Use the "uncompress" keyword to uncompress gzipped (.gz) 
 # or compress (.Z) files. (Assumes the file names are terminated .gz or .Z)
 #
 # Use the campaign dirctory ZIP to unzip a file containing a set of campaign 
 # files with their explicit directories, eg STA/STATIONS.CLU.  Zip files are 
 # always located on the local file system - S3 is not used.
 # 
 # eg: RAW uncompress TEST${ddd}0.${yy}O.gz
 #     ZIP ${configdir}/station_files.zip
 
 pcf_campaign_files

 # The name of the CPU file that will be used (default is UNIX.CPU)
 
 pcf_cpufile  UNIX
 
 # PCF parameters to override, written as 
 #  xxx1=yyy1 xxx2=yyy2 ...
 
 pcf_params         V_ORBTYP=FINAL V_O=s
 pcf_params-rapid   V_ORBTYP=RAPID V_O=r

 # File used to confirm success of BPE run
 
 pcf_test_success_file

 # Bernese output can be saved either by saving the entire directory to
 # a specified location (relative to the target directory), or by saving 
 # specific file.  In either case there are separate settings depending on
 # whether the outcome was successful or not. 
 
 # Files that will be copied to the target directory if the PCF succeeds
 # File can be suffixed ':gzip' or ':compress' to compress them after copying.
 
 pcf_save_files   BPE/PNZDAILY.OUT
 pcf_fail_save_files

 # Directory into which to copy Bernese campaign files if the PCF fails.
 # (Note: this is relative to the target directory for the daily process.
 # Files are not copied if this is not saved).

 pcf_copy_dir
 pcf_fail_copy_dir fail_data

 # By default the Bernese runtime environment is deleted once the script has finished.
 # Use pcf_save_campaign_dir to leave it unchanged (though it may be overwritten by
 # the campaign for subsequent days)

 pcf_save_campaign_dir 0
 
 # Pre-run and post run scripts.  These are run before and after the 
 # bernese job by the run_daily_processor script.  
 # When these are run the Bernese environment is configured,
 # including the Bernese environment variables.  Three additional variables
 # are defined
 #
 #    PROCESSOR_CAMPAIGN      The Bernese campaign id
 #    PROCESSOR_CAMPAIGN_DIR  The Bernese campaign id (if it has not been deleted)
 #    PROCESSOR_STATUS        (Only applies to the post_run script) either 0 or 1 
 #                            depending on whether the Bernese BPE script ran 
 #                            successfully or not.
 #
 # If a directory is not specified these are assumed to be in the same directory
 # as the configuration file
 
 # =======================================================================
 # The following items may be run by the runScripts function
 #
 # If "none" then no script is run.
 # If more than one script is to be run these can included using a <<EOD heredoc.
 # Each line can specify a script name and parameters.
 # If the script name is prefixed with perl: then it will be run as a perl script 
 # in the context of the processor using the perl "do" function, otherwise it 
 # will run as a normal command.
 # The postrun_script allows success or failure options to implement tasks conditional
 # on the daily processing .  If these are not explicitly
 # defined for the actual status then the generic postrun_script will be used.
 
 prerun_script  none
 postrun_script none
 postrun_script_success none
 postrun_script_fail none

 # =======================================================================
 # Log settings are managed by the LINZ::GNSS::Config module. 
 
 logdir
 logfile
 logsettings info

 # The logsettings can include the string [logfilename] which will be substituted with the name built from
 # logdir and logfile.
 #
 # Instead of a full Log::Log4perl definition logsettings can simply be the log level, one of trace, debug,
 # info, warn, error, or fatal.


 # =======================================================================
 # The following items may be used by the sendNotification function
 #
 # To ignore notifications altogether set to off

 notifications on

 # Email server configuration.  Defines how messages are sent
 # If the server configuration is "none", or a connection to the 
 # server cannot be established, then the notification email is sent
 # to the logger.
 #
 # SMTP server to use (can include :port)
 
 notification_smtp_server

 # File from which server credentials are read.  If not specified then the
 # script will assume that none is required.  The server name/port may also
 # be read from this file if it is defined and exists.
 # The file is formatted as:
 #
 # server the_server
 # user the_user
 # password xxxxxxx

 notification_auth_file

 # Address to send notifications to.  May include multiple address separated
 # by commas

 notification_email_address

 # Address from which the notification is sent

 notification_from_address

 # Notification subject line. Notifications may be sent on success or failure.  The
 # subject will be taken from the status specific message if it is defined, otherwise
 # the generic message.  If both are blank then no message is sent for the status

 notification_subject
 notification_subject_success
 notification_subject_fail

 # Notification message text. Notifications may be sent on success or failure.  The
 # text will be taken from the status specific message if it is defined, otherwise
 # the generic message.  If both are blank then no message is sent for the status
 #
 # The text can include [text] for text included in the sendNotification function
 # call, [info], [warning], and [error] for corresponding information recorded when
 # processing the current day

 notification_text
 notification_text_success
 notification_text_fail

 #end_config
 
=cut
