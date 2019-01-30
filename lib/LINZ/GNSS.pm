
=head1 LINZ::GNSS - Module providing GNSS data functions

This module provides functions to support processing GNSS data.  The majority of the 
scripts relate to selecting and retrieving GNSS reference data (orbits, ERP, reference
station RINEX files).  The module also provides components for handling RINEX and SINEX
files and managing GNSS time formats.

=head2 Scripts 

The module is packed with a number of utility scripts including:

=over

=item get_gnss_data

Script to retrieve GNSS data of various types (orbits, RINEX, etc) using the LINZ::GNSS::FileCache 
module.  

=item gnssdate

Utility to convert date and time between formats used for GNSS data

=item gnss_ftp_mirror 

Script to mirror GNSS data from a remote ftp site. Uses a configuration file to identify 
directories and files to retrieve based on the date.  Handles some automatic renaming of
files as they are downloaded

=item run_daily_processor

Script to manage running daily processing using a configuration file to manage which
days are to be processed.  Processing is run against a directory structure with directories
named according to the day being processed.

=item scan_rinex_files

Scans RINEX files in a directory tree and writes a CSV file summarizing the files found

=item scan_sinex_files

Scans SINEX files in a directory tree and writes a CSV file summarizing the files found.

=item sinex_to_db

Scans one or more SINEX files (including in directory trees) and writes station information
to an SQLite database.

=back

=head2 See Also

=over

=item LINZ::GNSS::DataCenter defines a location from which reference data can be retrieved

=item LINZ::GNSS::FileCache handles a local file cache in which downloaded data is saved

=item LINZ::GNSS::RinexFile limited reading and copying RINEX observation files

=item LINZ::GNSS::SinexFile limited reading and copying SINEX results files

=item LINZ::GNSS::BLQFile reading and writing BLQ ocean loading files

=item LINZ::GNSS::Time functions for converting GNSS time formats

=item /etc/bernese52/getdata.conf  Configuration file for data centres and reference stations

=back

=cut

use strict;

package LINZ::GNSS;
our $VERSION='1.0.2';

use LINZ::GNSS::FileCompression;
use LINZ::GNSS::FileTypeList;
use LINZ::GNSS::DataCenter;
use LINZ::GNSS::FileCache;
use LINZ::GNSS::RefStation;
use LINZ::Geodetic::CoordSysList;
use Sys::Hostname;
use Config::General qw(ParseConfig);
use Log::Log4perl qw(:easy);
use Carp;

=head2 LINZ::GNSS::LoadConfig($filename)

Loads the configuration information for the main modules, FileTypeList,
DataCenter, and FileCache.

The default filename is /etc/bernese52/getdata.conf.  This can be overridden
with the LINZGNSS_CONFIG_FILE environment variable.

Configuration from configfile.`hostname` will be merged into the 
configuration if it exists.

If the environment variable LINZGNSS_DEBUG is set then scripts 
using LINZ::GNSS will emit debug output.


=cut

our $CoordSysList;

sub _Config
{
    my($filename)=@_;
    if( ! $filename )
    {
        $filename=$ENV{LINZGNSS_CONFIG_FILE} || "/etc/bernese52/getdata.conf";
    }
    croak("LINZ::GNSS configuration file $filename cannot be found\n") if ! -e $filename;
    my %config=ParseConfig(-ConfigFile=>$filename,-LowerCaseNames=>1);

    my $configlocal=$filename.'.'.hostname;
    if( -e $configlocal )
    {
        %config=ParseConfig(
            -ConfigFile=>$configlocal,
            -LowerCaseNames=>1,
            -DefaultConfig=>\%config,
            -MergeDuplicateOptions=>1,
            -MergeDuplicateBlocks=>1
            );
    }
    return \%config;
}

sub _expandvar
{
    my($filename)=@_;
    my ($sec,$min,$hour,$day,$mon,$year)=localtime();
    my %timehash=
    (
        second=>sprintf("%02d",$sec),
        minute=>sprintf("%02d",$min),
        hour=>sprintf("%02d",$hour),
        day=>sprintf("%02d",$day),
        month=>sprintf("%02d",$mon+1),
        year=>sprintf("%04d",$year+1900),
    );
    $filename =~ s/\$\{(\w+)\}/
                    exists $ENV{$1} ? $ENV{$1} :
                    exists $timehash{$1} ? $timehash{$1} :
                    $1/exg;
    return $filename;
}

sub LoadConfig
{
    my($filename) = @_;
    my $config=_Config($filename);

    eval
    {
        if( exists $config->{logsettings} )
        {
            my $logcfg=$config->{logsettings};
            my $logfile=_expandvar($config->{logdir}).'/'._expandvar($config->{logfile});
            $logcfg =~ s/\[logfilename\]/$logfile/eg;
            Log::Log4perl->init(\$logcfg);
        }
        else
        {
            Log::Log4perl->easy_init($WARN);
        }
        # DEBUG_LINZGNSS variable deprecated - retained for backward compatibility
        my $debug=$ENV{LINZGNSS_DEBUG} || $ENV{DEBUG_LINZGNSS};
        if( $debug )
        {
            my $level=$DEBUG;
            $level=$WARN if lc($debug) eq 'warn';
            $level=$INFO if lc($debug) eq 'info';
            Log::Log4perl->easy_init( $level );
        }
    };
    # Set up default logger if fail to init
    if( $@ )
    {
        my $errmsg=$@;
        Log::Log4perl->easy_init($DEBUG);
        my $logger=Log::Log4perl::get_logger('LINZ.GNSS');
        $logger->error($errmsg);
    }
    LINZ::GNSS::FileCompression::LoadCompressionTypes( $config );
    LINZ::GNSS::FileTypeList::LoadDefaultTypes( $config );
    LINZ::GNSS::DataCenter::LoadDataCenters( $config );
    LINZ::GNSS::FileCache::LoadCache( $config );
    LINZ::GNSS::RefStation::LoadConfig( $config );
}

=head2 $cslist=LINZ::GNSS::CoordSysList

Returns a LINZ::Geodetic::CoordSysList based on the coordsys.def file defined in the configuration.

=cut

# Note: this routine is imperfect - in that it will use a default configuration file if none has
# been loaded yet..

sub CoordSysList
{
    if( ! $CoordSysList )
    {
        $CoordSysList=LINZ::Geodetic::CoordSysList->newFromCoordSysDef();
    }
    return $CoordSysList;
}

1;
