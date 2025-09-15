
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

=head2 Environment variables

The LINZ::GNSS modules use a number of environment variables:

=over

=item LINZGNSS_CONFIG_FILE 

Replaces the default configuration file used by scripts (eg getdata.conf)

=item LINZGNSS_LOG_DIR

Overwrites the directory used for logging

=item LINZGNSS_LOG_FILE

Overwrites the full file path used for logging (including the directory)

=item LINZGNSS_DEBUG

Sets the debugging options - can be one of trace, debug, info, warn, error, or fatal.  

=item LINZGNSS_NO_CACHE

Disables caching of GNSS data by data retrieval scripts

=item LINZGNSS_CACHE_DIR

Sets the directory used for caching of GNSS data.  The default is the bernese datapool

=item LINZGNSS_TMP_DIR

Sets the directory for storing scratch files

=item POSITIONZ_REFSTATION_DIR

The directory holding PositioNZ station coordinate models

=item POSITIONZ_REFSTATION_NO_CACHE

Disables caching of PositioNZ reference station information

=item POSITIONZ_REFSTATION_CACHE_DIR

The directory used for caching PositioNZ reference station information

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
use LINZ::GNSS::Variables qw(ExpandEnv);
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
using LINZ::GNSS will emit debug output.  It can take values 
warn, info, and debug.

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
    # Note: ideally this can be merged with LINZ::Config
    my $config=_Config($filename);

    eval
    {
        # DEBUG_LINZGNSS variable deprecated - retained for backward compatibility
        my $debug=$ENV{LINZGNSS_DEBUG} || $ENV{DEBUG_LINZGNSS};
        if( $debug )
        {
            my $level=$DEBUG;
            $level=$WARN if lc($debug) eq 'warn';
            $level=$INFO if lc($debug) eq 'info';
            my $logfile=$ENV{LINZGNSS_LOG_FILE};
            if( ! $logfile && $ENV{LINZGNSS_LOG_DIR} )
            {
                $logfile=$ENV{LINZGNSS_LOG_DIR}.'/linzgnss.log';
            }
            Log::Log4perl->easy_init({level=>$level, file=>">>$logfile"});
        }
        else
        {
            Log::Log4perl->easy_init($WARN);
        }
    };
    # Set up default logger if fail to init
    if( $@ )
    {
        my $errmsg=$@;
        Log::Log4perl->easy_init($DEBUG);
        my $logger=Log::Log4perl::get_logger('LINZ::GNSS');
        $logger->error($errmsg);
    }

    # Create the scratch directory
    my $scratchdir=$ENV{LINZGNSS_TMP_DIR} || $ENV{TMPDIR};
    $scratchdir=ExpandEnv($config->{scratchdir},"for temporary directory") if ! $scratchdir;
    $scratchdir='/tmp' if ! $scratchdir;
    LINZ::GNSS::DataCenter::makepublicpath($scratchdir) ||
        croak "Cannot create LINZ::GNSS scratch directory $scratchdir\n" ;
    $config->{scratchdir} = $scratchdir;

    # Configuration information used by all centres
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
