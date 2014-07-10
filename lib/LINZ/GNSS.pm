
=head1 LINZ::GNSS - Module providing GNSS data functions

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
use Config::General qw(ParseConfig);
use Log::Log4perl qw(:easy);
use Carp;

=head2 LINZ::GNSS::LoadConfig($filename)

Loads the configuration information for the main modules, FileTypeList,
DataCenter, and FileCache.

The default filename is /etc/bernese52/getdata.conf

=cut

our $CoordSysList;

sub _Config
{
    my($filename)=@_;
    if( ! $filename )
    {
        $filename="/etc/bernese52/getdata.conf";
    }
    croak("LINZ::GNSS configuration file $filename cannot be found\n") if ! -e $filename;
    my %config=ParseConfig(-ConfigFile=>$filename,-LowerCaseNames=>1);
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
        elsif( exists $ENV{DEBUG_LINZGNSS} )
        {
            Log::Log4perl->easy_init($DEBUG);
        }
        else
        {
            Log::Log4perl->easy_init($WARN);
        }
    };
    LINZ::GNSS::FileCompression::LoadCompressionTypes( $config );
    LINZ::GNSS::FileTypeList::LoadDefaultTypes( $config );
    LINZ::GNSS::DataCenter::LoadDataCenters( $config );
    LINZ::GNSS::FileCache::LoadCache( $config );
    LINZ::GNSS::RefStation::LoadConfig( $config );
}

=head2 $cslist=LINZ::GNSS::CoordSysList()

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
