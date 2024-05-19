use strict;
use Math::Trig;

package LINZ::GNSS::RefStation;

=head2 LINZ::GNSS::RefStation

Package just defines some constants.  Actual base class for reference stations is RefStationBase, as
this avoids recursive file loading with use base 'LINZ::GNSS::RefStation'.

=cut



use constant {
    MISSING=>0,
    UNRELIABLE=>1,
    AVAILABLE=>2
};

our @EXPORT_OK=qw(
    MISSING
    UNRELIABLE
    AVAILABLE
    );

our $DefaultDate='2020-01-01';



package LINZ::GNSS::RefStationBase;

=head1 LINZ::GNSS::RefStationBase

LINZ::GNSS::RefStationBase base class for reference stations.

=cut


=head2 my $station=LINZ::GNSS::RefStationBase->new($code, $xyz)

Loads the reference station data from the XML file. 

=cut

use fields qw (
    code
    xyz
    datum
);

sub new 
{
    my ( $self, $code, $xyz, $datum )=@_;
    $self=fields::new($self) unless ref $self;
    $self->{code}=$code;
    $self->{xyz}=$xyz;
    $self->{datum}=$datum;
    return $self;
}

=head2 $station->xxx

Accessor functions for the stations

=over

=item code  The station code

=item site  The station site code (several stations may be on the same site) - defaults to the code

=item priority   The stations priority on the site (lower numbered priority stations are used by preference)

=back

=cut 

sub code { return $_[0]->{code} }
sub xyz { return $_[0]->{xyz} }
sub datum { return $_[0]->{datum} }
sub site { return $_[0]->{code} }
sub priority { return 1 }

sub setXyz
{
    my($self,$xyz,$datum)=@_;
    $self->{xyz}=$xyz;
    $self->{datum}=$datum if $datum;
}

=head2 $station->available($time)

Determines if a station is available at a given time

=cut

sub available { return 1 }

=head2 $station->availability( $start, $end, $use_unreliable )

Determines the percentage availability of a station for the time from $start to $end.  
If $use_unreliable then days that have been excluded in calculating the model (because
they don't fit) will be allowed, otherwise they are excluded.

Returns a value between 0 and 1.

=cut

sub availability { return 1 }

=head2 my $xyz=$station->calc_xyz($date);

Calculates the coordinates at a specific date
(not implemented in base class). Returns XYZ coordinate
as an array ref, and datum as a string (eg ITRF2008).

=cut

sub calc_xyz 
{ 
    my ($self,$date)=@_;
    my $class=ref($self);
    die "Function calc_xyz not implemented in class $class\n";
}

=head1 LINZ::GNSS::RefStationList

LINZ::GNSS::RefStationList is the base class for lists of reference stations.  This 
manages accessing the list of available stations and prioritizing those stations for use
in processing the GNSS data for a given location and date.

=cut


package LINZ::GNSS::RefStationList;
use base qw(Exporter);
use Time::Local;
use Storable;
use LINZ::GNSS::Time qw/$SECS_PER_DAY datetime_seconds seconds_datetime seconds_yearday seconds_decimal_year/;
use LINZ::Geodetic::CoordSysList;
use LINZ::Geodetic::CoordSys;
use Log::Log4perl qw/get_logger/;
use Carp;


# Factors used in ranking stations...
#
our $default_distance_factors=[
    [0,100],
    [30000,85],
    [200000,50],
    [6000000,0],
    ];

our $default_cos_factor=35;

use fields qw(
    refstn_list
    available
    first
    test_date_func
    distance_factors
    cos_factor
    datum
    logger
    cslist
    datumcs
);

our $station_list=undef;

sub LoadConfig
{
    my ($cfg) = @_;
    my $refcfg=$cfg->{refstations};
    if( $refcfg  )
    {
        if( exists $refcfg->{modelfilename})  
        {
            $LINZ::GNSS::RefStationList::station_list = LINZ::GNSS::StationCoordModelList->new($refcfg);
        }
        elsif( exists $refcfg->{coordapiurl} )
        {
            my $sourcelists=$cfg->{sourcelists} || {};
            $LINZ::GNSS::RefStationList::station_list = LINZ::GNSS::StationCoordApiList->new($refcfg, $sourcelists );
        }
        else
        {
            croak("RefStations does not define either ModelFilename or CoordApiUrl");

        }
    }
    #!!! backwards compatibility
    elsif( exists $cfg->{refstationfilename} )
    {
        $LINZ::GNSS::RefStationList::station_list = LINZ::GNSS::StationCoordModelList->new($cfg);
    }

    return $LINZ::GNSS::RefStationList::station_list;
}

sub StationList
{
    return $LINZ::GNSS::RefStationList::station_list;
}

sub new
{
    my($self,$cfg)=@_;

    $self->{logger}=get_logger('LINZ.GNSS.RefStationList');


    if( exists($cfg->{rankdistancefactor}) )
    {
        my $data=$cfg->{rankdistancefactor};
        my $factors=[];
        foreach my $line (split(/\n/,$data))
        {
            push(@$factors,[$1,$2]) if $line=~/^\s*(\d+\.?\d*)\s+(\d+\.?\d*)\s*$/;
        }
        $self->{distance_factors} = $factors;
    }
    if( exists($cfg->{rankcosinefactor}))
    {
        $self->{cos_factor}=$cfg->{rankcosinefactor};
    }
    $self->{refstn_list}=undef;
    $self->{datum}=$cfg->{datum};
    $self->{cslist}=undef;
    $self->{datumcs}={};

    return $self;
}


sub _getRefStations
{
    my ($self)=@_;
    my $class = ref($self);
    die "Implementation error: _getRefStations not defined in $class\n";
}

sub _requiredDates
{
    my($self,%options)=@_;
    my $startdate=0;
    my $enddate=0;
    if( exists( $options{required_dates} ) )
    {
        ($startdate, $enddate)=@{$options{required_dates}};
    }
    elsif( exists( $options{required_date} ) )
    {
        $startdate = $options{required_date};
        $enddate = $startdate + $SECS_PER_DAY - 1;
    }
    return $startdate,$enddate;
}

sub _datumCartesianCoordsys
{
    my ($self, $datum ) = @_;
    return $self->{datumcs}->{$datum} if exists $self->{datumcs}->{$datum};

    $self->{cslist} = LINZ::Geodetic::CoordSysList->newFromCoordSysDef if ! $self->{cslist};
    my $dtm=$self->{cslist}->datum($datum);
    die "Invalid datum $datum requested." if ! defined $dtm;

    my $dtmcs=LINZ::Geodetic::CoordSys->new(LINZ::Geodetic::CARTESIAN,$datum."_XYZ",$dtm,undef,$datum."_XYZ");
    $self->{datumcs}->{datum}=$dtmcs;
    return $dtmcs;
}

sub stations
{
    my ($self,%options)=@_;
    return $self->_getRefStations(%options);
}

=head2 $stnlist=$list->rankStations($srclist,$xyz,%options)

Determines an ordered list of stations to use as reference stations.  Stations
are selected from a supplied list, ordering according to a factor based on their
distance from the test point, and the angle between the vector to the test point 
and the angle to the test point from higher ranked stations.

This function prepares a list of potential stations that is used by NextRankedRefStation.
The usage is:

   my $stationlist = LINZ::GNSS::RefStationList::LoadConfig($cfg);
   $stationlist->rankStations( ... );
   my $used=0;
   my $nused=0;
   while( my $stn=$stnlist->nextRankedRefStation($used))
   {
       
       $used=1 if canuse($stn);
       $nused += $used;
       last if $nused >= $number_required;
   }

=over

=item $srclist

A list of station definitions - each is a hash containing keys code and xyz.
It may also include an optional key parent, which defines the code of a preferred
station at the same location.

=item $xyz

The reference point for which the reference stations are desired

=item %options

Additional options, can include

=over

=item include

A list of codes of stations to include, can be a space delimited string, an
array ref of codes, or a hash ref keyed on codes.  

=item exclude

A list of codes of stations to exclude, can be a space delimited string, an
array ref of codes, or a hash ref keyed on codes.  Applied after the include filter.

=item distance_factors

An array ref of array refs organised as [distance, factor], ordered by increasing distance.
This value is interpolated between distances using a log(distance) scale.  A station is 
rejected if the distance factor calculates to 0. (Default ranges from 100 down to 0)

=item angle_factor

The factor applied to the (1-cosine)/2 where cosine is of the minimum angle 
between an already ranked station and the proposed next station.  (Default 35)

=item required_date=>date

The date at which the station is required supplied as timestamp in seconds.  
Stations not observed during the 24 hours starting at this date will not be included

=item required_dates=>[startdate,enddate]

Alternative to required_date, specifies the start and end of the range in which
the station is required (each as a timestamp in seconds)

=item use_unreliable=>1

If included then data marked as unreliable (days which were excluded in the 
station coordinate modelling) are not included. Default is 0.

=item datum

If this is defined then coordinates will be returned in this datum, otherwise
in the datum defined in the list configuration, and if that is empty, then
in the source coordinate datum.  Datum must be an ITRF, eg ITRF2008.  Assumes
that the coordsys.def file includes a 

=item availability_required=>95

If included then stations available for at least this percentage of the test
range will be included. Default is 100.

=back

=back

=cut

sub _calc_range_factor
{
    my ($dist,$factors) = @_;
    my ($found,$pt0,$pt1);
    return 0 if ! $factors || ! @$factors;
    foreach my $pt (@$factors)
    {
        $pt1=$pt;
        $found=1;
        last if $dist < $pt1->[0];
        $found=0;
        $pt0=$pt1;
    }
    return 0 if ! $found;
    return $pt1->[1] if ! $pt0 || $pt1->[0] <= 1.0;
    return $pt0->[1] if $dist <= 1.0;
    my $d0=$pt0->[0];
    $d0=1.0 if $d0 < 1;
    my $d1=$pt1->[0];
    $d0=log($d0);
    $d1=log($d1);
    $dist=log($dist);
    return ($pt0->[1]*($d1-$dist)+$pt1->[1]*($dist-$d0))/($d1-$d0);
}

sub _formCodeList
{
    my ($coderef)=@_;
    my $clref=ref($coderef);
    return $coderef if $clref eq 'HASH';
    if( ! $clref )
    {
        my @codes=split(' ',$coderef);
        $coderef=\@codes;
    }
    my $codelist={};
    foreach my $c (@$coderef)
    {
        $codelist->{$c}=1;
    }
    return $codelist;
}


sub rankStations() 
{
    my ( $self, $xyz, %options )=@_;

    foreach my $key (keys %options)
    {
        croak("Invalid option $key in RefStation::GetRefStations")
          if $key !~ /^(include|exclude|required_dates?|use_unreliable|availability_required|datum)$/;
    }

    my $logger=$self->{logger};

    my $debug=$logger->is_debug();
    $logger->debug(sprintf("Rank ref stations: basepoint [%.3f,%.3f,%.3f]",
            $xyz->[0],$xyz->[1],$xyz->[2]));

    my $factors = $options{distance_factors} || $self->{distance_factors};
    my $cosfactor= $options{angle_factor} || $self->{cos_factor};
    my $required=exists $options{availability_required} ? 
        $options{availability_required}/100.0 : 0.99999;
    my $use_unreliable=$options{use_unreliable};
    my  ($startdate, $enddate ) = $self->_requiredDates(%options);
    my $testdates=$startdate != 0;
    my $testdatefunc;
    if( $testdates )
    {
        $testdatefunc= sub 
        {
            my ($stn)=@_;
            return $stn->availability($startdate,$enddate,$use_unreliable) >= $required;
        };
    }
    else
    {
        $testdatefunc= sub { return 1; }
    }

    my $datum=$options{datum} || $self->{datum};
    my $datumcs = $self->_datumCartesianCoordsys($datum) if $datum;

    if( $debug )
    {
        $logger->debug("Distance factors:");
        foreach my $d (@$factors)
        {
            $logger->debug(sprintf("  Range %.1f  Factor %.1f",@$d));
        }
        $logger->debug(sprintf("Angle factor: %.1f",$cosfactor));
    }
    if( $testdates && $debug )
    {
        $logger->debug(sprintf("Require %.1f%% availability for dates %s to %s",
                $required*100.0,seconds_datetime($startdate),seconds_datetime($enddate)));
        if( $use_unreliable )
        {
            $logger->debug("Accepting days when time series indicates data unreliable");
        }
    }

    my $refdate=$startdate ? ($startdate+$enddate)/2.0 : datetime_seconds($LINZ::GNSS::RefStation::DefaultDate);

    my $include=_formCodeList($options{include});
    $include=undef if ! %$include;
    my $exclude=_formCodeList($options{exclude});

    my @available=();
    my %sitecode=();

    # Create a list of reference stations that are known to be available
    # Compile into sites (arrays of stations at the same site)
    # Calculate site information from the first station defined for the site

    my $srclist = $self->stations(required_date=>$refdate);
    foreach my $stn ( @$srclist ) 
    {
        my $code=$stn->code();
        next if $include && ! exists $include->{$code};
        next if exists $exclude->{$code};
        my $priority=$stn->priority();
        my $site=$stn->site();
        my $stndata={stn=>$stn,priority=>$priority};
        if( ! exists($sitecode{$site}) )
        {
            $sitecode{$site}={site=>$site,stations=>[]};
            push(@available,$sitecode{$site});
        }
        push(@{$sitecode{$site}->{stations}},$stndata);
    }

    # Order site stations by priority, and find site information from 
    # highest priority station
    
    foreach my $site (values @available)
    {
        my @sorted=sort {$a->{priority} <=> $b->{priority}} @{$site->{stations}};
        $site->{stations}=\@sorted;
        my $stn=$sorted[0]->{stn};
        my ($stnxyz,$stndatum) = $stn->calc_xyz($refdate);
        if( $datum && ($stndatum ne $datum) )
        {
            my $stncs = $self->_datumCartesianCoordsys($stndatum);
            my $epoch = seconds_decimal_year($refdate);
            my $crd=$stncs->coord(@$stnxyz)->as($datumcs,$epoch);
            $stnxyz=[$crd->X,$crd->Y,$crd->Z];
            $stndatum=$datum;
            $stn->setXyz($stnxyz,$stndatum);
        }
        my $dx=$stnxyz->[0]-$xyz->[0];
        my $dy=$stnxyz->[1]-$xyz->[1];
        my $dz=$stnxyz->[2]-$xyz->[2];
        my $dist=sqrt($dx*$dx+$dy*$dy+$dz*$dz);
        my $dfactor=_calc_range_factor($dist,$factors);
        # Reject sites too far away!
        next if $dfactor <= 0.0;

        # Update site priority information
        # dfactor is factor from distance
        # factor is the combination of distance and angle factors, 
        # initially the same as the distance factor
        # dcos is the direction cosine from the test location to the site
        $site->{dfactor}=$dfactor;
        $site->{factor}=$dfactor+$cosfactor;
        $site->{dcos}=[$dx/$dist,$dy/$dist,$dz/$dist];
        $site->{xyz}=$stnxyz;
        $site->{datum}=$stndatum;

        # $logger->debug(sprintf("Site %s: location [%.3f,%.3f,%.3f], distance factor %.3f\n",
        #         $site->{$site}, $stnxyz->[0],$stnxyz->[1],$stnxyz->[2], $dfactor));
    }

    # Remove stations too far away...

    @available = grep { exists($_->{dfactor}) } @available;
    @available=sort {$b->{factor} <=> $a->{factor}} @available;

    $self->{test_date_func}=$testdatefunc;
    $self->{available}=\@available;
    $self->{cos_factor}=$cosfactor;
    $self->{first} = 1;
    return $self;
};


sub nextRankedStation
{
    my($self,$usedlast) = @_;
    my @available = @{$self->{available}};
    my $testdatefunc=$self->{test_date_func};
    my $cosfactor=$self->{cos_factor};
    my $first=$self->{first};
    $self->{first} = 0;

    # If the last station returned was used then remove the site
    # from the available list and update the weighting of the remaining 
    # sites to include a factor reflecting the subtended angle between this site 
    # and the remaining sites.

    $usedlast=1 if not defined($usedlast);
    if( $usedlast && ! $first) 
    {
        my $site=shift(@available);
        my $dcos=$site->{dcos};
        foreach my $s (@available)
        {
            my $scos=$s->{dcos};
            my $cosangle=$dcos->[0]*$scos->[0]+$dcos->[1]*$scos->[1]+$dcos->[2]*$scos->[2];
            my $cfactor=$s->{dfactor}+0.5*$cosfactor*(1.0-$cosangle);
            $s->{factor}=$cfactor if $cfactor < $s->{factor};
        }
        @available=sort {$b->{factor} <=> $a->{factor}} @available;
    }

    # Find the first available station from the highest ranking site...

    my $stn;
    while( @available )
    {
        # Take the lowest valued site
        my $site=$available[0];

        # Find a candidate station at the site that hasn't already been tried, and
        # that passes the date test..
        
        foreach my $istnd (@{$site->{stations}})
        {
            next if $istnd->{used};
            $istnd->{used}=1;
            if( $testdatefunc->($istnd->{stn}) )
            {
                $stn=$istnd->{stn};
                last;
            }
        }

        # If found then return it
        last if $stn;

        # Otherwise move on to the next site
        shift(@available);
    }

    # Update the list of available sites, and return the station

    $self->{available} = \@available;
    return $stn;
}

=head1 LINZ::GNSS::StationCoordModelList

LINZ::GNSS::StationCoordModelList is a coordinate model based list of reference stations.

=cut

package LINZ::GNSS::StationCoordApiList;

=head1 LINZ::GNSS::StationCoordAPIlList

LINZ::GNSS::StationCoordAPIlList is a coordinate model based list of reference stations.

=cut

use LINZ::GNSS::Time qw/$SECS_PER_DAY datetime_seconds seconds_datetime seconds_yearday/;
use LINZ::GNSS::Variables qw/ExpandEnv/;
use LWP::Simple;
use JSON;

use base 'LINZ::GNSS::RefStationList';
use fields qw(
    coordapi_url
    strategies
    codes
    method
    days_before
    days_after
    min_days
    );

=head2 my $stnlist=LINZ::GNSS::StationCoordModelList->new($cfg)

Load reference station information from the configuration file.

Looks for a configuration items RefStationFilename and RefStationCacheDir.

=cut

sub new
{
    my ($self, $cfg, $sourcelists ) = @_;
    $self=fields::new($self) unless ref $self;
    $self->SUPER::new($cfg);
   
    my $url=$ENV{POSITIONZ_COORDAPI_URL};
    $url = ExpandEnv($cfg->{coordapiurl}) if ! defined $url;
    croak("Refstation CoordApiUrl is not defined in the configuration") if ! $url;
    $self->{coordapi_url} = $url;

    my $stglist = $cfg->{strategies};
    croak("RefStation strategies is not defined in the configuration") if ! $stglist;
    my @stglsta=(split(' ',$stglist));
    $self->{strategies}=\@stglsta;

    my $stnlist = $cfg->{codes};
    croak("RefStation codes is not defined in the configuration") if ! $stnlist;
 
    my $codes={};
    foreach my $s (split(' ',$stnlist))
    {
        if( $s =~ /^\@(\w+)$/ )
        {
            my $inclist=lc($1);
            foreach my $slk (keys %$sourcelists)
            {
                if( lc($slk) eq $inclist )
                {
                    foreach my $s (split(' ',$sourcelists->$slk))
                    {
                        $codes->{uc($s)}=$s if $s =~ /^\w+$/;
                    }
                }
            }
        }
        elsif( $s =~ /^\w+$/ )
        {
            $codes->{uc($s)}=$s;
        }
        else
        {
            warn("Refstation code $s is not valid");
        }
    }
    $self->{codes}=$codes;

    $self->{method}=lc($cfg->{method}) || 'mean';
    croak("RefStation method $self->{method} is not valid: must be mean or median") if $self->{method} !~ /^(mean|median)$/;

    foreach my $item ('days_before','days_after')
    {
        my $cfgitem=$item;
        $cfgitem =~ s/(?:^|_)(\w)/uc($1)/eg; # Camel case
        my $days=$cfg->{lc($cfgitem)};
        $days=28 if $days eq '';
        $self->{$item}=$days+0;
        croak("RefStation $item $days is not valid, must be a number > 0") if $self->{$item}==0;
    }

    my $mindays=$cfg->{mindays};
    $mindays=7 if $mindays eq '';
    $self->{min_days}= $mindays+0;
    croak("RefStation MinDays must be a number greater than 0") if $self->{min_days} == 0;

    return $self;
}

sub _getRefStations
{
    my ($self,%options)=@_;
    my  ($startdate, $enddate ) = $self->_requiredDates(%options);
    my $refdate=$startdate ? ($startdate+$enddate)/2.0 : datetime_seconds($LINZ::GNSS::RefStation::DefaultDate);
    my $apistart=$refdate-$self->{days_before}*$SECS_PER_DAY;
    my $apiend=$refdate-$self->{days_after}*$SECS_PER_DAY;
    my $ydstart=sprintf("%04d:%03d",seconds_yearday($apistart));
    my $ydend=sprintf("%04d:%03d",seconds_yearday($apiend));
    my @codes=keys %{$self->{codes}};
    my $endpoint=$self->{coordapi_url}."/strategy_coordinates";
    my $baseurl=$endpoint."?from_epoch=$ydstart&to_epoch=$ydend&method=$self->{method}&min_sessions=$self->{min_days}";

    my %stations=();
    foreach my $stg (@{$self->{strategies}})
    {
        my $url=$baseurl."&strategy=$stg&code=".join("%2B",@codes);
        my $crdjson=get($url);
        next if ! $crdjson;
        my $crddef=decode_json $crdjson;
        my $datum=$crddef->{datum};
        my $fields=$crddef->{fields};
        foreach my $stnfld (@{$crddef->{data}})
        {
            # Create hash from lists of field names and values
            my %stndef;
            @stndef{@$fields}=@$stnfld;
            my $stn=LINZ::GNSS::ApiRefStation->new($stndef{code},
                [$stndef{x},$stndef{y},$stndef{z}],$datum,$refdate);
            $stations{uc($stndef{code})}=$stn;
        }
        my @reqcodes=();
        foreach my $code (@codes)
        {
            push(@reqcodes,$code) if ! exists $stations{$code};
        }
        last if ! @reqcodes;
        @codes=@reqcodes;
    }
    my @refstns=(values %stations);
    return \@refstns;
}


package LINZ::GNSS::ApiRefStation;

=head1 LINZ::GNSS::ApiRefStation

LINZ::GNSS::RefStation manages the definition of a reference station based on a station coordinate model

=cut


=head2 my $station=LINZ::GNSS::ApiRefStation->new($code, $xyz, $datum, $refdate )

Loads the reference station data from the XML file. 

=cut

use LINZ::GNSS::Time qw($SECS_PER_DAY);
use Carp;

use base 'LINZ::GNSS::RefStationBase';
use fields qw(
    refdate
);


sub new
{
    my( $self, $code, $xyz, $datum, $refdate )=@_;
    $self=fields::new($self) unless ref $self;
    $self->SUPER::new($code,$xyz, $datum);
    $self->{refdate}=$refdate;
    return $self;
}

sub calc_xyz
{
    my($self,$date)=@_;
    croak("Cannot revaluate coordinate API station $self->{code} XYZ at different epoch")
        if abs($date-$self->{refdate}) > $SECS_PER_DAY;
    return $self->{xyz}, $self->{datum};
}

package LINZ::GNSS::StationCoordModelList;

=head1 LINZ::GNSS::StationCoordModelList

LINZ::GNSS::StationCoordModelList is a coordinate model based list of reference stations.

=cut

package LINZ::GNSS::StationCoordModelList;

use LINZ::GNSS::Time qw($SECS_PER_DAY);
use LINZ::GNSS::Variables qw/ExpandEnv/;
use File::Path qw(make_path);

use Carp;

use base 'LINZ::GNSS::RefStationList';
use fields qw(
    refstn_dir
    refstn_filename
    refstn_cachedir
    );

=head2 my $stnlist=LINZ::GNSS::StationCoordModelList->new($cfg)

Load reference station information from the configuration file.

Looks for a configuration items RefStationFilename and RefStationCacheDir.

=cut

# Note: backwards compatibility is for getdata.conf without <RefStations> as defined section in the configuration,
# using explicit config items prefixed RefStation.

sub new
{
    my ($self, $cfg) = @_;
    $self=fields::new($self) unless ref $self;
    $self->SUPER::new($cfg);

    #!!! backwards compatibility
    my $filename = $cfg->{modelfilename} || $cfg->{refstationfilename};
    croak("RefStationFilename is not defined in the configuration") if ! $filename;
    croak("Reference station filename in configuration must include [ssss] as code placeholder")
        if $filename !~ /\[ssss\]/; 
   
    my $dir=$ENV{POSITIONZ_REFSTATION_DIR};
    #!!! backwards compatibility
    $dir = ExpandEnv($cfg->{directory} || $cfg->{refstationdir}) if ! defined $dir;
    croak("RefStationDir is not defined in the configuration") if ! $dir;

    $self->{refstn_dir} = $dir;
    $self->{refstn_filename} = "$dir/$filename";

    if( ! $ENV{POSITIONZ_REFSTATION_NO_CACHE})
    {
        my $cache_dir=$ENV{POSITIONZ_REFSTATION_CACHE_DIR};
        #!!! backwards compatibility
        $cache_dir=$cfg->{cachedir} || $cfg->{refstationcachedir} || 'refstn_cache' if ! $cache_dir;
        $cache_dir=$self->{refstn_dir}.'/'.$cache_dir if $cache_dir !~ /^\//;
        $self->{refstn_cachedir} = $cache_dir;
    }
    return $self;
}

=head2 my $filepath=$self->refStationFile($code)

Returns the filepath in which a reference station definition file is stored

=cut

sub refStationFile
{
    my ($self,$code)=@_;
    croak("Reference station directory $self->{refstn_dir} doesn't exist") if ! -d $self->{refstn_dir};
    $code=uc($code);
    my $filepath=$self->{refstn_filename};
    $filepath=~s/\[ssss\]/$code/g;
    return $filepath;
}

=head2 my $filepath=LINZ::GNSS::RefStation::GetStation($code)

Returns the station corresponding to a station code

=cut

sub getStation
{
    my ($self,$code)=@_;
    my $file=$self->refStationFile($code);
    my $station=LINZ::GNSS::SCMRefStation->new($file);
    return $station;
}

=head2 my $list=$self->_getRefStations($filepattern,%options)

Returns an array hash of RefStation objects.  The list is loaded from the files
matching the supplied file name pattern, which should include the string [ssss] 
that will be substituted with the name of the station.  Each matching file will
attempt to be loaded, and added to the list if successful.  

The parameters are the filepattern, and options.  Options can include:

=over

=item cache_dir=>dirname

Defines a directory for caching the interpreted stations

=item required_date=>date

The date at which the station is required supplied as seconds.  Stations not
during the 24 hours starting at this date will not be included

=item required_dates=>[startdate,enddate]

Alternative to required_date, specifies the start and end of the range in which
the station is required

=item use_unreliable=>1

If included then data marked as unreliable (days which were excluded in the 
station coordinate modelling) are not included. Default is 0.

=item availability_required=>95

If included then stations available for at least this percentage of the test
range will be included. Default is 100.

=back

=cut

sub _cachefile
{
    my($self,$filename,$cachedir)=@_;
    $filename=~s/^.*(\/|\\)//;
    $filename=~s/\.xml$//i;
    return $cachedir.'/'.$filename.'.cache';
}

sub _getRefStations
{
    my ($self,%options) = @_;
    my $filename=$self->{refstn_filename};
    my $savelist = 0;
    if(scalar(@_) == 1)
    {
        return $self->{refstn_list} if defined $self->{refstn_list};
        $savelist = 1;
        $filename=$self->{refstn_filename};
        if( $self->{refstn_cachedir} )
        {
            $options{cache_dir} = $self->{refstn_cachedir};
        }
    }
    foreach my $key (keys %options)
    {
        croak("Invalid option $key in RefStation::GetRefStations")
          if $key !~ /^(cache_dir|required_dates?|use_unreliable|availability_required)$/;
    }
    my $required=exists $options{availability_required} ? 
        $options{availability_required}/100.0 : 0.99999;
    my $use_unreliable=$options{use_unreliable};
    my  ($startdate, $enddate ) = $self->_requiredDates(%options);
    my $testdates=$startdate != 0;

    my $cachedir;
    if( exists( $options{cache_dir} ) )
    {
        $cachedir = $options{cache_dir};
        if( ! -d $cachedir )
        {
            my $errval;
            my $umask=umask(0000);
            make_path($cachedir,{error=>\$errval});
            umask($umask);
        }
        croak("Invalid cache directory $cachedir") 
            if ! -d $cachedir;
    }
    my $fileglob=$filename;
    $fileglob =~ s/\[ssss\]/????/;
    my $stations=[];
    foreach my $spmf ( glob($fileglob) )
    {
        my $cachekey;
        my $cachefile;
        if( $cachedir )
        {
            $cachekey=sprintf("%s:%s",(stat($spmf))[7,9]);
            $cachefile=_cachefile($spmf,$cachedir);
            if( -e $cachefile )
            {
                eval
                {
                    my $ref=retrieve($cachefile);
                    if( $ref->{key} eq $cachekey )
                    {
                        my $value=$ref->{value};
                        push(@$stations,$value) if $value;
                        next;
                    }
                    unlink($cachefile);
                };
            }
        }
        my $m;
        eval
        {
            $m=LINZ::GNSS::SCMRefStation->new($spmf);
            if ($testdates)
            {
                my $availability=$m->availability($startdate,$enddate,$use_unreliable);
                next if $availability < $required;
            }
            push(@$stations,$m);
        };
        if( $@ )
        {
            carp($@);
            $m=0;
        }
        if( $cachedir )
        {
            my $ref={key=>$cachekey,value=>$m};
            eval
            {
                store($ref,$cachefile);
            };
        }
    }
    if( $savelist ) { $self->{refstn_list}=$stations; }
    return $stations;
}

package LINZ::GNSS::SCMRefStation;

=head1 LINZ::GNSS::SCMRefStation

LINZ::GNSS::RefStation manages the definition of a reference station based on a station coordinate model

=cut


=head2 my $station=LINZ::GNSS::SCMRefStation->new($filename)

Loads the reference station data from the XML file. 

=cut

use LINZ::GNSS::Time qw($SECS_PER_DAY datetime_seconds);
use XML::Simple;

use Carp;

use base 'LINZ::GNSS::RefStationBase';
use fields qw(
    model
    start_date
    end_date
    site
    priority
    outages
);

use constant {
    MISSING=>LINZ::GNSS::RefStation::MISSING,
    UNRELIABLE=>LINZ::GNSS::RefStation::UNRELIABLE,
    AVAILABLE=>LINZ::GNSS::RefStation::AVAILABLE,
};

sub new
{
    my($self,$filename) = @_;
    my $result= eval
    {
        $self=fields::new($self) unless ref $self;    
        my $xml=XMLin($filename,
           ForceArray=>[
               'outage',
               'parameter',
               'exclude',
               ],
           GroupTags=>{ 
               outages=>'outage', 
               components=>'component',
               excluded=>'exclude',
               },
        );
        my $code=$xml->{code} || die "Code not defined\n";
        my $start_date=datetime_seconds($xml->{start_date}) || 
            die "Start date not defined\n";
        my $version_date=datetime_seconds($xml->{version_date}) || 
            die "Version date not defined\n";
        my $end_date=datetime_seconds($xml->{end_date}) || 0;
        my $site=$xml->{site} || $code;
        my $priority=$xml->{priority} || 0;


        my $cpm=$xml->{coordinate_prediction_model};
        my $model = new LINZ::GNSS::CoordinateModel($cpm);

        my $outages=[];
        my $xo=$xml->{outages};
        if( ref($xo) eq 'ARRAY' )
        {
            foreach my $x (@$xo)
            {
                my $outage={start=>datetime_seconds($x->{start}),end=>datetime_seconds($x->{end}), status=>MISSING };
                push(@$outages,$outage);
            }
        }
        $xo=$cpm->{excluded};
        if( $xo )
        {
            foreach my $x (@$xo)
            {
                my $st=datetime_seconds($x->{date});
                my $outage={start=>$st,end=>$st+$SECS_PER_DAY-1, status=>UNRELIABLE };
                push(@$outages,$outage);
            }
        }
        $self->{outages}=$outages;
        $self->{model}=$model;
        $self->{start_date}=$start_date;
        $self->{end_date}=$end_date;
        $self->{site}=$site;
        $self->{priority}=$priority;
        $self->{outages}=$outages;
        $self->SUPER::new($code,$model->xyz0, $model->datum);
        return $self;
    };
    if( $@ )
    {
        croak("Cannot load reference station file $filename: $@");
    }
    return $result;
}

=head2 $station->xxx

Accessor functions for the stations

=over

=item code  The station code

=item site  The station site code (several stations may be on the same site) - defaults to the code

=item priority   The stations priority on the site (lower numbered priority stations are used by preference)

=back

=cut

sub site { return $_[0]->{site}; }
sub priority { return $_[0]->{priority}; }

=head2 $station->available($time)

Determines if a station is available at a given time

=cut

sub available
{
    my($self,$time) = @_;
    return 0 if $time < $self->{start_date};
    # End date is end of available observations when model generated, so
    # ignore for PositioNZ-PP as we expect to extrapolate.
    # return 0 if $self->{end_date} && $time > $self->{end_date};
    foreach my $outage (@{$self->{outages}})
    {
        return 0 if $time >= $outage->{start} && $time <= $outage->{end};
    }
    return 1;
}

=head2 $station->availability( $start, $end, $use_unreliable )

Determines the percentage availability of a station for the time from $start to $end.  
If $use_unreliable then days that have been excluded in calculating the model (because
they don't fit) will be allowed, otherwise they are excluded.

Returns a value between 0 and 1.

=cut

sub _min
{
    my ($self,$a,$b) = @_;
    return  $a < $b ? $a : $b;
}

sub _max
{
    my ($self,$a,$b) = @_;
    return  $a < $b ? $b : $a;
}
sub availability
{
    my ($self,$start,$end,$use_unreliable) = @_;
    return 0 if $self->{start_date} > $end;
    #return 0 if $self->{end_date} && $self->{end_date} < $start;
    my $total=$end-$start;
    return 0 if $total <= 0;
    my $available=$total;
    $available -= $self->_max(0,$self->{start_date}-$start);
    #$available -= $self->_max(0,$end-$self->{end_date}) if $self->{end_date};
    foreach my $outage (@{$self->{outages}})
    {
        next if $use_unreliable && $outage->{status} == UNRELIABLE;
        my $ostart=$self->_max($start,$outage->{start});
        my $oend=$self->_min($end,$outage->{end});
        $available -= $oend-$ostart if $oend > $ostart;
    }
    return $available/$total;
}

=head2 $station->calc_xyz( $date )

Determine the XYZ coordinate of the station at the specified date

=cut

sub calc_xyz
{
    my($self,$date) = @_;
    return $self->{model}->calc_xyz($date);
}

package LINZ::GNSS::CoordinateModel;
use Carp;
use Math::Trig;
use LINZ::GNSS::Time qw/$SECS_PER_DAY datetime_seconds/;
use LINZ::Geodetic::Ellipsoid;

our $SECSPERYEAR=$SECS_PER_DAY*365.25;

sub new
{
    my($class,$definition)=@_;

    my $date=datetime_seconds($definition->{ref_date});

    my $datum = $definition->{datum} || 'ITRF2008';

    my $xyz=[
        $definition->{X0},
        $definition->{Y0},
        $definition->{Z0},
        ];

    my $grs80=LINZ::Geodetic::Ellipsoid::GRS80();
    my $llh=$grs80->geog($xyz);
    my ($cln,$sln)=(cos(deg2rad($llh->lon)),sin(deg2rad($llh->lon)));
    my ($clt,$slt)=(cos(deg2rad($llh->lat)),sin(deg2rad($llh->lat)));
    my $venu=[
        [-$sln,$cln,0.0],
        [-$slt*$cln,-$slt*$sln,$clt],
        [$clt*$cln,$clt*$sln,$slt],
        ];

    my $self = bless
    {
        xyz0=>$xyz,
        ref_date=>$date,
        venu=>$venu,
        datum=>$datum
    }, $class;

    my $components=[];
    foreach my $comp (@{$definition->{components}})
    {
        next if lc($comp->{exclude}) eq 'yes';
        my $type=$comp->{type};
        my $params={};
        foreach my $prm (@{$comp->{parameter}})
        {
            $params->{$prm->{code}} = $prm->{value};
        }
        my $comp = 
            $type eq 'offset' ? new LINZ::GNSS::CoordinateModel::OffsetFunc($self,$params) :
            $type eq 'velocity' ? new LINZ::GNSS::CoordinateModel::VelocityFunc($self,$params) :
            $type eq 'velocity_change' ? new LINZ::GNSS::CoordinateModel::VelocityStepFunc($self,$params) :
            $type eq 'annual' ? new LINZ::GNSS::CoordinateModel::CyclicFunc($self,1.0,$params) :
            $type eq 'semiannual' ? new LINZ::GNSS::CoordinateModel::CyclicFunc($self,2.0,$params) :
            $type eq 'equipment_offset' ? new LINZ::GNSS::CoordinateModel::StepFunc($self,$params) :
            $type eq 'tectonic_offset' ? new LINZ::GNSS::CoordinateModel::StepFunc($self,$params) :
            $type eq 'slow_slip_ramp' ? new LINZ::GNSS::CoordinateModel::RampFunc($self,$params) :
            $type eq 'slow_slip' ? new LINZ::GNSS::CoordinateModel::ErfFunc($self,$params) :
            $type eq 'exponential_decay' ? new LINZ::GNSS::CoordinateModel::DecayFunc($self,$params) :
            undef;
        croak("Invalid Coordinate Prediction Model component type $type") if ! $comp;
        push(@$components,$comp);
    }
    $self->{components} = $components;

    return $self;
}

sub xyz0 { return $_[0]->{xyz0} }

sub datum { return $_[0]->{datum} }

sub offset_enu
{
    my($self,$date) = @_;
    my $offset=[0,0,0];
    foreach my $comp (@{$self->{components}})
    {
        my $cenu=$comp->offset_enu($date);
        $offset->[0] += $cenu->[0];
        $offset->[1] += $cenu->[1];
        $offset->[2] += $cenu->[2];
    }
    return $offset;
}


sub calc_xyz
{
    my($self,$date) = @_;
    my $denu=$self->offset_enu($date);
    my $xyz0=$self->{xyz0};
    my $venu=$self->{venu};
    my $xyz= [
       $xyz0->[0]+$denu->[0]*$venu->[0]->[0]+$denu->[1]*$venu->[1]->[0]+$denu->[2]*$venu->[2]->[0],
       $xyz0->[1]+$denu->[0]*$venu->[0]->[1]+$denu->[1]*$venu->[1]->[1]+$denu->[2]*$venu->[2]->[1],
       $xyz0->[2]+$denu->[0]*$venu->[0]->[2]+$denu->[1]*$venu->[1]->[2]+$denu->[2]*$venu->[2]->[2]
   ];
   return $xyz, $self->{datum};

}

package LINZ::GNSS::CoordinateModel::OffsetFunc;

sub new
{
    my($class,$model,$params) = @_;
    return bless {
        params=>$params,
        denu=>[ 
            $params->{de_mm}/1000.0,
            $params->{dn_mm}/1000.0,
            $params->{du_mm}/1000.0]
        }, $class;
}

sub offset_enu
{
    my($self,$date) = @_;
    return $self->{denu};
}

package LINZ::GNSS::CoordinateModel::StepFunc;
use LINZ::GNSS::Time;

sub new
{
    my($class,$model,$params) = @_;
    return bless {
        params=>$params,
        denu=>[ 
            $params->{de_mm}/1000.0,
            $params->{dn_mm}/1000.0,
            $params->{du_mm}/1000.0],
        date=>datetime_seconds($params->{date})
        }, $class;
}

sub offset_enu
{
    my($self,$date) = @_;
    return $date > $self->{date} ? $self->{denu} : [0,0,0];
}

package LINZ::GNSS::CoordinateModel::VelocityFunc;

sub new
{
    my($class,$model,$params) = @_;
    my $factor = 1.0/(1000*$LINZ::GNSS::CoordinateModel::SECSPERYEAR);
    return bless {
        params=>$params,
        venu=>[ 
            $params->{ve_mmpy}*$factor,
            $params->{vn_mmpy}*$factor,
            $params->{vu_mmpy}*$factor],
        date=>$model->{ref_date}
        }, $class;
}

sub offset_enu
{
    my($self,$date) = @_;
    $date-=$self->{date};
    my $venu=$self->{venu};
    return [$venu->[0]*$date,$venu->[1]*$date,$venu->[2]*$date];
}

package LINZ::GNSS::CoordinateModel::VelocityStepFunc;
use LINZ::GNSS::Time;

sub new
{
    my($class,$model,$params) = @_;
    my $factor = 1.0/(1000*$LINZ::GNSS::CoordinateModel::SECSPERYEAR);
    return bless {
        params=>$params,
        date=>datetime_seconds($params->{date}),
        venu=>[ 
            $params->{ve_mmpy}*$factor,
            $params->{vn_mmpy}*$factor,
            $params->{vu_mmpy}*$factor],
        }, $class;
}

sub offset_enu
{
    my($self,$date) = @_;
    $date-=$self->{date};
    return [0,0,0] if $date <= 0;
    my $venu=$self->{venu};
    return [$venu->[0]*$date,$venu->[1]*$date,$venu->[2]*$date];
}

package LINZ::GNSS::CoordinateModel::CyclicFunc;

our $YEARTOPHASE=atan2(1.0,1.0)*8/$LINZ::GNSS::CoordinateModel::SECSPERYEAR;

sub new
{
    my($class,$model,$frequency,$params) = @_;
    return bless {
        params=>$params,
        frequency=>$frequency*$YEARTOPHASE,
        denusin=>[ 
            $params->{esin_mm}/1000.0,
            $params->{nsin_mm}/1000.0,
            $params->{usin_mm}/1000.0],
        denucos=>[ 
            $params->{ecos_mm}/1000.0,
            $params->{ncos_mm}/1000.0,
            $params->{ucos_mm}/1000.0],
        date=>$model->{ref_date}
        }, $class;
}

sub offset_enu
{
    my($self,$date) = @_;
    my $phase=($date-$self->{date})*$self->{frequency};
    my $cphase=cos($phase);
    my $sphase=sin($phase);
    my $dsin=$self->{denusin};
    my $dcos=$self->{denucos};
    return [
        $cphase*$dcos->[0]+$sphase*$dsin->[0],
        $cphase*$dcos->[1]+$sphase*$dsin->[1],
        $cphase*$dcos->[2]+$sphase*$dsin->[2]
        ];
}

package LINZ::GNSS::CoordinateModel::RampFunc;
use LINZ::GNSS::Time;

sub new
{
    my($class,$model,$params) = @_;
    return bless {
        params=>$params,
        start_date=>datetime_seconds($params->{start_date}),
        end_date=>datetime_seconds($params->{end_date}),
        denu=>[ 
            $params->{de_mm}/1000.0,
            $params->{dn_mm}/1000.0,
            $params->{du_mm}/1000.0],
        }, $class;
}

sub offset_enu
{
    my($self,$date) = @_;
    my $factor = ($date-$self->{start_date})/($self->{end_date}-$self->{start_date});
    return [0,0,0] if $factor <= 0.0;
    return $self->{denu} if $factor >= 1.0;
    my $denu=$self->{denu};
    return [$denu->[0]*$factor, $denu->[1]*$factor, $denu->[2]*$factor];
}

package LINZ::GNSS::CoordinateModel::DecayFunc;
use LINZ::GNSS::Time;

sub new
{
    my($class,$model,$params) = @_;
    return bless {
        params=>$params,
        start_date=>datetime_seconds($params->{date}),
        factor=>log(2.0)/($params->{halflife_days}*60*60*24),
        denu=>[ 
            $params->{de_mm}/1000.0,
            $params->{dn_mm}/1000.0,
            $params->{du_mm}/1000.0],
        }, $class;
}

sub offset_enu
{
    my($self,$date) = @_;
    return [0,0,0] if $date < $self->{start_date};
    my $factor = 1.0-exp($self->{factor}*($self->{start_date}-$date));
    my $denu=$self->{denu};
    return [$denu->[0]*$factor, $denu->[1]*$factor, $denu->[2]*$factor];
}


package LINZ::GNSS::CoordinateModel::ErfFunc;
use Math::Libm qw(erf);
use LINZ::GNSS::Time;

sub new
{
    my($class,$model,$params) = @_;
    return bless {
        params=>$params,
        mid_date=>datetime_seconds($params->{mid_date}),
        # 3.92 converts 95% of slip to erf parmater
        factor=>3.92/(60*60*24*$params->{duration_days}),
        denu=>[ 
            $params->{de_mm}/1000.0,
            $params->{dn_mm}/1000.0,
            $params->{du_mm}/1000.0],
        }, $class;
}

sub offset_enu
{
    my($self,$date) = @_;
    my $factor = 0.5*(1+erf(($date-$self->{mid_date})*$self->{factor}));
    my $denu=$self->{denu};
    return [$denu->[0]*$factor, $denu->[1]*$factor, $denu->[2]*$factor];
}

1;
