use strict;
use Math::Trig;

=head1 LINZ::GNSS::RefStation

LINZ::GNSS::RefStation manages the definition of a reference sattion, including availability and the coordinate model.

=cut

package LINZ::GNSS::RefStation;
use base qw(Exporter);
use XML::Simple;
use Time::Local;
use File::Path qw(make_path);
use Storable;
use LINZ::GNSS::Time qw/$SECS_PER_DAY datetime_seconds seconds_datetime/;
use Log::Log4perl qw/get_logger/;
use Carp;

use constant {
    MISSING=>0,
    UNRELIABLE=>1,
    AVAILABLE=>2
};

our @EXPORT_OK=qw(
    MISSING
    UNRELIABLE
    AVAILABLE
    GetRefStations
    PrepareRankRefStations
    NextRankedRefStation
    );

# Factors used in ranking stations...
#
our $default_distance_factors=[
    [0,100],
    [30000,85],
    [200000,50],
    [6000000,0],
    ];

our $default_cos_factor=35;

our $refstn_filename;
our $refstn_cachedir;
our $refstn_list;

=head2 LINZ::GNSS::RefStation::LoadConfig

Load reference station information from the configuration file.

Looks for a configuration items RefStationFilename and RefStationCacheDir.

=cut

sub LoadConfig
{
   my ($cfg) = @_;
   if( ! exists $cfg->{refstationfilename} )
   {
       croak("RefStationFilename is not defined in the configuration");
   }

    $refstn_filename = $cfg->{refstationfilename};
    $refstn_filename =~ s/\$\{(\w+)\}/$ENV{$1} || croak "Environment variable $1 not defined for reference station filename\n"/eg;
    if( $refstn_filename !~ /\[ssss\]/)
    {
        croak("Reference station filename in configuration must include [ssss] as code placeholder");
    }

    my $dir=$refstn_filename;
    $dir =~ s/[\\\/][^\\\/]*$//;
    croak("Reference station directory $dir doesn't exist") if ! -d $dir;
    undef($refstn_list);

    if( exists( $cfg->{refstationcachedir} ) )
    {
        $refstn_cachedir = $cfg->{refstationcachedir};
        $refstn_cachedir =~ s/\$\{(\w+)\}/$ENV{$1} || croak "Environment variable $1 not defined for reference station cache directory\n"/eg;
    }

    if( exists($cfg->{rankdistancefactor}) )
    {
        my $data=$cfg->{rankdistancefactor};
        my $factors=[];
        foreach my $line (split(/\n/,$data))
        {
            push(@$factors,[$1,$2]) if $line=~/^\s*(\d+\.?\d*)\s+(\d+\.?\d*)\s*$/;
        }
        $default_distance_factors = $factors;
    }
    if( exists($cfg->{rankcosinefactor}))
    {
        $default_cos_factor=$cfg->{rankcosinefactor};
    }
}

=head2 my $filepath=LINZ::GNSS::RefStation::RefStationFile($code)

Returns the filepath in which a reference station definition file is stored

=cut

sub RefStationFile
{
    my ($code)=@_;
    $code=uc($code);
    my $filepath=$refstn_filename;
    $filepath=~s/\[ssss\]/$code/g;
    return $filepath;
}

=head2 my $list=LINZ::GNSS::RefStation::GetRefStations($filepattern,%options)

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
    my($filename,$cachedir)=@_;
    $filename=~s/^.*(\/|\\)//;
    $filename=~s/\.xml$//i;
    return $cachedir.'/'.$filename.'.cache';
}

sub GetRefStations
{
    my ($filename,%options) = @_;
    my $savelist = 0;
    if( ! @_ )
    {
        return $refstn_list if defined $refstn_list;
        $savelist = 1;
        $filename=$refstn_filename;
        if( $refstn_cachedir )
        {
            $options{cache_dir} = $refstn_cachedir;
        }
    }
    $filename =~ /\[ssss\]/ || croak("Filename in RefStation::GetRefStations must include [ssss]");
    foreach my $key (keys %options)
    {
        croak("Invalid option $key in RefStation::GetRefStations")
          if $key !~ /^(cache_dir|required_dates?|use_unreliable|availability_required)$/;
    }
    my $testdates=0;
    my $startdate=0;
    my $enddate=0;
    my $required=exists $options{availability_required} ? 
        $options{availability_required}/100.0 : 0.99999;
    my $use_unreliable=$options{use_unreliable};
    if( exists( $options{required_dates} ) )
    {
        $testdates=1;
        ($startdate, $enddate)=@{$options{required_dates}};
    }
    elsif( exists( $options{required_date} ) )
    {
        $testdates=1;
        $startdate = $options{required_date};
        $enddate = $startdate + $SECS_PER_DAY - 1;
    }
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
                };
                unlink($cachefile);
            }
        }
        my $m;
        eval
        {
            $m=LINZ::GNSS::RefStation->new($spmf);
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
            store($ref,$cachefile);
        }
    }
    if( $savelist ) { $refstn_list=$stations; }
    return $stations;
}

=head2 $stnlist=LINZ:GNSS::RefStation::PrepareRankRefStations($srclist,$xyz,%options)

Determines an ordered list of stations to use as reference stations.  Stations
are selected from a supplied list, ordering according to a factor based on their
distance from the test point, and the angle between the vector to the test point 
and the angle to the test point from higher ranked stations.

This function prepares a list of potential stations that is used by NextRankedRefStation.
The usage is:

   my $rankdata = LINZ::GNSS::RefStation::PrepareRankRefStations( ... );
   my $used=0;
   my $nused=0;
   while( my $stn=LINZ::GNSS:RefStation::NextRankedRefStation($rankdata,$used))
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

sub PrepareRankRefStations 
{
    my ( $srclist, $xyz, %options )=@_;

    foreach my $key (keys %options)
    {
        croak("Invalid option $key in RefStation::GetRefStations")
          if $key !~ /^(include|exclude|required_dates?|use_unreliable|availability_required)$/;
    }

    my $logger=get_logger('LINZ.GNSS.PrepareRankRefStations');

    my $debug=$logger->is_debug();
    $logger->debug(sprintf("Rank ref stations: basepoint [%.3f,%.3f,%.3f]",
            $xyz->[0],$xyz->[1],$xyz->[2]));

    my $factors = $options{distance_factors} || $default_distance_factors;
    my $cosfactor= $options{angle_factor} || $default_cos_factor;
    my $testdates=0;
    my $startdate=0;
    my $enddate=0;
    my $required=exists $options{availability_required} ? 
        $options{availability_required}/100.0 : 0.99999;
    my $use_unreliable=$options{use_unreliable};
    if( exists( $options{required_dates} ) )
    {
        $testdates=1;
        ($startdate, $enddate)=@{$options{required_dates}};
    }
    elsif( exists( $options{required_date} ) )
    {
        $testdates=1;
        $startdate = $options{required_date};
        $enddate = $startdate + $SECS_PER_DAY - 1;
    }
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

    my $refdate=$startdate ? ($startdate+$enddate)/2.0 : datetime_seconds('2000-01-01');

    my $include=_formCodeList($options{include});
    $include=undef if ! %$include;
    my $exclude=_formCodeList($options{exclude});

    my @available=();
    my %sitecode=();

    # Create a list of reference stations that are known to be available
    # Compile into sites (arrays of stations at the same site)
    # Calculate site information from the first station defined for the site

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
        my $stnxyz = $stn->calc_xyz($refdate);
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

        # $logger->debug(sprintf("Site %s: location [%.3f,%.3f,%.3f], distance factor %.3f\n",
        #         $site->{$site}, $stnxyz->[0],$stnxyz->[1],$stnxyz->[2], $dfactor));
    }

    # Remove stations too far away...

    @available = grep { exists($_->{dfactor}) } @available;
    @available=sort {$b->{factor} <=> $a->{factor}} @available;

    return 
    {
        sites=>\@available,
        testdatefunc=>$testdatefunc,
        cosfactor=>$cosfactor,
        logger=>$logger,
        first=>1,
    }
};


sub NextRankedRefStation
{
    my($rankdata,$usedlast) = @_;
    my @available = @{$rankdata->{sites}};
    my $testdatefunc=$rankdata->{testdatefunc};
    my $cosfactor=$rankdata->{cosfactor};
    my $first=$rankdata->{first};
    $rankdata->{first} = 0;

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

    $rankdata->{sites} = \@available;
    return $stn;
}


sub _min
{
    my ($a,$b) = @_;
    return  $a < $b ? $a : $b;
}

sub _max
{
    my ($a,$b) = @_;
    return  $a < $b ? $b : $a;
}

sub new
{
    my($class,$filename) = @_;
    my $result= eval
    {
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
        my $self= bless
        {
            code=>$code,
            start_date=>$start_date,
            end_date=>$end_date,
            site=>$site,
            priority=>$priority,
        }, $class;

        my $cpm=$xml->{coordinate_prediction_model};
        $self->{model} = new LINZ::GNSS::CoordinateModel($cpm);

        my $outages=[];
        my $xo=$xml->{outages};
        if( $xo )
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

sub code { return $_[0]->{code}; }
sub site { return $_[0]->{site}; }
sub priority { return $_[0]->{priority}; }

=head2 $station->available($time)

Determines if a station is available at a given time

=cut

sub available
{
    my($self,$time) = @_;
    return 0 if $time < $self->{start_date};
    return 0 if $self->{end_date} && $time > $self->{end_date};
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

sub availability
{
    my ($self,$start,$end,$use_unreliable) = @_;
    return 0 if $self->{start_date} > $end;
    return 0 if $self->{end_date} && $self->{end_date} < $start;
    my $total=$end-$start;
    return 0 if $total <= 0;
    my $available=$total;
    $available -= _max(0,$self->{start_date}-$start);
    $available -= _max(0,$end-$self->{end_date}) if $self->{end_date};
    foreach my $outage (@{$self->{outages}})
    {
        next if $use_unreliable && $outage->{status} == UNRELIABLE;
        my $ostart=_max($start,$outage->{start});
        my $oend=_min($end,$outage->{end});
        $available -= $oend-$ostart if $oend > $ostart;
    }
    return $available/$total;
}

=head2 $station->offset_enu( $date )

Determines the east/north/up offset relative to the reference coordinate at a given
date.

=cut

sub offset_enu
{
    my($self,$date) = @_;
    return $self->{model}->offset_enu($date);
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
        venu=>$venu
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
    return [
       $xyz0->[0]+$denu->[0]*$venu->[0]->[0]+$denu->[1]*$venu->[1]->[0]+$denu->[2]*$venu->[2]->[0],
       $xyz0->[1]+$denu->[0]*$venu->[0]->[1]+$denu->[1]*$venu->[1]->[1]+$denu->[2]*$venu->[2]->[1],
       $xyz0->[2]+$denu->[0]*$venu->[0]->[2]+$denu->[1]*$venu->[1]->[2]+$denu->[2]*$venu->[2]->[2]
   ];

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
