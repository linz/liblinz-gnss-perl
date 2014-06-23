use strict;

# Simple hash subtypes

package LINZ::GNSS::FileType::TimeCodes;
use fields qw( timestamp daysecs yyyy yy wwww ww ddd d hh h);

sub new
{
    my($self) = @_;
    $self = fields::new($self) unless ref $self;
    return$self;
}

package LINZ::GNSS::FileType::FileSpec;
use fields qw ( 
    path 
    filename 
    basepath 
    filepath 
    compression 
    type 
    subtype 
    station 
    jobid 
    timestamp 
);

sub new
{
    my($self) = @_;
    $self = fields::new($self) unless ref $self;
    return $self;
}

sub path { return $_[0]->{path}; }
sub filename { return $_[0]->{filename}; }
sub basepath { $_[0]->{basepath} = $_[1] if defined $_[1]; return $_[0]->{basepath}; }
sub compression { return $_[0]->{compression}; }
sub type { return $_[0]->{type}; }
sub subtype { return $_[0]->{subtype}; }
sub station { return $_[0]->{station}; }
sub timestamp { return $_[0]->{timestamp}; }

sub filepath 
{
    my ($self)=@_;
    my $bp=$self->{basepath};
    $bp .= '/' if $bp;
    return $bp.$self->{path}.'/'.$self->{filename};
}
    
sub asString
{
    my ($self,$prefix)=@_;
    my $result = <<EOD;
    LINZ::GNSS::FileSpec:
      path={path}
      filename={filename}
      compression={compression}
      type={type}
      subtype={subtype}
      station={station}
      jobid={jobid}
      timestamp={timestamp}
EOD
    $result=~ s/\{(\w+)\}/$self->{$1}/eg;
    $result=~ s/^$1//mg if $result =~ /^(\s+)/;
    $result=~ s/^/$prefix/emg if $prefix;
    return $result;
}

=head1 LINZ::GNSS::FileType

LINZ::GNSS::FileType class defines a type of GNSS file.  Types are defined by 
a type and subtype, for example obs/daily, orb/final.

Default settings for each file type are loaded from the DataTypes section
of the configuration.  This should be loaded before the DataCenters, each of 
which may overrides some of the default parameters.

Subtypes may define a priority defining the preference for using that subtype 
of the type.  For example final orbits will have a higher priority than rapid orbits.

=cut

package LINZ::GNSS::FileType;
use fields qw(
    type
    subtype
    filename
    path
    frequency
    frequencysecs
    priority
    retention
    use_station
    compression
    latency
    latencysecs
    latencydow
    retry
    retrysecs
    max_delay
    max_delaysecs
    );


use Carp;
use LINZ::GNSS::Time qw(
    $SECS_PER_DAY
    $SECS_PER_HOUR
    $SECS_PER_WEEK
    $GNSSTIME0
);

our $freqmap = {
    'hourly'  => $SECS_PER_HOUR,
    '6hourly' => $SECS_PER_HOUR * 6,
    'daily'   => $SECS_PER_DAY,
    'weekly'  => $SECS_PER_WEEK,
    };

our $hourcodes = {
    '00'=>'a',
    '01'=>'b',
    '02'=>'c',
    '03'=>'d',
    '04'=>'e',
    '05'=>'f',
    '06'=>'g',
    '07'=>'h',
    '08'=>'i',
    '09'=>'j',
    '10'=>'k',
    '11'=>'l',
    '12'=>'m',
    '13'=>'n',
    '14'=>'o',
    '15'=>'p',
    '16'=>'q',
    '17'=>'r',
    '18'=>'s',
    '19'=>'t',
    '20'=>'u',
    '21'=>'v',
    '22'=>'w',
    '23'=>'x',
    };


=head2 LINZ::GNSS::FileType->new($type,$subtype,$configdata)

Create a LINZ::GNSS::FileType from the definition in the config data.  

Parameters are 

=over

=item $type The file type (eg orb, obs, erp)

=item $subtype The subtype (eg rapid, final)

=item $cfgft The configuration data for the type from the configuration file

=back

=cut

sub new
{
    my($self,$type,$subtype,$cfgft) = @_;
    $self = fields::new($self) unless ref $self;
    $type=uc($type);
    $subtype=uc($subtype);
    require LINZ::GNSS::FileTypeList;
    my $default = LINZ::GNSS::FileTypeList->getType($type,$subtype);
    $default ||= {};
    my $filename = $cfgft->{filename} || $default->{filename} ||
        croak "Filename missing for file type $type:$subtype\n";
    my $use_station = $filename =~ /\[ssss\]/i;
    my $path = $cfgft->{path} || $default->{path} ||
        croak "Path missing for file type $type:$subtype\n";
    my $frequency = lc($cfgft->{frequency}) || $default->{frequency} || 'daily';
    croak "Inconsistent frequency for product $type:$subtype\n"
        if exists($default->{frequency}) && $frequency ne $default->{frequency};
    my $frequencysecs = $freqmap->{$frequency};
    $frequencysecs ||
        croak "Invalid frequency $frequency for file type $type:$subtype\n";
    my $priority = $cfgft->{priority} || $default->{priority} || 0;
    my $retention = $cfgft->{retention} || '';
    croak "Invalid retention $retention for file type $type:$subtype\n"
        if $retention !~ /^(?:(\d+)\s+days?)?$/;
    $retention=$1+1;
    my $compression=$cfgft->{compression} || $default->{compression} || '';
    $compression=lc($compression);
    croak "Invalid compression $compression for $type:$subtype" if
        $compression !~ /^(none|hatanaka|compress|gzip|hatanaka\+(compress|gzip))?$/;
    my $latency=$cfgft->{latency} || $default->{latency} || '';
    $latency=lc($latency);
    croak "Invalid latency $latency for $type:$subtype\n" if 
        $latency !~ /^(?:(\d+(?:\.\d+)?)\s+(minutes?|hours?)
                       |(\d+(?:\.\s+)?)\s+(days?)(?:\s+weekly\s+
                       (?:(mon|tues|wedsnes|thurs|fri|satur|sun)day))?)?$/x;
    my $latencysecs=($1+$3+0)*60;
    my $latencydow=-1;
    my $units=$2.$4;
    my $dow=$5;
    $latencysecs *= 60 if $units !~ /^m/;
    $latencysecs *= 24 if $units =~ /^d/;
    if( $dow )
    {
        $latencydow=index('sun mon tue wed thu fri sat',substr($dow,0,3))/4;
    }

    my $retry=$cfgft->{retry} || $default->{retry} || '1 day';
    $retry=lc($retry);
    croak "Invalid retry $retry for $type:$subtype\n" if 
        $retry !~ /^(\d+)\s+(minutes?|hours?|days?)$/;
    my $retrysecs=$1*60;
    $retrysecs *= 60 if $2 !~ /^m/;
    $retrysecs *= 24 if $2 =~ /^d/;

    my $max_delay=$cfgft->{max_delay} || $default->{max_delay} || '30 days';
    $max_delay=lc($max_delay);
    croak "Invalid max_delay $max_delay for $type:$subtype\n" if 
        $max_delay !~ /^(\d+)\s+(minutes?|hours?|days?)$/;
    my $max_delaysecs=$1*60;
    $max_delaysecs *= 60 if $2 !~ /^m/;
    $max_delaysecs *= 24 if $2 =~ /^d/;

    $self->{type}=$type;
    $self->{subtype}=$subtype;
    $self->{filename}=$filename;
    $self->{path}=$path;
    $self->{frequency}=$frequency;
    $self->{frequencysecs}=$frequencysecs;
    $self->{priority}=$priority;
    $self->{retention}=$retention;
    $self->{use_station}=$use_station;
    $self->{compression}=$compression;
    $self->{latency}=$latency;
    $self->{latencysecs}=$latencysecs;
    $self->{latencydow}=$latencydow;
    $self->{retry}=$retry;
    $self->{retrysecs}=$retrysecs;
    $self->{max_delay}=$max_delay;
    $self->{max_delaysecs}=$max_delaysecs;

    return $self;
}

=head2 $copy = $type->clone();

Generates a clone of the file type

=cut

sub clone
{
    my($self) = @_;
    my $copy=fields::new(__PACKAGE__);
    foreach my $k (keys(%$self)){ $copy->{$k}=$self->{$k}; }
    return $copy;
}

=head2 $type->component

Accessor functions for data in a LINZ::GNSS::FileType object. Accessors are:

=over

=item $type->type
The code for the type

=item $type->subtype
The code for the subtype

=item $type->filename
The pattern for the filename

=item $type->path
The pattern for the filepath (directory)

=item $type->compression
The compression used for the files

=item $type->priority
The priority of the subtype

=item $type->frequency
The frequency the data is generated (as a code)

=item $type->latency
The latency of the data (as a code)

=item $type->retention
The preferred retentation period for the data in days

=item $type->retry
The suggested time for retrying a download after it is delayed

=item $type->max_delay
The maximum delay before the data is deemed unavailable

=item $type->use_station 
True if the data is for a specific station (ie station must be 
defined in a data request)

=back

=cut

sub type{ return $_[0]->{type}; }
sub subtype{ return $_[0]->{subtype}; }
sub filename{ return $_[0]->{filename}; }
sub path{ return $_[0]->{path}; }
sub compression{ return $_[0]->{compression}; }
sub priority{ return $_[0]->{priority}; }
sub frequency{ return $_[0]->{frequency}; }
sub latency{ return $_[0]->{latency}; }
sub retention{ return $_[0]->{retention}; }
sub retry { return $_[0]->{retry}; }
sub max_delay { return $_[0]->{max_delay }; }
sub use_station{ return $_[0]->{use_station}; }


=head2 $timecode = $type->timeCodes($time)

Returns a hash with the time codes that may be used in a file name or 
path for the file applicable at the specified time.  The hash includes
an element 'time' that may be used to regenerate the list

=cut

sub timeCodes {
    my ($self,$timestamp) = @_;
    my $increment=$self->{frequencysecs};
    my $time = int(($timestamp-$GNSSTIME0)/$increment)*$increment+$GNSSTIME0;
    my ($year, $yday, $wday, $hour, $min, $sec) = (gmtime($time))[ 5, 7, 6, 2, 1, 0 ];
    $year += 1900;
    my $doy = sprintf( "%03d", $yday + 1 );
    my $woy = sprintf( "%02d", int($yday/7) );
    my $gnss_week = int( ( $time - $GNSSTIME0 ) / $SECS_PER_WEEK );
    my $ystr=sprintf("%04d",$year);
    my $hstr=sprintf("%02d",$hour);
    my $hcode=$hourcodes->{$hstr};
    my $codes=new LINZ::GNSS::FileType::TimeCodes();
    $codes->{timestamp}=$time;
    $codes->{daysecs}=$hour*3600+$min*60+$sec;
    $codes->{yyyy}=$ystr, 
    $codes->{yy}=substr($ystr,2);
    $codes->{wwww}=sprintf("%04d",$gnss_week);
    $codes->{ww}=$woy;
    $codes->{ddd}=$doy, 
    $codes->{d}=sprintf("%01d",$wday);
    $codes->{hh}=$hstr;
    $codes->{h}=$hcode;
    return $codes;
}

=head2 $timecodes = $type->timeCodeSequence($start_epoch,$end_epoch)

Returns an array of time code hashes that will generate file names
required to cover the period from $start_epoch to $end_epoch (in UTC)

=cut

sub timeCodeSequence
{
    my($self,$start_epoch,$end_epoch)=@_;
    my $st=$start_epoch;
    my $et=$end_epoch || $st; 
    croak "Invalid time $start_epoch\n" if ! $st;
    my $inc=$self->{frequencysecs};
    my $tc=$self->timeCodes($st);
    my $sequence=[$tc];
    $et -= ($inc-1);
    while($tc->{timestamp} < $et)
    {
        $tc=$self->timeCodes($tc->{timestamp}+$inc);
        push(@$sequence,$tc);
    }
    return $sequence;
}

=head2 $availabletime,$retry,$failtime = $type->availableTime($request)

Determine when a product is expected to be available. Takes a DataRequest as a
parameter.  

Returns three values:

=over

=item $availableTime 

the time the data is expected to be avaialable (timestamp in seconds)

=item $retry

the suggested interval for retrying if it is late

=item failTime

the time after which missing data is deemed to be unavaiable

=back

=cut 

sub availableTime
{
    my($self,$request) = @_;
    # Get the last element if this is a sequence
    my $timecode = $self->timeCodes( $request->end_epoch );
    my $seconds=$timecode->{timestamp};
    my $daysecs=$timecode->{daysecs};
    # Shift the end time to the end of period, except that 
    # don't move more than the end of the day
    my $freq=$self->{frequencysecs};
    $freq=$SECS_PER_DAY if $freq > $SECS_PER_DAY;
    $daysecs -= $freq while $daysecs > 0;
    $seconds -= $daysecs;
    $seconds--;

    $seconds += $self->{latencysecs};
    if( $self->{latencydow} >= 0 )
    {
        my $wday=$self->{latencydow}-(gmtime($seconds))[6];
        $wday+=7 if $wday < 0;
        $seconds+=$wday*$SECS_PER_DAY;
    }
    return $seconds, $self->{retrysecs}, $seconds+$self->{max_delaysecs};
}

# =head2 $name = $type->_expandName( $name, $jobid, $timeCodes, $station, $stncodes )
# 
# Expand a filename or path to include the components for station,
# jobid and timecode encoded as [ssss], [jobid], and [##] where ##
# is a valid time code, and to replace {xxx} with the value of environment
# variable xxx
#
# Time and station codes follow the case of the [##] string, except that 
# if $stncodes->{uc(station)} is defined then it is used to define the case
# of the station name
#
# =cut 

sub _expandName
{
    my($self,$name,$jobid,$timeCodes,$station,$stncodes)=@_;
    my $stncase = $stncodes ? $stncodes->{uc($station)} : undef;
    $name =~ s/\[(\w+)\]/
        lc($1) eq 'ssss' && $stncase ? $stncase :
        $1 eq 'SSSS' ? uc($station) :
        lc($1) eq 'ssss' ? lc($station) :
        $1 eq 'JOB' ? uc($jobid) :
        lc($1) eq 'job' ? $jobid :
        $1 eq 'TYPE' ? $self->type :
        lc($1) eq 'type' ? lc($self->type) :
        $1 eq 'SUBTYPE' ? $self->subtype :
        lc($1) eq 'subtype' ? lc($self->subtype) :
        $1 eq uc($1) ? uc($timeCodes->{lc($1)}) :
        $timeCodes->{lc($1)}
       /exg; 
    $name =~ s/\$\{(\w+)\}/$ENV{$1} || croak "Environment variable $1 not defined\n"/eg;
    return $name;
}

# Create a file specification for a given set of timecodes, station, and job id.


sub _filespec
{
    my($self,$jobid,$tc,$stn,$stncodes) =@_;

    my $path=$self->_expandName($self->path,$jobid,$tc,$stn,$stncodes);
    my $filename=$self->_expandName($self->filename,$jobid,$tc,$stn,$stncodes);

    my $spec = new LINZ::GNSS::FileType::FileSpec();
    $spec->{path}=$path;
    $spec->{filename}=$filename;
    $spec->{compression}=$self->compression;
    $spec->{type}=$self->type;
    $spec->{subtype}=$self->subtype;
    $spec->{station}=$stn;
    $spec->{jobid}=$jobid;
    $spec->{timestamp}=$tc->{timestamp};
    return $spec;
}

=head2 $files=$type->fileList($request, $stncodes)

Returns a list of files matching a request start and end epoch, and station (if relevant).
May optionally supply a $stncodes hash ref that defines a lookup from upper case station name
to the case to use in the repository.

The files are returned as an array of hashes, each with keys "path", "filename", "compression",
"type", "subtype", and "timestamp".

=cut

sub fileList
{
    my ($self, $request, $stncodes ) = @_;
    my $timeList = $self->timeCodeSequence($request->start_epoch,$request->end_epoch);
    my $files=[];
    my $stn=$request->station;
    my $jobid=$request->jobid;
    my $type=$request->type;
    my $subtype=$request->subtype;
    foreach my $tc (@$timeList)
    {
        push(@$files, $self->_filespec($jobid,$tc,$stn,$stncodes));
    }
    return $files;
}

=head2 $spec=getSpec($srcspec,$stncodes)

Gets the file specification for the file type matching the source specification.
If $stncodes is defined then it is used to determine the case of the station in 
the filename.

=cut

sub getFilespec
{
    my($self,$srcspec,$stncodes) = @_;
    my $tc=$self->timeCodes($srcspec->{timestamp});
    return $self->_filespec($srcspec->{jobid},$tc,$srcspec->{station},$stncodes);
}

1;
