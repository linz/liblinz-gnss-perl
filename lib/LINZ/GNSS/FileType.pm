use strict;

# Simple hash subtypes

package LINZ::GNSS::FileType::TimeCodes;
use fields qw( timestamp daysecs yyyy yy mm dd wwww ww ddd d hh h);

sub new
{
    my($self) = @_;
    $self = fields::new($self) unless ref $self;
    return$self;
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
    name
    type
    subtype
    filename
    path
    frequency
    frequencysecs
    priority
    retention
    expires
    use_station
    compression
    latency
    latencysecs
    supply_frequency
    supplyfreqsecs
    retry
    retrysecs
    max_delay
    max_delaysecs
    valid_before
    valid_after
    );


use Carp;
use LINZ::GNSS::FileSpec;
use LINZ::GNSS::Time qw(
    $SECS_PER_DAY
    $SECS_PER_HOUR
    $SECS_PER_WEEK
    $GNSSTIME0
    datetime_seconds
);
use LINZ::GNSS::Variables qw(ExpandEnv);

our $freqmap = {
    'hourly'  => $SECS_PER_HOUR,
    '3hourly' => $SECS_PER_HOUR * 3,
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


our $DefaultRetention=100;


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
    # The default is either the first matching type/subtype in the filetype list or else empty.
    my $default = LINZ::GNSS::FileTypeList->getType($type,$subtype);
    $default = $default ? $default->[0] : {};

    my $name = $cfgft->{name} || $default->{name} || 'No description available'; 

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
        if $retention !~ /^(?:\-?(\d+)\s+days?)?$/;
    $retention=$1 ? $1+1 : $LINZ::GNSS::FileType::DefaultRetention;

    my $expires = $cfgft->{expires} || '';
    croak "Invalid expires $expires for file type $type:$subtype\n"
        if $expires !~ /^(?:(\d+)\s+days?)?$/;
    $expires=0;
    if( $1 )
    {
        $expires=$1+0;
        $retention=$expires if $expires && $expires < $retention;
    }

    my $compression=$cfgft->{compression} || $default->{compression} || 'auto';
    $compression=lc($compression);
    croak "Invalid compression $compression for $type:$subtype" if
        ! LINZ::GNSS::FileCompression::IsValidCompression($compression);

    my $supplyfreq=lc($cfgft->{supply_frequency}) || $default->{supply_frequency}
          || $frequency;
    my $supplyfreqsecs = $freqmap->{$supplyfreq};
    $supplyfreqsecs ||
        croak "Invalid supply_frequency $supplyfreq for file type $type:$subtype\n";

    my $latency=$cfgft->{latency} || $default->{latency} || '';
    $latency=lc($latency);
    croak "Invalid latency $latency for $type:$subtype\n" if 
        $latency !~ /^(?:(\-?\d+(?:\.\d+)?)\s+(minutes?|hours?|days?))?$/x;
    my $latencysecs=($1+0)*60;
    my $units=$2;
    $latencysecs *= 60 if $units !~ /^m/;
    $latencysecs *= 24 if $units =~ /^d/;

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

    my $valid_before=$cfgft->{valid_before} || $default->{valid_before} || '';
    if( $valid_before )
    {
        eval
        {
            $valid_before = datetime_seconds($valid_before);
        };
        if( $@ )
        {
            croak "Invalid valid_before $valid_before for $type:$subtype\n";
        }
    }
    my $valid_after=$cfgft->{valid_after} || $default->{valid_after} || '';
    if( $valid_after )
    {
        eval
        {
            $valid_after = datetime_seconds($valid_after);
        };
        if( $@ )
        {
            croak "Invalid valid_after $valid_after for $type:$subtype\n";
        }
    }   
    $self->{type}=$type;
    $self->{subtype}=$subtype;
    $self->{name}=$name;
    $self->{filename}=$filename;
    $self->{path}=$path;
    $self->{frequency}=$frequency;
    $self->{frequencysecs}=$frequencysecs;
    $self->{priority}=$priority;
    $self->{retention}=$retention;
    $self->{expires}=$expires;
    $self->{use_station}=$use_station;
    $self->{compression}=$compression;
    $self->{latency}=$latency;
    $self->{latencysecs}=$latencysecs;
    $self->{supply_frequency}=$supplyfreq;
    $self->{supplyfreqsecs}=$supplyfreqsecs;
    $self->{retry}=$retry;
    $self->{retrysecs}=$retrysecs;
    $self->{max_delay}=$max_delay;
    $self->{max_delaysecs}=$max_delaysecs;
    $self->{valid_before}=$valid_before;
    $self->{valid_after}=$valid_after;

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

=item $type->name
The name (description) of the type 

=item $type->filename
The pattern for the filename

=item $type->setFilename($pattern)
Set the pattern for the filename

=item $type->path
The pattern for the filepath (directory)

=item $type->setPath( $path )
Set the pattern for the filepath (directory)

=item $type->compression
The compression used for the files

=item $type->setCompression( $compression )
Sets the compression used for the files

=item $type->priority
The priority of the subtype

=item $type->frequency
The frequency the data is generated (as a code)

=item $type->frequencysecs
The frequency the data is generated in seconds

=item $type->latency
The latency of the data (as a code)

=item $type->retention
The preferred retention period for the data in days

=item $type->expires
The maximum age for which a file is considered valid. (Used for 
rapid products for which the filename is overwritten with each
new version of the product).

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
sub name{ return $_[0]->{name}; }
sub filename{ return $_[0]->{filename}; }
sub setFilename{ $_[0]->{filename}=$_[1]; }
sub path{ return $_[0]->{path}; }
sub setPath{ $_[0]->{path}=$_[1]; }
sub compression{ return $_[0]->{compression}; }
sub setCompression{ $_[0]->{compression} = $_[1]; }
sub priority{ return $_[0]->{priority}; }
sub frequency{ return $_[0]->{frequency}; }
sub frequencysecs{ return $_[0]->{frequencysecs}; }
sub latency{ return $_[0]->{latency}; }
sub retention{ return $_[0]->{retention}; }
sub expires{ return $_[0]->{expires}; }
sub retry { return $_[0]->{retry}; }
sub max_delay { return $_[0]->{max_delay }; }
sub valid_before { return $_[0]->{valid_before}; }
sub valid_after { return $_[0]->{valid_after}; }
sub use_station{ return $_[0]->{use_station}; }

=head2 

=head2 $timecode = $type->timeCodes($time)

Returns a hash with the time codes that may be used in a file name or 
path for the file applicable at the specified time.  The hash includes
an element 'time' that may be used to regenerate the list

=cut

sub timeCodes {
    my ($self,$timestamp) = @_;
    my $increment=$self->{frequencysecs};
    my $time = int(($timestamp-$GNSSTIME0)/$increment)*$increment+$GNSSTIME0;
    my ($year, $month, $mday, $yday, $wday, $hour, $min, $sec) = (gmtime($time))[ 5, 4, 3, 7, 6, 2, 1, 0 ];
    $year += 1900;
    $month++;
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
    $codes->{mm}=sprintf("%02d",$month);
    $codes->{dd}=sprintf("%02d",$mday);
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

the time the data is expected to be available (timestamp in seconds).
Returns 0 if the file expires and will never be available for the requested
time or the the requested time is outside the valid_before and valid_after
limits.

=item $retry

the suggested interval for retrying if it is late

=item failTime

the time after which missing data is deemed to be unavailable

=back

=cut 

sub availableTime
{
    my($self,$request, $now) = @_;
    # Get the last element if this is a sequence

    $now ||= time();
    if( $self->valid_before && $request->end_epoch > $self->valid_before )
    {
        return 0,0,0;
    }
    if( $self->valid_after && $request->start_epoch < $self->valid_after )
    {
        return 0,0,0;
    }
    my $timestamp=$request->end_epoch;
    my $increment=$self->{supplyfreqsecs};

    # Find the beginning of the next supply period (end of this one)
    my $seconds = (int(($timestamp-$GNSSTIME0)/$increment)+1)*$increment+$GNSSTIME0;
    $seconds += $self->{latencysecs};

    # Has the file expired...
    if( $self->expires )
    {
        my $failtime=$request->start_epoch+$self->expires*$SECS_PER_DAY;
        if( $failtime < $now )
        {
            return 0,0,$failtime;
        }
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
    $name=ExpandEnv($name,"for filetype ".$self->{name});
    return $name;
}

# Create a file specification for a given set of timecodes, station, and job id.


sub _filespec
{
    my($self,$jobid,$tc,$stn,$stncodes) =@_;

    my $expandfunc=sub { return $self->_expandName($_[0],$jobid,$tc,$stn,$stncodes)};

    my $path=$expandfunc->($self->path);
    my $filename=$expandfunc->($self->filename);

    my $spec = new LINZ::GNSS::FileSpec(
        path=>$path,
        filename=>$filename,
        compression=>$self->compression,
        type=>$self->type,
        subtype=>$self->subtype,
        station=>$stn,
        jobid=>$jobid,
        timestamp=>$tc->{timestamp},
        expandfunc=>$expandfunc,
    );
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
