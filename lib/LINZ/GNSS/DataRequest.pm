use strict;

package LINZ::GNSS::DataRequest;
use base qw(Exporter);
use fields qw(
    id
    jobid
    type
    subtype
    start_epoch
    end_epoch
    use_station
    station
    status
    available_date
    supplied_subtype
    status_message
    );

use constant {
    REQUESTED=>'REQU',
    COMPLETED=>'COMP',
    UNAVAILABLE=>'UNAV',
    PENDING=>'PEND',
    DELAYED=>'DELA',
    INVALID=>'INVL',
    };

our @EXPORT_OK=qw(
    REQUESTED
    COMPLETED
    UNAVAILABLE
    PENDING
    DELAYED
    INVALID
    );

use Carp;
use LINZ::GNSS::FileTypeList;
use LINZ::GNSS::Time qw(seconds_datetime);


=head1 LINZ::GNSS::DataRequest

Defines a request for GNSS data files.  The request is formulated in terms of the 
data type, start and end epoch, and for RINEX files, the station.  It may also include
the data subtype.  

The default data subtype is the highest priority subtype of the type.  The subtype may be 
specified with a "+" to include the specified subtype or a higher priority type.  For example
orbit data can be specified as "rapid+", meaning final or rapid orbits.  The final orbits 
will be used if available, otherwise the rapid orbits may be used.

The DataRequest module can export status constants as follows:

=over

=item REQUESTED

The request has been constructed but not processed in any way

=item COMPLETED

The request has been filled

=item PENDING

The item is not available yet but is expected to become available

=item UNAVAILABLE

The request cannot be filled

=item DELAYED

The request is pending but was not found when it was expected to be ready for downloading

=item INVALID

The request is invalid in some way

=back

=cut

=head2 LINZ::GNSS::DataRequest->new($jobid,$type,$subtype,start,$end,$station)

Create a new data request.  The request parameters are

=over

=item $jobid

The id of the job requesting the data

=item $type 

The data type required (eg "orb")

=item $subtype 

The subtype required.  Empty to select the highest priority currently available
subtype (but not subtypes with priority 0). Append "+" to the subtype to select the highest
priority available subtype with priority greater than equal to the selected subtype.

=item $start 

The start epoch of data required (as a unix timestamp)

=item $end 

The final epoch of data required (as a unix timestamp)

=item $station 

The four character code for the station for which data is required. 
(Only applicable for data from stations such as RINEX data)

=back

=cut

sub new 
{
    my($self,$jobid,$type,$subtype,$start,$end,$station) = @_;
    $self=fields::new($self) unless ref $self;
    $type=uc($type);
    $subtype=uc($subtype);
    $station=uc($station);
    $start = int($start);
    $end = int($end);
    # Check that there is at least one matching type/subtype defined...
    my ($ftype)=LINZ::GNSS::FileTypeList->getTypes($type,$subtype);
    croak "Invalid data type requested $type:$subtype\n" if ! $ftype;
    croak "Data request doesn't specify station\n" if $ftype->use_station and ! $station;
    croak "Invalid station $station requested\n" if $ftype->use_station and $station !~ /^\w{4}/;
    $self->{id}=0;
    $self->{jobid}=$jobid;
    $self->{type}=$type;
    $self->{subtype}=$subtype;
    $self->{start_epoch}=int($start);
    $self->{end_epoch}=int($end);
    $self->{use_station}=$ftype->use_station;
    $self->{station}=$ftype->use_station ? $station : '';
    # Request result status
    $self->{status}=REQUESTED;
    $self->{status_message}=undef;
    $self->{available_date}=undef;
    $self->{supplied_subtype}=undef;
    return $self;
}

=head2 $request->component

Accessor functions for components of the request.  Available components are:

=over

=item jobid

The id of the job requesting the data

=item type 

The requested type

=item subtype 

The requested subtype

=item start_epoch 

The requested start epoch

=item end_epoch 

The requested end epoch

=item use_station 

True if the request is for station specific data

=item station 

The requested station

=item status 

The request status 

=item status_message 

A text message relating to the current status

=item available_date 

The date at which the request is expected to be available if the status DELAYED or PENDING

=item supplied_subtype

The actual subtype of the data supplied

=back

=cut

sub id { return $_[0]->{id}; }
sub jobid { return $_[0]->{jobid}; }
sub type { return $_[0]->{type}; }
sub subtype { return $_[0]->{subtype}; }
sub start_epoch { return $_[0]->{start_epoch}; }
sub end_epoch { return $_[0]->{end_epoch}; }
sub use_station { return $_[0]->{use_station}; }
sub station { return $_[0]->{station}; }
sub status { return $_[0]->{status}; }
sub status_message { return $_[0]->{status_message}; }
sub available_date { return $_[0]->{available_date}; }
sub supplied_subtype { return $_[0]->{supplied_subtype}; }

=head2 $request->asString()

Returns a string representing the request

=cut

sub asString
{
    my ($self,$prefix)=@_;
    my $template=<<EOD;
    LINZ::GNSS::DataRequest:
      id={id}
      jobid={jobid}
      type={type}
      subtype={subtype}
      start_epoch={start_epoch}
      end_epoch={end_epoch}
      station={station}
      status={status}
      status_message={status_message}
      available_date={available_date}
      supplied_subtype={supplied_subtype}
EOD
    $template=~ s/\{(\w+_(?:epoch|date))\}/seconds_datetime($self->{$1})/eg;
    $template=~ s/\{(\w+)\}/$self->{$1}/eg;
    $template=~ s/^$1//mg if $template =~ /^(\s+)/;
    $template=~ s/^/$prefix/emg if $prefix;
    return $template;
}

=head2 $request->reqid

Return a unique string defining the request, used as an external identifier in the 
request database.

=cut

sub reqid
{
    my($self) = @_;
    my $reqid='{jobid}:{type}:{subtype}:{station}:{start_epoch}:{end_epoch}';
    $reqid=~ s/\{(\w+)\}/$self->{$1}/eg;
    return $reqid;
}
    
=head2 $request->setStatus( $status, $message, $available )

Set the request status, message, and optionally when it will be available.
The $request->setComplete function must be called for setting the status to
complete in order to specify the data subtype used (as requests may be 
filled by several different subtypes).

=cut

sub setStatus
{
    my($self,$status,$message,$available) = @_;
    return if $status eq $self->status;
    if( $status eq PENDING || $status eq DELAYED )
    {
        $self->{available_date}=$available;
    }
    elsif( $status eq UNAVAILABLE )
    {
        $self->{available_date}=undef;
    }
    else
    {
        croak "Invalid status $status for data request";
    }
    $self->{status}=$status;
    $self->{status_message}=$message;
    return $self;
}
    
=head2 $request->setWhenAvailable( $available )

Set the request available date

=cut

sub setWhenAvailable
{
    my($self,$available) = @_;
    $self->{available_date} = $available;
    return $self;
}

=head2 $request->setCompleted( $subtype )

Set the request status to filled, updating the available date with the current time.

Parameters are 

=over

=item $subtype The actual subtype supplied.

=back


=cut

sub setCompleted
{
    my ($self,$subtype,$message) = @_;
    $self->{status}=COMPLETED;
    $self->{available_date}=time();
    $self->{supplied_subtype}=uc($subtype);
    $self->{status_message}=$message || '';
    return $self;
}

1;
