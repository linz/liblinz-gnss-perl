use strict;

=head1 LINZ::GNSS::FileSpec

A simple hash class returned by requests for data to the DataCenter and FileCache
repositories.  

=cut


package LINZ::GNSS::FileSpec;
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

=head2 $spec=new LINZ::GNSS::FileSpec($hash)

Blesses a hash into the FileSpec class.  It is assumed to have values for all of the 
attributes (as per below)

=cut

sub new
{
    my($self) = @_;
    $self = fields::new($self) unless ref $self;
    return $self;
}

=head2 $value=$spec->attribute

Accessor functions for the various FileSpec attributes.  The attributes defined are:

=over

=item path

Path for the item within the data centre

=item filename

Name of the file

=item basepath

Base path for a data centre 

=item compression

Compression applying to the file

=item type

Type of data (eg ORB, OBS)

=item subtype

Subtype of data (eg RAPID, FINAL)

=item station

Station code if applicable

=item timestamp

Timestamp of the file (? check this!)

=back

=cut

sub path { return $_[0]->{path}; }
sub filename { return $_[0]->{filename}; }
sub basepath { $_[0]->{basepath} = $_[1] if defined $_[1]; return $_[0]->{basepath}; }
sub compression { return $_[0]->{compression}; }
sub type { return $_[0]->{type}; }
sub subtype { return $_[0]->{subtype}; }
sub station { return $_[0]->{station}; }
sub timestamp { return $_[0]->{timestamp}; }

=head2 $path=$spec->filepath

Generates the full filepath from the filespec

=cut 

sub filepath 
{
    my ($self)=@_;
    my $bp=$self->{basepath};
    $bp .= '/' if $bp;
    return $bp.$self->{path}.'/'.$self->{filename};
}

=head2 print $spec->asString

Debugging function for printing the FileSpec object

=cut 
    
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

1;
