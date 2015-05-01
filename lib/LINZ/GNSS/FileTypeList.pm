use strict;

=head1

LINZ::GNSS::FileTypeList class defines a set of FileType objects.  These can be generic or
types for a particular data source or target. The class wraps a hash of hashes, on type and 
subtype, and provides accessor and processing functions relating to the group.

=cut

package LINZ::GNSS::FileTypeList;
use LINZ::GNSS::FileType;

our $defaultTypes={};

=head2 $list=new LINZ::GNSS::FileTypeList($cfg)

Creates a new file type list.  If a configuration is specified then the
the list is populated from that.

=cut

sub new 
{
    my($class,$cfg)=@_;
    my $self = bless {},$class;
    if( $cfg ) { $self->loadTypes($cfg); }
    return $self;
}

=head2 $copy=$list->clone()

Creates a clone of the list

=cut

sub clone 
{
    my($self) = @_;
    my $copy = bless {}, __PACKAGE__;
    foreach my $type (keys(%$self))
    {
        foreach my $subtype(keys(%{$self->{$type}}))
        {
            $copy->{$type}->{$subtype}=$self->{$type}->{$subtype}->clone();
        }
    }
    return $copy;
}

=head2 $list->loadTypes( $cfgtypes )

Loads a configuration section defining a set of file types.  The configuration section
is organised as a sequence of type sections such as obs, orbm, .. and subtypes such as
hourly, final, etc.

  <type>
     <subtype>
        priority #
        filename filename_pattern
        path path_pattern
        compression [hatanaka]+[compress|gzip]
        frequency (hourly|6hourly|daily|weekly)
        latency # (minutes|hours|days [weeky dow])
        retention # days
     </subtype>
  </type>
  ...


=cut

sub loadTypes
{
    my ($self, $cfgtypes)=@_;

    while (my ($type, $subtypedef) = each %$cfgtypes )
    {
        $type=uc($type);
        while (my ($subtype, $cfgft) = each %$subtypedef )
        {
            $subtype=uc($subtype);
            $self->{$type}->{$subtype} = 
                new LINZ::GNSS::FileType($type, $subtype, $cfgft );
        }
    }
    return $self;
}

=head2 LINZ::GNSS::FileTypeList::LoadDefaultTypes($cfg)

Loads the default types from the file type list.  Generally should be called
before loading any other file types, as the default list is used by FileType::new
to fill in missing information from the configuration.  The data types are defined
as:

  <datatypes>
     <type1>
     </type1>
     <type2>
     </type2>
     ...
  </datatypes>

=cut

sub LoadDefaultTypes
{
    my( $cfg ) = @_;
    my $ftl = new LINZ::GNSS::FileTypeList($cfg->{datatypes});
    $LINZ::GNSS::FileTypeList::defaultTypes=$ftl;
}

=head2 $type = $typelist->getType($type,$subtype)

Returns a type from the type list with the specified type and subtype.  If the type
does not exist return undefined value.

Can be called as LINZ::GNSS::FileTypeList->getType($type,$subtype) to return the 
default settings for the type.

=cut

sub getType
{
    my($self,$type,$subtype) = @_;
    $type=uc($type);
    $subtype=uc($subtype);
    if( ! ref($self) ) { $self = $LINZ::GNSS::FileTypeList::defaultTypes; }
    return exists $self->{$type} && exists $self->{$type}->{$subtype} ?
           $self->{$type}->{$subtype} :
           undef;
}

=head2 $type = $typelist->getTypes($type, $subtype); or $type=$typeList->getTypes($request);

Returns a list of types matching the specified type and subtype in a data request, ordered from 
highest to lowest priority.  The list will contain more than one element if 
the subtype is suffixed with '+'.  In this case all subtypes with equal or higher
priority are included.

Can be called as LINZ::GNSS::FileTypeList->getTypes($type,$subtype) to return the 
default settings for the type.

=cut

sub getTypes
{
    my ($self, $type, $subtype ) = @_;
    if( ! ref($self) ) { $self = $LINZ::GNSS::FileTypeList::defaultTypes; }
    if( ref($type) )
    {
        my $request=$type;
        $type=$request->type;
        $subtype=$request->supplied_subtype || $request->subtype;
    }
    $type=uc($type);
    $subtype=uc($subtype);
    my $plus= $subtype eq '' || $subtype=~s/\+$//;;
    my @result=();
    if( exists $self->{$type} )
    {
        my $basepriority=0;
        if( $subtype ne '' )
        {
            return @result if ! exists $self->{$type}->{$subtype};
            push(@result,$self->{$type}->{$subtype});
            $basepriority=$result[0]->{priority};
        }
        if( $plus )
        {
            foreach my $t (values %{$self->{$type}})
            {
                push(@result,$t) if $t->{priority} > $basepriority;
            }
            @result = sort {$b->{priority} <=> $a->{priority}} @result;
        }
    }
    return wantarray ? @result : \@result;
}

=head2 $typelist->setFilename( $type, $subtype, $filename );

Reset the filename for a type and subtype(s) in a list. The new filename should 
contain date and or station substitution strings matching the original name, though
this is not enforced.  Rash use can result in overwriting files and losing data. 
For example using a filename for ULTRA orbits which does not include hours will
cause the four daily ULTRA files to map to the same output name.

The filename can also refer to another subtype, for example 'final'.  For example

   $list->setFilename( 'ORB', 'RAPID', 'FINAL' );

will use the FINAL name for RAPID orbits.  This will only be used if the subtypes
have the same data frequency (eg daily), otherwise the renaming will be ignored 
for these types.

=cut

sub setFilename
{
    my( $self, $type, $subtype, $filename ) = @_;
    my $srctype=$self->{uc($type)}->{uc($filename)};
    my $freq;
    if( $srctype )
    {
        $filename=$srctype->filename;
        $freq=$srctype->frequencysecs;
    }
    foreach my $type ($self->getTypes($type,$subtype))
    {
        next if defined($freq) && $freq != $type->frequencysecs;
        $type->setFilename($filename);
    }
}

=head2 @types=$typelist->types()

Return an array of all types provided by the data source

=cut

sub types
{
    my($self)=@_;
    if( ! ref($self) ) { $self = $LINZ::GNSS::FileTypeList::defaultTypes; }
    my @basetypes=('ORB','ERP','OBS');
    my @othertypes=grep { $_ ne 'ORB' && $_ ne 'ERP' && $_ ne 'OBS' } sort keys %$self;
    my @result=();
    foreach my $t (@basetypes,@othertypes)
    {
        push(@result,@{$self->getTypes($t)});
    }
    return wantarray ? @result : \@result;
}

=head2 $when, $files = $typelist->checkRequest($request,$stncodes,$subtype,$now)

Checks a file type list for potential availability of files.  Returns the best
potentially available files, and when the should be available, or an empty
list of files and when they should be available.

$stncodes is an optional hash defining the mapping from uppercase station name
to the case in the repository.

$subtype if defined limits the search to the specific subtype

$now is an alternative to the current time, used only really for testing.

Returns $when=0 if the request cannot be filled from this list.

=cut 

sub checkRequest
{
    my($self,$request,$stncodes,$subtype,$now) = @_;
    my $files = undef;
    my $available=0;
    $now ||= time();
    foreach my $type ( $self->getTypes($request) )
    {
        next if $subtype && $type->subtype ne $subtype;
        my ($time)=$type->availableTime($request);
        next if ! $time;
        $available=$time if  ! $available || $time < $available;
        next if $time > $now;
        $files = $type->fileList($request,$stncodes);
        last;
    }
    return $available, $files;
}

=head2 $spec=getSpec($srcspec)

Gets the file specification for the file type matching the source specification.

=cut

sub getFilespec
{
    my($self,$srcspec,$stncodes) = @_;
    my $type = $srcspec->{type};
    my $subtype = $srcspec->{subtype};
    return undef if ! exists $self->{$type};
    return undef if ! exists $self->{$type}->{$subtype};
    return $self->{$type}->{$subtype}->getFilespec($srcspec,$stncodes);
}

=head2 $spec=getFilespecType($filespec)

Gets the file type matching the FileSpec object

=cut

sub getFilespecType
{
    my($self,$srcspec,$stncodes) = @_;
    my $type = $srcspec->{type};
    my $subtype = $srcspec->{subtype};
    return undef if ! exists $self->{$type};
    return undef if ! exists $self->{$type}->{$subtype};
    return $self->{$type}->{$subtype};
}


1;
