use strict;

=head1

LINZ::GNSS::FileTypeList class defines a set of FileType objects.  These can be generic or
types for a particular data source or target. The class wraps a hash of hashes, on type and 
subtype, and provides accessor and processing functions relating to the group.

Each type/subtype entry is an array ref of one or more FileType objects, as there may be
multiple definitions of a subtype with different suffixes (eg _1, _2, ...)

=cut

package LINZ::GNSS::FileTypeList;
use LINZ::GNSS::FileType;

our $DefaultTypes={};

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
            my $subtypes=$self->{$type}->{$subtype};
            my @newsubtypes=();
            foreach my $st (@$subtypes)
            {
                push(@newsubtypes,$st->clone());
            }
            $copy->{$type}->{$subtype}=\@newsubtypes;
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
        frequency (hourly|3hourly|6hourly|daily|weekly)
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
        my $frequencies = {};
        my $priorities = {};
        $type=uc($type);
        while (my ($subtypes, $cfgft) = each %$subtypedef )
        {
            $subtypes=uc($subtypes);
            my $subtype=$subtypes;
            $subtype =~ s/_\d+$//; # Remove suffix for multiply defined subtypes
            my $filetype = new LINZ::GNSS::FileType($type, $subtype, $cfgft);
            $frequencies->{$subtype} ||= $filetype->frequency;
            if( $filetype->frequency ne $frequencies->{$subtype} )
            {
                my $freq1 = $filetype->frequency;
                my $freq2 = $frequencies->{$subtype};
                croak("Conflicting frequency $freq1 and $freq2 for $type:$subtype\n");
            }
            if( $filetype->priority )
            {
                $priorities->{$subtype} ||= $filetype->priority;
                if( $filetype->priority != $priorities->{$subtype} )
                {
                    my $p1 = $filetype->priority;
                    my $p2 = $priorities->{$subtype};
                    croak("Conflicting priority $p1 and $p2 for $type:$subtype\n");
                }
            }
            $self->{$type}->{$subtype} ||= [];
            push(@{$self->{$type}->{$subtype}}, $filetype);
        }
        # Set priorities on subtypes which haven't been set for all variants.
        foreach my $subtype (keys %$priorities)
        {
            my $priority=$priorities->{$subtype};
            foreach my $ft (@{$self->{$type}->{$subtype}})
            {
                $ft->{priority}=$priority;
            }
        }
    }
    return $self;
}


=head2 $list->unsupportedTypes( $otherlist )

Checks that all the types defined in a DataCenter are defined in an another
data center (or DefaultTypes if not defined).

Returns an array of missing types/subtypes

=cut

sub unsupportedTypes
{
    my ($self,$other)=@_;
    $other=$other || $DefaultTypes;
    my @missing=();
    foreach my $type (keys %$self )
    {
        if( ! exists $other->{$type})
        {
            push(@missing,$type);
            next;
        }
        my $subtypes=$self->{$type};
        
        foreach my $subtype (keys %$subtypes)
        {
            if( ! exists $other->{$type}->{$subtype} )
            {
                push(@missing,"$type:$subtype")
            }
        }
    }
    @missing=sort(@missing);
    return wantarray ? @missing : \@missing;
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
    $LINZ::GNSS::FileTypeList::DefaultTypes=$ftl;
}

=head2 $type = $typelist->getType($type,$subtype)

Returns a type from the type list with the specified type and subtype.  If the type
does not exist return undefined value.

Can be called as LINZ::GNSS::FileTypeList->getType($type,$subtype) to return the 
default settings for the type.

The value returned is an array ref of one or more FileType objects, as there may be
multiple definitions of a subtype with different suffixes (eg _1, _2, ...)

=cut

sub getType
{
    my($self,$type,$subtype) = @_;
    $type=uc($type);
    $subtype=uc($subtype);
    if( ! ref($self) ) { $self = $LINZ::GNSS::FileTypeList::DefaultTypes }
    return exists $self->{$type} && exists $self->{$type}->{$subtype} ?
           $self->{$type}->{$subtype} :
           undef;
}

=head2 $type = $typelist->getTypes($type, $subtype); or $type=$typeList->getTypes($request);

Returns a list of types matching the specified type and subtype in a data request, ordered from 
highest to lowest priority.  The list will contain more than one element if 
the subtype is suffixed with '+'.  In this case all subtypes with equal or higher
priority are included.  Use a subtype of '' to list all subtypes with a priority > 0
or subtype of '*' to list all subtypes.  

The list will also include all subtypes matching the type and subtype with a suffix of _n
where n is a number.  This allows for different representations of the same product at 
different times (eg switch from RINEX2 to RINEX3 filenames).

Can be called as LINZ::GNSS::FileTypeList->getTypes($type,$subtype) to return the 
default settings for the type.

=cut

sub getTypes
{
    my ($self, $type, $subtype ) = @_;
    if( ! ref($self) ) { $self = $LINZ::GNSS::FileTypeList::DefaultTypes }
    if( ref($type) )
    {
        my $request=$type;
        $type=$request->type;
        $subtype=$request->supplied_subtype || $request->subtype;
    }
    $type=uc($type);
    $subtype=uc($subtype);
    my $plus= $subtype eq '' || $subtype eq '*' || $subtype=~s/\+$//;;
    my @result=();
    if( exists $self->{$type} )
    {
        my $basepriority=0;
        if( $subtype ne '' && $subtype ne '*' )
        {
            return @result if ! exists $self->{$type}->{$subtype};
            push(@result,@{$self->{$type}->{$subtype}});
            $basepriority=$result[0]->{priority};
        }
        if( $plus )
        {
            foreach my $t (values %{$self->{$type}})
            {
                push(@result,@$t) if $t->[0]->{priority} > $basepriority || $subtype eq '*';
            }
            @result = sort {$b->{priority} <=> $a->{priority}} @result;
        }
    }
    return wantarray ? @result : \@result;
}

=head2 @subtypes = $typelist->getSubTypes($type,$plustypes)

Returns a list of valid subtypes for a type.  If $plustypes is true then
the list may include '+' types where there is a higher priority option for a type.

May be called as LINZ::GNSS::FileTypeList->getSubTypes($type)

=cut

sub getSubTypes
{
    my($self,$type,$plustypes) = @_;
    if( ! ref($self) ) { $self = $LINZ::GNSS::FileTypeList::DefaultTypes }
    my @types=sort {$b->[0]->{priority} <=> $a->[0]->{priority} || $a->[0]->{subtype} cmp $b->[0]->{subtype}} values(%{$self->{$type}});
    my @subtypes=();
    foreach my $t (@types)
    {
        if( $plustypes && $t->[0]->{priority} && $t->[0]->{priority} < $types[0]->[0]->{priority} )
        {
            push(@subtypes,$t->[0]->{subtype}.'+');
        }
        push(@subtypes,$t->[0]->{subtype});
    }
    return wantarray ? @subtypes : \@subtypes;
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

Returns a list of FileType objects which have been renamed.

=cut

sub setFilename
{
    my( $self, $type, $subtype, $filename ) = @_;
    return [] if $filename eq '';
    my $srctype=$self->{uc($type)}->{uc($filename)};
    my $freq;
    if( $srctype )
    {
        $filename=$srctype->[0]->filename;
        $freq=$srctype->[0]->frequencysecs;
    }
    my $renamed_types=[];
    foreach my $type ($self->getTypes($type,$subtype))
    {
        next if defined($freq) && $freq != $type->[0]->frequencysecs;
        $type->setFilename($filename);
        push(@$renamed_types,$type);
    }
    return $renamed_types;
}

=head2 $typelist->canSetFilename($type,$subtypefrom,$subtypeto)

Determines whether it is possible to rename between two different subtypes 
using the setFilename function

=cut

sub canSetFilename
{
    my ( $self, $type, $subtype, $filename ) = @_;
    # If new name is blank then nothing to do, so result is always success!
    return 1 if $filename eq '';
    my $type1 = $self->getType($type,$subtype);
    # If not a valid subtype then cannot rename...
    return 0 if ! $type1;
    my $type2 = $self->getType($type,$filename);
    # If new filename is not a subtype then it is just a straight renaming
    return 1 if ! $type2;
    # Otherwise can rename if their have the same frequency
    return $type1->[0]->frequencysecs == $type2->[0]->frequencysecs ? 1 : 0;
}

=head2 @types=$typelist->types()

Return an array of all types provided by the data source

=cut

sub types
{
    my($self)=@_;
    if( ! ref($self) ) { $self = $LINZ::GNSS::FileTypeList::DefaultTypes }
    my @basetypes=('ORB','ERP','OBS');
    my @othertypes=grep { $_ ne 'ORB' && $_ ne 'ERP' && $_ ne 'OBS' } sort keys %$self;
    my @result=();
    foreach my $t (@basetypes,@othertypes)
    {
        my @subtypes=sort {$a->subtype cmp $b->subtype} @{$self->getTypes($t,'*')};
        push(@result,@subtypes);
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
    my $retry = 0;
    $now ||= time();
    foreach my $type ( $self->getTypes($request) )
    {
        my ($time,$tretry,$failtime)=$type->availableTime($request);
        next if ! $time;
        $available=$time if  ! $available || $time < $available;
        next if $time > $now;
        $retry ||= $tretry;
        $retry=$tretry if $tretry && $tretry < $retry;

        $files = $type->fileList($request,$stncodes);
        last;
    }
    return $available, $files, $retry;
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
    return $self->{$type}->{$subtype}->[0]->getFilespec($srcspec,$stncodes);
}


1;
