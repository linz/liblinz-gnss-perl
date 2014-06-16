use strict;

package LINZ::GNSS::RinexFile;

no warnings qw/substr/;
use Carp;
use LINZ::GNSS::Time qw(ymdhms_seconds);


=head1 LINZ::GNSS::RinexFile - scan a rinex observation file for various information

=cut

=head2 my $rxfile=new LINZ::GNSS::RinexFile($filename, %options)

Loads a RINEX file and scans the content.

The following options are supported:

=over

=item skip_obs

If true then the observations are not scanned - only the header

=item session=[start,end]

If defined then only counts observations within this session

=back

=cut 

sub new
{
    my($class,$filename,%options) = @_;
    open(my $f,"<$filename") || croak("Cannot open RINEX file $filename\n");
    my $self=bless { filename=>$filename, _replace=>{}, _options=>\%options }, $class;
    eval
    {
        $self->_scanHeader($f);
        $self->_scanObs($f,$options{session}) if ! $options{skip_obs};
    };
    if( $@ )
    {
       croak($@." at line $. of $filename\n"); 
    }
    close($f);
    return $self;
}

sub _trimsub
{
    my($src,$start,$len)=@_;
    my $copy= $len ? substr($src,$start,$len) : substr($src,$start);
    $copy =~ s/^\s+//;
    $copy =~ s/\s+$//;
    return $copy;
}

# Load a writeable value and update the input record if the value is changed

sub _loadWritableField
{
    my($self,$line,$start,$len,$field)=@_;
    my $value=_trimsub($$line,$start,$len);
    # Only hold the first value read..
    $self->{$field}=$value if ! $self->{$field};

    if( exists($self->{_replace}->{$field}))
    {
        my $replace=$self->{_replace}->{$field}->{$value};
        if( $replace )
        {
            substr($$line,$start,$len)=sprintf("%-*.*s",$len,$len,$replace);
        }
    }
}

sub _loadHeader
{
    my($self,$line) = @_;
    my $rectype=_trimsub($line,60);
    my @obstypes=();
    my $nobstypes=0;

    my $data=substr($line,0,60);
    push(@{$self->{headers}->{$rectype}},$data);

    if( $rectype eq 'MARKER NAME')
    {
        $self->_loadWritableField(\$line,0,60,'markname');
    }
    elsif( $rectype eq 'MARKER NUMBER')
    {
        $self->_loadWritableField(\$line,0,20,'marknumber');
    }
    elsif($rectype eq 'ANT # / TYPE')
    {
        $self->_loadWritableField(\$line,0,20,'antnumber');
        $self->_loadWritableField(\$line,20,20,'anttype');
    }
    elsif($rectype eq 'REC # / TYPE / VERS')
    {
        $self->_loadWritableField(\$line,0,20,'recnumber');
        $self->_loadWritableField(\$line,20,20,'rectype');
        $self->_loadWritableField(\$line,40,20,'recversion');
    }
    elsif($rectype eq 'ANTENNA: DELTA H/E/N')
    {
        my $x=_trimsub($data,0,14)+0.0;
        my $y=_trimsub($data,14,14)+0.0;
        my $z=_trimsub($data,28,14)+0.0;
        $self->{delta_hen}=[$x,$y,$z];
    }
    elsif($rectype eq 'APPROX POSITION XYZ')
    {
        my $x=_trimsub($data,0,14)+0.0;
        my $y=_trimsub($data,14,14)+0.0;
        my $z=_trimsub($data,28,14)+0.0;
        $self->{xyz}=[$x,$y,$z];
    }
    elsif($rectype eq '# / TYPES OF OBSERV')
    {
        my $ntypes=substr($data,0,6);
        if( $ntypes =~ /\d/ )
        {
            $self->{nobstypes} = $ntypes+0;
            $self->{obstypes}=[];
        }
        foreach my $nt (1..9)
        {
            my $ot=_trimsub($data,$nt*6,6);
            push(@{$self->{obstypes}},$ot) if $ot ne '';
        }
    }
    elsif( $rectype eq 'TIME OF FIRST OBS' )
    {
        $data =~ /(.{6})(.{6})(.{6})(.{6})(.{6})(.{13}).{0,5}(.{0,3})/;
        my ($year,$mon,$day,$hour,$min,$sec,$sys) = ($1,$2,$3,$4,$5,$6,$7);
        $self->{_year}=$year;
        $self->{starttime}=ymdhms_seconds($year,$mon,$day,$hour,$min,$sec);
        $self->{endtime}=$self->{starttime};
    }
    elsif( $rectype eq 'INTERVAL' )
    {
        $self->{interval}=_trimsub($data)+0.0;
    }
    return $line;
}

sub _scanHeader
{
    my($self, $f,$of)=@_;
    my $filename=$self->{filename};
    my $end = 0;
    my $rftype = 0;
    my $satsys = 0;
    my $version='';
    $self->{headers}={};
    while(my $line=<$f>)
    {
        # A bit of leniency!
        next if $line =~ /^\s*$/;

        my $rectype=_trimsub($line,60);
        if( ! $rftype )
        {
            die("RINEX VERSION / TYPE missing in $filename\n")
                if $rectype ne 'RINEX VERSION / TYPE';
            $version=_trimsub($line,0,20);
            $rftype=_trimsub($line,20,1);
            $satsys=_trimsub($line,40,1);
            $self->{type} = $rftype;
            $self->{version} = $version;
            $self->{satsys} = $satsys;
            die("Only OBSERVATION DATA rinex types are supported by RinexFile\n")
                if $rftype ne 'O';
        }
        elsif( $rectype eq 'END OF HEADER' )
        {
            $end=1;
        }
        else
        {
            $line=$self->_loadHeader($line);
        }
        print $of $line if $of;
        last if $end;
    }
    die("END OF HEADER missing in $filename\n") if ! $end;
    die("MARKER NAME missing in $filename\n") if ! $self->{markname};
    die("Observation types in header don't match count in $filename\n")
        if $self->{nobstypes} != scalar(@{$self->{obstypes}});
}

sub _scanObs
{
    my($self,$f,$session,$of) = @_;
    my $nobs=0;
    while( my $line=<$f> )
    {
        next if $line =~ /^\s*$/;
        print $of $line if $of;
        if( $line =~ /^
            \s([\s\d]\d)
            \s([\s\d]\d)
            \s([\s\d]\d)
            \s([\s\d]\d)
            \s([\s\d]\d)
            \s([\s\d\.]{10})
            \s\s([016])
            ([\s\d][\s\d]\d)
            /x )
        {
            my ($year,$mon,$day,$hour,$min,$sec,$eflag,$nsat)=($1,$2,$3,$4,$5,$6,$7,$8);
            $year += 1900;
            $year += 100 if $year < $self->{_year};
            my $endtime=ymdhms_seconds($year,$mon,$day,$hour,$min,$sec);
            if( ! $session || ($endtime >= $session->[0] || $endtime <= $session->[1]) )
            {
                $nobs++ if $eflag ne '6';
                $self->{endtime} = $endtime if $endtime > $self->{endtime};
            }
            # Skip for additional satellite ids
            my $nskip=int(($nsat-1)/12);
            # Number of obs records
            $nskip += $nsat * (1 + int(($self->{nobstypes}-1)/5));
            while( $line && $nskip-- )
            {
                $line=<$f>;
                print $of $line if $of;
            }
        }
        elsif( $line =~ /^
            \s([\s\d]{2})
            \s([\s\d]{2})
            \s([\s\d]{2})
            \s([\s\d]{2})
            \s([\s\d]{2})
            \s([\s\d\.]{10})
            \s\s([2345])
            ([\s\d][\s\d]\d)
            /x )
        {
            my $nskip=$8;
            while( $nskip--)
            {
                $line=<$f>;
                last if ! $line;
                $line=$self->_loadHeader($line);
                print $of $line if $of;
            }
        }
        else
        {
            die("Unrecognized record $line in ".$self->{filename});
        }
    }
    $self->{nobs}=$nobs;
}

=head2 $rxfile->xxx

Functions for accessing RINEX file information from the file.  The fields indicated with 
an asterisk can be used as functions to update the metadata.  The update values will be
used by the write function which copies the rinex file.

The following access functions are provided:

=over

=item $rxfile->filename

=item $rxfile->version

=item $rxfile->type

=item $rxfile->satsys

=item $rxfile->markname *

=item $rxfile->marknumber *

=item $rxfile->antnumber *

=item $rxfile->anttype *

=item $rxfile->recnumber *

=item $rxfile->rectype *

=item $rxfile->recversion *

=item $rxfile->xyz 

Returns an array [$x,$y,$z].  

=item $rxfile->delta_hen 

Returns an array [$dh,$de,$dn].  

=item $rxfile->obstypes

Returns an array ref of observation types in the file

=item $rxfile->nobstypes

Returns the number observation types in the file

=item $rxfile->starttime

The start time epoch in seconds

=item $rxfile->endtime

The start time epoch in seconds

=item $rxfile->interval

The data interval in seconds

=item $nobs

The number of observations in the file (or the number within the  session) specified
in the constructor.

=item $rxfile->headers

Returns a hash ref defining the headers.  For each value the hash contains an array 
ref of corresponding information from the headers.

=back

=cut

sub filename { return $_[0]->{filename}; }
sub version { return $_[0]->{version}; }
sub type { return $_[0]->{type}; }
sub satsys { return $_[0]->{satsys}; }
sub obstypes { return $_[0]->{obstypes}; }
sub nobstypes { return $_[0]->{nobstypes}; }
sub starttime { return $_[0]->{starttime}; }
sub endtime { return $_[0]->{endtime}; }
sub interval { return $_[0]->{interval}; }
sub xyz  { return $_[0]->{xyz}; }
sub delta_hen  { return $_[0]->{delta_hen}; }
sub nobs { return $_[0]->{nobs}; }
sub headers { return $_[0]->{headers}; }

# Get/set writeable fields.  Use a mapping of values to ensure that only 
# replace one value (eg if multiple marker names in RINEX file

sub _getset
{
    my($self,$value)=@_;
    my $field=(caller(1))[3];
    $field =~ s/.*\://;
    $self->{_replace}->{$field}->{$self->{$field}} = $value if defined($value);
    return $self->{_replace}->{$field}->{$self->{$field}} || $self->{$field};
}

sub markname { return _getset(@_) } 
sub marknumber  { return _getset(@_) } 
sub antnumber  { return _getset(@_) } 
sub anttype  { return _getset(@_) } 
sub recnumber  { return _getset(@_) } 
sub rectype  { return _getset(@_) } 
sub recversion  { return _getset(@_) } 

=head2 $rxfile->write($filename)

Copies the rinex file to a new location.  If any of the updatable fields have been altered
then the new values are copied into header records.  Note that then entire file is copied,
even if the file was loaded with skip_obs=>1.

=cut

sub write 
{
    my ($self,$filename) = @_;
    my $srcfile=$self->filename;
    my $tgtfile=$filename;
    my $options=$self->{_options};
    open( my $fsrc, "<$srcfile" ) || croak("Cannot reopen $srcfile\n");
    open( my $ftgt, ">$tgtfile" ) || croak("Cannot open $tgtfile\n");
    eval
    {
        $self->_scanHeader($fsrc,$ftgt);
        $self->_scanObs($fsrc,$options->{session},$ftgt);
    };
    if( $@ )
    {
       croak($@." at line $. of $filename\n"); 
    }

    close($fsrc);
    close($ftgt);
}


1;
