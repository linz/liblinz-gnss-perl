use strict;

package LINZ::GNSS::RinexFile;

no warnings qw/substr/;
use File::Copy;
use Carp;
use PerlIO::gzip;
use LINZ::GNSS::Time qw(ymdhms_seconds seconds_ymdhms);


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
    my $gzip=$filename =~ /\.gz$/;
    my $self=bless { filename=>$filename, _replace=>{}, _options=>\%options, gzip=>$gzip }, $class;
    my $f = $self->_open();
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

sub _open()
{
    my ($self) = @_;
    my $filename = $self->{filename};
    my $gzip = $self->{gzip};
    my $mode = $gzip ? "<:gzip" : "<";
    open(my $f,$mode,$filename) || croak("Cannot open RINEX file $filename\n");
    return $f;
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
    my $data=substr($line,0,60);
    push(@{$self->{headers}->{$rectype}},$data);

    if( $rectype eq 'RINEX VERSION / TYPE')
    {
        $self->{version} = _trimsub($line,9);
    }
    elsif( $rectype eq 'MARKER NAME')
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
        my $ntypes=substr($data,1,5);
        if( $ntypes =~ /\d/ )
        {
            $self->{nobstypes} += $ntypes+0;
        }
        foreach my $nt (1..9)
        {
            my $ot=_trimsub($data,$nt*6,6);
            push(@{$self->{obstypes}},$ot) if $ot ne '';
        }
    }
    elsif( $rectype eq 'TIME OF FIRST OBS' )
    {
        $self->_loadWritableField(\$line,0,43,'firstobstime');
        $data=substr($line,0,60);
        $data =~ /(.{6})(.{6})(.{6})(.{6})(.{6})(.{13}).{0,5}(.{0,3})/;
        my ($year,$mon,$day,$hour,$min,$sec,$sys) = ($1,$2,$3,$4,$5,$6,$7);
        $self->{_year}=$year;
        $self->{starttime}=ymdhms_seconds($year,$mon,$day,$hour,$min,$sec);
        $self->{endtime}=$self->{starttime};
    }
    elsif( $rectype eq 'TIME OF LAST OBS' )
    {
        $self->_loadWritableField(\$line,0,43,'lastobstime');
    }
    elsif( $rectype eq 'INTERVAL' )
    {
        $self->{interval}=_trimsub($data)+0.0;
    }
    $self->{marknumber} = $self->{markname} if ! exists $self->{marknumber} eq '';
    $self->{interval} = 0 if ! exists $self->{marknumber} eq '';
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
    my $hatanaka=0;
    $self->{hatanaka}=0;
    $self->{nobstypes}=0;
    $self->{obstypes}=[];
    $self->{headers}={};
    while(my $line=<$f>)
    {
        # A bit of leniency!
        next if $line =~ /^\s*$/;

        my $rectype=_trimsub($line,60);
        if( ! $rftype )
        {
            if( $rectype =~ /^CRINEX\s/ )
            {
                $hatanaka=1;
                $self->{hatanaka}=1;
                next;
            }
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
    if( $self->{version} =~ /^2/ )
    {
        $self->_scanObs2($f,$session,$of);
    }
    else
    {
        $self->_scanObs3($f,$session,$of);
    }

}

sub _scanObs3
{
    my($self,$f,$session,$of) = @_;
    my $nobs=0;
    my $lasttime;
    my $copy=1;
    if( $self->{hatanaka} )
    {
        die "Scanning RINEX observations in Hatanaka compressed files not supported\n";
    }
    while( my $line=<$f> )
    {
        next if $line =~ /^\s*$/;
        if( $line =~ /^
            \>\s(\d\d\d\d)
            \s([\s\d]\d)
            \s([\s\d]\d)
            \s([\s\d]\d)
            \s([\s\d]\d)
            \s([\s\d\.]{10})
            \s\s([0162345])
            ([\s\d][\s\d]\d)
            /x )
        {
            my ($year,$mon,$day,$hour,$min,$sec,$eflag,$nsat)=($1,$2,$3,$4,$5,$6,$7,$8);
            $year += 1900;
            $year += 100 if $year < $self->{_year};
            my $endtime=ymdhms_seconds($year,$mon,$day,$hour,$min,$sec);
            if( $lasttime )
            {
                my $interval=$endtime-$lasttime;
                $self->{interval} = $interval if
                    $self->{interval} == 0 || ($interval > 0 && $interval < $self->{interval});
            }
            $lasttime=$endtime;
        
            $copy=0;
            if( ! $session || ($endtime >= $session->[0] && $endtime <= $session->[1]) )
            {
                $copy=1;
                $nobs++ if $eflag < 2;
                $self->{endtime} = $endtime if $endtime > $self->{endtime};
            }
            # Skip for additional satellite ids
            # Number of obs records
            print $of $line if $of && $copy;
            my $nrec=$nsat;
            while( $line && $nrec-- )
            {
                $line=<$f>;
                print $of $line if $of && $copy;
            }
        }
        elsif( $line =~ /^
            \>\s(\d\d\d\d)
            \s([\s\d]\d)
            \s([\s\d]\d)
            \s([\s\d]\d)
            \s([\s\d]\d)
            \s([\s\d\.]{10})
            \s\s(\d])
            ([\s\d][\s\d]\d)
            /x )
        {
            print $of $line if $of;
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

sub _scanObs2
{
    my($self,$f,$session,$of) = @_;
    my $nobs=0;
    my $lasttime;
    my $copy=1;
    if( $self->{hatanaka} )
    {
        die "Scanning RINEX observations in Hatanaka compressed files not supported\n";
    }
    while( my $line=<$f> )
    {
        next if $line =~ /^\s*$/;
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
            if( $lasttime )
            {
                my $interval=$endtime-$lasttime;
                $self->{interval} = $interval if
                    $self->{interval} == 0 || ($interval > 0 && $interval < $self->{interval});
            }
            $lasttime=$endtime;
        
            $copy=0;
            if( ! $session || ($endtime >= $session->[0] && $endtime <= $session->[1]) )
            {
                $copy=1;
                $nobs++ if $eflag ne '6';
                $self->{endtime} = $endtime if $endtime > $self->{endtime};
            }

            # Skip for additional satellite ids
            my $nskip=int(($nsat-1)/12);
            # Number of obs records
            $nskip += $nsat * (1 + int(($self->{nobstypes}-1)/5));
            print $of $line if $of && $copy;
            while( $line && $nskip-- )
            {
                $line=<$f>;
                print $of $line if $of && $copy;
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
            print $of $line if $of;
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

The end time epoch in seconds (set when observations have been scanned)

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
sub gzipped { return $_[0]->{gzip}; }
sub hatanaka { return $_[0]->{hatanaka}; }

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
sub firstobstime  { return _getset(@_) } 
sub lastobstime  { return _getset(@_) } 

=head2 $rxfile->write($filename,%options)

Copies the rinex file to a new location.  If any of the updatable fields have been altered
then the new values are copied into header records.  Note that then entire file is copied,
even if the file was loaded with skip_obs=>1.

options can include:

=over

=item skip_header=1
The header will not be copied

=item skip_obs=1
The observations will not be copied

=item simple_copy=1
Copies the observations without processing at all

=item session=[start,end]
Only observations from the session will be copied

=back

=cut

sub write 
{
    my ($self,$filename,%options) = @_;
    my $srcfile=$self->filename;
    my $options=$self->{_options};
    my $fsrc = $self->_open();
    my $tgtfile=$filename;
    my $ftgt;
    my $close=0;
    if( ref( $tgtfile ))
    {
        $ftgt=$tgtfile;
        $tgtfile="output file";
    }
    else
    {
        open( $ftgt, ">$tgtfile" ) || croak("Cannot open $tgtfile\n");
        $close=1;
    }
    eval
    {
        $self->_scanHeader($fsrc,$options{skip_header} ? '' : $ftgt);
        if( $options{skip_obs})
        {
            # Do nothing
        }
        elsif( $options{simple_copy} && ! $options{session} )
        {
            while( my $line=<$fsrc>) { print $ftgt $line; }
        }
        else
        {
            $self->_scanObs($fsrc,$options{session},$ftgt,1);
        }
    };
    if( $@ )
    {
       croak($@." at line $. of $filename\n"); 
    }

    close($fsrc);
    close($ftgt) if $close;
}

=head2 LINZ::GNSS::RinexFile::Merge($sourcefiles,$target,$options)

Merges a set of Rinex files into a single file.  Assumes that the files are non-overlapping
sequential data.  Raises an exception if the contents are not consistent.

Parameters are 

=over

=item $sourcefiles

an array hash of input files

=item $target 

the name of a target file to write

=item $options

a hash that can include

=over

=item session=[start,end]

If defined that the data will be filtered to the specified session

=item remove=>1

If true then the original files will be removed

=back

=back

=cut

sub Merge
{
    my($sourcefiles,$target,%options) = @_;

    my @rxfiles;
    foreach my $rfn (@$sourcefiles)
    {
        push(@rxfiles,LINZ::GNSS::RinexFile->new($rfn,{skip_obs=>1}));
    }
    @rxfiles = sort { $a->starttime <=> $b->starttime } @rxfiles;

    my $rx0=shift(@rxfiles);

    foreach my $rx (@rxfiles)
    {
        foreach my $test (qw/ 
                version type satsys 
                markname marknumber 
                antnumber anttype 
                recnumber rectype  
                delta_hen 
                obstypes
            /)
        {
            my $v0=$rx0->{$test};
            my $v1=$rx->{$test};
            if( ref($v0) eq 'ARRAY') { $v0=join(' ',@$v0); $v1=join(' ',@$v1); }
            croak("Cannot merge RINEX: inconsistent $test\n") if $v0 ne $v1;
        }
    }

    my $ofn=$target.'.merge';
    open(my $of,">$ofn") || croak("Cannot open merged RINEX $ofn\n");
    eval
    {
        if( $options{session} )
        {
            my $format="%6d%6d%6d%6d%6d%13.7f";
            $rx0->firstobstime(sprintf($format,seconds_ymdhms($options{session}->[0])));
            $rx0->lastobstime(sprintf($format,seconds_ymdhms($options{session}->[1])));
        }
        elsif( @rxfiles )
        {
            $rx0->lastobstime($rxfiles[-1]->lastobstime());
        }
        $rx0->write($of,session=>$options{session});
        foreach my $rx (@rxfiles)
        {
            $rx->write($of,session=>$options{session},skip_header=>1);
        }
        close($of);
        if( $options{remove} )
        {
            foreach my $rfn (@$sourcefiles)
            {
                unlink($rfn);
            }
        }
        move($ofn,$target);
    };
    if( $@ )
    {
        my $msg=$@;
        unlink($ofn) if -f $ofn;
        croak($@);
    };



}

1;
