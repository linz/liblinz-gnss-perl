package LINZ::GNSS::BLQFile;

=head1 LINZ::GNSS::BLQFile

Module for processing ocean loading data in "BLQ" format.  Provides functions for
loading a BLQ file, extracting data for specific mark codes, calculating at 
arbitrary locations based on gridded data (grids do not need to be complete, just
at nodes regularly spaced in latitude and longitude), comparing two different offset
measures (assuming they contain the same tidal components), and writing data to 
an output BLQ file.

BLQ format files are generated by the ocean loading service at holt.oso.chalmers.se/loading/.
This mails back a file of calculated ocean loading coefficients. These files can be concatenated into
a single file for use by this module.

=cut

use strict;
use POSIX;
use Carp;

=head2 my $blqf=new LINZ::GNSS::BLQFile($filename)

Open an existing BLQ file for reading data. Opens the file and reads the file
header

=cut

sub new
{
    my( $class, $blq_file) = @_;
    my $self=bless {filename=>$blq_file}, $class;
    $self->readBlqHeader();
    return $self;
}


sub fh
{
    my($self)=@_;
    if( ! exists $self->{fh} )
    {
        my $blq_file=$self->{filename};
        open( my $blqf, "<", $blq_file ) || croak("Cannot open BLQ data file ".$blq_file."\n");
        binmode($blqf);
        $self->{fh}=$blqf;
    }
    return $self->{fh};
}

sub close
{
    my( $self ) = @_;
    close($self->{fh}) if $self->{fh};
    delete $self->{fh};
}

=head2 $blqf->readBlqHeader()

Read the header of the BLQ file

=cut

sub readBlqHeader
{
    my($self)=@_;
    my $blqf=$self->fh;

    my $l;
    while( $l=<$blqf> )
    {
        if($l =~ /\$\$\s+COLUMN\s+ORDER\:\s+(.*?)\s*$/)
        {
            my @columns=split(' ',$1);
            if( $self->{columns} )
            {
                croak("Inconsistent column order in BLQ file: $1\n") 
                    if join(' ',@{$self->{columns}}) != join(' ',@columns);
            }
            $self->{columns}=\@columns;
        }
        last if $l =~ /\$\$\s+END\s+HEADER\s*$/;
    }
    return $l ? 1 : 0;
}

=head2 $data=$blqf->readBlqData( $floc )

Reads BLQ ocean loading offset for a single station.  If $floc is specified and 
greater than zero then the input file will be reset to this offset.  Otherwise the
next record will be read.  Returns undefined if no data is found.  May raise 
exceptions for invalid data.

Valid records are returned as LINZ::GNSS::BLQFile::Data objects.

=cut

sub readBlqData
{
    my($self,$loc)=@_;
    my $fh=$self->fh;
    seek($fh,$loc,0) if $loc;
    my @data=();
    my ($code,$hcode,$lon,$lat,$hgt,$model,$computed);

    my ($l,$floc);
    $floc=tell($fh);
    while(1)
    {
        $l=<$fh>;
        return if ! $l;
        return if $l =~ /^\$\$\s+END\s+TABLE/;
        if( $l =~ /^\s\s*(\S.*?)\s*$/ )
        {
            $hcode=$1;
        }
        elsif( $l=~ /^\$\$\s+(Computed\s+by\s+.*?)\s*$/ )
        {
            $computed=$1;
        }
        elsif( $l =~ /^\$\$\s+([\w\s]+)\,\s+RADI\s+TANG\s+lon\/lat\:\s+(\S+)\s+(\S+)\s+(\S+)\s*$/ )
        {
            ($code,$lon,$lat,$hgt)=($1,$2,$3,$4);
            last;
        }
        elsif( $l =~ /^\$\$\s+(.*?)(?:\s+ID\:.*)?\s*$/ )
        {
            $model=$1;
        }
    }

    while($l=<$fh>)
    {
        next if $l =~ /^\$\$/;
        my @fields=grep(/\S/,split(/(.......)/,substr($l,1)));
        croak("Invalid BLQ data row: $l\n".join(' ',@{$self->{columns}})."\n") 
            if scalar(@fields) != scalar(@{$self->{columns}});
        push(@data,\@fields);
        last if scalar(@data) == 6;
    }
    my $codetest=$code;
    my $hcodetest=$hcode;
    $codetest=~s/\s+/ /g;
    $hcodetest=~s/\s+/ /g;
    croak("BLQ data doesn't define code/location\n") if ! defined($code);
    croak("BLQ codes not matched \"$code\":\"$hcode\"\n") if $codetest ne $hcodetest;
    croak("BLQ data doesn't define model\n") if ! defined($model);
    croak("BLQ data missing\n") if scalar(@data) != 6;
    $self->{model} ||= $self->{model};
    return LINZ::GNSS::BLQFile::Data->new(
        code=>$code,
        model=>$model,
        loc=>[$lon,$lat,$hgt],
        floc=>$floc,
        data=>\@data,
        computed=>$computed,
        components=>$self->{columns},
    );
}

=head2 $blqf->processBlqData( $mysub, $warn )

Process all data in the file. $mysub is a sub reference that is called for
each ocean loading value in turn.  The subroutine is called with one parameter
which is a LINZ::GNSS::BLQFile::Data object.  

By default errors in the data are ignored.  If $warn is 1 then they are reported 
with carp, and if greater than 1 then it will croak on errors.

=cut

sub processBlqData
{
    my ($self,$sub,$warn)=@_;
    my $fh=$self->fh;
    seek($fh,0,0);
    my $headed=0;
    while(1)
    {
        last if ! $self->readBlqHeader();
        $headed=1;
        while(1)
        {
            my $data;
            eval
            {
                $data=$self->readBlqData();
                last if ! $data || ! $data->{data};
                $sub->($data);
            };
            if( $@ && $warn > 0)
            {
                my $msg=$@;
                croak($msg) if $warn > 1;
                carp($msg);
            }
        }
    }
    if( $warn > 0 && ! $headed )
    {
        my $msg="BLQ file doesn't contain a valid header";
        croak($msg) if $warn > 1;
        carp($msg);
    }
}

sub codeIndex
{
    my($self)=@_;
    return $self->{code_index} if $self->{code_index};
    my $code_index={};
    my $alt_codes={};
    $self->processBlqData( sub {
        my ($data)=@_;
        my $code=uc($data->code);
        my $code2=$code;
        $code2=~ s/\s.*//;
        $code_index->{$code}=$data->floc;
        if( $code2 ne '' && $code2 ne $code )
        {
            $alt_codes->{$code}=$data->floc;
        }
    });
    foreach my $code (keys(%$alt_codes))
    {
        $code_index->{$code}=$alt_codes->{$code}
            if ! exists $code_index->{$code};
    }
    $self->{code_index}=$code_index;

    return $self->{code_index};
}

=head2 $data=$blqf->loadingForCode($code [,$matchword] )

Returns BLQ data matching a station code.  Codes are case insensitive.
If a station code in the BLQ file consists of multiple words it may be 
matched by just the first word if $matchword is true.

=cut

sub loadingForCode
{
    my ($self,$code,$matchword)=@_;
    $code=uc($code);
    $code =~ s/^\s+//;
    $code =~ s/\s+$//;
    my $floc=$self->codeIndex->{$code};
    if( ! $floc && $code =~ /\s/ )
    {
        $code =~ s/\s+/ /g;
        $floc=$self->codeIndex->{$code};
        if( ! $floc && $matchword )
        {
            $code=~ s/\s.*//;
            $floc=$self->codeIndex->{$code};
        }
    }
    return if ! $floc;
    return $self->readBlqData($floc);
}

sub gridIndex
{
    my($self)=@_;
    return $self->{grid_index} if $self->{grid_index};

    my $blq_file=$self->{filename};
    my $code='';
    my $codeloc=0;
    my $phase=0;
    my @columns=();
    my @locs=();
    my %lats=();
    my %lons=(); 
    my $model='';

    $self->processBlqData( sub {
        my ($data)=@_;
        $lons{$data->{loc}->[0]}=1;
        $lats{$data->{loc}->[1]}=1;
        $model=$data->{model} if $model eq '';
        croak("BLQ model not consistent in $blq_file ($model $data->{model})\n")
            if $model ne $data->{model};
        push(@locs,[$data->{loc}->[0],$data->{loc}->[1],$data->{floc}]);
        });

    my @lon=sort keys %lons;
    croak("BLQ grid doesn't contain enough data\n") if scalar(@lon) < 2;
    my $lon0=$lon[0];
    my $dlon=$lon[1]-$lon[0];
    my $nlon=int(($lon[-1]-$lon[0])/$dlon+0.01);
    $dlon=($lon[-1]-$lon[0])/$nlon;
    foreach my $lon (@lon)
    {
        my $tlon=int(($lon-$lon0)/$dlon+0.01);
        my $err=abs($lon-$lon0-$dlon*$tlon);
        croak("Longitudes not on regular grid ($lon $err)\n") if $err > 0.000001;
    }
    
    my @lat=sort keys %lats;
    croak("BLQ grid doesn't contain enough data\n") if scalar(@lat) < 2;
    my $lat0=$lat[0];
    my $dlat=$lat[1]-$lat[0];
    my $nlat=int(($lat[-1]-$lat[0])/$dlat+0.01);
    $dlat=($lat[-1]-$lat[0])/$nlat;
    foreach my $lat (@lat)
    {
        my $tlat=int(($lat-$lat0)/$dlat+0.01);
        my $err=abs($lat-$lat0-$dlat*$tlat);
        croak("Latitudes not on regular grid ($lat)\n") if $err > 0.000001;
    }
    
    my $index={};
    foreach my $data (@locs)
    {
        my($lon,$lat,$loc)=@$data;
        $lon=int(($lon-$lon0)/$dlon+0.01);
        $lat=int(($lat-$lat0)/$dlat+0.01);
        $index->{$lon}->{$lat}=$loc;
    }

    $self->{grid_index}={
        lon0=>$lon0,
        lat0=>$lat0,
        dlon=>$dlon,
        dlat=>$dlat,
        index=>$index,
        columns=>$self->{columns},
        filesize=>-s $blq_file,
        model=>$model,
        };

    return $self->{grid_index};
}

=head2 $data=$blqf->calcLoadingFromGrid( $code, $lon, $lat )

If the BLQ file contains data on a regular grid then this function can calculate
points within the grid cells.  The grid does not need to be complete, but does need to be 
on regular longitude and latitude points.  The ocean loading value will be interpolated 
by bilinear interpolation.

Returns a LINZ::GNSS::BLQFile::Data object if the value can be calculated, and croaks
otherwise.

=cut

sub calcLoadingFromGrid
{
    my( $self, $code, $lon, $lat )=@_;
    my $index=$self->gridIndex;
    my $glon=($lon-$index->{lon0})/$index->{dlon};
    my $glat=($lat-$index->{lat0})/$index->{dlat};
    my $ilon=floor($glon);
    $glon -= $ilon;
    my $ilat=floor($glat);
    $glat -= $ilat;
    my $grid_components=[
        [$ilon,$ilat,(1.0-$glon)*(1.0-$glat)],
        [$ilon+1,$ilat,($glon)*(1.0-$glat)],
        [$ilon,$ilat+1,(1.0-$glon)*($glat)],
        [$ilon+1,$ilat+1,($glon)*($glat)],
        ];
    my $griddata=[];
    my $gindex=$index->{index};
    my $model;
    foreach my $gsum (@$grid_components)
    {
        my ($ilon,$ilat,$factor) = @$gsum;
        croak("No data for ($lon,$lat) in BLQ grid\n") if ! exists $gindex->{$ilon}->{$ilat};
        my $gdata=$self->readBlqData( $gindex->{$ilon}->{$ilat} );
        $model=$gdata->{model};
        push(@$griddata,$gdata->{data},$factor);
    }
    my $blqdata=LINZ::GNSS::BLQFile::Data::sumComponents( $griddata );

    return LINZ::GNSS::BLQFile::Data->new(
        code=>$code,
        model=>$model,
        loc=>[$lon,$lat,0.0],
        floc=>-1,
        data=>$blqdata,
        computed=>"Computed from gridded model",
        components=>$self->{columns}
    );
}

=head2 $blqf->copyHeader( $ofh )

Copies the header information from the BLQ file to an output file handle $ofh.

=cut

sub copyHeader
{
    my( $self, $ofh ) = @_;
    my $fh = $self->fh;
    seek($fh,0,0);
    while( my $l=<$fh> )
    {
        next if $l !~ /^\$\$/;
        print $ofh $l;
        last if $l =~ /\$\$\s+END\s+HEADER\s*$/;
    }
}

=head2 $blqf->writeBlqData( $ofh, $data, $computed )

Writes a LINZ::GNSS::BLQFile object to an output file handle $ofh. The
data includes a comment on how the point is computed, which can be set with the
$computed parameter.

=cut

sub writeBlqData
{
    my($self,$ofh,$blqdata,$computed)=@_;
    $computed ||= $blqdata->{computed};
    $computed ||= "Computed by LINZ BLQFile routines";
    print $ofh "\$\$\n";
    print $ofh "  ".$blqdata->{code}."\n";
    print $ofh "\$\$ ".$self->{model}."\n";
    print $ofh "\$\$ ".$computed."\n";
    printf $ofh "\$\$ %-26s RADI TANG  lon/lat: %9.4f %9.4f %9.3f\n",
        $blqdata->{code}.',',
        $blqdata->{loc}->[0],$blqdata->{loc}->[1], $blqdata->{loc}->[2];
    for my $i (0..2)
    {
        my $d=$blqdata->{data}->[$i];
        print $ofh " ";
        foreach my $v (@$d)
        {
            my $s=sprintf("%7.5f",$v);
            $s=~s/0\./ ./;
            print $ofh $s;
        }
        print $ofh "\n";
    }
    for my $i (3..5)
    {
        my $d=$blqdata->{data}->[$i];
        print $ofh " ";
        foreach my $v (@$d)
        {
            printf $ofh "%7.1f",$v;
        }
        print $ofh "\n";

    }
}

=head2 $blqf->writeEndData( $ofh )

Writes an end of data record to an output file $ofh.

=cut

sub writeEndData
{
    my( $self, $ofh ) = @_;
    print $ofh "\$\$ END TABLE\n";
}

package LINZ::GNSS::BLQFile::Data;

=head1 package LINZ::GNSS::BLQFile::Data

Object type generated by the BLQFile routines.  Provides accessor functions
to retrieve data elements as follows

=over

=item $data->code

=item $data->location return [$lon,$lat,$hgt]

=item $data->lon

=item $data->lat

=item $data->hgt

=item $data->floc  returns the file offset of the data

=item $data->data  returns the offset coefficients

=item $data->computed comment on how the data was computed

=item $data->components array ref of components in the data

=back

Setter function for code:

=over

=item $data->setCode( $code ) resets the mark code

=back

=cut

sub new
{
    my $class=shift(@_);
    my %data=@_;
    return bless \%data, $class;
}

sub code { return $_[0]->{code}; }
sub setCode { $_[0]->{code}=$_[1]; return $_[0]; }
sub location { return $_[0]->{loc}; }
sub lon { return $_[0]->{loc}->[0]; }
sub lat { return $_[0]->{loc}->[1]; }
sub hgt { return $_[0]->{loc}->[2]; }
sub floc {return $_[0]->{floc}; }
sub data {return $_[0]->{data}; }
sub computed {return $_[0]->{computed}; }
sub model {return $_[0]->{model}; }
sub components {return $_[0]->{components}; }

=head2  LINZ::GNSS::BLQFile::Data::sumComponents( $data1, $factor1, $data2, $factor2, ... )_

Sums scaled data components for a set of LINZ::GNSS::BLQFile::Data objects.

=cut

sub sumComponents
{
    my(@data)=@_;
    shift(@data) if ref($data[0]) eq 'LINZ::GNSS::BLQFile::Data';
    my $sumdata=scalar(@data) == 1 ? $data[0] : \@data;
    my $rad2deg=atan2(1.0,1.0)/45.0;
    my $blqcomp=[];
    my $ds=0;
    my $ncolumns=0;
    while($ds < $#$sumdata )
    {
        my $srccomps=$sumdata->[$ds++];
        my $factor=$sumdata->[$ds++];

        $srccomps=$srccomps->data if ref($srccomps) eq 'LINZ::GNSS::BLQFile::Data';
        my $scolumns=scalar(@{$srccomps->[0]});
        $ncolumns ||= $scolumns;
        croak("Inconsistent data in LINZ::GNSS::BLQFile::Data::sumComponents\n")
            if $ncolumns != $scolumns;

        foreach my $i (0..$ncolumns-1)
        {
            foreach my $x (0..2)
            {
                my $angle=$rad2deg*$srccomps->[$x+3]->[$i];
                my $cs=cos($angle);
                my $sn=sin($angle);
                my $v=$srccomps->[$x]->[$i]*$factor;
                $blqcomp->[$x]->[$i] += $cs*$v;
                $blqcomp->[$x+3]->[$i] += $sn*$v;
            }
        }
    }
    foreach my $i (0..$ncolumns-1)
    {
        foreach my $x (0..2)
        {
            my $cs=$blqcomp->[$x]->[$i];
            my $sn=$blqcomp->[$x+3]->[$i];
            my $v=sqrt($cs*$cs+$sn*$sn);
            my $angle=0.0;
            if( $v > 0.0 )
            {
                $angle=atan2($sn,$cs)/$rad2deg;
                $angle -= 360.0 if $angle > 180.0;
            }
            $blqcomp->[$x]->[$i] = $v;
            $blqcomp->[$x+3]->[$i] = $angle;
        }
    }
    return $blqcomp;
}

=head2 $offset=$data->maxOffset()

Computes the maximum offset that can result from a set of ocean loading coefficients.
Can be called directly on a LINZ::GNSS::BLQ_file::Data object, or called as a passed a ->data hash
reference as a parameter, ie

  $offset=LINZ::GNSS::BLQFile::Data::maxOffset($data)

=cut

sub maxOffset
{
    my($self,$data)=@_;
    $data=$self if ! $data;
    $data=$data->data if ref($data) eq 'LINZ::GNSS::BLQFile::Data';
    my $total=0.0;
    my $ncolumns=scalar(@{$data->[0]});
    foreach my $i (0..$ncolumns-1)
    {
        my $sum=0.0;
        foreach my $x (0..2)
        {
            $sum += $data->[$x]->[$i]*$data->[$x]->[$i];
        }
        $total+=sqrt($sum);
    }
    return $total;
}

1;
