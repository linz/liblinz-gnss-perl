use strict;
package LINZ::GNSS::FileCompression;
use fields qw( name compress uncompress presuffix postsuffix );
use Carp;
use File::Copy;
use LINZ::GNSS::Variables qw(ExpandEnv);

our $compressionTypes={};
our $compressionSuffices=[];

=head1 LINZ::GNSS::FileCompression

Functions to manage compression and decompression of files

Compression types are defined in pairs, one for compression, and one for
decompression.  

Usage:

   LINZ::GNSS::FileCompression::LoadCompressionTypes( $cfg )

   eval
   {
       $newfile=LINZ::GNSS::FileCompression::RecompressFile( $oldfile, 'hatanaka+compress', 'gzip', $newfile );
   };
       

=cut

=head2 LINZ::GNSS::FileCompression::LoadCompressionTypes( $config )

Loads configuration information structured as

  <CompressionTypes>
     <Compression>
         name hatanaka
         compress /usr/bin/rnx2crx -f [input] 
         uncompress /usr/bin/rnx2crx -f [input]
         presuffix .ddo
         postsuffix .ddd
     </Compression>
     ...
  </CompressionTypes>


Each compression type has attributes:

=over

=item name: The name of the type (prefixed with "-" for the decompression command)

=item compress: The command to apply the compression.  May contain [input] and [output]
which will be replaced with the input and output file names

=item uncompress: The command to reverse the compression.  May contain [input] and [output]
which will be replaced with the input and output file names

=item presuffix: A suffix added to the filename before running the compression or removed
after running the decompression

=item postsuffix: A suffix removed from the filename after running the compression or added
before running the decompression

=back

The effect of adding the pre suffix, running the command, and removing the post suffix
should be to leave the file with the compression (or decompression applied) and with its
filename unchanged.

=cut

sub LoadCompressionTypes
{
    my ($cfg) = @_;
    my $cfgtypes=$cfg->{compressiontypes}->{compression};
    return if ! $cfgtypes;
    my $ctypes={};
    $cfgtypes = ref($cfgtypes) eq 'ARRAY' ? $cfgtypes : [$cfgtypes];
    foreach my $cfgct (@$cfgtypes)
    {
        my $fc = new LINZ::GNSS::FileCompression($cfgct);
        $ctypes->{$fc->name}=$fc;
    }
    my $suffices=[];
    if( exists($cfg->{compressionsuffices}) )
    {
        my $data=$cfg->{compressionsuffices};
        foreach my $line (split(/\n/,$data))
        {
            my($suffixre,$compdef)=split(' ',$line);
            $suffixre .= '$';
            my $re=qr/$suffixre/;
            push(@$suffices,[$re,$compdef]);
        }
    }    
    $LINZ::GNSS::FileCompression::compressionTypes=$ctypes;
    $LINZ::GNSS::FileCompression::compressionSuffices=$suffices;
}


=head2 my $compdef=LINZ::GNSS::FileCompression::InferCompressionType($filename)

Infers the compression type based on the filename

=cut

sub InferCompressionType
{
    my($filename)=@_;
    my $suffices=$LINZ::GNSS::FileCompression::compressionSuffices;
    foreach my $suffix (@$suffices)
    {
        my($re,$compdef)=@$suffix;
        return $compdef if $filename =~ /$re/;
    }
    return "none";
}


=head2 my $compdef=LINZ::GNSS::FileCompression::IsValidCompression($type)

Tests whether a compression definition as one or more types separated by '+'
can be a valid compression type.

=cut

sub IsValidCompression
{

    my($type)=@_;
    return 1 if $type eq 'auto' || $type eq 'none';
    return 0 if $type !~ /^\w+(\+\w+)*$/;
    my $ctypes=$LINZ::GNSS::FileCompression::compressionTypes;
    foreach my $stage (split(/\+/,$type))
    {
        return 0 if ! exists $ctypes->{$stage};
    }
    return 1;
}

=head2 LINZ::GNSS::FileCompression::RecompressFile($filename,$fromcomp,$tocomp,$tofilename)

Convert a file $filename from a source compression $fromcomp to a target compression $tocomp.  
The compression is defined by a string such as 'hatanaka+compress".  
$tofilename is optional - the file will retain its original name if this is not specified.

Returns the new filename.  Dies if an error is encountered.  Most errors leave the original file
in place, possibly partially recompressed.

=cut

sub RecompressFile
{
    my ($filename, $fromcomp, $tocomp, $tofilename ) = @_;
    my @fcmp=split(/\W+/,lc($fromcomp));
    my @tcmp=split(/\W+/,lc($tocomp));
    while( @fcmp && @tcmp && $fcmp[0] eq $tcmp[0] )
    {
        shift(@fcmp);
        shift(@tcmp);
    }
    my $ctypes=$LINZ::GNSS::FileCompression::compressionTypes;
    foreach my $fc (@fcmp,@tcmp)
    {
        croak "Invalid compression type $fc\n" 
            if $fc ne 'none' && ! exists $ctypes->{$fc};
    }

    foreach my $fc (reverse(@fcmp))
    {
        $ctypes->{$fc}->uncompressFile($filename,1) if $fc ne 'none';
    }

    foreach my $fc (@tcmp)
    {
        $ctypes->{$fc}->compressFile($filename) if $fc ne 'none';
    }

    if( $tofilename )
    {
        my $ok = move($filename,$tofilename);
        croak "Cannot move file to $tofilename\n" if ! $ok;
    }
    else
    {
        $tofilename=$filename;
    }

    return $tofilename;
}


=head2 $comp=new LINZ::GNSS::FileCompression( $cfgcmp )

Reads the details of a compression type from a configuration file.  

=cut

sub new
{
    my($self,$cfgcmp) = @_;
    $self = fields::new($self) unless ref $self;
    my $name=$cfgcmp->{name} || croak "Name is missing for compression type\n";
    my $command=$cfgcmp->{compress} || croak "Compress command is missing for compression type $name\n";
    $command=ExpandEnv($command,"compression command for $name compression");
    my @cmdparts=split(' ',$command);
    -x $cmdparts[0] || croak "Compression $name command $cmdparts[0] is not an executable file\n";
    my @compress=@cmdparts;
    $command=$cfgcmp->{uncompress} || croak "Uncompress command is missing for compression type $name\n";
    $command=ExpandEnv($command,"uncompression command for $name compression");
    @cmdparts=split(' ',$command);
    -x $cmdparts[0] || croak "Compression $name command $cmdparts[0] is not an executable file\n";
    my @uncompress=@cmdparts;
    my $presuffix=$cfgcmp->{presuffix} || '';
    my $postsuffix=$cfgcmp->{postsuffix} || '';
    $self->{name}=$name;
    $self->{compress}=\@compress;
    $self->{uncompress}=\@uncompress;
    $self->{presuffix}=$presuffix;
    $self->{postsuffix}=$postsuffix;
    return $self;
}

=head2 $type->component

Accessor functions for data in a LINZ::GNSS::FileCompression object. Accessors are:

=over

=item $type->name
The name for the file compression type

=item $type->compress
The command line to compress a file

=item $type->uncompress
The command line to uncompress a file

=item $type->presuffix
The suffix that applies to an uncompressed file

=item $type->postsuffix
The suffix that applies to a compressed file

=back

=cut

sub name { return $_[0]->{name}; }
sub compress { return $_[0]->{compress}; }
sub uncompress { return $_[0]->{uncompress}; }
sub presuffix { return $_[0]->{presuffix}; }
sub postsuffix { return $_[0]->{postsuffix}; }

=head2 $type->compressFile($filename,$uncompress)

Apply the compression or decompression to a file.  Use $uncompress=1 to uncompress the file.
The file is compressed or decompressed in place, any file renaming must be done independently.

=cut

sub compressFile
{
    my($self,$filename,$uncompress) = @_;
    if( ! -e $filename )
    {
        my $comp=$uncompress ? 'decompression' : 'compression';
        croak "File $filename to apply $self->{name} $comp to does not exist\n";
    }
    # Find a safe temporary name to use for the various versions of the filename that will exist
    my $presf=$uncompress ? $self->{postsuffix} : $self->{presuffix};
    my $postsf=$uncompress ? $self->{presuffix} : $self->{postsuffix};
    my $command = $uncompress ? $self->{uncompress} : $self->{compress};

    my $tmpn='';
    for( $tmpn='';;$tmpn++)
    {
        next if $tmpn && -e $filename.$tmpn;
        next if $presf && -e $filename.$tmpn.$presf;
        next if $postsf && -e $filename.$tmpn.$postsf;
        last;
    }
    my $infile=$filename.$tmpn.$presf;
    my $outfile=$filename.$tmpn.$postsf;
    my @cmdline=@{$command};
    foreach my $f (@cmdline) 
    { 
        $f = $f eq '[input]' ? $infile : $f; 
        $f = $f eq '[output]' ? $outfile : $f; 
    }
    eval
    {
        rename($filename,$infile);
        my $result = system(@cmdline);
        unlink $infile if $infile ne $outfile;
        unlink $outfile if $result;
        croak "Cannot apply $self->{name} compression to $filename\n" if $result || ! -f $outfile;
        rename($outfile,$filename);
    };
}

=head2 $type->uncompressFile($filename)

Apply the compression or decompression to a file.  
The file is compressed or decompressed in place, any file renaming must be done independently.

=cut

sub uncompressFile
{
    my ($self,$filename) = @_;
    $self->compressFile($filename,1);
}

1;
