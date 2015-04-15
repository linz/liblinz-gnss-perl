#!/usr/bin/perl

=head1 gnss_ftp_mirror.pl

Simple ftp mirror script to recursely mirror files from a remote ftp site to a local 
directory.  In each directory creates a file .ftpmirror which is a listing of that directory

Anything which changes gets downloaded.  Also anything which appears to have changed because 
the format of its directory listing is changed.

This is written for synchronising the Bernese GEN directory.  Unlike wget it downloads files
to a temporary file then renames the final destination.  This ensures that there is never an 
incomplete version of the file.

WARNING:  This version assumes that all file and directory names do not include spaces.
Any that do will fail to be downloaded.



=cut

use strict;
use Getopt::Std;
use URI;
use Net::FTP;
use File::Path;
use File::Copy;
use Config::General;
use LINZ::GNSS::Time qw/parse_gnss_date seconds_yearday time_elements seconds_ymdhms $SECS_PER_DAY/;

my $syntax=<<EOD;

Syntax: gnss_ftp_mirror.pl [-v] config_file

Options are:
   -v            verbose
   -s ###        start date to download (days before now)
   -e ###        end date to download (days before now)
   -i            ignore file size - download matching files
   -p ##         reset pause between downloads

EOD

#=====================================================================
# Get options

my %opts;
getopts('vis:e:p:',\%opts);
my $verbose=$opts{v};
my $ignoresize=$opts{s};

@ARGV == 1 || die $syntax;

my $conffile=$ARGV[0]; 
die "Cannot find config file $conffile\n" if ! -e $conffile;

my $confdir=$conffile;
$confdir =~ s/[^\\\/]*$//;
$confdir =~ s/[\\\/]$//;
$confdir = '.' if $confdir eq '';

my %config=Config::General->new(
    -ConfigFile=>$conffile,
    -LowerCaseNames=>1,)->getall();

my $remoteuri=$config{remoteuri};
my $user=$config{remoteuser};
my $password=$config{remotepassword};

my $remotedir=$config{remotedir};
my $remotefilere=$config{remotefilere};
my $targetpath=$config{targetpath};
my $timeout=$config{timeout}+0 || 30;
my $pause=($opts{p} // $config{downloadwait})+0 || 1; 
my $startage=($opts{s} // $config{startage})+0;
my $endage=($opts{e} // $config{endage})+0;
my $codes=$config{codes};
$codes = join(' ',@$codes) if ref($codes) eq 'ARRAY';
my $validcodes={};
my $allcodes=0;
foreach my $c (split(' ',$codes))
{
    $allcodes=1 if $c eq '*';
    $validcodes->{uc($c)} = 1;
}
$allcodes = 1 if ! %$validcodes;


my $uri=URI->new($remoteuri);

die "$remoteuri is not a valid FTP URI.\nMust be an ftp:// URI" if $uri->scheme != 'ftp';

my $host=$uri->host;
my $basedir=$uri->path;
my ($uri_user,$uri_pwd) = split(/\:/,$uri->userinfo,2);

my $remotere;
eval
{
    my $remotere=qr/$remotefilere/;
};
if( $@ )
{
    die "RemoteFileRe $remotefilere is not a valid regular expression.\n";
}

$user //= $uri_user || 'anonymous';
$password //= $uri_pwd || $ENV{USER}.'@nowhere.net';

my $startdate=parse_gnss_date("now-$startage");
my $enddate=parse_gnss_date("now-$endage");

if( $verbose )
{
    print "Mirroring options:\n"; 
    print "Remote host:      $host\n";
    print "Remote user:      $user\n";
    print "Remote base dir:  $basedir\n";
    print "Remote directory: $remotedir\n";
    print "Remote file RE:   $remotefilere\n";
    print "Target file path: $targetpath\n";
    printf "Start date:       %04d:%03d\n",seconds_yearday($startdate);
    printf "End date:         %04d:%03d\n",seconds_yearday($enddate);
    if( $allcodes )
    {
        print "Downloading:      all codes\n";
    }
    else
    {
        printf "Downloading:      %d codes\n",scalar(keys %$validcodes);
    }
    printf "FTP timeout:     $timeout\n";
}

$targetpath =~ s/^\~/$confdir/;

my @months=split(' ','jan feb mar apr may jun jul aug sep oct nov dec');
my @umonths=();
my @ccmonths=();
foreach my $mon (@months) 
{
    push(@umonths,uc($mon));
    my $ccmon=$mon;
    substr($ccmon,0,1)=uc(substr($ccmon,0,1));
    push(@ccmonths,uc($mon));
}

# @valid_dates is a list of possible date options that can be matched against fields directory/filenames.
# This is the starting point for filtering out the candidate files.

my @valid_dates=();
for( my $i = $startdate; $i <= $enddate; $i += $SECS_PER_DAY )
{
    my($yy,$mm,$dd)=(seconds_ymdhms($i));
    my($yy2,$skip,$ddd)=(time_elements($i));
    push(@valid_dates,{
            yyyy=>sprintf("%04d",$yy),
            yy=>sprintf("%02d",$yy%100),
            mm=>sprintf("%02d",$mm),
            ddd=>sprintf("%03d",$ddd),
            dd=>sprintf("%02d",$dd),
            mmm=>$months[$mm-1],
            MMM=>$umonths[$mm-1],
            Mmm=>$ccmonths[$mm-1],
        });
}

# @dirparts is an array of components of the remote file path.  Each in turn
# is used to filter down the valid options before passing on to the next directory.

my %patterns=(
    'yyyy' => '\d\d\d\d',
    'yy' =>   '\d\d',
    'mm' =>   '\d\d',
    'mmm' =>  '('.join('|',@months).')',
    'Mmm' =>  '('.join('|',@ccmonths).')',
    'MMM' =>  '('.join('|',@umonths).')',
    'dd'  =>  '\d\d',
    'ddd' =>  '\d\d\d',
    'code' => '[a-zA-Z0-9]{4}',
);

my @dirparts;
foreach my $p ( split(/\//,$remotedir),$remotefilere)
{
    next if $p eq '';
    $p =~ s/\./\\./g;
    $p =~ s/\?/./g;
    $p =~ s/\*/.*/g;
    $p =~ s/(\{(\w+)\})/
             exists $patterns{$2} ?
                '(?<'.$2.'>'.$patterns{$2}.')' :
                $1
                /exg;
    $p = '^'.$p.'$';

    my @fields=( $p =~ /\?\<(\w+)\>/g );
    push( @dirparts, { re=>qr/$p/, fields=>\@fields });
}


print "Connecting to $host\n" if $verbose;
my $ftp=Net::FTP->new($host, Timeout=>$timeout) || die "Cannot connect to $host\n";
$ftp->login($user,$password) || die "Cannot login as $user to $host\n";
$ftp->binary();
$ftp->cwd($basedir) || die "Cannot cd to $basedir on $host\n";
downloadDir( $ftp, $basedir, \@valid_dates, \@dirparts );
$ftp->quit();

#  filteredOptions:
#
#  $options is an array of currently valid options.  Each is a hash with a set of keys and values that match
#  $dirname is the name to filter the list against
#  $dirpart is the defines the structure of $dirname as a regular expression with named capture groups and a list of 
#     capture group names (fields)
#
#  Each currently valid option is tested against the name.  If all the fields in the name match currently existing
#  value in the option then it passes. Options that pass have any other fields in the name added to them.
#  
#  The field "code" is treated specially and is additionally matched against valid codes

sub filteredOptions
{
    my($options,$dirname,$dirpart) = @_;
    my @options=();
    my $fields=$dirpart->{fields};
    my $re=$dirpart->{re};
    my %fieldvalues=();
    return [] if $dirname !~ /$re/;
    foreach my $f (@$fields)
    {
        $fieldvalues{$f}=$+{$f};
    }

    my $code=$fieldvalues{code};
    return [] if $code ne '' && ! $allcodes && ! $validcodes->{uc($code)};

    my @matches=();

    foreach my $opt (@$options)
    {
        my $ok=1;
        foreach my $k (@$fields)
        {
            next if exists $opt->{$k} && $opt->{$k} ne $fieldvalues{$k};
            $ok = 0;
            last;
        }
        next if $ok;
        my %match=%$opt;
        while ( my ($k,$v) = each %fieldvalues ) { $match{$k}=$v; }
        push( @matches, \%match );
    }
    return \@matches;
}

sub downloadDir
{
    my( $ftp, $dirname, $options, $dirparts ) = @_;
    print "Processing $dirname\n" if $verbose;
    $ftp->cwd($dirname) || print "** Cannot access remote directory $dirname\n";
    my ($dirs, $files) = parseDir( $ftp->dir() );
    my @parts=@$dirparts;
    my $dirpart=shift(@parts);

    # If we are not at the list part of $dirparts, then we are matching against directories.
    # Recursively call this routine for valid directories.

    if( @parts )
    {
        return if ! ref $dirs;
        foreach my $dir (@$dirs)
        {
            my $opts=filteredOptions($options,$dir,$dirpart);
            next if ! @$opts;
            downloadDir( $ftp, "$dirname/$dir", $opts, \@parts );
            sleep($pause);
        }
        return;
    }

    # Otherwise we are matching against files for downloading, so 
    # try each one in turn

    return if ! ref $files;
    foreach my $file (sort keys %$files)
    {
        # Does it match the current filtered options
        my $opts=filteredOptions($options,$file,$dirpart);
        next if ! @$opts;

        # Build the target name. There may be more than one filtered option
        # remaining, so make sure that if so they uniquely define a target name.
        # If not then fail the download.

        my $target='';
        foreach my $opt (@$opts)
        {
            my $tgtname=$targetpath;
            $opt->{filename} = $file;
            $tgtname =~ s/\{(\w+)\}/$opt->{$1}/eg;
            if( $tgtname =~ /\{\w+\}/ )
            {
                print "** Unresolved target file name $tgtname for $dirname/$file\n";
                $target='';
                last;
            }
            if( $target && $tgtname ne $target )
            {
                print "** Ambiguous target name for $dirname/$file ($target,$tgtname)\n";
                $target='';
                last;
            }
            $target = $tgtname;
        }
        next if ! $target;

        # Now try and download the file
        if( -e $target )
        {
            if( ! -f $target )
            {
                print "** Cannot create file at $target - something is already there\n";
                next;
            }
            # Check the size - if it matches then assume the file is up to date.
            if( ! $ignoresize && $ftp->size($file) == -s $target )
            {
                print "$target is already available and of the correct size\n" if $verbose;
                next;
            }
        }

        # Ensure the target directory exists.. 
        my $tgtpath=$target;
        my $tgtname=$target;
        $tgtpath =~ s/[^\\\/]*$//;
        $tgtpath =~ s/[\\\/]$//;
        $tgtname =~ s/.*[\\\/]//;

        if( ! -d $tgtpath && ! File::Path::make_path($tgtpath) )
        {
            print "** Cannot create target directory at $tgtpath\n";
            next;
        }

        # Download to a temporary file first to ensure failed 
        # downloads don't generate incomplete files.

        my $tmp=$tgtpath.'/.download.'.$tgtname.'.tmp';
        unlink($tmp);

        if( ! $ftp->get($file,$tmp) )
        {
            print "** Failed to download $dirname/$file\n";
            print $ftp->message,"\n";
            unlink($tmp);
        }
        elsif( ! move($tmp,$target) )
        {
            print "Failed to overwrite $target\n";
            unlink($tmp);
        }
        else
        {
            print "Successfully downloaded $target\n" if $verbose;
        }
        sleep($pause);
    }
}

# Simplistic parsing of a directory listing.
#
# Assumes that directory and file names do not include space characters!
#
# Returns a list of direcories, and a hash of files keyed on the filename and
# having value the same as the directory entry

sub parseDir
{
    my(@listing)=@_;
    my $dirs=[];
    my $files={};
    @listing=@{$listing[0]} if ref $listing[0];
    foreach my $l (@listing)
    {
        $l =~ s/^\s+//;
        $l =~ s/\s+$//;
        my $isdir=$l=~/^d/;
        my $name=(split(' ',$l))[-1];
        # Skip names starting with '.";
        next if $name eq '';
        next if $name =~ /^\./;
        if( $isdir )
        {
            push(@$dirs,$name);
        }
        else
        {
            $files->{$name}=$l;
        }
    }
    return $dirs,$files;
}

__END__

# Example configuration file:

# RemoteUri is the the base of the remote directory

RemoteUri=ftp://ftp.geonet.org.nz/rawgps

RemoteUser=anonymous

RemotePassword=positionz@linz.govt.nz

TimeOut 30

# Delay added after each successful download to be polite to server

DownloadWait 0.5

# RemotePath is the path to the files to download.
#
# Can include {yyyy},{yy},{mmm},{mm},{ddd},{dd} which will map to 
# the corresponding date strings (mmm is 3 letter month name. Will
# also accept Mmm, MMM for different capitalisation)
# 
# These will be replaced with values corresponding to the maximum number
# of days before the current date to process.  Can also include ? for any
# character, and * for any set of characters.

RemoteDir=/{ddd}

# RemoteFileRe.  Remote file names are matched against this regular expression.
# Files that match are candidates for downloading.  Regular expression 
# capture groups (?<xxx>...) capture names that can be used in the target
# path.  The special name (?<code>) must match a target code if a list
# of codes is defined.  Similarly groups with the same name as date 
# components must match that component.

RemoteFileRe={code}{yyyy}\d{8}[a-z].T02

# Target directory.  This can be based absolute, or relative to the location
# of the configuration file (defined as ~).  Can include time components
# as for RemovePath.  Can also include {filename} to use the source filename.

TargetPath=~/{yyyy}/{ddd}/{filename}

# Codes to download

Codes KAIK RGRE
Codes SCTB

# Number of days before current date to start and end download

StartAge 30
EndAge 0
