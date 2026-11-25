=head1 LINZ::GNSS::AwsS3Bucket

Module to interface applications to an Amazon web services S3 bucket.

Requests are sent to a helper process, a python3 script using boto3, which is started the
first time it is needed and then serves all the requests from this process.  This avoids
starting the aws client for every request, which takes about a second, so that checking
for and retrieving many files (such as GETDATA checking a cache of RINEX files) is fast.

If the helper cannot be started (no python3 or no boto3) the module runs the aws client
for each request, as it always used to.  If a running helper fails it is restarted once,
and if that fails the module uses the aws client from then on.  The directory operations
putDir, syncToBucket and syncFromBucket always use the aws client.

The client is chosen by the client argument, the s3_client configuration item, or the
environment variable LINZGNSS_S3_CLIENT, in that order: auto (the default) as above, aws to
always use the aws client, or python to require the helper (failing if it cannot be started).
LINZGNSS_S3_PYTHON is the python interpreter (default python3 on the PATH), and
LINZGNSS_S3_TIMEOUT the time allowed for a single request in seconds (default 600).

A helper belongs to the process that started it.  A process forked after it started (such
as a parallel download worker) starts its own helper the first time it makes a request.

The helper only understands the --profile, --region and --endpoint-url aws parameters.
If aws_parameters contains anything else the aws client is used.

Note that this was initially tried with Net::Amazon::S3 module (version 0.80), but
at the available version on ubuntu this failed as it is using http communications
and not using API endpoints.

=cut

use strict;

package LINZ::GNSS::AwsS3Bucket;

use IPC::Run qw(run);
use POSIX qw(strftime);
use Time::Local;
use Date::Parse;
use Log::Log4perl;
use File::Which;
use Carp qw(croak);
use JSON::PP;

our $default_awsbin=which('aws') || '/usr/bin/aws';
our $idenv='AWS_ACCESS_KEY_ID';
our $keyenv='AWS_SECRET_ACCESS_KEY';

# Time allowed for a request to the python helper, and for it to start (seconds)
our $default_timeout=600;
our $startup_timeout=60;

# Running helpers, by process id and connection settings, and the connection settings for
# which a helper could not be started (so that it is not tried again for every bucket)
our %helpers=();
our %helper_failed=();

=head2 LINZ::GNSS::AwsS3Bucket->new( ... )

Opens an S3 bucket for operating with.  Takes the following optional arguments:

=over

=item config: A LINZ::GNSS::Config configuration from which to retrieve parameters s3_bucket, s3_prefix, s3_aws_parameters, s3_aws_client, s3_client

=item bucket: The name of the bucket

=item prefix: A prefix to apply to all keys passed to this module

=item aws_parameters: Additional parameters passed to the aws client program

=item aws_client: The location of the aws client program (if not the default location)

=item client: auto, aws or python (see above).  If not set by this or the configuration it is
taken from the environment variable LINZGNSS_S3_CLIENT, and defaults to auto.

=item access_key_id, secret_access_key: AWS credentials to use instead of the default
credential chain

=back

=cut

sub new()
{
    my( $class, %args ) = @_;
    my $self=bless {}, $class;
    my $cfg=$args{config};
    $self->{logger}=Log::Log4perl->get_logger('LINZ.GNSS.AwsS3Bucket');
    $self->{access_key_id}=$args{access_key_id};
    $self->{secret_access_key}=$args{secret_access_key};
    $self->{aws_endpoint} = $ENV{AWS_ENDPOINT_URL};
    my $cfg_prefix=$args{config_prefix} || '';
    foreach my $item ('bucket','prefix','aws_parameters','aws_client','debug_aws','client')
    {
        my $value=$args{$cfg_prefix.$item};
        $value = $cfg->get('s3_'.$item,'') if $cfg && ! $value;
        $self->{$item}=$value || '';
    }
    $self->{aws_client} ||= $default_awsbin;
    $self->{debug_aws}=1 if ($ENV{LINZGNSS_DEBUG_AWS} // '') eq 'debug';
    my $bucket=$self->bucket;
    $self->error("LINZ::GNSS::AwsS3Bucket::new - bucket name not defined\n") if ! $bucket;
    my $prefix=$self->prefix;
    if( $prefix )
    {
        $prefix=~ s/\/*$/\//;
        $prefix=~ s/^\///;
        $self->{prefix}=$prefix;
    }
    $self->_selectClient();
    if( ! $self->_usingHelper )
    {
        my $awsbin=$self->{aws_client};
        if( ! -x $awsbin )
        {
            $self->error("Cannot find or use aws command $awsbin");
            return;
        }
    }
    # Check the bucket exists
    my $ok;
    my $reply=$self->_request({op=>'head_bucket',bucket=>$bucket});
    if( $reply )
    {
        $ok=$reply->{ok};
        $self->debug("Cannot access bucket $bucket: $reply->{error}") if ! $ok;
    }
    else
    {
        ($ok)=$self->_runAws('s3api','head-bucket','--bucket',$bucket);
    }
    if( ! $ok )
    {
        $self->error("LINZ::GNSS::AwsS3Bucket::new Cannot find or access S3 bucket $bucket\n");
        return;
    }
    return $self;
}

=head2 Access functions $self->...

Functions to access attributes bucket, prefix

=cut

sub bucket { return $_[0]->{bucket}}
sub prefix { return $_[0]->{prefix}}
sub logger { return $_[0]->{logger}}
sub _aws_client { return $_[0]->{aws_client}}
sub _aws_endpoint { return $_[0]->{aws_endpoint}}
sub _aws_parameters { return $_[0]->{aws_parameters}}
sub _debug_aws { my $self=shift; return $self->{debug_aws} || 0; }

=head2 $bucket->client

Returns the client used for requests: python (the helper) or aws (the aws client).

=cut

sub client { return $_[0]->_usingHelper ? 'python' : 'aws'; }

sub error
{
    my($self,@msg)=@_;
    my $errmsg=join("",@msg);
    $self->logger->error($errmsg) if $self->logger;
    croak($errmsg."\n");
}

sub debug
{
    my($self,@msg)=@_;
    return if ! $self->logger;
    my $errmsg=join("",@msg);
    $self->logger->debug($errmsg);
}

# Choose whether to use the python helper, and start it if it is not already running

sub _selectClient
{
    my($self)=@_;
    my $mode=lc($self->{client} || $ENV{LINZGNSS_S3_CLIENT} || 'auto');
    $self->error("Invalid S3 client \"$mode\": must be auto, aws or python") if $mode !~ /^(auto|aws|python)$/;
    $self->{helperkey}=undef;
    $self->{requirehelper}= $mode eq 'python';
    return if $mode eq 'aws';
    my $opts=$self->_helperOptions();
    if( ! $opts )
    {
        my $msg="The python S3 helper does not support the aws parameters \"".$self->_aws_parameters."\"";
        $self->error($msg) if $self->{requirehelper};
        $self->logger->info("$msg: using the aws client");
        return;
    }
    my $key=join("\0",map {$_.'='.$opts->{$_}} sort keys %$opts);
    $self->{helperopts}=$opts;
    $self->{helperkey}=$key;
    if( ! $self->_helper() )
    {
        my $msg="Cannot start the python S3 helper: ".$helper_failed{$key};
        $self->{helperkey}=undef;
        $self->error($msg) if $self->{requirehelper};
        $self->logger->info("$msg: using the aws client");
    }
}

sub _usingHelper { return defined($_[0]->{helperkey}); }

# The helper settings, or undef if the aws parameters include options it cannot use

sub _helperOptions
{
    my($self)=@_;
    my $haskeys=$self->{access_key_id} && $self->{secret_access_key};
    my %opts=(
        python=>$ENV{LINZGNSS_S3_PYTHON} || which('python3') || '',
        endpoint=>$self->_aws_endpoint || '',
        access_key_id=>$haskeys ? $self->{access_key_id} : '',
        secret_access_key=>$haskeys ? $self->{secret_access_key} : '',
        profile=>'',
        region=>'',
        );
    my @params=split(' ',$self->_aws_parameters);
    while( @params )
    {
        my $param=shift(@params);
        my ($name,$value);
        if( $param =~ /^--(profile|region|endpoint-url)=(.*)$/ )
        {
            ($name,$value)=($1,$2);
        }
        elsif( $param =~ /^--(profile|region|endpoint-url)$/ && @params )
        {
            ($name,$value)=($1,shift(@params));
        }
        else
        {
            return;
        }
        $name='endpoint' if $name eq 'endpoint-url';
        $opts{$name}=$value;
    }
    return \%opts;
}

# The helper for this process, started if it is not already running.  Returns undef if
# it cannot be started.

sub _helper
{
    my($self)=@_;
    my $key=$self->{helperkey};
    return if ! defined $key;
    my $pkey="$$\0$key";
    my $helper=$helpers{$pkey};
    return $helper if $helper && $helper->alive;
    delete $helpers{$pkey};
    return if exists $helper_failed{$key};
    $helper=eval
    {
        LINZ::GNSS::AwsS3Bucket::Helper->new($self->{helperopts},$self->logger,$self->_debug_aws);
    };
    if( ! $helper )
    {
        my $error=_message($@ || "unknown error");
        $helper_failed{$key}=$error;
        return;
    }
    $helpers{$pkey}=$helper;
    return $helper;
}

# Send a request to the helper.  Returns the reply (with ok true or false according to whether
# S3 succeeded), or undef if the aws client is to be used instead.  If the helper fails it is
# restarted once, and if it fails again the aws client is used from then on.

sub _request
{
    my($self,$request,$timeout)=@_;
    return if ! $self->_usingHelper;
    $timeout ||= $ENV{LINZGNSS_S3_TIMEOUT} || $default_timeout;
    foreach my $attempt (1,2)
    {
        my $helper=$self->_helper();
        last if ! $helper;
        my $reply=eval { $helper->request($request,$timeout) };
        return $reply if $reply;
        my $error=_message($@ || "no reply");
        $self->logger->warn("Python S3 helper failed: $error");
        $helper->stop();
        delete $helpers{"$$\0".$self->{helperkey}};
    }
    my $msg="Cannot use the python S3 helper";
    $self->{helperkey}=undef;
    $self->error($msg) if $self->{requirehelper};
    $self->logger->warn("$msg: using the aws client from now on");
    return;
}

# Error message without the location added by croak

sub _message
{
    my($error)=@_;
    $error =~ s/\s+at\s+\S+\s+line\s+\d+\.?\s*$//;
    $error =~ s/\s+$//;
    return $error;
}

# Modification time from the mtime metadata written by putFile (UTC yyyy-mm-ddThh:mm:ss)

sub _metadataMtime
{
    my($mtime)=@_;
    return if ! defined($mtime);
    return if $mtime !~ /^(\d\d\d\d)\-(\d\d)\-(\d\d)T(\d\d)\:(\d\d)\:(\d\d)$/;
    my($y,$m,$d,$hr,$mi,$sc)=($1,$2,$3,$4,$5,$6);
    return timegm($sc,$mi,$hr,$d,$m-1,$y-1900);
}

=head2 $bucket->_runAws($command,$subcommand,@params)

Runs an AWS command

=cut

sub _runAws
{
    my($self,$command,$subcommand,@params)=@_;
    my $awsbin=$self->_aws_client;
    my $awsparams=$self->_aws_parameters;
    my @awsparams=split(' ',$awsparams);
    my @command=($awsbin);
    push(@command,'--debug') if $self->_debug_aws > 1;
    push(@command,'--only-show-errors') if $command eq 's3' && $self->_debug_aws < 1;
    push(@command,'--endpoint-url',$self->_aws_endpoint) if $self->_aws_endpoint;
    push(@command,$command,$subcommand,@awsparams,@params);
    my $in='';
    my $out;
    my $err;
    my $ok=0;
    my $oldid=$ENV{$idenv};
    my $oldkey=$ENV{$keyenv};
    eval
    {
        my $cmdstr=join(" ",@command);
        my $access_key_id=$self->{access_key_id};
        my $secret_access_key=$self->{secret_access_key};
        if( $access_key_id && $secret_access_key )
        {
            $ENV{$idenv}=$access_key_id;
            $ENV{$keyenv}=$secret_access_key;
        }
        foreach my $k (sort keys %ENV)
        {
            next if $k !~ /^AWS/;
            next if $k eq $idenv || $k eq $keyenv;
            $cmdstr .= "\n$k=$ENV{$k}";
        }
        $self->debug($cmdstr) if $self->_debug_aws > 1;
        $ok=run(\@command,\$in,\$out,\$err);
        $self->debug("Output: $out") if $self->_debug_aws > 1;
        $self->debug("Error: $err") if $self->_debug_aws > 0;
    };
    if( $@ )
    {
        $self->error("aws command failed: $@");
        return 0;
    }
    delete $ENV{$idenv};
    delete $ENV{$keyenv};
    $ENV{$idenv}=$oldid if $oldid;
    $ENV{$keyenv}=$oldkey if $oldkey;
    return wantarray ? ($ok,$out,$err) : $ok;
}

=head2 $bucket->fileKey($file)

Get the key used to identify a file in S3

=cut

sub fileKey
{
    my ($self, $name) = @_;
    $name=~s/^\///;
    my $key=$self->prefix.$name;
    return $key;
}

=head2 $bucket->fileUrl($file)

Get the url used to identify a file in S3

=cut

sub fileUrl
{
    my ($self, $name) = @_;
    my $bucket=$self->bucket;
    my $key=$self->fileKey($name);
    my $s3url="s3://$bucket/$key";
    return $s3url;
}

=head2 $bucket->putFile($sourcefile,$file)

Copy a file to the S3 bucket store for the process. Also adds
metadata for file modification date

=cut

sub putFile
{
    my($self,$sourcefile,$name)=@_;
    if(! -f $sourcefile || ! -r $sourcefile )
    {
        $self->error("Cannot copy $sourcefile to S3: not a file");
        return 0;
    }
    my $s3url=$self->fileUrl($name);
    my $timetag=(stat($sourcefile))[9];
    my $utctag=strftime("%Y-%m-%dT%H:%M:%S",gmtime($timetag));
    my $reply=$self->_request({op=>'put',bucket=>$self->bucket,key=>$self->fileKey($name),
        source=>$sourcefile,metadata=>{mtime=>$utctag}});
    if( $reply )
    {
        my $ok=$reply->{ok} ? 1 : 0;
        my $error=$reply->{error} // '';
        $self->debug("Error: $error") if ! $ok && $self->_debug_aws > 0;
        return wantarray ? ($ok,'',$error) : $ok;
    }
    my @params=('--metadata',"mtime=$utctag",$sourcefile,$s3url);
    my ($ok,$result,$error)=$self->_runAws('s3','cp',@params);
    return wantarray ? ($ok,$result,$error) : $ok;
}


=head2 $bucket->putDir($sourcedir,$dir)

Recursively copy a directory contents to the bucket.  (Note
this does not include modification time metadata).  Always uses the aws client.

=cut

sub putDir
{
    my($self,$sourcedir,$dir)=@_;
    if(! -d $sourcedir )
    {
        $self->error("Cannot copy directory to $sourcedir to S3: not a directory");
        return 0;
    }
    $dir .= '/' if $dir =~ /[^\/]$/;
    my $s3url=$self->fileUrl($dir);
    my @params=($sourcedir.'/',$s3url);
    my ($ok,$result,$error)=$self->_runAws('s3','cp','--recursive',@params);
    return wantarray ? ($ok,$result,$error) : $ok;
}

=head2 $bucket->getFile($name,$targetfile)

Copy a file from the S3 bucket to $targetfile, setting its modification time from the
mtime metadata written by putFile, or else from the S3 last modified time.

=cut

sub getFile
{
    my($self,$name,$targetfile)=@_;
    unlink($targetfile) if -e $targetfile;
    if( -e $targetfile )
    {
        $self->error("Cannot overwrite existing file $targetfile");
        return 0;
    }
    my $reply=$self->_request({op=>'get',bucket=>$self->bucket,key=>$self->fileKey($name),
        target=>$targetfile});
    if( $reply )
    {
        if( ! $reply->{ok} )
        {
            my $code=$reply->{code} // '';
            $self->error("File $name not available S3") if $code eq 'NotFound';
            $self->error("Cannot retrieve file $name from S3: $reply->{error}");
            return 0;
        }
        if( ! -e $targetfile )
        {
            $self->error("Cannot retrieve file $name from S3");
            return 0;
        }
        my $mtime=_metadataMtime($reply->{mtime}) // $reply->{last_modified};
        utime($mtime,$mtime,$targetfile) if $mtime;
        return wantarray ? (1,'','') : 1;
    }
    my $filestat=$self->fileStats($name);
    if( ! $filestat )
    {
        $self->error("File $name not available S3");
        return 0;
    }
    my $s3url=$self->fileUrl($name);
    my @params=($s3url,$targetfile);
    my ($ok,$result,$error)=$self->_runAws('s3','cp',@params);
    if( ! -e $targetfile )
    {
        $self->error("Cannot retrieve file $name from S3: $error");
        return 0;
    }
    my $mtime=$filestat->{mtime};
    utime($mtime,$mtime,$targetfile) if $mtime;
    return wantarray ? ($ok,$result,$error) : $ok;
}

=head2 $bucket->deleteFile($file)

Delete a file from the S3 bucket.

=cut

sub deleteFile
{
    my($self,$name)=@_;
    my $bucket=$self->bucket;
    my $key=$self->fileKey($name);
    my $reply=$self->_request({op=>'delete',bucket=>$bucket,key=>$key});
    if( $reply )
    {
        $self->debug("Error: $reply->{error}") if ! $reply->{ok} && $self->_debug_aws > 0;
        return $reply->{ok} ? 1 : 0;
    }
    my $s3url="s3://$bucket/$key";
    return $self->_runAws('s3','rm',$s3url);
}

=head2 $bucket->fileStats($file)

Return {size=>size, mtime=>mtime} if a file exists, undef otherwise

=cut

sub fileStats
{
    my($self,$name)=@_;
    my $bucket=$self->bucket;
    my $key=$self->fileKey($name);
    my $reply=$self->_request({op=>'head',bucket=>$bucket,key=>$key});
    if( $reply )
    {
        if( ! $reply->{ok} )
        {
            $self->debug("Error: $reply->{error}") if $self->_debug_aws > 0;
            return;
        }
        my $result={};
        $result->{size}=$reply->{size} if defined $reply->{size};
        my $mtime=_metadataMtime($reply->{mtime}) // $reply->{last_modified};
        $result->{mtime}=$mtime if defined $mtime;
        return $result;
    }
    my ($ok,$head,$err)=$self->_runAws('s3api','head-object','--bucket',$bucket,'--key',$key);
    my $result;
    if( $ok )
    {
        $result={};
        if( $head =~ /\"ContentLength\"\s*\:\s*(\d+)[\s\,\}]/ )
        {
            $result->{size}=$1;
        }
        if( $head =~ /\"mtime\"\s*\:\s*\"(\d\d\d\d\-\d\d\-\d\dT\d\d\:\d\d\:\d\d)\"/s )
        {
            $result->{mtime}=_metadataMtime($1);
        }
        elsif( $head =~ /\"LastModified\"\s*\:\s*\"([^\"]+\d\d\d\d\s+\d\d\:\d\d\:\d\d[^\"]*)\"/ )
        {
            $result->{mtime}=str2time($1);

        }
    }
    return $result;
}

=head2 $bucket->fileExists($file)

Returns true (1) if a file exists, false (0) otherwise.

=cut

sub fileExists { my ($self,$file)=@_; return $self->fileStats($file) ? 1 : 0; }

=head2 $bucket->listFiles($dir)

Returns a reference to an array of the names (without the directory part)
of the files directly under the "directory" $dir.  Keys in deeper
"subdirectories" are not included.  Returns an empty list if there are no
matching files, and croaks if the bucket cannot be listed.

=cut

sub listFiles
{
    my($self,$dir)=@_;
    my $bucket=$self->bucket;
    my $prefix=$self->fileKey($dir);
    $prefix .= '/' if $prefix ne '' && $prefix !~ /\/$/;
    my $keys;
    my $reply=$self->_request({op=>'list',bucket=>$bucket,prefix=>$prefix});
    if( $reply )
    {
        $self->error("Cannot list files in s3://$bucket/$prefix: $reply->{error}") if ! $reply->{ok};
        $keys=$reply->{keys};
    }
    else
    {
        my ($ok,$out,$err)=$self->_runAws('s3api','list-objects-v2',
            '--bucket',$bucket,'--prefix',$prefix,'--delimiter','/',
            '--query','Contents[].Key','--output','json');
        $self->error("Cannot list files in s3://$bucket/$prefix: $err") if ! $ok;
        $keys=($out =~ /\S/) ? decode_json($out) : undef;
    }
    $keys=[] if ref($keys) ne 'ARRAY';
    my @files=grep { $_ ne '' } map { substr($_,length($prefix)) } @$keys;
    return \@files;
}

=head2 $bucket->syncToBucket($sourcedir,$targetkey,[delete=>1])

Synchonise files from source directory to target "directory".
Runs aws s3 sync --delete unless delete=>0.  Always uses the aws client.

=cut

sub syncToBucket
{
    my($self,$sourcedir,$targetkey,%opts)=@_;
    if( ! -d $sourcedir )
    {
        $self->error("Cannot sync to S3: $sourcedir is not a directory");
        return 0;
    }
    if( ! $targetkey )
    {
        # In principle could sync entire directory structure but
        # don't want to permit this.
        $self->error("Cannot sync to S3: target directory name cannot be empty");
        return 0;
    }
    my $s3url=$self->fileUrl($targetkey);
    my @cmd=('s3','sync');
    push(@cmd,'--delete') unless exists $opts{delete} && ! $opts{delete};
    push(@cmd,$sourcedir,$s3url);
    my ($ok,$result,$error)=$self->_runAws(@cmd);
    return wantarray ? ($ok,$result,$error) : $ok;
}

=head2 $bucket->syncFromBucket($sourcekey,$targetdir,[delete=>1])

Synchronise files from source directory to target "directory".
Runs aws s3 sync --delete unless delete=>0.  Always uses the aws client.

=cut

sub syncFromBucket
{
    my($self,$sourcekey,$targetdir,%opts)=@_;
    if( ! $sourcekey )
    {
        # In principle could sync entire directory structure but
        # don't want to permit this.
        $self->error("Cannot sync from S3: source directory name cannot be empty");
        return 0;
    }
    if( ! -d $targetdir )
    {
        $self->error("Cannot sync to S3: $targetdir is not a directory");
        return 0;
    }
    my $s3url=$self->fileUrl($sourcekey);
    my @cmd=('s3','sync');
    push(@cmd,'--delete') unless exists $opts{delete} && ! $opts{delete};
    push(@cmd,$s3url,$targetdir);
    my ($ok,$result,$error)=$self->_runAws(@cmd);
    return wantarray ? ($ok,$result,$error) : $ok;
}

# Stop the helpers started by this process when it exits.  (A forked process leaves its
# parent's helpers alone.)  $? is the exit status of the program, so is not changed.

END
{
    local $?;
    foreach my $helper (values %helpers)
    {
        $helper->stop() if $helper;
    }
}

=head1 LINZ::GNSS::AwsS3Bucket::Helper

The python helper process.  It reads one JSON request per line from its standard input
and writes one JSON reply per line to its standard output.  Each request has an id and an
op (head_bucket, head, get, put, delete, list, or exit), and each reply has the request id
and ok (true if S3 succeeded) with the result, or error and code if it did not.  When it
starts it writes a line with ready true, or ready false and an error.

Credentials and connection settings are passed in its environment, not in requests, so
that they are not written to debug logs.

=cut

package LINZ::GNSS::AwsS3Bucket::Helper;

use IPC::Run;
use JSON::PP;
use Carp qw(croak);

our $script = <<'PYTHON';
import sys
# Don't import modules from the current directory
sys.path = [p for p in sys.path if p not in ("", ".")]
import calendar
import json
import os


def send(obj):
    sys.stdout.buffer.write(json.dumps(obj).encode("utf-8") + b"\n")
    sys.stdout.buffer.flush()


# Perl sends its byte strings as latin-1 characters, so convert them back to the bytes they were
def text(value):
    try:
        return value.encode("latin-1").decode("utf-8")
    except (UnicodeError, AttributeError):
        return value


def path(value):
    try:
        return os.fsdecode(value.encode("latin-1"))
    except (UnicodeError, AttributeError):
        return value


def perl_text(value):
    return value.encode("utf-8").decode("latin-1")


def setting(name):
    return os.environ.get(name) or None


try:
    import boto3
    from botocore.config import Config
    from botocore.exceptions import ClientError
except Exception as exc:
    send({"ready": False, "error": "Cannot import boto3: %s" % exc})
    sys.exit(1)

try:
    session = boto3.session.Session(
        aws_access_key_id=setting("LINZGNSS_S3_ACCESS_KEY_ID"),
        aws_secret_access_key=setting("LINZGNSS_S3_SECRET_ACCESS_KEY"),
        profile_name=setting("LINZGNSS_S3_PROFILE"),
        region_name=setting("LINZGNSS_S3_REGION"),
    )
    s3 = session.client(
        "s3",
        endpoint_url=setting("LINZGNSS_S3_ENDPOINT"),
        config=Config(retries={"max_attempts": 5, "mode": "standard"}),
    )
except Exception as exc:
    send({"ready": False, "error": "Cannot create the S3 client: %s" % exc})
    sys.exit(1)


def epoch(value):
    return calendar.timegm(value.utctimetuple()) if value else None


def op_head_bucket(req):
    s3.head_bucket(Bucket=req["bucket"])
    return {}


def op_head(req):
    resp = s3.head_object(Bucket=req["bucket"], Key=text(req["key"]))
    return {
        "size": resp.get("ContentLength"),
        "mtime": resp.get("Metadata", {}).get("mtime"),
        "last_modified": epoch(resp.get("LastModified")),
    }


def op_get(req):
    resp = s3.get_object(Bucket=req["bucket"], Key=text(req["key"]))
    target = path(req["target"])
    tmpfile = target + ".s3tmp"
    try:
        with open(tmpfile, "wb") as f:
            for chunk in resp["Body"].iter_chunks(1024 * 1024):
                f.write(chunk)
        os.replace(tmpfile, target)
    except BaseException:
        if os.path.exists(tmpfile):
            os.remove(tmpfile)
        raise
    return {
        "size": resp.get("ContentLength"),
        "mtime": resp.get("Metadata", {}).get("mtime"),
        "last_modified": epoch(resp.get("LastModified")),
    }


def op_put(req):
    s3.upload_file(
        path(req["source"]),
        req["bucket"],
        text(req["key"]),
        ExtraArgs={"Metadata": req.get("metadata") or {}},
    )
    return {}


def op_delete(req):
    s3.delete_object(Bucket=req["bucket"], Key=text(req["key"]))
    return {}


def op_list(req):
    keys = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=req["bucket"], Prefix=text(req["prefix"]), Delimiter="/"):
        keys.extend(perl_text(item["Key"]) for item in page.get("Contents", []))
    return {"keys": keys}


OPS = {
    "head_bucket": op_head_bucket,
    "head": op_head,
    "get": op_get,
    "put": op_put,
    "delete": op_delete,
    "list": op_list,
}

send({"ready": True, "python": sys.version.split()[0], "boto3": boto3.__version__})

for line in sys.stdin.buffer:
    try:
        req = json.loads(line.decode("utf-8"))
    except ValueError as exc:
        send({"id": None, "ok": False, "code": "InvalidRequest", "error": "Invalid request: %s" % exc})
        continue
    op = req.get("op")
    if op == "exit":
        break
    reply = {"id": req.get("id")}
    try:
        if op not in OPS:
            raise ValueError("Unknown operation %s" % op)
        reply.update(OPS[op](req))
        reply["ok"] = True
    except ClientError as exc:
        code = str(exc.response.get("Error", {}).get("Code", ""))
        if code in ("404", "NoSuchKey", "NotFound"):
            code = "NotFound"
        reply.update(ok=False, code=code, error=str(exc))
    except Exception as exc:
        reply.update(ok=False, code=type(exc).__name__, error=str(exc))
    send(reply)
PYTHON

sub new
{
    my($class,$opts,$logger,$debug)=@_;
    my $python=$opts->{python};
    croak("python3 not found\n") if ! $python;
    my $self=bless {
        pid=>$$,
        in=>'',
        out=>'',
        err=>'',
        nextid=>0,
        logger=>$logger,
        debug=>$debug || 0,
        }, $class;
    my %env=(
        LINZGNSS_S3_PROFILE=>$opts->{profile},
        LINZGNSS_S3_REGION=>$opts->{region},
        LINZGNSS_S3_ENDPOINT=>$opts->{endpoint},
        LINZGNSS_S3_ACCESS_KEY_ID=>$opts->{access_key_id},
        LINZGNSS_S3_SECRET_ACCESS_KEY=>$opts->{secret_access_key},
        );
    my $ready;
    {
        local @ENV{keys %env}=map { $_ // '' } values %env;
        $self->{timer}=IPC::Run::timeout($LINZ::GNSS::AwsS3Bucket::startup_timeout,name=>'python S3 helper');
        $self->{harness}=IPC::Run::start([$python,'-u','-c',$script],
            '<',\$self->{in},'>',\$self->{out},'2>',\$self->{err},$self->{timer});
    }
    $ready=eval { $self->_readReply() };
    if( ! $ready || ! $ready->{ready} )
    {
        my $error=$ready ? $ready->{error} : ($@ || 'no response');
        $error =~ s/\s+$//;
        $error .= ": ".$self->{err} if $self->{err} =~ /\S/;
        $self->stop();
        croak("$error\n");
    }
    $logger->info("Using the python S3 helper (python $ready->{python}, boto3 $ready->{boto3})");
    return $self;
}

sub alive
{
    my($self)=@_;
    return $self->{harness} && $self->{pid} == $$ && $self->{harness}->pumpable;
}

sub _readReply
{
    my($self)=@_;
    my $harness=$self->{harness};
    while( $self->{out} !~ /\n/ )
    {
        croak("helper has stopped\n") if ! $harness->pumpable;
        $harness->pump;
    }
    $self->{out} =~ s/^([^\n]*)\n//;
    my $line=$1;
    if( $self->{err} =~ /\S/ )
    {
        $self->{logger}->debug("Python S3 helper: ".$self->{err});
        $self->{err}='';
    }
    return decode_json($line);
}

sub request
{
    my($self,$request,$timeout)=@_;
    croak("helper is not running\n") if ! $self->alive;
    my $id=++$self->{nextid};
    my $json=encode_json({%$request,id=>$id});
    $self->{logger}->debug("Python S3 helper request: $json") if $self->{debug} > 1;
    $self->{in} .= $json."\n";
    $self->{timer}->start($timeout);
    my $reply=$self->_readReply();
    croak("helper reply is for request ".($reply->{id} // 'none')." not $id\n")
        if ($reply->{id} // '') ne $id;
    return $reply;
}

sub stop
{
    my($self)=@_;
    my $harness=$self->{harness};
    return if ! $harness || $self->{pid} != $$;
    $self->{harness}=undef;
    eval { $harness->kill_kill(grace=>2); };
}

1;
