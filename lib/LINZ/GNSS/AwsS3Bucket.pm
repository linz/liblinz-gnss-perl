=head1 LINZ::GNSS::AwsS3Bucket

Crude module to interface applications to an Amazon web services S3 bucket.
It is crude because this implementation simply runs the aws client program for 
all its operations.  This is inefficient, but does work!

Note that this was initially tried with Net::Amazon::S3 module (version 0.80), but
at the available version on ubuntu this failed as it is using http communications
and not using API endpoints.  

For the moment building using the aws client is the simplest/quickest approach.  If this becomes
an issue then this could be reconfigured to use the web api directly (CC 19/2/2020)

The main impact will be when searching daily processing for existing files where many
files may be tested before the required processing is identified.

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

our $default_awsbin=which('aws') || '/usr/bin/aws';
our $idenv='AWS_ACCESS_KEY_ID';
our $keyenv='AWS_SECRET_ACCESS_KEY';

=head2 LINZ::GNSS::AwsS3Bucket->new( ... )

Opens an S3 bucket for operating with.  Takes the following optional arguments:

=over

=item config: A LINZ::GNSS::Config configuration from which to retrieve parameters s3_bucket, s3_prefix, s3_aws_parameters, s3_aws_client

=item bucket: The name of the bucket

=item prefix: A prefix to apply to all keys passed to this module

=item aws_parameters: Additional parameters passed to the aws client program

=item aws_client: The location of the aws client program (if not the default location)

=back

=cut

sub new()
{
    my( $class, %args ) = @_;
    my $self=bless {}, $class;
    $self->{aws_client}=$default_awsbin;
    my $cfg=$args{config};
    $self->{logger}=Log::Log4perl->get_logger('LINZ.GNSS.AwsS3Bucket');
    $self->{aws_access_key_id}=$args{access_key_id};
    $self->{aws_secret_access_key}=$args{secret_access_key};
    my $cfg_prefix=$args{config_prefix} || '';
    foreach my $item ('bucket','prefix','aws_parameters','aws_client','debug_aws')
    {
        my $value=$self->{$item} || '';
        $value=$args{$cfg_prefix.$item} if ! $value;
        $value = $cfg->get('s3_'.$item,'') if $cfg && ! $value;
        $self->{$item}=$value;
    }
    $self->{debug_aws}=1 if  $ENV{LINZGNSS_DEBUG_AWS} eq 'debug';
    my $bucket=$self->bucket;
    $self->error("LINZ::GNSS::AwsS3Bucket::new - bucket name not defined\n") if ! $bucket;
    my $awsbin=$self->{aws_client};
    if( ! -x $awsbin )
    {
        $self->error("Cannot find or use aws command $awsbin");
        return;
    }
    # Check the bucket exists
    my ($ok,$result,$error) =$self->_runAws('s3api','head-bucket','--bucket',$bucket);
    if( ! $ok )
    {
        $self->error("LINZ::GNSS::AwsS3Bucket::new Cannot find or access S3 bucket $bucket\n");
        return;
    }
    my $prefix=$self->prefix;
    if( $prefix )
    {
        $prefix=~ s/\/*$/\//;
        $prefix=~ s/^\///;
        $self->{prefix}=$prefix;
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
sub _aws_parameters { return $_[0]->{aws_parameters}}
sub _debug_aws { my $self=shift; return $self->{debug_aws}; }

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
    push(@command,'--debug') if $self->_debug_aws;
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
        $self->debug($cmdstr);
        $ok=run(\@command,\$in,\$out,\$err);
        $self->debug("Output: $out");
        $self->debug("Error: $err")
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
        $self->error("Cannot copy to $sourcefile to S3: not a file");
        return 0;
    }
    my $s3url=$self->fileUrl($name);
    my $timetag=(stat($sourcefile))[9];
    my $utctag=strftime("%Y-%m-%dT%H:%M:%S",gmtime($timetag));
    my @params=('--metadata',"mtime=$utctag",$sourcefile,$s3url);
    my ($ok,$result,$error)=$self->_runAws('s3','cp','--only-show-errors',@params);
    return wantarray ? ($ok,$result,$error) : $ok;
}


=head2 $bucket->putDir($sourcedir,$dir)

Recursively copy a directory contents to the bucket.  (Note 
this does not include modification time metadata)

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
    my ($ok,$result,$error)=$self->_runAws('s3','cp','--recursive','--only-show-errors',@params);
    return wantarray ? ($ok,$result,$error) : $ok;
}

=head2 $bucket->getFile($name,$targetfile)

Copy a file to the S3 bucket store for the process.

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
    my $filestat=$self->fileStats($name);
    if( ! $filestat )
    {
        $self->error("File $name not available S3");
        return 0;        
    }
    my $s3url=$self->fileUrl($name);
    my @params=($s3url,$targetfile);
    my ($ok,$result,$error)=$self->_runAws('s3','cp','--only-show-errors',@params);
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

Copy a file to the S3 bucket store for the process.

=cut

sub deleteFile
{
    my($self,$name)=@_;
    my $bucket=$self->bucket;
    my $key=$self->fileKey($name);
    my $s3url="s3://$bucket/$key";
    return $self->_runAws('s3','rm','--only-show-errors',$s3url);
}

=head2 $bucket->fileStats($file)

Return {size=>size, mtime=>mtime} if a file exists, undef otherwise

=cut

sub fileStats
{
    my($self,$name)=@_;
    my $bucket=$self->bucket;
    my $key=$self->fileKey($name);    
    my ($ok,$head,$err)=$self->_runAws('s3api','head-object','--bucket',$bucket,'--key',$key);
    my $result;
    if( $ok )
    {
        $result={};
        if( $head =~ /\"ContentLength\"\s*\:\s*(\d+)[\s\,\}]/ )
        {
            $result->{size}=$1;
        }
        if( $head =~ /\"mtime\"\s*\:\s*\"(\d\d\d\d)\-(\d\d)\-(\d\d)T(\d\d)\:(\d\d)\:(\d\d)\"/s )
        {
            my($y,$m,$d,$hr,$mi,$sc)=($1,$2,$3,$4,$5,$6);
            $y -= 1900;
            $m--;
            $result->{mtime}=timegm($sc,$mi,$hr,$d,$m,$y);
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

=head2 $bucket->syncToBucket($sourcedir,$targetkey,[delete=>1])

Synchonise files from source directory to target "directory".  
Runs aws s3 sync --delete unless delete=>0

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
    my @cmd=('s3','sync','--only-show-errors');
    push(@cmd,'--delete') unless exists $opts{delete} && ! $opts{delete};
    push(@cmd,$sourcedir,$s3url);
    my ($ok,$result,$error)=$self->_runAws(@cmd);
    return wantarray ? ($ok,$result,$error) : $ok;
}

=head2 $bucket->syncFromBucket($sourcekey,$targetdir,[delete=>1])

Synchronise files from source directory to target "directory".  
Runs aws s3 sync --delete unless delete=>0 

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
    my @cmd=('s3','sync','--only-show-errors');
    push(@cmd,'--delete') unless exists $opts{delete} && ! $opts{delete};
    push(@cmd,$s3url,$targetdir);
    my ($ok,$result,$error)=$self->_runAws(@cmd);    
    return wantarray ? ($ok,$result,$error) : $ok;
}

1;
