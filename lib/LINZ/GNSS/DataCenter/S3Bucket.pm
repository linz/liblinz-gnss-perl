use strict;

package LINZ::GNSS::DataCenter::S3Bucket;
use base "LINZ::GNSS::DataCenter";
use fields qw (
    awsparams
    readonly
    bucket
    connecterror
    );

use Carp;
use LINZ::GNSS::AwsS3Bucket;
use LINZ::GNSS::Variables qw(ExpandEnv);

=head1 LINZ::GNSS::DataCenter::S3Bucket

DataCenter backed by an AWS S3 bucket, defined by a Uri of the form
s3://bucket-name/key/prefix.  Operations are delegated to
LINZ::GNSS::AwsS3Bucket (which runs the aws client).

Configuration options (in addition to the common DataCenter options):

=over

=item S3AwsParameters

Extra parameters passed to the aws client (eg --profile xxx).

=item ReadOnly

If set, files cannot be uploaded to the bucket.

=item UseFileList

If set, the bucket is listed to check that a file exists before
retrieving it.  Listings are always used for filenames containing
wildcards (* or ?).

=back

Credentials (User/Password or CredentialsFile) are used as the AWS
access key id and secret access key.  If none are given the aws client's
normal credential chain is used.

=cut

sub new
{
    my($self,$cfgdc)=@_;
    $self=fields::new($self) unless ref $self;
    $self->SUPER::new($cfgdc);
    my $name=$self->name;
    my $params=$cfgdc->{s3awsparameters} // '';
    $self->{awsparams}=ExpandEnv($params,"S3AwsParameters of datacenter $name");
    $self->{readonly}=($cfgdc->{readonly} || '') =~ /^(1|y|yes|true|on)$/i ? 1 : 0;
    $self->{_checkfilelist}=($cfgdc->{usefilelist} || '') =~ /^(1|y|yes|true|on)$/i ? 1 : 0;
    return $self;
}

=head2 $center->connect

Opens the S3 bucket.  A failure is remembered so that later calls fail
immediately rather than re-running the aws client for every file.

=cut

sub connect
{
    my($self) = @_;
    return if $self->{bucket};
    croak($self->{connecterror}) if $self->{connecterror};
    my $bucketname=$self->host;
    my ($user,$pwd) = $self->credentials(0);
    my $bucket=eval
    {
        LINZ::GNSS::AwsS3Bucket->new(
            bucket=>$bucketname,
            prefix=>$self->basepath,
            aws_parameters=>$self->{awsparams},
            access_key_id=>$user,
            secret_access_key=>$pwd
            );
    };
    if( ! $bucket )
    {
        my $error=$@ || "Failed to open S3 bucket $bucketname\n";
        $self->{connecterror}="Connection to ".$self->name." S3 bucket $bucketname failed: $error";
        $self->_logger->warn($self->{connecterror});
        croak($self->{connecterror});
    }
    $self->{bucket}=$bucket;
    $self->SUPER::connect();
}

sub disconnect
{
    my($self) = @_;
    $self->{bucket}=undef;
    $self->SUPER::disconnect();
}

sub _key
{
    my($self,$path,$file)=@_;
    return defined($file) && $file ne '' ? "$path/$file" : $path;
}

# Get a file from the bucket.  AwsS3Bucket::getFile croaks on failure.

sub getfile
{
    my($self,$path,$file,$target)=@_;
    $self->connect;
    $self->{bucket}->getFile($self->_key($path,$file),$target);
    croak("Failed to retrieve $file from ".$self->name."\n") if ! -f $target;
    my $size=-s $target;
    $self->_logger->info("Retrieved $file ($size bytes) from ".$self->name);
}

# List the files under the spec's path, used to resolve wildcard filenames
# (and to check files exist if UseFileList is set)

sub getfilelist
{
    my($self,$spec)=@_;
    $self->connect;
    my $path=$spec->path;
    my $files=$self->{bucket}->listFiles($path);
    $self->_logger->debug("Found ".scalar(@$files)." files in $path on ".$self->name);
    return $files;
}

# Check whether a file is in the bucket

sub hasfile
{
    my($self,$spec)=@_;
    $self->connect;
    return $self->{bucket}->fileExists($self->_key($spec->path,$spec->filename));
}

# Upload a file to the bucket

sub putfile
{
    my($self,$source,$spec)=@_;
    return $self->SUPER::putfile($source,$spec) if $self->{readonly};
    $self->connect;
    my $target=$self->_key($spec->path,$spec->filename);
    my ($ok,undef,$error)=$self->{bucket}->putFile($source,$target);
    croak("Failed to upload $target to ".$self->name.": $error\n") if ! $ok;
    my $size=-s $source;
    $self->_logger->info("Uploaded $target ($size bytes) to ".$self->name);
}

1;
