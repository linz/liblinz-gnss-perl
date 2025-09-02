
use strict;

package LINZ::GNSS::DataCenter::S3Bucket;
use base "LINZ::GNSS::DataCenter";
use fields qw (
    bucketname
    bucketprefix
    bucketawsparams
    bucket
    );

use URI;
use LINZ::GNSS::DataCenter;
use LINZ::GNSS::AwsS3Bucket;
use LINZ::GNSS::Variables qw(ExpandEnv);

sub new
{
    my($self,$cfgdc)=@_;
    $self=fields::new($self) unless ref $self;
    $self->SUPER::new($cfgdc);
    my $name=$self->name;
    $self->{bucketawsparams}=ExpandEnv($cfgdc->{s3awsparameters},"s3awsparameters of datacenter $name");
    return $self;
}

=head2 $center->connect

Connect to S3 bucket

=cut

sub connect
{
    my($self) = @_;
    my $bucketname=$self->host;
    my ($user,$pwd) = $self->credentials(0);
    eval
    {
        $self->{bucket}=LINZ::GNSS::AwsS3Bucket->new(
            bucket=>$bucketname,
            prefix=>$self->basepath,
            aws_parameters=>$self->{bucketawsparams},
            access_key_id=>$user,
            secret_access_key=>$pwd
            );
        die "Failed to connect to S3 bucket $bucketname\n"
            if ! $self->{bucket};
    };
    if( $@ )
    {
        my $error=$@;
        $self->{_logger}->warn("Connection to ".$self->name." S3 bucket failed: ".
            $error);
        croak("Connection to ".$self->name." S3 bucket failed: ".
            $error);
    }
    $self->SUPER::connect();
}

# Get a file from the GNSS Archive

sub getfile
{
    my($self,$path,$file,$target)=@_;
    $self->{bucket}->getFile("$path/$file",$target);
    my $size=-s $target;
    my $name=$self->{name};
    $self->{_logger}->info("Retrieved $file ($size bytes) from $name");
}

sub putfile
{
    my($self,$source, $spec)=@_;
    my $target=$spec->{path}.'/'.$spec->{filename};
    $self->{bucket}->putFile($source,$target);
    my $size=-s $source;
    my $name=$self->{name};
    $self->{_logger}->info("Uploaded $target ($size bytes) to $name");
}

1;
