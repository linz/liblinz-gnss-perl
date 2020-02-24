
use strict;

package LINZ::GNSS::DataCenter::S3Bucket;
use base "LINZ::GNSS::DataCenter";
use fields qw (
    bucketname
    bucketprefix
    bucketawsparams
    bucket
    );

use LINZ::GNSS::DataCenter;
use LINZ::GNSS::AwsS3Bucket;

sub new
{
    my($self,$cfgdc)=@_;
    $self=fields::new($self) unless ref $self;
    $self->SUPER::new($cfgdc);
    $self->{bucketname}=$cfgdc->{s3bucket};
    $self->{bucketprefix}=$cfgdc->{s3prefix};
    $self->{bucketawsparams}=$cfgdc->{s3awsparameters};
    if( ! $self->{bucketname} )
    {
        croak("GNSSArchive datacenter ".$self->name." needs S3Bucket defined\n");
    }
    return $self;
}

=head2 $center->connect

Connect to S3 bucket

=cut

sub connect
{
    my($self) = @_;
    my $bucketname=$self->{bucketname};
    eval
    {
        $self->{bucket}=LINZ::GNSS::AwsS3Bucket->new(
            bucket=>$bucketname,
            prefix=>$self->{bucketprefix},
            aws_parameters=>$self->{bucketawsparameters}
            );
        die "Failed to connect to S3 bucket $bucketname\n"
            if ! $self->{bucket};
    };
    if( $@ )
    {
        my $error=$@;
        $self->_logger->warn("Connection to ".$self->name." S3 bucket failed: ".
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
    $self->_logger->info("Retrieved $file ($size bytes) from $name");
}

1;
