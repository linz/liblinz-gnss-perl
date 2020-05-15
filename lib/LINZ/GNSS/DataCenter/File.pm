
use strict;

package LINZ::GNSS::DataCenter::File;
use base "LINZ::GNSS::DataCenter";
use fields qw (
    );

use Carp;
use File::Copy;

sub new
{
    my($self,$cfgdc)=@_;
    $self=fields::new($self) unless ref $self;
    $self->SUPER::new($cfgdc);
    return $self;
}


# Get a file from the data centre

sub getfile
{
    my($self,$path,$file,$target)=@_;
    $path=$self->{basepath}.'/'.$path if $self->{basepath};
    my $source="$path/$file";
    if( ! copy($source,$target) )
    {
        croak "Cannot retrieve file $source\n";
    }
    $self->_logger->info("Retrieved $source");
}

# Check to see whether a file system based data center has a file

sub hasfile
{
    my($self,$spec)=@_;
    my $target=$spec->{path}.'/'.$spec->{filename};
    $target = $self->{basepath}.'/'.$target if $self->{basepath};
    return -e $target;
}

sub putfile
{
    my ($self,$source,$spec) = @_;
    my $target=$spec->{path};
    $target = $self->{basepath}.'/'.$target if $self->{basepath};
    LINZ::GNSS::DataCenter::makepublicpath($target) || croak "Cannot create target directory $target\n"; 
    $target=$target.'/'.$spec->{filename};
    move($source,$target) || croak "Cannot copy file to $target\n";
}

1;
