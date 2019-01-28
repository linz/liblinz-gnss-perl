
use strict;

package LINZ::GNSS::DataCenter::Http;
use base "LINZ::GNSS::DataCenter";
use fields qw (
    );

use Carp;

sub new
{
    my($self,$cfgdc)=@_;
    $self=fields::new($self) unless ref $self;
    $self->SUPER::new();
}


sub getfile
{
    my($self,$path,$file,$target)=@_;
    $path=$self->{basepath}.'/'.$path if $self->{basepath};
    my $source="$path/$file";
    require LWP::Simple;
    my $host=$self->{host};
    my $url="http://$host"."$path/$file";
    my $content=LWP::Simple::get($url);
    if( ! $content )
    {
        self->_logger->warn("Cannot retrieve $url");
        croak("Cannot retrieve $url");
    }
    open(my $f, ">$target" ) || croak("Cannot create file $target\n");
    binmode($f);
    print $f $content;
    close($f);
}

1;
