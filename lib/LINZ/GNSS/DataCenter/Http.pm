
use strict;

package LINZ::GNSS::DataCenter::Http;
use base "LINZ::GNSS::DataCenter";
use fields qw (
    );

use LWP::UserAgent;
use HTTP::Request;
use MIME::Base64;
use Carp;

sub new
{
    my($self,$cfgdc)=@_;
    $self=fields::new($self) unless ref $self;
    $self->SUPER::new($cfgdc);
    return $self;
}


sub getfile
{
    my($self,$path,$file,$target)=@_;
    my $url=$self->{uri}.$path.'/'.$file;
    my ($user,$pwd)=$self->credentials;
    my $ua=new LWP::UserAgent;
    $ua->env_proxy;
    my %headers=();
    if( $user )
    {
        $headers{Authorization}="Basic ".encode_base64("$user:$pwd");
    }
    my $response=$ua->get($url,%headers);
    if( $response->code ne '200' )
    {
        croak("Cannot retrieve $url: ".$response->message."\n");
    }
    my $content=$response->content;
    if( ! $content )
    {
        croak("Cannot retrieve $url: No data\n");
    }
    open(my $f, ">$target" ) || croak("Cannot create file $target\n");
    binmode($f);
    print $f $content;
    close($f);
}

1;
