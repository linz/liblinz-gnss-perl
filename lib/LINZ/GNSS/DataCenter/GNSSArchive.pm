
use strict;

package LINZ::GNSS::DataCenter::GNSSArchive;
use base "LINZ::GNSS::DataCenter";
use fields qw (
    authuri
    alturi
    cookies
    );

use LINZ::GNSS::DataCenter;
use URI::URL;
use JSON;
use LWP::UserAgent;
use HTTP::Request;
use MIME::Base64;
use Carp;

sub new
{
    my($self,$cfgdc)=@_;
    $self=fields::new($self) unless ref $self;
    $self->SUPER::new($cfgdc);
    $self->{authuri}=$cfgdc->{authkeyuri};
    $self->{alturi}=$cfgdc->{alternativeuri};
    if( ! $self->{authuri} )
    {
        croak("GNSSArchive datacenter ".$self->name." needs AuthKeyUri defined\n");
    }
    return $self;
}

=head2 $center->connect

Get Authorization cookies for GNSS Archive

=cut

sub connect
{
    my($self) = @_;
    my ($user,$pwd)=$self->credentials();
    my $ua=new LWP::UserAgent;
    $ua->env_proxy;
    my $authuri=$self->{authuri};
    my $authhdr="Basic ".encode_base64("$user:$pwd");
    my $response=$ua->get($authuri,"Authorization"=>$authhdr);
    if( $response->code ne '200')
    {
        $self->_logger->warn("Connection to ".$self->name." AuthKeyUri failed: ".$response->message);
        croak("Connection to ".$self->name." AuthKeyUri failed: ".$response->message."\n");
    }
    my $uri=new URI::URL($self->{uri});
    my $netloc=$uri->netloc;
    my $authdata=decode_json($response->content);
    my @cookies=();
    foreach my $cookie (@{$authdata->{'cookies'}})
    {
        my @parts=split(/\;\s+/,$cookie);
        push(@cookies,$parts[0]);
    }
    $self->{cookies}=\@cookies;
    $self->SUPER::connect();
}

# Get a file from the GNSS Archive

sub getfile
{
    my($self,$path,$file,$target)=@_;
    my $ua=new LWP::UserAgent;
    $ua->env_proxy;
    my $response;
    foreach my $base ($self->{uri},$self->{alturi})
    {
        next if ! $base;
        my $url=$base.$path.'/'.$file;
        $self->_logger->debug("GNSSArchive: Trying $url\n");
        my $request=HTTP::Request->new(GET=>$url);
        foreach my $cookie (@{$self->{cookies}})
        {
            $request->push_header("Cookie",$cookie);
        }
        $response=$ua->request($request);
        last if $response->code eq '200';
    }
    my $name=$self->name;
    if($response->code ne '200')
    {
        $self->_logger->warn("Cannot retrieve $file from $name: ".$response->message."\n");
        croak("Cannot retrieve $file from $name: ".$response->message."\n");
    }
    open(my $f,">:raw",$target) || die "Cannot open file\n";
    print $f $response->content;
    close($f);
    my $size= -s $target;
    $self->_logger->info("Retrieved $file ($size bytes) from $name");
}

1;
