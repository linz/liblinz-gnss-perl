
use strict;

package LINZ::GNSS::DataCenter::Http;
use base "LINZ::GNSS::DataCenter";
use fields qw (
    cookies
    filelistpath
    filelistregex
    timeout
    );

use LWP::UserAgent;
use HTTP::Request;
use HTTP::Cookies;
use MIME::Base64;
use Carp;

sub new
{
    my($self,$cfgdc)=@_;
    $self=fields::new($self) unless ref $self;
    $self->SUPER::new($cfgdc);
    $self->{cookies}=HTTP::Cookies->new();
    $self->{filelistpath}=${cfgdc}->{filelisturipath};
    $self->{timeout} = $cfgdc->{timeout} || $LINZ::GNSS::DataCenter::http_timeout;
    my $fre=${cfgdc}->{filelistregex} || '^\s*([\w\.]+)(?:\s|$)';
    $self->{filelistregex}=qr/$fre/;
    if( $self->{filelistpath} )
    {
        $self->{_checkfilelist} = exists $cfgdc->{usefilelist} ? $cfgdc->{usefilelist} : 1;
    }
    return $self;
}

sub getfilelist
{
    my( $self,$spec)=@_;
    my $path=$spec->path;
    my $uripath=$self->{filelistpath};
    $uripath =~ s/\[path\]/$path/eg;
    $uripath = $spec->expandName($uripath);
    croak "Getting file listings is not supported on DataCenter ".$self->name.".  Use FileListUri in configuration\n"
        if ! $uripath;
    my $url=$self->{uri}.$uripath;
    my $content=$self->_content($url);
    my $filere=$self->{filelistregex};
    my $list=[];
    foreach my $line (split(/[\r\n]+/,$content))
    {
        push(@$list,$1) if $line =~ /$filere/;
    }
    return $list;
}

sub getfile
{
    my($self,$path,$file,$target)=@_;
    my $url=$self->{uri}.$path.'/'.$file;
    my $content=$self->_content($url);
    my $host=$self->{host};
    if( ! $content )
    {
        croak("Cannot retrieve $url: No data\n");
    }
    open(my $f, ">$target" ) || croak("Cannot create file $target\n");
    binmode($f);
    print $f $content;
    close($f);
    my $size= -s $target;
    $self->_logger->info("Retrieved $file ($size bytes) from $host");    
}

sub _content
{
    my ($self, $url) = @_;
    $self->_logger->debug("Retrieving $url");    
    my ($user,$pwd)=$self->credentials;
    my $ua=new LWP::UserAgent;
    $ua->env_proxy;
    $ua->cookie_jar($self->{cookies});
    $ua->timeout($self->{timeout});
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
    return $content;
}


1;
