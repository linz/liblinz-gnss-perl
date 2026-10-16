
use strict;

package LINZ::GNSS::DataCenter::GNSSArchive;
use base "LINZ::GNSS::DataCenter::Http";
use fields qw (
    authuri
    alturi
    authcookies
    access
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
    $self->{access}=lc($cfgdc->{access}) || 'private';
    $self->{authuri}=$cfgdc->{authkeyuri};
    $self->{alturi}=$cfgdc->{alternativeuri};
    if( ! $self->{authuri} && $self->{access} ne 'public' )
    {
        croak("GNSSArchive datacenter ".$self->name." needs AuthKeyUri defined\n");
    }
    if( ! $self->{filelisturi} )
    {
        $self->{filelisturi} = $self->dailyFileSearchUri();
    }
    return $self;
}
=head2 $self->_readCredentials($credfile)

Reads the GNSS Archive machine account from a credentials file.  The archive
authenticates automated consumers with the OAuth2 client credentials flow
(the /machineAuth endpoint), so the file must define a client_id and
client_secret.  User names and passwords are not supported, as they cannot be
used once MFA is enabled.

A file with extension .json should look like

{"client_id": "the-client-id", "client_secret": "the-client-secret"}

otherwise it should have lines

   client_id the-client-id
   client_secret the-client-secret

Only the first such lines are read, other lines are ignored.  Returns the
client_id and client_secret, which are used as the Basic authorization
credentials for the AuthKeyUri request.

=cut

sub _readCredentials
{
    my ($self,$credfile)=@_;
    open(my $crdf,'<',$credfile) ||
        croak("Cannot open credentials file $credfile for DataCenter ".$self->name);
    my %cred;
    if( $credfile =~ /\.json$/ )
    {
        my $json=join('',<$crdf>);
        my $def=eval { decode_json($json) };
        croak("Credentials file $credfile for DataCenter ".$self->name." is not valid JSON\n") if ref($def) ne 'HASH';
        %cred=map { $_ => $def->{$_} } qw(client_id client_secret);
    }
    else
    {
        while( my $line=<$crdf> )
        {
            if( $line =~ /^\s*(client_id|client_secret)\s+(\S.*?)\s*$/i )
            {
                my $key=lc($1);
                $cred{$key}=$2 if ! defined $cred{$key};
            }
        }
    }
    close($crdf);
    foreach my $key (qw(client_id client_secret))
    {
        croak("Credentials file $credfile for DataCenter ".$self->name." doesn't define $key. ".
            "The GNSS Archive requires client credentials (client_id and client_secret), not a user name and password\n")
            if ! defined($cred{$key}) || $cred{$key} eq '';
    }
    return @cred{qw(client_id client_secret)};
}

=head2 $center->searchUri

Get the URI to retrieve a file list from the GNSS Archive

=cut

sub dailyFileSearchUri
{
    my($self)=@_;
    return $self->{filelisturi} if $self->{filelisturi};
    my $searchUri = $self->{uri};
    $searchUri =~ s/\/\w+\./\/search./;
    $searchUri =~ $1 if $searchUri =~ /https?:\/\/(search\.[^\/]+)/;
    my $access = $self->{access} eq 'public' ? 'public' : 'private';
    $searchUri .= "/v1/rinexFiles?access=$access&startTime=[yyyy]/[ddd]&endTime=[yyyy+1]/[ddd+1]&dataType=rnx_daily_30s";
    return $searchUri;
}

=head2 $center->connect

Get Authorization cookies for GNSS Archive

=cut

sub connect
{
    my($self) = @_;
    my ($client_id,$client_secret)=$self->credentials();
    my $ua=new LWP::UserAgent;
    $ua->env_proxy;
    my $authuri=$self->{authuri};
    if( $self->{authuri}  )
    {
        my $authhdr="Basic ".encode_base64("$client_id:$client_secret",'');
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
        $self->{authcookies}=\@cookies;
    }
    else
    {
        $self->{authcookies}=[];
    }
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
        foreach my $cookie (@{$self->{authcookies}})
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
