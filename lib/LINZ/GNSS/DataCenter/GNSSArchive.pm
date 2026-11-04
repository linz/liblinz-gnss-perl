
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
use LINZ::GNSS::Variables qw(ExpandEnv);
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
    # Uri is expanded by LINZ::GNSS::DataCenter, AuthKeyUri needs expanding here so that
    # ${ENV||default} can be used to select the archive environment
    $self->{authuri}=ExpandEnv($self->{authuri},"AuthKeyUri of datacenter ".$self->name) if $self->{authuri};
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

Reads the GNSS Archive credentials from a credentials file.  The file can
hold either the client_id and client_secret of a machine account, or a
username and password.  These are sent as the Basic authorization
credentials of the AuthKeyUri request.

A file with extension .json should look like

{"client_id": "the-client-id", "client_secret": "the-client-secret"}

or

{"username": "the-user-name", "password": "the-password"}

otherwise it should have lines

   client_id the-client-id
   client_secret the-client-secret

or

   user the-user-name
   password the-password

(username is accepted in place of user).  Only the first of each line is
read, other lines are ignored.  If the file defines both a complete
client_id/client_secret pair and a username/password, the client_id and
client_secret are used.  The two pairs are never mixed.

=cut

sub _readCredentials
{
    my ($self,$credfile)=@_;
    open(my $crdf,'<',$credfile) ||
        croak("Cannot open credentials file $credfile for DataCenter ".$self->name."\n");
    my %cred;
    if( $credfile =~ /\.json$/ )
    {
        my $json=join('',<$crdf>);
        my $def=eval { decode_json($json) };
        croak("Credentials file $credfile for DataCenter ".$self->name." is not valid JSON\n")
            if ref($def) ne 'HASH';
        %cred=map { $_ => $def->{$_} } qw(client_id client_secret username password);
    }
    else
    {
        while( my $line=<$crdf> )
        {
            next if $line !~ /^\s*(client_id|client_secret|user(?:name)?|password)\s+(\S.*?)\s*$/i;
            my ($key,$value)=(lc($1),$2);
            $key='username' if $key eq 'user';
            $cred{$key}=$value if ! defined $cred{$key};
        }
    }
    close($crdf);
    my $defined=sub { defined($cred{$_[0]}) && $cred{$_[0]} ne '' };
    return @cred{qw(client_id client_secret)}
        if $defined->('client_id') && $defined->('client_secret');
    return @cred{qw(username password)}
        if $defined->('username') && $defined->('password');
    croak("Credentials file $credfile for DataCenter ".$self->name.
        " must define either client_id and client_secret, or username and password\n");
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
    # Either a client_id and client_secret or a username and password (see _readCredentials)
    my ($user,$pwd)=$self->credentials();
    my $ua=new LWP::UserAgent;
    $ua->env_proxy;
    my $authuri=$self->{authuri};
    if( $self->{authuri}  )
    {
        my $authhdr="Basic ".encode_base64("$user:$pwd",'');
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
