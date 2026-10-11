
use strict;

package LINZ::GNSS::DataCenter::Http;
use base "LINZ::GNSS::DataCenter";
use fields qw (
    cookies
    filelisturi
    filelistregex
    timeout
    authhosts
    allowdowngrade
    );

use LWP::UserAgent;
use HTTP::Request;
use HTTP::Cookies;
use MIME::Base64;
use Carp;

our $MaxRedirects=5;

sub new
{
    my($self,$cfgdc)=@_;
    $self=fields::new($self) unless ref $self;
    $self->SUPER::new($cfgdc);
    $self->{cookies}=HTTP::Cookies->new();

    # Hosts to which the credentials are sent, including when reached by a redirect,
    # eg a separate login server.  The data centre host is always included.
    my $authhosts=$cfgdc->{authhosts} || [];
    $authhosts=join(' ',@$authhosts) if ref($authhosts) eq 'ARRAY';
    $self->{authhosts}={ map { lc($_) => 1 } ($self->{host}, split(' ',$authhosts)) };

    # Follow redirects from https to http (refused by default by newer versions of LWP)
    $self->{allowdowngrade}=($cfgdc->{allowdowngrade} || '') =~ /^(1|y|yes|true|on)$/i ? 1 : 0;

    my $filelisturi;
    if( $cfgdc->{filelisturi} || $cfgdc->{filelisturipath} )
    {
        my $filelistpath=${cfgdc}->{filelisturipath} || '[path]';
        $filelisturi=${cfgdc}->{filelisturi} || $self->{uri}.$filelistpath;
    }
    $self->{filelisturi} = $filelisturi;

    $self->{timeout} = $cfgdc->{timeout} || $LINZ::GNSS::DataCenter::http_timeout;
    my $fre=${cfgdc}->{filelistregex};
    # If we have a file list and no regex, 
    # then compile a regex from all product filenames that contain a wildcard
    if( $self->{filelisturi} )
    {
        $self->{_checkfilelist} = exists $cfgdc->{usefilelist} ? $cfgdc->{usefilelist} : 0;
    }
    if( ! $fre ) 
    {
        foreach my $df (values %{$cfgdc->{datafiles}} ) {
            foreach my $dt ( values %$df ) {
                my $filename = $dt->{filename};
                # If we are checking for all files, or if a filename has a wildcard
                # then we need to add it to the regex filelist patterns.
                if( $filename =~ /\*|\?/ || $self->{_checkfilelist} )
                {
                    $filename =~ s/\./\\./g;
                    $filename =~ s/\*/.*/g;
                    $filename =~ s/\?/./g;
                    $filename =~ s/\[h(?:[+-]\d+[dh]?)?\]/[a-x]/gi;
                    $filename =~ s/\[d(?:[+-]\d+[dh]?)?\]/\\d/gi;
                    $filename =~ s/\[(?:yy|ww|hh)(?:[+-]\d+[dh]?)?\]/\\d\\d/gi;
                    $filename =~ s/\[ddd(?:[+-]\d+[dh]?)?\]/\\d\\d\\d/gi;
                    $filename =~ s/\[dddh(?:[+-]\d+[dh]?)?\]/\\d\\d\\d[a-x]/gi;
                    $filename =~ s/\[(?:yyyy|wwww)(?:[+-]\d+[dh]?)?\]/\\d\\d\\d\\d/gi;
                    $filename =~ s/\[ssss\]/\\w\\w\\w\\w/gi;
                    $fre .= $filename."|";
                }
            }
        }
        $fre =~ s/\|$//;
        $self->_logger->debug("Data centre $self->{name}: file list regex \"$fre\"") if $fre;
        $self->{filelistregex}=qr/\b($fre)\b/;
        my ($user,$pwd)=$self->credentials;
        if( $user )
        {
            $user=~ s/(.).*/$1xxxxxx/;
            $pwd=~ s/(.).*/$1xxxxxx/;
            $self->_logger->debug("Using credentials $user:$pwd for ".join(' ',sort keys %{$self->{authhosts}}));
        }
    }
    return $self;
}

sub getfilelist
{
    my( $self,$spec)=@_;
    my $path=$spec->path;
    my $url=$self->{filelisturi};
    $url =~ s/\[path\]/$path/eg;
    $url = $spec->expandName($url);
    croak "Getting file listings is not supported on DataCenter ".$self->name.".  Use FileListUri or FileListUriPath in configuration\n"
        if ! $url;
    $self->_logger->debug("Retrieving file list from $url");
    my $content=$self->_content($url);
    my $filere=$self->{filelistregex};
    my $listhash={};
    while ($content =~ /$filere/g) {
        $listhash->{$1}=1;
    }
    my $list = [keys %$listhash];
    my $file0 = $list->[0];
    $self->_logger->debug("Found ".scalar(keys %$listhash)." files: $file0 ...");
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
    $ua->allow_downgrade(1) if $self->{allowdowngrade} && $ua->can('allow_downgrade');
    if( $user )
    {
        # Add the credentials to each request in the redirect chain that goes to one of
        # the authorised hosts, over https or the data centre's own scheme.  Remove them
        # from any other request (older versions of LWP copy them across redirects).
        my $auth="Basic ".encode_base64("$user:$pwd",'');
        my $authhosts=$self->{authhosts};
        my $scheme=$self->{scheme};
        $ua->add_handler(request_prepare => sub {
            my ($request)=@_;
            my $uri=$request->uri;
            if( $authhosts->{lc($uri->host)} && ($uri->scheme eq 'https' || $uri->scheme eq $scheme) )
            {
                $request->header(Authorization=>$auth);
            }
            else
            {
                $request->remove_header('Authorization');
            }
            return;
        });
    }
    my $response = $ua->get($url);
    if( $response->code ne '200' )
    {
        croak("Cannot retrieve $url: ".$response->message."\n");
    }
    my $content=$response->content;
    return $content;
}


1;
