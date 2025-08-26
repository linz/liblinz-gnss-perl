
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
    if( $self->{filelistpath} )
    {

        my $fre=${cfgdc}->{filelistregex};
        # If we have a file list and no regex, 
        # then compile a regex from all product filenames that contain a wildcard
        if( ! $fre ) 
        {
            foreach my $df (values %{$cfgdc->{datafiles}} ) {
                foreach my $dt ( values %$df ) {
                    my $filename = $dt->{filename};
                    if( $filename =~ /\*|\?/ )
                    {
                        $filename =~ s/\./\\./g;
                        $filename =~ s/\*/.*/g;
                        $filename =~ s/\?/./g;
                        $filename =~ s/\[h\]/[a-x]/gi;
                        $filename =~ s/\[d\]/\\d/gi;
                        $filename =~ s/\[(?:yy|ww|hh)\]/\\d\\d/gi;
                        $filename =~ s/\[ddd\]/\\d\\d\\d/gi;
                        $filename =~ s/\[dddh\]/\\d\\d\\d[a-x]/gi;
                        $filename =~ s/\[(?:yyyy|wwww)\]/\\d\\d\\d\\d/gi;
                        $filename =~ s/\[ssss\]/\\w\\w\\w\\w/gi;
                        $fre .= $filename."|";
                    }
                }
            }
            $fre =~ s/\|$//;
            $self->_logger->debug("Data centre $self->{name}: file list regex \"$fre\"");
            $fre = "\\b($fre)\\b";
        }
        $self->{filelistregex}=qr/$fre/;
        if( $self->{filelistpath} )
        {
            $self->{_checkfilelist} = exists $cfgdc->{usefilelist} ? $cfgdc->{usefilelist} : 1;
        }
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
