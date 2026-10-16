#!/usr/bin/perl
# Tests for LINZ::GNSS::DataCenter::GNSSArchive client credentials authentication
# Run with: perl -I../lib gnssarchive.t  (from the test directory)

use strict;
use warnings;
use Test::More;
use File::Temp qw(tempdir);
use MIME::Base64;
use HTTP::Response;
use LWP::UserAgent;
use LINZ::GNSS::DataCenter;

my $dir = tempdir(CLEANUP => 1);
my $MACHINE_AUTH = 'https://search.archive.geodesy.linz.govt.nz/v1/machineAuth';

sub credfile
{
    my ($filename, $content) = @_;
    my $path = "$dir/$filename";
    open(my $f, '>', $path) || die "Cannot write $path: $!";
    print $f $content;
    close($f);
    return $path;
}

sub newcenter
{
    my ($creds) = @_;
    return LINZ::GNSS::DataCenter::Create({
        name => 'testarchive',
        type => 'GNSSArchive',
        datafiles => {},
        uri  => 'https://private.archive.geodesy.linz.govt.nz',
        authkeyuri => $MACHINE_AUTH,
        credentialsfile => $creds,
    });
}

sub creds { return [newcenter(credfile(@_))->credentials] }

# Reading client credentials

is_deeply(creds('c.json', '{"client_id":"cid","client_secret":"csecret"}'),
    ['cid', 'csecret'], 'json client_id/client_secret');
is_deeply(creds('c.txt', "client_id cid\nclient_secret csecret\n"),
    ['cid', 'csecret'], 'text client_id/client_secret');
is_deeply(creds('c2.txt', "# comment\nclient_id first\nclient_id second\nclient_secret csecret\n"),
    ['first', 'csecret'], 'text: first definition is used, other lines ignored');

# User/password files are rejected

foreach my $case (
    ['u.json', '{"username":"bob","password":"secret"}'],
    ['u.txt', "user bob\npassword secret\n"],
    ['partial.json', '{"client_id":"cid"}'],
    ['partial.txt', "client_secret csecret\n"],
    ['bad.json', 'not json'],
)
{
    my ($name, $content) = @$case;
    my $path = credfile($name, $content);
    eval { newcenter($path) };
    like($@, qr/client_id|client_secret|not valid JSON/, "$name is rejected");
}

# connect: GET to machineAuth with Basic client_id:client_secret

my @requests;
my $response_code = 200;
{
    no warnings 'redefine';
    *LWP::UserAgent::get = sub {
        my ($ua, $url, %hdr) = @_;
        push @requests, { url => $url, auth => $hdr{Authorization} };
        my $r = HTTP::Response->new($response_code, $response_code == 200 ? 'OK' : 'Unauthorized');
        $r->content('{"cookies":["session=abc123; Path=/","other=xyz; Path=/"]}');
        return $r;
    };
    *LINZ::GNSS::DataCenter::Http::connect = sub { 1 };
}

{
    my $center = newcenter(credfile('ok.json', '{"client_id":"cid","client_secret":"csecret"}'));
    $center->connect();
    is(scalar(@requests), 1, 'one auth request made');
    is($requests[0]{url}, $MACHINE_AUTH, 'auth request sent to machineAuth');
    is($requests[0]{auth}, 'Basic ' . encode_base64('cid:csecret', ''),
        'Basic auth header built from client_id and client_secret');
    is_deeply($center->{authcookies}, ['session=abc123', 'other=xyz'], 'cookies from auth response are kept');
}

# Cognito client ids and secrets are long enough for the encoded header to pass 76
# characters, where encode_base64 would otherwise insert a line break
{
    @requests = ();
    $response_code = 200;
    my ($id, $secret) = ('i' x 26, 's' x 52);
    my $center = newcenter(credfile('long.json', qq({"client_id":"$id","client_secret":"$secret"})));
    $center->connect();
    is($requests[0]{auth}, 'Basic ' . encode_base64("$id:$secret", ''),
        'Basic auth header has no line breaks for long credentials');
    unlike($requests[0]{auth}, qr/\s\S*\s/, 'header is a single token pair');
}

{
    $response_code = 401;
    my $center = newcenter(credfile('ok.json', '{"client_id":"cid","client_secret":"csecret"}'));
    eval { $center->connect() };
    like($@, qr/AuthKeyUri failed/, 'connect croaks if the credentials are rejected');
}

done_testing();
