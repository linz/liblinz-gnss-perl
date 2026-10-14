#!/usr/bin/perl
# Tests for LINZ::GNSS::DataCenter::GNSSArchive authentication settings:
# AuthKeyUri environment expansion (urlAuth / machineAuth) and connect.
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

my $URL_AUTH     = 'https://search.archive.geodesy.linz.govt.nz/v1/urlAuth';
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
    my (%cfg) = @_;
    return LINZ::GNSS::DataCenter::Create({
        name => 'testarchive',
        type => 'GNSSArchive',
        datafiles => {},
        uri  => 'https://private.archive.geodesy.linz.govt.nz',
        %cfg,
    });
}

# The default used in conf/getdata.conf
my $conf_authuri = '${LINZ_GNSS_ARCHIVE_AUTH_URL||' . $URL_AUTH . '}';

# AuthKeyUri handling

{
    local $ENV{LINZ_GNSS_ARCHIVE_AUTH_URL};
    delete $ENV{LINZ_GNSS_ARCHIVE_AUTH_URL};

    is(newcenter(authkeyuri => $URL_AUTH)->{authuri}, $URL_AUTH,
        'literal AuthKeyUri is used unchanged');

    is(newcenter(authkeyuri => $conf_authuri)->{authuri}, $URL_AUTH,
        'defaults to urlAuth when LINZ_GNSS_ARCHIVE_AUTH_URL is not set');
}

{
    local $ENV{LINZ_GNSS_ARCHIVE_AUTH_URL} = $MACHINE_AUTH;
    is(newcenter(authkeyuri => $conf_authuri)->{authuri}, $MACHINE_AUTH,
        'LINZ_GNSS_ARCHIVE_AUTH_URL overrides the default (machineAuth)');
}

{
    local $ENV{LINZ_GNSS_ARCHIVE_AUTH_URL};
    delete $ENV{LINZ_GNSS_ARCHIVE_AUTH_URL};

    eval { newcenter() };
    like($@, qr/needs AuthKeyUri defined/, 'private archive without AuthKeyUri croaks');

    my $public = eval { newcenter(access => 'public') };
    ok($public && !$public->{authuri}, 'public archive does not need AuthKeyUri');
}

# connect: request URL, Basic auth header and cookies, for each endpoint

my @requests;
my $response_code = 200;
{
    no warnings 'redefine';
    *LWP::UserAgent::get = sub
    {
        my ($ua, $url, %headers) = @_;
        push @requests, { url => $url, auth => $headers{Authorization} };
        return HTTP::Response->new($response_code, $response_code == 200 ? 'OK' : 'Unauthorized',
            [], '{"cookies":["session=abc123; Path=/; Secure","other=xyz; Path=/"]}');
    };
}

my %cases = (
    urlAuth => {
        uri   => $URL_AUTH,
        creds => credfile('user.json', '{"username":"bob","password":"secret"}'),
        id    => 'bob', secret => 'secret',
    },
    machineAuth => {
        uri   => $MACHINE_AUTH,
        creds => credfile('client.json', '{"client_id":"cid","client_secret":"csecret"}'),
        id    => 'cid', secret => 'csecret',
    },
);

foreach my $name (sort keys %cases)
{
    my $case = $cases{$name};
    @requests = ();
    $response_code = 200;
    my $center = newcenter(authkeyuri => $case->{uri}, credentialsfile => $case->{creds});
    $center->connect();

    is(scalar(@requests), 1, "$name: one auth request made");
    is($requests[0]{url}, $case->{uri}, "$name: auth request sent to $case->{uri}");
    is($requests[0]{auth}, 'Basic ' . encode_base64("$case->{id}:$case->{secret}"),
        "$name: Basic auth header built from credentials file");
    is_deeply($center->{authcookies}, ['session=abc123', 'other=xyz'],
        "$name: cookies from auth response are kept");
}

# Authentication failure

{
    @requests = ();
    $response_code = 401;
    my $center = newcenter(authkeyuri => $MACHINE_AUTH, credentialsfile => $cases{machineAuth}{creds});
    eval { $center->connect() };
    like($@, qr/AuthKeyUri failed/, 'connect croaks if the auth endpoint rejects the credentials');
}

done_testing();
