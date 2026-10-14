#!/usr/bin/perl
# Tests for LINZ::GNSS::DataCenter::_readCredentials
# Run with: perl -I../lib credentials.t  (from the test directory)

use strict;
use warnings;
use Test::More;
use File::Temp qw(tempdir);
use LINZ::GNSS::DataCenter;

my $dir = tempdir(CLEANUP => 1);

package FakeCenter { sub name { 'testcenter' } }
my $center = bless {}, 'FakeCenter';

sub readcreds
{
    my ($filename, $content) = @_;
    my $path = "$dir/$filename";
    open(my $f, '>', $path) || die "Cannot write $path: $!";
    print $f $content;
    close($f);
    return LINZ::GNSS::DataCenter::_readCredentials($center, $path);
}

# User credentials (urlAuth) - existing formats must keep working

is_deeply([readcreds('user.json', '{"username":"bob","password":"secret"}')],
    ['bob', 'secret'], 'json username/password');

is_deeply([readcreds('user.txt', "user bob\npassword secret\n")],
    ['bob', 'secret'], 'text user/password');

is_deeply([readcreds('username.txt', "username bob\npassword secret\n")],
    ['bob', 'secret'], 'text username/password');

# Machine credentials (machineAuth)

is_deeply([readcreds('client.json', '{"client_id":"cid","client_secret":"csecret"}')],
    ['cid', 'csecret'], 'json client_id/client_secret');

is_deeply([readcreds('client.txt', "client_id cid\nclient_secret csecret\n")],
    ['cid', 'csecret'], 'text client_id/client_secret');

# username/password take precedence if both are present

is_deeply([readcreds('both.json',
    '{"username":"bob","password":"secret","client_id":"cid","client_secret":"csecret"}')],
    ['bob', 'secret'], 'json username/password preferred over client credentials');

is_deeply([readcreds('both.txt',
    "user bob\npassword secret\nclient_id cid\nclient_secret csecret\n")],
    ['bob', 'secret'], 'text user/password preferred over client credentials');

# Precedence does not depend on the order of lines in the file

is_deeply([readcreds('order.txt',
    "client_id cid\nclient_secret csecret\nuser bob\npassword secret\n")],
    ['bob', 'secret'], 'text user/password preferred when client credentials come first');

is_deeply([readcreds('order.json',
    '{"client_id":"cid","client_secret":"csecret","username":"bob","password":"secret"}')],
    ['bob', 'secret'], 'json user/password preferred when client credentials come first');

# User and client credentials are never mixed

is_deeply([readcreds('mixed.json', '{"username":"bob","client_secret":"csecret"}')],
    ['bob', undef], 'json username is not paired with client_secret');

is_deeply([readcreds('mixed.txt', "client_secret csecret\nuser bob\n")],
    ['bob', undef], 'text user is not paired with client_secret');

# Missing user is an error

eval { readcreds('empty.txt', "password secret\n") };
like($@, qr/doesn't define user/, 'croaks if no user defined');

eval { readcreds('empty.json', '{"foo":"bar"}') };
like($@, qr/doesn't define user/, 'croaks if no user defined in json');

done_testing();
