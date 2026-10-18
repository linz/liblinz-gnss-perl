#!/usr/bin/perl
# With GNSS_ARCHIVE_DOMAIN unset, the GNSS Archive data centers in conf/getdata.conf
# must use the production URLs, unchanged from before the variable was introduced.
#
# Run from the source tree:  PERL5LIB=lib prove test/archive_domain.t

use strict;
use warnings;
use File::Temp ();
use FindBin;
use Test::More;

use LINZ::GNSS::Variables qw(ExpandEnv);

# Literal production URLs.  Do not derive these from the config.
my %prod = (
    'public Uri'        => 'https://public.archive.geodesy.linz.govt.nz',
    'private Uri'       => 'https://private.archive.geodesy.linz.govt.nz',
    'search AuthKeyUri' => 'https://search.archive.geodesy.linz.govt.nz/v1/machineAuth',
);

# The default must apply whatever the caller's environment holds
delete $ENV{GNSS_ARCHIVE_DOMAIN};

# Uri and AuthKeyUri values in conf/getdata.conf that use GNSS_ARCHIVE_DOMAIN
my $conf = "$FindBin::Bin/../conf/getdata.conf";
my %raw;
open( my $f, '<', $conf ) or die "Cannot open $conf: $!\n";
while (<$f>) {
    next if /^\s*#/;
    next if !/^\s*(Uri|AuthKeyUri)\s+(\S*GNSS_ARCHIVE_DOMAIN\S*)/;
    my ( $key, $value ) = ( $1, $2 );
    my ($host) = $value =~ m{^https://(\w+)\.};
    $raw{"$host $key"} = $value;
}
close($f);

is_deeply( [ sort keys %raw ], [ sort keys %prod ], 'config has the public, private and search archive URLs' );

foreach my $key ( sort keys %prod ) {
    is( ExpandEnv( $raw{$key} // '', "test" ), $prod{$key}, "unset: $key is production" );
}

# The GNSSArchive data center must also end up on production.  It expands AuthKeyUri
# itself, so check the object rather than just the string expansion.  Needs the full
# runtime dependencies: skipped where they are missing, but never skipped in CI.
SKIP: {
    my $loaded = eval { require LINZ::GNSS::DataCenter::GNSSArchive; 1 };
    skip( 'GNSSArchive dependencies not installed', 2 ) if !$loaded && !$ENV{CI};
    ok( $loaded, 'load LINZ::GNSS::DataCenter::GNSSArchive' ) or BAIL_OUT("cannot load GNSSArchive: $@");

    # DataCenter::new clones the default file types and builds its scratch directory
    # path from globals that are normally set up when the config is loaded
    require LINZ::GNSS::FileTypeList;
    LINZ::GNSS::FileTypeList::LoadDefaultTypes( {} );
    {
        no warnings 'once';
        $LINZ::GNSS::DataCenter::scratchdir = File::Temp::tempdir( CLEANUP => 1 );
    }

    my $dc = LINZ::GNSS::DataCenter::GNSSArchive->new(
        {
            name       => 'test-archive',
            uri        => $raw{'private Uri'},
            authkeyuri => $raw{'search AuthKeyUri'},
        }
    );
    is( $dc->uri,       $prod{'private Uri'},       'unset: GNSSArchive Uri is production' );
    is( $dc->{authuri}, $prod{'search AuthKeyUri'}, 'unset: GNSSArchive AuthKeyUri is production' );
}

done_testing();
