#!usr/bin/perl
use strict;
use warnings;

use Config;
use Module::Build;
use inc::MyBuilder;

my $requires = {
    'Carp' => 0,
    'Config::General' => 0,
    'DBI' => 0,
    'DBD::SQLite' => 0,
    'File::Copy' => 0,
    'File::Path' => 0,
    'LINZ::Geodetic::CoordSysList' => 0,
    'LINZ::Geodetic::Ellipsoid' => 0,
    'Log::Log4perl' => 0,
    'Math::Libm' => 0,
    'Math::Trig' => 0,
    'Net::FTP' => 0,
    'Storable' => 0,
    'Time::Local' => 0,
    'URI' => 0,
    'XML::Simple' => 0,
};

my $build =  inc::MyBuilder->new(
    module_name        => 'LINZ::GNSS',
    license            => 'bsd',
    dist_author        => 'LINZ',
    dist_version_from  => 'lib/LINZ/GNSS.pm',
    dist_abstract      => 'Modules for accessing GNSS reference data',
    requires           => $requires,
    create_makefile_pl => 'small',
);

$build->add_build_element('conf');

$build->create_build_script;
