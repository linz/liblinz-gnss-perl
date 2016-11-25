
=head1 LINZ::GNSS::Variables

Usage:

   use LINZ::GNSS::Variables qw/ExpandEnv/;

=cut

# This package is used by the getdata routines to expand environment variables
# in a string. This is a late addition that could be refactored into other 
# modules

use strict;

package LINZ::GNSS::Variables;

use base qw(Exporter);
use Carp;

our @EXPORT_OK = qw(
   ExpandEnv
   );

=head2 $result=LINZ::GNSS::Variables::ExpandEnv($source,$context)

Replaces strings like ${env} with the corresponding environment variable.
env can include optional environment variables, and a default value using
the pipe character separator eg {env1|env2|... ||default}

Will croakif there none of the optional environment variables
exist and there is no default.  The optional parameter context can be added 
to the error message.

=cut

sub _expand
{
    my ($string,$context)=@_;
    my($varstr,$default)=split(/\|\|/,$string,2);
    my @vars=split(/\|/,$varstr);

    foreach my $v (@vars)
    {
        return $ENV{$v} if exists $ENV{$v};
    }
    return $default if defined $default;
    my $errmsg;
    if( @vars > 1 )
    {
        $errmsg='None of environment variables '.join(' ',@vars).' defined';
    }
    else
    {
        $errmsg='Environment variable '.$vars[0].' not defined';
    }
    $errmsg .= ' '.$context if $context;
    $errmsg .= "\n";
    croak $errmsg;
}

sub ExpandEnv
{
    my($string, $context )=@_;
    my $maxiterations=5;
    while( $maxiterations-- > 0 )
    {
        last if ! ($string =~ s/\$\{([\w\|]+(?:\|\|[^\{\}]*)?)\}/_expand($1,$context)/eg);
    }
    return $string;
}

1;
