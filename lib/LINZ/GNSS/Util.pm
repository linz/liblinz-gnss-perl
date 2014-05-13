=head1 LINZ::GNSS::Util

Provides a number of utility functions to assist in GNSS analysis

Usage:

   use LINZ::GNSS::Util qw/ConvertXYZToLLH CalcErrorEllipse/;

=cut

use strict;

package LINZ::GNSS::Util;

use LINZ::Geodetic::Ellipsoid;
use base qw(Exporter);

our @EXPORT_OK = qw(
   ConvertXYZToLLH
   CalcErrorEllipse
   );

our $grs80=LINZ::Geodetic::Ellipsoid::GRS80();
our $deg2rad=atan2(1.0,1.0)/45.0;

=head2 ($llh,$cvrenu)=LINZ::GNSS::Util::ConvertXYZToLLH($xyz,$covar)

Converts XYZ coordinate and covariance to lat,lon,height ordinates and E,N,U covariance.
Uses the GRS80 ellipsoid.

=cut

sub ConvertXYZToLLH
{
    my($xyz,$cvr) = @_;
    my $llh=$grs80->geog($xyz);
    my $cvrenu;
    if( $cvr )
    {
        my $cln=cos($deg2rad*$llh->lon);
        my $sln=sin($deg2rad*$llh->lon);

        my $clt=cos($deg2rad*$llh->lat);
        my $slt=sin($deg2rad*$llh->lat);

        my $rot=[
            [-$sln,$cln,0.0],
            [-($cln*$slt),-($sln*$slt),$clt],
            [$cln*$clt,$sln*$clt,$slt],
        ];

        my $cvr1;
        foreach my $i (0,1,2)
        {
            foreach my $j (0,1,2)
            {
                $cvr1->[$i]->[$j]=(
                     $rot->[$i]->[0]*$cvr->[0]->[$j] +
                     $rot->[$i]->[1]*$cvr->[1]->[$j] +
                     $rot->[$i]->[2]*$cvr->[2]->[$j]
                    );
            }
        }

        foreach my $i (0,1,2)
        {
            foreach my $j (0,1,2)
            {
                $cvrenu->[$i]->[$j]=(
                     $rot->[$i]->[0]*$cvr1->[$j]->[0] +
                     $rot->[$i]->[1]*$cvr1->[$j]->[1] +
                     $rot->[$i]->[2]*$cvr1->[$j]->[2]
                    );
            }
        }
    }

    return $llh,$cvrenu;

}

=head2 ($emax,$emin,$azemax) = CalcHorErrorEllipse($covenu)

Calculates the horizontal error ellipse from the ENU covariance matrix.
The azimuth is in degrees east of north.

=cut

sub CalcErrorEllipse
{
    my ($covenu) = @_;
    my $cee=$covenu->[0]->[0];
    my $cen=$covenu->[0]->[1];
    my $cnn=$covenu->[1]->[1];
    my $v1=($cnn+$cee)/2.0;
    my $v2=($cnn-$cee)/2.0;
    my $v3=$cen;
    my $v4=$v2*$v2+$v3*$v3;
    $v4 = $v4 > 0.0 ? sqrt($v4) : 0.0;
    my $azm = $v4 > 0 ? atan2($v3,$v2)/2.0 : 0.0;
    $azm /= $deg2rad;
    $v2=$v1-$v4;
    $v1=$v1+$v4;
    my $emax=$v1 > 0.0 ? sqrt($v1) : 0.0;
    my $emin=$v2 > 0.0 ? sqrt($v2) : 0.0;
    return ($emax,$emin,$azm);

}

1;
