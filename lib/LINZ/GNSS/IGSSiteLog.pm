
package LINZ::GNSS::IGSSiteLog;

=head1 LINZ::GNSS::IGSSiteLog

Package to read an IGS XML site log file as generated on the Geonet site.  Note that this
format may change (particularly if GeodesyML gets adopted).

=cut

use XML::LibXML qw/:libxml/;
use XML::LibXML::XPathContext;
use LINZ::GNSS::Time qw/ymdhms_seconds/;

our $IGSNamespaces={
        equip=>"http://sopac.ucsd.edu/ns/geodesy/doc/igsSiteLog/equipment/2004" ,
        contact=>"http://sopac.ucsd.edu/ns/geodesy/doc/igsSiteLog/contact/2004" ,
        mi=>"http://sopac.ucsd.edu/ns/geodesy/doc/igsSiteLog/monumentInfo/2004" ,
        li=>"http://sopac.ucsd.edu/ns/geodesy/doc/igsSiteLog/localInterferences/2004" ,
        igs=>"http://sopac.ucsd.edu/ns/geodesy/doc/igsSiteLog/2011" ,
        xsi=>"http://www.w3.org/2001/XMLSchema-instance" ,
    };

sub _timestampFromIGSDate
{
    my($igsdate)=@_;
    return undef if $igsdate =~ /^\s*$/;
    my($y,$m,$d,$hh,$mm)=$igsdate=~/(\d+)/g;
    return ymdhms_seconds($y,$m,$d,$hh,$mm,0);
}

sub _readSiteLogNode
{
    my($node)=@_;
    my $data={};
    foreach my $c ($node->nonBlankChildNodes)
    {
        next if $c->nodeType != XML_ELEMENT_NODE;

        $data->{$c->localname}=$c->textContent;
    }
    return $data;
}

=head2 $sitelog=new LINZ::GNSS::IGSSiteLog( $xmlfilename )

Open and scan the site log file.

=cut

sub new
{
    my($class,$logxml)=@_;

    my $sitelog=XML::LibXML->load_xml(location=>$logxml);
    my $xpc=XML::LibXML::XPathContext->new($sitelog->documentElement);

    while( my ($prefix,$uri)=each(%$IGSNamespaces) ) 
    {
        $xpc->registerNs($prefix,$uri);
    }

    my $idnode=$xpc->find('igs:siteIdentification')->[0];
    my $id=_readSiteLogNode($idnode);

    my $xyzdata=_readSiteLogNode($xpc->find('igs:siteLocation/mi:approximatePositionITRF')->[0]);
    my $xyz=[
        ($xyzdata->{xCoordinateInMeters} // 0.0) + 0.0,
        ($xyzdata->{yCoordinateInMeters} // 0.0) + 0.0,
        ($xyzdata->{zCoordinateInMeters} // 0.0) + 0.0,
        ];

    my @receivers=();
    foreach my $recnode ($xpc->findnodes('igs:gnssReceiver'))
    {
        my $rec=_readSiteLogNode($recnode);
        foreach my $d ('dateInstalled','dateRemoved')
        {
            $rec->{$d}=_timestampFromIGSDate($rec->{$d});
        }
        push(@receivers,$rec);
    }
    @receivers = sort {$a->{dateInstalled} <=> $b->{dateInstalled}} @receivers;


    my @antennae=();
    foreach my $antnode ($xpc->findnodes('igs:gnssAntenna'))
    {
        my $ant=_readSiteLogNode($antnode);
        foreach my $d ('dateInstalled','dateRemoved')
        {
            $ant->{$d}=_timestampFromIGSDate($ant->{$d});
        }
        my $enu=[
            ($ant->{'marker-arpEastEcc.'} // 0.0) + 0.0,
            ($ant->{'marker-arpNorthEcc.'} // 0.0) + 0.0,
            ($ant->{'marker-arpUpEcc.'} // 0.0) + 0.0
        ];
        $ant->{offsetENU}=$enu;
        push(@antennae,$ant);
    }
    @antennae = sort {$a->{dateInstalled} <=> $b->{dateInstalled}} @antennae;

    my $self={id=>$id,approxXYZ=>$xyz, antennae=>\@antennae,receivers=>\@receivers};
    return bless $self, $class;
}

=head2 $code=$sitelog->code

Return the four character code from the site log identification section.

=cut

sub code
{
    my($self)=@_;
    return $self->{id}->{fourCharacterID};
}

=head2 $name=$sitelog->name

Return the name from the site log identification section.

=cut

sub name
{
    my($self)=@_;
    return $self->{id}->{siteName};
}

=head2 $domes=$sitelog->domesNumber

Return the domes number from the site log identification section.

=cut

sub domesNumber
{
    my($self)=@_;
    return $self->{id}->{iersDOMESNumber};
}

=head2 $xyz=$sitelog->approxXYZ

Return the approximate XYZ coordinate of the mark

=cut

sub approxXYZ
{
    my($self)=@_;
    return $self->{approxXYZ};
}

=head2 foreach my $antenna ($sitelog->antennaList)

Returns an array (or array ref, depending on context) of antennae installed
on the site.  The entries match the elements in the XML antenna section
except that dateInstalled and dateRemoved are converted to timestamps.
An additional field offsetENU is compiled from the offset information.
The list is sorted by dateInstalled.

=cut

sub antennaList
{
    my($self)=@_;
    return wantarray ? @{$self->{antennae}} : $self->{antennae};
}

=head2 foreach my $receiver ($sitelog->receiverList)

Returns an array (or array ref, depending on context) of antennae installed
on the site.  The entries match the elements in the XML antenna section
except that dateInstalled and dateRemoved are converted to timestamps.
The list is sorted by dateInstalled.

=cut

sub receiverList
{
    my($self)=@_;
    return wantarray ? @{$self->{receivers}} : $self->{receivers};
}

1;
