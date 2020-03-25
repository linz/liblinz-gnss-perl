
use strict;

package LINZ::GNSS::DataCenter::Ftp;
use base "LINZ::GNSS::DataCenter";
use fields qw (
    ftp_passive
    ftp
    timeout
    );

use LINZ::GNSS::DataCenter;
use Net::FTP;
use Carp;

sub new
{
    my($self,$cfgdc)=@_;
    $self=fields::new($self) unless ref $self;
    $self->SUPER::new($cfgdc);
    $self->{timeout} = $cfgdc->{timeout} || $LINZ::GNSS::DataCenter::ftp_timeout;
    $self->{ftp_passive} = $cfgdc->{ftppassive} || $LINZ::GNSS::DataCenter::ftp_passive;
    return $self;
}

=head2 $center->connect

Initiates an FTP connection with the server

=cut

sub connect
{
    my($self) = @_;
    if( ! $self->{ftp} )
    {
        my $host=$self->{host};
        my ($user,$pwd)=$self->credentials(1);
        my $timeout=$self->{timeout};
        my $ftpmode=$self->{ftp_passive};
        my $passive;
        $passive=1 if lc($ftpmode) eq 'on';
        $passive=0 if lc($ftpmode) eq 'off';
	my %options=();
	$options{Passive}=$passive if defined($passive);

        eval
        {
            my $name=$self->{name};
            $self->_logger->info("Connecting datacenter $name to host $host");
            $self->_logger->debug("Connection info: host $host");
            $self->_logger->debug("Connection info: user $user");
            $self->_logger->debug("Connection info: timeout $timeout");
            $self->_logger->debug("Connection info: passive $passive");

            $self->_logger->debug("Connection info: host $host: user $user: password $pwd");
            my $ftp=Net::FTP->new( $host, Timeout=>$timeout, %options )
               || croak "Cannot connect to $host\n";

            $self->{ftp}=$ftp;
            $ftp->login( $user, $pwd )
               || croak "Cannot login to $host as $user\n";
            $ftp->binary();
       };
       if( $@ )
       {
           my $message=$@;
           $self->_logger->warn($@);
           croak $@;
       }
   }
   $self->SUPER::connect();
}

=head2 $center->disconnect

Terminates an FTP connection with the server

=cut

sub disconnect
{
    my($self) = @_;
    $self->{ftp}->quit() if $self->{ftp};
    $self->{ftp} = undef;
    $self->SUPER::disconnect();
}

# Get a file from the data centre

sub getfile
{
    my($self,$path,$file,$target)=@_;
    $path=$self->{basepath}.'/'.$path if $self->{basepath};
    my $source="$path/$file";
    my $ftp = $self->{ftp};
    my $host=$self->{host};
    $self->_logger->debug("Retrieving file $path/$file");
    if( ! $ftp || ! $ftp->cwd($path) || ! $ftp->get($file,$target) )
    {
        $self->_logger->warn("Cannot retrieve file $path/$file from $host");
        croak "Cannot retrieve $file from $host\n";
    }
    my $size= -s $target;
    $self->_logger->info("Retrieved $file ($size bytes) from $host");
}

1;
