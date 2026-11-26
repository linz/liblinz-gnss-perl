
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
use LINZ::GNSS::TransferError;

# Error for a failed FTP command, retryable if the reply code is transient (or there was
# no reply, as for a dropped connection or timeout)

sub _ftpError
{
    my($self,$message)=@_;
    my $ftp=$self->{ftp};
    my $code=$ftp ? $ftp->code : 0;
    my $reply=$ftp ? $ftp->message : '';
    $reply =~ s/\s+$//;
    $message .= " ($code $reply)" if $code;
    return LINZ::GNSS::TransferError->new(
        message=>"$message\n",
        code=>$code,
        retryable=>LINZ::GNSS::TransferError::IsRetryableFtpCode($code),
        );
}

sub new
{
    my($self,$cfgdc)=@_;
    $self=fields::new($self) unless ref $self;
    $self->SUPER::new($cfgdc);
    $self->{timeout} = $cfgdc->{timeout} || $LINZ::GNSS::DataCenter::ftp_timeout;
    $self->{ftp_passive} = $cfgdc->{ftppassive} || $LINZ::GNSS::DataCenter::ftp_passive;
    $self->{_checkfilelist} = exists $cfgdc->{usefilelist} ? $cfgdc->{usefilelist} : 1;
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
            # Failing to connect (refused, timed out, ...) may succeed if retried
            my $ftp=Net::FTP->new( $host, Timeout=>$timeout, %options )
               || die LINZ::GNSS::TransferError->new(message=>"Cannot connect to $host: $@\n",retryable=>1);

            $self->{ftp}=$ftp;
            $ftp->login( $user, $pwd )
               || die $self->_ftpError("Cannot login to $host as $user");
            $ftp->binary();
       };
       if( $@ )
       {
           my $error=$@;
           $self->_logger->warn("$error");
           eval { $self->{ftp}->quit() if $self->{ftp}; };
           $self->{ftp}=undef;
           die $error;
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
    eval { $self->{ftp}->quit() if $self->{ftp}; };
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
        my $error=$self->_ftpError("Cannot retrieve $file from $host");
        $self->_logger->warn("Cannot retrieve file $path/$file from $host") if ! $error->retryable;
        die $error;
    }
    my $size= -s $target;
    $self->_logger->info("Retrieved $file ($size bytes) from $host");
}

# Get a directory listing - used for wildcard file names

sub getfilelist
{
    my ($self,$spec) = @_;
    my $path=$spec->path;
    my $ftp = $self->{ftp};
    my $filelist;
    if( ! $ftp || ! $ftp->cwd($path) || ! ($filelist = $ftp->ls()))
    {
        my $host = $self->{host};
        my $error=$self->_ftpError("Cannot get file list from $path on $host");
        $self->_logger->warn("$error") if ! $error->retryable;
        die $error;
    }
    return $filelist;
}

1;
