=head1 LINZ::GNSS::Config

Module to read a configuration file.  Supports the following features:

=over

=item Configuration file loaded with Config::General::ParseConfig

Supports apache format configuration files with nested configuration etc.
Note: uses the -LowerCase flag so all variable names are converted to 
lower case.

=item Allows interpolation of variables using ${xxx} syntax

Interpolation will use by default the configuration value of xxx, or
if that is not defined it will try to use an environment variable xxx.

=item Variables can be overridden by supplied configuration items

Supplied as an array of var=value strings can be interpreted and will over-ride
any settings in the configuration file

=item An configuration override can be specified config=xxx

If specified then the script will look for variables "var-xxx" before var
in the configuration file.  This allows overriding a few parameters in the
configuration file

The script will also look for version of the config file name with .xxx 
appended to the name, and use that to override any configuration items 
in the original file.

=item Automatically defined special variables 

The following variables are defined:

=over

=item ${configdir} - the directory of the configuration file

=item ${configname} - the name of the configuration file

=item ${yyyy} - the currently set year

=item ${ddd} - the currently set day of year

=item ${mm} - the currently set month

=item ${dd} - the currently set day of month

=item ${pid_#} - the process ID modified to # characters long

=back

The date values are by default based on the current time, but can be reset
using the setDate function. They are in terms of gmtime.

=back

=cut

use strict;

package LINZ::GNSS::Config;

use Carp;
use Config::General qw/ParseConfig/;
use Log::Log4perl qw(:easy);
use LINZ::GNSS::Time qw/parse_gnss_date/;

=head2 $cfg=LINZ::GNSS::Config->new($cfgfile,@overrides)

Load a configuration file.  $cfgfile is the filename.  @overrides is a list
of strings, within which strings formatted as "name=value" will be used
as configuration overrides.

An override config=xxx will test for an extra configuration file $cfgfile.xxx
which will overwrite values from the base configuration file. Also it will cause
variable var-xxx to override variable var.

=cut

sub new
{
    my($class,$cfgfile,@args)=@_;
    my %data=();
    my %args=(); # Args are overrides

    foreach my $a (@args)
    {
        $args{lc($1)}=$2 if $a =~ /^([^\=]+)\=(.*)/;
    }

    my $configdir=$cfgfile;
    $configdir=~ s/[^\\\/]*$//;
    $configdir=~ s/.$//;
    $configdir='.' if $configdir eq '';
    $args{configdir}=$configdir;
    my $configname=$cfgfile;
    $configname=~ s/.*[\\\/]//;
    $configname=~ s/\..*//;
    $args{configname}=$configname;


    croak("Configuration file $cfgfile is missing\n") if ! -f $cfgfile;

    my $errfile;
    eval
    {
        my @files=($cfgfile);
        push(@files,$cfgfile.'.'.$args{config}) 
            if $args{config} && -f $cfgfile.'.'.$args{config};
        foreach my $errfile (@files)
        {
            my %cfg=ParseConfig(-ConfigFile=>$errfile,-LowerCaseNames=>1);
            while( my ($key,$value) = each(%cfg) )
            {
                $data{$key}=$value;
            }
        }
    };
    if( $@ )
    {
        my $message=$@;
        $message =~ s/\s*\n.*//;
        croak("Error reading config file $errfile: $message\n");
    }

    my $self={data=>\%data,args=>\%args,configfile=>$cfgfile,_logger_init=>0};
    bless $self,$class;
    $self->setTime(time());

    return $self;
}

sub _set
{
    my($self,$key,$value)=@_;
    $self->{args}->{$key}=$value;
}

=head2 $cfg->setTime( $timestamp )

Sets the time used to expand ${yyyy}, ${ddd}, ${dd}, and ${mm} variables.

=cut

sub setTime
{
    my($self,$timestamp)=@_;
    my ($year,$mon,$day,$yday)=(gmtime($timestamp))[5,4,3,7];
    $self->_set('yyyy',sprintf("%04d",$year+1900));
    $self->_set('mm',sprintf("%02d",$mon+1));
    $self->_set('dd',sprintf("%02d",$day));
    $self->_set('ddd',sprintf("%03d",$yday+1));
    return $self;
}

=head2 $cfg->getRaw( $var )

Returns the un-interpolated value for a variable

=cut

sub getRaw
{
    my($self,$key,$default)=@_;

    my $lkey=lc($key);

    return $self->{args}->{$lkey} if exists $self->{args}->{$lkey};

    my $cfg=$self->{args}->{config};

    if( $cfg )
    {
        my $cfgkey=$lkey.'-'.$cfg;
        return $self->{data}->{$cfgkey} if exists $self->{data}->{$cfgkey};
    };

    return $self->{data}->{$lkey} if exists $self->{data}->{$lkey};

    if( $lkey =~ /^pid_(\d)$/ )
    {
        my $nch=$1;
        my $value='0000000000'.$$;
        return substr($value,-$nch);
    }

    return $ENV{$key} if exists $ENV{$key};

    return $default if defined $default;

    croak("Key $key missing in configuration ".$self->{configfile});
}

=head2 $value=$cfg->get($var)

Returns the interpolated value for a variable

=cut

sub get
{
    my( $self, $key, $default ) = @_;
    my $value=$self->getRaw($key,$default);
    my $maxexpand=5;
    while( $value=~ /\$\{\w+\}/ && $maxexpand-- > 0)
    {
        $value =~ s/\$\{(\w+)\}/$self->getRaw($1)/eg;
    }
    return $value;
}

=head2 $value=$cfg->getDate($var)

Returns the value of an interpolated variable interpreted as
a GNSS date (using LINZ::GNSS::Time::parse_gnss_date).  Returns
the date as a timestamp.

=cut

sub getDate
{
    my ($self,$key)=@_;
    my $datestr=$self->get($key);
    my $seconds=0;
    $datestr = "now$datestr" if $datestr =~ /^\-(\d+)$/;
    eval
    {
        $seconds = parse_gnss_date($datestr);
    };
    return $seconds;
}

=head2 $logger=$cfg->logger($loggerid);

Gets a Log::Log4perl logger based on configuration settings. The logger
is initiallized to the warning level if log settings are not defined.

The log settings are defined by variables 

=over

=item logdir: the directory for the log file

=item logfile: the name of the logfile

=item logsettings: the Log4perl logger definition

=back

The logsettings can include the string [logfilename] which will be substituted
with the name built from logdir and logfile.

The logger is initiallized with an id specified by the $loggerid parameter,
which defaults to 'LINZ::GNSS'.

=cut

sub logger
{
    my ($self, $loggerid) = @_;
    if( ! $self->{_logger_init} )
    {
        my $logcfg=$self->get('logsettings','');
        if(  $logcfg )
        {
            my $logfile=$self->get('logdir').'/'.$self->get('logfile');
            $logcfg =~ s/\[logfilename\]/$logfile/eg;
            Log::Log4perl->init(\$logcfg);
        }
        else
        {
            Log::Log4perl->easy_init($WARN);
        }
    }
    $loggerid ||= 'LINZ::GNSS';
    return Log::Log4perl->get_logger($loggerid);
};

1;
