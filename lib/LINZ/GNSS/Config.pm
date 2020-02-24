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

=item A configuration override can be specified config=xxx

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

=item ${user} - the logged in user id

=item ${yyyy} - the currently set year

=item ${yy} - the currently set year (two digits)

=item ${ddd} - the currently set day of year

=item ${mm} - the currently set month

=item ${dd} - the currently set day of month

=item ${pid_#} - the process ID modified to # characters long

=back

The date values are by default based on the current time, but can be reset
using the setTime function. They are in terms of gmtime.

Each of the variables can also be offset by a number of days, eg
${yyyy+14} ${ddd+14}

A very limited conditional value can be defined using ${vvvv?xxx:yyy} which
will evaluate to xxx if variable v is not empty or yyyy if it is.  xxx and
yyy must be simple strings not including ?, :, or }.

=item Date based configuration overrides

The file can include a special configuration item "date_based_configuration" 
which defines alternative configuration settings based on date range, similar
to the configuration override.  Date based configuration is defined by a multiline
configuration in which each line is formatted as 

    cfgtype before|after date [comment]

For example

  date_based_configuration <<END
    finalorbits after now-30
    posteq after 2016-11-13
    nocod2 before 2005-08-01  CODE2 products not available before August 2005
  END

  products CODE2
  products-nocod2 CODE

The comment is ignored.  The configuration setting nocod2 will apply before 2005-08-01.
Any other configuration item xxx can have an alternative version xxx-nocod2 which will
be used before that date. If several date configurations are defined that could apply
for an item then the first one in the date_based_configuration list will be used.
For example in the configuration above if both finalorbits and posteq apply at
the currently set date, then xxx-finalorbits would be used in preference to xxx-posteq.

=back

=cut

use strict;

package LINZ::GNSS::Config;

use Carp;
use Config::General qw/ParseConfig/;
use Log::Log4perl qw(:easy);
use LINZ::GNSS::Time qw/parse_gnss_date $SECS_PER_DAY/;

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
    $args{user} = getlogin if ! $args{user};


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

    my $suffices=[];
    push(@$suffices,'-'.$args{config}) if $args{config};
    push(@$suffices,'');

    my $self={data=>\%data,args=>\%args,configfile=>$cfgfile,suffices=>$suffices,_logger_init=>0};
    bless $self,$class;
    $self->_loadDateBasedConfiguration();
    $self->setTime(time());

    return $self;
}

sub _set
{
    my($self,$key,$value)=@_;
    $self->{args}->{$key}=$value;
}

sub _loadDateBasedConfiguration
{
    my($self)=@_;
    my $dbcfg=$self->getRaw('date_based_configuration','');
    my $dateconfigs=[];
    foreach my $cfg (split(/\n/,$dbcfg))
    {
        my @parts=split(' ',$cfg);
        next if ! @parts;
        eval
        {
            die "Incomplete date based configuration item\n" 
               if $#parts < 2;
            my $cfgcode = lc($parts[0]);
            my $before = lc($parts[1]);
            my $datestr = $parts[2];
            die "Invalid configuration $cfgcode\n" if $cfgcode !~ /^\w+$/;
            die "Need before/after not $before\n" if $before !~ /^(before|after)$/;
            my $date;
            eval
            {
                $date=parse_gnss_date($datestr);
            };
            die "Invalid date $datestr\n" if $@;
            push(@$dateconfigs,[$before,$date,$cfgcode]);
        };
        if( $@ )
        {
            my $msg=$@;
            $msg =~ s/\s*$//;
            $msg .= " in date based configuration: $cfg\n";
            die $msg;
        }
    }
    $self->{date_configuration}=$dateconfigs;
}

sub _setDateBasedConfiguration
{
    my($self,$timestamp) = @_;
    my @configs=();
    my @sprefix=();
    my $arg=$self->{args}->{config};
    push(@sprefix,'-'.$arg) if $arg;
    push(@sprefix,'');
    foreach my $dbc ( @{$self->{date_configuration}} )
    {
        my ($before,$date,$cfgcode) = @$dbc;
        my $isok=$timestamp < $date;
        $isok = ! $isok if $before eq 'after';
        push(@configs,$cfgcode) if $isok;
    }
    my @suffices=();
    foreach my $sp ( @sprefix )
    {
        foreach my $dc (@configs)
        {
            push( @suffices, $sp.'-'.$dc );
        }
        push( @suffices, $sp );
    }
    $self->{suffices}=\@suffices;
}


=head2 $cfg->setTime( $timestamp )

Sets the time used to expand ${yyyy}, ${ddd}, ${dd}, and ${mm} variables.

=cut

sub setTime
{
    my($self,$timestamp)=@_;
    my ($year,$mon,$day,$yday)=(gmtime($timestamp))[5,4,3,7];
    my $yyyy=sprintf("%04d",$year+1900);
    $self->{_timestamp}=$timestamp;
    $self->_set('yyyy',$yyyy);
    $self->_set('yy',substr($yyyy,2));
    $self->_set('mm',sprintf("%02d",$mon+1));
    $self->_set('dd',sprintf("%02d",$day));
    $self->_set('ddd',sprintf("%03d",$yday+1));
    $self->_setDateBasedConfiguration( $timestamp );
}


=head2 $cfg->getRaw( $var )

Returns the un-interpolated value for a variable

=cut

sub getRaw
{
    my($self,$key,$default)=@_;

    my $lkey=lc($key);

    return $self->{args}->{$lkey} if exists $self->{args}->{$lkey};

    foreach my $sf (@{$self->{suffices}})
    {
        my $cfgkey=$lkey.$sf;
        return $self->{data}->{$cfgkey} if exists $self->{data}->{$cfgkey};
    };

    if( $lkey =~ /^pid_(\d)$/ )
    {
        my $nch=$1;
        my $value='0000000000'.$$;
        return substr($value,-$nch);
    }

    return $ENV{$key} if exists $ENV{$key};

    return $default if defined $default;

    die "$key missing\n";
}

=head2 $value=$cfg->get($var, $default)

Returns the interpolated value for a variable

=cut

sub get
{
    my( $self, $key, $default, $depth, $offset ) = @_;
    $depth //= 0;
    $offset //=0;
    my $maxdepth=$self->getRaw('max_config_interpolation_depth',5);
    if( $depth > 0 && $depth > $maxdepth )
    {
        die "Configuration file interpolation nesting > max_config_interpolation_depth\n";
    }
    my $timestamp=$self->{_timestamp};
    if( $offset )
    {
        $self->setTime($timestamp+$offset*$SECS_PER_DAY);
    }
    my $value='';
    eval
    {
        $value=$self->getRaw($key,$default);
        my $nextdepth=$depth;
        while( $value=~ /\$\{\w+(?:[+-]\d+|\?[^\?\:\}]*\:?[^\?\:\}]*)?\}/)
        {
            $nextdepth++;
            $value =~ s/\$\{(\w+)\}/$self->get($1,undef,$nextdepth,0)/eg;
            $value =~ s/\$\{(\w+)([+-]\d+)\}/$self->get($1,undef,$nextdepth,$2)/eg;
            $value=~ s/\$\{(\w+)\?([^\?\:\}]*)(?:\:([^\?\:\}]*))?\}/
                      $self->get($1,undef,$nextdepth,0) ? ($2 || '') : ($3 || '')
                      /xeg;
        }
        $self->setTime($timestamp) if $offset;
    };
    if( $@ )
    {
        $self->setTime($timestamp) if $offset;
        my $msg=$@;
        die $msg if $depth > 0;
        $msg =~ s/\s*$//;
        $msg .= ' evaluating '.$key.' in '.$self->{configfile}."\n";
        croak($msg);
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

=item logsettings: the Log::Log4perl logger definition

=back

The logsettings can include the string [logfilename] which will be substituted
with the name built from logdir and logfile.

Instead of a full Log::Log4perl definition logsettings can simply be the log level,
one of trace, debug, info, warn, error, or fatal.

The logger is initiallized with an id specified by the $loggerid parameter,
which defaults to 'LINZ::GNSS'.

=cut

sub logger
{
    my ($self, $loggerid) = @_;
    if( ! $self->{_logger_init} )
    {
        my $logcfg=$self->get('logsettings','WARN');
        my $logfile=$self->get('logdir','');
        $logfile .= '/' if $logfile;
        $logfile .= $self->get('logfile','');

        if( $logcfg =~ /^(trace|debug|info|warn|error|fatal|)$/i )
        {
            my $options={};
            if( $logfile ne '' ){ $options->{file}=$logfile; }
            $logcfg = uc($logcfg) || 'WARN';
            if( $logcfg eq 'TRACE') { $options->{level}=$TRACE; }
            if( $logcfg eq 'DEBUG') { $options->{level}=$DEBUG; }
            if( $logcfg eq 'INFO') { $options->{level}=$INFO; }
            if( $logcfg eq 'WARN') { $options->{level}=$WARN; }
            if( $logcfg eq 'ERROR') { $options->{level}=$ERROR; }
            if( $logcfg eq 'FATAL') { $options->{level}=$FATAL; }
            Log::Log4perl->easy_init($options);
        }
        elsif(  $logcfg )
        {
            $logcfg =~ s/\[logfilename\]/$logfile/eg;
            Log::Log4perl->init(\$logcfg);
        }
    }
    $loggerid ||= 'LINZ::GNSS';
    return Log::Log4perl->get_logger($loggerid);
};

1;
