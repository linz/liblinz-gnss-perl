
=head1 LINZ::GNSS::DataCenter

Defines and accesses a source of GNSS products, such as orbit information or RINEX files.

Each data centre is defined by the connection information and by the set of file types
that it supports (as a LINZ::GNSS::FileTypeList instance).  The data centre configuration may
override the default configuration information for the file types, for example to redefine
the filename, path, or compression.

Data can be either accessed via ftp, in which case they have a uri structured as 

    ftp://usr:pwd@ftp.somewhere

or http

    http://www.somewhere..

or they can be simple file system locations (typically for a cache), in which case the uri is 
a local file uri defining the base directory of the store, eg

    file:///usr/local/share/gnssdata

Data centres have a priority value which defines the preference for using them to get
the files. 

They may also define a list of four letter station codes defining the stations for which
they hold information.

=cut

package LINZ::GNSS::DataCenter;
use fields qw (
    id
    name
    user
    password
    filetypes
    stncodes
    excludestations
    allstations
    priority
    scratchdir
    uri
    scheme
    host
    basepath
    connected
    fileid
    _logger
    _filelistcache
    );

# scratchdir, fileid, and ftp are managed internally to 

use Carp;
use JSON;
use URI;
use URI::file;
use File::Path qw( make_path remove_tree );
use File::Copy;
use Log::Log4perl;

use LINZ::GNSS::FileCompression;
use LINZ::GNSS::DataRequest qw(UNAVAILABLE PENDING DELAYED COMPLETED);
use LINZ::GNSS::Variables qw(ExpandEnv);

our $nextid=0;
our $centers=[];
our $prioritized_centers=[];
our $ftp_user='anonymous';
our $ftp_password='none';
our $ftp_timeout=30;
our $ftp_passive='default';

sub makepublicpath
{
    my ($path)=@_;
    return 1 if -d $path;
    my $errval;
    my $umask=umask(0000);
    make_path($path,{error=>\$errval, mode=>0777});
    umask($umask);
    return -d $path ? 1 : 0;
}

sub _getnameuri
{
    my ($cfgdc)=@_;
    my $name=$cfgdc->{name} || croak "Name missing for datacenter\n";
    my $uri=$cfgdc->{uri} || croak "Uri missing for datacenter $name\n";
    $uri = ExpandEnv($uri,"uri of datacenter $name");
    my $uriobj=URI->new($uri);
    my $scheme=$uriobj->scheme || 'file';
    return $name,$uri,$scheme;
}

=head2 LINZ::GNSS::DataCenter::Create($cfgdc)

Class factory creating a DataCenter.  This instantiates a 
DataCenter subclass based on the type defined in the 
configuration.  If this is not explicitly defined then the 
scheme of the URI is used (eg ftp, http, https, file).

=cut

sub Create
{
    my ($cfgdc) = @_;
    my ($name,$uri,$scheme)=_getnameuri($cfgdc);
    $scheme='http' if $scheme eq 'https';
    my $class=$cfgdc->{type} || ucfirst(lc($scheme));
    $class='S3Bucket' if $class eq 'S3';
    $class="LINZ::GNSS::DataCenter::$class";
    my $location=$class;
    $location =~ s/\:\:/\//g;
    $location .= '.pm';
    require $location;
    return $class->new($cfgdc);
}

=head2 LINZ::GNSS::DataCenter->new($cfgdc)

Creates a LINZ::GNSS::DataCenter from the information in a configuration section.  The
configuration is defined as 

  name data_center_name
  priority #
  uri  data_center_uri
  stations stn1 stn2 ...
  ....
  <datafiles>
     <type>
        <subtype>
        config items
        </subtype>
     </type>
     ...
  </datafiles>

The configuration may contain many station records if necessary.  It may also 
omit the station record if it does not hold data about specific stations.

The datafiles section is documented in the LINZ::GNSS::FileType documentation.
The datafile section can be omitted, in which case the default file type list is 
used.

The priority defines the preference from which files are located.  A priority 
of 0 means a datacentre will not be selected by default, it must be explicitly 
chosen.  

The URI can either be an ftp or local file scheme (eg ftp://user:password@ftp.somewhere)
or a file base scheme (eg file:///home/gps/data).  

File based data centers can be both source and target data centres.  Ftp based data centers 
are only supported as sources.

=cut

sub new
{
    my ($self,$cfgdc) = @_;
    $self=fields::new($self) unless ref $self;
    my ($name,$uri,$scheme)=_getnameuri($cfgdc);
    my $filetypes;
    if( exists $cfgdc->{datafiles} )
    {
        $filetypes=new LINZ::GNSS::FileTypeList($cfgdc->{datafiles}); 
    }
    else
    {
        $filetypes=$LINZ::GNSS::FileTypeList::defaultTypes->clone();
    }

    my $stnlist=$cfgdc->{stations} || [];
    $stnlist = ref($stnlist) eq 'ARRAY' ? join(' ',@$stnlist) : $stnlist;
    my $stncodes = {};
    my $allstations=0;
    my $stationlists=$cfgdc->{stationlists} || {};
    foreach my $s (split(' ',$stnlist))
    {
        if( $s eq '*' ) { $allstations=1; }
        elsif( $s =~ /^\@(\w+)$/ )
        {
            foreach my $s (@{$stationlists->{lc($1)}})
            {
                $stncodes->{uc($s)}=$s;
            }
        }
        else { $stncodes->{uc($s)}=$s; }
    }
    my $notstnlist=$cfgdc->{notstations} || [];
    $notstnlist = ref($notstnlist) eq 'ARRAY' ? join(' ',@$notstnlist) : $notstnlist;
    my $excludestations={};
    foreach my $s (split(' ',$notstnlist))
    {
        if( $s =~ /^\@(\w+)$/ )
        {
            foreach my $s (@{$stationlists->{lc($1)}})
            {
                $excludestations->{uc($s)}=$s;
            }
        }
        else { $excludestations->{uc($s)}=$s; }
    }
    my $priority=$cfgdc->{priority} || 0;

    # Create a scratch directory that is unique
    # Use the data centre id and the process id ($$) to achieve this
    # Each downloaded file will use fileid to generate a unique file name
    my $id='dc'.(++$LINZ::GNSS::DataCenter::nextid);
    my $scratchdir=$LINZ::GNSS::DataCenter::scratchdir."/gnss_gdt_$id"."_".$$;

    # Process the object
    my $host='';
    $self->{id}=$id;
    $self->{name}=$name;
    $self->{filetypes}=$filetypes;
    $self->{stncodes}=$stncodes;
    $self->{excludestations}=$excludestations;
    $self->{allstations}=$allstations;
    $self->{priority}=$priority;
    $self->{scratchdir}=$scratchdir;
    $self->{uri}=$uri;

    # Crude handling for S3 urls
    my $testuri=$uri;
    $testuri =~ s/^s3\:/ftp:/;
    my $uriobj=URI->new($testuri);
    my ($user,$pwd);
    if( $scheme ne 'file' )
    {
        $host=$uriobj->host;
        my $userinfo = $uriobj->userinfo;
        ($user,$pwd)=split(/\:/,$userinfo,2);
    }

    if( $cfgdc->{user} )
    {
        $user = $cfgdc->{user};
        $pwd = $cfgdc->{password};
    }

    my $credfile=$cfgdc->{credentialsfile};
    if( $credfile )
    {
        $credfile=ExpandEnv($credfile);
        ($user,$pwd)=$self->_readCredentials($credfile);
    }

    $self->{scheme}=$scheme;
    $self->{host}=$host;
    $self->{basepath}=$uriobj->path;
    $self->{user} = $user;
    $self->{password} = $pwd;
    $self->{connected}=undef;
    $self->{fileid}=0;
    $self->{_filelistcache}={};
    $self->{_logger}=Log::Log4perl->get_logger('LINZ.GNSS.DataCenter'.$name);
    $self->_logger->debug("Created DataCenter $name");
    return $self;
}

sub DESTROY
{
    my($self)=@_;
    if( $self->{ftp} )
    {
        $self->disconnect();
    }
    if( -d $self->{scratchdir} )
    {
        my $errval=undef;
        remove_tree( $self->{scratchdir}, {error=>\$errval});
        # Should add some logging here for failure to clean up..
    }
}

=head2 LINZ::GNSS::DataCenter::LoadDataCenters( $cfg )

Loads the set of data centres from a configuration file.  The data centres are 
defined as 

<datacenters>
   <datacenter>
      config items
   </datacenter>
   ...
</datacenters>

Also loads common information for

AnonymousFtpPassword

=cut

sub LoadDataCenters
{
    my($cfg) = @_;

    my $logger=Log::Log4perl->get_logger('LINZ.GNSS.DataCenter');
    $logger->debug("Loading data centres");

    # Configuration information used by all centres
    $LINZ::GNSS::DataCenter::scratchdir=$cfg->{scratchdir};

    # Default login information
    my $pwd=$cfg->{anonymousftppassword};
    $LINZ::GNSS::DataCenter::ftp_password=$pwd if $pwd;

    my $passive=$cfg->{ftppassive};
    $LINZ::GNSS::DataCenter::ftp_passive=$passive if $passive;

    # Load data centers
    my $dcs=$cfg->{datacenters}->{datacenter};

    $dcs = ref($dcs) eq 'ARRAY' ? $dcs : [$dcs];
    my $centers=[];
    my @prioritized_centers=();
    my $stationlists={};
    my $sourcelists=$cfg->{stationlists};
    if( $sourcelists )
    {
       foreach my $list (keys %$sourcelists)
       {
           my @stations=split(' ',$sourcelists->{$list});
           $stationlists->{lc($list)}=\@stations;
       }
    }
    foreach my $cfgdc (@$dcs)
    {
        eval
        {
            $cfgdc->{stationlists}=$stationlists;
            my $center = LINZ::GNSS::DataCenter::Create($cfgdc);
            push(@$centers,$center);
            push(@prioritized_centers,$center) if $center->priority > 0;
        };
        if( $@ )
        {
            carp "Datacenter not defined: ",$@;
        }
    }
    @prioritized_centers = sort {$b->priority <=> $a->priority} @prioritized_centers;
    $LINZ::GNSS::DataCenter::centers=$centers;
    $LINZ::GNSS::DataCenter::prioritized_centers=\@prioritized_centers;

    $logger->debug("Prioritized data centres: ".join(', ', map {$_->name} @prioritized_centers));

    return $centers;
}

=head2 LINZ::GNSS::DataCenter::GetCenter( $name )

Returns the named data center

=cut

sub GetCenter
{
    my ($name) = @_;
    foreach my $c (@{$LINZ::GNSS::DataCenter::centers})
    {
        return $c if lc($c->name) eq lc($name);
    }
    return undef;
}

=head2 LINZ::GNSS::DataCenter::LocalDirectory( $dir, %opts )

Creates a data center representing a local directory to which files can be stored.
Files are uncompressed, filenames upper case unless specified otherwise. If a name
for the center is not defined in the options then it will create a unique name.

Options can include:

=over

=item * name=>'local'       Defines the name of the data centre

=item * lowerCaseNames=>1   Saved file names will be lower case

=item * compress=>1         Retain default compression

=item * paths=>1            Keep default paths within directory (default is just use filename)

=back

=cut

sub LocalDirectory
{
    my($dir,%opts)=@_;
    croak("GNSS data target $dir is not a directory\n") if ! -d $dir;
    my $name=$opts{name};
    if( ! $name )
    {
        $name='local';
        my $ntry=0;
        while(1)
        {
            last if ! LINZ::GNSS::DataCenter::GetCenter($name);
            $ntry++;
            $name=sprintf("local%02d",$ntry);
        }
    }
    my $cfg={
        name=>$name,
        uri=>$dir,
        stations=>'*',
        };
    my $dtc=LINZ::GNSS::DataCenter::Create($cfg);
    foreach my $ft ($dtc->filetypes->types)
    {
        my $filename=uc($ft->filename);
        if( ! $opts{compress} )
        {
            $filename =~ s/\.Z$//;
            $filename =~ s/\.GZ$//;
            $filename =~ s/\]D$/]O/;
        }
        $filename = lc($filename) if $opts{lowerCaseNames};
        $ft->setFilename($filename);
        $ft->setPath('') if ! $opts{paths};
        $ft->setCompression('none');
    }
    return $dtc;
}

=head2 LINZ::GNSS::DataCenter::AvailableStations()

Returns a sorted list of stations codes that may be available from the data centres

=cut

sub AvailableStations
{
    my %codes=();
    foreach my $c (@{$LINZ::GNSS::DataCenter::centers})
    {
        foreach my $scode ($c->stations)
        {
            $codes{$scode} = 1;
        }
    }
    my @stncodes=sort(keys(%codes));
    return wantarray ? @stncodes : \@stncodes;
}

=head2 LINZ::GNSS::DataCenter::SourceDescriptions( $request )

Returns a string listing the priorities data centres.

If $request is not defined then returns a list of data centres and 
what they can provide.  If $request is defined then returns the 
data centres and URLs from which they can be retrieved

=cut

sub SourceDescriptions
{
    my ($request)=@_;

    my $dsc='';
    my $prefix="";

    # If no request, just list the descriptions of the centres
    if( ! defined($request) )
    {
        foreach my $center (@$LINZ::GNSS::DataCenter::prioritized_centers)
        {
            $prefix="\n";
            $dsc .= $prefix.$center->description();
        }
    }

    # Otherwise find the files from each centre that will provide the data
    else
    {

        my $uris={};
        foreach my $center (@$LINZ::GNSS::DataCenter::prioritized_centers)
        {
            my($when,$files)=$center->checkRequest($request);
            next if ! $when;
            my $ctrdsc .= $prefix.$center->description(1);
            $prefix="\n";
            foreach my $spec (@$files)
            {
                my $uri=$center->{uri};
                $uri .= $spec->{path}.'/' if $spec->{path};
                $uri .= $spec->{filename};
                next if exists($uris->{$uri});
                $uris->{$uri}=1;
                $dsc .= $ctrdsc."  ".$uri."\n";
                $ctrdsc='';
            }
        }
    }
    return $dsc;
}

=head2 LINZ::GNSS::DataCenter::WhenAvailable( $request )

Tests the prioritised datacenters to see when the request should be able to be serviced.
Returns the earliest date available.

=cut

sub WhenAvailable
{
    my($request) = @_;
    $request = LINZ::GNSS::DataRequest::Parse($request) if ! ref $request;
    my $available=undef;
    foreach my $center (@$LINZ::GNSS::DataCenter::prioritized_centers)
    {
        my ($when, $files) =  $center->checkRequest($request);
        next if ! $when;
        $available = $when if ! $available || $when < $available;
    }
    return $available;
}

=head2 $status, $when, $files = LINZ::GNSS::DataCenter::FillRequest( $request, $target )

Tries to fill a data request to a target datacenter.  Will try each prioritized 
data center from highest to lowest priority, except that if the request includes a 
station then the algorithm will favour data centers that explicitly provide the station
over wildcard matches.

Returns a status, when the files expect to be available, and an array ref of downloaded file specs. 
The status is one of the values returned by getData;

=over

=item COMPLETED - the request is completed and the files used are listed

=item PENDING - the request cannot be filled yet

=item DELAYED - the request should have been able to be filled now but cannot be

=item UNAVAILABLE - the request will never be able to be filled

=back

=cut

sub FillRequest
{
    my($request,$target) = @_;
    $request = LINZ::GNSS::DataRequest::Parse($request) if ! ref $request;
    my $available=undef;
    my $status = UNAVAILABLE;
    my $downloaded = [];

    # Try each valid subtype in order of priority...
   
    foreach my $typeoption (LINZ::GNSS::FileTypeList->getTypes($request))
    {
        my $subtype=$typeoption->subtype;

        # Find potential centers to supply the data, based on centers priority,
        # but favouring exact station match over wildcard match
        my @centers=();
        my @unmatch_centers=();
        foreach my $center (@$LINZ::GNSS::DataCenter::prioritized_centers)
        {
            # Try matching exact station
            my ($when) = $center->checkRequest($request,1,$subtype);
            if( $when )
            {
                push(@centers,$center);
                next;
            }
            # Try matching inexactly
            ($when) = $center->checkRequest($request,0,$subtype);
            if( $when )
            {
                push(@unmatch_centers,$center);
            }
        }

        foreach my $center (@centers, @unmatch_centers)
        {
            my ($result, $when, $files) = $center->getData($request,$target,$subtype);
            next if $result eq UNAVAILABLE;
            $available=$when if ! $available || $when < $available;

            if( $result eq COMPLETED )
            {
                push(@$downloaded,@$files);
                $status = $result;
                last;
            }
            elsif( $result eq DELAYED )
            {
                $status = $result;
            }
            elsif( $result eq PENDING && $status ne DELAYED )
            {
                $status = $result;
            }
        }
        last if $status eq COMPLETED;
    }
    # If status not completed then set the status based on the best option

    if( $status ne COMPLETED )
    {
        $request->setStatus($status,'',$available);
    }
    return $status, $available, $downloaded;
}


=head2 $center->component

Accessor functions for attributes of the data centre. Attributes are:

=over

=item name

=item uri

=item filetypes

=item stations

=item excludestations

=item priority

=item scheme

=item host

=item basepath

=back

=cut

sub name { return $_[0]->{name}; }
sub uri { return $_[0]->{uri}; }
sub filetypes { return $_[0]->{filetypes}; }
sub stations { return sort(keys(%{$_[0]->{stncodes}})); }
sub excludestations { return sort(keys(%{$_[0]->{excludestations}})); }
sub priority { return $_[0]->{priority}; }
sub scheme { return $_[0]->{scheme}; }
sub host { return $_[0]->{host}; }
sub basepath { return $_[0]->{basepath}; }
sub _logger { return $_[0]->{_logger}; }

=head2 $text = $center->description()

Returns a text description of the data center and the file types it provides

=cut

sub description
{
    my($self,$brief)=@_;
    my $dsc='Data center: '.$self->name.' ('.$self->{scheme}.'://'.$self->{host}.")\n";
    return $dsc if $brief;
    my $usestn=0;
    my $prefix="    Data types: ";
    foreach my $ft (@{$self->filetypes->types})
    {
        $dsc .= $prefix.$ft->type.': '.$ft->subtype."\n";
        $prefix="                ";
        $usestn ||= $ft->use_station;
    }
    if( $usestn )
    {
        $prefix="      Stations: ";
        if( $self->{allstations} )
        {
            $dsc .= $prefix."Any station code.";
            $prefix="  Explicitly supports the following codes\n                ";  
        }
        my $nst=0;
        foreach my $st ($self->stations)
        {
            $dsc.=$prefix.$st;
            $nst++;
            $prefix=' ';
            if( $nst == 12 )
            {
                $prefix="\n                ";
                $nst=0;
            }
        }
        $dsc .= "\n";
        my @excludestations=$self->excludestations;
        if( $self->{allstations} && @excludestations )
        {
            $prefix="  Not stations: ";
            my $nst=0;
            foreach my $st (@excludestations)
            {
                $dsc.=$prefix.$st;
                $nst++;
                $prefix=' ';
                if( $nst == 12 )
                {
                    $prefix="\n                ";
                    $nst=0;
                }
            }
            $dsc .= "\n";
        }
    }
    return $dsc;
}

=head2 $center->setFilename($type,$subtype,$filename)

Resets the name for a file type (or set of filetypes if subtype include +).

=cut

sub setFilename
{
    my($self,$type,$subtype,$filename) = @_;
    $self->filetypes->setFilename($type,$subtype,$filename);
}

=head2 $center->canSetFilename($type,$subtype,$filename)

Tests whether setFilename is valid for a specific subtype.  

=cut

sub canSetFilename
{
    my($self,$type,$subtype,$filename) = @_;
    $self->filetypes->canSetFilename($type,$subtype,$filename);
}

=head2 $when,$files = $center->checkRequest($request, $matchstation, $subtype)

Checks whether a data centre should be able to supply a request. Returns when 
the request should be able to be filled, and the list of files the it will 
supply.  If the time is in the future then the file list returned is empty.

The list of files returned is based on the best available type expected.  
Currently the system does not support trying a less good type if the best
is not available.

If $matchstation is set to true then the the request must match a listed
station for the data centre.  Otherwise if the data center supports "all stations"
(ie wildcard station code *), then it may return a value even if the station
code is not matched explicitly.

If $subtype is defined then only that subtype will be checked.

Returns $when=0 if the request cannot be filled from this list.
Return $files undefined when the request cannot be filled now

=cut

sub checkRequest 
{
    my( $self, $request, $matchstation, $subtype ) = @_;
    $request = LINZ::GNSS::DataRequest::Parse($request) if ! ref $request;
    return 0,undef if 
        $request->use_station && 
        ! ( ($self->{allstations} && ! $matchstation
              || exists $self->{stncodes}->{uc($request->station)}));

    return 0,undef if
        $request->use_station  
        && exists $self->{excludestations}->{uc($request->station)};

    return $self->filetypes->checkRequest($request,undef,$subtype);
}


#=head2 $center->filesAvailable($request, $now)
#
#Checks what files the data center should be able to fill the request currently.
#
#Will only return a list if all should be available. 
#Files selected are from highest priority type that should be available.
#
#Can use the parameter $now to specify at what time the list is valid (mainly 
#for testing).
#
#=cut
#
#sub filesAvailable 
#{
#    my( $self, $request, $now ) = @_;
#    return undef if 
#        $request->use_station && 
#        ! ($self->{allstations} || exists $self->{stncodes}->{uc($request->station)});
#    return $self->filetypes->filesAvailable($request,$self->{stncodes},$now);
#}

=head2 $center->credentials( $default )

Returns the user credentials for the DataCenter.  If default is true
then will return the default user/password if none explicitly defined.
Credentials can be defined either with a user/password or with a 
credentials_file.  

If a credentials_file is specified then it can either be a JSON formatted 
file with file extension .json, or a text formatted file.  The JSON format 
should look like:

{"username": "the-user-name": "password":"the-password"}

The text format file should include lines

   user the-user-name
   password the-password

Only the first such lines will be read.  Lines not matching this are
are ignored.

=cut

sub _readCredentials
{
    my ($self,$credfile)=@_;
    open(my $crdf,$credfile) || 
        croak("Cannot open credentials files $credfile for DataCenter ".$self->name);
    my ($user,$pwd);
    if( $credfile =~ /\.json$/ )
    {
        my $json=join('',<$crdf>);
        eval
        {
            my $userdef=decode_json($json);
            $user=$userdef->{username};
            $pwd=$userdef->{password};
        };
    }
    else
    {
        while( my $line=<$crdf> )
        {
            $user=$1 if ! $user && $line =~ /^\s*user(?:name)?\s+(\S.*?)\s*$/i;
            $pwd=$1 if ! $pwd && $line =~ /^\s*password\s+(\S.*?)\s*$/i;
        }
    }
    croak("Credentials file $credfile doesn't define user") if ! defined($user);
    return $user,$pwd;
}

sub credentials
{
    my($self,$default)=@_;
    if( $self->{user} )
    {
        return $self->{user}, $self->{password};
    }
    if( $default )
    {
        return $LINZ::GNSS::DataCenter::ftp_user,$LINZ::GNSS::DataCenter::ftp_password;
    }
}

=head2 $center->connect

Initiates a connection with the server.  Override in subclass.

=cut

sub connect
{
    my($self) = @_;
    $self->{connected}=1;
}

=head2 $center->disconnect

Terminates a connection with the server.  Override in subclass.

=cut

sub disconnect
{
    my($self) = @_;
    $self->{connected}=0;
}

# Routine to actually get a file from the data centre
# Override in subclasses.

sub getfile
{
    my($self,$path,$file,$target)=@_;    
    croak("getfile not implemented in ".$self->{scheme}." Datacenter ".$self->name."\n");
}

sub cachedFileList
{
    my ($self,$key,$list)=@_;
    $self->{_filelistcache}->{$key} = $list if defined $list;
    return $self->{_filelistcache}->{$key};
}

sub _hasWildcard
{
    my($self,$filename)=@_;
    return $filename =~ /[\?\*]/;
}

sub _fileMatchingWildcard
{
    my($self,$spec,$croak)=@_;
    my $path=$spec->path;
    my $filename=$spec->filename;
    return $filename if ! $self->_hasWildcard($filename);
    my $filelist=$self->getfilelist($spec);
    my $filere='^'.join('',map {$_ eq '*' ? '.*' : $_ eq '?' ? '.' : quotemeta($_)} split(/([\?\*])/,$filename)).'$';
    my @filenames = grep(/$filere/,@$filelist);
    if( scalar(@filenames) != 1 )
    {
        return '' if ! $croak;
        if( ! @filenames )
        {
            croak("File $filename not found on $path in Datacenter ".$self->name."\n");
        }
        else
        {
            croak("File $filename on $path is ambiguous in Datacenter ".$self->name."\n");
        }
    }
    return $filenames[0];
}


sub _getfile
{
    my($self,$spec,$target)=@_;
    my $path=$spec->path;
    my $filename=$self->_fileMatchingWildcard($spec,1);
    return $self->getfile($path,$filename,$target);
}

# Get a list of files available at the specified path, used to handle wildcard requests

sub getfilelist
{
    my($self,$path)=@_;
    croak("getfilelist not implemented in ".$self->{scheme}." Datacenter ".$self->name."\n");
}

# Check to see whether a data center has a file
# Optionally override in subclasses.

sub hasfile
{
    my($self,$spec)=@_;
    croak("Cannot check files in ".$self->{scheme}." Datacenter ".$self->name."\n");
}

# Upload a file to a data center has a file
# Optionally override in subclasses.

sub putfile
{
    my ($self,$source,$spec) = @_;
    croak("Cannot save files to Datacenter ".$self->name."\n");
}


=head2 $status, $when, $files = $center->getData( $request, $target, $subtype )

Retrieves a set of files defined by a LINZ::GNSS::DataRequest.

For each file the script first checks to see if it is already installed, and if so
skips the file.  It then attempts to download the file to a temporary location.
If successful it converts it the the target compression, and finally copies it to the
target location.  This way it should only install complete files in the target location.

The target location can be either another data center object, or the name of a data center,
or the name of a directory.

The subtype can be defined to restrict the search to a specific subtype.

Returns a status and an array ref of downloaded file specs, or already downloaded specs.

Will return one of the following status codes

=over

=item COMPLETED

Request successfully filled - the request status will be updated

=item PENDING

Request not filled but expected to be available

=item DELAYED

Request not filled even though expected to be available at this time.  

=item UNAVAILABLE

Request cannot be filled by this datacenter.
May also be because of an error encountered storing the data.

=back

=cut

sub getData
{
    my($self,$request,$target,$subtype) = @_;

    # If target is not an object then try first a directory, then a named data center
    if( ! ref($target) )
    {
        $target=LINZ::GNSS::DataCenter::LocalDirectory($target) if -d $target;
    }
    if( ! ref($target) )
    {
        my $dtc=LINZ::GNSS::DataCenter::GetCenter($target);
        $target = $dtc if defined($dtc);
    }
    croak("Invalid target $target for DataCenter::getData\n") if ! ref($target) || ! $target->isa('LINZ::GNSS::DataCenter');

    $request = LINZ::GNSS::DataRequest::Parse($request) if ! ref $request;

    $self->_logger->debug("Running getData on ".$self->name." to ".$target->name." for request ".$request->reqid);
    my ($when, $files)=$self->checkRequest($request,0,$subtype);
    my $downloaded=[];

    # If the files are not expected to be available at the moment, 
    # then check that the are at least potentially available, 
    # and if so return PENDING, else UNAVAILABLE

    if( ! $files )
    {
        $self->_logger->debug("Cannot service request from datacenter ".$self->name);
        my $status = $when > 0 ? PENDING : UNAVAILABLE;
        $self->_logger->debug("Returning status $status: available $when: files: ".scalar(@$downloaded));
        $request->setStatus($status,'',$when);
        return $status, $when, $downloaded;
    }

    # Check the request has a destination
    if( ! $target )
    {
        $self->_logger->fatal("Cannot service request with no target datacenter");
        croak "Cannot service request with no target datacenter\n";
    }

    # Ensure we have a download location
    my $scratchdir=$self->{scratchdir};
    if( ! -d $scratchdir )
    {
        my $errval;
        if( ! makepublicpath($scratchdir) )
        {
            $self->_logger->fatal("Cannot make download directory $scratchdir");
            croak "Cannot make download directory $scratchdir\n";
        }
    }

    # Process each file in turn..

    my $connected=$self->{connected};
    my $canconnect=1;
    my $delayed=0;
    my $error=0;
    my $supsubtype='';
    $when=0;

    foreach my $spec (@$files)
    {
        my $id=$self->{fileid}++;
        my $tempfile="$scratchdir/file$id";
        eval
        {
            my $tospec=$target->filetypes->getFilespec($spec,$target->{stncodes});
            if( ! $tospec )
            {
                croak "Datastore ".$target->name." cannot store ".$spec->{type}.'/'.$spec->{subtype}." files\n";
            }
            
            $supsubtype=$spec->{subtype};

            # If already available, then there's nothing to do
            # Just add it to the list of downloaded files (so that it is treated as part
            # of this download)
            if($target->hasfile($tospec))
            {
                push(@$downloaded,$tospec);
                next;
            }

            # If not already connected then connect

            if( ! $self->{connected} )
            {
                $canconnect=0;
                $self->connect();
                $canconnect=1;
            }

            # Define the name of the temporary download file...

            my $targetfile=$tospec->{path};
            $targetfile .= '/' if $targetfile ne '';
            $targetfile .= $tospec->{filename};
            $self->_logger->info("Retrieving file $targetfile");
            $self->_logger->debug("Using scratch location $tempfile");

            # Retrieve the file
            # Use $delayed to count the number that fail to be retrieved, 
            # assumed because the file is delayed.

            eval
            {
                $self->_getfile($spec,$tempfile);
            };
            if( $@ )
            {
                my $message=$@;
                chomp($message);
                $self->_logger->info("Retrieve failed: $message");
                unlink($tempfile) if -e $tempfile;
                my $ft=$self->filetypes->getFilespecType($spec);
                my($available,$retry,$fail)=$ft->availableTime($request);
                my $now=time();
                if( $fail < $now )
                {
                    die "Download failed - maximum delay exceeded for this file\n";
                }
                $delayed=1;
                $retry += $now;
                $when = $retry if $retry > $when;
                last;
            }

            # Change the compression if required
            my $fromcomp=$spec->compression;
            my $tocomp=$tospec->compression;
            if($fromcomp ne $tocomp)
            {
                $self->_logger->debug("Converting compression from $fromcomp to $tocomp");
                LINZ::GNSS::FileCompression::RecompressFile($tempfile,$fromcomp,$tocomp);
            }

            # Move the file to the target directory
            $target->putfile($tempfile,$tospec);
            push(@$downloaded,$tospec);
        };
        if( $@ )
        {
            my $message=$@;
            chomp($message);
            $self->_logger->warn($message);
            $error++;
            last if ! $canconnect;
        }
        # Don't want the temporary file retained..
        unlink($tempfile) if -e $tempfile;
    }

    # Disconnect if wasn't connected before this request
    $self->disconnect() if ! $connected;

    # Update the location of the downloaded files to point to the target base path.
    foreach my $d (@$downloaded)
    {
        $d->basepath($target->{basepath});
    }

    # Update the request status

    my $status = $error ? UNAVAILABLE : $delayed ? DELAYED : COMPLETED;

    if( $status eq COMPLETED )
    {
        $request->setCompleted($supsubtype,"Filled from data center ".$self->name);
    }
    else
    {
        $request->setStatus($status,'',$when);
    }

    $self->_logger->debug("Returning status $status: available $when: files: ".scalar(@$downloaded));
    return $status, $when, $downloaded;
}


1;
