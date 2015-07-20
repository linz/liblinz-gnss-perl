use strict;

=head1 LINZ::GNSS::FileCache

LINZ::GNSS::FileCache manages a cache of GNSS data files.  

The files are stored in a directory structure, at the root of which is a 
database file cache.db.  This maintains a list of files in the cache, as well
as pending data requests.  Each request is associated with a job.

Files that are cached are stored with a retention period, after which they 
may be purged.  The cache file locations, retention policies etc are all 
stored in a file type list of the data center

The cache itself retrieves files from the prioritized source data centres.

=cut

package LINZ::GNSS::FileCache;
use fields qw(
    dbh 
    dbfile
    basepath
    datacenter
    jobretention
    queuelatency
    _logger
    );

use DBI;
use Carp;
use LINZ::GNSS::FileTypeList;
use LINZ::GNSS::DataRequest qw(REQUESTED COMPLETED UNAVAILABLE PENDING DELAYED INVALID);
use LINZ::GNSS::Time qw($SECS_PER_DAY);
use LINZ::GNSS::DataCenter;


use File::Path qw( make_path remove_tree );
use Log::Log4perl;

our $cache;
our $DefaultJobRetention=28;
our $DefaultQueueLatency=1800;

=head2 LINZ::GNSS::FileCache->new($location, $filetypes)

Creates the cache in the specified location and supporting the types in the filetypes
list.

=cut

sub new
{
    my($self,$datacenter_name) = @_;
    $self=fields::new($self) unless ref $self;
    my $datacenter = LINZ::GNSS::DataCenter::GetCenter($datacenter_name);
    croak "Invalid datacenter $datacenter_name for FileCache\n" if ! $datacenter;
    croak "FileCache datacenter $datacenter_name invalid as is not a file based center\n"
        if $datacenter->scheme ne 'file';
    my $basepath=$datacenter->basepath;
    if( ! -d $basepath )
    {
        my $errval;
        my $umask=umask 0000;
        make_path($basepath,{error=>\$errval});
        umask $umask;
        croak "Cannot create LINZ::GNSS::FileCache cache directory at $basepath\n" if @$errval;
    }
    my $dbfile=$basepath.'/cache.db';
    my $dbh = DBI->connect("dbi:SQLite:dbname=$dbfile","","",
        {sqlite_use_immediate_transaction=>1} )
       || croak "Cannot create LINZ::GNSS::FileCache cache database $dbfile\n".
            DBI->errstr."\n";
    chmod 0664, $dbfile;
    $self->{dbh} = $dbh;
    $self->{dbfile} = $dbfile;
    $self->{basepath} = $basepath;
    $self->{datacenter} = $datacenter;
    $self->{jobretention} = $DefaultJobRetention;
    $self->{queuelatency} = $DefaultQueueLatency;
    $self->{_logger}=Log::Log4perl->get_logger('LINZ.GNSS.FileCache');
    $self->_logger->debug("Created FileCache for datacenter $datacenter_name using $dbfile");
    $self->_setupTables();
    return $self;
}

sub dbfile { return $_[0]->{dbfile}; }
sub datacenter { return $_[0]->{datacenter}; }
sub _logger { return $_[0]->{_logger}; }

=head2 LINZ::GNSS::FileCache::LoadCache($cfg)

Sets up the file cache using the information in a configuration variable, which 
names the file based datacenter that is to be used by the cache. The cache 
database is stored in the base directory of that datacenter

   <cache>
      datacenter name
      job_retentation ndays
      queue_latency nseconds
    </cache>

=cut

sub LoadCache
{
    my($cfg) = @_;
    if( $cfg && ! $LINZ::GNSS::FileCache::cache )
    {
        my $cfgc=$cfg->{cache} || croak "<cache> not defined in configuration\n";
        my $datacenter_name=$cfgc->{datacenter};

        my $cache=LINZ::GNSS::FileCache->new($datacenter_name);
        $cache->{jobretention} = $cfgc->{job_retention} if exists $cfgc->{job_retention};
        $cache->{queuelatency} = $cfgc->{queue_latency} if exists $cfgc->{queue_latency};
        $LINZ::GNSS::FileCache::cache=$cache;
    }
    return $LINZ::GNSS::FileCache::cache;
}

=head2 LINZ::GNSS::Cache()

Returns the default cache (only defined if LoadCache has been called first).

=cut

sub Cache()
{
    my $cache = $LINZ::GNSS::FileCache::cache;
    croak "LINZ::GNSS::FileCache::Cache() is not defined - use LoadCache first\n"
        if ! $cache;
    return $cache;
}

sub _setupTables
{
    my($self) = @_;
    my $dbh=$self->{dbh};
    $dbh->do(<<EOD);
    CREATE TABLE IF NOT EXISTS files
    (
        id INTEGER PRIMARY KEY,
        type CHAR(10),
        subtype CHAR(10),
        filename TEXT,
        expiry DATETIME
    )
EOD
    $dbh->do(<<EOD);
    CREATE TABLE IF NOT EXISTS jobs
    (
        id VARCHAR(20) PRIMARY KEY,
        createddate DATETIME,
        expirydate DATETIME
    )
EOD
    $dbh->do(<<EOD);
    CREATE TABLE IF NOT EXISTS requests
    (
        id INTEGER PRIMARY KEY,
        reqid VARCHAR(50),
        jobid VARCHAR(20),
        type VARCHAR(10),
        subtype VARCHAR(10),
        startepoch DATETIME,
        endepoch DATETIME,
        station VARCHAR(4),
        status VARCHAR(4),
        status_message TEXT,
        supplied_subtype VARCHAR(10),
        available_date DATETIME
    )
EOD
    $dbh->do(<<EOD);
    CREATE UNIQUE INDEX IF NOT EXISTS request_reqid ON requests(reqid)
EOD
    $dbh->do(<<EOD);
    CREATE TABLE IF NOT EXISTS file_requests
    (
        request_id INTEGER NOT NULL,
        file_id INTEGER NOT NULL,
        PRIMARY KEY (request_id, file_id)
    )
EOD

}

=head2 $cache->addRequest( $request );

Adds or updates (replaces) a request (LINZ::GNSS::DataRequest) 
to the cache request database.  Requests are assigned a unique id when 
they are added.  Also they have a unique external
id compiled defined by reqid.  If the request matches existing
values of either then it replaces them.

=cut

sub addRequest
{
    my($self,$request) = @_;
    my $reqid=$request->reqid;
    my $dbh=$self->{dbh};
    
    my ($exists) = $self->getRequests( reqid=>$reqid );
    if( $exists )
    {
        $request->{id}=$exists->id;
        $dbh->do('DELETE FROM requests WHERE id=?',{},$exists->id);
    }
    else
    {
        my ($id) = $dbh->selectrow_array('SELECT MAX(id)+1 FROM requests');
        $request->{id}=$id;
    }
    
    # If this is a new request then set the available date and status
    if( $request->status eq REQUESTED )
    {
        my $available_date = LINZ::GNSS::DataCenter::WhenAvailable($request);
        my $status=UNAVAILABLE;
        if( $available_date)
        {
            $status = PENDING;
            my $now = time();
            $available_date=$now if $available_date < $now;
        }

        $request->setStatus($status,'',$available_date);
    }

    my $sql3=<<EOD;
    INSERT INTO requests 
         (id,reqid,jobid,type,subtype,startepoch,endepoch,station,
          status,status_message,supplied_subtype,available_date)
         VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
EOD
    $self->{dbh}->do(
        $sql3,{},
        $request->id,
        $request->reqid,
        $request->jobid,
        $request->type,
        $request->subtype,
        $request->start_epoch,
        $request->end_epoch,
        $request->station,
        $request->status,
        $request->status_message,
        $request->supplied_subtype,
        $request->available_date
        );

    $self->updateJob( $request->jobid );

    return $request;
}

=head2 $cache->addRequests( $requests )

Adds a list of LINZ::GNSS::DataRequest requests to the cache request database.
The list is supplied in an array ref $requests.

=cut

sub addRequests
{
    my( $self, $requests )=@_;
    foreach my $r (@$requests) { $self->addRequest($r); }
}

=head2 $cache->deleteRequest( $request )

Deletes a request from the cache (once it has been retrieved)

=cut

sub deleteRequest
{
    my ($self,$request) = @_;
    $self->{dbh}->do('delete from file_requests where request_id = (select id from requests where reqid=?)',
        {},$request->reqid);
    $self->{dbh}->do('delete from requests where reqid=?',{},$request->reqid);
}

=head2 $cache->fillRequest( $id )

Attempts to fill a job from the prioritized datacenters

=cut

sub fillRequest
{
    my($self,$request)=@_;
    return INVALID if ! $request;
    my $id = $request->id;
    $self->_logger->debug("Filling request $id: ".$request->reqid);

    my $datacenter=$self->datacenter;
    my ($status,$when,$files) = LINZ::GNSS::DataCenter::FillRequest($request,$datacenter);
    $self->_logger->debug("Request $id FillRequest status $status");
    foreach my $dnldfile (@$files)
    {
        my $ftype = $datacenter->filetypes->getType($dnldfile->type,$dnldfile->subtype);
        my $fexpiry = time() + $ftype->retention * $SECS_PER_DAY;
        $self->addFile( $id, $dnldfile, $fexpiry );
    }
    # Update the request...
    $self->addRequest($request);
    return $status;
}

=head2 $cache->fillRequests( %where )

Attempts to fill the requests specified by the conditions %where.  The conditions are the same 
as used in the $cache->getRequests function

=cut

sub fillRequests
{
    my($self,%where) = @_;
    my $requests = $self->getRequests(%where);
    foreach my $request (@$requests)
    {
        $self->fillRequest( $request );
    }
}

=head2 my $jobids = $cache->fillPendingRequests($time)

Attempts to fill the pending requests queued for downloading.
Can take an optional parameter defining the availability date to test - the default
is the current date/time.

Returns a list of jobids of jobs for which all pending requests have now been completed.

=cut

sub fillPendingRequests
{
    my($self,$time) = @_;
    $time ||= time();
    my $requests = $self->getRequests(
        where=>'status in (?,?) and available_date < ?',
        values=>[PENDING,DELAYED,$time]
        );
    my %jobids=();
    foreach my $request (@$requests)
    {
        $self->fillRequest( $request );
        $jobids{$request->jobid}=1 if $request->status eq COMPLETED;
    }

    # For the jobs that have had requests filled, check that they have no
    # more requests pending, and if not then add them to the list of 
    # jobs that are ready to continue.
    my @jobids;
    foreach my $jobid ( sort keys %jobids )
    {
        my $pending=$self->getRequests(
            where=>'status in (?,?) and jobid=?',
            values=>[PENDING,DELAYED,$jobid]
            );
        push(@jobids,$jobid) if ! @$pending;
    }
    return \@jobids;
}

=head2 $cache->retrieveRequest( $target, $request )

Retrieves a request to the target data source (assumed to be the original requester).
Once successfully retrieved, the request is deleted.  Will only retrieve the request
if has status completed.

=cut

sub retrieveRequest 
{
    my($self,$target,$request) = @_;
    $self->_logger->debug("Retrieving request ".$request->reqid." to ".$target->name);

    my ($qrequest) = $self->getRequests( request=>$request );

    # If the request is not in the database, then just try and get it, set unavailable if 
    # it is not already available (as there is no plan to queue or download it)
    if( ! $qrequest )
    {
        my ($status, $when, $downloaded) = $self->datacenter->getData( $request, $target );
        if( $status ne COMPLETED )
        {
            $status = UNAVAILABLE;
            $request->setStatus($status);
        }
        return $status, $downloaded;
    }

    # Otherwise if the request is defined then check its status and download if not available
    $request=$qrequest;
    my $status = $request ? $request->status : INVALID;
    my $downloaded=[];
    my $when=0;
    if( $status eq COMPLETED )
    {
        ($status, $when, $downloaded) = $self->datacenter->getData( $request, $target );
        if( $status ne COMPLETED )
        {
            $self->_logger->warn("Failed to retrieve completed request");
        }
        $self->deleteRequest( $request );
    }
    elsif( $status eq UNAVAILABLE )
    {
        $self->deleteRequest( $request );
    }
    return $status, $downloaded;
}

=head2 $cache->retrieveRequests( $target, %where )

Retrieves a group of requests to the target data source (assumed to be the original requester).
Once successfully retrieved, the requests are deleted.  Will only retrieve the requests
if all have status completed or unavailable (ie nothing more to do)

=cut

sub retrieveRequests
{
    my($self,$target,%where) = @_;
    my $requests = $self->getRequests( %where );
    my $downloaded=[];
    my $status=INVALID;
    my $when=0;
    if(  @$requests )
    {
        my $ready=1;
        foreach my $request (@$requests)
        {
            $status=$request->status;
            if( $status eq PENDING || $status eq DELAYED )
            {
                $ready=0;
                last;
            }
        }
        
        if( $ready )
        {
            foreach my $request (@$requests)
            {
                my($rstatus,$addfiles)=$self->retrieveRequest($target,$request);
                push(@$downloaded,@$addfiles);
                $status=$rstatus if $rstatus eq UNAVAILABLE;
            }
        }
    }
    return $status, $downloaded;
}


=head2 $cache->addFile( $spec, $expiry )

Adds a file to the list of files managed by the cache.  If the file is already there, then
just updates its expiry date to the latest defined.

=cut

sub addFile
{
    my($self,$request_id, $spec, $expiry) = @_;
    my $dbh = $self->{dbh};
    my $filename = $spec->path.'/'.$spec->filename;
    chmod 0664, $filename;
    $self->_logger->debug("Adding file $filename for request $request_id");
    my($id,$oldexpiry) = $dbh->selectrow_array('select id, expiry from files where filename=?',{},$filename);
    if( $id && $oldexpiry < $expiry )
    {
        $dbh->do('update files set expiry=? where id=?',{},$expiry,$id);
    }
    else
    {
        $dbh->do('insert into files (type,subtype,filename,expiry) values (?,?,?,?)',{},
            $spec->type,$spec->subtype,$filename,$expiry);
        ($id) = $dbh->selectrow_array('select last_insert_rowid()');
    }
    $dbh->do('insert or ignore into file_requests (request_id,file_id) values (?,?)',{},
        $request_id,$id);
}

=head2 $cache->purgeFiles($now)

Removes files from the cache for which the retention period has expired.
The parameter $now is for testing - default is the current time.

=cut

sub purgeFiles
{
    my($self,$now) = @_;
    $now ||= time();
    my $basepath = $self->{basepath};
    my $dbh = $self->{dbh};
    my $sql='select id, filename from files where expiry < ? and id not in (select file_id from file_requests)';
    my $sth=$dbh->prepare($sql);
    $sth->execute($now);
    while( my($id,$filename) = $sth->fetchrow_array() )
    {
        unlink($basepath.'/'.$filename);
        $self->_logger->debug("Purging expired file $filename");

    }
    $dbh->do('delete from files where expiry < ?',{},$now);
    $dbh->do('delete from file_requests where file_id not in (select id from files)');
}

=head2 $cache->updateJob( $jobid )

Updates a job to reflect the status of its constituent requests.  
Also resets the expiry time for the job based on the maximum of dates at which requested data
will be available and the current time.

=cut

sub updateJob
{
    my( $self, $jobid ) = @_;
    my $dbh = $self->{dbh};
    my ($exists) = $dbh->selectrow_array('select id from jobs where id=?',{},$jobid);
    my ($expiry) = $dbh->selectrow_array('select max(available_date) from requests where jobid=?',{},$jobid);
    $expiry = time() if ! $expiry || $expiry < time();
    $expiry += $self->{jobretention}*$SECS_PER_DAY;
    if( ! $exists )
    {
        $self->_logger->debug("Creating job $jobid");
        $dbh->do('insert into jobs (id, createddate,expirydate) values (?,?,?)',{},$jobid,time(),$expiry);
    }
    else
    {
        $dbh->do('update jobs set expirydate=? where id=?',{},$expiry,$jobid);
    }
}

=head2 $cache->deleteJob( $jobid )

Remove data for job $jobid from the cache

=cut

sub deleteJob
{
    my($self,$jobid) = @_;
    $self->_logger->debug("Deleting job $jobid");
    my $dbh=$self->{dbh};
    $dbh->do('delete from file_requests where request_id in (select id from requests where jobid=?)',{},$jobid);
    $dbh->do('delete from requests where jobid=?',{},$jobid);
    $dbh->do('delete from jobs where id=?',{},$jobid);
}

=head2 $cache->purgeJobs($now)

Remove expired jobs from the cache. $now is principally for testing - it defaults to the current 
date.  Jobs that have expired by $now are deleted.

=cut

sub purgeJobs
{
    my($self,$now) = @_;
    $now ||= time();
    my $ids = $self->{dbh}->selectcol_arrayref("select id from jobs where expirydate<?",{},$now);
    foreach my $id (@$ids) { $self->deleteJob($id); }
}

=head2 $cache->purge($now)

Remove expired jobs and files

=cut

sub purge
{
    my($self,$now) = @_;
    $now ||= time();
    $self->purgeJobs($now);
    $self->purgeFiles($now);
}

=head2 $cache->getRequests( request=>$r, id=>$id, jobid=>$id, type=>$t, station=>$s, where=>$sql, values=>[values] )

Retrieves a set of data requests from the request database.  Requests are selected 
by the criteria added to the job, which are structured as a hash with keys id
(references the internal database request id), jobid (the external job id), and 
reqid (the external request id for the job).  If the request id ends with a '*' then
it is matched with a SQL LIKE clause, rather than a equality test.

The where= and values= can be used to add arbitrary SQL to the query.  Values is an array
ref of values corresponding to '?' placeholders in where.

The criteria request=$r is treated specially - $r is assumed to be a DataRequest and is 
updated with the data from the database.



=cut

sub getRequests
{
    my ($self,%where) = @_;
    my $requests=[];
    my $sql="select * from requests";
    my @params=();
    my @where=();
    my $prmwhere='';
    my $prmval=[];
    my $srcreq;
    foreach my $k (keys %where)
    {
        my $wc;
        my $v=$where{$k};
        if( $k eq 'id')
        {
            $wc='id=?';
        }
        elsif( $k eq 'reqid')
        {
            $wc='reqid=?';
        }
        elsif( $k eq 'jobid' )
        {
            $wc='jobid=?';
        }
        elsif( $k eq 'request' )
        {
            $wc='reqid=?';
            $srcreq=$v;
            $v=$srcreq->reqid;
        }
        elsif( $k eq 'type' )
        {
            $wc='type=?';
            $v=uc($v);
        }
        elsif( $k eq 'station' )
        {
            $wc='station=?';
            $v=uc($v);
        }
        elsif( $k eq 'where' )
        {
            $prmwhere=$v;
        }
        elsif( $k eq 'values' )
        {
            $prmval= ref($v) ? $v : [$v];
        }
        else
        {
            croak "Invalid condition $k in LINZ::GNSS::FileCache::getRequests\n";
        }
        push(@where,$wc);
        push(@params,$v);
    }
    push(@where,$prmwhere) if $prmwhere;
    push(@params,@$prmval) if $prmwhere;
    $sql .= ' where '.join(' and ',@where ) if @where;
    $self->_logger->debug("getRequests SQL: $sql: ".join(', ',@params));
    my $dbh=$self->{dbh};
    my $sth=$dbh->prepare($sql) || croak "SQL error: ".$dbh->errstr;
    $sth->execute(@params);
    while( my $row=$sth->fetchrow_hashref() )
    {
        my $request = $srcreq || LINZ::GNSS::DataRequest->new(
            $row->{jobid},
            $row->{type},
            $row->{subtype},
            $row->{startepoch},
            $row->{endepoch},
            $row->{station},
        );
        $request->{id}=$row->{id};
        $request->{status}=$row->{status};
        $request->{status_message}=$row->{status_message};
        $request->{available_date}=$row->{available_date};
        $request->{supplied_subtype}=$row->{supplied_subtype};
        push(@$requests,$request);
    }
    $sth->finish;
    return wantarray ? @$requests : $requests;
}

=head2 ($status,$when,$files) $cache->getData($request,$target,%options)

Emulate the LINZ::GNSS::DataCenter getData function (with an additional options).

$request is the data request 

$target is the target datacenter or directory name for the request. If this is not defined then the data will be downloaded (if requested) but not copied to a target.  

%options defines the options for getting the data:

The following options are supported:

=over

=item download=>0/1

If true then the script will attempt to download the data, otherwise it will only attempt to 
fill from the cache.  The default is 1.

=item 1 queue=>0/1

If true and the request cannot be filled from the cache then it will be added to the 
cache download queue.  The data will be queued if the status is PENDING or DELAYED.

=back

Returns the $status, $when, $files. 

$status is the return status, one of COMPLETED, PENDING, DELAYED, UNAVAILABLE.  If it is
PENDING or DELAYED then $when is the suggested time for retrying the request. $files is an
array of FileSpec objects that have been downloaded to the target datacenter.

If the request cannot be completed then the status and retry time depend on the download
options.  If the request doesn't allow queuing or downloading the request, then the job
will return UNAVAILABLE if the files are not already in the data cache.

=cut

sub getData
{
    my($self,$request,$target,%options) = @_;
    $request = LINZ::GNSS::DataRequest::Parse($request) if ! ref $request;
    $target = LINZ::GNSS::DataCenter::LocalDirectory($target) if $target && ! ref ($target);
    my ($lodged) = $self->getRequests(request=>$request);
    my $download= exists($options{download}) ? $options{download} : 1;
    my $queue= exists($options{queue}) ? $options{queue} : 1;
    if( $lodged ) 
    { 
        $request=$lodged; 
    }
    elsif( $queue || $download ) 
    { 
        $self->addRequest($request); 
    }

    # Fill the request if downloading
    my $status = $self->fillRequest($request) if $download;
    my $downloaded=[];

    # Retrieve the request
    if( $target )
    {
        ($status,$downloaded)= $self->retrieveRequest($target,$request);
    }

    # If the request wasn't already queued, and we didn't want to queue it
    # then delete the request.
    if(! $lodged && ! $queue)
    {
        $self->deleteRequest($request);
    }

    # Assume the next request will have the same options, so if queued and
    # not downloading in the request, then add the queue latency to the suggested
    # download time.

    my $when=$request->available_date;
    if( $status eq PENDING || $status eq DELAYED ) 
    {
        $when += $self->{queuelatency} if $when && $queue && ! $download;
    }

    return $status, $when, $downloaded;
}

=head2 $available_time=$cache->whenAvailable( $request )

Returns when a dataset is expected to be available

=cut

sub whenAvailable
{
    my($self,$request) = @_;
    return LINZ::GNSS::DataCenter::WhenAvailable($request);
}

1;
