#!/usr/bin/perl
#
# Tests retrying data center requests, reused HTTP connections and the transfer
# statistics, against test/retry/stub_server.py (started by run_retry_test.sh).
#
# Usage: retry_test.pl port
#
# Prints ok/not ok lines and exits non-zero if any test fails.

use strict;
use warnings;
use Log::Log4perl qw(:easy);
use File::Temp qw(tempdir);
use LWP::UserAgent;
use JSON::PP;
use Time::HiRes qw(time);
use LINZ::GNSS::DataCenter;
use LINZ::GNSS::DataCenter::Http;
use LINZ::GNSS::DataCenter::GNSSArchive;
use LINZ::GNSS::FileSpec;
use LINZ::GNSS::FileTypeList;
use LINZ::GNSS::TransferError;

Log::Log4perl->easy_init({level=>$ENV{TEST_LOG_LEVEL} ? $DEBUG : $ERROR, layout=>'# %p %m%n'});

my $port=shift(@ARGV) || die "Usage: retry_test.pl port\n";
my $base="http://127.0.0.1:$port";
my $tmpdir=tempdir(CLEANUP=>1);
my $nfail=0;
my $ntest=0;

sub ok
{
    my($ok,$name)=@_;
    $ntest++;
    $nfail++ if ! $ok;
    print(($ok ? "ok" : "not ok")." $ntest - $name\n");
    return $ok;
}

sub readfile
{
    my($file)=@_;
    open(my $f,'<',$file) || return;
    local $/;
    my $content=<$f>;
    close($f);
    return $content;
}

sub serverstats
{
    my $response=LWP::UserAgent->new->get("$base/_stats");
    return decode_json($response->content);
}

sub code { my($error)=@_; return ref($error) && $error->can('code') ? ($error->code // 0) : 0; }

sub requests { my($path)=@_; return (serverstats()->{$path} || {})->{count} // 0; }

# Data centers need a default file type list (empty is enough here)
LINZ::GNSS::FileTypeList::LoadDefaultTypes({datatypes=>{}});

sub newcenter
{
    my($name,%cfg)=@_;
    return LINZ::GNSS::DataCenter::Http->new({
        name=>$name, uri=>$base, retrydelay=>0.2, maxretrydelay=>5, maxretrywait=>30, timeout=>1, %cfg });
}

# Get a file with _getfile, as getData does.  Returns the error if it fails.

sub getfile
{
    my($center,$path,$file)=@_;
    my $target="$tmpdir/$file";
    unlink($target);
    my $spec=LINZ::GNSS::FileSpec->new(path=>$path,filename=>$file);
    eval { $center->_getfile($spec,$target); };
    return ($@, readfile($target));
}

my $center=newcenter('stub');
my ($error,$content,$start,$elapsed);

($error,$content)=getfile($center,'/data/ok','a.dat');
ok(! $error && ($content // '') eq 'content of /data/ok/a.dat',"ok: file retrieved");

($error)=getfile($center,'/data/missing','m.dat');
ok($error && requests('/data/missing/m.dat') == 1,"missing: fails without retrying");
ok(! LINZ::GNSS::TransferError::IsRetryable($error) && code($error) == 404,"missing: error is not retryable, code 404");

($error)=getfile($center,'/data/forbidden','f.dat');
ok($error && requests('/data/forbidden/f.dat') == 1,"forbidden: fails without retrying");

$start=time();
($error,$content)=getfile($center,'/data/throttle1','t.dat');
$elapsed=time()-$start;
ok(! $error && ($content // '') eq 'content of /data/throttle1/t.dat',"throttle1: retrieved after a 429");
ok($elapsed >= 1 && $elapsed < 3,sprintf("throttle1: waited for Retry-After 1 (%.1f s)",$elapsed));

($error,$content)=getfile($center,'/data/unavailable2','u.dat');
ok(! $error && requests('/data/unavailable2/u.dat') == 3,"unavailable2: retrieved on the third try");

$start=time();
($error)=getfile($center,'/data/always503','x.dat');
$elapsed=time()-$start;
ok($error && requests('/data/always503/x.dat') == 4,"always503: fails after 3 retries (".requests('/data/always503/x.dat')." requests)");
ok(code($error) == 503,"always503: error code 503");
ok($elapsed >= 1.4 && $elapsed < 3,sprintf("always503: backed off 0.2+0.4+0.8 s plus jitter (%.1f s)",$elapsed));

($error)=getfile($center,'/data/longwait','l.dat');
ok($error && requests('/data/longwait/l.dat') == 1,"longwait: Retry-After longer than MaxRetryDelay is not waited for");

($error,$content)=getfile($center,'/data/slowonce','s.dat');
ok(! $error && requests('/data/slowonce/s.dat') == 2,"slowonce: retried after a timeout");

my $stats=LINZ::GNSS::DataCenter::TransferStats()->{stub};
ok($stats->{requests} == 15,"stats: requests ($stats->{requests})");
ok($stats->{succeeded} == 4,"stats: succeeded ($stats->{succeeded})");
ok($stats->{unavailable} == 2,"stats: unavailable ($stats->{unavailable})");
ok($stats->{failed} == 2,"stats: failed ($stats->{failed})");
ok($stats->{retries} == 7,"stats: retries ($stats->{retries})");
ok($stats->{throttled} == 8,"stats: throttled ($stats->{throttled})");
my ($summary)=grep { /^Data center stub:/ } LINZ::GNSS::DataCenter::TransferSummary();
ok(defined $summary && $summary =~ /15 requests: 4 succeeded, 2 unavailable, 2 failed; 7 retries, 8 throttled, waited \d+ s/,
    "stats: summary line (".($summary // '').")");

# Retry settings

my $noretry=newcenter('noretry',maxretries=>0);
getfile($noretry,'/data/always503','n.dat');
ok(requests('/data/always503/n.dat') == 1,"MaxRetries 0: not retried");

my $limited=newcenter('limited',maxretrywait=>0.3);
getfile($limited,'/data/always503','w1.dat');
getfile($limited,'/data/always503','w2.dat');
ok(requests('/data/always503/w1.dat') == 2,"MaxRetryWait: first file retried until the limit (".requests('/data/always503/w1.dat')." requests)");
ok(requests('/data/always503/w2.dat') == 1,"MaxRetryWait: later file not retried (".requests('/data/always503/w2.dat')." requests)");

my $nodelay=newcenter('nodelay',retrydelay=>0,maxretrydelay=>0,maxretrywait=>0);
getfile($nodelay,'/data/unavailable2','z.dat');
ok(requests('/data/unavailable2/z.dat') == 3,"RetryDelay 0: retried without waiting (".requests('/data/unavailable2/z.dat')." requests)");

my $invalid=eval { newcenter('invalid',maxretries=>'lots'); 1 };
ok(! $invalid,"invalid MaxRetries is rejected");

# File listings are retried

my $lister=newcenter('lister',filelisturi=>"$base/list[path]",filelistregex=>'(f\d)');
my $list=eval { $lister->cachedFileList(LINZ::GNSS::FileSpec->new(path=>'/d1',filename=>'f1')) } || [];
ok(join(',',sort @$list) eq 'f1,f2,f3',"listing: retried after a 503 (".join(',',sort @$list).")");

# Connections are reused, and a forked process uses its own

my $reuse=newcenter('reuse');
getfile($reuse,'/data/ok','r1.dat');
getfile($reuse,'/data/ok','r2.dat');
my $pid=fork();
if( $pid == 0 )
{
    my ($childerror)=getfile($reuse,'/data/ok','r3.dat');
    exit($childerror ? 1 : 0);
}
waitpid($pid,0);
ok($? == 0,"fork: forked process retrieved a file");

# A forked process starts with its own statistics and retry waiting limit
getfile($limited,'/data/always503','w3.dat');
$pid=fork();
if( $pid == 0 )
{
    my $before=LINZ::GNSS::DataCenter::TransferStats();
    getfile($limited,'/data/always503','w4.dat');
    my $after=LINZ::GNSS::DataCenter::TransferStats();
    exit(1) if %$before;
    exit(2) if $after->{limited}->{requests} != 2;
    exit(0);
}
waitpid($pid,0);
ok($? == 0,"fork: forked process has its own statistics (exit ".($? >> 8).")");
ok(requests('/data/always503/w4.dat') == 2,"fork: forked process has its own MaxRetryWait (".requests('/data/always503/w4.dat')." requests)");
getfile($reuse,'/data/ok','r4.dat');
my $s=serverstats();
my ($p1,$p2,$p3,$p4)=map { $s->{"/data/ok/r$_.dat"}->{ports}->[0] } 1 .. 4;
ok($p1 == $p2 && $p2 == $p4,"keep-alive: parent requests use one connection ($p1 $p2 $p4)");
ok($p3 != $p1,"keep-alive: forked process uses its own connection ($p3)");

# GNSS Archive: auth throttled then retried, cookies sent, credentials not sent with files

my $archive=LINZ::GNSS::DataCenter::GNSSArchive->new({
    name=>'archive', uri=>$base, authkeyuri=>"$base/auth", filelisturi=>"$base/list[path]",
    user=>'client', password=>'secret', retrydelay=>0.2, maxretrydelay=>5, timeout=>1 });
eval { $archive->_withRetry("connection",sub { $archive->connect() },count=>0,reconnect=>0); };
ok(! $@ && requests('/auth') == 2,"archive: auth retried after a 429 (".requests('/auth')." requests)");
($error,$content)=getfile($archive,'/archive','g1.dat');
ok(! $error && ($content // '') eq 'content of /archive/g1.dat',"archive: file retrieved with the auth cookie");
ok(serverstats()->{'/auth'}->{authorization},"archive: credentials sent to AuthKeyUri");
ok(! serverstats()->{'/archive/g1.dat'}->{authorization},"archive: credentials not sent with file requests");

# A temporary failure of the main URI is retried even if the alternative URI says not found

my $altarchive=LINZ::GNSS::DataCenter::GNSSArchive->new({
    name=>'altarchive', uri=>$base, alternativeuri=>"$base/nowhere", authkeyuri=>"$base/auth",
    filelisturi=>"$base/list[path]", user=>'client', password=>'secret',
    retrydelay=>0.2, maxretrydelay=>5, timeout=>1 });
$altarchive->connect();
($error,$content)=getfile($altarchive,'/archive503','a1.dat');
ok(! $error && requests('/archive503/a1.dat') == 2,"archive: retried after a 503 when the alternative URI gives 404 (".requests('/archive503/a1.dat')." requests)");

print "1..$ntest\n";
print $nfail ? "# $nfail of $ntest tests failed\n" : "# All $ntest tests passed\n";
exit($nfail ? 1 : 0);
