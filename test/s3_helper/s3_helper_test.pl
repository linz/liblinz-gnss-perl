#!/usr/bin/perl
#
# Tests LINZ::GNSS::AwsS3Bucket with the python helper and with the aws client, against the
# S3 endpoint in AWS_ENDPOINT_URL (run_s3_helper_test.sh starts LocalStack for this).
#
# Usage: s3_helper_test.pl bucket [client ...]
#
# client is auto, python or aws.  Each is tested in turn (default: python aws).  Prints ok/not ok
# lines and exits non-zero if any test fails.

use strict;
use warnings;
use Log::Log4perl qw(:easy);
use File::Temp qw(tempdir);
use File::Which;
use LINZ::GNSS::AwsS3Bucket;
use LINZ::GNSS::DataCenter;
use LINZ::GNSS::DataCenter::S3Bucket;
use LINZ::GNSS::DataCenter::Http;
use LINZ::GNSS::FileTypeList;

Log::Log4perl->easy_init({level=>$ENV{TEST_LOG_LEVEL} ? $DEBUG : $WARN, layout=>'# %p %m%n'});

my $bucketname=shift(@ARGV) || die "Usage: s3_helper_test.pl bucket [client ...]\n";
my @clients=@ARGV ? @ARGV : ('python','aws');
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

sub writefile
{
    my($file,$content,$mtime)=@_;
    open(my $f,'>',$file) || die "Cannot create $file\n";
    binmode($f);
    print $f $content;
    close($f);
    utime($mtime,$mtime,$file) if $mtime;
}

sub readfile
{
    my($file)=@_;
    open(my $f,'<',$file) || return;
    binmode($f);
    local $/;
    my $content=<$f>;
    close($f);
    return $content;
}

sub newbucket
{
    my($client,$prefix)=@_;
    return LINZ::GNSS::AwsS3Bucket->new(bucket=>$bucketname,prefix=>$prefix,client=>$client);
}

my $content=join('',map { chr($_ % 256) } 0 .. 300000);
my $mtime=1700000000;
my $source="$tmpdir/source.dat";
writefile($source,$content,$mtime);

foreach my $client (@clients)
{
    my $prefix="test-$client-$$/";
    my $bucket=newbucket($client,$prefix);
    my $expected=$client eq 'auto' ? ($ENV{TEST_AUTO_CLIENT} || 'python') : $client;
    ok($bucket->client eq $expected,"$client: uses the $expected client (".$bucket->client.")");

    ok(! $bucket->fileExists('dir/file.dat'),"$client: file does not exist before upload");
    my $put=$bucket->putFile($source,'dir/file.dat');
    ok($put,"$client: putFile");
    ok($bucket->fileExists('dir/file.dat'),"$client: fileExists after upload");
    my $stats=$bucket->fileStats('dir/file.dat') || {};
    ok(($stats->{size} // -1) == length($content),"$client: fileStats size");
    ok(($stats->{mtime} // -1) == $mtime,"$client: fileStats mtime from metadata");

    $bucket->putFile($source,'dir/other.dat');
    $bucket->putFile($source,'dir/sub/deeper.dat');
    my $files=$bucket->listFiles('dir');
    ok(join(',',sort @$files) eq 'file.dat,other.dat',"$client: listFiles (".join(',',sort @$files).")");
    ok(scalar(@{$bucket->listFiles('nodir')}) == 0,"$client: listFiles of empty directory");

    my $target="$tmpdir/$client-target.dat";
    writefile($target,'old');
    ok(scalar($bucket->getFile('dir/file.dat',$target)),"$client: getFile");
    ok((readfile($target) // '') eq $content,"$client: retrieved content matches");
    ok((stat($target))[9] == $mtime,"$client: retrieved file mtime");

    my $missing=eval { $bucket->getFile('dir/missing.dat',"$tmpdir/missing.dat"); 1 };
    my $error=$@ || '';
    ok(! $missing && $error =~ /not available/,"$client: getFile of missing file croaks not available");
    ok(! -e "$tmpdir/missing.dat","$client: no file left for a missing file");

    # Files written by one client can be read by the other
    foreach my $other (@clients)
    {
        next if $other eq $client;
        my $otherbucket=newbucket($other,$prefix);
        my $otarget="$tmpdir/$client-$other.dat";
        $otherbucket->getFile('dir/file.dat',$otarget);
        ok((readfile($otarget) // '') eq $content && (stat($otarget))[9] == $mtime,
            "$client: file uploaded is read by $other client with its mtime");
    }

    foreach my $file ('dir/file.dat','dir/other.dat','dir/sub/deeper.dat')
    {
        $bucket->deleteFile($file);
    }
    ok(! $bucket->fileExists('dir/file.dat'),"$client: file does not exist after delete");
    ok(scalar(@{$bucket->listFiles('dir')}) == 0,"$client: directory empty after delete");
}

# Timing of many existence checks

foreach my $client (@clients)
{
    my $bucket=newbucket($client,"timing-$$/");
    my $n=20;
    my $start=time();
    $bucket->fileExists("file$_.dat") foreach 1 .. $n;
    my $elapsed=time()-$start;
    print "# $client: $n fileExists calls took $elapsed seconds\n";
}

if( grep { $_ eq 'python' || ($_ eq 'auto' && ! $ENV{TEST_AUTO_CLIENT}) } @clients )
{
    my $prefix="test-fork-$$/";
    my $bucket=newbucket('python',$prefix);
    $bucket->putFile($source,'file.dat');

    # Relative paths are relative to the current directory, not the one the helper started in

    {
        mkdir("$tmpdir/cwd");
        chdir("$tmpdir/cwd") || die "Cannot change to $tmpdir/cwd\n";
        writefile('relative.dat',$content,$mtime);
        $bucket->putFile('relative.dat','relative.dat');
        $bucket->getFile('relative.dat','relative-copy.dat');
        ok((readfile("$tmpdir/cwd/relative-copy.dat") // '') eq $content,
            "cwd: relative paths used after a change of directory");
        $bucket->deleteFile('relative.dat');
        chdir('/');
    }

    # Forked processes start their own helpers, and leave the parent's running

    my @pids;
    foreach my $i (1 .. 4)
    {
        my $pid=fork();
        die "fork failed\n" if ! defined $pid;
        if( $pid == 0 )
        {
            my $ok=1;
            foreach my $j (1 .. 5)
            {
                my $target="$tmpdir/fork$i-$j.dat";
                $bucket->getFile('file.dat',$target);
                $ok=0 if (readfile($target) // '') ne $content;
            }
            exit($ok ? 0 : 1);
        }
        push(@pids,$pid);
    }
    my $childok=1;
    foreach my $pid (@pids)
    {
        waitpid($pid,0);
        $childok=0 if $? != 0;
    }
    ok($childok,"fork: 4 forked processes each retrieved the file 5 times");
    my $nhelpers=scalar(keys %LINZ::GNSS::AwsS3Bucket::helpers);
    ok($nhelpers == 1,"fork: parent still has only its own helper ($nhelpers)");
    ok($bucket->fileExists('file.dat') && $bucket->client eq 'python',"fork: parent helper still works after children exit");

    # A helper that dies is restarted

    my ($helper)=values %LINZ::GNSS::AwsS3Bucket::helpers;
    my $oldpid=($helper->{harness}->{KIDS}->[0] || {})->{PID};
    kill('KILL',$oldpid) if $oldpid;
    select(undef,undef,undef,0.5);
    ok($bucket->fileExists('file.dat'),"restart: request succeeds after the helper is killed");
    ok($bucket->client eq 'python',"restart: still using the python helper");
    my ($newhelper)=values %LINZ::GNSS::AwsS3Bucket::helpers;
    my $newpid=($newhelper->{harness}->{KIDS}->[0] || {})->{PID} || 0;
    ok($newpid && $oldpid && $newpid != $oldpid,"restart: a new helper process was started");

    # The exit status of a process is not changed by stopping its helper

    my $pid=fork();
    if( $pid == 0 )
    {
        my $child=newbucket('python',$prefix);
        $child->fileExists('file.dat');
        exit(3);
    }
    waitpid($pid,0);
    ok(($? >> 8) == 3,"exit: exit status kept when the helper is stopped (".($? >> 8).")");
    $bucket->deleteFile('file.dat');
}

# Once the helper has failed (restarted and failed again), other buckets with the same
# settings use the aws client without trying it again

if( grep { $_ eq 'python' } @clients )
{
    local %LINZ::GNSS::AwsS3Bucket::helpers=();
    local %LINZ::GNSS::AwsS3Bucket::helper_failed=();
    # A python that starts the helper but closes its output after the ready line, so that
    # every request fails, including after the helper is restarted.  It counts its starts.
    my $python=which('python3');
    my $wrapper="$tmpdir/failing-python";
    my $starts="$tmpdir/failing-python.starts";
    writefile($wrapper,"#!/bin/sh\necho start >> \"$starts\"\n\"$python\" \"\$@\" | head -n 1\n");
    chmod(0755,$wrapper);
    my $nstarts=sub { my $c=readfile($starts) // ''; return scalar(() = $c =~ /start/g); };
    local $ENV{LINZGNSS_S3_PYTHON}=$wrapper;
    my $bucket=newbucket('auto','failing/');
    ok($bucket->client eq 'aws',"failed helper: bucket falls back to the aws client");
    ok($nstarts->() == 2,"failed helper: helper started and restarted once (".$nstarts->()." starts)");
    ok(! $bucket->fileExists('nothing.dat'),"failed helper: aws client requests work");
    ok(join('',values %LINZ::GNSS::AwsS3Bucket::helper_failed) eq 'helper failed twice',
        "failed helper: failure is remembered for the connection settings");
    my $other=newbucket('auto','failing2/');
    ok($other->client eq 'aws',"failed helper: other bucket with the same settings uses the aws client");
    ok($nstarts->() == 2,"failed helper: no helper started for the other bucket (".$nstarts->()." starts)");
}

# Falling back to the aws client if the helper cannot start

{
    local $ENV{LINZGNSS_S3_PYTHON}='/nonexistent/python3';
    local %LINZ::GNSS::AwsS3Bucket::helpers=();
    local %LINZ::GNSS::AwsS3Bucket::helper_failed=();
    my $bucket=newbucket('auto','fallback/');
    ok($bucket->client eq 'aws',"fallback: auto uses the aws client when the helper cannot start");
    ok(! $bucket->fileExists('nothing.dat'),"fallback: aws client requests work");
    my $required=eval { newbucket('python','fallback/'); 1 };
    ok(! $required,"fallback: python client fails when the helper cannot start");
    my $unsupported=LINZ::GNSS::AwsS3Bucket->new(bucket=>$bucketname,client=>'auto',
        aws_parameters=>'--no-verify-ssl');
    ok($unsupported->client eq 'aws',"fallback: unsupported aws parameters use the aws client");
}

# Maximum downloads per connection

{
    # Data centers need a default file type list (empty is enough here)
    LINZ::GNSS::FileTypeList::LoadDefaultTypes({datatypes=>{}});
    my $s3=LINZ::GNSS::DataCenter::S3Bucket->new({name=>'test-s3',uri=>"s3://$bucketname/x"});
    ok($s3->{maxdownloads} == 0,"maxdownloads: S3Bucket default is 0 (no limit)");
    $s3=LINZ::GNSS::DataCenter::S3Bucket->new({name=>'test-s3b',uri=>"s3://$bucketname/x",maxdownloads=>7});
    ok($s3->{maxdownloads} == 7,"maxdownloads: S3Bucket MaxDownloads 7");
    my $http=LINZ::GNSS::DataCenter::Http->new({name=>'test-http',uri=>"https://example.com/x"});
    ok($http->{maxdownloads} == 50,"maxdownloads: Http default is 50");
    $http=LINZ::GNSS::DataCenter::Http->new({name=>'test-http0',uri=>"https://example.com/x",maxdownloads=>0});
    ok($http->{maxdownloads} == 0,"maxdownloads: Http MaxDownloads 0 (no limit)");
}

print "1..$ntest\n";
print $nfail ? "# $nfail of $ntest tests failed\n" : "# All $ntest tests passed\n";
exit($nfail ? 1 : 0);
