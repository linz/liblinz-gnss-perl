#!/bin/sh

# Local configuration

outputdir=output
logfile=$outputdir/test_get_gnss_data.log
script=../bin/get_gnss_data
cachedir=test_cache

export LINZGNSS_DEBUG=1
export LINZGNSS_CONFIG_FILE=./getdata.conf
export LINZGNSS_CACHE_DIR=$cachedir
export PERL5LIB=../lib:$PERL5LIB

mkdir -p $outputdir
rm -rf $outputdir/*
rm -rf $cachedir
mkdir -p $cachedir

if [ ! -e linz-gnss-credentials ]; then
    echo "Need GNSS Archive credentials in ./linz-gnss-credentials file"
    exit
fi

$script -l > $outputdir/datatypes.txt
$script -s > $outputdir/sources.txt
# Get an FTP site...
echo "Getting FTP UNAVCO file"
$script -n UNAVCO -d $outputdir 2018:100 OBS ROB4 > $outputdir/ftp_unavco.log 2>&1
echo "Getting unauthorised HTTP"
$script -n GGOS -d $outputdir 2018:100 VMF  > $outputdir/http_ggos_2.log 2>&1
echo "Getting HTTP SOPAC file"
$script -n SOPAC -d $outputdir 2018:100 OBS MCM4 > $outputdir/http_sopac.log 2>&1
echo "Getting GNSSArchive file"
$script -n LINZ-GNSSArchive -d $outputdir 2018:200 OBS GSAL > $outputdir/gnssa_linz.log 2>&1
echo "Getting file to cache"
$script -c 2018:200 OBS AUCK > $outputdir/cachefile.log 2>&1
ls $outputdir >> $outputdir/cachefile.log
echo "Getting file from cache"
$script -d $outputdir 2018:200 OBS AUCK > $outputdir/get_cache_file.log 2>&1
# Form checksum of output files to reduce output size
ls $outputdir/*.18* $outputdir/*.H* | xargs cksum > $outputdir/output_files.txt
rm $outputdir/*.18* $outputdir/*.H*

# Remove date reference from log files
perl -pi -e 's~^20\d{2}/\d{2}/\d{2}\s\d{2}\:\d{2}\:\d{2}~2000/01/01 00:00:00~' $outputdir/*.log
perl -pi -e 's~/tmp/gnss\w+/~/tmp/gnss/~' $outputdir/*.log

echo =======================================
echo Differences

diff $outputdir check
