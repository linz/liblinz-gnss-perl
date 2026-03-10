#!/bin/sh

# Local configuration

outputdir=output
filedir=files
logfile=$outputdir/test_get_gnss_data.log
script=../bin/get_gnss_data
cachedir=test_cache

export LINZGNSS_DEBUG=1
export LINZGNSS_CONFIG_FILE=./getdata.conf
export LINZGNSS_CACHE_DIR=$cachedir
export PERL5LIB=../lib:$PERL5LIB
export P=$(realpath "$filedir-campaign")
export D=$(realpath "$filedir-datapool")

rm -rf $outputdir/*
rm -rf $filedir/*
rm -rf $cachedir
rm -rf $cachedir/*
rm -rf $D/*
rm -rf $P/*
mkdir -p $outputdir
mkdir -p $filedir
mkdir -p $cachedir
mkdir -p $cachedir
mkdir -p $D
mkdir -p $P

if [ ! -e $HOME/.gnss-archive-credentials ]; then
    echo "Need GNSS Archive credentials in $HOME/.gnss-archive-credentials file"
    exit
fi

test=$1

if [ -z "$test" ]; then
$script -l > $outputdir/datatypes.log 2>&1
$script -s > $outputdir/sources.log 2>&1
fi

######################################################
if [ -z "$test" -o "$test" = "http" ]; then
echo "Getting unauthorised HTTP"
$script -n GGOS -d $filedir 2018:100 VMF  > $outputdir/http_ggos_2.log 2>&1
cksum $filedir/* >> $outputdir/http_ggos_2.log
rm -f $filedir/*
rm -rf $P/*
rm -rf $D/*
fi

######################################################
if [ -z "$test" -o "$test" = "httpa" ]; then
echo "Getting HTTP file (basic authentication)"
$script -n SOPAC -d $filedir 2018:100 OBS MCM4 > $outputdir/http_sopac.log 2>&1
cksum $filedir/* >> $outputdir/http_sopac.log
rm -f $filedir/*
rm -rf $P/*
rm -rf $D/*
fi

######################################################
if [ -z "$test" -o "$test" = "gnssa" ]; then
echo "Getting Public GNSSArchive file"
$script -n LINZ-GNSSArchive -d $filedir 2018:200 OBS AUCK > $outputdir/gnssa_linz.log 2>&1
cksum $filedir/* >> $outputdir/gnssa_linz.log
rm -f $filedir/*
rm -rf $P/*
rm -rf $D/*
fi

######################################################
if [ -z "$test" -o "$test" = "gnssp" ]; then
echo "Getting Private GNSSArchive file"
$script -n LINZ-GNSSArchive -d $filedir 2018:200 OBS GSAL > $outputdir/gnssp_linz.log 2>&1
cksum $filedir/* >> $outputdir/gnssp_linz.log
rm -f $filedir/*
rm -rf $P/*
rm -rf $D/*
fi

######################################################
if [ -z "$test" -o "$test" = "cache" ]; then
echo "Getting file to cache"
$script -c 2018:200 OBS AUCK > $outputdir/cachefile.log 2>&1
ls $outputdir >> $filedir/cachefile.log
echo "Getting file from cache"
$script -d $filedir 2018:200 OBS AUCK > $outputdir/get_cache_file.log 2>&1
cksum $filedir/* >> $outputdir/get_cache_file.log
rm -f $filedir/*
rm -rf $P/*
rm -rf $D/*
fi

######################################################
if [ -z "$test" -o "$test" = "buffer" ]; then
echo "Getting unbuffered ION file"
$script -n CODE -d $filedir 2022:200 ION:FINAL > $outputdir/buffer.log 2>&1
ls $outputdir >> $filedir/buffer.log
echo "==================================================" >> $outputdir/buffer.log
echo "Getting buffered ION file"
$script -n CODE -d $filedir 2022:200 ION:BUFFER >> $outputdir/buffer.log 2>&1
cksum $filedir/* >> $outputdir/buffer.log
rm -f $filedir/*
rm -rf $P/*
rm -rf $D/*
fi

######################################################
# Remove date reference from log files
perl -pi -e 's~^20\d{2}/\d{2}/\d{2}\s\d{2}\:\d{2}\:\d{2}~2000/01/01 00:00:00~' $outputdir/*.log
perl -pi -e 's~/tmp/gnss\w+/~/tmp/gnss/~' $outputdir/*.log
sed -i '/Warning: The LINZ::BERN package is not installed/d' $outputdir/*.log

echo =======================================
echo Differences

diff $outputdir check
