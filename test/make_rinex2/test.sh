#!/bin/sh
#set -x

# Local configuration

datadir=data
outputdir=out
script=../../bin/make_rinex2
export PERL5LIB=../../lib:$PERL5LIB

rm -rf $outputdir

files='
gsht1500.20d 
gsht1500.20d.gz 
gsht1500.20o 
gsht1500.20o.gz 
gshx1500.20d 
gshx1500.20d.gz 
gshx1500.20o 
gshx1500.20o.gz
'
verbose=-v
#verbose=

for f in $files; do
    echo "Testing $f"

    outdir=$outputdir/rnx/$f
    mkdir -p $outdir
    outfile=$outdir/dummy/gsht1500.20o
    $script $verbose $datadir/$f $outfile > $outdir/make_rinex2.log 2>&1
 
    outdir=$outputdir/crx/$f
    mkdir -p $outdir
    outfile=$outdir/dummy/gsht1500.20d
    $script $verbose $datadir/$f $outfile > $outdir/make_rinex2.log 2>&1

    outdir=$outputdir/crxgz/$f
    mkdir -p $outdir
    outfile=$outdir/dummy/gsht1500.20d.gz
    $script $verbose $datadir/$f $outfile > $outdir/make_rinex2.log 2>&1
 
    outdir=$outputdir/rename/$f
    mkdir -p $outdir
    outfile=$outdir/dummy/qqq11500.20d
    $script $verbose -r AAAA:BBBB+GSHT:QQQ1+CCCC:DDDD $datadir/$f $outfile > $outdir/make_rinex2.log 2>&1

done
