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
    teqc +qc -s -l $outfile 2>/dev/null | grep -P '(Time of|observations|ID| type)' > $outdir/obs_summary.txt
 
    outdir=$outputdir/crx/$f
    mkdir -p $outdir
    outfile=$outdir/dummy/gsht1500.20d
    $script $verbose $datadir/$f $outfile > $outdir/make_rinex2.log 2>&1
    crx2rnx < $outfile > $outdir/rnx
    teqc +qc -s -l $outdir/rnx 2>/dev/null | grep -P '(Time of|observations|ID| type)' > $outdir/obs_summary.txt
    rm $outdir/rnx

    outdir=$outputdir/crxgz/$f
    mkdir -p $outdir
    outfile=$outdir/dummy/gsht1500.20d.gz
    $script $verbose $datadir/$f $outfile > $outdir/make_rinex2.log 2>&1
    gzip -d -c < $outfile | crx2rnx > $outdir/rnx
    teqc +qc -s -l $outdir/rnx 2>/dev/null | grep -P '(Time of|observations|ID| type)' > $outdir/obs_summary.txt
    rm $outdir/rnx
 
    outdir=$outputdir/rename/$f
    mkdir -p $outdir
    outfile=$outdir/dummy/qqq11500.20d
    $script $verbose -r AAAA:BBBB+GSHT:QQQ1+CCCC:DDDD $datadir/$f $outfile > $outdir/make_rinex2.log 2>&1
    crx2rnx < $outfile > $outdir/rnx
    teqc +qc -s -l $outdir/rnx 2>/dev/null | grep -P '(Time of|observations|ID| type)' > $outdir/obs_summary.txt
    rm $outdir/rnx

done

f=gshx1500.20o 
outdir=$outputdir/rename/subcode
mkdir -p $outdir
outfile=$outdir/dummy/{code}1500.20d
outfile2=$outdir/dummy/qqq11500.20d
$script -p -r AAAA:BBBB+GSHT:QQQ1+CCCC:DDDD $datadir/$f $outfile > $outdir/make_rinex2.log 2>&1
crx2rnx < $outfile2 > $outdir/rnx
teqc +qc -s -l $outdir/rnx 2>/dev/null | grep -P '(Time of|observations|ID| type)' >> $outdir/obs_summary.txt
rm $outdir/rnx

rm -rf out/*/*/dummy
for f in `find out -name make_rinex2.log`; do
    perl clean.pl $f
done
if diff -q -r out/ check/; then
    echo "All checks passed :-)"
fi

