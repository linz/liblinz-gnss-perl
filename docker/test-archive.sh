#!/bin/sh
# Test the connection to the GNSS archive and download a file from it.
#
# Environment:
#   GNSS_ARCHIVE_CREDENTIALS_FILE  credentials file, either JSON with "username" and
#                                  "password" or lines "user ..." and "password ..."
#   ARCHIVE_DATE                   yyyy:ddd of the file to download (default 2018:200)
#   ARCHIVE_STATION                station code (default AUCK)
#   ARCHIVE_SUBTYPE                OBS subtype, DAILY or HOURLY (default DAILY)
#   ARCHIVE_PRIVATE                1, true or yes: run the download again as the private
#                                  check, for ARCHIVE_STATION if supplied, otherwise GSAL.
#                                  Anything else (unset, empty, 0) skips it.
#   OUTDIR                         where downloaded files go (default /out), each in its
#                                  own <station>-<date>-<subtype> directory, replaced on
#                                  each run
#
# Any command line arguments are run as a command instead of the test.

if [ $# -gt 0 ]; then
    exec "$@"
fi

date=${ARCHIVE_DATE:-2018:200}
station=${ARCHIVE_STATION:-AUCK}
# The subtype is requested explicitly. Without one get_gnss_data tries the subtypes in
# priority order (DAILY, then HOURLY), so a failed daily download would fall back to an
# hourly file and could hide the failure.
subtype=${ARCHIVE_SUBTYPE:-DAILY}
outdir=${OUTDIR:-/out}
mounted_creds=$GNSS_ARCHIVE_CREDENTIALS_FILE

fail() {
    echo "FAILED: $*" >&2
    exit 1
}

if [ ! -r "$mounted_creds" ]; then
    fail "credentials file $mounted_creds not found. Mount it (see docker-compose.yml)."
fi

# get_gnss_data reads credentials as JSON only if the file name ends in .json, otherwise as
# "user ..." / "password ..." lines. The mounted file always has the same name, so copy it
# under the name that matches its content and use that copy, so both formats work.
credsdir=$(mktemp -d) || fail "cannot create a temporary directory"
if grep -q '^[[:space:]]*{' "$mounted_creds"; then
    creds=$credsdir/credentials.json
else
    creds=$credsdir/credentials.txt
fi
cp "$mounted_creds" "$creds" && chmod 600 "$creds" || fail "cannot copy $mounted_creds"
export GNSS_ARCHIVE_CREDENTIALS_FILE=$creds

mkdir -p "$outdir" || fail "cannot create $outdir"

# The data center configuration is test/getdata.conf (LINZGNSS_CONFIG_FILE).
authuri=$(awk '$1=="Name" && $2=="LINZ-GNSSArchive" {f=1} f && tolower($1)=="authkeyuri" {print $2; exit}' "$LINZGNSS_CONFIG_FILE")
[ -n "$authuri" ] || fail "no AuthKeyUri for LINZ-GNSSArchive in $LINZGNSS_CONFIG_FILE"

echo "== 1. Data center configuration"
get_gnss_data -s | grep -A3 LINZ-GNSSArchive || fail "LINZ-GNSSArchive is not configured"

echo "== 2. Authentication request ($authuri)"
# Reads the user name and password the way DataCenter::_readCredentials does.
# The password goes to curl through stdin so it is not visible in the process list.
body=$(mktemp)
status=$(perl -MJSON -0777 -e '
        my ($file) = @ARGV;
        open(my $fh, "<", $file) or exit 1;
        my $text = <$fh>;
        my ($user, $pwd);
        if ($file =~ /\.json$/) {
            my $def = eval { decode_json($text) };
            ($user, $pwd) = ($def->{username}, $def->{password}) if $def;
        } else {
            for my $line (split /\n/, $text) {
                $user = $1 if !$user && $line =~ /^\s*user(?:name)?\s+(\S.*?)\s*$/i;
                $pwd = $1 if !$pwd && $line =~ /^\s*password\s+(\S.*?)\s*$/i;
            }
        }
        exit 2 if !defined $user || !defined $pwd;
        print "user = \"$user:$pwd\"\n";
    ' "$creds" |
    curl -sS -K - -o "$body" -w '%{http_code}' "$authuri") \
    || fail "cannot connect to $authuri. The nonprod archive is only reachable from the LINZ network."
echo "HTTP status $status"
[ "$status" = 200 ] || fail "AuthKeyUri returned HTTP $status, expected 200 (check the credentials)"
grep -q cookies "$body" || fail "AuthKeyUri response has no cookies"
rm -f "$body"

fetch() {
    mark=$1
    echo "== Download OBS:$subtype $mark for $date"
    # get_gnss_data skips a file that is already in the output directory, and the cache
    # can also supply it, so use a fresh directory and no cache for each download. Then a
    # file left by an earlier run, or for another date, can't be reported as a success.
    dest=$outdir/$mark-$(echo "$date" | tr ':' '-')-$subtype
    rm -rf "$dest"
    mkdir -p "$dest" || fail "cannot create $dest"
    LINZGNSS_NO_CACHE=1 get_gnss_data -n LINZ-GNSSArchive -d "$dest" "$date" "OBS:$subtype" "$mark" || fail "get_gnss_data exited with an error for $mark"
    found=$(find "$dest" -type f -size +0 | head -n1)
    [ -n "$found" ] || fail "no file for $mark was downloaded to $dest"
    ls -l "$found"
    cksum "$found"
}

echo "== 3. Download from the archive"
fetch "$station"
case "$ARCHIVE_PRIVATE" in
    1|true|yes)
        fetch "${ARCHIVE_STATION:-GSAL}"
        ;;
esac

echo "OK: connected to the GNSS archive and downloaded data into $outdir"
