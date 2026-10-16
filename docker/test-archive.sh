#!/bin/sh
# Test the connection to the GNSS archive and download a file from it.
#
# Environment:
#   GNSS_ARCHIVE_CREDENTIALS_FILE  JSON file with "username" and "password"
#   ARCHIVE_DATE                   yyyy:ddd of the file to download (default 2018:200)
#   ARCHIVE_STATION                station code (default AUCK)
#   ARCHIVE_SUBTYPE                OBS subtype, DAILY or HOURLY (default DAILY)
#   ARCHIVE_PRIVATE                1, true or yes: run the download again as the private
#                                  check, for ARCHIVE_STATION if supplied, otherwise GSAL.
#                                  Anything else (unset, empty, 0) skips it.
#   OUTDIR                         where downloaded files go (default /out)
#
# Any command line arguments are run as a command instead of the test.

if [ $# -gt 0 ]; then
    exec "$@"
fi

date=${ARCHIVE_DATE:-2018:200}
station=${ARCHIVE_STATION:-AUCK}
# Without a subtype get_gnss_data uses the first by priority, which is HOURLY (the last
# hour of the day), not the daily file.
subtype=${ARCHIVE_SUBTYPE:-DAILY}
outdir=${OUTDIR:-/out}
creds=$GNSS_ARCHIVE_CREDENTIALS_FILE

fail() {
    echo "FAILED: $*" >&2
    exit 1
}

if [ ! -r "$creds" ]; then
    fail "credentials file $creds not found. Mount it (see docker-compose.yml)."
fi

mkdir -p "$outdir" || fail "cannot create $outdir"

# The data center configuration is test/getdata.conf (LINZGNSS_CONFIG_FILE).
authuri=$(awk '$1=="Name" && $2=="LINZ-GNSSArchive" {f=1} f && tolower($1)=="authkeyuri" {print $2; exit}' "$LINZGNSS_CONFIG_FILE")
[ -n "$authuri" ] || fail "no AuthKeyUri for LINZ-GNSSArchive in $LINZGNSS_CONFIG_FILE"

echo "== 1. Data center configuration"
get_gnss_data -s | grep -A3 LINZ-GNSSArchive || fail "LINZ-GNSSArchive is not configured"

echo "== 2. Authentication request ($authuri)"
user=$(perl -MJSON -0777 -e 'print decode_json(<STDIN>)->{username}' < "$creds") \
    || fail "cannot read username from $creds"
pass=$(perl -MJSON -0777 -e 'print decode_json(<STDIN>)->{password}' < "$creds") \
    || fail "cannot read password from $creds"
# The password goes to curl through stdin so it is not visible in the process list.
body=$(mktemp)
status=$(printf 'user = "%s:%s"\n' "$user" "$pass" |
    curl -sS -K - -o "$body" -w '%{http_code}' "$authuri") \
    || fail "cannot connect to $authuri. The nonprod archive is only reachable from the LINZ network."
echo "HTTP status $status"
[ "$status" = 200 ] || fail "AuthKeyUri returned HTTP $status, expected 200 (check the credentials)"
grep -q cookies "$body" || fail "AuthKeyUri response has no cookies"
rm -f "$body"

fetch() {
    mark=$1
    echo "== Download OBS:$subtype $mark for $date"
    get_gnss_data -n LINZ-GNSSArchive -d "$outdir" "$date" "OBS:$subtype" "$mark" || fail "get_gnss_data exited with an error for $mark"
    found=$(find "$outdir" -type f -iname "${mark}*" -size +0 | head -n1)
    [ -n "$found" ] || fail "no file for $mark was downloaded to $outdir"
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
