# Environment for testing the GNSS archive connection and downloading a file.
# Build from the repository root:  docker build -f docker/Dockerfile -t gnss-archive-test .
# See docs/testing-gnss-archive-connection.md.
#
# The dependency set mirrors .github/workflows/push.yml. linz-rinex-rnxcmp is an
# internal package, so the Hatanaka tools are built from the GSI source release.
FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive

# libjson-perl is used by bin/get_gnss_data and the GNSSArchive data center, but is
# not listed in Build.PL or debian/control.
RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates curl git gcc libc6-dev gzip ncompress \
        libconfig-general-perl libarchive-zip-perl libdbd-sqlite3-perl \
        libdbi-perl libipc-run-perl liblog-log4perl-perl \
        libtime-parsedate-perl libmath-libm-perl liburi-perl \
        libxml-simple-perl libxml-libxml-perl libfile-which-perl \
        libfile-copy-recursive-perl libwww-perl libperlio-gzip-perl \
        libjson-perl \
    && rm -rf /var/lib/apt/lists/*

# rnx2crx and crx2rnx, installed where test/getdata.conf expects them.
RUN curl -sSfLO https://terras.gsi.go.jp/ja/crx2rnx/RNXCMP_4.2.0_src.tar.gz \
    && echo "9a452d2908a27c5b3152cc424e048847221c065e97f6b1f1a585c7b0c34fbace  RNXCMP_4.2.0_src.tar.gz" | sha256sum -c - \
    && mkdir /tmp/rnxcmp \
    && tar -xzf RNXCMP_4.2.0_src.tar.gz -C /tmp/rnxcmp --strip-components=1 \
    && gcc /tmp/rnxcmp/source/rnx2crx.c -o /usr/bin/rnx2crx \
    && gcc /tmp/rnxcmp/source/crx2rnx.c -o /usr/bin/crx2rnx \
    && rm -rf /tmp/rnxcmp RNXCMP_4.2.0_src.tar.gz

# LINZ::Geodetic::* is not available as an apt package.
RUN git clone --depth 1 https://github.com/linz/liblinz-geodetic-perl /opt/liblinz-geodetic-perl

COPY . /app
WORKDIR /app

ENV PERL5LIB=/app/lib:/opt/liblinz-geodetic-perl/lib \
    LINZGNSS_CONFIG_FILE=/app/test/getdata.conf \
    LINZGNSS_CACHE_DIR=/tmp/cache \
    LINZGNSS_DEBUG=1 \
    GNSS_ARCHIVE_CREDENTIALS_FILE=/run/secrets/gnss-archive-credentials.json \
    PATH=/app/bin:$PATH

RUN mkdir -p /tmp/cache /out

# With no arguments, run the archive connection test. Any arguments are run as a
# command instead, e.g.  docker run --rm gnss-archive-test get_gnss_data -l
ENTRYPOINT ["/app/docker/test-archive.sh"]
