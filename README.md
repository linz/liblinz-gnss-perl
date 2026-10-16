# liblinz-gnss-perl

The `LINZ::GNSS` Perl modules, which provide functions for selecting and retrieving GNSS
reference data (orbits, ERP, reference station RINEX files and similar) from remote data
centers, and for handling RINEX and SINEX files and GNSS time formats.

Data retrieved from a data center is stored in a local file cache. Which data centers are
used, and what each one serves, is defined in a `getdata.conf` configuration file, not in
code. The commented `conf/getdata.conf` documents the configuration options.

## Layout

| Path | Contents |
| ---- | -------- |
| `lib/LINZ/GNSS/` | The modules. `FileCache.pm` is the local cache. |
| `lib/LINZ/GNSS/DataCenter/` | Data center types: `Ftp`, `Http`, `File`, `S3Bucket`, `GNSSArchive`. |
| `bin/` | Command line tools. `get_gnss_data` is the main one. |
| `conf/getdata.conf` | Default configuration, installed to `<prefix>/etc/bernese52` (`/etc/bernese52` for prefix `/usr`). `bernese52` is an obsolete name that is still listed as the default and may be overridden (see below). |
| `test/` | Test scripts, the configuration they use (`test/getdata.conf`), and expected output (`test/check`). |
| `debian/` | Debian packaging. |

## Dependencies

Runtime dependencies are listed in two places, which should be kept consistent: `requires`
in `Build.PL`, and `Depends` in `debian/control`.

From `debian/control`:

- `liblinz-geodetic-perl` ([linz/liblinz-geodetic-perl](https://github.com/linz/liblinz-geodetic-perl))
- `linz-rinex-rnxcmp`, which provides `rnx2crx` and `crx2rnx`. `test/getdata.conf` expects them at `/usr/bin/rnx2crx` and `/usr/bin/crx2rnx`.
- `linz-coordsys`
- `gzip`, `ncompress`
- Perl modules: `libconfig-general-perl`, `libarchive-zip-perl`, `libdbd-sqlite3-perl`, `libdbi-perl`, `libipc-run-perl`, `liblog-log4perl-perl`, `libtime-modules-perl | libtime-parsedate-perl`, `libmath-libm-perl`, `liburi-perl`, `libxml-simple-perl`, `libxml-libxml-perl`, `libstorable-perl`, `libtime-local-perl | perl-modules-5.30`, `libfile-which-perl`, `libfile-copy-recursive-perl`, `libwww-perl`, `libperlio-gzip-perl`

`debian/control` also lists `awscli` as a recommended package.

`LINZ::BERN` is not required. Without it, `get_gnss_data` prints a warning and some functions are disabled.

## Build and install

```sh
perl -I . Build.PL
./Build
./Build install
```

`-I .` is needed because `Build.PL` loads `inc::MyBuilder` from this directory.

## Using `get_gnss_data`

```sh
# When running from the source tree:
export PERL5LIB=lib
export LINZGNSS_CONFIG_FILE=conf/getdata.conf

bin/get_gnss_data -l                                       # list file types
bin/get_gnss_data -s                                       # list configured data centers and file types
bin/get_gnss_data -d outdir 2018:200 OBS AUCK              # download into outdir
bin/get_gnss_data -n LINZ-GNSSArchive 2018:200 OBS AUCK    # use one data center
```

The date is `yyyy:ddd` (year and day of year). Without `LINZGNSS_CONFIG_FILE` or
`-C cfgfile`, the configuration is read from `/etc/bernese52/getdata.conf`, which exists only
once the package is installed. `bernese52` is an obsolete name (Bernese is now 5.4, managed
elsewhere) that is still listed as the default but is normally overridden with
`LINZGNSS_CONFIG_FILE` or `-C`. Likewise, setting the `D` environment variable overrides the
datapool defaults in the config (for example `/var/lib/bernese52/datapool`). See the POD in `bin/get_gnss_data` for all options.

## Tests

### Full test suite

```sh
cd test
./run_tests.sh            # all tests
./run_tests.sh gnssa      # one test: http httpa gnssa gnssp cache buffer
```

`run_tests.sh` runs `bin/get_gnss_data` using `test/getdata.conf`, writes logs to
`test/output/`, and finishes by diffing them against `test/check/`. It does not set an exit
status from the diff, so read the "Differences" section at the end of its output.

What it needs:

- Network access to the data centers in `test/getdata.conf`.
- The file `~/.gnss-archive-credentials` to exist. The script only checks that it exists. If it doesn't, the script prints a message and exits without running any tests.
- Credentials for the GNSS archive, in the file named by `CredentialsFile` in `test/getdata.conf`, which is `$GNSS_ARCHIVE_CREDENTIALS_FILE` if set, otherwise `~/.credentials/linz-gnss-archive-credentials.json`. See [Testing the connection to the GNSS archive](docs/testing-gnss-archive-connection.md).
- `rnx2crx` and `crx2rnx` in `/usr/bin`.

`test/getdata.conf` points the GNSS archive at the nonprod URLs
(`*.nonprod.gnss-archive.awsint.linz.govt.nz`). From GitHub-hosted runners that host name
did not resolve.

The files in `test/check/` were generated on another machine. Their `Created DataCenter`
lines include that machine's paths, and the archive lines use the production URLs, so
expect differences in those lines even when the tests work. Check the lines that matter,
such as `Retrieved ...` for each download.

There is also a test for `bin/make_rinex2`:

```sh
cd test/make_rinex2
./test.sh
```

### Continuous integration

`.github/workflows/push.yml` runs on every push. It runs a smoke test only: it installs the
dependencies, checks out `liblinz-geodetic-perl`, builds `rnx2crx` and `crx2rnx` from the
GSI RNXCMP source, runs `perl -c` on every module in `lib/` and on `bin/get_gnss_data`, and
checks that `get_gnss_data -l` and `-s` run and list data centers. It does not run
`run_tests.sh` and makes no network requests, so it does not test the GNSS archive
connection.

### GNSS archive connection

To check that credentials and the connection to the GNSS archive work, including the
`AuthKeyUri` authentication request and a file download, follow
[Testing the connection to the GNSS archive](docs/testing-gnss-archive-connection.md).
The same checks run in a container with `docker compose run --rm archive-test` (see
"Using Docker" in that guide). Set `ARCHIVE_DNS` to the DNS server the host uses for the
nonprod archive, or the container may not resolve its host name.
