# AGENTS.md

Guidance for AI coding agents working in this repo. There is no README or `doc/`; the POD in the modules and the commented `conf/getdata.conf` are the documentation.

## What this is

`liblinz-gnss-perl`: the `LINZ::GNSS` Perl modules for downloading GNSS data (RINEX, orbits, ionosphere, VMF, etc.) from remote data centers and managing a local file cache. It is the core of the Bernese GETDATA utility. It is packaged as a Debian package (`debian/`) and built with `Module::Build` (`Build.PL`, `inc/MyBuilder.pm`).

Data centers are pluggable subclasses of `LINZ::GNSS::DataCenter` in `lib/LINZ/GNSS/DataCenter/` (`Ftp`, `Http`, `File`, `S3Bucket`, `GNSSArchive`). What each one serves, and the file naming and compression for each data type, is defined in `conf/getdata.conf`, not in code.

## Layout

- `bin/`: command line tools. `get_gnss_data` is the main one (get/cache/preload data, list types and sources).
- `lib/LINZ/GNSS/FileCache.pm`: the local cache (SQLite index plus files). `DailyProcessor.pm`: batch daily processing driver.
- `conf/getdata.conf`: default config, installed to `/etc/bernese52/` (see `inc/MyBuilder.pm`). It is also the reference for every config option, in comments.
- `test/getdata.conf`: separate config used only by the tests. Keep it in sync by hand when changing `conf/getdata.conf` behaviour the tests rely on.

## Commands

```bash
perl Build.PL && ./Build       # build
cd test && ./run_tests.sh      # integration tests, optionally with one name: http httpa gnssa gnssp cache buffer
cd test/make_rinex2 && ./test.sh
```

There is no unit test suite and no CI workflow in the repo.

- `test/run_tests.sh` runs `bin/get_gnss_data` against live remote servers and diffs its logs against `test/check/`. It needs network access and `~/.gnss-archive-credentials`. If the credentials file is missing it prints a message and exits without failing. If either is unavailable, say so; don't claim the tests passed.
- Run scripts from the source tree with `PERL5LIB=lib`. `test/run_tests.sh` sets `../lib` itself.

## Conventions and gotchas

- **`.gitignore` ignores `test`**, yet `test/` files are tracked. New test files need `git add -f`.
- Behaviour is driven by environment variables as well as config: `LINZGNSS_CONFIG_FILE`, `LINZGNSS_CACHE_DIR`, `LINZGNSS_NO_CACHE`, `LINZGNSS_CACHE_NODB`, `LINZGNSS_TMP_DIR`, `LINZGNSS_LOG_DIR`, `LINZGNSS_DEBUG`, `POSITIONZ_REFSTATION_DIR`. Check these before assuming a config value is in effect.
- `conf/getdata.conf` uses `${D||default}` and `[ssss]`/`yyyy`/`ddd` style substitutions. Variables `$D` and `$P` are the datapool and campaign directories.
- Credentials: a data center's `CredentialsFile` overrides `User`/`Password`, which override credentials embedded in `Uri`. Http data centers only send credentials to the data center host, plus any `AuthHosts`, and only over https unless the Uri is http (recent change; see the commented `AllowDowngrade` option in `conf/getdata.conf`).
- A failure in one data center must not abort the request: the code continues on to the next. Keep that when editing `DataCenter.pm` or the subclasses.
- Optional dependency `LINZ::BERN` is not required. `bin/get_gnss_data` prints a warning and disables some functions without it, and the test script strips that warning from logs.
- Test logs in `test/check/` are the expected output, with dates and temp dirs normalised by `run_tests.sh`. If you intentionally change log output, update them.
- Runtime dependencies are listed in two places that must stay consistent: `Build.PL` (`requires`) and `debian/control` (`Depends`). Add new modules to both. Releases are tracked in `debian/changelog`.
