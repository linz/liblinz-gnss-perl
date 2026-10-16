# AGENTS.md

Guidance for AI coding agents working in this repo. Human docs are `README.md`, `CONTRIBUTING.md` (releases) and `docs/`; the POD in the modules and the commented `conf/getdata.conf` document the code and config.

## What this is

`liblinz-gnss-perl`: the `LINZ::GNSS` Perl modules for downloading GNSS data (RINEX, orbits, ionosphere, VMF, etc.) from remote data centers and managing a local file cache. It is the core of the Bernese GETDATA utility. It is packaged as a Debian package (`debian/`) and built with `Module::Build` (`Build.PL`, `inc/MyBuilder.pm`).

Data centers are pluggable subclasses of `LINZ::GNSS::DataCenter` in `lib/LINZ/GNSS/DataCenter/` (`Ftp`, `Http`, `File`, `S3Bucket`, `GNSSArchive`). What each one serves, and the file naming and compression for each data type, is defined in `conf/getdata.conf`, not in code.

## Layout

- `bin/`: command line tools. `get_gnss_data` is the main one (get/cache/preload data, list types and sources).
- `lib/LINZ/GNSS/FileCache.pm`: the local cache (SQLite index plus files). `DailyProcessor.pm`: batch daily processing driver.
- `conf/getdata.conf`: default config, installed to `/etc/bernese52/` (see `inc/MyBuilder.pm`). `bernese52` is an obsolete name (Bernese is now 5.4, managed elsewhere) that is still listed as the default but is normally overridden: deployments point at their own config with `LINZGNSS_CONFIG_FILE` or `-C`. It is also the reference for every config option, in comments.
- `test/getdata.conf`: separate config used only by the tests. Keep it in sync by hand when changing `conf/getdata.conf` behaviour the tests rely on.

## Commands

```bash
perl -I . Build.PL && ./Build  # build; -I . because Build.PL loads inc::MyBuilder
cd test && ./run_tests.sh      # integration tests, optionally with one name: http httpa gnssa gnssp cache buffer
cd test/make_rinex2 && ./test.sh
```

There is no unit test suite. The only CI (`.github/workflows/push.yml`) is a smoke test: `perl -c` on every module and `bin/get_gnss_data`, then `get_gnss_data -l` and `-s`. It does not run `run_tests.sh`.

- The GNSS archive connection and download are checked by the Docker test, `docker/test-archive.sh` (see `docs/testing-gnss-archive-connection.md`). It needs the LINZ network and a machine account.
- `test/run_tests.sh` runs `bin/get_gnss_data` against live remote servers and diffs its logs against `test/check/`. It needs network access and `~/.gnss-archive-credentials`. If the credentials file is missing it prints a message and exits without failing. If either is unavailable, say so; don't claim the tests passed.
- Run scripts from the source tree with `PERL5LIB=lib`. `test/run_tests.sh` sets `../lib` itself.

## Conventions and gotchas

- **`.gitignore` ignores `test`**, yet `test/` files are tracked. New test files need `git add -f`.
- Behaviour is driven by environment variables as well as config: `LINZGNSS_CONFIG_FILE`, `LINZGNSS_CACHE_DIR`, `LINZGNSS_NO_CACHE`, `LINZGNSS_CACHE_NODB`, `LINZGNSS_TMP_DIR`, `LINZGNSS_LOG_DIR`, `LINZGNSS_DEBUG`, `POSITIONZ_REFSTATION_DIR`, `D`. Check these before assuming a config value is in effect.
- `conf/getdata.conf` uses `${D||default}` and `[ssss]`/`yyyy`/`ddd` style substitutions. Variables `$D` and `$P` are the datapool and campaign directories. When the `D` environment variable is set it overrides the defaults in the config, so paths such as `/var/lib/bernese52/datapool` are only fallbacks. The code itself does not read `D`; only the config substitutions do.
- Credentials: a data center's `CredentialsFile` overrides `User`/`Password`, which override credentials embedded in `Uri`. Http data centers only send credentials to the data center host, plus any `AuthHosts`, and only over https unless the Uri is http (recent change; see the commented `AllowDowngrade` option in `conf/getdata.conf`).
- GNSSArchive credentials: the `GNSSArchive` data center authenticates with the OAuth2 client credentials flow, not a user login, so MFA does not apply. Its `CredentialsFile` must define `client_id` and `client_secret` (JSON or `client_id ...`/`client_secret ...` lines); `GNSSArchive::_readCredentials` rejects user/password files and does not fall back to them. `AuthKeyUri` is `.../v1/machineAuth` (a GET with Basic `client_id:client_secret`, returning CloudFront cookies). The Basic header must be built with `encode_base64(..., '')`, as long credentials otherwise get line breaks. Other data center types are unchanged. Machine accounts are managed in the gnss-data-archive repo (`docs/MACHINE_ACCOUNTS.md`).
- A failure in one data center must not abort the request: the code continues on to the next. Keep that when editing `DataCenter.pm` or the subclasses.
- Optional dependency `LINZ::BERN` is not required. `bin/get_gnss_data` prints a warning and disables some functions without it, and the test script strips that warning from logs.
- Test logs in `test/check/` are the expected output, with dates and temp dirs normalised by `run_tests.sh`. If you intentionally change log output, update them.
- Runtime dependencies are listed in two places that must stay consistent: `Build.PL` (`requires`) and `debian/control` (`Depends`). Add new modules to both. Releases are git tags like `1.115.0-1` (see `CONTRIBUTING.md`); `debian/changelog` has not been updated since `1.97.0-1`.
