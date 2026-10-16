# Testing the connection to the GNSS archive

The `GNSSArchive` data center first requests `AuthKeyUri` (HTTP basic authentication with
the username and password) and expects a `200` response with a JSON body containing a
`cookies` list. It then downloads files from `Uri` using those cookies. The steps below
check each stage. Run the commands from the root of the repository.

See the [README](../README.md) for the dependencies and for running the full test suite.

1. Create the credentials file. A `.json` file looks like:

   ```json
   { "username": "the-user-name", "password": "the-password" }
   ```

   A text file with lines `user the-user-name` and `password the-password` is also accepted.
   Do not commit this file.

   ```sh
   mkdir -p ~/.credentials
   chmod 700 ~/.credentials
   # create ~/.credentials/linz-gnss-archive-credentials.json, then
   chmod 600 ~/.credentials/linz-gnss-archive-credentials.json
   touch ~/.gnss-archive-credentials     # only needed by run_tests.sh
   ```

   To use a different location, set `GNSS_ARCHIVE_CREDENTIALS_FILE` (used by
   `test/getdata.conf`). The default `conf/getdata.conf` reads `GNSS_ARCHIVE_AUTH_FILE`
   instead, defaulting to `~/linz-gnss-credentials`.

2. Check the data center is configured:

   ```sh
   cd test
   export LINZGNSS_CONFIG_FILE=./getdata.conf PERL5LIB=../lib
   ../bin/get_gnss_data -s | grep -A3 LINZ-GNSSArchive
   ```

3. Check the authentication request on its own. Use the `AuthKeyUri` from the
   configuration file in use (`test/getdata.conf` has the nonprod one):

   ```sh
   curl -i -u '<username>' '<AuthKeyUri>'
   ```

   `curl` asks for the password. Expect `HTTP/... 200` and a JSON body with `cookies`. Any
   other status is what makes `get_gnss_data` fail with
   `Connection to LINZ-GNSSArchive AuthKeyUri failed: ...`.

4. Download a file through `get_gnss_data`. These are the commands the `gnssa` and `gnssp`
   tests run:

   ```sh
   cd test
   export LINZGNSS_CONFIG_FILE=./getdata.conf PERL5LIB=../lib LINZGNSS_DEBUG=1
   mkdir -p files
   ../bin/get_gnss_data -n LINZ-GNSSArchive -d files 2018:200 OBS AUCK   # public station
   ../bin/get_gnss_data -n LINZ-GNSSArchive -d files 2018:200 OBS GSAL   # private station
   ```

   A successful download ends with lines like these (the host name depends on the
   configuration):

   ```
   GNSSArchive: Trying https://<host>/rnx2_daily_30s/2018/200/auck2000.18d.gz
   Retrieved auck2000.18d.gz (915808 bytes) from LINZ-GNSSArchive
   Retrieved OBS:DAILY for AUCK: files/AUCK2000.18O
   ```

   `LINZGNSS_DEBUG=1` is what produces the detailed log lines. If the data center can't be
   reached, the output has `Retrieving file failed: Connection to LINZ-GNSSArchive
   AuthKeyUri failed: ...` and `Data OBS for AUCK not available`.

## Using Docker

`docker-compose.yml` and `docker/` run the same checks in a container, without installing
the Perl dependencies, `liblinz-geodetic-perl` or the Hatanaka tools locally. It uses
`test/getdata.conf`, so it connects to the nonprod archive and only works from the LINZ
network (or VPN). It needs the file from step 1.

```sh
ARCHIVE_DNS=<dns-server-ip> docker compose run --build --rm archive-test
```

`docker/test-archive.sh` runs these steps in order and stops at the first failure, with a
non-zero exit code:

1. Checks that `LINZ-GNSSArchive` is configured (`get_gnss_data -s`).
2. Makes the `AuthKeyUri` request and expects `200` with `cookies`.
3. Downloads one `OBS:DAILY` file with `get_gnss_data` and checks that it is in
   `docker/output/` on the host. This is a check of the connection, not the full test suite
   (`test/run_tests.sh`).

### Options

Pass these as environment variables to `docker compose`.

| Variable | Default | Purpose |
| -------- | ------- | ------- |
| `ARCHIVE_DNS` | `1.1.1.1` | DNS server for the container. See below. |
| `GNSS_ARCHIVE_CREDENTIALS_FILE` | `~/.credentials/linz-gnss-archive-credentials.json` | The file from step 1 on the host. |
| `ARCHIVE_DATE` | `2018:200` | Date of the file, `yyyy:ddd`. |
| `ARCHIVE_STATION` | `AUCK` | Station code. |
| `ARCHIVE_SUBTYPE` | `DAILY` | `DAILY` or `HOURLY`. |
| `ARCHIVE_PRIVATE` | unset | `1`, `true` or `yes` repeats the download as a private check: for `ARCHIVE_STATION` if it is supplied, otherwise for the private station `GSAL`. Unset, empty, `0` or anything else skips it. |

Without `ARCHIVE_STATION` the download is for `AUCK`, whatever `ARCHIVE_PRIVATE` is. For
example:

```sh
ARCHIVE_DATE=2026:276 ARCHIVE_STATION=GSAA docker compose run --rm archive-test
```

The credentials file is mounted into the container at run time and is not copied into the
image.

The script asks for `OBS:DAILY` explicitly. If the subtype is left out, `get_gnss_data`
uses the first subtype by priority, which is `HOURLY`, so it tries the last hourly file of
the day (`/rnx2_hourly_30s/<yyyy>/<ddd>/23/<ssss><ddd>x.<yy>d.gz`) instead of the daily file
(`/rnx2_daily_30s/<yyyy>/<ddd>/<ssss><ddd>0.<yy>d.gz`).

### DNS

Containers don't use the host's split DNS, so the nonprod host name may not resolve even
when the host is on the LINZ network or VPN (`curl: (6) Could not resolve host`). Set
`ARCHIVE_DNS` to the DNS server the host uses for it. Find it with `resolvectl status`: it
is the "Current DNS Server" of the VPN tunnel link. If `ARCHIVE_DNS` is not set,
`docker-compose.yml` uses the public server `1.1.1.1`, which can't resolve the nonprod host
name.

`ARCHIVE_DNS` is an environment variable that `docker compose` reads. Pass it in any of
these ways:

```sh
# For one run
ARCHIVE_DNS=<dns-server-ip> docker compose run --rm archive-test

# For the shell session
export ARCHIVE_DNS=<dns-server-ip>
docker compose run --rm archive-test

# Persistently: a line in a .env file next to docker-compose.yml, which compose reads
# automatically (the file is not committed)
echo 'ARCHIVE_DNS=<dns-server-ip>' >> .env
```

### Notes and troubleshooting

- **Rebuild after changes.** `docker/test-archive.sh` is copied into the image, and
  `docker compose run` does not rebuild it. After changing the script or the Dockerfile,
  use `docker compose run --build ...` or `docker compose build`. Otherwise the container
  runs the old script.
- **Options that seem ignored.** Compose reads variables from the shell and from a `.env`
  file next to `docker-compose.yml`. A value set in either, such as `ARCHIVE_PRIVATE=1`,
  applies to every run. Check what compose uses with
  `docker compose config | grep ARCHIVE_`. Set the variable before the command, not after
  the service name: anything after `archive-test` is run as a command instead.
- **`AuthKeyUri returned HTTP 401`.** The credentials were rejected. Check that the file
  holds a username and password for the nonprod archive.
- **`Cannot retrieve ...: Forbidden` (403).** Authentication worked but the file could not
  be read. The archive may return 403 for a file that doesn't exist for that date or
  station, as well as for one the user can't access. Try a station and date that you know
  have a file. `get_gnss_data` then reports the data as `appears to be delayed`.
- **Output ownership.** The container runs as root, so files in `docker/output/` are owned
  by root on the host.
- **Other commands.** Arguments after the service name run as a command in the image
  instead of the test, for example to list the file types:

  ```sh
  docker compose run --rm archive-test get_gnss_data -l
  ```
