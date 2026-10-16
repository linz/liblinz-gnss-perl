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
