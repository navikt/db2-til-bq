#!/usr/bin/env bash

# Copy the DB2 license file to the appropriate location
cp /var/run/secrets/db2-license/db2consv_zs.lic /usr/local/lib/python3.12/site-packages/clidriver/license/db2consv_zs.lic

python -u /main.py
