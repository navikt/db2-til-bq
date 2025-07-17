#!/usr/bin/env bash

set -e

# Copy the DB2 license file to the appropriate location
cp /var/run/secrets/db2-license/db2consv_zs.lic /usr/local/lib/python3.12/site-packages/clidriver/license/db2consv_zs.lic

# Run Python unbuffered to ensure logs are printed immediately using -u
python -u ./main.py
