#!/usr/bin/env bash

set -e

# Copy the DB2 license file to the appropriate location
cp "$(realpath /var/run/secrets/db2-license/db2consv_zs.lic)" /app/venv/lib/python3.12/site-packages/clidriver/license/

# Run Python unbuffered to ensure logs are printed immediately using -u
python -u ./main.py
