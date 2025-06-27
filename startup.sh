#!/usr/bin/env bash

cp "$(realpath /var/run/secrets/db2-license/db2consv_zs.lic)" /usr/local/lib/python3.12/site-packages/clidriver/license/db2consv_zs.lic

while :
do
	echo "Going to sleep for 5 min!"
	sleep 300
done

python -u /main.py
