FROM python:3.12

# Update/install everything non-python
RUN apt-get update && apt-get install -yq --no-install-recommends \
    build-essential wget python3-dev libxml2

RUN pip install --upgrade pip

# Install python deps non-virtualenv-like
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

RUN pip install ibm_db --no-binary :all: --no-cache-dir

RUN ln -s /var/run/secrets/db2-license/db2consv_zs.lic /usr/local/lib/python3.12/site-packages/clidriver/license/db2consv_zs.lic # Python 3.12 only
COPY main.py .

CMD [ "python", "-u", "./main.py" ]
