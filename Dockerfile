FROM europe-north1-docker.pkg.dev/cgr-nav/pull-through/nav.no/python:3.12-dev

# Update/install everything non-python
RUN apt-get update && apt-get install -yq --no-install-recommends \
    build-essential wget python3-dev libxml2

RUN pip install --upgrade pip

WORKDIR /app

# Install python deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# db2client can't follow symlinks, so we need to be able to copy the license file
RUN chmod a+w /usr/local/lib/python3.12/site-packages/clidriver/license

COPY startup.sh .
COPY main.py .
COPY src ./src

ENV PYTHONPATH=/app

CMD [ "./startup.sh" ]
