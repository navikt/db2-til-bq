FROM europe-north1-docker.pkg.dev/cgr-nav/pull-through/nav.no/python:3.12-dev

RUN pip install --upgrade pip setuptools wheel

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
