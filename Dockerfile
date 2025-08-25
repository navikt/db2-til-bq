FROM python:3.12-slim
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Update/install everything non-python
RUN apt-get update && apt-get install -yq --no-install-recommends \
    build-essential wget python3-dev libxml2

RUN pip install --upgrade pip
RUN pip install uv

WORKDIR /app

# Install python deps
COPY pyproject.toml poetry.lock .
RUN uv sync --locked --no-install-project --no-dev

# db2client can't follow symlinks, so we need to be able to copy the license file
RUN chmod a+w /usr/local/lib/python3.12/site-packages/clidriver/license

COPY startup.sh .
COPY main.py .
COPY src ./src

ENV PYTHONPATH=/app

CMD [ "./startup.sh" ]
