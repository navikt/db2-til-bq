FROM python:3.12-slim

RUN apt-get update && apt-get install -yq --no-install-recommends \
    build-essential \
    git

COPY requirements.txt ./
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

USER apprunner
CMD [ "python", "./main.py" ]