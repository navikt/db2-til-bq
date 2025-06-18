FROM python:3

RUN apt-get update && apt-get install -yq --no-install-recommends \
    build-essential \
    git

COPY requirements.txt .
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

RUN ln -s .venv/lib/python3.13/site-packages/clidriver/license/db2consv_zs.lic db2consv_zs.lic

COPY main.py .
CMD [ "python", "./main.py" ]