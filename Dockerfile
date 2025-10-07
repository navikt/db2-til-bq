FROM europe-north1-docker.pkg.dev/cgr-nav/pull-through/nav.no/python:3.13-dev AS builder

WORKDIR /app

USER root
# Install system dependencies in builder stage
RUN apk add --no-cache linux-pam
USER nonroot

RUN python3 -m venv venv
ENV PATH=/app/venv/bin:$PATH
RUN pip install --upgrade pip setuptools wheel

# Install python deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Use distroless images for runtime
FROM europe-north1-docker.pkg.dev/cgr-nav/pull-through/nav.no/python:3.13

# Copy system libraries required by ibm_db from builder
COPY --from=builder /lib/libpam.* /lib/libcrypt.* /lib/

# 1069 is enforced user and group ID used by the Nais platform
COPY --chown=1069:1069 --from=builder /app /app

ENV PATH=/app/venv/bin:$PATH
ENV PYTHONPATH=/app

WORKDIR /app

COPY main.py .
COPY src ./src

ENTRYPOINT [ "/app/venv/bin/python", "main.py" ]