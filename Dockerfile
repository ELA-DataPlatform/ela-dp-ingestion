FROM python:3.12-slim AS builder

RUN pip install --no-cache-dir --no-compile --target=/deps \
    spotipy==2.26.0 \
    "garminconnect @ git+https://github.com/cyberjunky/python-garminconnect.git@react" \
    google-cloud-storage==3.9.0 \
    google-cloud-bigquery==3.40.1 \
    pyyaml==6.0.3

FROM python:3.12-slim

RUN pip uninstall -y pip setuptools && \
    rm -rf /var/lib/apt/lists/* /tmp/*

WORKDIR /app

COPY --from=builder /deps /usr/local/lib/python3.12/site-packages/

COPY src/ ./src/
COPY config/ ./config/
COPY run.py ./

VOLUME /app/output

ENTRYPOINT ["python", "run.py"]
