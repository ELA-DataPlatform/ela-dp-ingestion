FROM python:3.12-slim

WORKDIR /app

RUN pip install --no-cache-dir spotipy google-cloud-storage google-cloud-bigquery pyyaml

COPY src/ ./src/
COPY config/ ./config/
COPY run.py ./

VOLUME /app/output

ENTRYPOINT ["python", "run.py"]
