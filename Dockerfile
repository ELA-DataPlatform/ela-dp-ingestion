FROM python:3.12-slim

WORKDIR /app

RUN pip install --no-cache-dir spotipy google-cloud-storage

COPY src/ ./src/
COPY run.py ./

VOLUME /app/output

ENTRYPOINT ["python", "run.py"]
