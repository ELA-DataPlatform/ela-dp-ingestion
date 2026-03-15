IMAGE   := ela-dp-ingestion
ENV     ?= dev
SOURCE  ?= spotify
TYPES   ?= recently_played
OUTPUT  ?= /Users/etienne/MyAll/DEV/ela-dp/ela-dp-storage
GCS_DIR ?=

ENV_FILE := $(wildcard .env)
ENV_FILE_FLAG := $(if $(ENV_FILE),--env-file .env,)

.PHONY: build fetch load run

build:
	docker build -t $(IMAGE) .

## make fetch OUTPUT=gs://bucket/path [TYPES="recently_played top_tracks"]
fetch: build
	$(if $(filter gs://%,$(OUTPUT)), \
	  docker run --rm \
	    $(ENV_FILE_FLAG) \
	    $(IMAGE) \
	    --mode fetch \
	    --env $(ENV) \
	    --source $(SOURCE) \
	    --data-types $(TYPES) \
	    --output-dir $(OUTPUT), \
	  docker run --rm \
	    $(ENV_FILE_FLAG) \
	    -v "$(OUTPUT):/app/output" \
	    $(IMAGE) \
	    --mode fetch \
	    --env $(ENV) \
	    --source $(SOURCE) \
	    --data-types $(TYPES))

## make load GCS_DIR=gs://bucket/path/spotify/
load: build
	docker run --rm \
	  $(ENV_FILE_FLAG) \
	  -v "$(HOME)/.config/gcloud:/root/.config/gcloud:ro" \
	  -e GOOGLE_APPLICATION_CREDENTIALS=/root/.config/gcloud/application_default_credentials.json \
	  $(IMAGE) \
	  --mode load \
	  --env $(ENV) \
	  --source $(SOURCE) \
	  --gcs-dir $(GCS_DIR)

## make run OUTPUT=gs://bucket/path — fetch + load en une seule commande
run: build
	$(if $(filter gs://%,$(OUTPUT)), \
	  docker run --rm \
	    $(ENV_FILE_FLAG) \
	    $(IMAGE) \
	    --mode all \
	    --env $(ENV) \
	    --source $(SOURCE) \
	    --data-types $(TYPES) \
	    --output-dir $(OUTPUT), \
	  docker run --rm \
	    $(ENV_FILE_FLAG) \
	    -v "$(OUTPUT):/app/output" \
	    $(IMAGE) \
	    --mode fetch \
	    --env $(ENV) \
	    --source $(SOURCE) \
	    --data-types $(TYPES))
