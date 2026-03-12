IMAGE   := ela-dp-ingestion
ENV     ?= dev
SOURCE  ?= spotify
TYPES   ?= recently_played
OUTPUT  ?= /Users/etienne/MyAll/DEV/ela-dp/ela-dp-storage

.PHONY: build run

build:
	docker build -t $(IMAGE) .

run: build
	$(if $(filter gs://%,$(OUTPUT)), \
	  docker run --rm \
	    --env-file .env \
	    $(IMAGE) \
	    --env $(ENV) \
	    --source $(SOURCE) \
	    --data-types $(TYPES) \
	    --output-dir $(OUTPUT), \
	  docker run --rm \
	    --env-file .env \
	    -v "$(OUTPUT):/app/output" \
	    $(IMAGE) \
	    --env $(ENV) \
	    --source $(SOURCE) \
	    --data-types $(TYPES))
