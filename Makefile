NS = default

SERVICE_NAME = bigtable-autoscaler
SERVICE_IMAGE_VERSION = latest 
SERVICE_IMAGE_TAG = $(NS)/$(SERVICE_NAME):$(SERVICE_IMAGE_VERSION)

MVN_IMAGE_NAME = mvn-builder
MVN_IMAGE_VERSION = latest
MVN_IMAGE_TAG = $(NS)/$(MVN_IMAGE_NAME):$(MVN_IMAGE_VERSION)

VOLUMES = -v $(PWD)/target:/usr/src/app/target -v $(PWD)/src:/usr/src/app/src


# DOCKER COMPOSE

# Run the service (and postgres) with docker-compose
up:
	docker-compose up --detach

# Stop the service (and postgres)
down:
	docker-compose down

# run only the service container (pointing to your own postgresql)
run:
	docker run --name $(SERVICE_NAME) --detach --env-file .env --rm $(SERVICE_IMAGE_TAG)

# stop service container only
stop:
	docker stop $(SERVICE_NAME)

# See service logs
logs:
	docker logs --follow $(SERVICE_NAME)

# most make targets are phony (not real files)
.PHONY: up down run stop logs build-image maven-build build-maven-builder clean
