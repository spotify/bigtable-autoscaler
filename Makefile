NS = default

SERVICE_NAME = bigtable-autoscaler
SERVICE_IMAGE_VERSION ?= 0.0.1 
SERVICE_IMAGE_TAG = $(NS)/$(SERVICE_NAME):$(SERVICE_IMAGE_VERSION)

MVN_IMAGE_NAME = mvn-builder
MVN_IMAGE_VERSION = latest
MVN_IMAGE_TAG = $(NS)/$(MVN_IMAGE_NAME):$(MVN_IMAGE_VERSION)

VOLUMES = -v $(PWD)/target:/usr/src/app/target -v $(PWD)/src:/usr/src/app/src


# DOCKER COMPOSE

# Run the service (and postgres) with docker-compose
up: build-image
	docker-compose up --detach

# Stop the service (and postgres)
down:
	docker-compose down



# DOCKER ONLY

# run only the service container (pointing to your own postgresql)
run: build-image
	docker run --name $(SERVICE_NAME) --detach --env-file .env --rm $(SERVICE_IMAGE_TAG)

# stop service container only
stop:
	docker stop $(SERVICE_NAME)



# See service logs
logs:
	docker logs --follow $(SERVICE_NAME)

# Build the service image
build-image: maven-build Dockerfile
	docker build --tag $(SERVICE_IMAGE_TAG) .

# just to have a short name for next target
maven-build: target/bigtable-autoscaler.jar

# package service as jar
target/bigtable-autoscaler.jar: build-maven-builder src/**/*
	docker run --name $(MVN_IMAGE_NAME) $(VOLUMES) --rm $(MVN_IMAGE_TAG) mvn -T 1C -o package

# rebuild the maven image if pom.xml changed
build-maven-builder: pom.xml Builder.Dockerfile
	docker build --tag $(MVN_IMAGE_TAG) -f Builder.Dockerfile --cache-from $(MVN_IMAGE_TAG) .

clean:
	rm -rf target
	docker rmi $(SERVICE_IMAGE_TAG)

# most make targets are phony (not real files)
.PHONY: up down run stop logs build-image maven-build build-maven-builder clean
