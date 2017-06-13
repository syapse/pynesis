.PHONY:  build shell test default

# Name of this service/application
export SERVICE_NAME := pynesis

# Configure shell to use bash by default
SHELL := $(shell which bash)

# Get docker path or an empty string
DOCKER := $(shell command -v docker)

# Get the main unix group for the user running make (to be used later)
export GID := $(shell id -g)

# Get the unix user id for the user running make (to be used later)
export UID := $(shell id -u)

# Home directory within the image, for .ssh and similar volumes
SERVICE_HOME := "/home/$(SERVICE_NAME)"

# Bash history file for container shell
HISTORY_FILE := ~/.bash_history.$(SERVICE_NAME)

default: build

deps:
ifndef DOCKER
	@echo "Docker is not available. Please install docker"
	@exit 1
endif

build:
	$(DOCKER) build --build-arg uid=$(UID) --build-arg gid=$(GID) -t $(SERVICE_NAME):latest .

shell: build
	$(DOCKER) run -v $$PWD:/code -v pynesis-tox:/code/.tox --rm -it $(SERVICE_NAME):latest /bin/bash

test: build
	# Required to clean ephemeral storage after running tests (even if they fail)
	$(DOCKER) run -v $$PWD:/code -v pynesis-tox:/code/.tox --rm $(SERVICE_NAME):latest tox

publish: test
	python setup.py sdist upload -r pypi

