SHELL := bash
.ONESHELL:
NAME := $(notdir $(CURDIR))

SIF       ?= $(NAME).sif
TAR       ?= $(NAME).tar
CONTAINER ?= $(if $(shell command -v podman),podman,docker)

all: $(SIF)

$(SIF): Dockerfile Main.lean
	git archive -o $(TAR) HEAD
	$(CONTAINER) build -t $(NAME) - < $(TAR)
	$(CONTAINER) save $(NAME) > $(TAR)
	apptainer build --force $(SIF) docker-archive://$(TAR)

clean:
	$(CONTAINER) rmi $(NAME)
	rm -f $(SIF) $(TAR)
