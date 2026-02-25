SHELL := bash
.ONESHELL:
NAME := $(notdir $(CURDIR))

CONTAINER      ?= $(if $(shell command -v podman),podman,docker)
SIF            ?= $(NAME).sif
TAR            ?= $(NAME).tar
LEAN_TOOLCHAIN ?= leanprover/lean4:stable

all: $(SIF)

$(SIF):
	git archive -o $(TAR) HEAD
	$(CONTAINER) build -t $(NAME) - < $(TAR)
	$(CONTAINER) save $(NAME) > $(TAR)
	apptainer build --force $(SIF) docker-archive://$(TAR)

clean:
	$(CONTAINER) -rm $(NAME)
	rm -f $(SIF) $(TAR)
