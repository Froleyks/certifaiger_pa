SHELL := bash
.ONESHELL:

all: bin

.PHONY: bin
bin:
	mkdir bin
	$(MAKE) -C src
