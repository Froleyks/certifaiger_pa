SHELL := bash
.ONESHELL:
bin := $(abspath ../bin)

STATIC ?= 1
ifeq ($(STATIC),0)
  STATIC_FLAG :=
else
  STATIC_FLAG := -static
endif

all: cake_lpr lrat_isa cadical lrat-trim certifaiger bv_check
cake_lpr:
	-git clone https://github.com/tanyongkiam/cake_lpr
	cd cake_lpr
	[ -n "$(STATIC_FLAG)" ] && \
		gcc $(STATIC_FLAG) basis_ffi.c cake_lpr.S -o cake_lpr -std=c99 || \
		$(MAKE)
	cp cake_lpr $(bin)
lrat_isa:
	-git clone https://github.com/lammich/lrat_isa
	$(MAKE) -C lrat_isa
	cp lrat_isa/code/lrat_isa $(bin)
cadical:
	-git clone https://github.com/arminbiere/cadical
	cd cadical
	./configure $(STATIC_FLAG)
	$(MAKE)
	cp build/cadical $(bin)
lrat-trim:
	-git clone https://github.com/arminbiere/lrat-trim
	cd lrat-trim
	./configure
	[ -n "$(STATIC_FLAG)" ] && grep -q 'COMPILE=.*-static' makefile || \
		sed -i "s#^COMPILE=.*#& $(STATIC_FLAG)#" makefile
	$(MAKE)
	cp lrat-trim $(bin)
certifaiger:
	-git clone https://github.com/Froleyks/certifaiger
	$(MAKE) -C certifaiger
	cd certifaiger/build/
	cp aigsplit aigtocnf certifaiger runlim limit $(bin)
bv_check:
	$(MAKE) -C bv_check
	cp bv_check/.lake/build/bin/bv_check $(bin)

clean:
	rm -rf cake_lpr lrat_isa cadical lrat-trim certifaiger
