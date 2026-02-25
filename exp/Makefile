-include config.mk

CONFIGS := bv_check-lrat cake_lpr-lrat lrat_isa-lrat lrat-trim-lrat
DATA    := $(addprefix log-,$(addsuffix /data,$(CONFIGS)))

data: $(DATA)
	awk 'FNR==1 && NR!=1 { next } { print }' $^ > $@

cadical: log-cadical/data
	cp $^ data-cadical
log-%/data: benchmarks-cadical
	../bin/parallel.sh benchmarks-$* log-$*

benchmarks-cadical: make_benchmarks.sh aiger certificates
	./make_benchmarks.sh
	../bin/pack.py --time 200 data benchmarks-*
	../bin/pack.py --time 200 data-cadical benchmarks-cadical
HWMCC24 := https://zenodo.org/records/14775242/files
aiger:
	wget -nc $(HWMCC24)/benchmarks_aiger.tar.gz
	tar xf benchmarks_aiger.tar.gz
	rm benchmarks_aiger.tar.gz
certificates:
	wget -nc $(HWMCC24)/hwmcc24_results_aiger_cert.csv
	wget -nc $(HWMCC24)/hwmcc24_certificates.tar.gz
	mkdir $@
	tar xf hwmcc24_certificates.tar.gz -C $@
	rm hwmcc24_certificates.tar.gz
	ls certificates | grep -v '^ric3' | xargs -I{} rm -r certificates/{}

clean:
	rm -rf sat lrat trim log-*
