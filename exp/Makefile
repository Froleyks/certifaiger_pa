-include config.mk

CONFIGS := cadical bvcheck-lrat bvcheck-trim cake_lpr-lrat cake_lpr-trim lrat_isa-lrat lrat_isa-trim lrat-trim-lrat lrat-trim-trim
DATA           := $(addprefix log-,$(addsuffix /data,$(CONFIGS)))

data: log-cadical/data .WAIT $(DATA)
	awk 'FNR==1 && NR!=1 { next } { print }' $^ > $@

log-%/data: benchmarks-cadical
	../bin/parallel.sh benchmarks-$* log-$*

benchmarks-cadical: make_benchmarks.sh aiger certificates
	# ./make_benchmarks.sh
	# ../bin/pack.py --time 600 data benchmarks-*

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
