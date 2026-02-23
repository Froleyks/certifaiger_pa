#!/usr/bin/env bash

if [ $# -lt 2 ]; then
	echo "usage: $(basename "$0") <model> <certificate>" >&2
	exit 1
fi

PATH="$(cd -- "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" && pwd -P):$PATH"
model=$(realpath $1)
witness=$(realpath $2)
name=$4
: "${LOG:=$(pwd)/log}"
mkdir -p "$LOG"
log="$LOG/$name"
exec 1>"$log".log 2>"$log".err

work="$(pwd -P)"
sat="$work/sat"
lrat="$work/lrat"
trim="$work/trim"
mkdir -p "$sat" "$lrat" "$trim"

mkdir -p ${TMPDIR:-/tmp}/froleyks
tmp=$(mktemp -d "${TMPDIR:-/tmp}"/froleyks/$(basename "$work")-XXXXXXXX)
trap 'rm -rf "$tmp"; exit' EXIT HUP INT QUIT TERM
cd "$tmp"

t="$(date +%s%N)"
limit certifaiger certifaiger "$model" "$witness" check.aig
limit aigsplit aigsplit -n check.aig

export LIMIT_LOG="$log"
for aig in *.aig; do
	[ -e "$aig" ] || {
		echo "No split AIG files found" >&2
		exit 1
	}
	[ "$aig" = "check.aig" ] && continue
	base="$(basename "$aig" .aig)"
	cnf="$sat/$name-$base.cnf"
	proof="$lrat/$name-$base.lrat"
	trimmed="$trim/$name-$base.lrat"

	limit "$base"_aigtocnf aigtocnf "$aig" "$cnf"
	limit "$base"_cadical cadical --lrat --no-factor --unsat --quiet "$cnf" "$proof"
	limit "$base"_lrat-trim lrat-trim "$proof" "$trimmed"
done
t="$(($(date +%s%N) - t))"
t="$(printf '%d.%09d' "$((t / 1000000000))" "$((t % 1000000000))")"

printf 'RUN_HEAD: name time'
for tool in aigtocnf cadical lrat-trim; do
	for c in Reset Transition Safe Base Inductive; do
		printf " %s-%s" $tool $c
	done
done
echo

printf 'RUN_RESULT: %s %s' "$name" "$t"
{
	for t in aigtocnf cadical lrat-trim; do
		for c in Reset Transition Safe Base Inductive; do
			grep "t_$t_$c" "$log".log | awk '{printf " %s", $2}'
		done
	done
	echo
}
