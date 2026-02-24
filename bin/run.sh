#!/usr/bin/env bash

if [ $# -lt 2 ]; then
	echo "usage: $(basename "$0") <model> <certificate>" >&2
	exit 1
fi

PATH="$(cd -- "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" && pwd -P):$PATH"
checker=$1
cnf=$(realpath $2)
proof=$(realpath $3)
name=$5
: "${LOG:=$(pwd)/log}"
mkdir -p "$LOG"
log="$LOG/$name"
exec 1>"$log".log 2>"$log".err

export LIMIT_LOG="$log"
t="$(date +%s%N)"
limit check "$checker" "$cnf" "$proof"
t="$(($(date +%s%N) - t))"
t="$(printf '%d.%09d' "$((t / 1000000000))" "$((t % 1000000000))")"
status=unknown
if grep -q "VERIFIED" "$log".log; then
	status="unsat"
elif rg "out of memory" "$log"*; then
	status="memory"
elif rg "out of time" "$log"*; then
	status="time"
elif ((res == 124)); then
	status="time"
fi

echo RUN_HEAD: name time status
echo RUN_RESULT: $name $t $status
