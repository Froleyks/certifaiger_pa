#!/usr/bin/env bash
set -euo pipefail
# Runs benchmark commands in parallel.
# Benchmark files list commands followed by "@group name"
# where name is a unique identifier (checked below) and
# group determines which benchmarks are run sequentially
# (same group) or in parallel (different groups).

benchmarks=${1:-benchmarks}
log=${2:-"log-$(basename "$(pwd)")"}
name=${3:-"$(basename "$(pwd)")"}
CPUS=${CPUS:-1}

bin="$(cd -- "$(dirname -- "$(readlink -f "${BASH_SOURCE[0]}")")" && pwd -P)"
PATH="$bin:$PATH"

[[ -f "$benchmarks" ]] || {
    printf 'error: benchmarks file not found: %s\n' "$benchmarks" >&2
    exit 1
}
# trim leading/trailing whitespace in-place
sed -i -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//' "$benchmarks"

# detect duplicate ids (last whitespace-separated field)
duplicates="$(awk '{
      id=$NF
      if (id in x) {
        if (x[id] != "") print x[id]
        print
        x[id] = ""
      } else {
        x[id] = $0
      }
    }' "$benchmarks" | sort)"
[[ -z "$duplicates" ]] || {
    printf 'error: duplicate ids\n%s\n' "$duplicates" >&2
    exit 1
}

if ! [[ -d "$log" ]]; then
    mkdir -p "$log"
    {
        printf '%s\n' "$(realpath "$benchmarks")"
        printf '%s benchmarks\n' "$(wc -l <"$benchmarks")"
        printf 'TIME: %s\n' "${TIME:-UNLIMITED}"
        printf 'SPACE: %s\n' "${SPACE:-UNLIMITED}"
        printf 'CPUS: %s\n' "$CPUS"
        printf 'hostname: %s\n\n' "$(hostname)"
        printf 'Nils Froleyks\nKU Leuven\n%s\n' "$(date +"%Y-%m-%d %H:%M %Z")"
    } >"$log/Readme"
fi

mkdir -p "$log"
ts="$(date +"%Y%m%dT%H%M%S")"

remaining_benchmarks() {
    # Keep only benchmarks that don't already have a corresponding log.
    local benchmarks="$1"
    local log="$2"
    local remaining="$log/benchmarks-$ts"
    find "$log" -maxdepth 1 -type f -name '*.log' -print 2>/dev/null |
        awk ' FILENAME == ARGV[1] {
            sub(/^.*\//,"",$0)
            sub(/\.log$/,"",$0)
            done[$0]=1
            next
        } {
            id=$NF
            if (!(id in done)) print
        } ' - "$benchmarks" >"$remaining"
    printf '%s\n' "$remaining"
}

completed() {
    local benchmarks="$1"
    if [[ ! -s "$benchmarks" ]]; then
        rm -f -- "$benchmarks"
        # header (first match)
        grep -H -a --text 'RUN_HEAD:' "$log"/*.log |
            sed -n '1p' |
            sed -E 's@.*RUN_HEAD:[[:space:]]*@run @' |
            awk '($1=="run"){print;next}{print "run "$0}' >"$log/data"
        # rows
        grep -H -a --text 'RUN_RESULT:' "$log"/*.log |
            sed 's!/[^/]*RUN_RESULT:!!' >>"$log/data"
        secs="$(awk '{s+=$3} END{printf "%.0f\n", s}' "$log/data")"
        printf '%s completed in %s\n' "$log" "$(date -u -d "@$secs" +'%H:%M:%S')"
        missing=$(grep -L 'RUN_RESULT:' "$log"/*.log | wc -l)
        [[ "$missing" -gt 0 ]] && printf '%s missing %s results\n' "$log" "$missing"
        return 0
    fi
    return 1
}

benchmarks="$(remaining_benchmarks "$benchmarks" "$log")"
completed "$benchmarks" && exit 0

n="$(wc -l <"$benchmarks" | tr -d '[:space:]')"
if [[ "$n" -eq 0 ]]; then
    printf 'Complete %s\n' "$log"
    rm -f "$benchmarks"
    exit 0
fi

# Collect distinct numeric group ids (in first-seen order)
group_ids_file="$log/group-ids-$ts"
awk '
  match($0, / @[0-9]+ /) {
    g = substr($0, RSTART+2, RLENGTH-3) + 0
    if (!(g in seen)) { seen[g]=1; order[++n]=g }
  }
  END { for (i=1;i<=n;i++) print order[i] }
' "$benchmarks" >"$group_ids_file"
groups="$(wc -l <"$group_ids_file" | tr -d "[:space:]")"
array_spec="$(paste -sd, "$group_ids_file")"

LOG="$(cd -- "$log" && pwd -P)"
export LOG
banner="parallel.sh running $n benchmarks in $groups groups with $CPUS cpus each${TIME:+ for ${TIME}s}${SPACE:+ with ${SPACE}MB}"
if command -v sbatch >/dev/null 2>&1; then
    printf "$banner using slurm\n"
    [ -z "${WAIT+x}" ] || echo Waiting for result...
    mkdir -p "$log/slurm"

    max_jobs=500
    already_in_queue="$(squeue -M wice -h -t pending,running -r | wc -l)"
    capacity=$((max_jobs - 5 - already_in_queue))
    ((capacity <= 0)) && {
        printf 'Reached capacity %s\n' "$max_jobs" >&2
        exit 1
    }
    if ((groups > capacity)); then
        echo "Exceeding capacity, $already_in_queue / $max_jobs queued, only submitting $capacity groups"
        keep_ids="$log/group-ids-$ts-keep"
        head -n "$capacity" "$group_ids_file" >"$keep_ids"
        remaining="$log/benchmarks-$ts-slurm"
        awk '
            FNR==NR { keep[$1]=1; next }
            match($0, / @[0-9]+ /) {
                g = substr($0, RSTART+2, RLENGTH-3) + 0
                if (keep[g]) print
                next
            }
            { print }
            ' "$keep_ids" "$benchmarks" >"$remaining"
        benchmarks="$remaining"
        mv "$keep_ids" "$group_ids_file"
        groups="$capacity"
        array_spec="$(paste -sd, "$group_ids_file")"
    fi

    ARRAY="$log/array.sh"
    {
        printf "#!/bin/sh\n"
        printf "set -eu\n"
        printf "%s\n" "export PATH=\"$bin:\$PATH\""
        printf "%s\n" "${TIME:+export TIME=$TIME}"
        printf "%s\n" "${SPACE:+export SPACE=$SPACE}"
        printf "%s\n" "export LOG=$LOG"
        printf "%s\n" "grep -F \" @\${SLURM_ARRAY_TASK_ID:?} \" \"$benchmarks\" | while IFS= read -r line; do bash -c \"\$line\"; done"
    } >"$ARRAY"
    time=$(( ${TIME:-86400} * ${SLACK:-105} / 100))
    sbatch ${SLURM:-} \
        ${WAIT:+--wait} \
        --export=ALL \
        --chdir="$PWD" \
        --time=$time \
        --job-name="$name" \
        --array="$array_spec" \
        --cpus-per-task="$CPUS" \
        --output="$LOG/slurm/$ts-%a.log" \
        --error="$LOG/slurm/$ts-%a.err" \
        --parsable \
        "$ARRAY"

elif command -v xargs >/dev/null 2>&1; then
    printf "$banner using GNU parallel\n"
    cores=$(
	getconf _NPROCESSORS_ONLN 2>/dev/null ||
	    nproc 2>/dev/null ||
	    sysctl -n hw.ncpu 2>/dev/null ||
	    echo 1
	 )
    jobs=$(( cores / CPUS ))
    (( jobs < 1 )) && jobs=1
    awk '{
        g=$(NF-1)
        if (!(g in seen)) { seen[g]=1; order[++n]=g }
     }
     END { for (i=1;i<=n;i++) print order[i] }
    ' "$benchmarks" |
	xargs -r -n 1 -P "$jobs" bash -c '
    benchmarks="$1"
    g="$2"
    awk -v g="$g" "\$(NF-1)==g{print}" "$benchmarks" |
      while IFS= read -r line; do
        bash -c "$line"
      done
  ' _ "$benchmarks"

    benchmarks="$(remaining_benchmarks "$benchmarks" "$log")"
    completed "$benchmarks"
else
    printf "$banner sequentially\n"
    while read -r args; do
        eval "$args"
    done <"$benchmarks"
    benchmarks="$(remaining_benchmarks "$benchmarks" "$log")"
    completed "$benchmarks"
fi
