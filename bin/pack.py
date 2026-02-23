#!/usr/bin/env python3
"""
Optimize @<group-id> annotations in benchmark files based on historical runtimes.

INPUTS
------
1) Data file (first positional argument):
   - Whitespace-separated table with a header line.
   - Must contain columns: run, name, time
   - time is in seconds (float).

2) One or more benchmark files (remaining positional arguments):
   - Each task is a single line ending in:  @<group_id> <unique_name>
   - <unique_name> must match the "name" column in the data.

DEFAULT MODE (no --sub)
-----------------------
For each benchmark file, rewrite it in place:
  - Estimate each task time:
      * prefer (run,name) from data
      * else use max time for that name across other runs
      * else "no data anywhere": keep alone
  - Only tasks with est_time <= 5% of --time are eligible to be combined.
  - Combine eligible tasks into groups up to 80% of --time (bin-packing/FFD).
  - Non-eligible tasks (or no-data tasks) stay as singletons.
  - Reassign group IDs to consecutive 1..N.
  - Reorder so groups are contiguous; groups ordered by descending group size.

SUBSET MODE (--sub)
-------------------
Creates a *subset* benchmark file next to each input benchmark file:
  - Output file name: inserts ".sub" before any extension (e.g., bench -> bench.sub).
  - Picks a subset of tasks expected to finish within SUB_TIME seconds when running
    up to CORES groups concurrently (default CORES=1).
  - Uses conservative estimates for missing data (max across runs; else assumes --time).
  - After selection, it writes the subset with efficient group annotations.
  - In subset mode, group packing for small tasks is additionally capped so that
    each group length is <= SUB_TIME (otherwise finishing within SUB_TIME is impossible).

USAGE
-----
  optimize_groups.py data.txt benchmark-foo benchmark-bar [--time 3600]
  optimize_groups.py data.txt benchmark-foo --sub 600 4

"""

import argparse
import math
import os
import re
import tempfile


TASK_LINE_RE = re.compile(r"^(?P<before>.*)\s+@(?P<gid>\d+)\s+(?P<name>\S+)\s*$")


class TaskLine(object):
    __slots__ = (
        "idx",
        "before",
        "old_gid",
        "name",
        "raw",
        "est_time",
        "has_data",
        "sel_time",
    )

    def __init__(self, idx, before, old_gid, name, raw):
        self.idx = idx
        self.before = before
        self.old_gid = old_gid
        self.name = name
        self.raw = raw

        self.est_time = None     # float seconds, only if has_data True
        self.has_data = False    # True iff we found any time (run-specific or max across runs)
        self.sel_time = None     # float seconds used for --sub selection (est_time or timeout)


def parse_data_file(path):
    """
    Returns:
      times_by_run[run][name] = time (max over duplicates)
      max_time_by_name[name]  = max time over all runs
    """
    times_by_run = {}
    max_time_by_name = {}

    header = None
    idx_run = idx_name = idx_time = -1

    with open(path, "r") as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue

            parts = line.split()
            if header is None:
                header = parts
                if "run" not in header or "name" not in header or "time" not in header:
                    raise ValueError(
                        "{}: header must contain columns 'run', 'name', 'time'. Found: {}".format(
                            path, header
                        )
                    )
                idx_run = header.index("run")
                idx_name = header.index("name")
                idx_time = header.index("time")
                continue

            need = max(idx_run, idx_name, idx_time)
            if len(parts) <= need:
                continue

            run = parts[idx_run]
            name = parts[idx_name]
            time_str = parts[idx_time]

            try:
                t = float(time_str)
            except ValueError:
                continue

            # Treat non-positive or NaN as missing/incomplete
            if not (t > 0.0) or math.isnan(t):
                continue

            per_run = times_by_run.get(run)
            if per_run is None:
                per_run = {}
                times_by_run[run] = per_run

            prev = per_run.get(name)
            if prev is None or t > prev:
                per_run[name] = t

            prev_max = max_time_by_name.get(name)
            if prev_max is None or t > prev_max:
                max_time_by_name[name] = t

    if header is None:
        raise ValueError("{}: no header/data found.".format(path))

    return times_by_run, max_time_by_name


def infer_run_key_from_benchmark_path(bench_path):
    """
    Benchmarks are named like 'run' but with 'benchmark' instead of 'log'.

      benchmark-foo-bar  -> log-foo-bar

    Also strips one extension (e.g., ".txt") from the filename.
    """
    base = os.path.basename(bench_path)
    stem, _ext = os.path.splitext(base)

    if stem.startswith("benchmark-"):
        return "log-" + stem[len("benchmark-") :]
    if stem.startswith("benchmark"):
        return "log" + stem[len("benchmark") :]
    return stem


def read_benchmark_file(path):
    """
    Returns: (raw_lines, tasks, non_task_lines)
      - raw_lines: list of file lines
      - tasks: list of TaskLine
      - non_task_lines: list of (idx, raw_line) for lines that don't match TASK_LINE_RE
    """
    with open(path, "r") as f:
        raw_lines = f.readlines()

    tasks = []
    non_task = []

    for i, raw in enumerate(raw_lines):
        m = TASK_LINE_RE.match(raw.rstrip("\n"))
        if not m:
            non_task.append((i, raw))
            continue

        before = m.group("before")
        old_gid = int(m.group("gid"))
        name = m.group("name")
        tasks.append(TaskLine(i, before, old_gid, name, raw))

    return raw_lines, tasks, non_task


def estimate_times_for_tasks(tasks, run_key, times_by_run, max_time_by_name):
    """
    Populates task.est_time and task.has_data.

    Returns: no_data_anywhere_count
      - count of tasks whose name has no time in any run.
    """
    per_run = times_by_run.get(run_key, {})
    no_data_anywhere = 0

    for t in tasks:
        est = per_run.get(t.name)
        if est is None:
            est = max_time_by_name.get(t.name)

        if est is None or not (est > 0.0) or math.isnan(est):
            t.est_time = None
            t.has_data = False
            no_data_anywhere += 1
        else:
            t.est_time = est
            t.has_data = True

    return no_data_anywhere


def atomic_write(path, lines, copy_mode_from=None):
    """
    Atomically write 'lines' to 'path' using a temp file in the same directory.
    If copy_mode_from is provided and exists, copy its chmod mode.
    """
    abs_path = os.path.abspath(path)
    directory = os.path.dirname(abs_path)

    orig_mode = None
    src = copy_mode_from if copy_mode_from is not None else abs_path
    try:
        st = os.stat(src)
        orig_mode = st.st_mode
    except OSError:
        orig_mode = None

    fd, tmp_path = tempfile.mkstemp(prefix=".tmp.optimize_groups.", dir=directory)
    try:
        with os.fdopen(fd, "w") as tmp:
            tmp.writelines(lines)
        if orig_mode is not None:
            os.chmod(tmp_path, orig_mode)
        os.replace(tmp_path, abs_path)
    finally:
        try:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
        except OSError:
            pass


def pack_short_tasks_ffd(short_tasks, capacity):
    """
    First-Fit Decreasing (FFD) bin packing for eligible "short" tasks.

    Input: list of TaskLine with task.est_time set (float) and <= short_limit.
    Output: list of groups: each is dict {'tasks': [...], 'sum_time': float}
    """
    tasks_sorted = sorted(short_tasks, key=lambda t: (-t.est_time, t.idx))

    groups = []
    for task in tasks_sorted:
        placed = False
        for g in groups:
            if g["sum_time"] + task.est_time <= capacity + 1e-9:
                g["tasks"].append(task)
                g["sum_time"] += task.est_time
                placed = True
                break
        if not placed:
            groups.append({"tasks": [task], "sum_time": task.est_time})
    return groups


def make_groups(tasks, timeout_seconds, group_capacity_seconds):
    """
    Apply the grouping rules to a list of TaskLine.

    Returns: (groups, no_data_anywhere_in_this_tasks_list)
      groups: list of dict {'tasks': [...], 'sum_time': float}
    """
    short_limit = 0.05 * timeout_seconds

    short_tasks = []
    singleton_groups = []
    no_data_anywhere = 0

    for t in tasks:
        if not t.has_data:
            # No data anywhere: keep alone, assume timeout runtime conservatively.
            no_data_anywhere += 1
            singleton_groups.append({"tasks": [t], "sum_time": float(timeout_seconds)})
            continue

        # Known (or imputed) time:
        if t.est_time <= short_limit:
            short_tasks.append(t)
        else:
            singleton_groups.append({"tasks": [t], "sum_time": float(t.est_time)})

    packed = pack_short_tasks_ffd(short_tasks, group_capacity_seconds)

    all_groups = packed + singleton_groups

    # Sort groups:
    #   - primary: more elements first
    #   - secondary: larger estimated time first (helps scheduling / still respects size ordering)
    #   - tertiary: earlier appearance in file
    def group_key(g):
        min_idx = min(tt.idx for tt in g["tasks"]) if g["tasks"] else 10**18
        return (-len(g["tasks"]), -g["sum_time"], min_idx)

    all_groups.sort(key=group_key)

    return all_groups, no_data_anywhere


def render_benchmark(non_task_lines, groups):
    """
    Render a benchmark file:
      - all non-task lines first in original order
      - then groups in current group order
      - within each group, tasks in original file order
      - assigns new group IDs 1..N
    """
    out = []
    for _i, line in sorted(non_task_lines, key=lambda x: x[0]):
        out.append(line)

    gid = 1
    for g in groups:
        for t in sorted(g["tasks"], key=lambda tt: tt.idx):
            out.append(t.before.rstrip() + " @{} {}\n".format(gid, t.name))
        gid += 1
    return out


def schedule_makespan_for_groups(groups, cores):
    """
    Estimate makespan when groups are executed with at most 'cores' groups in parallel.

    We assume groups are started in the *given order* (group id order) subject to 'cores' parallelism:
      - maintain per-core loads
      - assign next group to the core that becomes available first (min load)
    """
    if cores <= 0:
        raise ValueError("cores must be >= 1")

    loads = [0.0] * cores
    for g in groups:
        # assign to least-loaded core (earliest available)
        j = 0
        best = loads[0]
        for k in range(1, cores):
            if loads[k] < best:
                best = loads[k]
                j = k
        loads[j] += float(g["sum_time"])
    return max(loads) if loads else 0.0


def derive_sub_path(bench_path):
    base, ext = os.path.splitext(bench_path)
    return base + "-sub" + ext


def select_subset_tasks(tasks, timeout_seconds, sub_time_seconds, cores):
    """
    Heuristic selection:
      1) define each task's selection time:
           - known -> est_time
           - unknown -> timeout_seconds
      2) filter tasks that individually cannot fit: sel_time > sub_time_seconds
      3) greedily include shortest tasks first under total work budget (sub_time * cores)
      4) group + schedule; if still exceeds sub_time, drop the largest remaining tasks until it fits
    """
    budget = float(sub_time_seconds) * float(cores)

    # Prepare candidate list (sel_time, idx, task)
    cands = []
    for t in tasks:
        if t.has_data:
            t.sel_time = float(t.est_time)
        else:
            t.sel_time = float(timeout_seconds)

        if t.sel_time <= sub_time_seconds + 1e-9:
            cands.append((t.sel_time, t.idx, t))

    cands.sort(key=lambda x: (x[0], x[1]))

    selected = []
    total = 0.0
    for sel_time, _idx, t in cands:
        if total + sel_time <= budget + 1e-9:
            selected.append(t)
            total += sel_time

    # Sort selected by sel_time ascending so we can pop() the largest
    selected.sort(key=lambda t: (t.sel_time, t.idx))

    # Now enforce schedule feasibility under our grouping rules.
    # In subset mode, cap the "short-task packing capacity" by SUB_TIME as well.
    pack_cap = min(0.8 * float(timeout_seconds), float(sub_time_seconds))

    while True:
        if not selected:
            return [], [], 0.0

        groups, _no_data = make_groups(selected, timeout_seconds, pack_cap)
        makespan = schedule_makespan_for_groups(groups, cores)
        if makespan <= sub_time_seconds + 1e-9:
            return selected, groups, makespan

        # Too slow: drop the longest task and retry
        selected.pop()


def main():
    ap = argparse.ArgumentParser(
        description="Optimize benchmark @group-id annotations based on historical runtimes."
    )
    ap.add_argument(
        "data_file",
        help="Historical data file (whitespace-separated) with columns: run name time ...",
    )
    ap.add_argument(
        "benchmark_files",
        nargs="+",
        help="Benchmark files to rewrite (default) or to generate .sub subsets for (--sub).",
    )
    ap.add_argument(
        "--time",
        type=float,
        default=3600.0,
        help="Timeout in seconds (default: 3600). Used for 5%% and 80%% thresholds.",
    )
    ap.add_argument(
        "--sub",
        nargs="+",
        help="Create a subset expected to finish in TIME seconds with optional CORES (default 1): --sub TIME [CORES]",
    )
    args = ap.parse_args()

    if args.time <= 0:
        raise SystemExit("--time must be > 0")

    sub_time = None
    sub_cores = 1
    if args.sub is not None:
        if len(args.sub) == 1:
            sub_time = float(args.sub[0])
            sub_cores = 1
        elif len(args.sub) == 2:
            sub_time = float(args.sub[0])
            sub_cores = int(args.sub[1])
        else:
            raise SystemExit("--sub expects 1 or 2 arguments: --sub TIME [CORES]")

        if sub_time <= 0:
            raise SystemExit("--sub TIME must be > 0")
        if sub_cores <= 0:
            raise SystemExit("--sub CORES must be >= 1")

    times_by_run, max_time_by_name = parse_data_file(args.data_file)

    for bench_path in args.benchmark_files:
        run_key = infer_run_key_from_benchmark_path(bench_path)
        _raw_lines, tasks, non_task = read_benchmark_file(bench_path)

        no_data_total = estimate_times_for_tasks(tasks, run_key, times_by_run, max_time_by_name)

        if sub_time is None:
            # Default mode: rewrite in place
            group_capacity = 0.8 * float(args.time)
            groups, no_data_in_groups = make_groups(tasks, args.time, group_capacity)
            out_lines = render_benchmark(non_task, groups)
            atomic_write(bench_path, out_lines, copy_mode_from=bench_path)
            print("{}: groups={} no_data={}".format(bench_path, len(groups), no_data_in_groups))
        else:
            # Subset mode: write to <file>.sub (do not touch original)
            selected, groups, makespan = select_subset_tasks(tasks, args.time, sub_time, sub_cores)

            # Recompute no-data among selected (for reporting)
            no_data_selected = 0
            for t in selected:
                if not t.has_data:
                    no_data_selected += 1

            out_path = derive_sub_path(bench_path)
            out_lines = render_benchmark(non_task, groups)
            atomic_write(out_path, out_lines, copy_mode_from=bench_path)

            print(
                "{}: sub->{} sel={}/{} groups={} est={:.3f}s cores={} nodata={}/{}".format(
                    bench_path,
                    out_path,
                    len(selected),
                    len(tasks),
                    len(groups),
                    makespan,
                    sub_cores,
                    no_data_selected,
                    no_data_total,
                )
            )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
