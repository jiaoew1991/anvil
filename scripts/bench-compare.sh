#!/usr/bin/env bash
# Runs the worker-profile bench on this code and on the previous release, on
# the same machine, and fails when this code is slower by more than the
# tolerance. A shared runner's speed swings by close to half from one job to
# the next, so only a comparison within one job is meaningful.
#
# The two builds run alternately, ROUNDS times each, each on a fresh store,
# and their medians are compared, so drift during the job hits both alike.
# The previous release is the newest v* tag that HEAD contains and that is not
# on HEAD itself, so a release is measured against the one before it. Each
# build runs its own bench with the same ANVIL_BENCH_* environment.
#
#   ANVIL_BENCH_KEYS=300000 ANVIL_BENCH_WORKERS=1000 scripts/bench-compare.sh
#
# KEYS_PER_SEC_RATIO_MIN: lowest allowed throughput of this code over the
#   release's (default 0.85).
# ACK_P99_RATIO_MAX: highest allowed ack p99 of this code over the release's;
#   0 (the default) disables the check.
# ROUNDS: runs per build (default 3).
set -euo pipefail
cd "$(dirname "$0")/.."

# A release's PyO3 refuses to build against a Python newer than it knows.
# These tests run no Python, so let every tree build against any.
export PYO3_USE_ABI3_FORWARD_COMPATIBILITY=1

keys_per_sec_ratio_min=${KEYS_PER_SEC_RATIO_MIN:-0.85}
ack_p99_ratio_max=${ACK_P99_RATIO_MAX:-0}
rounds=${ROUNDS:-3}

head_tags=$(git tag --points-at HEAD --list 'v*')
baseline=""
for tag in $(git tag --merged HEAD --list 'v*' --sort=-version:refname); do
  if ! grep -qxF "$tag" <<<"$head_tags"; then
    baseline=$tag
    break
  fi
done
if [ -z "$baseline" ]; then
  echo "no earlier release to compare with" >&2
  exit 1
fi

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
mkdir "$work/$baseline"
git archive "$baseline" | tar -x -C "$work/$baseline"
# The release builds into a target directory of its own: cargo names a
# package's test binary by its name and version, not its path, so a shared
# directory would run one tree's binary for both when the versions match.
baseline_target=$PWD/target/baseline
(cd "$work/$baseline" && CARGO_TARGET_DIR=$baseline_target cargo test --release --locked --lib --no-run)
cargo test --release --locked --lib --no-run

# Runs the bench of source tree $2, built into target directory $3, once and
# appends "<keys/s> <ack p99 ms>" to $work/$1.
bench() {
  local db_dir
  db_dir=$(mktemp -d "$work/db.XXXXXX")
  echo "=== $1"
  (cd "$2" && CARGO_TARGET_DIR=$3 ANVIL_BENCH_DB="file://$db_dir" \
    cargo test --release --locked --lib bench_worker_profile -- --ignored --nocapture) \
    2>&1 | tee "$work/log"
  sed -nE 's/^process: .* = ([0-9]+) keys\/s .* ack\+ledger p50=[0-9]+ms p99=([0-9]+)ms .*/\1 \2/p' \
    "$work/log" >>"$work/$1.results"
  rm -rf "$db_dir"
}

for round in $(seq "$rounds"); do
  bench "$baseline" "$work/$baseline" "$baseline_target"
  bench current . "$PWD/target"
done

# Prints the median of column $2 of file $1.
median() {
  cut -d' ' -f"$2" "$1" | sort -n | awk '{ v[NR] = $1 } END { print (NR % 2) ? v[(NR + 1) / 2] : (v[NR / 2] + v[NR / 2 + 1]) / 2 }'
}

for build in "$baseline" current; do
  if [ "$(wc -l <"$work/$build.results")" -ne "$rounds" ]; then
    echo "the bench of $build did not finish all $rounds rounds" >&2
    exit 1
  fi
done
baseline_keys_per_sec=$(median "$work/$baseline.results" 1)
current_keys_per_sec=$(median "$work/current.results" 1)
baseline_ack_p99=$(median "$work/$baseline.results" 2)
current_ack_p99=$(median "$work/current.results" 2)
echo "median of $rounds: $baseline $baseline_keys_per_sec keys/s, ack p99 ${baseline_ack_p99}ms;" \
  "current $current_keys_per_sec keys/s, ack p99 ${current_ack_p99}ms"

awk -v base="$baseline_keys_per_sec" -v cur="$current_keys_per_sec" -v min="$keys_per_sec_ratio_min" \
  -v base_p99="$baseline_ack_p99" -v cur_p99="$current_ack_p99" -v max="$ack_p99_ratio_max" '
  BEGIN {
    failed = 0
    ratio = cur / base
    printf "throughput ratio %.2f (min %.2f)\n", ratio, min
    if (ratio < min) { print "throughput regressed against the previous release"; failed = 1 }
    if (max > 0) {
      # A p99 under 1ms rounds to 0; compare from 1ms.
      ratio_p99 = (cur_p99 < 1 ? 1 : cur_p99) / (base_p99 < 1 ? 1 : base_p99)
      printf "ack p99 ratio %.2f (max %.2f)\n", ratio_p99, max
      if (ratio_p99 > max) { print "ack p99 regressed against the previous release"; failed = 1 }
    }
    exit failed
  }'
