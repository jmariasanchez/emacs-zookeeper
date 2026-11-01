#!/usr/bin/env bash
# Runs the zk-core and zk-bridge integration suites against every ZooKeeper
# distribution found under ~/.cache/zk (or the directories given as args) and
# prints a compatibility summary.
#
#   scripts/compat-matrix.sh                       # all cached versions
#   scripts/compat-matrix.sh ~/zk/apache-zookeeper-3.8.4-bin
set -u
cd "$(dirname "$0")/.."
[ -f env.sh ] && . ./env.sh

if [ $# -gt 0 ]; then
  dists=("$@")
else
  dists=()
  for d in "$HOME"/.cache/zk/apache-zookeeper-*-bin; do
    [ -d "$d/lib" ] && dists+=("$d")
  done
fi

cargo build -q --tests -p zk-core -p zk-bridge -p zk-cache -p zk-recipes || exit 1

for dist in "${dists[@]}"; do
  version=$(basename "$dist" | sed -e 's/^apache-zookeeper-//' -e 's/-bin$//')
  for suite in "zk-core:integration" "zk-bridge:bridge" "zk-cache:" "zk-recipes:"; do
    crate=${suite%%:*}
    target=${suite#*:}
    args=(-q -p "$crate")
    [ -n "$target" ] && args+=(--test "$target")
    out=$(ZK_TESTS_REQUIRED=1 ZOOKEEPER_HOME="$dist" cargo test "${args[@]}" -- --test-threads 4 2>&1)
    summary=$(printf '%s\n' "$out" | grep -E '^test result' | awk '{p+=$4; f+=$6} END {printf "%d passed, %d failed", p, f}')
    failed=$(printf '%s\n' "$out" | grep -E '^test .* FAILED$' | sed -e 's/^test //' -e 's/ \.\.\. FAILED$//' | tr '\n' ' ')
    printf '%-8s %-12s %s %s\n' "$version" "$crate" "$summary" "${failed:+[$failed]}"
  done
done
