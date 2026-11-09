#!/usr/bin/env bash
# Checks that this code reads what every release with a versioned storage
# format wrote: runs the format writer of each such release into a store of
# its own, then this code's verifier against every store. This code's own
# writer is always among them, so the check runs before the first versioned
# release is tagged too. See docs/storage-format.md, "Testing".
#
#   scripts/format-compat.sh
set -euo pipefail
cd "$(dirname "$0")/.."

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
# Runs format-compat test $2 of source tree $1, built into target directory
# $4, on store $3. Each release builds into a directory of its own: cargo
# names a package's test binary by its name and version, not its path, so a
# shared directory would run one tree's binary for another's when the
# versions match, as they do for this code until its release bumps them.
run() {
  (cd "$1" && CARGO_TARGET_DIR=$4 ANVIL_FORMAT_COMPAT_DIR=$3 cargo test --locked --lib \
    "format_compat::tests::$2" -- --ignored --exact)
}

stores=()
# A release has a versioned format exactly when it ships the harness.
for tag in $(git tag --list 'v*' --sort=version:refname); do
  git cat-file -e "$tag:src/format_compat.rs" 2>/dev/null || continue
  mkdir -p "$work/src/$tag"
  git archive "$tag" | tar -x -C "$work/src/$tag"
  echo "::group::writer of $tag"
  run "$work/src/$tag" format_compat_write "$work/store/$tag" "$PWD/target/compat/$tag"
  echo "::endgroup::"
  stores+=("$tag")
done
echo "::group::writer of this code"
run . format_compat_write "$work/store/current" "$PWD/target"
echo "::endgroup::"
stores+=(current)

for store in "${stores[@]}"; do
  echo "::group::verifier on the store of $store"
  run . format_compat_verify "$work/store/$store" "$PWD/target"
  echo "::endgroup::"
done
echo "this code reads the stores of: ${stores[*]}"
