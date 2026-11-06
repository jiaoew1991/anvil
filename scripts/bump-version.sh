#!/usr/bin/env bash
# Sets the release version in Cargo.toml, pyproject.toml and Cargo.lock.
# Merge the change through a pull request, then tag that commit on main
# with v<version> to release it:
#
#   scripts/bump-version.sh 0.3.0
#   # ...pull request merged...
#   git tag v0.3.0 origin/main && git push origin v0.3.0
set -euo pipefail

if [ $# -ne 1 ] || ! [[ $1 =~ ^[0-9]+\.[0-9]+\.[0-9]+([-.][0-9A-Za-z.]+)?$ ]]; then
  echo "usage: $0 <version>, e.g. $0 0.3.0" >&2
  exit 2
fi
version=$1
cd "$(dirname "$0")/.."

# Only the package's own version line: the first `version = ` in each file.
for manifest in Cargo.toml pyproject.toml; do
  awk -v version="$version" '
    !done && /^version = "/ { print "version = \"" version "\""; done = 1; next }
    { print }
  ' "$manifest" > "$manifest.tmp"
  mv "$manifest.tmp" "$manifest"
done
cargo update --workspace --offline --quiet

git --no-pager diff --stat -- Cargo.toml pyproject.toml Cargo.lock
