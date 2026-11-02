#!/usr/bin/env bash
# Installs every module at <version> from the module proxy, in a fresh module
# outside the repo, then builds an import of one package from each module. The
# proxy can lag behind a tag push, so go get retries. The 5-minute deadline is
# shared across all modules, not reset per module.
#
# Usage: .github/scripts/verify-install.sh <version>
# Environment: GOPROXY_URL (default https://proxy.golang.org).

set -euo pipefail

GOPROXY_URL=${GOPROXY_URL:-https://proxy.golang.org}

die() {
  echo "::error::$*"
  exit 1
}

[[ $# -eq 1 ]] || { echo "usage: verify-install.sh <version>" >&2; exit 2; }
version=$1
[[ $version =~ ^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-[0-9A-Za-z.-]+)?$ ]] \
  || die "$version is not vMAJOR.MINOR.PATCH"

export GOPROXY=$GOPROXY_URL GOFLAGS=-mod=mod GOWORK=off

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)

# has_root_go succeeds when dir directly holds a non-test .go file.
has_root_go() {
  local f
  for f in "$1"/*.go; do
    [[ -e $f && $f != *_test.go ]] && return 0
  done
  return 1
}

# first_subpackage prints the first subdirectory of dir (in sorted order) that
# has a non-test .go file and is importable from outside the module.
first_subpackage() {
  local rel
  while IFS= read -r rel; do
    rel=${rel#./}
    if [[ -n $rel && $rel != . ]]; then
      printf '%s\n' "$rel"
      return 0
    fi
  done < <(cd "$1" && find . -name '*.go' ! -name '*_test.go' \
    -not -path '*/internal/*' -not -path '*/testdata/*' -not -path './vendor/*' |
    sed 's|/[^/]*$||' | sort -u)
  return 1
}

mods=()
imports=()
for dir in "$root"/*/; do
  dir=${dir%/}
  name=$(basename "$dir")
  [[ $name != vendor && -f $dir/go.mod ]] || continue

  modpath=$(awk '/^module[[:space:]]/ { print $2; exit }' "$dir/go.mod")
  if has_root_go "$dir"; then
    pkg=$modpath
  else
    rel=$(first_subpackage "$dir") || die "$name has no importable package with Go files"
    pkg="$modpath/$rel"
  fi
  mods+=("$modpath")
  imports+=("$pkg")
  echo "$modpath -> import $pkg"
done
(( ${#mods[@]} > 0 )) || die "no module directories with a go.mod found"

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
cd "$work"
go mod init verify >/dev/null

deadline=$((SECONDS + 300))
for mod in "${mods[@]}"; do
  until go get "$mod@$version"; do
    if (( SECONDS >= deadline )); then
      die "$mod@$version was not installable from $GOPROXY_URL after 5 minutes"
    fi
    echo "waiting for $GOPROXY_URL to serve $mod@$version; retrying in 15s"
    sleep 15
  done
  echo "installed $mod@$version"
done

{
  echo 'package main'
  echo
  echo 'import ('
  for pkg in "${imports[@]}"; do
    printf '\t_ "%s"\n' "$pkg"
  done
  echo ')'
  echo
  echo 'func main() {}'
} > main.go

go build ./...
echo "Verified: ${#mods[@]} modules install and build at $version."
