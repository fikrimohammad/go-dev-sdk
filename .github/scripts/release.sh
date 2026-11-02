#!/usr/bin/env bash
# Releases every module in the repo at one version.
#
# Usage: .github/scripts/release.sh <version> [--dry-run]
#
# Checks, in order: the version shape; each module's major against its module
# path; that every sibling require is <version>; that no <module>/<version> tag
# exists locally or on origin; and that proxy.golang.org does not already serve
# the version. Without --dry-run it tags HEAD for every module and pushes all
# tags in one atomic push. Nothing is tagged unless every check passes.
#
# Environment overrides, used by release_test.sh:
#   GOPROXY_URL    module proxy base URL (default https://proxy.golang.org)
#   MODULE_PREFIX  module path prefix of the repo (default github.com/fikrimohammad/go-servicekit/)

set -euo pipefail

GOPROXY_URL=${GOPROXY_URL:-https://proxy.golang.org}
MODULE_PREFIX=${MODULE_PREFIX:-github.com/fikrimohammad/go-servicekit/}

usage() {
  echo "usage: release.sh <version> [--dry-run]" >&2
  exit 2
}

# fail records a check failure and keeps going, so every problem is reported.
errors=0
fail() {
  echo "::error::$*"
  errors=$((errors + 1))
}

die() {
  echo "::error::$*"
  exit 1
}

# require_lines prints "path version" for each require in a go.mod. It skips
# replace, exclude, and other blocks.
require_lines() {
  awk '
    { sub(/\/\/.*/, "") }
    $1 == ")" { block = ""; next }
    NF == 2 && $2 == "(" { block = $1; next }
    block == "require" && NF >= 2 { print $1, $2; next }
    $1 == "require" && NF >= 3 { print $2, $3 }
  ' "$1"
}

[[ $# -ge 1 && $# -le 2 ]] || usage
version=$1
dry_run=false
if [[ $# -eq 2 ]]; then
  [[ $2 == --dry-run ]] || usage
  dry_run=true
fi

semver_re='^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-[0-9A-Za-z.-]+)?$'
[[ $version =~ $semver_re ]] || die "$version is not vMAJOR.MINOR.PATCH (an optional -prerelease suffix is allowed)"
major=${BASH_REMATCH[1]}

cd "$(git rev-parse --show-toplevel)"

modules=()
for dir in */; do
  dir=${dir%/}
  [[ $dir != vendor && -f $dir/go.mod ]] || continue
  modules+=("$dir")
done
(( ${#modules[@]} > 0 )) || die "no module directories with a go.mod found"

echo "Checking $version for ${#modules[@]} modules"

# 1. Module path against the directory and the major version.
declare -A module_path=()
for dir in "${modules[@]}"; do
  path=$(awk '/^module[[:space:]]/ { print $2; exit }' "$dir/go.mod")
  if [[ -z $path ]]; then
    fail "$dir/go.mod has no module line"
    continue
  fi
  module_path[$dir]=$path

  base=$path
  path_major=""
  if [[ $path =~ ^(.+)/v([0-9]+)$ ]]; then
    base=${BASH_REMATCH[1]}
    path_major=${BASH_REMATCH[2]}
  fi
  [[ $base == "$MODULE_PREFIX$dir" ]] \
    || fail "$dir/go.mod: module path $path should be $MODULE_PREFIX$dir (plus /vN for major 2+)"
  if [[ -n $path_major ]]; then
    [[ $major == "$path_major" ]] || fail "$path needs a v$path_major.x.y version, not $version"
  elif (( major >= 2 )); then
    fail "$version needs the module path to end in /v$major (it is $path)"
  fi
done

# 2. Every sibling require must be the release version.
for dir in "${modules[@]}"; do
  [[ -n ${module_path[$dir]:-} ]] || continue
  while read -r req_path req_version; do
    [[ $req_path == "$MODULE_PREFIX"* ]] || continue
    [[ $req_version == "$version" ]] \
      || fail "$dir/go.mod requires $req_path $req_version; want $version"
  done < <(require_lines "$dir/go.mod")
done

# 3. No tag and no proxy version for the release yet.
remote_tags=$(git ls-remote --tags origin) || die "cannot list tags on origin"
for dir in "${modules[@]}"; do
  path=${module_path[$dir]:-}
  [[ -n $path ]] || continue
  tag="$dir/$version"

  if git rev-parse -q --verify "refs/tags/$tag" >/dev/null; then
    fail "tag $tag already exists locally"
  fi
  if awk -v ref="refs/tags/$tag" '$2 == ref { found = 1 } END { exit !found }' <<<"$remote_tags"; then
    fail "tag $tag already exists on origin"
  fi

  code=$(curl -s -o /dev/null -w '%{http_code}' "$GOPROXY_URL/$path/@v/$version.info") || code=000
  case $code in
    200) fail "$path@$version is already in $GOPROXY_URL; pick a new version" ;;
    404 | 410) ;;
    *) fail "cannot check $path@$version on $GOPROXY_URL (HTTP $code)" ;;
  esac
done

branch=$(git rev-parse --abbrev-ref HEAD)
if [[ $dry_run == false && $branch != main && ${GITHUB_REF:-} != refs/heads/main ]]; then
  fail "a real release must run on main; HEAD is on $branch"
fi

if (( errors > 0 )); then
  die "$errors check(s) failed; no tags were created"
fi

echo "All checks passed. Tags for $version:"
for dir in "${modules[@]}"; do
  printf '  %-14s %-58s %s\n' "$dir" "${module_path[$dir]}" "$dir/$version"
done

if [[ $dry_run == true ]]; then
  echo "Dry run: no tags created."
  exit 0
fi

tags=()
for dir in "${modules[@]}"; do
  tag="$dir/$version"
  git tag -a "$tag" -m "Release $dir $version"
  tags+=("$tag")
done

if ! git push --atomic origin "${tags[@]}"; then
  git tag -d "${tags[@]}" >/dev/null
  die "push of ${#tags[@]} tags failed; local tags were removed and nothing was pushed"
fi
echo "Pushed ${#tags[@]} tags for $version at $(git rev-parse --short HEAD)."
