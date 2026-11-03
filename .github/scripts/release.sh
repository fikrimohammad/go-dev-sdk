#!/usr/bin/env bash
# Releases every module in the repo at one version.
#
# Usage: .github/scripts/release.sh <version> [--dry-run | --check]
#
# Checks, in order: the version shape; each module's major against its module
# path; that every sibling require is <version>; that no <module>/<version> tag
# exists locally or on origin; and that proxy.golang.org does not already serve
# the version. Without --dry-run it tags HEAD for every module and pushes all
# tags in one atomic push. Nothing is tagged unless every check passes.
#
# --check runs only the offline checks (version shape, module paths, sibling
# requires). It never reads tags or the proxy, so it can run on every PR, even
# after the version has been released.
#
# Environment overrides, used by release_test.sh:
#   GOPROXY_URL    module proxy base URL (default https://proxy.golang.org)
#   MODULE_PREFIX  module path prefix of the repo (default github.com/fikrimohammad/go-servicekit/)

set -euo pipefail

GOPROXY_URL=${GOPROXY_URL:-https://proxy.golang.org}
MODULE_PREFIX=${MODULE_PREFIX:-github.com/fikrimohammad/go-servicekit/}

usage() {
  echo "usage: release.sh <version> [--dry-run | --check]" >&2
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

# has_remote_tag <tag> <ls-remote output>: succeeds when the tag is listed.
has_remote_tag() {
  awk -v ref="refs/tags/$1" '$2 == ref { found = 1 } END { exit !found }' <<<"$2"
}

# has_listed_version <version> <file>: succeeds when <file> has a line equal
# to <version> (stripping optional CR in CRLF responses).
has_listed_version() {
  awk -v ver="$1" '{ sub(/\r$/, ""); if ($0 == ver) found = 1 } END { exit !found }' "$2"
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

[[ $# -ge 1 ]] || usage
version=$1
shift
dry_run=false
check_only=false
for arg in "$@"; do
  case $arg in
    --dry-run) dry_run=true ;;
    --check) check_only=true ;;
    *) usage ;;
  esac
done
if [[ $dry_run == true && $check_only == true ]]; then
  die "--check and --dry-run are mutually exclusive"
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

if [[ $check_only == true ]]; then
  (( errors == 0 )) || die "$errors consistency check(s) failed for $version"
  echo "Consistency checks passed for $version (${#modules[@]} modules)."
  exit 0
fi

# 3. No tag and no proxy version for the release yet.
# Query /@v/list (max-age=60) rather than /@v/<version>.info: requesting .info
# for an unreleased version causes proxy.golang.org to negative-cache the 404
# for 30 minutes, blocking verify-install.sh right after the tags are pushed.
list_file=$(mktemp)
trap 'rm -f "$list_file"' EXIT
remote_tags=$(git ls-remote --tags origin) || die "cannot list tags on origin"
for dir in "${modules[@]}"; do
  path=${module_path[$dir]:-}
  [[ -n $path ]] || continue
  tag="$dir/$version"

  if git rev-parse -q --verify "refs/tags/$tag" >/dev/null; then
    fail "tag $tag already exists locally"
  fi
  if has_remote_tag "$tag" "$remote_tags"; then
    fail "tag $tag already exists on origin"
  fi

  : >"$list_file"
  code=$(curl -s --max-time 10 --retry 2 -o "$list_file" -w '%{http_code}' "$GOPROXY_URL/$path/@v/list") || code=000
  case $code in
    200)
      if has_listed_version "$version" "$list_file"; then
        fail "$path@$version is already in $GOPROXY_URL; pick a new version"
      fi
      ;;
    404 | 410) ;;
    *) fail "cannot check $path@$version on $GOPROXY_URL (HTTP $code)" ;;
  esac
done

branch=$(git rev-parse --abbrev-ref HEAD)
if [[ $dry_run == false && $branch != main && ${GITHUB_REF:-} != refs/heads/main ]]; then
  fail "a real release must run on main; HEAD is on $branch"
fi

# A real release tags HEAD, so HEAD must be the commit on origin/main and the
# tree must be clean. A dry run only reports, so it skips these checks.
if [[ $dry_run == false ]]; then
  if git fetch -q origin main; then
    head_sha=$(git rev-parse HEAD)
    main_sha=$(git rev-parse origin/main)
    if [[ $head_sha != "$main_sha" ]]; then
      fail "HEAD ${head_sha:0:7} is not origin/main ${main_sha:0:7}; push or pull so they match"
    fi
  else
    fail "cannot fetch origin/main to compare with HEAD"
  fi
  if [[ -n $(git status --porcelain) ]]; then
    fail "working tree has uncommitted changes; commit or stash them before a real release"
  fi
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

# A failed push can still have reached origin (for example, a dropped
# connection after the server accepted it), so ask origin what it has before
# touching the local tags.
if ! git push --atomic origin "${tags[@]}"; then
  if ! remote_now=$(git ls-remote --tags origin); then
    die "push of ${#tags[@]} tags failed and origin could not be checked, so the remote state is unknown. Local tags were kept. Run git ls-remote --tags origin: if the tags exist there, the release happened, so fix forward with a new version; if not, delete the local tags with git tag -d."
  fi
  on_origin=()
  for tag in "${tags[@]}"; do
    if has_remote_tag "$tag" "$remote_now"; then
      on_origin+=("$tag")
    else
      git tag -d "$tag" >/dev/null
    fi
  done
  if (( ${#on_origin[@]} > 0 )); then
    die "push reported failure, but ${on_origin[*]} exist on origin, so the release may have happened. Fix forward with a new version; do not reuse $version."
  fi
  die "push of ${#tags[@]} tags failed; nothing reached origin and the local tags were removed"
fi
echo "Pushed ${#tags[@]} tags for $version at $(git rev-parse --short HEAD)."
