#!/usr/bin/env bash
# Tests release.sh --dry-run against throwaway repos, without network access.
# Each case builds a repo with modules a and b (b requires a), points
# MODULE_PREFIX at a fake prefix, and serves a fake proxy from a local HTTP
# server. Usage: bash .github/scripts/release_test.sh

set -euo pipefail

# Real runs check GITHUB_REF; the tests set it per case.
unset GITHUB_REF

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
release=$script_dir/release.sh
prefix=example.com/fake/

work=$(mktemp -d)
server_pid=""
cleanup() {
  if [[ -n $server_pid ]]; then
    kill "$server_pid" 2>/dev/null || true
  fi
  rm -rf "$work"
}
trap cleanup EXIT

# Serve the fake proxy from $work/proxy. Missing files get 404, as the real
# proxy does for an unknown version.
proxy_dir=$work/proxy
mkdir -p "$proxy_dir"
port=$(python3 -c 'import socket; s = socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1])')
python3 -m http.server --bind 127.0.0.1 --directory "$proxy_dir" "$port" >/dev/null 2>&1 &
server_pid=$!
proxy_url=http://127.0.0.1:$port
for _ in $(seq 50); do
  if curl -s -o /dev/null "$proxy_url/"; then
    break
  fi
  sleep 0.1
done

# new_repo <name> <version>: repo with modules a and b, where b requires a at
# <version>. Its origin is a bare repo, so git ls-remote works.
new_repo() {
  local dir=$work/$1 version=$2
  mkdir -p "$dir/a" "$dir/b"
  printf 'module %sa\n\ngo 1.26\n' "$prefix" >"$dir/a/go.mod"
  printf 'module %sb\n\ngo 1.26\n\nrequire %sa %s\n' "$prefix" "$prefix" "$version" >"$dir/b/go.mod"
  git -C "$dir" init -q -b main
  git -C "$dir" config user.name test
  git -C "$dir" config user.email test@example.com
  git -C "$dir" add -A
  git -C "$dir" commit -q -m init
  git init -q --bare "$work/$1-origin.git"
  git -C "$dir" remote add origin "$work/$1-origin.git"
  git -C "$dir" push -q origin main
}

# run_release <repo> <args...>: sets rc and out.
run_release() {
  local dir=$1
  shift
  if out=$(cd "$dir" && GOPROXY_URL=$proxy_url MODULE_PREFIX=$prefix "$release" "$@" 2>&1); then
    rc=0
  else
    rc=$?
  fi
}

pass=0
failed=0

expect_pass() {
  if (( rc == 0 )); then
    echo "PASS $1"
    pass=$((pass + 1))
  else
    echo "FAIL $1: exit $rc, want 0"
    sed 's/^/    /' <<<"$out"
    failed=$((failed + 1))
  fi
}

# expect_fail <name> <text>: the run must exit non-zero and print text.
expect_fail() {
  if (( rc != 0 )) && grep -qF -- "$2" <<<"$out"; then
    echo "PASS $1"
    pass=$((pass + 1))
  else
    echo "FAIL $1: exit $rc, want non-zero output containing: $2"
    sed 's/^/    /' <<<"$out"
    failed=$((failed + 1))
  fi
}


# local_tags <repo-dir>: sorted tag names in a repo, space separated.
local_tags() {
  git -C "$1" tag -l | sort | paste -sd' ' -
}

# origin_tags <origin-dir>: sorted tag names on a bare origin, space separated.
origin_tags() {
  git ls-remote --tags "$1" | awk '$2 ~ /^refs\/tags\/[^^]+$/ { sub(/^refs\/tags\//, "", $2); print $2 }' |
    sort | paste -sd' ' -
}

# check <name> <command...>: PASS when the command succeeds.
check() {
  local name=$1
  shift
  if "$@"; then
    echo "PASS $name"
    pass=$((pass + 1))
  else
    echo "FAIL $name"
    failed=$((failed + 1))
  fi
}

# 1. Happy path.
new_repo happy v0.1.0
run_release "$work/happy" v0.1.0 --dry-run
expect_pass "happy path dry run exits 0"

# 2. Sibling require that does not match the release version.
new_repo mismatch v0.0.0
run_release "$work/mismatch" v0.1.0 --dry-run
expect_fail "mismatched sibling require names the module" "b/go.mod requires ${prefix}a v0.0.0; want v0.1.0"

# 3. Bad version string.
run_release "$work/happy" 0.1.0 --dry-run
expect_fail "bad version string" "0.1.0 is not vMAJOR.MINOR.PATCH"

# 4. v2 version on a path without /v2.
new_repo v2path v2.0.0
run_release "$work/v2path" v2.0.0 --dry-run
expect_fail "v2 version on a non-/v2 path" "needs the module path to end in /v2"

# 5. Version already in the proxy.
new_repo proxied v0.1.0
mkdir -p "$proxy_dir/${prefix}a/@v"
touch "$proxy_dir/${prefix}a/@v/v0.1.0.info"
run_release "$work/proxied" v0.1.0 --dry-run
expect_fail "version already in proxy" "${prefix}a@v0.1.0 is already in"
rm -rf "$proxy_dir/example.com"

# 6. Tag already exists locally.
new_repo localtag v0.1.0
git -C "$work/localtag" tag -a a/v0.1.0 -m "existing"
run_release "$work/localtag" v0.1.0 --dry-run
expect_fail "existing local tag" "tag a/v0.1.0 already exists locally"


# 7. Real run tags every module locally and on origin.
new_repo real v0.1.0
run_release "$work/real" v0.1.0
expect_pass "real run exits 0"
check "real run tags every module locally" test "$(local_tags "$work/real")" = "a/v0.1.0 b/v0.1.0"
check "real run pushes every tag to origin" test "$(origin_tags "$work/real-origin.git")" = "a/v0.1.0 b/v0.1.0"

# 8. Origin rejects the push: no tags remain anywhere.
new_repo rejected v0.1.0
printf '#!/bin/sh\necho "rejected by test hook" >&2\nexit 1\n' >"$work/rejected-origin.git/hooks/pre-receive"
chmod +x "$work/rejected-origin.git/hooks/pre-receive"
run_release "$work/rejected" v0.1.0
expect_fail "rejected push exits non-zero" "push of 2 tags failed"
check "rejected push leaves no local tags" test -z "$(local_tags "$work/rejected")"
check "rejected push leaves no origin tags" test -z "$(origin_tags "$work/rejected-origin.git")"

# 9. Real run off main, with and without GITHUB_REF pointing at a feature branch.
new_repo offmain v0.1.0
git -C "$work/offmain" checkout -q -b feature
run_release "$work/offmain" v0.1.0
expect_fail "real run off main fails" "must run on main"
check "off-main run creates no tags" test -z "$(local_tags "$work/offmain")"
GITHUB_REF=refs/heads/feature run_release "$work/offmain" v0.1.0
expect_fail "real run with feature GITHUB_REF fails" "must run on main"
check "feature GITHUB_REF run creates no tags" test -z "$(local_tags "$work/offmain")"

# 10. Tag exists on origin but not locally.
new_repo remotetag v0.1.0
git -C "$work/remotetag" tag -a a/v0.1.0 -m existing
git -C "$work/remotetag" push -q origin a/v0.1.0
git -C "$work/remotetag" tag -d a/v0.1.0 >/dev/null
run_release "$work/remotetag" v0.1.0 --dry-run
expect_fail "tag only on origin" "tag a/v0.1.0 already exists on origin"

# 11. Unpushed commit: a real run must refuse to tag it.
new_repo unpushed v0.1.0
git -C "$work/unpushed" commit -q --allow-empty -m local
run_release "$work/unpushed" v0.1.0
expect_fail "real run with unpushed commit fails" "is not origin/main"
check "unpushed run creates no tags" test -z "$(local_tags "$work/unpushed")"

# 12. Dirty tree: dry runs pass, real runs refuse.
new_repo dirty v0.1.0
echo scratch >"$work/dirty/scratch.txt"
run_release "$work/dirty" v0.1.0 --dry-run
expect_pass "dirty tree passes dry run"
run_release "$work/dirty" v0.1.0
expect_fail "real run with dirty tree fails" "uncommitted changes"
check "dirty run creates no tags" test -z "$(local_tags "$work/dirty")"

echo "$pass passed, $failed failed"
(( failed == 0 ))
