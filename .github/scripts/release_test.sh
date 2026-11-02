#!/usr/bin/env bash
# Tests release.sh --dry-run against throwaway repos, without network access.
# Each case builds a repo with modules a and b (b requires a), points
# MODULE_PREFIX at a fake prefix, and serves a fake proxy from a local HTTP
# server. Usage: bash .github/scripts/release_test.sh

set -euo pipefail

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

echo "$pass passed, $failed failed"
(( failed == 0 ))
