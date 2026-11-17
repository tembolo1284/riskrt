#!/usr/bin/env bash
# riskrt build driver. One command per thing you want to do:
#
#   ./build.sh                 same as build
#   ./build.sh build           compile every crate, test and bench target
#   ./build.sh test            build, then run the whole test suite
#   ./build.sh check           the full gate: fmt, build, test, clippy -D warnings
#   ./build.sh flake [N]       run the threaded crates' tests N times (default 5)
#   ./build.sh run [args...]   run the CLI demo in release mode
#   ./build.sh fmt             format the workspace in place
#   ./build.sh clean
#
# CI runs `./build.sh check`. Anything that passes here passes there.

set -euo pipefail
cd "$(dirname "$0")"

FQ="${FERRUMQUANT_DIR:-../ferrumquant}"
# Crates with threads in their tests; the ones worth running repeatedly.
THREADED=(riskrt-market riskrt-curves riskrt-sched riskrt-agg riskrt-engine riskrt-grpc)

step() { printf '\n==> %s\n' "$*"; }
die()  { printf 'build.sh: %s\n' "$*" >&2; exit 1; }

need_ferrumquant() {
    [[ -f "$FQ/crates/fq-rates/Cargo.toml" ]] || die \
        "ferrumquant not found at $FQ (clone https://github.com/tembolo1284/ferrumquant next to this repo, or set FERRUMQUANT_DIR)"
}

do_fmt_check() { step "cargo fmt --check";   cargo fmt --all -- --check; }
do_fmt()       { step "cargo fmt";           cargo fmt --all; }
do_build()     { step "cargo build";         cargo build --workspace --all-targets; }
do_test()      { step "cargo test";          cargo test --workspace; }
do_clippy()    { step "cargo clippy";        cargo clippy --workspace --all-targets -- -D warnings; }

do_flake() {
    local n="${1:-5}"
    local pkgs=()
    for p in "${THREADED[@]}"; do pkgs+=(-p "$p"); done
    step "flake: ${THREADED[*]} x$n"
    for ((i = 1; i <= n; i++)); do
        printf '  run %d/%d\n' "$i" "$n"
        cargo test -q "${pkgs[@]}" 2>&1 | grep -E "FAILED|panicked" && die "flaky failure on run $i"
    done
    echo "  no failures in $n runs"
}

cmd="${1:-build}"
[[ $# -gt 0 ]] && shift

need_ferrumquant
case "$cmd" in
    build) do_build ;;
    test)  do_build; do_test ;;
    check) do_fmt_check; do_build; do_test; do_clippy; step "check passed" ;;
    flake) do_flake "${1:-5}" ;;
    run)   step "cargo run -p riskrt-cli --release -- $*"; cargo run -p riskrt-cli --release -- "$@" ;;
    fmt)   do_fmt ;;
    clean) step "cargo clean"; cargo clean ;;
    -h|--help|help) sed -n '2,13p' "$0" ;;
    *) die "unknown command '$cmd' (try: build, test, check, flake, run, fmt, clean)" ;;
esac
