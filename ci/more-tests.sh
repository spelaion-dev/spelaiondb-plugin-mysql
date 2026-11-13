#!/usr/bin/env bash
# What `cargo test` does not reach: the patched mysql_async under vendor/ is a crate of its
# own. This is its regression test for the change SpelaionDB made (vendor/mysql_async/PATCHED.md).
set -euo pipefail
cd "$(dirname "$0")/.."
cargo test --manifest-path vendor/mysql_async/Cargo.toml --lib result_count_tests
