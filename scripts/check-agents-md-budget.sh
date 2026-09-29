#!/usr/bin/env bash
# AGENTS.md is intentionally concise working guidance, not a second behavior
# contract. Measure the checked-out artifact: lefthook may format Markdown after
# a contributor inspected its working tree.
set -euo pipefail

readonly maximum_bytes=40000

check_file() {
  local file=$1 size over

  if [[ ! -f "$file" ]]; then
    printf '::error::AGENTS guide file is missing: %s\n' "$file" >&2
    return 2
  fi

  size=$(LC_ALL=C wc -c <"$file")
  printf '%s: %s bytes (limit %s)\n' "$file" "$size" "$maximum_bytes"
  if ((size <= maximum_bytes)); then
    return 0
  fi

  over=$((size - maximum_bytes))
  printf '::error file=%s::AGENTS guide exceeds its %s-byte budget by %s bytes\n' \
    "$file" "$maximum_bytes" "$over" >&2
  return 1
}

self_test() (
  local at_limit over_limit accepted rejected status
  local directory
  directory=$(mktemp -d)
  trap 'rm -rf "$directory"' EXIT
  at_limit="$directory/at-limit.md"
  over_limit="$directory/over-limit.md"

  head -c "$maximum_bytes" /dev/zero >"$at_limit"
  head -c "$((maximum_bytes + 1))" /dev/zero >"$over_limit"

  if ! accepted=$(check_file "$at_limit"); then
    printf '::error::budget self-test rejected the exact limit: %s\n' "$accepted" >&2
    return 1
  fi
  if [[ $accepted != *"$maximum_bytes bytes (limit $maximum_bytes)"* ]]; then
    printf '::error::budget self-test did not report the exact-limit measurement\n' >&2
    return 1
  fi

  rejected="$({ check_file "$over_limit"; } 2>&1)" || status=$?
  if [[ ${status:-0} -eq 0 ]]; then
    printf '::error::budget self-test accepted a one-byte overage: %s\n' "$rejected" >&2
    return 1
  fi
  if [[ $rejected != *"$((maximum_bytes + 1)) bytes (limit $maximum_bytes)"* || \
    $rejected != *"exceeds its $maximum_bytes-byte budget by 1 bytes"* ]]; then
    printf '::error::budget self-test did not report the one-byte overage\n' >&2
    return 1
  fi

  printf 'AGENTS.md budget self-test passed: %s accepted, %s rejected\n' \
    "$maximum_bytes" "$((maximum_bytes + 1))"
)

case $# in
  0)
    check_file AGENTS.md
    ;;
  1)
    if [[ $1 == --self-test ]]; then
      self_test
    else
      check_file "$1"
    fi
    ;;
  *)
    printf 'usage: %s [--self-test | path]\n' "$0" >&2
    exit 2
    ;;
esac
