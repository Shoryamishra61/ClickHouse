#!/usr/bin/env bash
# Tags: long, distributed, no-fasttest
# no-fasttest: the `PCO` codec needs the Rust part of the build
# `PCO` is gated by `enable_pco_codec` and, independently, cannot compress untyped spill data at all.
# A remote shard spilling a deserialized plan must receive the initiator's opt-in for the gated codec,
# so that it fails with the same typed-only error as a local spill rather than reporting that the
# session did not enable the codec.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

SETTINGS="
    serialize_query_plan = 1,
    prefer_localhost_replica = 0,
    max_bytes_before_external_group_by = '100K',
    max_bytes_ratio_before_external_group_by = 0,
    group_by_two_level_threshold = '100K',
    group_by_two_level_threshold_bytes = '50M',
    max_memory_usage = '2G'"

query()
{
    $CLICKHOUSE_CLIENT -q "
        SELECT number, count() FROM remote('127.0.0.{1,2}', numbers(2_000_000)) GROUP BY number
        SETTINGS $SETTINGS, $1
        FORMAT Null" 2>&1
}

echo "without the opt-in"
query "temporary_files_codec = 'PCO'" | grep -o -m1 -e 'the session did not enable it' -e 'requires a column type'

echo "with the opt-in"
OUTPUT=$(query "enable_pco_codec = 1, temporary_files_codec = 'PCO'")
echo "$OUTPUT" | grep -o -m1 -e 'the session did not enable it' -e 'requires a column type'
echo "$OUTPUT" | grep -c 'the session did not enable it'
