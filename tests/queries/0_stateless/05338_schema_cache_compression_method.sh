#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

user_files_path="${CLICKHOUSE_USER_FILES:-/var/lib/clickhouse/user_files}"

gz_file="${CLICKHOUSE_TEST_UNIQUE_NAME}_gz.raw"
rev_file="${CLICKHOUSE_TEST_UNIQUE_NAME}_rev.raw"
plain_file="${CLICKHOUSE_TEST_UNIQUE_NAME}_plain.raw"
same_file="${CLICKHOUSE_TEST_UNIQUE_NAME}_same.raw"
match_file="${CLICKHOUSE_TEST_UNIQUE_NAME}_match.csv.gz"

cleanup()
{
    rm -f "${user_files_path}/${CLICKHOUSE_TEST_UNIQUE_NAME}"*
}
trap cleanup EXIT

# 1. Create test files
$CLICKHOUSE_CLIENT -q "INSERT INTO FUNCTION file('$gz_file', 'LineAsString', 'line String', 'gzip') SELECT toString(number) FROM numbers(1000) SETTINGS engine_file_truncate_on_insert = 1"
$CLICKHOUSE_CLIENT -q "INSERT INTO FUNCTION file('$rev_file', 'LineAsString', 'line String', 'gzip') SELECT toString(number) FROM numbers(1000) SETTINGS engine_file_truncate_on_insert = 1"
$CLICKHOUSE_CLIENT -q "INSERT INTO FUNCTION file('$plain_file', 'CSV', 'a UInt8, b UInt8') SELECT 1, 2 SETTINGS engine_file_truncate_on_insert = 1"
$CLICKHOUSE_CLIENT -q "INSERT INTO FUNCTION file('$same_file', 'LineAsString', 'line String', 'gzip') SELECT toString(number) FROM numbers(50) SETTINGS engine_file_truncate_on_insert = 1"
$CLICKHOUSE_CLIENT -q "INSERT INTO FUNCTION file('$match_file', 'CSV', 'x UInt64', 'gzip') SELECT number FROM numbers(100) SETTINGS engine_file_truncate_on_insert = 1"

# Bounded wait to ensure file modification timestamp is older than current second for cache registration
for _ in {1..30}; do
    if [ "$($CLICKHOUSE_CLIENT -q "SELECT now() > (SELECT max(_time) FROM file('${CLICKHOUSE_TEST_UNIQUE_NAME}_*.raw', 'One'))")" = "1" ]; then
        break
    fi
    sleep 0.1
done

settings="--use_cache_for_count_from_files=1 --optimize_count_from_files=1 --schema_inference_use_cache_for_file=1"

# Case A: wrong row count (auto then explicit gzip)
echo "Case A: wrong row count"
$CLICKHOUSE_CLIENT $settings -q "SELECT count() FROM file('$gz_file', 'LineAsString', 'line String')" > /dev/null
$CLICKHOUSE_CLIENT $settings -q "SELECT count() FROM file('$gz_file', 'LineAsString', 'line String', 'gzip')"

# Case B: reverse order (explicit gzip then auto)
echo "Case B: reverse order"
$CLICKHOUSE_CLIENT $settings -q "SELECT count() FROM file('$rev_file', 'LineAsString', 'line String', 'gzip')"
$CLICKHOUSE_CLIENT $settings -q "SELECT count() FROM file('$rev_file', 'LineAsString', 'line String')"

# Case C: schema inference (auto then explicit gzip on plain CSV)
echo "Case C: schema inference"
$CLICKHOUSE_CLIENT $settings -q "DESCRIBE file('$plain_file', 'CSV')" > /dev/null
$CLICKHOUSE_CLIENT $settings -q "DESCRIBE file('$plain_file', 'CSV', 'auto', 'gzip')" 2>&1 | grep -q 'CANNOT_DECOMPRESS' && echo "CANNOT_DECOMPRESS" || echo "UNEXPECTED_SUCCESS"

# Case D: same-mode reuse (repeated explicit gzip)
echo "Case D: same-mode reuse"
$CLICKHOUSE_CLIENT $settings -q "SELECT count() FROM file('$same_file', 'LineAsString', 'line String', 'gzip')"
$CLICKHOUSE_CLIENT $settings -q "SELECT count() FROM file('$same_file', 'LineAsString', 'line String', 'gzip')"

# Case E: URL behavior
echo "Case E: URL behavior"
url_settings="--use_cache_for_count_from_files=1 --optimize_count_from_files=1 --schema_inference_use_cache_for_url=1 --schema_inference_cache_require_modification_time_for_url=0"
url_plain="http://127.0.0.1:${CLICKHOUSE_PORT_HTTP}/?query=SELECT+number+FROM+numbers(50)+FORMAT+CSV"
url_gzip="http://127.0.0.1:${CLICKHOUSE_PORT_HTTP}/?query=SELECT+number+FROM+numbers(50)+FORMAT+CSV&enable_http_compression=1"

# Positive URL count with gzip and cache reuse
$CLICKHOUSE_CLIENT $url_settings -q "SELECT count() FROM url('$url_gzip', 'CSV', 'x UInt64', 'gzip', headers('Accept-Encoding' = 'gzip'))"
$CLICKHOUSE_CLIENT $url_settings -q "SELECT count() FROM url('$url_gzip', 'CSV', 'x UInt64', 'gzip', headers('Accept-Encoding' = 'gzip'))"

# Positive URL schema inference with gzip
$CLICKHOUSE_CLIENT $url_settings -q "DESCRIBE url('$url_gzip', 'CSV', 'auto', 'gzip', headers('Accept-Encoding' = 'gzip')) FORMAT TSV" | cut -f1,2

# Incompatible decompression fails deterministically
$CLICKHOUSE_CLIENT $url_settings -q "SELECT count() FROM url('$url_plain', 'CSV', 'x UInt64', 'gzip')" 2>&1 | grep -q 'CANNOT_DECOMPRESS' && echo "CANNOT_DECOMPRESS" || echo "UNEXPECTED_SUCCESS"
$CLICKHOUSE_CLIENT $url_settings -q "DESCRIBE url('$url_plain', 'CSV', 'auto', 'gzip')" 2>&1 | grep -q 'CANNOT_DECOMPRESS' && echo "CANNOT_DECOMPRESS" || echo "UNEXPECTED_SUCCESS"

# Case F: auto versus explicit codec matching effective codec
echo "Case F: auto vs explicit matching"
$CLICKHOUSE_CLIENT $settings -q "SELECT count() FROM file('$match_file', 'CSV', 'x UInt64')"
$CLICKHOUSE_CLIENT $settings -q "SELECT count() FROM file('$match_file', 'CSV', 'x UInt64', 'gzip')"

# Case G: system.schema_inference_cache distinction
echo "Case G: schema cache system table"
$CLICKHOUSE_CLIENT -q "SELECT count() > 0 FROM system.schema_inference_cache WHERE compression_method = 'gzip'"
$CLICKHOUSE_CLIENT -q "SELECT count() > 0 FROM system.schema_inference_cache WHERE compression_method = ''"
