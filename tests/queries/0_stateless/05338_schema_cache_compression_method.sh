#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

user_files_path=$($CLICKHOUSE_CLIENT -q "SELECT _path FROM file('nonexistent_probe', 'One') SETTINGS schema_inference_use_cache_for_file=0" 2>&1 | sed -n "s/.*in file '\(.*\)nonexistent_probe'.*/\1/p" | head -n1)
if [ -z "$user_files_path" ]; then
    user_files_path="${CLICKHOUSE_USER_FILES:-/var/lib/clickhouse/user_files/}"
fi

gz_file="${CLICKHOUSE_TEST_UNIQUE_NAME}_gz.raw"
rev_file="${CLICKHOUSE_TEST_UNIQUE_NAME}_rev.raw"
plain_file="${CLICKHOUSE_TEST_UNIQUE_NAME}_plain.raw"
same_file="${CLICKHOUSE_TEST_UNIQUE_NAME}_same.raw"
match_file="${CLICKHOUSE_TEST_UNIQUE_NAME}_match.csv.gz"

cleanup()
{
    rm -f "${user_files_path}/${gz_file}" \
          "${user_files_path}/${rev_file}" \
          "${user_files_path}/${plain_file}" \
          "${user_files_path}/${same_file}" \
          "${user_files_path}/${match_file}"
}
trap cleanup EXIT

# 1. Create test files
$CLICKHOUSE_CLIENT -q "INSERT INTO FUNCTION file('$gz_file', 'LineAsString', 'line String', 'gzip') SELECT toString(number) FROM numbers(1000) SETTINGS engine_file_truncate_on_insert = 1"
$CLICKHOUSE_CLIENT -q "INSERT INTO FUNCTION file('$rev_file', 'LineAsString', 'line String', 'gzip') SELECT toString(number) FROM numbers(1000) SETTINGS engine_file_truncate_on_insert = 1"
$CLICKHOUSE_CLIENT -q "INSERT INTO FUNCTION file('$plain_file', 'CSV', 'a UInt8, b UInt8') SELECT 1, 2 SETTINGS engine_file_truncate_on_insert = 1"
$CLICKHOUSE_CLIENT -q "INSERT INTO FUNCTION file('$same_file', 'LineAsString', 'line String', 'gzip') SELECT toString(number) FROM numbers(50) SETTINGS engine_file_truncate_on_insert = 1"
$CLICKHOUSE_CLIENT -q "INSERT INTO FUNCTION file('$match_file', 'CSV', 'x UInt64', 'gzip') SELECT number FROM numbers(100) SETTINGS engine_file_truncate_on_insert = 1"

# Ensure file modification timestamp is older than current second for cache registration
while [ "$($CLICKHOUSE_CLIENT -q "SELECT now() > (SELECT max(_time) FROM file('${CLICKHOUSE_TEST_UNIQUE_NAME}_*.raw', 'One'))")" != 1 ]
do
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
$CLICKHOUSE_CLIENT $settings -q "DESCRIBE file('$plain_file', 'CSV', 'auto', 'gzip')" 2>&1 | grep -cm1 "Code: " || true

# Case D: same-mode reuse (repeated explicit gzip)
echo "Case D: same-mode reuse"
$CLICKHOUSE_CLIENT $settings -q "SELECT count() FROM file('$same_file', 'LineAsString', 'line String', 'gzip')"
$CLICKHOUSE_CLIENT $settings -q "SELECT count() FROM file('$same_file', 'LineAsString', 'line String', 'gzip')"

# Case E: URL behavior
echo "Case E: URL behavior"
url_settings="--use_cache_for_count_from_files=1 --optimize_count_from_files=1 --schema_inference_use_cache_for_url=1 --schema_inference_cache_require_modification_time_for_url=0"
url="http://127.0.0.1:${CLICKHOUSE_PORT_HTTP}/?query=SELECT+number+FROM+numbers(50)+FORMAT+CSV"

$CLICKHOUSE_CLIENT $url_settings -q "SELECT count() FROM url('$url', 'CSV', 'x UInt64')"
$CLICKHOUSE_CLIENT $url_settings -q "SELECT count() FROM url('$url', 'CSV', 'x UInt64', 'gzip')" 2>&1 | grep -cm1 "Code: " || true

$CLICKHOUSE_CLIENT $url_settings -q "DESCRIBE url('$url', 'CSV')" > /dev/null
$CLICKHOUSE_CLIENT $url_settings -q "DESCRIBE url('$url', 'CSV', 'auto', 'gzip')" 2>&1 | grep -cm1 "Code: " || true

# Case F: auto versus explicit codec matching effective codec
echo "Case F: auto vs explicit matching"
$CLICKHOUSE_CLIENT $settings -q "SELECT count() FROM file('$match_file', 'CSV', 'x UInt64')"
$CLICKHOUSE_CLIENT $settings -q "SELECT count() FROM file('$match_file', 'CSV', 'x UInt64', 'gzip')"
