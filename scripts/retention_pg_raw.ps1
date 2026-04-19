param(
    [int]$RetentionDays = 90
)

$ErrorActionPreference = "Stop"

$cutoff = (Get-Date).ToUniversalTime().AddDays(-$RetentionDays).ToString("yyyy-MM-dd HH:mm:ss+00")

Write-Host "PG_RAW retention cleanup. Deleting rows older than $cutoff (UTC)."

$sql = @"
DELETE FROM bars_stocks_moex WHERE datetime < '$cutoff'::timestamptz;
DELETE FROM bars_stocks_alor WHERE datetime < '$cutoff'::timestamptz;
DELETE FROM bars_stocks_moex_fallback_1m WHERE datetime < '$cutoff'::timestamptz;
"@

docker exec pg_raw psql -U postgres -d pg_raw -c "$sql"

Write-Host "Retention cleanup done."
