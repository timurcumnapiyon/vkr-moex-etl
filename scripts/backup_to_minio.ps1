param(
    [string]$BackupDir = (Join-Path $PSScriptRoot "..\\backups")
)

$ErrorActionPreference = "Stop"

New-Item -ItemType Directory -Force -Path $BackupDir | Out-Null

$timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$PgUser = if ($env:PG_BACKUP_USER) { $env:PG_BACKUP_USER } elseif ($env:PG_RAW_USER) { $env:PG_RAW_USER } else { "postgres" }
$PgPassword = if ($env:PG_BACKUP_PASSWORD) { $env:PG_BACKUP_PASSWORD } elseif ($env:PG_RAW_PASSWORD) { $env:PG_RAW_PASSWORD } else { "postgres" }
$MinioEndpoint = if ($env:MINIO_ENDPOINT) { $env:MINIO_ENDPOINT } else { "http://minio:9000" }
$MinioAccessKey = if ($env:AWS_ACCESS_KEY_ID) { $env:AWS_ACCESS_KEY_ID } else { "" }
$MinioSecretKey = if ($env:AWS_SECRET_ACCESS_KEY) { $env:AWS_SECRET_ACCESS_KEY } else { "" }

if (-not $MinioAccessKey -or -not $MinioSecretKey) {
    throw "Переменные AWS_ACCESS_KEY_ID и AWS_SECRET_ACCESS_KEY должны быть заданы."
}

function Backup-Db {
    param(
        [string]$DbName,
        [string]$Host,
        [string]$Port,
        [string]$FilePrefix,
        [string]$BucketPath
    )

    $fileName = "${FilePrefix}_${timestamp}.sql"
    $hostPath = Join-Path $BackupDir $fileName

    Write-Host "Creating dump: $fileName"
    docker run --rm --network moex_net `
        -e PGPASSWORD=$PgPassword `
        -v "${BackupDir}:/backup" `
        postgres:16 `
        sh -c "pg_dump -h $Host -U $PgUser -p $Port -d $DbName > /backup/$fileName"

    Write-Host "Uploading to MinIO: $BucketPath/$fileName"
    docker run --rm --network moex_net `
        -v "${BackupDir}:/backup" `
        --entrypoint /bin/sh `
        minio/mc -c "mc alias set local $MinioEndpoint $MinioAccessKey $MinioSecretKey >/dev/null && mc cp /backup/$fileName local/moex-backup/$BucketPath/"

    Write-Host "OK: $hostPath"
}

Backup-Db -DbName "pg_raw" -Host "pg_raw" -Port "5432" -FilePrefix "pg_raw" -BucketPath "pg_raw/dumps"
Backup-Db -DbName "pg_dwh" -Host "pg_dwh" -Port "5432" -FilePrefix "pg_dwh" -BucketPath "pg_dwh/dumps"

Write-Host "All backups completed."
