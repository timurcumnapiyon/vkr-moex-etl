param(
    [string]$BackupDir = (Join-Path $PSScriptRoot "..\\backups")
)

$ErrorActionPreference = "Stop"

New-Item -ItemType Directory -Force -Path $BackupDir | Out-Null

$timestamp = Get-Date -Format "yyyyMMdd_HHmmss"

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
        -e PGPASSWORD=postgres `
        -v "${BackupDir}:/backup" `
        postgres:16 `
        sh -c "pg_dump -h $Host -U postgres -p $Port -d $DbName > /backup/$fileName"

    Write-Host "Uploading to MinIO: $BucketPath/$fileName"
    docker run --rm --network moex_net `
        -v "${BackupDir}:/backup" `
        --entrypoint /bin/sh `
        minio/mc -c "mc alias set local http://minio:9000 minioadmin minioadmin >/dev/null && mc cp /backup/$fileName local/moex-backup/$BucketPath/"

    Write-Host "OK: $hostPath"
}

Backup-Db -DbName "pg_raw" -Host "pg_raw" -Port "5432" -FilePrefix "pg_raw" -BucketPath "pg_raw/dumps"
Backup-Db -DbName "pg_dwh" -Host "pg_dwh" -Port "5432" -FilePrefix "pg_dwh" -BucketPath "pg_dwh/dumps"

Write-Host "All backups completed."
