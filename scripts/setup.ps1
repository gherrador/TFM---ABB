$ErrorActionPreference = "Stop"

function Get-ContainerVariable {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Name
    )

    $value = docker exec tfm-postgres printenv $Name

    if ($LASTEXITCODE -ne 0) {
        throw "No se pudo leer $Name desde el contenedor PostgreSQL."
    }

    $cleanValue = ($value -join "").Trim()

    if ([string]::IsNullOrWhiteSpace($cleanValue)) {
        throw "La variable $Name esta vacia en el contenedor PostgreSQL."
    }

    return $cleanValue
}

Write-Host "============================================"
Write-Host " TFM ABB - Setup reproducible"
Write-Host "============================================"

Write-Host "`n[1/5] Levantando infraestructura..."
docker compose up -d

if ($LASTEXITCODE -ne 0) {
    throw "No se pudo levantar la infraestructura Docker."
}

Write-Host "`nLeyendo configuracion PostgreSQL..."
$PostgresUser = Get-ContainerVariable -Name "POSTGRES_USER"
$PostgresDatabase = Get-ContainerVariable -Name "POSTGRES_DB"

Write-Host "  Usuario: $PostgresUser"
Write-Host "  Base de datos: $PostgresDatabase"

Write-Host "`n[2/5] Esperando PostgreSQL..."
while ($true) {
    docker exec tfm-postgres pg_isready `
        -U $PostgresUser `
        -d $PostgresDatabase *> $null

    if ($LASTEXITCODE -eq 0) {
        break
    }

    Write-Host "  PostgreSQL todavia no esta listo..."
    Start-Sleep -Seconds 3
}

Write-Host "  PostgreSQL disponible."

Write-Host "`n[3/5] Esperando Kafka..."
while ($true) {
    docker exec tfm-kafka kafka-topics `
        --bootstrap-server kafka:29092 `
        --list *> $null

    if ($LASTEXITCODE -eq 0) {
        break
    }

    Write-Host "  Kafka todavia no esta listo..."
    Start-Sleep -Seconds 3
}

Write-Host "  Kafka disponible."

Write-Host "`n[4/5] Ejecutando pipeline batch historico..."
docker compose --profile batch run --rm pipeline

if ($LASTEXITCODE -ne 0) {
    throw "El pipeline batch fallo."
}

Write-Host "`n[5/5] Verificando baseline unificado..."
docker exec tfm-postgres psql `
    -U $PostgresUser `
    -d $PostgresDatabase `
    -c "SELECT SUM(event_count) AS total_eventos FROM analytics.vw_process_daily_kpis;"

if ($LASTEXITCODE -ne 0) {
    throw "No se pudo verificar el baseline en PostgreSQL."
}

Write-Host "`n============================================"
Write-Host " Setup completado."
Write-Host "============================================"

Write-Host "`nSiguiente paso:"
Write-Host "  .\scripts\demo_streaming.ps1"