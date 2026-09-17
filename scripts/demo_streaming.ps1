param(
    [int]$Count = 100,
    [double]$Interval = 0.1
)

$ErrorActionPreference = "Stop"

if ($Count -lt 0) {
    throw "Count debe ser mayor o igual que 0."
}

if ($Interval -lt 0) {
    throw "Interval debe ser mayor o igual que 0."
}

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

$PostgresUser = Get-ContainerVariable -Name "POSTGRES_USER"
$PostgresDatabase = Get-ContainerVariable -Name "POSTGRES_DB"

function Get-TotalEvents {
    $output = docker exec tfm-postgres psql `
        -U $PostgresUser `
        -d $PostgresDatabase `
        -t `
        -A `
        -c "SELECT COALESCE(SUM(event_count), 0) FROM analytics.vw_process_daily_kpis;"

    if ($LASTEXITCODE -ne 0) {
        throw "No se pudo consultar PostgreSQL."
    }

    $numericValue = $output |
        ForEach-Object { "$_".Trim() } |
        Where-Object { $_ -match '^\d+$' } |
        Select-Object -Last 1

    if ($null -eq $numericValue) {
        $received = ($output -join " | ").Trim()
        throw "PostgreSQL no devolvio un total numerico. Salida recibida: $received"
    }

    return [int64]$numericValue
}

Write-Host "============================================"
Write-Host " TFM ABB - Demo Streaming"
Write-Host "============================================"

Write-Host "`nConfiguracion PostgreSQL:"
Write-Host "  Usuario: $PostgresUser"
Write-Host "  Base de datos: $PostgresDatabase"

Write-Host "`n[1/4] Total antes del streaming..."
$baseline = Get-TotalEvents
$expected = $baseline + $Count

Write-Host "  Baseline: $baseline"
Write-Host "  Eventos a generar: $Count"
Write-Host "  Total esperado: $expected"

Write-Host "`n[2/4] Generando $Count eventos..."
docker compose --profile producer run --rm producer `
    --count $Count `
    --interval $Interval

if ($LASTEXITCODE -ne 0) {
    throw "El productor Kafka fallo."
}

Write-Host "`n[3/4] Esperando a Spark/PostgreSQL..."
$current = $baseline

for ($attempt = 1; $attempt -le 30; $attempt++) {
    $current = Get-TotalEvents

    Write-Host "  Intento ${attempt}/30: $current / $expected"

    if ($current -ge $expected) {
        break
    }

    Start-Sleep -Seconds 2
}

if ($current -lt $expected) {
    throw "Timeout esperando el procesamiento streaming. Total actual: $current; esperado: $expected."
}

Write-Host "`n[4/4] Validacion final..."
$final = Get-TotalEvents

if ($final -eq $expected) {
    Write-Host "PASS: $baseline + $Count = $final"
}
else {
    Write-Warning "Se esperaban $expected eventos, pero se obtuvieron $final. Puede haber eventos concurrentes."
}

Write-Host "`nAhora abre Power BI y ejecuta: Inicio -> Actualizar"