<#
.SYNOPSIS
  Pull WarMap zone data using a reader-tier API key.

.EXAMPLE
  $env:WARMAP_KEY = "<your-key>"
  .\share-map.ps1 -List
  .\share-map.ps1 -All
  .\share-map.ps1 Skov_Cerrigar Step_South PIT_Cave_Coast

.NOTES
  Default server is http://87.99.138.184:30100.  Override via $env:WARMAP_SERVER.
  Output dir is .\zones (override via $env:WARMAP_OUT).
#>

param(
    [Parameter(ValueFromRemainingArguments)]
    [string[]]$Zones,
    [switch]$List,
    [switch]$All
)

$Server = if ($env:WARMAP_SERVER) { $env:WARMAP_SERVER } else { 'http://87.99.138.184:30100' }
$Key    = $env:WARMAP_KEY
$Out    = if ($env:WARMAP_OUT)    { $env:WARMAP_OUT }    else { '.\zones' }

if (-not $Key) {
    Write-Error 'Set $env:WARMAP_KEY to your reader-tier API key.'
    exit 2
}

if (-not (Test-Path $Out)) { New-Item -ItemType Directory -Path $Out | Out-Null }

$Headers = @{ 'X-WarMap-Key' = $Key }

function Get-ZoneList {
    (Invoke-RestMethod -Headers $Headers -Uri "$Server/zones").zones
}

function Get-Zone {
    param([string]$ZoneKey)
    $target = Join-Path $Out "$ZoneKey.json"
    try {
        # -OutFile + -Headers + automatic gzip decoding
        Invoke-WebRequest -Headers $Headers -Uri "$Server/zones/$ZoneKey" `
                          -OutFile $target -ErrorAction Stop | Out-Null
        Write-Host "+ $ZoneKey"
    } catch {
        Write-Warning "  failed: $ZoneKey -- $($_.Exception.Message)"
    }
}

if ($List) {
    Get-ZoneList
    return
}

if ($All) {
    foreach ($z in (Get-ZoneList)) { Get-Zone $z }
} elseif ($Zones -and $Zones.Count -gt 0) {
    foreach ($z in $Zones) { Get-Zone $z }
} else {
    Write-Host "usage: .\share-map.ps1 [-List | -All | <zone-key> ...]"
    exit 2
}

Write-Host "done -> $Out"
