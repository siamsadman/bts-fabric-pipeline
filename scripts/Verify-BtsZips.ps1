<#
.SYNOPSIS
    Validates the local BTS On-Time Performance monthly zip archives before Fabric ingestion.

.DESCRIPTION
    Checks, for the expected range Jan 2020 - May 2026 (77 months):
      - every expected month is present (filename month is NOT zero-padded)
      - no file is under the minimum size
      - each file is a structurally valid zip (central directory readable)
      - each archive contains exactly one CSV, and reports its uncompressed size
    Also lists any unexpected or unparseable files in the folder - including
    2026_6, which is deliberately held back for the incremental pipeline demo.

.PARAMETER DeepCheck
    Additionally streams every entry to force CRC validation. Catches silent
    corruption that the central-directory check misses. Slow: decompresses ~2.3 GB.

.EXAMPLE
    .\Verify-BtsZips.ps1
    .\Verify-BtsZips.ps1 -DeepCheck -ReportCsv .\zip_validation.csv
#>

[CmdletBinding()]
param(
    [string]$Root       = 'L:\Python_Env\Fabric_Portfolio\RAW_zips',
    [double]$MinSizeMB  = 1,
    [datetime]$Start    = '2020-01-01',
    [datetime]$End      = '2026-05-01',
    [switch]$DeepCheck,
    [string]$ReportCsv
)

$ErrorActionPreference = 'Stop'
Add-Type -AssemblyName System.IO.Compression.FileSystem

$prefix = 'On_Time_Reporting_Carrier_On_Time_Performance_1987_present'

if (-not (Test-Path -LiteralPath $Root)) { throw "Folder not found: $Root" }

# --- index what is actually on disk, keyed "yyyy_M" -------------------------
$pattern    = '^{0}_(\d{{4}})_(\d{{1,2}})\.zip$' -f [regex]::Escape($prefix)
$onDisk     = @{}
$unparsable = New-Object System.Collections.Generic.List[object]

Get-ChildItem -LiteralPath $Root -Filter '*.zip' -File | ForEach-Object {
    if ($_.Name -match $pattern) {
        $onDisk['{0}_{1}' -f $Matches[1], [int]$Matches[2]] = $_
    }
    else {
        $unparsable.Add($_)
    }
}

# --- walk the expected range ------------------------------------------------
$minBytes = $MinSizeMB * 1MB
$report   = New-Object System.Collections.Generic.List[object]
$cursor   = $Start

while ($cursor -le $End) {

    $key    = '{0}_{1}' -f $cursor.Year, $cursor.Month
    $file   = $onDisk[$key]
    $status = 'OK'
    $detail = ''
    $entry  = ''
    $rawMB  = $null
    $sizeMB = $null

    if (-not $file) {
        $status = 'MISSING'
        $detail = "expected {0}_{1}.zip" -f $prefix, $key
    }
    else {
        $sizeMB = [math]::Round($file.Length / 1MB, 2)

        if ($file.Length -lt $minBytes) {
            $status = 'TOO SMALL'
            $detail = "$sizeMB MB - likely an HTML error page saved as .zip"
        }
        else {
            try {
                $zip = [System.IO.Compression.ZipFile]::OpenRead($file.FullName)
                try {
                    $csv = @($zip.Entries | Where-Object { $_.Name -match '\.csv$' })

                    if ($csv.Count -ne 1) {
                        $status = 'BAD CONTENT'
                        $detail = "$($csv.Count) CSV entries found (expected 1)"
                    }
                    else {
                        $entry = $csv[0].Name
                        $rawMB = [math]::Round($csv[0].Length / 1MB, 1)
                    }

                    if ($DeepCheck -and $status -eq 'OK') {
                        $buffer = New-Object byte[] 1048576
                        foreach ($e in $zip.Entries) {
                            $stream = $e.Open()
                            try   { while ($stream.Read($buffer, 0, $buffer.Length) -gt 0) { } }
                            finally { $stream.Dispose() }
                        }
                    }
                }
                finally { $zip.Dispose() }
            }
            catch {
                $status = 'CORRUPT'
                $detail = $_.Exception.Message
            }
        }
    }

    $row = [pscustomobject]@{
        Period      = '{0:0000}-{1:00}' -f $cursor.Year, $cursor.Month
        FileName    = if ($file) { $file.Name } else { '' }
        ZipMB       = $sizeMB
        CsvEntry    = $entry
        CsvMB       = $rawMB
        Status      = $status
        Detail      = $detail
    }
    $report.Add($row)

    $onDisk.Remove($key)   # leaves only unexpected periods behind
    $cursor = $cursor.AddMonths(1)
}

# --- output -----------------------------------------------------------------
$expectedCount = $report.Count
$problems      = @($report | Where-Object Status -ne 'OK')

Write-Host ''
Write-Host "Folder            : $Root"
Write-Host "Expected months   : $expectedCount  ($($Start.ToString('yyyy-MM')) to $($End.ToString('yyyy-MM')))"
Write-Host "Valid             : $($expectedCount - $problems.Count)"
Write-Host "Problems          : $($problems.Count)"
Write-Host ("Total size        : {0} GB" -f [math]::Round((($report | Measure-Object ZipMB -Sum).Sum / 1024), 2))
Write-Host "Deep CRC check    : $(if ($DeepCheck) { 'yes' } else { 'no (run with -DeepCheck)' })"
Write-Host ''

if ($problems.Count) {
    Write-Host 'PROBLEMS' -ForegroundColor Red
    $problems | Format-Table Period, Status, ZipMB, Detail -AutoSize
}
else {
    Write-Host 'All expected months present and structurally valid.' -ForegroundColor Green
}

if ($onDisk.Count) {
    Write-Host ''
    Write-Host 'Files outside the expected range:' -ForegroundColor Yellow
    $onDisk.GetEnumerator() | Sort-Object Key | ForEach-Object {
        $note = if ($_.Key -eq '2026_6') { '  <-- held back for the incremental demo; do not backfill' } else { '' }
        Write-Host ("  {0} ({1} MB){2}" -f $_.Value.Name, [math]::Round($_.Value.Length / 1MB, 2), $note)
    }
}

if ($unparsable.Count) {
    Write-Host ''
    Write-Host 'Zip files whose names do not match the BTS pattern:' -ForegroundColor Yellow
    $unparsable | ForEach-Object { Write-Host "  $($_.Name)" }
}

# size outliers - a month far from the median is worth a look even if "valid"
$sizes = @($report | Where-Object { $_.ZipMB } | Select-Object -ExpandProperty ZipMB | Sort-Object)
if ($sizes.Count -gt 4) {
    $median   = $sizes[[int]($sizes.Count / 2)]
    $outliers = @($report | Where-Object { $_.ZipMB -and ($_.ZipMB -lt $median * 0.5 -or $_.ZipMB -gt $median * 2) })
    if ($outliers.Count) {
        Write-Host ''
        Write-Host "Size outliers (median $median MB):" -ForegroundColor Yellow
        $outliers | Format-Table Period, ZipMB, CsvMB, FileName -AutoSize
    }
}

if ($ReportCsv) {
    $report | Export-Csv -LiteralPath $ReportCsv -NoTypeInformation -Encoding UTF8
    Write-Host ''
    Write-Host "Full report written to $ReportCsv"
}

# non-zero exit so this can gate a later step if you script the sequence
if ($problems.Count) { exit 1 }
