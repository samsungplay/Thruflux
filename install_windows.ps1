#Requires -Version 5.1
$ErrorActionPreference = "Stop"

$AppExe     = "thru.exe"
$UrlX64     = "https://github.com/samsungplay/Thruflux-C-/releases/download/v18/thru_windows.exe"
$InstallDir = Join-Path $env:LOCALAPPDATA "Thruflux\bin"
$Target     = Join-Path $InstallDir $AppExe

function Info([string]$m){ Write-Host ("[thruflux] " + $m) -ForegroundColor Cyan }
function Warn([string]$m){ Write-Host ("[thruflux] " + $m) -ForegroundColor Yellow }
function Fail([string]$m){ Write-Host ("[thruflux] " + $m) -ForegroundColor Red; exit 1 }

if ($env:PROCESSOR_ARCHITECTURE -ne "AMD64") { Fail "Windows x64 (AMD64) only." }

try { [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12 } catch {}

Info "Install dir: $InstallDir"
New-Item -ItemType Directory -Force -Path $InstallDir | Out-Null

$tmp = Join-Path $env:TEMP ("thru-" + [Guid]::NewGuid().ToString() + ".exe")

function Download-File([string]$url, [string]$out) {
  Remove-Item -Force $out -ErrorAction SilentlyContinue | Out-Null

  # 1) BITS (most reliable on Windows; supports resume)
  if (Get-Command Start-BitsTransfer -ErrorAction SilentlyContinue) {
    try {
      Info "Downloading (BITS)..."
      Start-BitsTransfer -Source $url -Destination $out -TransferType Download -ErrorAction Stop
      if (Test-Path $out) { return }
    } catch {
      Warn "BITS failed: $($_.Exception.Message)"
    }
  }

  # 2) curl.exe (Windows 10/11)
  $curl = Get-Command curl.exe -ErrorAction SilentlyContinue
  if ($curl) {
    try {
      Info "Downloading (curl)..."
      & $curl.Source "-fL" "--retry" "5" "--retry-delay" "1" "--connect-timeout" "15" "--max-time" "0" "-o" $out $url
      if ($LASTEXITCODE -eq 0 -and (Test-Path $out)) { return }
      Warn "curl failed with exit code $LASTEXITCODE"
    } catch {
      Warn "curl failed: $($_.Exception.Message)"
    }
  }

  # 3) Invoke-WebRequest with retries (last resort)
  Info "Downloading (PowerShell)..."
  for ($i=1; $i -le 5; $i++) {
    try {
      Invoke-WebRequest -Uri $url -OutFile $out -UseBasicParsing -TimeoutSec 60
      return
    } catch {
      Warn "Attempt $i/5 failed: $($_.Exception.Message)"
      Start-Sleep -Seconds ([Math]::Min(8, $i * 2))
    }
  }

  throw "Download failed using all methods."
}

try {
  Download-File $UrlX64 $tmp
} catch {
  Remove-Item -Force $tmp -ErrorAction SilentlyContinue
  Fail $_.Exception.Message
}

if (-not (Test-Path $tmp) -or ((Get-Item $tmp).Length -lt 1024)) {
  Remove-Item -Force $tmp -ErrorAction SilentlyContinue
  Fail "Download looks invalid. Check: $UrlX64"
}

Info "Installing..."
Copy-Item -Force $tmp $Target
Remove-Item -Force $tmp -ErrorAction SilentlyContinue

function Add-ToUserPath([string]$dir) {
  $dirNorm = $dir.TrimEnd('\')
  $current = [Environment]::GetEnvironmentVariable("Path", "User")
  if ([string]::IsNullOrWhiteSpace($current)) { $current = "" }

  $parts = $current -split ';' | ForEach-Object { $_.Trim() } | Where-Object { $_ -ne "" }

  foreach ($p in $parts) {
    if ($p.TrimEnd('\') -ieq $dirNorm) {
      Info "User PATH already contains install dir."
      return
    }
  }

  $newPath = (($parts + $dirNorm) -join ';')
  [Environment]::SetEnvironmentVariable("Path", $newPath, "User")
  Warn "Added to User PATH. Restart your terminal to pick it up."
}

Add-ToUserPath $InstallDir

Info "Installed: $Target"
Info "Run: thru --help"
Info "Re-run this installer any time to update."
