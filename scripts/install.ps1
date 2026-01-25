$Repo = "iiasa/accli"
$BinaryName = "accli.exe"

# Fetch latest version
Write-Host "Fetching latest version of accli..."
$LatestRelease = (Invoke-RestMethod -Uri "https://api.github.com/repos/$Repo/releases/latest").tag_name

if (-not $LatestRelease) {
    Write-Error "Could not find latest release for $Repo."
    exit 1
}

Write-Host "Installing accli $LatestRelease for Windows..."

# Download URL
$Url = "https://github.com/$Repo/releases/download/$LatestRelease/accli-windows.exe"

# Determine install location
$InstallDir = "$env:LOCALAPPDATA\accli"
if (-not (Test-Path $InstallDir)) {
    New-Item -ItemType Directory -Path $InstallDir | Out-Null
}

$DestPath = Join-Path $InstallDir $BinaryName

# Download
Invoke-WebRequest -Uri $Url -OutFile $DestPath

Write-Host "Successfully installed accli to $DestPath"

# Add to PATH for current session
$env:PATH += ";$InstallDir"

Write-Host ""
Write-Host "To use accli from any terminal, add the following directory to your PATH environment variable:"
Write-Host "  $InstallDir"
Write-Host ""
Write-Host "You can do this by running:"
Write-Host "  [Environment]::SetEnvironmentVariable('Path', [Environment]::GetEnvironmentVariable('Path', 'User') + ';$InstallDir', 'User')"
