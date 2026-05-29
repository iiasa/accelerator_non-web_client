<#
.SYNOPSIS
    Reverts all Windows NFS Client and Task Scheduler changes made by setup-nfs.ps1.
.DESCRIPTION
    This script removes the scheduled task, registry keys, ProgramData configs, and disables
    the native Windows NFS Client features. Useful for testing first-time accli setups.
.NOTES
    Must be run in an elevated (Administrator) PowerShell window.
.EXAMPLE
    # Open an Administrator PowerShell window and execute:
    powershell -ExecutionPolicy Bypass -File .\accli\cleanup-nfs.ps1
#>

# Ensure running as Administrator
$isAdmin = ([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
if (-not $isAdmin) {
    Write-Error "This script must be run as Administrator."
    Write-Host "Please open PowerShell as Administrator and run:" -ForegroundColor Yellow
    Write-Host "  powershell -ExecutionPolicy Bypass -File `"$PSCommandPath`"" -ForegroundColor Yellow
    exit 1
}

Write-Host "=== Reverting accli Windows NFS Client Configuration ===" -ForegroundColor Cyan

# 1. Unregister Scheduled Task
Write-Host "Removing accli-mount-nfs Scheduled Task..." -ForegroundColor Yellow
if (Get-Command Unregister-ScheduledTask -ErrorAction SilentlyContinue) {
    Unregister-ScheduledTask -TaskName "accli-mount-nfs" -Confirm:$false -ErrorAction SilentlyContinue | Out-Null
} else {
    schtasks /delete /tn "accli-mount-nfs" /f 2> $null | Out-Null
}
Write-Host "[OK] Scheduled Task removed." -ForegroundColor Green

# 2. Delete Registry Policy Key
Write-Host "Removing EnableLinkedConnections registry policy..." -ForegroundColor Yellow
Remove-ItemProperty -Path "HKLM:\SOFTWARE\Microsoft\Windows\CurrentVersion\Policies\System" -Name "EnableLinkedConnections" -ErrorAction SilentlyContinue | Out-Null
Write-Host "[OK] Registry policy removed." -ForegroundColor Green

# 3. Delete Config Folder & Helpers
Write-Host "Deleting ProgramData configurations..." -ForegroundColor Yellow
Remove-Item -Path "C:\ProgramData\accli" -Recurse -Force -ErrorAction SilentlyContinue | Out-Null
Write-Host "[OK] ProgramData folder removed." -ForegroundColor Green

# 4. Disable Windows NFS Client features
Write-Host "Disabling Windows Client for NFS features..." -ForegroundColor Yellow
if (Get-Command Disable-WindowsOptionalFeature -ErrorAction SilentlyContinue) {
    Disable-WindowsOptionalFeature -Online -FeatureName ServicesForNFS-ClientOnly,ClientForNFS-Infrastructure -NoRestart | Out-Null
}
Write-Host "[OK] NFS Client features disabled." -ForegroundColor Green

Write-Host ""
Write-Host "Reversion completed successfully!" -ForegroundColor Green
Write-Host "⚠ Note: A Windows sign-out/sign-in or system restart is required for changes to take effect." -ForegroundColor Yellow
