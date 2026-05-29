$certsDir = Join-Path $PSScriptRoot "..\certs"
certutil.exe -addstore -f root (Join-Path $certsDir "dev-minio-ca.crt")
certutil.exe -addstore -f root (Join-Path $certsDir "dev-backend.crt")
Write-Host "Certificates successfully imported via certutil!" -ForegroundColor Green
