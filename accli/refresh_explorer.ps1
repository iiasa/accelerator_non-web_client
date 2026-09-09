$ErrorActionPreference = "SilentlyContinue"
$mountPoint = $args[0]
if (-not $mountPoint) { $mountPoint = "W:" }
$mountClean = $mountPoint.TrimEnd("\/")

$shell = New-Object -ComObject Shell.Application
foreach ($win in $shell.Windows()) {
    $url = $win.LocationURL
    if ($url -and ($url -like "*$mountClean*" -or $win.Path -like "*$mountClean*")) {
        $win.Refresh()
    }
}
