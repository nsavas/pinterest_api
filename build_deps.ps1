# Zip the common/ package for deployment to Glue via --extra-py-files.
#
# Usage:
#   .\build_deps.ps1
#   aws s3 cp pinterest_common.zip s3://<your-bucket>/pinterest_common.zip
#
# Then set, on each of the three jobs in jobs/:
#   --extra-py-files s3://<your-bucket>/pinterest_common.zip
#
# The zip's root must contain the `common` folder itself (not its contents
# directly) so that `import common.auth` etc. resolves once Glue adds the zip
# to sys.path.

$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $MyInvocation.MyCommand.Path
$zipPath = Join-Path $root "pinterest_common.zip"

if (Test-Path $zipPath) {
    Remove-Item $zipPath -Force
}

Compress-Archive -Path (Join-Path $root "common") -DestinationPath $zipPath

Write-Host "Built $zipPath"
