Set-Location -Path "$PSScriptRoot\.."
if (Test-Path ".venv\\Scripts\\Activate.ps1") {
    Write-Host "Activating .venv..."
    & .\.venv\Scripts\Activate.ps1
} else {
    Write-Host ".venv not found. Create it with: python -m venv .venv" -ForegroundColor Yellow
}
