# deploy.ps1 — Windows PowerShell

$VENV_DIR = "venv"
$PYTHON = "python"  # will automatically pick python3.13 if it's in PATH

# Check if python3.13 is available
python3.13 --version > $null 2>&1
if ($LASTEXITCODE -eq 0) {
    $PYTHON = "python3.13"
} else {
    python --version | findstr "3.13" > $null
    if ($LASTEXITCODE -ne 0) {
        Write-Error "Python 3.13 not found! Install it first."
        exit 1
    }
}

Write-Host "Creating virtual environment with Python 3.13..." -ForegroundColor Green
& $PYTHON -m venv $VENV_DIR

Write-Host "Activating virtual environment..." -ForegroundColor Green
& ".\$VENV_DIR\Scripts\Activate.ps1"

Write-Host "Upgrading pip..." -ForegroundColor Green
pip install --upgrade pip

Write-Host "Installing requirements.txt..." -ForegroundColor Green
pip install -r requirements.txt

Write-Host ""
Write-Host "SUCCESS! Virtual environment is ready." -ForegroundColor Cyan
Write-Host "To activate it later, run:"
Write-Host "   .\$VENV_DIR\Scripts\Activate.ps1"