# Better Day Energy - start_project.ps1
# =======================================

Write-Host "Clearing ports 8010 and 5173..."

$portsToKill = @(8010, 5173)
foreach ($port in $portsToKill) {
    $netstatOutput = netstat -ano | findstr ":$port"
    foreach ($line in $netstatOutput) {
        $parts = $line -split '\s+'
        $procId = $parts[-1]
        if ($procId -match '^\d+$') {
            try {
                Stop-Process -Id $procId -Force -ErrorAction Stop
                Write-Host "Killed process ${procId} on port ${port}"
            } catch {
                Write-Host "Failed to kill process ${procId} on port ${port}: $_"
            }
        }
    }
}

Start-Sleep -Seconds 2
Write-Host "Starting backend (FastAPI on port 8010)..."
Start-Process powershell -ArgumentList "-NoExit", "-Command", "cd bde_project; uvicorn main:app --reload --port 8010"

Start-Sleep -Seconds 2
Write-Host "Starting frontend (React Vite on port 5173)..."
Start-Process powershell -ArgumentList "-NoExit", "-Command", "cd ../bde-frontend; npm run dev"

Start-Sleep -Seconds 5
Write-Host "Opening browser to frontend..."
Start-Process "http://localhost:5173"

Write-Host "✅ Backend and Frontend started! Ready to go. 🚀"
