@echo off
setlocal enabledelayedexpansion

echo ==========================================================
echo  Better Day Energy Server Starter
echo ==========================================================
echo.

echo Checking for any processes on port 8500...
set FOUND=0

for /f "tokens=5" %%a in ('netstat -ano ^| findstr :8500') do (
    set FOUND=1
    echo Killing process ID %%a ...
    taskkill /PID %%a /F >nul 2>&1
)

if !FOUND! equ 0 (
    echo No processes found on port 8500. Proceeding...
)

echo.
echo Activating virtual environment...
call .venv\Scripts\activate.bat

echo.
echo Starting FastAPI server on port 8500...
start cmd /k "uvicorn main:app --reload --port 8500"

timeout /t 2 >nul

echo.
echo Opening Swagger Docs in your browser...
start http://127.0.0.1:8500/docs

echo.
echo All done! Server running separately. This window will now close.
timeout /t 3 >nul
exit
