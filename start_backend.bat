@echo off
chcp 65001 >nul
echo ========================================
echo   HR Assistant Backend Starting...
echo ========================================

set BACKEND_DIR=%~dp0backendV2
echo [DEBUG] Backend dir: %BACKEND_DIR%

if not exist "%BACKEND_DIR%" (
    echo [ERROR] Directory not found: %BACKEND_DIR%
    pause
    exit /b 1
)

pushd "%BACKEND_DIR%"
if errorlevel 1 (
    echo [ERROR] Cannot enter directory: %BACKEND_DIR%
    pause
    exit /b 1
)

echo [INFO] Current dir: %CD%
echo [INFO] Installing dependencies via uv...

uv sync

echo [INFO] Starting FastAPI server...
uv run python -m uvicorn app.main:app --reload --host 0.0.0.0 --port 7389
popd
pause
