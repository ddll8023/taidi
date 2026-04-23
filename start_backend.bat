@echo off
chcp 65001 >nul
echo ========================================
echo   HR Assistant Backend Starting...
echo ========================================

set BACKEND_DIR=%~dp0backend
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
echo [INFO] Activating virtual environment...

if not exist ".venv\Scripts\activate.bat" (
    echo [ERROR] Virtual environment not found: .venv\Scripts\activate.bat
    echo [INFO] Please run: cd backend && python -m venv .venv
    pause
    exit /b 1
)

call .venv\Scripts\activate

echo [INFO] Starting FastAPI server...
uvicorn app.main:app --reload --host 0.0.0.0 --port 7389

popd
pause
