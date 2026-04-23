@echo off
chcp 65001 >nul
echo ========================================
echo   HR Assistant Frontend Starting...
echo ========================================

set FRONTEND_DIR=%~dp0frontend
echo [DEBUG] Frontend dir: %FRONTEND_DIR%

if not exist "%FRONTEND_DIR%" (
    echo [ERROR] Directory not found: %FRONTEND_DIR%
    pause
    exit /b 1
)

pushd "%FRONTEND_DIR%"
if errorlevel 1 (
    echo [ERROR] Cannot enter directory: %FRONTEND_DIR%
    pause
    exit /b 1
)

echo [INFO] Current dir: %CD%
echo [INFO] Starting Vite dev server...

npm run dev

popd
pause
