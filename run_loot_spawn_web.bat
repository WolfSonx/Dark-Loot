@echo off
setlocal
title DungeonCrawler Loot Browser
set "SCRIPT_DIR=%~dp0"
set "EXIT_CODE=1"
set "BUNDLED_PY=%USERPROFILE%\.cache\codex-runtimes\codex-primary-runtime\dependencies\python\python.exe"
set "PYTHONUTF8=1"
set "PYTHONUNBUFFERED=1"
pushd "%SCRIPT_DIR%"

echo DungeonCrawler Loot Browser
echo.
echo Starting local browser app from:
echo %SCRIPT_DIR%
echo.

if exist "%BUNDLED_PY%" (
    "%BUNDLED_PY%" "%SCRIPT_DIR%loot_spawn_web.py" --auto-scan --open %*
    set "EXIT_CODE=%ERRORLEVEL%"
    goto :done
)

where python >nul 2>nul
if %errorlevel%==0 (
    python "%SCRIPT_DIR%loot_spawn_web.py" --auto-scan --open %*
    set "EXIT_CODE=%ERRORLEVEL%"
    goto :done
)

where py >nul 2>nul
if %errorlevel%==0 (
    py -3 "%SCRIPT_DIR%loot_spawn_web.py" --auto-scan --open %*
    set "EXIT_CODE=%ERRORLEVEL%"
    goto :done
)

echo Could not find Python on PATH.
echo Install Python 3.10+ and run this launcher again.
pause

:done
popd
if not "%EXIT_CODE%"=="0" (
    echo.
    echo Loot browser exited with error code %EXIT_CODE%.
    pause
) else (
    echo.
    echo Loot browser closed. If it was already running, use the browser tab at http://127.0.0.1:8765/
    timeout /t 5 >nul
)
endlocal
