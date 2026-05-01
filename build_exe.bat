@echo off
setlocal
title Build DungeonCrawler Loot Browser EXE
set "SCRIPT_DIR=%~dp0"
set "BUNDLED_PY=%USERPROFILE%\.cache\codex-runtimes\codex-primary-runtime\dependencies\python\python.exe"
set "PYTHONUTF8=1"
pushd "%SCRIPT_DIR%"

if exist "%BUNDLED_PY%" (
    set "PY=%BUNDLED_PY%"
    goto :build
)

where python >nul 2>nul
if %errorlevel%==0 (
    set "PY=python"
    goto :build
)

where py >nul 2>nul
if %errorlevel%==0 (
    set "PY=py -3"
    goto :build
)

echo Could not find Python 3.10+.
pause
exit /b 1

:build
%PY% -m PyInstaller --version >nul 2>nul
if not "%ERRORLEVEL%"=="0" (
    echo PyInstaller is not installed for this Python.
    echo Run: %PY% -m pip install pyinstaller
    echo Then run this build script again.
    pause
    exit /b 1
)

if exist "%SCRIPT_DIR%loot_spawn_cache.pkl.gz" (
    %PY% "%SCRIPT_DIR%make_bundle_cache.py"
    if not "%ERRORLEVEL%"=="0" (
        echo Could not prepare bundled cache.
        pause
        exit /b 1
    )
    %PY% -m PyInstaller ^
      --noconfirm ^
      --clean ^
      --onefile ^
      --name "DungeonCrawler Loot Browser" ^
      --add-data "%SCRIPT_DIR%bundle_cache\loot_spawn_cache.pkl.gz;." ^
      "%SCRIPT_DIR%loot_spawn_web.py"
) else (
    %PY% -m PyInstaller ^
      --noconfirm ^
      --clean ^
      --onefile ^
      --name "DungeonCrawler Loot Browser" ^
      "%SCRIPT_DIR%loot_spawn_web.py"
)

if not "%ERRORLEVEL%"=="0" (
    echo Build failed.
    pause
    exit /b 1
)

echo.
echo Built EXE:
echo %SCRIPT_DIR%dist\DungeonCrawler Loot Browser.exe
echo.
echo If loot_spawn_cache.pkl.gz existed, it was bundled into the EXE.
pause
popd
endlocal
