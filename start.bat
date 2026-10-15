@echo off
chcp 65001 >nul
cd /d "%~dp0"
where node >nul 2>nul
if %errorlevel%==0 goto RUN_NODE
if exist "D:\node\node.exe" goto RUN_D
echo.
echo Node.js not found. Please install Node.js (https://nodejs.org) or edit this file.
echo.
pause
exit /b

:RUN_NODE
node server.js
pause
exit /b

:RUN_D
"D:\node\node.exe" server.js
pause
