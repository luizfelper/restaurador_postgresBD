@echo off
setlocal
set "SCRIPT=%~dp0restaurador_legiao.py"

if exist "%LOCALAPPDATA%\Programs\Python\Python312\pythonw.exe" (
    start "" "%LOCALAPPDATA%\Programs\Python\Python312\pythonw.exe" "%SCRIPT%"
    exit /b 0
)

where pyw >nul 2>nul
if %errorlevel%==0 (
    start "" pyw -3 "%SCRIPT%"
    exit /b 0
)

where pythonw >nul 2>nul
if %errorlevel%==0 (
    start "" pythonw "%SCRIPT%"
    exit /b 0
)

echo Python nao encontrado.
echo Instale o Python 3.10 ou superior em https://www.python.org/downloads/
echo Marque a opcao "Add python.exe to PATH" durante a instalacao.
pause
