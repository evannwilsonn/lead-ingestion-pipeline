@echo off
rem Windows runner. .env is loaded by the script itself.
rem   run_pipeline.bat              normal run, output appended to logs\pipeline.log
rem   run_pipeline.bat --dry-run    print parsed leads to this window, write nothing
rem   run_pipeline.bat --authorize  one-time Gmail sign-in
setlocal
cd /d "%~dp0"
if not exist "venv\Scripts\python.exe" (
  echo venv not found. Run: python -m venv venv ^&^& venv\Scripts\pip install -r requirements.txt
  exit /b 2
)
if "%~1"=="" (
  if not exist logs mkdir logs
  "venv\Scripts\python.exe" lead_pipeline.py >> logs\pipeline.log 2>&1
) else (
  "venv\Scripts\python.exe" lead_pipeline.py %*
)
exit /b %ERRORLEVEL%
