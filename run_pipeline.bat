@echo off

cd /d "C:\Users\HP\Desktop\Crime Trends\crime-trends-monitor"

call .venv\Scripts\activate.bat

python -m etl.flow

pause