@echo off
cd /d %~dp0
python aup_updater.py >> logs\update_log.txt 2>&1
