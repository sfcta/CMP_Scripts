@echo off
REM Set the path to the TOML file as the first argument
SET TOML_FILE_PATH=%1

REM Execute Python scripts within the conda environment
CALL uv run %0\..\sf_inrix_xd_monthly.py "%TOML_FILE_PATH%"
CALL uv run Y:\warehouse\scripts\cmprt\aggregate_hourly_expanded_month.py
CALL uv run Y:\warehouse\scripts\cmprt\aggregate_weekly_expanded_month.py
CALL uv run Y:\warehouse\scripts\cmprt\import_cmprt_weekly_expanded.py
CALL uv run Y:\warehouse\scripts\cmprt\import_cmprt_hourly_expanded.py
CALL uv run Y:\warehouse\scripts\cmprt\create_cmpaggregate_view_rt_weekly_expanded.py
CALL uv run Y:\warehouse\scripts\cmprt\create_cmpaggregate_view_rt_hourly_expanded.py
