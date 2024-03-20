@echo off
REM Set the path to the TOML file as the first argument
SET TOML_FILE_PATH=%1

REM Create the temporary conda environment
CALL conda create --name tmp_monthly_congestion_update_env -c conda-forge python=3.11 -y

REM Install pip in the environment
CALL conda run --name tmp_monthly_congestion_update_env conda install pip -y

REM Install requirements using pip
CALL conda run --name tmp_monthly_congestion_update_env python -m pip install -r Q:\repos\CMP_Scripts\speed\inrix\process_cmp_in_virtual_env\requirements.txt

REM Execute Python scripts within the conda environment
CALL conda run --name tmp_monthly_congestion_update_env python Q:\repos\CMP_Scripts\speed\inrix\sf_inrix_xd_realtime.py "%TOML_FILE_PATH%"
CALL conda run --name tmp_monthly_congestion_update_env python Y:\warehouse\scripts\cmprt\aggregate_hourly_expanded_month.py
CALL conda run --name tmp_monthly_congestion_update_env python Y:\warehouse\scripts\cmprt\aggregate_weekly_expanded_month.py
CALL conda run --name tmp_monthly_congestion_update_env python Y:\warehouse\scripts\cmprt\import_cmprt_weekly_expanded.py
CALL conda run --name tmp_monthly_congestion_update_env python Y:\warehouse\scripts\cmprt\import_cmprt_hourly_expanded.py
CALL conda run --name tmp_monthly_congestion_update_env python Y:\warehouse\scripts\cmprt\create_cmpaggregate_view_rt_weekly_expanded.py
CALL conda run --name tmp_monthly_congestion_update_env python Y:\warehouse\scripts\cmprt\create_cmpaggregate_view_rt_hourly_expanded.py

REM Deactivate and remove the temporary conda environment
CALL conda env remove --name tmp_monthly_congestion_update_env -y
