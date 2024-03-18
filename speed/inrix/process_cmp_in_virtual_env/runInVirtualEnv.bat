@echo off
REM Create a temporary batch file to hold your commands
SET TEMP_BATCH_FILE=%TEMP%\_temp_conda_commands.bat
SET TOML_FILE_PATH=%1

REM Write commands to the temporary batch file
(
echo @echo off
echo CALL conda create --name temp_cmp_env -c conda-forge python=3.11 -y
echo CALL conda activate temp_cmp_env
echo CALL conda install pip -y
echo CALL python -m pip install -r Q:\repos\CMP_Scripts\speed\inrix\process_cmp_in_virtual_env\requirements.txt
echo CALL python Q:\repos\CMP_Scripts\speed\inrix\sf_inrix_xd_realtime.py "%TOML_FILE_PATH%"
echo CALL python Y:\warehouse\scripts\cmprt\aggregate_hourly_expanded_month.py
echo CALL python Y:\warehouse\scripts\cmprt\aggregate_weekly_expanded_month.py
echo CALL python Y:\warehouse\scripts\cmprt\import_cmprt_weekly_expanded.py
echo CALL python Y:\warehouse\scripts\cmprt\import_cmprt_hourly_expanded.py
echo CALL python Y:\warehouse\scripts\cmprt\create_cmpaggregate_view_rt_weekly_expanded.py
echo CALL python Y:\warehouse\scripts\cmprt\create_cmpaggregate_view_rt_hourly_expanded.py
echo CALL conda deactivate
echo CALL conda env remove --name temp_cmp_env -y
echo DEL "%TEMP_BATCH_FILE%"
) > "%TEMP_BATCH_FILE%"

REM Execute the temporary batch file in a new Anaconda Prompt window
START "Anaconda Prompt" cmd /K "%CONDA_PREFIX%\Scripts\activate.bat & "%TEMP_BATCH_FILE%""
