@REM
@REM kyt_process.cmd Copyright (C) 2020 You-Cast on Earth, Moon and Mars 2020
@REM This file is part of com.castsoftware.uc.kyt extension
@REM which is released under GNU GENERAL PUBLIC LICENS v3, Version 3, 29 June 2007.
@REM See file LICENCE or go to https://www.gnu.org/licenses/ for full license details.
@REM
@ECHO OFF

TITLE %~n0
%~d0
PUSHD %~p0
CD ..\kyt

SET V_SCRIPT_PY=runner.py
SET V_CONFIG_JSON=%~dp0try.config.json
SET V_OPTIONS=process

SET V_CMD=python %V_SCRIPT_PY% %V_OPTIONS% %V_CONFIG_JSON%

:L_LOOP
ECHO.^>^>^>^>^>^> %V_CMD%
%V_CMD%

POPD

:L_END
IF /I [%1] NEQ [-NoPause] PAUSE

:L_END_NOPAUSE