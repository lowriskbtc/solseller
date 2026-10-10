@echo off
rem TRADE BOX in a frameless, always-on-top, see-through window.
rem
rem Chrome forces an address bar onto every window.open popup and ignores
rem location=no, so the dashboard POP OUT button cannot remove it. App mode
rem can: --app opens a real OS window with no address bar, no tab strip and no
rem menu, and unlike Document PiP it is an ordinary window, so it drags and
rem resizes normally.
rem
rem App-mode windows are NOT on top by default - click the browser and the box
rem goes behind it. pin_topmost.ps1 sets WS_EX_TOPMOST on it, which is what
rem keeps it above the browser while still being a normal, movable window.
rem To unpin without closing it:
rem   powershell -NoProfile -ExecutionPolicy Bypass -File pin_topmost.ps1 -Off
rem
rem jup_trader.py must already be running (run.bat).
rem
rem This file is ASCII with CRLF endings on purpose. cmd.exe mis-parses a .bat
rem saved with LF-only endings - the first attempt at this file was written
rem that way and every rem line executed as a command.
rem
rem No if(...) blocks either: %ProgramFiles(x86)% contains a closing paren,
rem which ends a parenthesised block early and silently breaks the branch.

setlocal
set URL=http://127.0.0.1:5010/popout
set SIZE=--window-size=390,620
rem window opacity, 0-255. 217 is ~85%. Below ~140 the text gets hard
rem to read against a busy chart. Change it live without relaunching:
rem   powershell -NoProfile -ExecutionPolicy Bypass -File pin_topmost.ps1 -Alpha 170
set ALPHA=217

rem --user-data-dir is deliberately NOT set: the box reads the fee mode you
rem picked on the dashboard out of localStorage, and a separate profile would
rem be a separate localStorage, silently resetting it to MEV on every launch.

set "EXE=%ProgramFiles%\Google\Chrome\Application\chrome.exe"
if exist "%EXE%" goto launch
set "EXE=%ProgramFiles(x86)%\Google\Chrome\Application\chrome.exe"
if exist "%EXE%" goto launch
set "EXE=%LocalAppData%\Google\Chrome\Application\chrome.exe"
if exist "%EXE%" goto launch
set "EXE=%ProgramFiles(x86)%\Microsoft\Edge\Application\msedge.exe"
if exist "%EXE%" goto launch
set "EXE=%ProgramFiles%\Microsoft\Edge\Application\msedge.exe"
if exist "%EXE%" goto launch

echo Could not find Chrome or Edge in the usual locations.
echo Open the box manually with:  chrome.exe --app=%URL%
pause
exit /b 1

:launch
start "" "%EXE%" --app=%URL% %SIZE%
rem the script waits for the window to appear, so no sleep needed here
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0pin_topmost.ps1" -Alpha %ALPHA%
exit /b 0
