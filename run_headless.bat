@echo off
title Mo Tab Bot - Edge CDP 9222
cd /d "C:\bot_san_code"

echo.
echo ============================================================
echo  BUOC 1: Kiem tra / Mo Edge voi CDP port 9222
echo ============================================================

REM Tim duong dan Edge
set EDGE_PATH=C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe
if not exist "%EDGE_PATH%" set EDGE_PATH=C:\Program Files\Microsoft\Edge\Application\msedge.exe
if not exist "%EDGE_PATH%" (
    echo [LOI] Khong tim thay Microsoft Edge!
    goto :eof
)

echo [OK] Tim thay Edge tai: %EDGE_PATH%

REM Kiem tra port 9222 qua IPv4 (dung 127.0.0.1 thay vi localhost)
curl -s --max-time 2 http://127.0.0.1:9222/json/version >nul 2>&1
if %errorlevel%==0 (
    echo [OK] Da co trinh duyet chay port 9222. Bo qua buoc mo.
    goto :open_tabs
)

echo [OK] Dang mo Microsoft Edge voi CDP port 9222...
start "" "%EDGE_PATH%" ^
    --remote-debugging-port=9222 ^
    --remote-debugging-address=127.0.0.1 ^
    --user-data-dir="%LOCALAPPDATA%\Microsoft\Edge\User Data" ^
    --profile-directory="Default" ^
    --no-first-run ^
    --mute-audio ^
    --window-size=900,700 ^
    --window-position=0,0 ^
    --disable-blink-features=AutomationControlled ^
    --exclude-switches=enable-automation ^
    --disable-features=IsolateOrigins,site-per-process ^
    --flag-switches-begin ^
    --flag-switches-end

echo [OK] Cho Edge khoi dong (8 giay)...
timeout /t 8 /nobreak >nul

echo [OK] Kiem tra CDP qua 127.0.0.1...
curl -s --max-time 3 http://127.0.0.1:9222/json/version >nul 2>&1
if not %errorlevel%==0 (
    echo [CANH BAO] CDP chua phan hoi, cho them 5 giay...
    timeout /t 5 /nobreak >nul
)

curl -s --max-time 3 http://127.0.0.1:9222/json/version >nul 2>&1
if not %errorlevel%==0 (
    echo.
    echo [LOI] Khong ket noi duoc CDP port 9222!
    echo Hay thu:
    echo   1. Dong tat ca cua so Edge dang mo
    echo   2. Chay lai file nay voi quyen Admin
    goto :eof
)

echo [OK] CDP da san sang!

:open_tabs
echo.
echo ============================================================
echo  BUOC 2: Mo cac tab can thiet trong Edge
echo ============================================================
echo.

echo [1/8] mm88code.com...
curl -s -X PUT "http://127.0.0.1:9222/json/new?https://mm88code.com" >nul
timeout /t 1 /nobreak >nul

echo [2/8] llwincode.com...
curl -s -X PUT "http://127.0.0.1:9222/json/new?https://llwincode.com" >nul
timeout /t 1 /nobreak >nul

echo [3/8] new88b.today/giftcode...
curl -s -X PUT "http://127.0.0.1:9222/json/new?https://new88b.today/giftcode" >nul
timeout /t 1 /nobreak >nul

echo [4/8] xx88code.com...
curl -s -X PUT "http://127.0.0.1:9222/json/new?https://xx88code.com/" >nul
timeout /t 1 /nobreak >nul

echo [5/8] o8code.com...
curl -s -X PUT "http://127.0.0.1:9222/json/new?https://o8code.com/" >nul
timeout /t 1 /nobreak >nul

echo [6/8] tangquaqq88.com...
curl -s -X PUT "http://127.0.0.1:9222/json/new?https://tangquaqq88.com/" >nul
timeout /t 1 /nobreak >nul

echo [7/8] uy88code.org/inputcode...
curl -s -X PUT "http://127.0.0.1:9222/json/new?https://uy88code.org/inputcode/" >nul
timeout /t 1 /nobreak >nul

echo [8/8] mmoocode.shop/inputcode...
curl -s -X PUT "http://127.0.0.1:9222/json/new?https://mmoocode.shop/inputcode/" >nul
timeout /t 1 /nobreak >nul

echo.
echo [OK] Da mo xong 8 tab!
echo.
echo ============================================================
echo  BUOC 3: XAC MINH CLOUDFLARE - LAM THU CONG
echo ============================================================
echo.
echo  Lan luot click vao tung tab va xu ly Cloudflare:
echo    - Neu hien checkbox "I am human"      -> Click vao do
echo    - Neu hien form nhap code binh thuong -> Tab ok, bo qua
echo    - Neu hien trang trang / loi          -> F5 de tai lai
echo.
echo  Sau khi TAT CA tab hien form nhap code,
echo  he thong se tu dong chay bot.
echo.
timeout /t 5 /nobreak >nul

echo.
echo ============================================================
echo  BUOC 4: CHAY BOT
echo ============================================================
echo.

if exist "venv\Scripts\python.exe" (
    echo [OK] Dung Python trong venv...
    venv\Scripts\python.exe main_script.py
) else (
    echo [OK] Dung Python he thong...
    python main_script.py
)

echo.
echo [BOT] Bot da thoat.
