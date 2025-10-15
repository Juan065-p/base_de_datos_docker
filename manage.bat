@echo off
REM Script de gestión del sistema de consultas SQL para Windows
REM Uso: manage.bat [comando]

setlocal enabledelayedexpansion

REM Función para mostrar ayuda
if "%1"=="" goto :show_help
if "%1"=="help" goto :show_help
if "%1"=="--help" goto :show_help
if "%1"=="-h" goto :show_help

REM Verificar si Docker está instalado
where docker >nul 2>nul
if %errorlevel% neq 0 (
    if not "%1"=="dev" (
        echo [ERROR] Docker no esta instalado o no esta en el PATH
        exit /b 1
    )
)

REM Ejecutar comando
if "%1"=="start" goto :start_system
if "%1"=="stop" goto :stop_system
if "%1"=="restart" goto :restart_system
if "%1"=="build" goto :build_system
if "%1"=="logs" goto :view_logs
if "%1"=="shell" goto :access_shell
if "%1"=="clean" goto :clean_system
if "%1"=="backup" goto :backup_db
if "%1"=="restore" goto :restore_db
if "%1"=="dev" goto :dev_mode
if "%1"=="test" goto :run_tests

echo [ERROR] Comando no reconocido: %1
echo.
goto :show_help

:show_help
echo === Sistema de Consultas SQL - Gestion ===
echo.
echo Comandos disponibles:
echo   start     - Iniciar el sistema con Docker
echo   stop      - Detener el sistema
echo   restart   - Reiniciar el sistema
echo   build     - Construir la imagen Docker
echo   logs      - Ver logs en tiempo real
echo   shell     - Acceder al contenedor
echo   clean     - Limpiar contenedores e imagenes
echo   backup    - Crear backup de la base de datos
echo   restore   - Restaurar backup de la base de datos
echo   dev       - Ejecutar en modo desarrollo (sin Docker)
echo   test      - Ejecutar pruebas basicas
echo   help      - Mostrar esta ayuda
echo.
echo Ejemplos:
echo   manage.bat start
echo   manage.bat logs
echo   manage.bat backup
goto :eof

:start_system
echo Iniciando sistema de consultas SQL...
docker-compose up -d --build
if %errorlevel% equ 0 (
    echo [SUCCESS] Sistema iniciado correctamente
    echo Accede a: http://localhost:5000
) else (
    echo [ERROR] Error al iniciar el sistema
)
goto :eof

:stop_system
echo Deteniendo sistema...
docker-compose down
if %errorlevel% equ 0 (
    echo [SUCCESS] Sistema detenido
) else (
    echo [ERROR] Error al detener el sistema
)
goto :eof

:restart_system
echo Reiniciando sistema...
docker-compose restart
if %errorlevel% equ 0 (
    echo [SUCCESS] Sistema reiniciado
) else (
    echo [ERROR] Error al reiniciar el sistema
)
goto :eof

:build_system
echo Construyendo imagen Docker...
docker-compose build --no-cache
if %errorlevel% equ 0 (
    echo [SUCCESS] Imagen construida
) else (
    echo [ERROR] Error al construir la imagen
)
goto :eof

:view_logs
echo Mostrando logs (Ctrl+C para salir)...
docker-compose logs -f
goto :eof

:access_shell
echo Accediendo al contenedor...
docker-compose exec sql-query-app /bin/bash
goto :eof

:clean_system
echo Limpiando contenedores e imagenes...
docker-compose down -v --rmi all --remove-orphans
docker system prune -f
echo [SUCCESS] Limpieza completada
goto :eof

:backup_db
echo Creando backup de la base de datos...
if not exist "backups" mkdir backups

REM Crear timestamp
for /f "tokens=2-4 delims=/ " %%a in ('date /t') do (set mydate=%%c%%a%%b)
for /f "tokens=1-2 delims=/:" %%a in ('time /t') do (set mytime=%%a%%b)
set timestamp=%mydate%_%mytime%
set timestamp=%timestamp: =0%

set backup_file=backups\backup_%timestamp%.db

if exist "database\consultas.db" (
    copy "database\consultas.db" "%backup_file%" >nul
    echo [SUCCESS] Backup creado: %backup_file%
) else (
    echo [ERROR] No se encontro la base de datos
)
goto :eof

:restore_db
if "%2"=="" (
    echo [ERROR] Especifica el archivo de backup
    echo Uso: manage.bat restore backup_file.db
    goto :eof
)

echo Restaurando backup de la base de datos...
if exist "%2" (
    copy "%2" "database\consultas.db" >nul
    echo [SUCCESS] Base de datos restaurada desde: %2
) else (
    echo [ERROR] Archivo de backup no encontrado: %2
)
goto :eof

:dev_mode
echo Iniciando en modo desarrollo...

where python >nul 2>nul
if %errorlevel% neq 0 (
    echo [ERROR] Python no esta instalado
    goto :eof
)

REM Crear entorno virtual si no existe
if not exist "venv" (
    echo Creando entorno virtual...
    python -m venv venv
)

REM Activar entorno virtual e instalar dependencias
call venv\Scripts\activate.bat
pip install -r requirements.txt

echo [SUCCESS] Iniciando aplicacion en modo desarrollo
echo Accede a: http://localhost:5000
python app.py
goto :eof

:run_tests
echo Ejecutando pruebas basicas...

REM Verificar que el contenedor este corriendo
docker-compose ps | findstr "Up" >nul
if %errorlevel% neq 0 (
    echo Iniciando sistema para pruebas...
    docker-compose up -d
    timeout /t 5 /nobreak >nul
)

REM Prueba basica de conectividad usando PowerShell
powershell -Command "try { Invoke-WebRequest -Uri 'http://localhost:5000' -UseBasicParsing | Out-Null; exit 0 } catch { exit 1 }"
if %errorlevel% equ 0 (
    echo [SUCCESS] Servidor web funcionando
) else (
    echo [ERROR] Servidor web no responde
    goto :eof
)

REM Prueba de esquema
powershell -Command "try { Invoke-WebRequest -Uri 'http://localhost:5000/schema' -UseBasicParsing | Out-Null; exit 0 } catch { exit 1 }"
if %errorlevel% equ 0 (
    echo [SUCCESS] Endpoint de esquema funcionando
) else (
    echo [ERROR] Endpoint de esquema no responde
    goto :eof
)

echo [SUCCESS] Todas las pruebas basicas pasaron
goto :eof

:eof