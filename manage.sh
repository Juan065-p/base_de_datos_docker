#!/bin/bash

# Script de gestión del sistema de consultas SQL
# Uso: ./manage.sh [comando]

set -e

# Colores para output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Función para mostrar ayuda
show_help() {
    echo -e "${BLUE}=== Sistema de Consultas SQL - Gestión ===${NC}"
    echo ""
    echo "Comandos disponibles:"
    echo -e "${GREEN}  start${NC}     - Iniciar el sistema con Docker"
    echo -e "${GREEN}  stop${NC}      - Detener el sistema"
    echo -e "${GREEN}  restart${NC}   - Reiniciar el sistema"
    echo -e "${GREEN}  build${NC}     - Construir la imagen Docker"
    echo -e "${GREEN}  logs${NC}      - Ver logs en tiempo real"
    echo -e "${GREEN}  shell${NC}     - Acceder al contenedor"
    echo -e "${GREEN}  clean${NC}     - Limpiar contenedores e imágenes"
    echo -e "${GREEN}  backup${NC}    - Crear backup de la base de datos"
    echo -e "${GREEN}  restore${NC}   - Restaurar backup de la base de datos"
    echo -e "${GREEN}  dev${NC}       - Ejecutar en modo desarrollo (sin Docker)"
    echo -e "${GREEN}  test${NC}      - Ejecutar pruebas básicas"
    echo -e "${GREEN}  help${NC}      - Mostrar esta ayuda"
    echo ""
    echo -e "${YELLOW}Ejemplos:${NC}"
    echo "  ./manage.sh start"
    echo "  ./manage.sh logs"
    echo "  ./manage.sh backup"
}

# Función para iniciar el sistema
start_system() {
    echo -e "${BLUE}Iniciando sistema de consultas SQL...${NC}"
    docker-compose up -d --build
    echo -e "${GREEN}✓ Sistema iniciado correctamente${NC}"
    echo -e "${YELLOW}Accede a: http://localhost:5000${NC}"
}

# Función para detener el sistema
stop_system() {
    echo -e "${BLUE}Deteniendo sistema...${NC}"
    docker-compose down
    echo -e "${GREEN}✓ Sistema detenido${NC}"
}

# Función para reiniciar el sistema
restart_system() {
    echo -e "${BLUE}Reiniciando sistema...${NC}"
    docker-compose restart
    echo -e "${GREEN}✓ Sistema reiniciado${NC}"
}

# Función para construir
build_system() {
    echo -e "${BLUE}Construyendo imagen Docker...${NC}"
    docker-compose build --no-cache
    echo -e "${GREEN}✓ Imagen construida${NC}"
}

# Función para ver logs
view_logs() {
    echo -e "${BLUE}Mostrando logs (Ctrl+C para salir)...${NC}"
    docker-compose logs -f
}

# Función para acceder al shell
access_shell() {
    echo -e "${BLUE}Accediendo al contenedor...${NC}"
    docker-compose exec sql-query-app /bin/bash
}

# Función para limpiar
clean_system() {
    echo -e "${BLUE}Limpiando contenedores e imágenes...${NC}"
    docker-compose down -v --rmi all --remove-orphans
    docker system prune -f
    echo -e "${GREEN}✓ Limpieza completada${NC}"
}

# Función para backup
backup_db() {
    echo -e "${BLUE}Creando backup de la base de datos...${NC}"
    BACKUP_DIR="backups"
    mkdir -p "$BACKUP_DIR"
    TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
    BACKUP_FILE="$BACKUP_DIR/backup_$TIMESTAMP.db"
    
    if [ -f "database/consultas.db" ]; then
        cp "database/consultas.db" "$BACKUP_FILE"
        echo -e "${GREEN}✓ Backup creado: $BACKUP_FILE${NC}"
    else
        echo -e "${RED}✗ No se encontró la base de datos${NC}"
    fi
}

# Función para restaurar
restore_db() {
    echo -e "${BLUE}Restaurando backup de la base de datos...${NC}"
    
    if [ -z "$2" ]; then
        echo -e "${RED}✗ Especifica el archivo de backup${NC}"
        echo -e "${YELLOW}Uso: ./manage.sh restore backup_file.db${NC}"
        return 1
    fi
    
    if [ -f "$2" ]; then
        cp "$2" "database/consultas.db"
        echo -e "${GREEN}✓ Base de datos restaurada desde: $2${NC}"
    else
        echo -e "${RED}✗ Archivo de backup no encontrado: $2${NC}"
    fi
}

# Función para desarrollo
dev_mode() {
    echo -e "${BLUE}Iniciando en modo desarrollo...${NC}"
    
    if ! command -v python3 &> /dev/null; then
        echo -e "${RED}✗ Python3 no está instalado${NC}"
        return 1
    fi
    
    # Instalar dependencias si no existen
    if [ ! -d "venv" ]; then
        echo -e "${YELLOW}Creando entorno virtual...${NC}"
        python3 -m venv venv
    fi
    
    source venv/bin/activate
    pip install -r requirements.txt
    
    echo -e "${GREEN}✓ Iniciando aplicación en modo desarrollo${NC}"
    echo -e "${YELLOW}Accede a: http://localhost:5000${NC}"
    python app.py
}

# Función para pruebas
run_tests() {
    echo -e "${BLUE}Ejecutando pruebas básicas...${NC}"
    
    # Verificar que el contenedor esté corriendo
    if ! docker-compose ps | grep -q "Up"; then
        echo -e "${YELLOW}Iniciando sistema para pruebas...${NC}"
        docker-compose up -d
        sleep 5
    fi
    
    # Prueba básica de conectividad
    if curl -f http://localhost:5000 >/dev/null 2>&1; then
        echo -e "${GREEN}✓ Servidor web funcionando${NC}"
    else
        echo -e "${RED}✗ Error: Servidor web no responde${NC}"
        return 1
    fi
    
    # Prueba de esquema
    if curl -f http://localhost:5000/schema >/dev/null 2>&1; then
        echo -e "${GREEN}✓ Endpoint de esquema funcionando${NC}"
    else
        echo -e "${RED}✗ Error: Endpoint de esquema no responde${NC}"
        return 1
    fi
    
    echo -e "${GREEN}✓ Todas las pruebas básicas pasaron${NC}"
}

# Función principal
main() {
    case ${1:-help} in
        start)
            start_system
            ;;
        stop)
            stop_system
            ;;
        restart)
            restart_system
            ;;
        build)
            build_system
            ;;
        logs)
            view_logs
            ;;
        shell)
            access_shell
            ;;
        clean)
            clean_system
            ;;
        backup)
            backup_db
            ;;
        restore)
            restore_db "$@"
            ;;
        dev)
            dev_mode
            ;;
        test)
            run_tests
            ;;
        help|--help|-h)
            show_help
            ;;
        *)
            echo -e "${RED}✗ Comando no reconocido: $1${NC}"
            echo ""
            show_help
            exit 1
            ;;
    esac
}

# Verificar si Docker está instalado
if ! command -v docker &> /dev/null && [ "$1" != "dev" ] && [ "$1" != "help" ]; then
    echo -e "${RED}✗ Docker no está instalado o no está en el PATH${NC}"
    exit 1
fi

# Ejecutar función principal
main "$@"