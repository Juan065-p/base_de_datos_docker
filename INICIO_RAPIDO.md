# 🚀 Inicio Rápido - Sistema de Consultas SQL

## Para Windows

### Opción 1: Docker (Recomendado)
```bash
# 1. Abrir PowerShell o CMD en esta carpeta
# 2. Iniciar el sistema
manage.bat start

# 3. Abrir navegador en http://localhost:5000
```

### Opción 2: Ejecución local
```bash
# 1. Instalar Python 3.8+ si no lo tienes
# 2. Ejecutar en modo desarrollo
manage.bat dev

# 3. Abrir navegador en http://localhost:5000
```

## Para Linux/Mac

### Opción 1: Docker (Recomendado)
```bash
# 1. Hacer el script ejecutable
chmod +x manage.sh

# 2. Iniciar el sistema
./manage.sh start

# 3. Abrir navegador en http://localhost:5000
```

### Opción 2: Ejecución local
```bash
# 1. Ejecutar en modo desarrollo
./manage.sh dev

# 2. Abrir navegador en http://localhost:5000
```

## Comandos Útiles

```bash
# Ver logs en tiempo real
manage.bat logs    # Windows
./manage.sh logs   # Linux/Mac

# Detener el sistema
manage.bat stop    # Windows
./manage.sh stop   # Linux/Mac

# Ejecutar pruebas
manage.bat test    # Windows
./manage.sh test   # Linux/Mac

# Crear backup de la base de datos
manage.bat backup  # Windows
./manage.sh backup # Linux/Mac
```

## ¿Problemas?

1. **Puerto 5000 ocupado**: Cambia el puerto en `docker-compose.yml`
2. **Docker no instalado**: Usa `manage.bat dev` o `./manage.sh dev`
3. **Permisos en Linux**: `chmod +x manage.sh`

## 🎯 Primeros pasos

1. **Abrir**: http://localhost:5000
2. **Probar**: Haz clic en un ejemplo de consulta
3. **Ejecutar**: Botón "🚀 Ejecutar"
4. **Explorar**: Mira el esquema de la base de datos

¡Ya puedes empezar a hacer consultas SQL! 🎉