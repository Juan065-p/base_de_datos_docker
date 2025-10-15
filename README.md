# Sistema de Consultas SQL con Python y Docker

Este es un sistema completo para ejecutar consultas SQL en una base de datos SQLite usando Python, Flask y Docker.

## 🚀 Características

- **Interfaz Web Intuitiva**: Interfaz moderna y responsive para ejecutar consultas SQL
- **Base de Datos SQLite**: Base de datos ligera y fácil de usar
- **Containerización**: Completamente dockerizado para fácil despliegue
- **Ejemplos Incluidos**: Consultas SQL de ejemplo para empezar rápidamente
- **Esquema Dinámico**: Visualización automática del esquema de la base de datos
- **Soporte Completo SQL**: SELECT, INSERT, UPDATE, DELETE y más

## 📋 Estructura del Proyecto

```
base_de_datos_docker/
├── app.py                 # Aplicación principal de Flask
├── requirements.txt       # Dependencias de Python
├── Dockerfile            # Configuración de Docker
├── docker-compose.yml    # Orquestación de contenedores
├── README.md             # Este archivo
├── templates/
│   └── index.html        # Interfaz web
└── database/             # Directorio para la base de datos (se crea automáticamente)
    └── consultas.db      # Base de datos SQLite
```

## 🛠️ Instalación y Uso

### Opción 1: Con Docker (Recomendado)

1. **Construir y ejecutar con Docker Compose:**
   ```bash
   docker-compose up --build
   ```

2. **Acceder a la aplicación:**
   - Abrir navegador en: http://localhost:5000

3. **Detener la aplicación:**
   ```bash
   docker-compose down
   ```

### Opción 2: Ejecución Local (Sin Docker)

1. **Instalar dependencias:**
   ```bash
   pip install -r requirements.txt
   ```

2. **Ejecutar la aplicación:**
   ```bash
   python app.py
   ```

3. **Acceder a la aplicación:**
   - Abrir navegador en: http://localhost:5000

## 📊 Base de Datos de Ejemplo

El sistema incluye una base de datos de ejemplo con las siguientes tablas:

### Tabla: usuarios
- `id` (INTEGER, PRIMARY KEY)
- `nombre` (TEXT)
- `email` (TEXT, UNIQUE)
- `edad` (INTEGER)
- `fecha_registro` (DATE)

### Tabla: productos
- `id` (INTEGER, PRIMARY KEY)
- `nombre` (TEXT)
- `precio` (REAL)
- `categoria` (TEXT)
- `stock` (INTEGER)

### Tabla: ventas
- `id` (INTEGER, PRIMARY KEY)
- `usuario_id` (INTEGER, FOREIGN KEY)
- `producto_id` (INTEGER, FOREIGN KEY)
- `cantidad` (INTEGER)
- `fecha_venta` (DATETIME)

## 💡 Ejemplos de Consultas SQL

### Consultas SELECT Básicas
```sql
-- Listar todos los usuarios
SELECT * FROM usuarios;

-- Productos con precio mayor a 100
SELECT * FROM productos WHERE precio > 100;

-- Contar usuarios por edad
SELECT edad, COUNT(*) as cantidad 
FROM usuarios 
GROUP BY edad 
ORDER BY edad;
```

### Consultas con JOINs
```sql
-- Ventas con información completa
SELECT 
    v.id as venta_id,
    u.nombre as usuario,
    p.nombre as producto,
    v.cantidad,
    v.fecha_venta
FROM ventas v
JOIN usuarios u ON v.usuario_id = u.id
JOIN productos p ON v.producto_id = p.id
ORDER BY v.fecha_venta DESC;
```

### Consultas de Modificación
```sql
-- Insertar nuevo usuario
INSERT INTO usuarios (nombre, email, edad) 
VALUES ('Nuevo Usuario', 'nuevo@email.com', 25);

-- Actualizar precio de producto
UPDATE productos 
SET precio = 899.99 
WHERE nombre = 'Laptop';

-- Eliminar usuario
DELETE FROM usuarios 
WHERE id = 1;
```

## 🔧 Configuración

### Variables de Entorno
- `FLASK_ENV`: Entorno de Flask (development/production)
- `PYTHONUNBUFFERED`: Para logs en tiempo real

### Volúmenes Docker
- `./database:/app/database`: Persiste la base de datos entre reinicios
- `.:/app`: Para desarrollo (comentado por defecto)

## 🌐 API Endpoints

### GET /
- **Descripción**: Página principal con la interfaz web
- **Respuesta**: HTML con la interfaz de usuario

### POST /execute
- **Descripción**: Ejecutar consulta SQL
- **Body**: `{"query": "SELECT * FROM usuarios;"}`
- **Respuesta**: 
  ```json
  {
    "success": true,
    "data": [...],
    "columns": [...]
  }
  ```

### GET /schema
- **Descripción**: Obtener esquema de la base de datos
- **Respuesta**: 
  ```json
  {
    "success": true,
    "schema": {...}
  }
  ```

### GET /examples
- **Descripción**: Obtener consultas de ejemplo
- **Respuesta**: 
  ```json
  {
    "examples": [...]
  }
  ```

## 🔒 Seguridad

- Las consultas SQL se ejecutan de forma segura usando parámetros
- No hay acceso directo al sistema de archivos desde las consultas
- La aplicación está containerizada para mayor aislamiento

## 🚦 Salud del Contenedor

El sistema incluye un health check que verifica:
- Disponibilidad del servicio web en el puerto 5000
- Respuesta correcta de la aplicación

## 📝 Logs

Para ver los logs en tiempo real:
```bash
docker-compose logs -f
```

## 🛡️ Troubleshooting

### Problema: El contenedor no inicia
**Solución:**
```bash
docker-compose down
docker-compose up --build --force-recreate
```

### Problema: Base de datos no se crea
**Solución:**
- Verificar permisos en el directorio `./database/`
- Eliminar el directorio database y reiniciar

### Problema: Puerto 5000 ocupado
**Solución:**
- Cambiar el puerto en `docker-compose.yml`:
  ```yaml
  ports:
    - "8080:5000"  # Cambiar 5000 por otro puerto
  ```

## 📈 Extensiones Futuras

- Soporte para múltiples bases de datos
- Autenticación de usuarios
- Export de resultados a CSV/Excel
- Historial de consultas
- Editor SQL con syntax highlighting
- Visualizaciones gráficas de datos

## 🤝 Contribución

1. Fork el repositorio
2. Crear una rama feature (`git checkout -b feature/nueva-caracteristica`)
3. Commit los cambios (`git commit -am 'Agregar nueva característica'`)
4. Push a la rama (`git push origin feature/nueva-caracteristica`)
5. Crear un Pull Request

## 📄 Licencia

Este proyecto está bajo la Licencia MIT. Ver el archivo `LICENSE` para más detalles.

---

## 🚀 ¡Comienza Ahora!

```bash
# Clonar y ejecutar
git clone <tu-repositorio>
cd base_de_datos_docker
docker-compose up --build
# Abrir http://localhost:5000
```

---

## 🖥️ Instrucciones Específicas por Sistema Operativo

### 📟 Windows

#### Opción 1: Con Docker Desktop (Recomendado)

1. **Instalar Docker Desktop:**
   - Descargar desde: https://www.docker.com/products/docker-desktop
   - Instalar y reiniciar la computadora
   - Asegurarse de que Docker Desktop esté ejecutándose

2. **Ejecutar con PowerShell o CMD:**
   ```powershell
   # Navegar al directorio del proyecto
   cd "c:\Users\TuUsuario\ruta\base_de_datos_docker"
   
   # Usar el script de gestión
   .\manage.bat start
   
   # O ejecutar directamente con Docker Compose
   docker-compose up --build
   ```

3. **Acceder a la aplicación:**
   - Abrir navegador en: http://localhost:5000

#### Opción 2: Sin Docker (Ejecución Local)

1. **Instalar Python:**
   - Descargar Python 3.8+ desde: https://www.python.org/downloads/
   - Durante la instalación, marcar "Add Python to PATH"

2. **Ejecutar la aplicación:**
   ```powershell
   # Navegar al directorio del proyecto
   cd "c:\Users\TuUsuario\ruta\base_de_datos_docker"
   
   # Instalar dependencias
   pip install -r requirements.txt
   
   # Ejecutar la aplicación
   python app.py
   ```

3. **Acceder a la aplicación:**
   - Abrir navegador en: http://localhost:5000

#### Comandos útiles para Windows:
```powershell
# Iniciar el sistema
.\manage.bat start

# Ver logs
.\manage.bat logs

# Detener el sistema
.\manage.bat stop

# Ejecutar en modo desarrollo
.\manage.bat dev

# Crear backup de la base de datos
.\manage.bat backup

# Ejecutar pruebas
.\manage.bat test
```

### 🍎 macOS

#### Opción 1: Con Docker Desktop (Recomendado)

1. **Instalar Docker Desktop:**
   ```bash
   # Con Homebrew (recomendado)
   brew install --cask docker
   
   # O descargar desde: https://www.docker.com/products/docker-desktop
   ```

2. **Ejecutar la aplicación:**
   ```bash
   # Navegar al directorio del proyecto
   cd ~/ruta/base_de_datos_docker
   
   # Hacer el script ejecutable
   chmod +x manage.sh
   
   # Usar el script de gestión
   ./manage.sh start
   
   # O ejecutar directamente con Docker Compose
   docker-compose up --build
   ```

3. **Acceder a la aplicación:**
   - Abrir navegador en: http://localhost:5000

#### Opción 2: Sin Docker (Ejecución Local)

1. **Instalar Python con Homebrew:**
   ```bash
   # Instalar Homebrew si no lo tienes
   /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
   
   # Instalar Python
   brew install python
   ```

2. **Ejecutar la aplicación:**
   ```bash
   # Navegar al directorio del proyecto
   cd ~/ruta/base_de_datos_docker
   
   # Crear entorno virtual (opcional pero recomendado)
   python3 -m venv venv
   source venv/bin/activate
   
   # Instalar dependencias
   pip install -r requirements.txt
   
   # Ejecutar la aplicación
   python app.py
   ```

3. **Acceder a la aplicación:**
   - Abrir navegador en: http://localhost:5000

#### Comandos útiles para macOS:
```bash
# Hacer el script ejecutable (solo la primera vez)
chmod +x manage.sh

# Iniciar el sistema
./manage.sh start

# Ver logs
./manage.sh logs

# Detener el sistema
./manage.sh stop

# Ejecutar en modo desarrollo
./manage.sh dev

# Crear backup de la base de datos
./manage.sh backup

# Ejecutar pruebas
./manage.sh test
```

### 🐧 Linux (Ubuntu/Debian)

#### Opción 1: Con Docker (Recomendado)

1. **Instalar Docker:**
   ```bash
   # Actualizar paquetes
   sudo apt update
   
   # Instalar Docker
   sudo apt install docker.io docker-compose
   
   # Agregar usuario al grupo docker
   sudo usermod -aG docker $USER
   
   # Reiniciar sesión o usar:
   newgrp docker
   ```

2. **Ejecutar la aplicación:**
   ```bash
   # Navegar al directorio del proyecto
   cd ~/ruta/base_de_datos_docker
   
   # Hacer el script ejecutable
   chmod +x manage.sh
   
   # Usar el script de gestión
   ./manage.sh start
   ```

#### Opción 2: Sin Docker
```bash
# Instalar Python y pip
sudo apt update
sudo apt install python3 python3-pip python3-venv

# Navegar al proyecto
cd ~/ruta/base_de_datos_docker

# Ejecutar en modo desarrollo
./manage.sh dev
```

### 🚨 Solución de Problemas Comunes

#### Windows:
- **Error "Docker no encontrado"**: Instalar Docker Desktop y reiniciar
- **Puerto 5000 ocupado**: Cambiar puerto en `docker-compose.yml` o usar `netstat -an | findstr :5000`
- **Permisos de PowerShell**: Ejecutar como administrador o cambiar política: `Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser`

#### macOS:
- **Error "Permission denied"**: Usar `chmod +x manage.sh`
- **Docker no inicia**: Abrir Docker Desktop desde Applications
- **Python no encontrado**: Instalar con Homebrew: `brew install python`

#### General:
- **Puerto ocupado**: Cambiar el puerto en `docker-compose.yml`:
  ```yaml
  ports:
    - "8080:5000"  # Usar puerto 8080 en lugar de 5000
  ```
- **Base de datos no se crea**: Verificar permisos en el directorio del proyecto
- **Consultas fallan**: Revisar sintaxis SQL y estructura de la base de datos

¡Disfruta ejecutando tus consultas SQL! 🎉


