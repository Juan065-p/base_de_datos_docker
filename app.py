from flask import Flask, request, render_template, jsonify
import sqlite3
import os
import json
from datetime import datetime

app = Flask(__name__)

# Configuración de la base de datos
DATABASE_PATH = 'database/consultas.db'

def init_db():
    """Inicializa la base de datos con algunas tablas de ejemplo"""
    if not os.path.exists('database'):
        os.makedirs('database')
    
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()
    
    # Crear tabla de usuarios de ejemplo
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS usuarios (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nombre TEXT NOT NULL,
            email TEXT UNIQUE NOT NULL,
            edad INTEGER,
            fecha_registro DATE DEFAULT CURRENT_DATE
        )
    ''')
    
    # Crear tabla de productos de ejemplo
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS productos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nombre TEXT NOT NULL,
            precio REAL NOT NULL,
            categoria TEXT,
            stock INTEGER DEFAULT 0
        )
    ''')
    
    # Crear tabla de ventas de ejemplo
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS ventas (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            usuario_id INTEGER,
            producto_id INTEGER,
            cantidad INTEGER,
            fecha_venta DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (usuario_id) REFERENCES usuarios (id),
            FOREIGN KEY (producto_id) REFERENCES productos (id)
        )
    ''')
    
    # Insertar datos de ejemplo si no existen
    cursor.execute('SELECT COUNT(*) FROM usuarios')
    if cursor.fetchone()[0] == 0:
        usuarios_ejemplo = [
            ('Juan Pérez', 'juan@email.com', 28),
            ('María García', 'maria@email.com', 34),
            ('Carlos López', 'carlos@email.com', 22),
            ('Ana Martín', 'ana@email.com', 31),
            ('Luis Rodríguez', 'luis@email.com', 26)
        ]
        cursor.executemany('INSERT INTO usuarios (nombre, email, edad) VALUES (?, ?, ?)', usuarios_ejemplo)
        
        productos_ejemplo = [
            ('Laptop', 999.99, 'Electrónicos', 15),
            ('Mouse', 25.50, 'Accesorios', 50),
            ('Teclado', 45.00, 'Accesorios', 30),
            ('Monitor', 299.99, 'Electrónicos', 20),
            ('Silla Gaming', 199.99, 'Muebles', 8)
        ]
        cursor.executemany('INSERT INTO productos (nombre, precio, categoria, stock) VALUES (?, ?, ?, ?)', productos_ejemplo)
        
        ventas_ejemplo = [
            (1, 1, 1),
            (2, 2, 2),
            (1, 3, 1),
            (3, 1, 1),
            (4, 4, 1)
        ]
        cursor.executemany('INSERT INTO ventas (usuario_id, producto_id, cantidad) VALUES (?, ?, ?)', ventas_ejemplo)
    
    conn.commit()
    conn.close()

def execute_query(query, params=None):
    """Ejecuta una consulta SQL y retorna los resultados"""
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        conn.row_factory = sqlite3.Row  # Para obtener resultados como diccionarios
        cursor = conn.cursor()
        
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        
        # Determinar si es una consulta SELECT o una operación de modificación
        if query.strip().upper().startswith('SELECT'):
            results = [dict(row) for row in cursor.fetchall()]
            columns = [description[0] for description in cursor.description]
        else:
            conn.commit()
            results = {"affected_rows": cursor.rowcount, "message": "Query executed successfully"}
            columns = []
        
        conn.close()
        return {"success": True, "data": results, "columns": columns}
    
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.route('/')
def index():
    """Página principal con el formulario para consultas SQL"""
    return render_template('index.html')

@app.route('/execute', methods=['POST'])
def execute_sql():
    """Endpoint para ejecutar consultas SQL"""
    data = request.get_json()
    query = data.get('query', '').strip()
    
    if not query:
        return jsonify({"success": False, "error": "Query cannot be empty"})
    
    result = execute_query(query)
    return jsonify(result)

@app.route('/schema')
def get_schema():
    """Endpoint para obtener el esquema de la base de datos"""
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        cursor = conn.cursor()
        
        # Obtener información de las tablas
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
        tables = cursor.fetchall()
        
        schema = {}
        for table in tables:
            table_name = table[0]
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns = cursor.fetchall()
            schema[table_name] = [{"name": col[1], "type": col[2], "nullable": not col[3], "primary_key": bool(col[5])} for col in columns]
        
        conn.close()
        return jsonify({"success": True, "schema": schema})
    
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})

@app.route('/examples')
def get_examples():
    """Endpoint que retorna consultas SQL de ejemplo"""
    examples = [
        {
            "title": "Listar todos los usuarios",
            "query": "SELECT * FROM usuarios;"
        },
        {
            "title": "Productos con precio mayor a 100",
            "query": "SELECT * FROM productos WHERE precio > 100;"
        },
        {
            "title": "Contar usuarios por edad",
            "query": "SELECT edad, COUNT(*) as cantidad FROM usuarios GROUP BY edad ORDER BY edad;"
        },
        {
            "title": "Ventas con información de usuarios y productos",
            "query": """SELECT 
                v.id as venta_id,
                u.nombre as usuario,
                p.nombre as producto,
                v.cantidad,
                v.fecha_venta
            FROM ventas v
            JOIN usuarios u ON v.usuario_id = u.id
            JOIN productos p ON v.producto_id = p.id
            ORDER BY v.fecha_venta DESC;"""
        },
        {
            "title": "Insertar nuevo usuario",
            "query": "INSERT INTO usuarios (nombre, email, edad) VALUES ('Nuevo Usuario', 'nuevo@email.com', 25);"
        },
        {
            "title": "Actualizar precio de producto",
            "query": "UPDATE productos SET precio = 899.99 WHERE nombre = 'Laptop';"
        }
    ]
    return jsonify({"examples": examples})

if __name__ == '__main__':
    init_db()
    app.run(host='0.0.0.0', port=5000, debug=True)