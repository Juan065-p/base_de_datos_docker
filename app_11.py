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
        CREATE TABLE IF NOT EXISTS Sucursal (
            id_Sucursal INTEGER PRIMARY KEY AUTOINCREMENT,
            nombre_Sucursal TEXT NOT NULL,
            direccion TEXT UNIQUE NOT NULL,
            fecha_registro DATE DEFAULT CURRENT_DATE
        )
    ''')
    
    # Crear tabla de retiros
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS retiros (
            id_retiro INTEGER PRIMARY KEY AUTOINCREMENT,
            cantidad_retiros INTEGER NOT NULL,
            fecha_retiro DATE DEFAULT CURRENT_DATE
        )
    ''')
    
    # # Crear tabla de transaccion
    cursor.execute('''
         CREATE TABLE IF NOT EXISTS transaccion (
             id_deposito INTEGER PRIMARY KEY AUTOINCREMENT,
             cantidad_deposito INTEGER,
             monto_ingreso REAL,
             fecha_deposito DATETIME DEFAULT CURRENT_TIMESTAMP
         )
    ''')
    
    # Insertar datos de ejemplo si no existen
    cursor.execute('SELECT COUNT(*) FROM Sucursal')
    if cursor.fetchone()[0] == 0:
        usuarios_ejemplo = [
            ("Sucursal Central","Av. Principal 123"),
            ("Sucursal Norte","Calle Secundaria 456"),
            ("Sucursal Sur","Boulevard Tercero 789"),
            ("Sucursal Este","Avenida Cuarto 101"),
            ("Sucursal Oeste","Calle Quinta 202"),
            ("Sucursal Centro","Calle Sexta 303"),
            ("Sucursal Internacional","Avenida Séptima 404"),
            ("Sucursal Local","Calle Octava 505"),
            ("Sucursal Urbana","Boulevard Noveno 606"),
            ("Sucursal Rural","Calle Décima 707"),
            ("Sucursal Metropolitana","Avenida Once 808"),
            ("Sucursal Provincial","Calle Doce 909"),
            ("Sucursal Regional","Boulevard Trece 111"),
            ("Sucursal Nacional","Avenida Catorce 222"),
            ("Sucursal Internacional II","Calle Quince 333"),
            ("Sucursal Local II","Boulevard Dieciséis 444"),
            ("Sucursal Urbana II","Avenida Diecisiete 555"),
            ("Sucursal Rural II","Calle Dieciocho 666"),
            ("Sucursal Metropolitana II","Boulevard Diecinueve 777"),
            ("Sucursal Provincial II","Avenida Veinte 888"),
            ("Sucursal Regional II","Calle Veintiuno 999"),
            ("Sucursal Nacional II","Boulevard Veintidós 121"),
            ("Sucursal Central III","Avenida Veintitrés 232"),
            ("Sucursal Norte III","Calle Veinticuatro 343"),
            ("Sucursal Sur III","Boulevard Veinticinco 454")

        ]
        cursor.executemany('INSERT INTO Sucursal (nombre_Sucursal,direccion) VALUES (?, ?)', usuarios_ejemplo)
        
        retiros_ejemplo = [
            (5000,),
            (5100,),
            (5200,),
            (5300,),
            (5400,),
            (5500,),
            (5600,),
            (5700,),
            (5800,),
            (5900,),
            (6000,),
            (6100,),
            (6200,),
            (6300,),
            (6400,),
            (6500,),
            (6600,),
            (6700,),
            (6800,),
            (6900,),
            (7000,),
            (7100,),
            (7200,),
            (7300,),
            (7400,)
         ]
        cursor.executemany('INSERT INTO retiros (cantidad_retiros) VALUES (?)',  retiros_ejemplo)
        
        transaccion_ejemplo = [
            (100,2500.00),
            (150,3750.50),
            (200,4200.00),
            (250,5250.75),
            (300,6000.00),
            (350,7700.25),
            (400,8200.00),
            (450,9450.90),
            (500,10000.00),
            (550,11000.50),
            (600,12100.00),
            (650,13550.75),
            (700,14000.00),
            (750,15750.25),
            (800,16000.00),
            (850,17650.80),
            (900,18000.00),
            (950,19950.45),
            (1000,20000.00),
            (1050,22050.60),
            (1100,23000.00),
            (1150,24150.35),
            (1200,25000.00),
            (1250,27500.00),
            (1300,28600.00)
         ]
        cursor.executemany('INSERT INTO transaccion (cantidad_deposito,monto_ingreso) VALUES (?, ?)', transaccion_ejemplo)
    
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
            "query": "SELECT * FROM sucursal;"
        },
        {
            "title": "Productos con precio mayor a 100",
            "query": "SELECT * FROM productos WHERE precio > 100;"
        },
        {
            "title": "Contar usuarios por edad",
            "query": "SELECT edad, COUNT(*) as cantidad FROM sucursal GROUP BY edad ORDER BY edad;"
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