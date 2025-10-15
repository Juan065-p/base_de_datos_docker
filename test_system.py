"""
Script de prueba para verificar el funcionamiento del sistema
"""
import requests
import json
import sys
import time

BASE_URL = "http://localhost:5000"

def test_connection():
    """Prueba de conectividad básica"""
    try:
        response = requests.get(BASE_URL, timeout=5)
        return response.status_code == 200
    except:
        return False

def test_schema():
    """Prueba del endpoint de esquema"""
    try:
        response = requests.get(f"{BASE_URL}/schema", timeout=5)
        if response.status_code == 200:
            data = response.json()
            return data.get('success', False) and 'schema' in data
        return False
    except:
        return False

def test_examples():
    """Prueba del endpoint de ejemplos"""
    try:
        response = requests.get(f"{BASE_URL}/examples", timeout=5)
        if response.status_code == 200:
            data = response.json()
            return 'examples' in data and len(data['examples']) > 0
        return False
    except:
        return False

def test_query_execution():
    """Prueba de ejecución de consulta"""
    try:
        query = "SELECT COUNT(*) as total FROM usuarios;"
        payload = {"query": query}
        
        response = requests.post(
            f"{BASE_URL}/execute",
            json=payload,
            headers={'Content-Type': 'application/json'},
            timeout=10
        )
        
        if response.status_code == 200:
            data = response.json()
            return data.get('success', False) and 'data' in data
        return False
    except:
        return False

def run_all_tests():
    """Ejecutar todas las pruebas"""
    tests = [
        ("Conectividad básica", test_connection),
        ("Endpoint de esquema", test_schema),
        ("Endpoint de ejemplos", test_examples),
        ("Ejecución de consultas", test_query_execution),
    ]
    
    print("=== Ejecutando pruebas del sistema ===\n")
    
    passed = 0
    total = len(tests)
    
    for name, test_func in tests:
        print(f"Probando: {name}... ", end="")
        try:
            if test_func():
                print("✓ PASS")
                passed += 1
            else:
                print("✗ FAIL")
        except Exception as e:
            print(f"✗ ERROR: {str(e)}")
    
    print(f"\n=== Resultados ===")
    print(f"Pruebas pasadas: {passed}/{total}")
    print(f"Estado: {'ÉXITO' if passed == total else 'FALLOS DETECTADOS'}")
    
    return passed == total

def wait_for_service(max_wait=30):
    """Esperar a que el servicio esté disponible"""
    print(f"Esperando a que el servicio esté disponible (máximo {max_wait}s)...")
    
    for i in range(max_wait):
        if test_connection():
            print(f"✓ Servicio disponible después de {i+1} segundos")
            return True
        print(".", end="", flush=True)
        time.sleep(1)
    
    print(f"\n✗ Servicio no disponible después de {max_wait} segundos")
    return False

if __name__ == "__main__":
    print("Sistema de Consultas SQL - Pruebas Automáticas")
    print("=" * 50)
    
    # Esperar a que el servicio esté disponible
    if not wait_for_service():
        print("Error: No se puede conectar al servicio")
        sys.exit(1)
    
    # Ejecutar pruebas
    success = run_all_tests()
    
    if success:
        print("\n🎉 ¡Todas las pruebas pasaron! El sistema está funcionando correctamente.")
        sys.exit(0)
    else:
        print("\n❌ Algunas pruebas fallaron. Revisa la configuración del sistema.")
        sys.exit(1)