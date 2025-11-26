import os
import xml.etree.ElementTree as ET
from datetime import datetime
from flask import Flask, request, render_template_string, redirect, url_for, session
import mysql.connector
from mysql.connector import Error as MySQLError

# --- 1. CONFIGURACIÓN DE LA APLICACIÓN Y BASE DE DATOS (AWS MySQL RDS) ---


app = Flask(__name__)
app.secret_key = 'clave_secreta_para_sesiones' # ¡Cámbiala por una clave fuerte!

# Parámetros de Conexión a MySQL (Tus datos de AWS)
DB_HOST = 'database-1.ctdqnborhqq5.us-east-1.rds.amazonaws.com'
DB_DATABASE = 'CFDI_DB' # Asumimos este nombre de base de datos
DB_USER = 'admin'
DB_PASSWORD = 'admin1234' 

# Namespaces CFDI
NS = {
    'cfdi': 'http://www.sat.gob.mx/cfd/4',
    'pago20': 'http://www.sat.gob.mx/Pagos20',
    'tfd': 'http://www.sat.gob.mx/TimbreFiscalDigital',
    'xsi': 'http://www.w3.org/2001/XMLSchema-instance'
}

# --- 2. FUNCIONES DE CONEXIÓN A MYSQL ---

def get_db_connection_mysql():
    """Establece la conexión a MySQL Server en AWS."""
    try:
        conn = mysql.connector.connect(
            host=DB_HOST,
            database=DB_DATABASE,
            user=DB_USER,
            password=DB_PASSWORD
        )
        if conn.is_connected():
            return conn
    except MySQLError as e:
        # Captura el error y lo propaga para ser manejado en la interfaz
        raise ConnectionError(f"Error al conectar con MySQL. Detalles: {e.msg}. Revisa el grupo de seguridad de AWS (Puerto 3306).")

def ejecutar_consulta_db(query, parametros=None, fetch_one=False):
    """Función unificada para ejecutar consultas (SELECT, INSERT, UPDATE)."""
    conn = None
    try:
        conn = get_db_connection_mysql()
        cursor = conn.cursor()
        
        # Ejecución de la consulta
        if parametros:
            cursor.execute(query, parametros)
        else:
            cursor.execute(query)
            
        # Retorno de resultados para SELECT o confirmación para INSERT/UPDATE
        if query.strip().upper().startswith('SELECT'):
            resultado = cursor.fetchone() if fetch_one else cursor.fetchall()
            return resultado
        else:
            conn.commit()
            return True
            
    finally:
        if conn and conn.is_connected():
            conn.close()

# --- 3. LÓGICA DEL DIAGRAMA DE FLUJO (Integra la lógica CFDI y la DB remota) ---

def logica_procesamiento(xml_content):
    """Implementa el diagrama de flujo: leer XML, verificar DB, registrar cambios."""
    cambios_detectados = []
    
    try:
        root = ET.fromstring(xml_content)
        
        # Extracción de Datos
        prov_rfc = root.find('cfdi:Emisor', NS).get('Rfc')
        prov_nombre = root.find('cfdi:Emisor', NS).get('Nombre')
        docs_relacionados = root.findall('./cfdi:Complemento/pago20:Pagos/pago20:Pago/pago20:DoctoRelacionado', NS)
        fecha_pago = root.find('./cfdi:Complemento/pago20:Pagos/pago20:Pago', NS).get('FechaPago')

        # --- VERIFICAR PROVEEDOR ---
        sql_select_prov = "SELECT rfc FROM Proveedores WHERE rfc = %s"
        proveedor_existe = ejecutar_consulta_db(sql_select_prov, (prov_rfc,), fetch_one=True)

        # --- IDENTIFICAR PROVEEDORES NUEVOS & REGISTRAR CAMBIOS ---
        if not proveedor_existe:
            cambios_detectados.append(f"🆕 PROVEEDOR NUEVO DETECTADO: {prov_nombre} ({prov_rfc})")
            sql_insert_prov = "INSERT INTO Proveedores (rfc, nombre) VALUES (%s, %s)"
            ejecutar_consulta_db(sql_insert_prov, (prov_rfc, prov_nombre))
        
        # --- CONSULTAR PRECIOS EXISTENTES (UUIDs de Documentos Relacionados) ---
        for doc in docs_relacionados:
            uuid_doc = doc.get('IdDocumento')
            monto_pagado_nuevo = float(doc.get('ImpPagado'))
            
            sql_select_doc = "SELECT imp_pagado FROM Precios_Documentos WHERE uuid_original = %s"
            resultado_db = ejecutar_consulta_db(sql_select_doc, (uuid_doc,), fetch_one=True)
            
            # --- IDENTIFICAR PRECIOS NUEVOS Y CAMBIOS ---
            if resultado_db is None:
                # Precio Nuevo: El UUID se registra por primera vez
                cambios_detectados.append(f"💰 NUEVO PAGO REGISTRADO: UUID {uuid_doc} = ${monto_pagado_nuevo:.2f}")
                sql_insert_doc = """
                    INSERT INTO Precios_Documentos (uuid_original, rfc_emisor, imp_pagado, fecha_pago) 
                    VALUES (%s, %s, %s, %s)
                """
                ejecutar_consulta_db(sql_insert_doc, (uuid_doc, prov_rfc, monto_pagado_nuevo, fecha_pago))
                
            else:
                # Cambio de Precio: Se registra una nueva parcialidad o un pago diferente
                monto_pagado_antiguo = resultado_db[0]
                if monto_pagado_nuevo != monto_pagado_antiguo:
                    cambios_detectados.append(f"🔄 CAMBIO EN IMPORTE: UUID {uuid_doc}. Último: ${monto_pagado_antiguo:.2f} | Nuevo: ${monto_pagado_nuevo:.2f}")

                    sql_update_doc = """
                        UPDATE Precios_Documentos SET imp_pagado = %s, fecha_pago = %s WHERE uuid_original = %s
                    """
                    ejecutar_consulta_db(sql_update_doc, (monto_pagado_nuevo, fecha_pago, uuid_doc))
        
        # --- NOTIFICAR/REPORTAR (El retorno es la notificación en la interfaz) ---
        return cambios_detectados
    
    except ConnectionError as e:
        return [f"❌ ERROR CRÍTICO DB: {e}"]
    except ET.ParseError:
        return ["❌ ERROR DE FORMATO: Fallo al parsear el archivo XML."]
    except Exception as e:
        return [f"❌ ERROR INESPERADO: {e}"]

# --- 4. RUTAS DE FLASK (Interfaz Web) ---

@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        # Manejo de subida de archivo (igual que en el ejemplo anterior)
        # ... [El código de manejo de POST y redirección es el mismo] ...
        if 'xml_file' not in request.files or request.files['xml_file'].filename == '':
            session['results'] = ["❌ Ningún archivo seleccionado."]
            return redirect(url_for('index'))
        
        file = request.files['xml_file']
        if file and file.filename.endswith('.xml'):
            xml_content = file.read()
            results = logica_procesamiento(xml_content)
            session['results'] = results
            return redirect(url_for('index'))
        else:
            session['results'] = ["❌ Formato de archivo inválido. Debe ser .xml"]
            return redirect(url_for('index'))

    results = session.pop('results', None)
    
    # Template HTML mínimo para la interfaz
    html_template = """
    <!doctype html>
    <title>Procesador CFDI (API AWS - MySQL)</title>
    <style>body { font-family: Arial, sans-serif; } h1, h2 { color: #333; } ul { list-style-type: none; padding: 0; } li { margin-bottom: 5px; padding: 5px; border-bottom: 1px dotted #ccc; }</style>
    <h1>Procesar Factura CFDI</h1>
    
    <form method="POST" enctype="multipart/form-data">
        <input type="file" name="xml_file" accept=".xml">
        <input type="submit" value="Subir y Procesar CFDI" style="background-color: #4CAF50; color: white; padding: 10px 15px; border: none; cursor: pointer; border-radius: 4px;">
    </form>
    
    {% if results %}
        <hr>
        <h2>Resultados del Procesamiento</h2>
        <ul style="border: 1px solid #ddd; padding: 10px; background-color: #f9f9f9;">
        {% for result in results %}
            <li style="color: {% if '❌' in result %}red{% elif 'CAMBIO' in result or 'NUEVO' in result %}green{% else %}blue{% endif %};">{{ result | safe }}</li>
        {% endfor %}
        </ul>
        <p style="font-weight: bold;">
            {% if "❌" in results[0] %}Procesamiento con ERRORES.{% else %}Proceso finalizado. Consulte la lista de cambios para el detalle.{% endif %}
        </p>
    {% endif %}
    """
    return render_template_string(html_template, results=results)

if __name__ == '__main__':
    # Para correr en modo desarrollo
    app.run(debug=True, host='0.0.0.0', port=5000)