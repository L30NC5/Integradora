import os
import xml.etree.ElementTree as ET
from datetime import datetime
from flask import Flask, request, render_template, redirect, url_for, session, send_file
import mysql.connector
from mysql.connector import Error as MySQLError
from fpdf import FPDF
import requests # Necesario para llamar a la API Gateway

# --- 1. CONFIGURACIÓN DE LA APLICACIÓN Y BASE DE DATOS (AWS RDS) ---

# Inicializar la aplicación Flask al inicio
app = Flask(__name__) 

# La clave secreta se lee de las variables de entorno de Elastic Beanstalk (EB).
# Usamos un valor de respaldo solo para pruebas muy locales si fuera necesario.
app.secret_key = os.environ.get('SECRET_KEY', 'clave_secreta_para_sesiones_aws') 

# Parámetros de Conexión a MySQL (Obtenidos de Variables de Entorno de EB)
DB_HOST = os.environ.get('DB_HOST') 
DB_DATABASE = os.environ.get('DB_DATABASE') or 'CFDI_DB'
DB_USER = os.environ.get('DB_USER')     
DB_PASSWORD = os.environ.get('DB_PASSWORD') 

# URL Base para invocar la API Gateway (Lectura de la variable de entorno configurada manualmente)
API_GATEWAY_BASE_URL = os.environ.get('API_GATEWAY_BASE_URL', 'http://localhost:8000') 

# Namespaces CFDI (Asegura la lectura correcta del XML)
NS = {
    'cfdi': 'http://www.sat.gob.mx/cfd/4',
    'cfdi33': 'http://www.sat.gob.mx/cfd/3',
    'pago20': 'http://www.sat.gob.mx/Pagos20',
    'tfd': 'http://www.sat.gob.mx/TimbreFiscalDigital',
    'xsi': 'http://www.w3.org/2001/XMLSchema-instance'
}

# --- 2. FUNCIONES DE CONEXIÓN Y CONSULTA A MYSQL (RDS) ---

def get_db_connection_mysql():
    """Establece la conexión a MySQL Server (RDS) usando variables de entorno."""
    
    # Comprobación crucial en el entorno AWS
    if not all([DB_HOST, DB_DATABASE, DB_USER, DB_PASSWORD]):
         raise ConnectionError("Faltan variables de entorno críticas (DB_HOST, DB_USER, etc.) de Elastic Beanstalk.")
        
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
        # Lanza un error de conexión para depuración en AWS
        raise ConnectionError(f"Error al conectar con RDS. Detalles: {e.msg}. ¿El Security Group de EB permite el puerto 3306?")

def ejecutar_consulta_db(query, parametros=None, fetch_one=False):
    """Función unificada para ejecutar consultas (SELECT, INSERT, UPDATE)."""
    conn = None
    try:
        conn = get_db_connection_mysql()
        cursor = conn.cursor()
        
        if parametros:
            cursor.execute(query, parametros)
        else:
            cursor.execute(query)
            
        if query.strip().upper().startswith('SELECT'):
            resultado = cursor.fetchone() if fetch_one else cursor.fetchall()
            return resultado
        else:
            conn.commit()
            return True
            
    finally:
        if conn and conn.is_connected():
            conn.close()

# --- 3. LÓGICA DEL DIAGRAMA DE FLUJO: PROCESAMIENTO CFDI ---

def logica_procesamiento(xml_content):
    """Implementa el diagrama de flujo, incluyendo la invocación al Microservicio SAT."""
    cambios_detectados = []
    uuid_doc = "UUID-NO-ENCONTRADO"
    
    try:
        if isinstance(xml_content, bytes):
            xml_content = xml_content.decode('utf-8')
            
        root = ET.fromstring(xml_content)
        
        cfdi_version = root.get('Version')
        main_ns = 'cfdi33' if cfdi_version == '3.3' else 'cfdi'

        # Extracción de datos básicos
        emisor_node = root.find(f'{main_ns}:Emisor', NS)
        if emisor_node is None:
             raise Exception(f"No se encontró el nodo Emisor. Versión {cfdi_version}")

        prov_rfc = emisor_node.get('Rfc')
        prov_nombre = emisor_node.get('Nombre') or f"RFC: {prov_rfc}"

        timbre_node = root.find('.//tfd:TimbreFiscalDigital', NS)
        uuid_doc = timbre_node.get('UUID') if timbre_node is not None else "UUID-NO-ENCONTRADO"
        
        total_comprobante = float(root.get('Total') or 0.0) 
        fecha_comprobante = root.get('Fecha')
        tipo_comprobante = root.get('TipoDeComprobante')
        
        if fecha_comprobante:
             fecha_comprobante = datetime.fromisoformat(fecha_comprobante).strftime('%Y-%m-%d %H:%M:%S')

        documentos_a_procesar = []

        if tipo_comprobante == 'P': # Complemento de Pago
            docs_relacionados = root.findall('.//pago20:DoctoRelacionado', NS)
            fecha_pago_comp = root.find('.//pago20:Pago', NS).get('FechaPago')
            if fecha_pago_comp:
                fecha_pago_comp = datetime.fromisoformat(fecha_pago_comp).strftime('%Y-%m-%d %H:%M:%S')

            for doc in docs_relacionados:
                 uuid_relacionado = doc.get('IdDocumento')
                 importe_pagado_rel = float(doc.get('ImpPagado') or 0.0)
                 
                 documentos_a_procesar.append({
                     'uuid': uuid_relacionado, 
                     'importe': importe_pagado_rel, 
                     'fecha': fecha_pago_comp 
                 })
                 
        elif tipo_comprobante == 'I': # Factura de Ingreso
             documentos_a_procesar.append({
                 'uuid': uuid_doc, 
                 'importe': total_comprobante, 
                 'fecha': fecha_comprobante
             })
        
        # --- Lógica de DB y Persistencia (Misma lógica) ---
        
        sql_select_prov = "SELECT rfc FROM Proveedores WHERE rfc = %s"
        proveedor_existe = ejecutar_consulta_db(sql_select_prov, (prov_rfc,), fetch_one=True)

        if not proveedor_existe:
            cambios_detectados.append(f"🆕 PROVEEDOR NUEVO DETECTADO: {prov_nombre} ({prov_rfc})")
            sql_insert_prov = "INSERT INTO Proveedores (rfc, nombre) VALUES (%s, %s)"
            ejecutar_consulta_db(sql_insert_prov, (prov_rfc, prov_nombre))
        
        for doc_data in documentos_a_procesar:
            uuid_proc = doc_data['uuid']
            monto_proc = doc_data['importe']
            fecha_proc = doc_data['fecha']
            
            sql_select_doc = "SELECT imp_pagado FROM Precios_Documentos WHERE uuid_original = %s"
            resultado_db = ejecutar_consulta_db(sql_select_doc, (uuid_proc,), fetch_one=True)
            
            if resultado_db is None:
                cambios_detectados.append(f"💰 NUEVO GASTO REGISTRADO: UUID {uuid_proc} = ${monto_proc:.2f}")
                sql_insert_doc = """
                    INSERT INTO Precios_Documentos (uuid_original, rfc_emisor, imp_pagado, fecha_pago) 
                    VALUES (%s, %s, %s, %s)
                """
                ejecutar_consulta_db(sql_insert_doc, (uuid_proc, prov_rfc, monto_proc, fecha_proc))
            else:
                monto_pagado_antiguo = resultado_db[0]
                if monto_proc != monto_pagado_antiguo:
                    cambios_detectados.append(f"🔄 CAMBIO EN IMPORTE/PARCIALIDAD: UUID {uuid_proc}. Antes: ${monto_pagado_antiguo:.2f} | Ahora: ${monto_proc:.2f}")
                    sql_update_doc = """
                        UPDATE Precios_Documentos SET imp_pagado = %s, fecha_pago = %s WHERE uuid_original = %s
                    """
                    ejecutar_consulta_db(sql_update_doc, (monto_proc, fecha_proc, uuid_proc))
        
        # --- NUEVA LÓGICA: INVOCACIÓN AL MICROSERVICIO (Lambda 1: Verificador SAT) ---
        if uuid_doc != "UUID-NO-ENCONTRADO" and API_GATEWAY_BASE_URL:
            # Usamos el path del Microservicio 1
            microservicio_url = f"{API_GATEWAY_BASE_URL}/v1/validar_sat" 
            payload = {'uuid': uuid_doc}
            
            try:
                # Llamada al Microservicio vía API Gateway
                response = requests.post(microservicio_url, json=payload, timeout=5) 
                
                if response.status_code == 200:
                    validacion = response.json()
                    # Añade el resultado de la Lambda a los resultados
                    cambios_detectados.append(f"🤖 Microservicio SAT: {validacion.get('validacion_status')} (Cód.: {validacion.get('codigo_sat')})")
                else:
                    cambios_detectados.append(f"⚠️ Microservicio SAT Falló: HTTP {response.status_code}. Revisar Logs de API Gateway.")
            except requests.exceptions.RequestException as e:
                cambios_detectados.append(f"🚨 Error Conexión API: No se pudo contactar al API Gateway. {str(e)}")


        if not cambios_detectados:
             cambios_detectados.append("✅ INFO: Factura procesada y registrada sin cambios relevantes.")

        return cambios_detectados
    
    except ConnectionError as e:
        return [f"❌ ERROR CRÍTICO DB: {e}"]
    except ET.ParseError:
        return ["❌ ERROR DE FORMATO: El archivo XML está mal formado o no es un CFDI válido."]
    except Exception as e:
        return [f"❌ ERROR INESPERADO: Fallo en la lógica del programa. {type(e).__name__}: {str(e)}"]

# --- 4. RUTAS DE FLASK: INTERFAZ DE PROCESAMIENTO Y REPORTE ---

@app.route('/', methods=['GET', 'POST'])
def index():
    """Ruta principal para subir el archivo XML."""
    if request.method == 'POST':
        if 'xml_file' not in request.files or request.files['xml_file'].filename == '':
            session['flash'] = {'type': 'warning', 'title': 'Atención', 'text': 'Ningún archivo seleccionado.'}
            return redirect(url_for('index'))
        
        file = request.files['xml_file']
        if file and file.filename.endswith('.xml'):
            xml_content = file.read()
            results = logica_procesamiento(xml_content)
            
            if results and len(results) > 0:
                if "❌ ERROR" in results[0] or "🚨 Error" in results[0]:
                    flash_message = {'type': 'error', 'title': '¡Error Crítico!', 'text': results[0]}
                elif any("NUEVO" in r or "CAMBIO" in r for r in results):
                    flash_message = {'type': 'success', 'title': '¡Procesamiento Exitoso con Cambios!', 'text': f'Se detectaron {len(results)} eventos. Revisa la sección de detalle.'}
                else: 
                    flash_message = {'type': 'info', 'title': 'Procesamiento Finalizado', 'text': results[0]} 
            else:
                flash_message = {'type': 'error', 'title': 'Error Desconocido', 'text': 'El procesamiento falló de manera inesperada. Revisar logs del servidor.'}

            session['flash'] = flash_message
            session['results'] = results
            return redirect(url_for('index'))
        else:
            session['flash'] = {'type': 'warning', 'title': 'Error de Archivo', 'text': 'El archivo debe ser un XML válido.'}
            return redirect(url_for('index'))

    flash_message = session.pop('flash', None)
    results = session.pop('results', None)
    
    return render_template('index.html', flash_message=flash_message, results=results)

@app.route('/reporte_historico')
def reporte_historico():
    """Ruta que obtiene los datos de MySQL para la visualización del reporte."""
    data = {'gasto_mensual': [], 'proveedores_top': [], 'documentos_historico': []}
    
    try:
        # CONSULTA 1: Gasto Total por Mes (últimos 12)
        sql_mensual = """
            SELECT 
                DATE_FORMAT(fecha_pago, '%%Y-%%m') AS Mes,
                SUM(imp_pagado) AS GastoTotal
            FROM Precios_Documentos
            GROUP BY Mes
            ORDER BY Mes DESC
            LIMIT 12;
        """
        resultados_mensuales = ejecutar_consulta_db(sql_mensual)
        
        if resultados_mensuales:
            data['gasto_mensual'] = {
                'labels': [row[0] for row in resultados_mensuales],
                'data': [float(row[1]) for row in resultados_mensuales]
            }

        # CONSULTA 2: Top 5 Proveedores por Monto Pagado
        sql_proveedores = """
            SELECT 
                p.nombre, 
                SUM(pd.imp_pagado) AS TotalPagado
            FROM Proveedores p
            JOIN Precios_Documentos pd ON p.rfc = pd.rfc_emisor
            GROUP BY p.nombre
            ORDER BY TotalPagado DESC
            LIMIT 5;
        """
        resultados_proveedores = ejecutar_consulta_db(sql_proveedores)
        
        if resultados_proveedores:
            data['proveedores_top'] = {
                'labels': [row[0] for row in resultados_proveedores],
                'data': [float(row[1]) for row in resultados_proveedores]
            }
            
        # CONSULTA 3: Histórico Detallado de Documentos (Para la tabla)
        sql_documentos_historico = """
            SELECT 
                pd.uuid_original, 
                p.nombre AS NombreProveedor,
                pd.imp_pagado, 
                pd.fecha_pago
            FROM Precios_Documentos pd
            JOIN Proveedores p ON pd.rfc_emisor = p.rfc
            ORDER BY pd.fecha_pago DESC
            LIMIT 20; 
        """
        resultados_documentos = ejecutar_consulta_db(sql_documentos_historico)
        if resultados_documentos:
            data['documentos_historico'] = [
                {
                    'id_documento': row[0],
                    'proveedor': row[1],
                    'importe_pagado': row[2],
                    'fecha_pago': row[3].strftime('%Y-%m-%d %H:%M:%S')
                } for row in resultados_documentos
            ]
            
    except ConnectionError as e:
        data['error'] = str(e)
    except Exception as e:
        data['error'] = f"Error al generar reporte: {str(e)}"

    return render_template('reporte.html', data=data) 

# --- 5. RUTA: GENERACIÓN DE REPORTE PDF ---

@app.route('/generar_reporte_pdf')
def generar_reporte_pdf():
    """Consulta todos los documentos y genera un PDF con fpdf2."""
    
    sql_documentos_completo = """
        SELECT 
            p.nombre AS NombreProveedor,
            pd.uuid_original, 
            pd.imp_pagado, 
            pd.fecha_pago
        FROM Precios_Documentos pd
        JOIN Proveedores p ON pd.rfc_emisor = p.rfc
        ORDER BY pd.fecha_pago DESC;
    """
    
    try:
        documentos = ejecutar_consulta_db(sql_documentos_completo)
    except ConnectionError as e:
         return f"Error de conexión a la base de datos: {e}", 500
    except Exception as e:
         return f"Error al consultar documentos para PDF: {e}", 500

    pdf = FPDF(orientation='L', unit='mm', format='Letter') 
    pdf.add_page()
    
    # Título
    pdf.set_font("Arial", 'B', 16)
    pdf.cell(0, 10, "Reporte Histórico de Gastos CFDI", 0, 1, 'C')
    pdf.set_font("Arial", '', 10)
    pdf.cell(0, 5, f"Fecha de Generación: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", 0, 1, 'C')
    pdf.ln(5)
    
    # Encabezados de la tabla
    pdf.set_fill_color(52, 73, 94)
    pdf.set_text_color(255, 255, 255)
    pdf.set_font("Arial", 'B', 8)
    
    col_widths = [60, 100, 30, 30]
    
    pdf.cell(col_widths[0], 7, "Proveedor", 1, 0, 'C', 1)
    pdf.cell(col_widths[1], 7, "UUID", 1, 0, 'C', 1)
    pdf.cell(col_widths[2], 7, "Importe (MXN)", 1, 0, 'C', 1)
    pdf.cell(col_widths[3], 7, "Fecha Pago", 1, 1, 'C', 1) 

    # Contenido de la tabla
    pdf.set_fill_color(240, 240, 240) 
    pdf.set_text_color(0, 0, 0)
    pdf.set_font("Arial", '', 8)
    
    fill = False
    
    for proveedor, uuid, importe, fecha in documentos:
        fecha_str = fecha.strftime('%Y-%m-%d') if fecha else 'N/A'
        
        pdf.cell(col_widths[0], 6, proveedor[:40], 1, 0, 'L', fill)
        pdf.cell(col_widths[1], 6, uuid, 1, 0, 'L', fill)
        pdf.cell(col_widths[2], 6, f"{importe:.2f}", 1, 0, 'R', fill)
        pdf.cell(col_widths[3], 6, fecha_str, 1, 1, 'C', fill)
        fill = not fill
        
    # Guardar y enviar el archivo
    pdf_filename = "reporte_historico.pdf"
    pdf.output(pdf_filename)
    
    return send_file(
        pdf_filename, 
        as_attachment=True, 
        download_name='Reporte_CFDI_Historico.pdf', 
        mimetype='application/pdf'
    )

if __name__ == '__main__':
    # Esta sección sólo se usa para desarrollo local y fallará si no configuras el entorno
    print(f"Flask corriendo. DB Host: {DB_HOST}")
    if not API_GATEWAY_BASE_URL.startswith('http://localhost'):
         print("🚨 ¡ALERTA! API_GATEWAY_BASE_URL NO configurada para local. La invocación al microservicio fallará.")
    app.run(debug=True, host='0.0.0.0', port=5000)