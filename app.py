import streamlit as st
import pandas as pd
import io
import datetime
from dateutil.relativedelta import relativedelta
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload

# Configuración de la página
st.set_page_config(page_title="Consignas - CRA", layout="wide")
st.title("💎 CRA INT: Análisis Global de Consignas")
st.markdown("Generación automatizada de inventarios y sugeridos para almacenes foráneos y consignas.")

# --- CONFIGURACIÓN GOOGLE DRIVE ---
@st.cache_resource
def get_drive_service():
    try:
        gcp_creds = dict(st.secrets["gcp_service_account"])
        creds = service_account.Credentials.from_service_account_info(
            gcp_creds, scopes=['https://www.googleapis.com/auth/drive']
        )
        return build('drive', 'v3', credentials=creds)
    except Exception as e:
        st.error(f"⚠️ Error de conexión: {e}")
        st.stop()

drive_service = get_drive_service()
MASTER_SALES_ID = st.secrets["general"].get("master_sales_id")
INVENTORY_FOLDER_ID = st.secrets["general"].get("inventory_folder_id")
PARENT_FOLDER_ID = st.secrets["general"]["drive_folder_id"]

# --- FUNCIONES DRIVE ---
def descargar_archivo_drive(file_id):
    try:
        request = drive_service.files().get_media(fileId=file_id)
        file = io.BytesIO()
        downloader = MediaIoBaseDownload(file, request)
        done = False
        while done is False: status, done = downloader.next_chunk()
        file.seek(0)
        return file
    except Exception: return None

def buscar_o_crear_carpeta(nombre_carpeta, parent_id):
    try:
        query = f"mimeType='application/vnd.google-apps.folder' and name='{nombre_carpeta}' and '{parent_id}' in parents and trashed=false"
        results = drive_service.files().list(q=query, fields="files(id, name)", supportsAllDrives=True, includeItemsFromAllDrives=True).execute()
        files = results.get('files', [])
        if files: return files[0]['id']
        else:
            metadata = {'name': nombre_carpeta, 'mimeType': 'application/vnd.google-apps.folder', 'parents': [parent_id]}
            folder = drive_service.files().create(body=metadata, fields='id', supportsAllDrives=True).execute()
            return folder.get('id')
    except Exception: return None

def subir_excel_a_drive(buffer, nombre_archivo):
    try:
        fecha_hoy = datetime.datetime.now()
        anio = str(fecha_hoy.year)
        meses_es = {1:"01_Enero", 2:"02_Febrero", 3:"03_Marzo", 4:"04_Abril", 5:"05_Mayo", 6:"06_Junio", 7:"07_Julio", 8:"08_Agosto", 9:"09_Septiembre", 10:"10_Octubre", 11:"11_Noviembre", 12:"12_Diciembre"}
        mes_carpeta = meses_es[fecha_hoy.month]

        id_anio = buscar_o_crear_carpeta(anio, PARENT_FOLDER_ID)
        id_mes = buscar_o_crear_carpeta(mes_carpeta, id_anio)
        
        media = MediaIoBaseUpload(buffer, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', resumable=True)
        file_metadata = {'name': nombre_archivo, 'parents': [id_mes]}
        archivo = drive_service.files().create(body=file_metadata, media_body=media, fields='id, webViewLink', supportsAllDrives=True).execute()
        return archivo.get('webViewLink')
    except Exception: return None

# --- CARGA INVENTARIO MAESTRO ---
@st.cache_data(ttl=3600)
def cargar_inventario_maestro():
    if not INVENTORY_FOLDER_ID: return None
    query = f"name contains 'INVENTARIO_CRA' and '{INVENTORY_FOLDER_ID}' in parents and trashed=false"
    results = drive_service.files().list(q=query, fields="files(id, name)", supportsAllDrives=True, includeItemsFromAllDrives=True).execute()
    files = results.get('files', [])
    if not files: return None
    content = descargar_archivo_drive(files[0]['id'])
    if content:
        try:
            engine = 'xlrd' if 'xls' in files[0]['name'] and 'xlsx' not in files[0]['name'] else 'openpyxl'
            df_inv = pd.read_excel(content, engine=engine)
            df_inv.columns = df_inv.columns.str.upper().str.strip()
            df_inv['NP'] = df_inv['NP'].astype(str).str.strip()
            df_inv['ALMACEN'] = df_inv['ALMACEN'].astype(str).str.strip().str.upper()
            return df_inv
        except Exception: return None
    return None

# --- EXTRACCIÓN MASIVA DE VENTAS ---
def buscar_archivos_ventas(agencia, anios):
    archivos_encontrados = []
    if not MASTER_SALES_ID: return []
    for anio in anios:
        query = f"name contains '{agencia}' and name contains '{anio}' and name contains 'MASTER' and '{MASTER_SALES_ID}' in parents and trashed=false"
        results = drive_service.files().list(q=query, fields="files(id, name)", supportsAllDrives=True, includeItemsFromAllDrives=True).execute()
        archivos_encontrados.extend(results.get('files', []))
    return archivos_encontrados

@st.cache_data(ttl=3600)
def descargar_todas_las_ventas_12m():
    hoy = datetime.datetime.now()
    fecha_fin = hoy.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    fecha_inicio = fecha_fin - relativedelta(years=1)
    anios_drive = list(set([fecha_inicio.year, fecha_fin.year]))
    
    sucursales = ["CUAUTITLAN", "TULTITLAN", "BAJIO"]
    files_metadata = []
    for suc in sucursales:
        files_metadata.extend(buscar_archivos_ventas(suc, anios_drive))
        
    dfs = []
    for i, file_meta in enumerate(files_metadata):
        content = descargar_archivo_drive(file_meta['id'])
        if content:
            try:
                engine = 'xlrd' if 'xls' in file_meta['name'] and 'xlsx' not in file_meta['name'] else 'openpyxl'
                df_temp = pd.read_excel(content, engine=engine)
                df_temp.columns = df_temp.columns.str.upper().str.strip()
                # Filtrar solo columnas necesarias para ahorrar memoria
                cols_utiles = [c for c in df_temp.columns if c in ['NP', 'DESCR', 'FECHA', 'ALMACEN', 'CANTIDAD']]
                dfs.append(df_temp[cols_utiles])
            except Exception: pass
            
    if not dfs: return None, fecha_inicio, fecha_fin
    
    df_global = pd.concat(dfs, ignore_index=True)
    df_global['FECHA'] = pd.to_datetime(df_global['FECHA'], dayfirst=True, errors='coerce')
    
    # Filtrar estrictamente 12 meses
    mask = (df_global['FECHA'] >= fecha_inicio) & (df_global['FECHA'] < fecha_fin)
    df_global = df_global[mask].copy()
    
    df_global['NP'] = df_global['NP'].astype(str).str.strip()
    df_global['ALMACEN'] = df_global['ALMACEN'].astype(str).str.strip().str.upper()
    df_global['CANTIDAD'] = pd.to_numeric(df_global['CANTIDAD'], errors='coerce').fillna(0)
    
    return df_global, fecha_inicio, fecha_fin

# --- LISTADOS Y COLORES POR ZONA ---
ALMACENES_CUAUTI = ["ALM. BOÑAR", "ALM. FAST FOOD", "ALM. LIPU", "ALM. MYM", "ALM. UTEP"]
ALMACENES_TULTI = ["ALM. ENLACES LOGISTICOS", "ALMACEN AFN", "BISONTE TEPOTZOTLAN", "CULVERT", "TDR", "TEISA", "TUMSA", "ZONTE"]
ALMACENES_BAJIO = ["ALM. UTEP SAN LUIS", "BISONTE SLP"]
TODOS_ALMACENES = ALMACENES_CUAUTI + ALMACENES_TULTI + ALMACENES_BAJIO

def obtener_color_pestana(almacen):
    alm = almacen.upper()
    if alm in [x.upper() for x in ALMACENES_CUAUTI]: return '#4B8BBE' # Azul
    if alm in [x.upper() for x in ALMACENES_TULTI]: return '#FF9999' # Rojo Claro
    if alm in [x.upper() for x in ALMACENES_BAJIO]: return '#99FF99' # Verde Claro
    return '#FFFFFF'

# --- GENERADOR DE EXCEL MULTIPESTAÑA ---
def crear_excel_consignas(df_ventas, df_inv):
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
        workbook = writer.book
        
        # Formatos
        fmt_blue = workbook.add_format({'bold': True, 'valign': 'vcenter', 'align': 'center', 'bg_color': '#10345C', 'font_color': 'white', 'border': 1})
        fmt_gray = workbook.add_format({'bold': True, 'valign': 'vcenter', 'align': 'center', 'bg_color': '#D3D3D3', 'font_color': 'black', 'border': 1})
        fmt_white = workbook.add_format({'bold': True, 'valign': 'vcenter', 'align': 'center', 'border': 1})
        cell_fmt = workbook.add_format({'align': 'center', 'valign': 'vcenter', 'border': 1, 'border_color': '#D3D3D3'})
        
        # 1. HOJA "CONSIGNAS" (Indice/Portada)
        ws_cons = workbook.add_worksheet("CONSIGNAS")
        ws_cons.set_tab_color('#D3D3D3') # Gris Claro
        ws_cons.write("A1", "REPORTE GLOBAL DE CONSIGNAS", fmt_blue)
        ws_cons.set_column('A:A', 40)
        
        # 2. PROCESAR CADA ALMACÉN
        for alm in TODOS_ALMACENES:
            # Filtrar ventas del almacen
            df_v_alm = df_ventas[df_ventas['ALMACEN'] == alm.upper()]
            
            if df_v_alm.empty:
                # Si no hay ventas, pasamos al siguiente o creamos hoja vacia
                resumen = pd.DataFrame(columns=['NP', 'DESCR', 'VENTA', 'HITS'])
            else:
                # Agrupar ventas (Venta Total y HITS)
                resumen = df_v_alm.groupby('NP').agg(
                    DESCR=('DESCR', 'first'),
                    VENTA=('CANTIDAD', 'sum'),
                    total_ev=('CANTIDAD', 'count'),
                    neg_ev=('CANTIDAD', lambda x: (x < 0).sum())
                ).reset_index()
                # HITS: Total eventos positivos y quitamos los negativos dobles
                resumen['HITS'] = (resumen['total_ev'] - (resumen['neg_ev'] * 2)).clip(lower=0)
            
            # Cruzar con inventario para Existencia
            if df_inv is not None and not df_inv.empty:
                df_i_alm = df_inv[df_inv['ALMACEN'] == alm.upper()]
                inv_exist = df_i_alm.groupby('NP')['EXISTENCIA'].sum().reset_index()
                resumen = pd.merge(resumen, inv_exist, on='NP', how='left')
                resumen['EXISTENCIA'] = resumen['EXISTENCIA'].fillna(0)
            else:
                resumen['EXISTENCIA'] = 0
            
            # Limpiar casos donde las ventas sean 0 y existencia 0 (Basura)
            if not resumen.empty:
                resumen = resumen[(resumen['VENTA'] != 0) | (resumen['HITS'] > 0)]
            
            # Preparar DataFrame de la Hoja
            df_hoja = pd.DataFrame()
            df_hoja['N° DE PARTE'] = resumen['NP'] if not resumen.empty else []
            df_hoja['DESCR'] = resumen['DESCR'] if not resumen.empty else []
            df_hoja['VENTA'] = resumen['VENTA'] if not resumen.empty else []
            df_hoja['HITS'] = resumen['HITS'] if not resumen.empty else []
            df_hoja['DEMANDA'] = ''
            df_hoja['PROMEDIO (12)'] = ''
            df_hoja['MIN (1)'] = ''
            df_hoja['MAX (3)'] = ''
            df_hoja['INVENTARIO EXISTENCIA'] = resumen['EXISTENCIA'] if not resumen.empty else []
            df_hoja['VENTA ACTUAL'] = ''
            df_hoja['EXCESO INVENTARIO'] = ''
            df_hoja['TRASPASO REQUERIDO'] = ''
            df_hoja['COMENTARIOS'] = ''
            
            sheet_name = alm[:31] # Excel limit
            df_hoja.to_excel(writer, sheet_name=sheet_name, index=False)
            ws = writer.sheets[sheet_name]
            
            # Colores de la pestaña
            ws.set_tab_color(obtener_color_pestana(alm))
            ws.freeze_panes(1, 0)
            
            # Escribir Encabezados con colores
            columnas = df_hoja.columns.tolist()
            for col_num, col_name in enumerate(columnas):
                if col_name in ['N° DE PARTE', 'DESCR', 'VENTA', 'HITS']:
                    ws.write(0, col_num, col_name, fmt_blue)
                elif col_name == 'COMENTARIOS':
                    ws.write(0, col_num, col_name, fmt_white)
                else:
                    ws.write(0, col_num, col_name, fmt_gray)
            
            # Anchos de columna
            ws.set_column('A:A', 20, cell_fmt)
            ws.set_column('B:B', 45, cell_fmt)
            ws.set_column('C:L', 15, cell_fmt)
            ws.set_column('M:M', 30, cell_fmt) # Comentarios ancho
            
            # Fórmulas de Excel
            # A=0, B=1, C(VENTA)=2, D(HITS)=3, E(DEM)=4, F(PROM)=5, G(MIN)=6, H(MAX)=7, I(INV)=8, J(VTA ACT)=9, K(EXC)=10, L(TRASP)=11
            start_row = 1
            for i in range(len(df_hoja)):
                row = start_row + i
                ex_row = row + 1
                
                # E: DEMANDA -> Alta (>12), Media (6 a 12), Baja (<6)
                f_dem = f'=IF(D{ex_row}>12,"ALTA",IF(D{ex_row}>=6,"MEDIA","BAJA"))'
                ws.write_formula(row, 4, f_dem, cell_fmt)
                
                # F: PROMEDIO (12) -> Venta / 12
                f_prom = f'=IFERROR(C{ex_row}/12, 0)'
                ws.write_formula(row, 5, f_prom, cell_fmt)
                
                # G: MIN (1) -> Promedio * 1
                f_min = f'=F{ex_row}*1'
                ws.write_formula(row, 6, f_min, cell_fmt)
                
                # H: MAX (3) -> Promedio * 3
                f_max = f'=F{ex_row}*3'
                ws.write_formula(row, 7, f_max, cell_fmt)
                
                # J: VENTA ACTUAL -> Inventario / Promedio
                f_vtact = f'=IFERROR(I{ex_row}/F{ex_row}, 0)'
                ws.write_formula(row, 9, f_vtact, cell_fmt)
                
                # K: EXCESO INVENTARIO -> SI Inventario > Max
                f_exc = f'=IF(I{ex_row}>H{ex_row},"SI","NO")'
                ws.write_formula(row, 10, f_exc, cell_fmt)
                
                # L: TRASPASO REQUERIDO -> Inventario - Max
                f_trasp = f'=I{ex_row}-H{ex_row}'
                ws.write_formula(row, 11, f_trasp, cell_fmt)

    buffer.seek(0)
    return buffer

# --- INTERFAZ GRAFICA STREAMLIT ---
st.info("💡 Haz clic en el botón para que el sistema descargue todas las bases, filtre las fechas dinámicas y genere el Excel de 16 pestañas.")

if st.button("🚀 Generar Reporte de Consignas"):
    with st.spinner("Iniciando motor de descarga (Esto puede tomar un par de minutos)..."):
        
        # 1. Cargar Inventario General
        df_inv = cargar_inventario_maestro()
        if df_inv is None:
            st.error("No se pudo leer el archivo INVENTARIO_CRA de Drive.")
            st.stop()
            
        # 2. Cargar TODAS las ventas de los últimos 12 meses
        df_ventas, f_inicio, f_fin = descargar_todas_las_ventas_12m()
        if df_ventas is None:
            st.error("No se encontraron registros de ventas en Master Ventas.")
            st.stop()
            
        st.success(f"✅ Bases descargadas. Analizando periodo cerrado: **{f_inicio.strftime('%b %Y')} a { (f_fin - relativedelta(days=1)).strftime('%b %Y')}**")
        
        # 3. Generar el MEGA EXCEL
        with st.spinner("Procesando almacenes, calculando hits, promedios y coloreando pestañas..."):
            buffer_excel = crear_excel_consignas(df_ventas, df_inv)
            
        # 4. Subir a Drive
        with st.spinner("☁️ Subiendo archivo final a Google Drive..."):
            fecha_str = datetime.datetime.now().strftime("%d_%m_%Y")
            name_file = f"Analisis_Consignas_{fecha_str}.xlsx"
            link = subir_excel_a_drive(buffer_excel, name_file)
            
            if link:
                st.balloons()
                st.success(f"🎉 ¡Reporte Multi-Almacén Creado Exitosamente: {name_file}!")
                st.markdown(f"### [📂 Abrir Reporte de Consignas en Drive]({link})")
