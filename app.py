import streamlit as st
import pandas as pd
import io
import datetime
import math
from dateutil.relativedelta import relativedelta
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload

# Configuración de la página
st.set_page_config(page_title="Planeación Mensual - CRA", layout="wide")
st.title("💎 CRA INT: Planeación Mensual de Compras")

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

# --- CARGA AUTOMÁTICA DEL INVENTARIO MAESTRO ---
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
            df_inv['ALMACEN'] = df_inv['ALMACEN'].astype(str).str.strip()
            df_inv['SUCURSAL'] = df_inv['SUCURSAL'].astype(str).str.strip()
            return df_inv
        except Exception: return None
    return None

# --- REGLAS DE NEGOCIO: ALMACÉN DE APOYO ---
def determinar_apoyo(suc_local, alm_local):
    """
    Define automáticamente contra qué almacén se va a comparar (Apoyo).
    Retorna: (sucursal_apoyo, almacen_apoyo)
    """
    alm = str(alm_local).upper().strip()
    suc = str(suc_local).upper().strip()
    
    if "UTEP" in alm:
        return "CUAUTITLAN", "ALM. GENERAL"
    elif "BISONTE" in alm:
        return "TULTITLAN", "ALM. GENERAL"
    elif "GENERAL" in alm:
        if suc == "CUAUTITLAN":
            return "TULTITLAN", "ALM. GENERAL"
        elif suc == "TULTITLAN":
            return "CUAUTITLAN", "ALM. GENERAL"
        else:
            return "CUAUTITLAN", "ALM. GENERAL" # Respaldo
    else:
        # Para almacenes secundarios normales (ej. BOÑAR), comparan contra su propio General
        return suc, "ALM. GENERAL"

# --- LÓGICA DE VENTAS HISTÓRICAS ---
def buscar_archivos_ventas(agencia, anios):
    archivos_encontrados = []
    if not MASTER_SALES_ID: return []
    for anio in anios:
        query = f"name contains '{agencia}' and name contains '{anio}' and name contains 'MASTER' and '{MASTER_SALES_ID}' in parents and trashed=false"
        results = drive_service.files().list(q=query, fields="files(id, name)", supportsAllDrives=True, includeItemsFromAllDrives=True).execute()
        archivos_encontrados.extend(results.get('files', []))
    return archivos_encontrados

def clasificar_movimiento(row):
    m10, v12 = row['meses_distintos_10m'], row['ventas_meses_11_y_12']
    if m10 >= 6: return 'ALTO MOVIMIENTO'
    if m10 >= 3: return 'MEDIO MOVIMIENTO'
    if m10 >= 1: return 'BAJO MOVIMIENTO'
    if v12 > 0: return 'RIESGO'
    return 'OBSOLETO'

def procesar_transito(archivo):
    try:
        df = pd.read_excel(archivo)
        df.columns = df.columns.str.strip().str.upper()
        if "N° PARTE" in df.columns: df.rename(columns={"N° PARTE": "NP"}, inplace=True)
        if "TRANSITO" not in df.columns: df["TRANSITO"] = 0
        df["NP"] = df["NP"].astype(str).str.strip()
        df["TRANSITO"] = pd.to_numeric(df["TRANSITO"], errors='coerce').fillna(0)
        return df.groupby("NP", as_index=False)["TRANSITO"].sum()
    except Exception: return pd.DataFrame(columns=["NP", "TRANSITO"])

def procesar_traspasos(archivo, filtro):
    try:
        engine = 'xlrd' if archivo.name.endswith('.xls') else 'openpyxl'
        df = pd.read_excel(archivo, header=None, engine=engine)
        df = df[df[0].astype(str).str.strip() == filtro].copy()
        if df.empty: return pd.DataFrame(columns=["NP", "CANTIDAD_TRASPASO"])
        df = df[[2, 4]].copy()
        df.columns = ["NP", "CANTIDAD_TRASPASO"]
        df["NP"] = df["NP"].astype(str).str.strip()
        df["CANTIDAD_TRASPASO"] = pd.to_numeric(df["CANTIDAD_TRASPASO"], errors='coerce').fillna(0).abs()
        return df.groupby("NP", as_index=False)["CANTIDAD_TRASPASO"].sum()
    except Exception: return pd.DataFrame(columns=["NP", "CANTIDAD_TRASPASO"])

def extraer_metricas_ventas(sucursal, almacen, df_inventario_filtrado, bar_obj, progress_start, progress_end):
    hoy = datetime.datetime.now()
    fecha_inicio_12m = hoy - relativedelta(months=12)
    fecha_inicio_10m = hoy - relativedelta(months=10)
    anios_drive = list(set([fecha_inicio_12m.year, hoy.year]))
    
    files_metadata = buscar_archivos_ventas(sucursal.upper(), anios_drive)
    dfs = []
    
    for i, file_meta in enumerate(files_metadata):
        content = descargar_archivo_drive(file_meta['id'])
        if content:
            try:
                engine = 'xlrd' if 'xls' in file_meta['name'] and 'xlsx' not in file_meta['name'] else 'openpyxl'
                df_temp = pd.read_excel(content, engine=engine)
                df_temp.columns = df_temp.columns.str.upper().str.strip()
                dfs.append(df_temp)
            except Exception: pass
        current_prog = progress_start + (progress_end - progress_start) * ((i + 1) / max(len(files_metadata), 1))
        bar_obj.progress(int(current_prog), text=f"Consultando ventas de {sucursal}...")
            
    if not dfs: 
        df_vacio = df_inventario_filtrado[['NP']].drop_duplicates().copy()
        df_vacio[['HITS', 'CONSUMO MENSUAL', 'meses_distintos_10m', 'ventas_meses_11_y_12']] = 0
        return df_vacio
    
    df_total = pd.concat(dfs, ignore_index=True)
    if 'ALMACEN' in df_total.columns:
        df_total['ALMACEN'] = df_total['ALMACEN'].astype(str).str.strip()
        df_total = df_total[df_total['ALMACEN'] == almacen]
        
    if df_total.empty or 'FECHA' not in df_total.columns:
        df_vacio = df_inventario_filtrado[['NP']].drop_duplicates().copy()
        df_vacio[['HITS', 'CONSUMO MENSUAL', 'meses_distintos_10m', 'ventas_meses_11_y_12']] = 0
        return df_vacio
        
    df_total['FECHA'] = pd.to_datetime(df_total['FECHA'], dayfirst=True, errors='coerce')
    df_total['NP'] = df_total['NP'].astype(str).str.strip()
    df_total['CANTIDAD'] = pd.to_numeric(df_total['CANTIDAD'], errors='coerce').fillna(0)

    df_10m = df_total[(df_total['FECHA'] >= fecha_inicio_10m) & (df_total['FECHA'] <= hoy)]
    df_11_12 = df_total[(df_total['FECHA'] >= fecha_inicio_12m) & (df_total['FECHA'] < fecha_inicio_10m)]
    df_12m = df_total[(df_total['FECHA'] >= fecha_inicio_12m) & (df_total['FECHA'] <= hoy)]

    df_10m_pos = df_10m[df_10m['CANTIDAD'] > 0].copy()
    if not df_10m_pos.empty:
        df_10m_pos['PERIODO'] = df_10m_pos['FECHA'].dt.strftime('%Y-%m')
        meses_10m = df_10m_pos.groupby('NP')['PERIODO'].nunique().reset_index().rename(columns={'PERIODO': 'meses_distintos_10m'})
    else: meses_10m = pd.DataFrame(columns=['NP', 'meses_distintos_10m'])

    ventas_11_12 = df_11_12[df_11_12['CANTIDAD'] > 0].groupby('NP').size().reset_index().rename(columns={0: 'ventas_meses_11_y_12'})

    metricas_12m = df_12m.groupby('NP').agg(
        total_eventos=('CANTIDAD', 'count'), eventos_negativos=('CANTIDAD', lambda x: (x < 0).sum()), suma_cantidad=('CANTIDAD', 'sum')
    ).reset_index()
    
    metricas_12m['HITS'] = (metricas_12m['total_eventos'] - (metricas_12m['eventos_negativos'] * 2)).clip(lower=0)
    metricas_12m['CONSUMO MENSUAL'] = metricas_12m['suma_cantidad'] / 12

    df_base = df_inventario_filtrado[['NP']].drop_duplicates()
    df_base = pd.concat([df_base, metricas_12m[['NP']]]).drop_duplicates().reset_index(drop=True)
    
    df_base = pd.merge(df_base, meses_10m, on='NP', how='left').fillna(0)
    df_base = pd.merge(df_base, ventas_11_12, on='NP', how='left').fillna(0)
    df_base = pd.merge(df_base, metricas_12m[['NP', 'HITS', 'CONSUMO MENSUAL']], on='NP', how='left').fillna(0)
    
    return df_base

# --- DISEÑO EXCEL ---
def formatear_excel_planeacion(writer, df, sheet_name, cols_apoyo):
    workbook = writer.book
    worksheet = writer.sheets[sheet_name]
    
    # Congelar paneles (Fila 1 fija)
    worksheet.freeze_panes(1, 0)
    
    fmt_base = workbook.add_format({'bold': True, 'valign': 'vcenter', 'align': 'center', 'bg_color': '#10345C', 'font_color': 'white', 'border': 1})
    fmt_local = workbook.add_format({'bold': True, 'valign': 'vcenter', 'align': 'center', 'bg_color': '#4B8BBE', 'font_color': 'white', 'border': 1})
    fmt_foraneo = workbook.add_format({'bold': True, 'valign': 'vcenter', 'align': 'center', 'bg_color': '#A64d4d', 'font_color': 'white', 'border': 1})
    fmt_input = workbook.add_format({'bold': True, 'valign': 'vcenter', 'align': 'center', 'bg_color': '#F2F2F2', 'font_color': 'black', 'border': 1})
    cell_fmt = workbook.add_format({'align': 'center', 'valign': 'vcenter', 'border': 1, 'border_color': '#D3D3D3'})
    
    # 1. PINTAR ENCABEZADOS
    for col_num, value in enumerate(df.columns.values):
        col_name = str(value).upper()
        
        if col_num < 4: 
            style = fmt_base
        elif "NUEVO TRASPASO" in col_name or "CANTIDAD A TRASPASAR" in col_name:
            style = fmt_input
        elif col_name in cols_apoyo:
            style = fmt_foraneo
        else: 
            style = fmt_local
            
        worksheet.write(0, col_num, value, style)
    
    worksheet.set_column('A:A', 18, cell_fmt)
    worksheet.set_column('B:B', 40, cell_fmt) 
    worksheet.set_column('C:Z', 15, cell_fmt)
    
    # 2. INYECTAR FÓRMULAS EXCEL ROW BY ROW
    # Orden de columnas en el DF generado:
    # A=NP, B=DESC, C=LINEA, D=CLASIFICACION, E=SUGERIDO, F=EXIST LOCAL, G=FEC COMPRA LOCAL, H=PROM LOCAL, I=HITS LOCAL
    # J=INV APOYO, K=PROM APOYO, L=HITS APOYO, M=FEC APOYO, N=TRANSITO, O=TRASPASO APOYO A LOCAL
    # P=NUEVO TRASPASO, Q=CANT TRASPASAR, R=INV TOTAL, S=MESES VTA ACT, T=POR FINCAR
    
    start_row = 1
    for i in range(len(df)):
        row = start_row + i
        excel_row = row + 1 
        
        # P: NUEVO TRASPASO (Validación Lista SI/NO)
        worksheet.data_validation(row, 15, row, 15, {'validate': 'list', 'source': ['SI', 'NO']})
        
        # R: INV. TOTAL = F (Existencia) + N (Transito) + O (Traspaso Viejo) + Q (Cantidad Traspasar Nueva)
        f_inv_total = f'=F{excel_row}+N{excel_row}+O{excel_row}+Q{excel_row}'
        worksheet.write_formula(row, 17, f_inv_total, cell_fmt)
        
        # S: MESES VENTA ACTUAL = R (Inv Total) / H (Promedio Local)
        f_meses_act = f'=IFERROR(R{excel_row}/H{excel_row}, 0)'
        worksheet.write_formula(row, 18, f_meses_act, cell_fmt)
        
        # T: POR FINCAR = Sugerido (E) - Cantidad a Traspasar (Q) (Y que no sea negativo)
        f_por_fincar = f'=IF((E{excel_row}-Q{excel_row})>0, E{excel_row}-Q{excel_row}, 0)'
        worksheet.write_formula(row, 19, f_por_fincar, cell_fmt)

# --- INTERFAZ GRAFICA ---
with st.spinner("⏳ Conectando con Drive y descargando Inventario Maestro..."):
    df_inventario_maestro = cargar_inventario_maestro()

if df_inventario_maestro is not None:
    st.success("✅ Base de Datos Conectada.")
    
    # Creamos lista combinada "SUCURSAL - ALMACEN" para evitar duplicados visuales
    df_inventario_maestro['SUC_ALM'] = df_inventario_maestro['SUCURSAL'].astype(str) + " - " + df_inventario_maestro['ALMACEN'].astype(str)
    lista_opciones = sorted(df_inventario_maestro['SUC_ALM'].dropna().unique().tolist())
    
    st.markdown("### 1. Selección de Parámetros")
    opcion_seleccionada = st.selectbox("🏬 Selecciona la Sucursal y Almacén a Planear (Local):", lista_opciones)
    
    # Separar la selección
    sucursal_local, almacen_local = opcion_seleccionada.split(" - ", 1)
    
    # Motor de Reglas Automáticas
    sucursal_apoyo, almacen_apoyo = determinar_apoyo(sucursal_local, almacen_local)
    
    # Mostrar la lógica deducida al usuario para que sepa qué está pasando
    st.info(f"📍 **Almacén Local a planear:** {almacen_local} ({sucursal_local})\n\n🤝 **El sistema cruzará automáticamente con:** {almacen_apoyo} ({sucursal_apoyo})")

    st.markdown("---")
    col_c, col_d = st.columns([1, 2])
    meses_cob = col_c.number_input("🎯 Meta de Cobertura (Meses):", min_value=0.5, value=1.5, step=0.5)
    
    st.markdown("### 2. Archivos en Tránsito y Situación")
    col3, col4 = st.columns(2)
    file_transito = col3.file_uploader("🚢 Archivo de Tránsito (Local)", type=["xlsx"])
    filtro_traspaso = col4.text_input(f"Nomenclatura Traspaso hacia {sucursal_local} (Ej. TRASUCTU):", "")
    file_traspasos = col4.file_uploader("🚛 Archivo Situación (Traspasos)", type=["xlsx", "xls"])

    if st.button("🚀 Generar Planeación Mensual"):
        my_bar = st.progress(5, text="Iniciando cálculos...")
        
        # 1. Extraer Local
        df_inv_local = df_inventario_maestro[(df_inventario_maestro['ALMACEN'] == almacen_local) & (df_inventario_maestro['SUCURSAL'] == sucursal_local)]
        df_local = extraer_metricas_ventas(sucursal_local, almacen_local, df_inv_local, my_bar, 10, 45)
        
        inv_resumen_local = df_inv_local.groupby('NP').agg({'DESCRIPCION': 'first', 'LINEA': 'first', 'EXISTENCIA': 'sum', 'FEC_ULT_COMPRA': 'first'}).reset_index()
        df_final = pd.merge(df_local, inv_resumen_local, on='NP', how='left')
        df_final['EXISTENCIA'] = df_final['EXISTENCIA'].fillna(0)
        df_final['CLASIFICACIÓN'] = df_final.apply(clasificar_movimiento, axis=1)
        
        # 2. Extraer Apoyo
        my_bar.progress(50, text=f"Procesando datos del apoyo: {almacen_apoyo}...")
        df_inv_apoyo = df_inventario_maestro[(df_inventario_maestro['ALMACEN'] == almacen_apoyo) & (df_inventario_maestro['SUCURSAL'] == sucursal_apoyo)]
        df_apoyo = extraer_metricas_ventas(sucursal_apoyo, almacen_apoyo, df_inv_apoyo, my_bar, 55, 80)
        inv_resumen_apoyo = df_inv_apoyo.groupby('NP').agg({'EXISTENCIA': 'sum', 'FEC_ULT_COMPRA': 'first'}).reset_index()
        
        df_apoyo_completo = pd.merge(df_apoyo, inv_resumen_apoyo, on='NP', how='left')
        
        # Generar nombres dinámicos para columnas de apoyo
        col_inv_apoyo = f'INV. {almacen_apoyo} ({sucursal_apoyo})'
        col_prom_apoyo = f'PROM. {almacen_apoyo} ({sucursal_apoyo})'
        col_hits_apoyo = f'HITS {almacen_apoyo} ({sucursal_apoyo})'
        col_fec_apoyo = f'FEC ULT COMPRA {almacen_apoyo} ({sucursal_apoyo})'
        nombre_traspaso = f'TRASPASO {almacen_apoyo} A {almacen_local}'
        
        df_apoyo_completo.rename(columns={
            'EXISTENCIA': col_inv_apoyo,
            'CONSUMO MENSUAL': col_prom_apoyo,
            'HITS': col_hits_apoyo,
            'FEC_ULT_COMPRA': col_fec_apoyo
        }, inplace=True)
        
        # Cruzar con Local
        cols_to_merge = ['NP', col_inv_apoyo, col_prom_apoyo, col_hits_apoyo, col_fec_apoyo]
        df_final = pd.merge(df_final, df_apoyo_completo[cols_to_merge], on='NP', how='left').fillna(0)
        
        my_bar.progress(85, text="Aplicando tránsitos y matemáticas...")
        # 3. Transitos y Traspasos
        df_transito = procesar_transito(file_transito) if file_transito else pd.DataFrame(columns=["NP", "TRANSITO"])
        df_traspasos = procesar_traspasos(file_traspasos, filtro_traspaso) if (file_traspasos and filtro_traspaso) else pd.DataFrame(columns=["NP", "CANTIDAD_TRASPASO"])
        
        df_final = pd.merge(df_final, df_transito, on='NP', how='left').fillna(0)
        df_final = pd.merge(df_final, df_traspasos, on='NP', how='left').fillna(0)
        df_final.rename(columns={'CANTIDAD_TRASPASO': nombre_traspaso}, inplace=True)

        # 4. CÁLCULO DEL SUGERIDO MENSUAL
        df_final['SUGERIDO MENSUAL'] = (df_final['CONSUMO MENSUAL'] * meses_cob) - df_final['EXISTENCIA'] - df_final['TRANSITO']
        df_final['SUGERIDO MENSUAL'] = df_final['SUGERIDO MENSUAL'].apply(lambda x: math.ceil(x) if x > 0 else 0)

        # Agregamos las columnas "Input" y "Formula" vacías para estructurar el Excel
        df_final['NUEVO TRASPASO'] = ''
        df_final['CANTIDAD A TRASPASAR'] = 0
        df_final['INV. TOTAL'] = 0
        df_final['MESES VENTA ACTUAL'] = 0
        df_final['POR FINCAR'] = 0

        # 5. ORDENAR COLUMNAS PARA EL EXCEL (20 Columnas Exactas)
        df_final.rename(columns={'CONSUMO MENSUAL': f'PROMEDIO LOCAL', 'HITS': 'HITS LOCAL'}, inplace=True)
        
        columnas_ordenadas = [
            'NP', 'DESCRIPCION', 'LINEA', 'CLASIFICACIÓN', 'SUGERIDO MENSUAL', 
            'EXISTENCIA', 'FEC_ULT_COMPRA', 'PROMEDIO LOCAL', 'HITS LOCAL',
            col_inv_apoyo, col_prom_apoyo, col_hits_apoyo, col_fec_apoyo,
            'TRANSITO', nombre_traspaso,
            'NUEVO TRASPASO', 'CANTIDAD A TRASPASAR', 
            'INV. TOTAL', 'MESES VENTA ACTUAL', 'POR FINCAR'
        ]
        
        df_export = df_final[columnas_ordenadas].copy()
        
        # 6. EXPORTAR A EXCEL
        my_bar.progress(95, text="🎨 Generando Excel Corporativo...")
        buffer = io.BytesIO()
        with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
            nombre_hoja = almacen_local[:30].replace('/', '-') 
            df_export.to_excel(writer, sheet_name=nombre_hoja, index=False)
            
            # Pasamos los nombres exactos de las columnas de apoyo para que la función sepa cuáles pintar de Rojo
            cols_apoyo = [col_inv_apoyo.upper(), col_prom_apoyo.upper(), col_hits_apoyo.upper(), col_fec_apoyo.upper(), nombre_traspaso.upper()]
            formatear_excel_final(writer, df_export, nombre_hoja, cols_apoyo)
            
        buffer.seek(0)
        fecha_str = datetime.datetime.now().strftime("%d_%m_%Y")
        name_file = f"Planeacion_{almacen_local.replace(' ', '')}_{fecha_str}.xlsx"
        
        link = subir_excel_a_drive(buffer, name_file)
        my_bar.progress(100, text="✅ ¡Completado!")
        
        if link:
            st.success(f"✅ Reporte Maestro Creado: {name_file}")
            st.markdown(f"### [📂 Abrir en Google Drive]({link})")

else:
    st.warning("⚠️ Revisa los accesos del robot al archivo INVENTARIO_CRA.")
