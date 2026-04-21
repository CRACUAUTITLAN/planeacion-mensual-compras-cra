import streamlit as st
import pandas as pd
import io
import datetime
from dateutil.relativedelta import relativedelta
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload
from xlsxwriter.utility import xl_col_to_name

# Configuración de la página
st.set_page_config(page_title="Consignas - CRA", layout="wide")
st.title("💎 CRA INT: Análisis Global de Consignas")
st.markdown("Generación automatizada de inventarios y requerimientos para almacenes foráneos y consignas.")

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
    except Exception as e:
        print(f"Error al descargar archivo de Drive: {e}")
        return None

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

@st.cache_data(ttl=3600, show_spinner=False)
def cargar_inventario_maestro():
    if not INVENTORY_FOLDER_ID: return None
    try:
        query = f"name contains 'INVENTARIO_CRA' and '{INVENTORY_FOLDER_ID}' in parents and trashed=false"
        results = drive_service.files().list(q=query, fields="files(id, name)", supportsAllDrives=True, includeItemsFromAllDrives=True).execute()
        files = results.get('files', [])
        
        if not files: 
            print("No se encontró ningún archivo con 'INVENTARIO_CRA'")
            return None

        target_file = files[0]
        content = descargar_archivo_drive(target_file['id'])
        if content:
            engine = 'xlrd' if 'xls' in target_file['name'].lower() and 'xlsx' not in target_file['name'].lower() else 'openpyxl'
            df_inv = pd.read_excel(content, engine=engine)
            df_inv.columns = df_inv.columns.str.upper().str.strip()
            
            if 'NP' in df_inv.columns and 'ALMACEN' in df_inv.columns:
                df_inv['NP'] = df_inv['NP'].astype(str).str.strip()
                df_inv['ALMACEN'] = df_inv['ALMACEN'].astype(str).str.strip().str.upper()
                
            return df_inv
        return None
    except Exception as e:
        print(f"Error en carga de inventario maestro: {e}")
        return None

def buscar_archivos_ventas(agencia, anios):
    archivos_encontrados = []
    if not MASTER_SALES_ID: return []
    for anio in anios:
        query = f"name contains '{agencia}' and name contains '{anio}' and name contains 'MASTER' and '{MASTER_SALES_ID}' in parents and trashed=false"
        results = drive_service.files().list(q=query, fields="files(id, name)", supportsAllDrives=True, includeItemsFromAllDrives=True).execute()
        archivos_encontrados.extend(results.get('files', []))
    return archivos_encontrados

@st.cache_data(ttl=3600, show_spinner=False)
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
    for file_meta in files_metadata:
        content = descargar_archivo_drive(file_meta['id'])
        if content:
            try:
                engine = 'xlrd' if 'xls' in file_meta['name'].lower() and 'xlsx' not in file_meta['name'].lower() else 'openpyxl'
                df_temp = pd.read_excel(content, engine=engine)
                df_temp.columns = df_temp.columns.str.upper().str.strip()
                cols_utiles = [c for c in df_temp.columns if c in ['NP', 'DESCR', 'FECHA', 'ALMACEN', 'CANTIDAD']]
                dfs.append(df_temp[cols_utiles])
            except Exception as e: 
                print(f"Error al procesar ventas {file_meta['name']}: {e}")
                pass
            
    if not dfs: return None, fecha_inicio, fecha_fin
    
    df_global = pd.concat(dfs, ignore_index=True)
    df_global['FECHA'] = pd.to_datetime(df_global['FECHA'], dayfirst=True, errors='coerce')
    
    mask = (df_global['FECHA'] >= fecha_inicio) & (df_global['FECHA'] < fecha_fin)
    df_global = df_global[mask].copy()
    
    df_global['NP'] = df_global['NP'].astype(str).str.strip()
    df_global['ALMACEN'] = df_global['ALMACEN'].astype(str).str.strip().str.upper()
    df_global['CANTIDAD'] = pd.to_numeric(df_global['CANTIDAD'], errors='coerce').fillna(0)
    
    return df_global, fecha_inicio, fecha_fin

ALMACENES_CUAUTI = ["ALM. BOÑAR", "ALM. FAST FOOD", "ALM. LIPU", "ALM. MYM", "ALM. UTEP"]
ALMACENES_TULTI = ["ALM. ENLACES LOGISTICOS", "ALMACEN AFN", "BISONTE TEPOTZOTLAN", "CULVERT", "TDR", "TEISA", "TUMSA", "ZONTE"]
ALMACENES_BAJIO = ["ALM. UTEP SAN LUIS", "BISONTE SLP"]
TODOS_ALMACENES = sorted(ALMACENES_CUAUTI + ALMACENES_TULTI + ALMACENES_BAJIO)

def obtener_color_pestana(almacen):
    alm = almacen.upper()
    if alm in [x.upper() for x in ALMACENES_CUAUTI]: return '#4B8BBE' 
    if alm in [x.upper() for x in ALMACENES_TULTI]: return '#FF9999' 
    if alm in [x.upper() for x in ALMACENES_BAJIO]: return '#99FF99' 
    return '#FFFFFF'

# --- GENERADOR DE EXCEL MULTIPESTAÑA ---
def crear_excel_consignas(df_ventas, df_inv):
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
        workbook = writer.book
        
        fmt_blue = workbook.add_format({'bold': True, 'valign': 'vcenter', 'align': 'center', 'bg_color': '#10345C', 'font_color': 'white', 'border': 1, 'text_wrap': True})
        fmt_gray = workbook.add_format({'bold': True, 'valign': 'vcenter', 'align': 'center', 'bg_color': '#D3D3D3', 'font_color': 'black', 'border': 1, 'text_wrap': True})
        fmt_white = workbook.add_format({'bold': True, 'valign': 'vcenter', 'align': 'center', 'border': 1, 'text_wrap': True})
        
        cell_fmt = workbook.add_format({'align': 'center', 'valign': 'vcenter', 'border': 1, 'border_color': '#D3D3D3', 'num_format': '0'})
        cell_fmt_text = workbook.add_format({'align': 'center', 'valign': 'vcenter', 'border': 1, 'border_color': '#D3D3D3'})
        
        datos_almacenes = {}
        todas_partes = []

        for alm in TODOS_ALMACENES:
            df_v_alm = df_ventas[df_ventas['ALMACEN'] == alm.upper()]
            resumen_ventas = pd.DataFrame()
            if not df_v_alm.empty:
                resumen_ventas = df_v_alm.groupby('NP').agg(
                    DESCR=('DESCR', 'first'),
                    VENTA=('CANTIDAD', 'sum'),
                    total_ev=('CANTIDAD', 'count'),
                    neg_ev=('CANTIDAD', lambda x: (x < 0).sum())
                ).reset_index()
                resumen_ventas['HITS'] = (resumen_ventas['total_ev'] - (resumen_ventas['neg_ev'] * 2)).clip(lower=0)
            else:
                resumen_ventas = pd.DataFrame(columns=['NP', 'DESCR', 'VENTA', 'HITS'])

            inv_exist = pd.DataFrame()
            if df_inv is not None and not df_inv.empty:
                df_i_alm = df_inv[df_inv['ALMACEN'] == alm.upper()]
                inv_exist = df_i_alm.groupby('NP').agg(EXISTENCIA=('EXISTENCIA', 'sum'), DESCR_INV=('DESCRIPCION', 'first')).reset_index()
            else:
                inv_exist = pd.DataFrame(columns=['NP', 'EXISTENCIA', 'DESCR_INV'])

            if not resumen_ventas.empty or not inv_exist.empty:
                resumen = pd.merge(resumen_ventas, inv_exist, on='NP', how='outer')
                resumen['VENTA'] = resumen['VENTA'].fillna(0)
                resumen['HITS'] = resumen['HITS'].fillna(0)
                resumen['EXISTENCIA'] = resumen['EXISTENCIA'].fillna(0)
                if 'DESCR_INV' in resumen.columns and 'DESCR' in resumen.columns:
                    resumen['DESCR'] = resumen['DESCR'].combine_first(resumen['DESCR_INV']).fillna('')
                elif 'DESCR_INV' in resumen.columns:
                    resumen['DESCR'] = resumen['DESCR_INV'].fillna('')
                resumen = resumen[(resumen['VENTA'] != 0) | (resumen['HITS'] > 0) | (resumen['EXISTENCIA'] != 0)].reset_index(drop=True)
            else:
                resumen = pd.DataFrame(columns=['NP', 'DESCR', 'VENTA', 'HITS', 'EXISTENCIA'])
                
            datos_almacenes[alm] = resumen
            if not resumen.empty:
                todas_partes.append(resumen[['NP', 'DESCR']])

        # --- 1. CREAR HOJA "CONSIGNAS" ---
        df_cons_base = pd.concat(todas_partes).drop_duplicates(subset=['NP']).reset_index(drop=True) if todas_partes else pd.DataFrame(columns=['NP', 'DESCR'])
        if df_inv is not None and not df_inv.empty:
            df_inv_gral = df_inv[df_inv['ALMACEN'] == 'ALM. GENERAL']
            inv_gral_agg = df_inv_gral.groupby('NP')['EXISTENCIA'].sum().reset_index()
            df_cons_base = pd.merge(df_cons_base, inv_gral_agg, on='NP', how='left')
            df_cons_base['EXISTENCIA'] = df_cons_base['EXISTENCIA'].fillna(0)
        else:
            df_cons_base['EXISTENCIA'] = 0

        ws_cons = workbook.add_worksheet("CONSIGNAS")
        ws_cons.set_tab_color('#D3D3D3')
        ws_cons.freeze_panes(2, 0)
        
        last_col_cons = 4 + len(TODOS_ALMACENES)
        if not df_cons_base.empty:
            ws_cons.autofilter(1, 0, len(df_cons_base) + 1, last_col_cons)

        for c in range(5):
            ws_cons.write(0, c, "", fmt_blue)
        ws_cons.merge_range(0, 5, 0, last_col_cons, "TRASPASO / REQUERIDO POR CONSIGNA", fmt_blue)

        ws_cons.write(1, 0, "N° DE PARTE", fmt_blue)
        ws_cons.write(1, 1, "DESCR", fmt_blue)
        ws_cons.write(1, 2, "TRASPASO TOTAL", fmt_blue)
        ws_cons.write(1, 3, "INV. DISPONIBLE CRA", fmt_blue)
        ws_cons.write(1, 4, "COMPRA SUGERIDA", fmt_blue)  

        for i, alm in enumerate(TODOS_ALMACENES):
            ws_cons.write(1, 5 + i, alm, fmt_gray)

        ws_cons.set_column('A:A', 20, cell_fmt_text)
        ws_cons.set_column('B:B', 45, cell_fmt_text)
        ws_cons.set_column('C:E', 18, cell_fmt)
        ws_cons.set_column(5, last_col_cons, 16, cell_fmt)

        last_col_letter = xl_col_to_name(last_col_cons)
        for i in range(len(df_cons_base)):
            row = 2 + i
            ex_row = row + 1
            ws_cons.write(row, 0, df_cons_base.loc[i, 'NP'], cell_fmt_text)
            ws_cons.write(row, 1, df_cons_base.loc[i, 'DESCR'], cell_fmt_text)
            ws_cons.write_formula(row, 2, f"=SUM(F{ex_row}:{last_col_letter}{ex_row})", cell_fmt)
            ws_cons.write(row, 3, df_cons_base.loc[i, 'EXISTENCIA'], cell_fmt)
            ws_cons.write_formula(row, 4, f"=MAX(0, C{ex_row}-D{ex_row})", cell_fmt) 
            
            for j, alm in enumerate(TODOS_ALMACENES):
                sheet_name_alm = alm[:31]
                # Ahora busca en la columna M (index 12) que es donde está el nuevo TRASPASO REQUERIDO
                formula = f"=SUMIF('{sheet_name_alm}'!A:A, $A{ex_row}, '{sheet_name_alm}'!M:M)"
                ws_cons.write_formula(row, 5 + j, formula, cell_fmt)

        # --- 2. CREAR HOJAS INDIVIDUALES DE ALMACENES ---
        for alm in TODOS_ALMACENES:
            df_hoja = datos_almacenes[alm]
            sheet_name = alm[:31]
            ws = workbook.add_worksheet(sheet_name)
            ws.set_tab_color(obtener_color_pestana(alm))
            ws.freeze_panes(1, 0)
            
            # Encabezados actualizados según tu nueva regla
            encabezados = [
                'N° DE PARTE', 'DESCR', 'VENTA', 'HITS', 'DEMANDA', 
                'PROMEDIO (12)', 'BAJA (.5)', 'MEDIA (1)', 'ALTA (1.5)', 
                'INVENTARIO EXISTENCIA', 'VENTA ACTUAL', 'EXCESO INVENTARIO', 
                'TRASPASO REQUERIDO', 'COMENTARIOS'
            ]
            
            for col_num, col_name in enumerate(encabezados):
                if col_name in ['N° DE PARTE', 'DESCR', 'VENTA', 'HITS']:
                    ws.write(0, col_num, col_name, fmt_blue)
                elif col_name == 'COMENTARIOS':
                    ws.write(0, col_num, col_name, fmt_white)
                else:
                    ws.write(0, col_num, col_name, fmt_gray)
            
            ws.set_column('A:A', 20, cell_fmt_text)
            ws.set_column('B:B', 45, cell_fmt_text)
            ws.set_column('C:M', 15, cell_fmt)
            ws.set_column('N:N', 30, cell_fmt_text)
            
            start_row = 1
            for i in range(len(df_hoja)):
                row = start_row + i
                ex_row = row + 1
                
                # Columnas base
                ws.write(row, 0, df_hoja.loc[i, 'NP'], cell_fmt_text)
                ws.write(row, 1, df_hoja.loc[i, 'DESCR'], cell_fmt_text)
                ws.write(row, 2, df_hoja.loc[i, 'VENTA'], cell_fmt)
                ws.write(row, 3, df_hoja.loc[i, 'HITS'], cell_fmt)
                
                # DEMANDA (Col E / Index 4)
                f_dem = f'=IF(D{ex_row}>12,"ALTA",IF(AND(D{ex_row}>=6,D{ex_row}<=12),"MEDIA","BAJA"))'
                ws.write_formula(row, 4, f_dem, cell_fmt_text)
                
                # PROMEDIO (Col F / Index 5)
                f_prom = f'=IFERROR(C{ex_row}/12, 0)'
                ws.write_formula(row, 5, f_prom, cell_fmt)
                
                # BAJA .5 (Col G / Index 6) -> PROMEDIO * 0.5
                ws.write_formula(row, 6, f'=F{ex_row}*0.5', cell_fmt)
                
                # MEDIA 1 (Col H / Index 7) -> PROMEDIO * 1
                ws.write_formula(row, 7, f'=F{ex_row}*1', cell_fmt)
                
                # ALTA 1.5 (Col I / Index 8) -> PROMEDIO * 1.5
                ws.write_formula(row, 8, f'=F{ex_row}*1.5', cell_fmt)
                
                # EXISTENCIA (Col J / Index 9)
                ws.write(row, 9, df_hoja.loc[i, 'EXISTENCIA'], cell_fmt)
                
                # VENTA ACTUAL (Col K / Index 10)
                f_vtact = f'=IFERROR(J{ex_row}/F{ex_row}, 0)'
                ws.write_formula(row, 10, f_vtact, cell_fmt)
                
                # EXCESO INVENTARIO (Col L / Index 11)
                # Lógica: Si existencia > (Stock según demanda)
                f_exc = f'=IF(J{ex_row}>IF(E{ex_row}="ALTA",I{ex_row},IF(E{ex_row}="MEDIA",H{ex_row},G{ex_row})),"SI","NO")'
                ws.write_formula(row, 11, f_exc, cell_fmt_text)
                
                # TRASPASO REQUERIDO (Col M / Index 12)
                # Lógica según tu nueva regla: (Factor según Demanda) - EXISTENCIA
                f_trasp = f'=IF(E{ex_row}="ALTA", I{ex_row}-J{ex_row}, IF(E{ex_row}="MEDIA", H{ex_row}-J{ex_row}, G{ex_row}-J{ex_row}))'
                ws.write_formula(row, 12, f_trasp, cell_fmt)
                
                # COMENTARIOS (Col N / Index 13)
                ws.write(row, 13, '', cell_fmt_text)

    buffer.seek(0)
    return buffer

# --- INTERFAZ GRAFICA STREAMLIT ---
st.info("💡 Haz clic en el botón para que el sistema descargue todas las bases, filtre las fechas dinámicas y genere el Excel de Consignas con Múltiples Pestañas.")

if st.button("🚀 Generar Reporte de Consignas"):
    with st.spinner("Iniciando motor de descarga..."):
        df_inv = cargar_inventario_maestro()
        if df_inv is None:
            st.error("No se pudo leer el archivo INVENTARIO_CRA de Drive.")
            st.stop()
            
        df_ventas, f_inicio, f_fin = descargar_todas_las_ventas_12m()
        if df_ventas is None:
            st.error("No se encontraron registros de ventas.")
            st.stop()
            
        st.success(f"✅ Bases descargadas. Periodo: **{f_inicio.strftime('%b %Y')} a { (f_fin - relativedelta(days=1)).strftime('%b %Y')}**")
        
        with st.spinner("Procesando almacenes y aplicando nueva lógica de demanda (0.5, 1, 1.5)..."):
            buffer_excel = crear_excel_consignas(df_ventas, df_inv)
            
        with st.spinner("☁️ Subiendo archivo final a Google Drive..."):
            fecha_str = datetime.datetime.now().strftime("%d_%m_%Y")
            name_file = f"Analisis_Consignas_{fecha_str}.xlsx"
            link = subir_excel_a_drive(buffer_excel, name_file)
            
            if link:
                st.balloons()
                st.success(f"🎉 ¡Reporte con Lógica de Demanda Creado Exitosamente!")
                st.markdown(f"### [📂 Abrir Reporte de Consignas en Drive]({link})")
