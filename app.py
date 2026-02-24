import streamlit as st
import pandas as pd
import io
import datetime
from dateutil.relativedelta import relativedelta
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

# Configuración de la página
st.set_page_config(page_title="Planeación Mensual - CRA", layout="wide")
st.title("📊 CRA INT: Planeación Mensual de Compras (Fase 1)")

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
    except Exception as e: return None

# --- CARGA AUTOMÁTICA DEL INVENTARIO MAESTRO ---
@st.cache_data(ttl=3600) # Guarda en caché por 1 hora para que sea rápido
def cargar_inventario_maestro():
    if not INVENTORY_FOLDER_ID:
        return None
    
    # Buscamos el archivo que contenga "INVENTARIO_CRA" en la carpeta especificada
    query = f"name contains 'INVENTARIO_CRA' and '{INVENTORY_FOLDER_ID}' in parents and trashed=false"
    results = drive_service.files().list(
        q=query, fields="files(id, name)", supportsAllDrives=True, includeItemsFromAllDrives=True
    ).execute()
    
    files = results.get('files', [])
    if not files:
        return None
    
    file_meta = files[0] # Tomamos el primero que encuentre
    content = descargar_archivo_drive(file_meta['id'])
    
    if content:
        try:
            engine = 'xlrd' if 'xls' in file_meta['name'] and 'xlsx' not in file_meta['name'] else 'openpyxl'
            df_inv = pd.read_excel(content, engine=engine)
            # Limpiamos nombres de columnas
            df_inv.columns = df_inv.columns.str.upper().str.strip()
            df_inv['NP'] = df_inv['NP'].astype(str).str.strip()
            df_inv['ALMACEN'] = df_inv['ALMACEN'].astype(str).str.strip()
            return df_inv
        except Exception as e:
            st.error(f"Error leyendo inventario maestro: {e}")
            return None
    return None

# --- LÓGICA DE VENTAS HISTÓRICAS ---
def buscar_archivos_ventas(agencia, anios):
    archivos_encontrados = []
    if not MASTER_SALES_ID: return []
    for anio in anios:
        query = f"name contains '{agencia}' and name contains '{anio}' and name contains 'MASTER' and '{MASTER_SALES_ID}' in parents and trashed=false"
        results = drive_service.files().list(
            q=query, fields="files(id, name)", supportsAllDrives=True, includeItemsFromAllDrives=True
        ).execute()
        files = results.get('files', [])
        archivos_encontrados.extend(files)
    return archivos_encontrados

def clasificar_movimiento(row):
    m10 = row['meses_distintos_10m']
    v12 = row['ventas_meses_11_y_12']
    
    if m10 >= 6: return 'ALTO MOVIMIENTO'
    if m10 >= 3: return 'MEDIO MOVIMIENTO'
    if m10 >= 1: return 'BAJO MOVIMIENTO'
    if v12 > 0: return 'RIESGO'
    return 'OBSOLETO'

def obtener_y_clasificar_ventas(sucursal, almacen_seleccionado, df_inventario_filtrado):
    hoy = datetime.datetime.now()
    fecha_inicio_12m = hoy - relativedelta(months=12)
    fecha_inicio_10m = hoy - relativedelta(months=10)
    
    anios_drive = list(set([fecha_inicio_12m.year, hoy.year]))
    
    # Buscamos ventas usando la SUCURSAL
    files_metadata = buscar_archivos_ventas(sucursal.upper(), anios_drive)
    
    if not files_metadata: 
        st.error(f"No se encontraron archivos de ventas para la sucursal {sucursal} en los años {anios_drive}.")
        return None

    dfs = []
    bar = st.progress(0, text="Descargando histórico de ventas...")
    for i, file_meta in enumerate(files_metadata):
        content = descargar_archivo_drive(file_meta['id'])
        if content:
            try:
                engine = 'xlrd' if 'xls' in file_meta['name'] and 'xlsx' not in file_meta['name'] else 'openpyxl'
                df_temp = pd.read_excel(content, engine=engine)
                df_temp.columns = df_temp.columns.str.upper().str.strip()
                dfs.append(df_temp)
            except Exception: pass
        bar.progress((i + 1) / len(files_metadata))
            
    if not dfs: return None
    
    df_total = pd.concat(dfs, ignore_index=True)
    
    # 1. FILTRAR POR ALMACÉN EXACTO
    if 'ALMACEN' in df_total.columns:
        df_total['ALMACEN'] = df_total['ALMACEN'].astype(str).str.strip()
        df_total = df_total[df_total['ALMACEN'] == almacen_seleccionado]
        
    if df_total.empty:
        st.warning(f"No hay registros de venta para el almacén '{almacen_seleccionado}' en los últimos 12 meses.")
        # Aun así, crearemos la base con el inventario para marcar todo como OBSOLETO
        df_total = pd.DataFrame(columns=['NP', 'FECHA', 'CANTIDAD'])

    if 'FECHA' not in df_total.columns and not df_total.empty: 
        st.error("No se encontró la columna FECHA en el histórico.")
        return None
        
    df_total['FECHA'] = pd.to_datetime(df_total['FECHA'], dayfirst=True, errors='coerce')
    df_total['NP'] = df_total['NP'].astype(str).str.strip()
    df_total['CANTIDAD'] = pd.to_numeric(df_total['CANTIDAD'], errors='coerce').fillna(0)

    # 2. SEPARAR BLOQUES DE TIEMPO
    mask_10m = (df_total['FECHA'] >= fecha_inicio_10m) & (df_total['FECHA'] <= hoy)
    df_10m = df_total[mask_10m].copy()
    
    mask_11_12 = (df_total['FECHA'] >= fecha_inicio_12m) & (df_total['FECHA'] < fecha_inicio_10m)
    df_11_12 = df_total[mask_11_12].copy()

    mask_12m = (df_total['FECHA'] >= fecha_inicio_12m) & (df_total['FECHA'] <= hoy)
    df_12m = df_total[mask_12m].copy()

    # 3. CÁLCULOS
    df_10m_positivos = df_10m[df_10m['CANTIDAD'] > 0].copy()
    df_10m_positivos['PERIODO'] = df_10m_positivos['FECHA'].dt.strftime('%Y-%m')
    meses_10m = df_10m_positivos.groupby('NP')['PERIODO'].nunique().reset_index()
    meses_10m.columns = ['NP', 'meses_distintos_10m']

    ventas_11_12 = df_11_12[df_11_12['CANTIDAD'] > 0].groupby('NP').size().reset_index()
    ventas_11_12.columns = ['NP', 'ventas_meses_11_y_12']

    metricas_12m = df_12m.groupby('NP').agg(
        total_eventos=('CANTIDAD', 'count'),
        eventos_negativos=('CANTIDAD', lambda x: (x < 0).sum()),
        suma_cantidad=('CANTIDAD', 'sum')
    ).reset_index()
    
    metricas_12m['HITS'] = metricas_12m['total_eventos'] - (metricas_12m['eventos_negativos'] * 2)
    metricas_12m['HITS'] = metricas_12m['HITS'].clip(lower=0)
    metricas_12m['CONSUMO MENSUAL'] = metricas_12m['suma_cantidad'] / 12

    # 4. LISTADO MAESTRO (Inventario Actual + Ventas Históricas)
    nps_inventario = df_inventario_filtrado[['NP']].drop_duplicates()
    nps_ventas = df_12m[['NP']].drop_duplicates()
    df_maestro = pd.concat([nps_inventario, nps_ventas]).drop_duplicates().reset_index(drop=True)

    # 5. UNIR Y CLASIFICAR
    df_maestro = pd.merge(df_maestro, meses_10m, on='NP', how='left').fillna(0)
    df_maestro = pd.merge(df_maestro, ventas_11_12, on='NP', how='left').fillna(0)
    df_maestro = pd.merge(df_maestro, metricas_12m[['NP', 'HITS', 'CONSUMO MENSUAL']], on='NP', how='left').fillna(0)

    # También traemos la información del inventario (Existencia, etc) para tenerla lista
    # Como filtramos el inventario por almacén, cada NP debería ser único o lo agrupamos
    inv_resumen = df_inventario_filtrado.groupby('NP').agg({
        'DESCRIPCION': 'first',
        'LINEA': 'first',
        'EXISTENCIA': 'sum',
        'FEC_ULT_COMPRA': 'first'
    }).reset_index()

    df_maestro = pd.merge(df_maestro, inv_resumen, on='NP', how='left')
    df_maestro['EXISTENCIA'] = df_maestro['EXISTENCIA'].fillna(0)
    df_maestro['CLASIFICACIÓN'] = df_maestro.apply(clasificar_movimiento, axis=1)

    return df_maestro[['NP', 'DESCRIPCION', 'LINEA', 'CLASIFICACIÓN', 'HITS', 'CONSUMO MENSUAL', 'EXISTENCIA', 'FEC_ULT_COMPRA']]


# --- INTERFAZ GRAFICA ---
st.markdown("### 1. Extracción de Catálogo")
with st.spinner("⏳ Conectando con Drive y descargando el Inventario Maestro..."):
    df_inventario_maestro = cargar_inventario_maestro()

if df_inventario_maestro is not None:
    st.success("✅ Inventario Maestro cargado exitosamente.")
    
    # Extraemos la lista única de almacenes del inventario
    lista_almacenes = sorted(df_inventario_maestro['ALMACEN'].dropna().unique().tolist())
    
    almacen_seleccionado = st.selectbox("🏬 Selecciona el Almacén a Planear:", lista_almacenes)
    
    # Descubrimos automáticamente a qué Sucursal pertenece ese almacén
    sucursal_inferida = df_inventario_maestro[df_inventario_maestro['ALMACEN'] == almacen_seleccionado]['SUCURSAL'].iloc[0]
    st.caption(f"📍 Sucursal detectada: **{sucursal_inferida}** (Se usarán los archivos de ventas de esta sucursal)")
    
    st.markdown("---")
    if st.button("🚀 Procesar Fase 1 (Listado y Clasificación)"):
        # Filtramos el inventario solo para el almacén seleccionado
        df_inv_filtrado = df_inventario_maestro[df_inventario_maestro['ALMACEN'] == almacen_seleccionado]
        
        # Ejecutamos el motor
        resultado = obtener_y_clasificar_ventas(sucursal_inferida, almacen_seleccionado, df_inv_filtrado)
        
        if resultado is not None:
            st.success(f"✅ Análisis completado para {almacen_seleccionado}")
            
            # Resumen de Clasificación
            resumen_cat = resultado['CLASIFICACIÓN'].value_counts().reset_index()
            resumen_cat.columns = ['Categoría', 'Cantidad de Piezas']
            
            col_res1, col_res2 = st.columns([1, 2])
            col_res1.write("**Resumen de Clasificación:**")
            col_res1.dataframe(resumen_cat)
            
            col_res2.write("**Vista Previa del Catálogo Base:**")
            col_res2.dataframe(resultado.head(20))
else:
    st.warning("⚠️ No se pudo cargar el archivo 'INVENTARIO_CRA'. Revisa que el ID en secrets sea correcto y que el robot tenga acceso a la carpeta.")
