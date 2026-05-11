# app.py
import streamlit as st
from google.oauth2 import service_account
from googleapiclient.discovery import build

# Importamos los módulos (asegúrate de que los 3 archivos estén en la misma carpeta en GitHub)
import modelotraspasos
import cuautirafa

# ==========================================
# CONFIGURACIÓN DE PÁGINA
# ==========================================
st.set_page_config(page_title="Consignas - CRA", layout="wide")

# ==========================================
# 1. SISTEMA DE LOGIN
# ==========================================
if "logged_in" not in st.session_state:
    st.session_state["logged_in"] = False

if not st.session_state["logged_in"]:
    st.title("🔒 Acceso Restringido")
    st.markdown("Por favor, inicia sesión para continuar.")
    
    with st.form("login_form"):
        user_input = st.text_input("Usuario")
        pass_input = st.text_input("Contraseña", type="password")
        submitted = st.form_submit_button("Ingresar")
        
        if submitted:
            if user_input == "Eduardo Barrera" and pass_input == "Barrera1234.":
                st.session_state["logged_in"] = True
                st.rerun()
            else:
                st.error("Usuario o contraseña incorrectos.")
    st.stop()

# ==========================================
# 2. CONEXIÓN A GOOGLE DRIVE
# ==========================================
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

# ==========================================
# 3. BARRA LATERAL Y ENRUTAMIENTO
# ==========================================
st.sidebar.title("Navegación")
menu = st.sidebar.radio("Ir a:", ["MODELO TRASPASOS", "CUAUTITLAN_RAFA"])

if menu == "MODELO TRASPASOS":
    # Pasamos las credenciales al módulo
    modelotraspasos.modulo_traspasos(
        drive_service, 
        MASTER_SALES_ID, 
        INVENTORY_FOLDER_ID, 
        PARENT_FOLDER_ID
    )
    
elif menu == "CUAUTITLAN_RAFA":
    # Llamamos al módulo que no requiere Drive, solo procesar un archivo
    cuautirafa.modulo_cuautitlan_rafa()
