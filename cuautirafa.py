# cuautirafa.py
import streamlit as st
import pandas as pd
import io
import datetime

# --- LISTA DE ALMACENES EXCLUSIVA ---
ALMACENES_CUAUTI = ["ALM. BOÑAR", "ALM. FAST FOOD", "ALM. LIPU", "ALM. MYM", "ALM. UTEP", "ALM. UTEP SAN LUIS"]

def procesar_cuautitlan_rafa(uploaded_file):
    try:
        all_sheets = pd.read_excel(uploaded_file, sheet_name=None)
        if 'CONSIGNAS' not in all_sheets:
            st.error("El archivo no contiene la hoja 'CONSIGNAS'. Asegúrate de subir el reporte correcto.")
            return None
        
        df_cons = all_sheets['CONSIGNAS']
        
        cols_base = ['N° DE PARTE', 'DESCR', 'TRASPASO CUAUTITLAN', 'INV. CUAUTITLAN']
        cols_existentes = [c for c in cols_base + ALMACENES_CUAUTI if c in df_cons.columns]
        df_cons = df_cons[cols_existentes].copy()

        alm_data = {}
        for alm in ALMACENES_CUAUTI:
            sheet_name = alm[:31]
            if sheet_name in all_sheets:
                df_alm = all_sheets[sheet_name]
                if 'N° DE PARTE' in df_alm.columns and 'DEMANDA' in df_alm.columns and 'ALTA (1.5)' in df_alm.columns:
                    alm_data[alm] = df_alm.set_index('N° DE PARTE')[['DEMANDA', 'ALTA (1.5)']].to_dict('index')
                else: alm_data[alm] = {}
            else: alm_data[alm] = {}

        renombres = {}
        for alm in ALMACENES_CUAUTI:
            if alm in df_cons.columns:
                total_filas = len(df_cons)
                if total_filas > 0:
                    col_num = pd.to_numeric(df_cons[alm], errors='coerce').fillna(0)
                    exitosos = (col_num <= 0).sum()
                    perc = (exitosos / total_filas) * 100
                else:
                    perc = 0
                renombres[alm] = f"{alm} ({perc:.0f}%)"
        
        df_cons_renamed = df_cons.rename(columns=renombres)

        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            workbook = writer.book
            ws = workbook.add_worksheet("CONSIGNAS")
            ws.freeze_panes(1, 0)
            
            fmt_header = workbook.add_format({'bold': True, 'valign': 'vcenter', 'align': 'center', 'bg_color': '#4B8BBE', 'font_color': 'white', 'border': 1, 'text_wrap': True})
            cell_fmt_txt = workbook.add_format({'valign': 'vcenter', 'border': 1, 'border_color': '#D3D3D3'})
            cell_fmt_num = workbook.add_format({'align': 'center', 'valign': 'vcenter', 'border': 1, 'border_color': '#D3D3D3', 'num_format': '0'})
            
            fmt_green = workbook.add_format({'align': 'center', 'valign': 'vcenter', 'border': 1, 'bg_color': '#C6EFCE', 'font_color': '#006100', 'num_format': '0'})
            fmt_yellow = workbook.add_format({'align': 'center', 'valign': 'vcenter', 'border': 1, 'bg_color': '#FFEB9C', 'font_color': '#9C5700', 'num_format': '0'})
            fmt_red = workbook.add_format({'align': 'center', 'valign': 'vcenter', 'border': 1, 'bg_color': '#FFC7CE', 'font_color': '#9C0006', 'num_format': '0'})

            for col_idx, col_name in enumerate(df_cons_renamed.columns):
                ws.write(0, col_idx, col_name, fmt_header)
            
            ws.set_column('A:A', 20)
            ws.set_column('B:B', 45)
            ws.set_column('C:Z', 18)

            for row_idx in range(len(df_cons)):
                row_data = df_cons.iloc[row_idx]
                np_val = row_data.get('N° DE PARTE', '')
                
                for col_idx, col_original in enumerate(df_cons.columns):
                    val = row_data[col_original]
                    
                    if col_original in ALMACENES_CUAUTI:
                        t_val = pd.to_numeric(val, errors='coerce')
                        t_val = t_val if pd.notnull(t_val) else 0
                        current_fmt = cell_fmt_num
                        
                        if np_val in alm_data[col_original]:
                            demanda = alm_data[col_original][np_val].get('DEMANDA', '')
                            v_alta = pd.to_numeric(alm_data[col_original][np_val].get('ALTA (1.5)', 0), errors='coerce')
                            v_alta = v_alta if pd.notnull(v_alta) else 0
                            
                            if demanda == 'ALTA' and t_val > 0:
                                if t_val <= (v_alta * (1/3)):
                                    current_fmt = fmt_green
                                elif t_val <= (v_alta * 0.5):
                                    current_fmt = fmt_yellow
                                else:
                                    current_fmt = fmt_red
                        
                        ws.write(row_idx + 1, col_idx, t_val, current_fmt)
                    elif col_original in ['N° DE PARTE', 'DESCR']:
                        ws.write(row_idx + 1, col_idx, val, cell_fmt_txt)
                    else:
                        ws.write(row_idx + 1, col_idx, val, cell_fmt_num)
        output.seek(0)
        return output
    except Exception as e:
        st.error(f"Error procesando el archivo: {e}")
        return None

def modulo_cuautitlan_rafa():
    st.title("🏭 CUAUTITLÁN RAFA: Análisis Específico")
    st.markdown("Carga el **Reporte Segmentado de Consignas** generado en el otro módulo para obtener la versión exclusiva de Cuautitlán con semáforos y % de éxito.")
    
    archivo_subido = st.file_uploader("📂 Sube tu archivo Excel", type=["xlsx"])
    
    if archivo_subido is not None:
        if st.button("🪄 Generar Archivo Filtrado"):
            with st.spinner("Leyendo estructura, evaluando semáforos y procesando % de éxito..."):
                buffer_resultado = procesar_cuautitlan_rafa(archivo_subido)
                
                if buffer_resultado:
                    st.success("✅ Archivo Cuautitlán Rafa generado exitosamente.")
                    st.download_button(
                        label="📥 Descargar Excel Final",
                        data=buffer_resultado,
                        file_name=f"Reporte_Cuautitlan_Rafa_{datetime.datetime.now().strftime('%d_%m_%Y')}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )
