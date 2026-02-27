import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, time
import urllib.parse
import requests
import io
import plotly.graph_objects as go
import unicodedata
import re
import difflib

# --- 1. CONFIGURACIÓN DE PÁGINA ---
st.set_page_config(page_title="Supervisión RER - Osinergmin", layout="wide", initial_sidebar_state="expanded")
st.title("☀️🌬️ Dashboard de Supervisión RER - Energía Primaria y Generación")
st.markdown("Seguimiento Dinámico de Recurso Primario (m/s, W/m2) y Potencia Inyectada (MW) en el SEIN")

MESES = {
    1: "Enero", 2: "Febrero", 3: "Marzo", 4: "Abril",
    5: "Mayo", 6: "Junio", 7: "Julio", 8: "Agosto",
    9: "Setiembre", 10: "Octubre", 11: "Noviembre", 12: "Diciembre"
}

# --- 2. FUNCIONES DE EXTRACCIÓN Y LIMPIEZA (ETL) ---
def generar_urls_coes(fecha):
    año = fecha.strftime("%Y")
    mes_num = fecha.strftime("%m")
    dia = fecha.strftime("%d")
    mes_titulo = MESES[fecha.month]
    fecha_str = fecha.strftime("%d%m")
    
    path_nuevo = f"Post Operación/Reportes/IEOD/{año}/{mes_num}_{mes_titulo}/{dia}/AnexoA_{fecha_str}.xlsx"
    path_legacy = f"Post Operación/Reportes/IEOD/{año}/{mes_num}_{mes_titulo}/{dia}/Anexo1_Resumen_{fecha_str}.xlsx"
    
    return [
        (f"https://www.coes.org.pe/portal/browser/download?url={urllib.parse.quote(path_nuevo)}", "AnexoA"),
        (f"https://www.coes.org.pe/portal/browser/download?url={urllib.parse.quote(path_legacy)}", "Anexo1")
    ]

def clean_match_name(text):
    if pd.isna(text) or str(text).strip().lower() == 'nan': return ""
    t = str(text).upper()
    t = re.sub(r'\(.*?\)', '', t)
    t = unicodedata.normalize('NFKD', t).encode('ASCII', 'ignore').decode('utf-8')
    t = re.sub(r'[^A-Z0-9]', '', t)
    palabras_a_remover = [
        'CE', 'CS', 'PARQUEEOLICO', 'CENTRALSOLAR', 'CENTRALEOLICA', 'CENTRAL', 
        'PARQUE', 'EOLICA', 'SOLAR', 'EXP', 'RADIACION', 'IRRADIANCIA', 'VELOCIDAD', 
        'VIENTO', 'WM2', 'MS'
    ]
    palabras_a_remover.sort(key=len, reverse=True)
    for word in palabras_a_remover:
        t = t.replace(word, '')
    return t

@st.cache_data(show_spinner=False)
def extraer_datos_rer_dinamico(fecha):
    urls = generar_urls_coes(fecha)
    headers = {'User-Agent': 'Mozilla/5.0'}
    df_raw_prim = None
    df_raw_pot = None
    es_anexoa = False
    
    for url, tipo_anexo in urls:
        try:
            res = requests.get(url, headers=headers, timeout=20)
            if res.status_code == 200:
                es_anexoa = (tipo_anexo == "AnexoA")
                archivo_excel = io.BytesIO(res.content)
                xls = pd.ExcelFile(archivo_excel, engine='openpyxl')
                hojas = xls.sheet_names
                
                hoja_primaria = next((h for h in hojas if "PRIMARIA" in h.upper()), None)
                hoja_potencia = next((h for h in hojas if "GENER" in h.upper() and "RER" in h.upper()), None)
                
                if hoja_primaria:
                    df_raw_prim = pd.read_excel(xls, sheet_name=hoja_primaria, header=None, nrows=60)
                if hoja_potencia:
                    df_raw_pot = pd.read_excel(xls, sheet_name=hoja_potencia, header=None, nrows=60)
                    
                if df_raw_prim is not None:
                    break
        except Exception:
            continue
            
    if df_raw_prim is None:
        return None, None, [f"[{fecha.strftime('%d/%m/%Y')}] Error de extracción o no se halló la hoja de Energía Primaria."], []

    # ==========================
    # EXTRACCIÓN DE PRIMARIA
    # ==========================
    fila_empresas = df_raw_prim.iloc[5].ffill().astype(str).str.strip()
    fila_centrales_prim = df_raw_prim.iloc[6].astype(str).str.strip()
    
    columnas_objetivo_prim = {}
    nombres_base_match = {} 
    
    for col_idx in range(df_raw_prim.shape[1]):
        central = fila_centrales_prim.iloc[col_idx].upper()
        empresa = fila_empresas.iloc[col_idx]
        
        if central.startswith("C.E") or central.startswith("C.S"):
            nombre_columna = f"{central} | {empresa}"
            columnas_objetivo_prim[nombre_columna] = col_idx
            nombres_base_match[nombre_columna] = clean_match_name(central)

    mensajes_alerta_extraccion = []
    mensajes_alerta_calidad = []
    
    if not columnas_objetivo_prim:
        mensajes_alerta_extraccion.append(f"[{fecha.strftime('%d/%m/%Y')}] No se hallaron centrales Eólicas (C.E) ni Solares (C.S) en Primaria.")

    df_datos_prim = pd.DataFrame()
    df_datos_pot = pd.DataFrame()
    fechas_horas = [datetime.combine(fecha, time(0, 30)) + timedelta(minutes=30*i) for i in range(48)]
    df_datos_prim['Fecha_Hora'] = fechas_horas
    df_datos_pot['Fecha_Hora'] = fechas_horas
    
    def procesar_columna_numerica(df, start_idx, col_idx):
        serie = df.iloc[start_idx:start_idx+48, col_idx] 
        serie_str = serie.astype(str).str.replace(',', '', regex=False).str.strip()
        serie_str = serie_str.replace(['nan', 'NAN', '', 'None'], np.nan)
        return pd.to_numeric(serie_str, errors='coerce').values

    for nombre, idx in columnas_objetivo_prim.items():
        df_datos_prim[nombre] = procesar_columna_numerica(df_raw_prim, 7, idx)
        
        max_val = df_datos_prim[nombre].max()
        if pd.notna(max_val):
            if nombre.startswith("C.E") and max_val > 50:
                mensajes_alerta_calidad.append(f"[{fecha.strftime('%d/%m/%Y')}] {nombre.split('|')[0].strip()}: Valor anómalo ({max_val:.1f} m/s > 50). Datos descartados.")
                df_datos_prim[nombre] = np.nan
            elif nombre.startswith("C.S") and max_val > 2500:
                mensajes_alerta_calidad.append(f"[{fecha.strftime('%d/%m/%Y')}] {nombre.split('|')[0].strip()}: Valor anómalo ({max_val:.1f} W/m2 > 2500). Datos descartados.")
                df_datos_prim[nombre] = np.nan

    # ==========================
    # EXTRACCIÓN DE POTENCIA (Punta Lomitas Totalizado y validación COLCA)
    # ==========================
    MAPEO_EXCEPCIONES = {
        "PUNTA LOMITAS EXP - BL-1": "PUNTA LOMITAS",
        "PUNTA LOMITAS EXP - BL-2": "PUNTA LOMITAS",
        "PUNTA LOMITAS - I": "PUNTA LOMITAS",
        "PUNTA LOMITAS - II": "PUNTA LOMITAS",
        "MAJES SOLAR 20T": "MAJES"
    }

    if df_raw_pot is not None:
        idx_nombres_pot = 6 if es_anexoa else 5 
        idx_datos_pot = 7 if es_anexoa else 6   
        
        fila_centrales_pot = df_raw_pot.iloc[idx_nombres_pot].astype(str).str.strip()
        fila_empresas_pot = df_raw_pot.iloc[idx_nombres_pot - 1].ffill().astype(str).str.strip().str.upper()
        
        for nombre_completo, match_base in nombres_base_match.items():
            matched_col_idx = None
            excepcion_pot_name = None
            
            for key_prim, val_pot in MAPEO_EXCEPCIONES.items():
                if key_prim in nombre_completo:
                    excepcion_pot_name = val_pot
                    break
            
            if excepcion_pot_name:
                exc_clean = unicodedata.normalize('NFKD', excepcion_pot_name.upper()).encode('ASCII', 'ignore').decode('utf-8')
                for col_idx in range(df_raw_pot.shape[1]):
                    pot_name_raw = str(fila_centrales_pot.iloc[col_idx]).strip()
                    empresa_pot_raw = str(fila_empresas_pot.iloc[col_idx]).strip()
                    
                    if pd.notna(pot_name_raw):
                        pot_clean = unicodedata.normalize('NFKD', pot_name_raw.upper()).encode('ASCII', 'ignore').decode('utf-8')
                        if "YARUCAYA" in pot_clean and "COLCA" not in empresa_pot_raw: continue
                            
                        if exc_clean in pot_clean:
                            matched_col_idx = col_idx
                            break
            else:
                opciones_candidatas = []
                for col_idx in range(df_raw_pot.shape[1]):
                    pot_name_raw = str(fila_centrales_pot.iloc[col_idx]).strip()
                    empresa_pot_raw = str(fila_empresas_pot.iloc[col_idx]).strip()
                    
                    if pd.isna(pot_name_raw) or pot_name_raw.lower() == 'nan': continue
                        
                    pot_clean_check = unicodedata.normalize('NFKD', pot_name_raw.upper()).encode('ASCII', 'ignore').decode('utf-8')
                    if "YARUCAYA" in pot_clean_check and "COLCA" not in empresa_pot_raw: continue
                        
                    match_pot = clean_match_name(pot_name_raw)
                    if len(match_base) > 2 and len(match_pot) > 2:
                        fuerza_coincidencia = difflib.SequenceMatcher(None, match_base, match_pot).ratio()
                        if fuerza_coincidencia >= 0.5:
                            opciones_candidatas.append((col_idx, fuerza_coincidencia))
                
                if opciones_candidatas:
                    opciones_candidatas.sort(key=lambda x: x[1], reverse=True)
                    matched_col_idx = opciones_candidatas[0][0] 
                            
            if matched_col_idx is not None:
                df_datos_pot[nombre_completo] = procesar_columna_numerica(df_raw_pot, idx_datos_pot, matched_col_idx)
            else:
                df_datos_pot[nombre_completo] = np.nan
                mensajes_alerta_extraccion.append(f"[{fecha.strftime('%d/%m/%Y')}] No se encontró match de Potencia para {nombre_completo.split('|')[0].strip()}.")
    else:
        for nombre_completo in columnas_objetivo_prim.keys():
            df_datos_pot[nombre_completo] = np.nan
            
    return df_datos_prim, df_datos_pot, mensajes_alerta_extraccion, mensajes_alerta_calidad

def procesar_rango_fechas(start_date, end_date, progress_bar, status_text):
    fechas = pd.date_range(start_date, end_date)
    total_dias = len(fechas)
    lista_dfs_prim = []
    lista_dfs_pot = []
    alertas_extraccion_global = []
    alertas_calidad_global = []
    
    for i, f in enumerate(fechas):
        status_text.markdown(f"**⏳ Procesando Datos COES:** {f.strftime('%d/%m/%Y')} *(Día {i+1} de {total_dias})*")
        
        df_prim, df_pot, alertas_extr, alertas_cal = extraer_datos_rer_dinamico(f)
        if df_prim is not None and not df_prim.empty:
            lista_dfs_prim.append(df_prim)
            lista_dfs_pot.append(df_pot)
            
        if alertas_extr: alertas_extraccion_global.extend(alertas_extr)
        if alertas_cal: alertas_calidad_global.extend(alertas_cal)
            
        progress_bar.progress((i + 1) / total_dias)
            
    if lista_dfs_prim:
        return pd.concat(lista_dfs_prim, ignore_index=True), pd.concat(lista_dfs_pot, ignore_index=True), alertas_extraccion_global, alertas_calidad_global
    return None, None, alertas_extraccion_global, alertas_calidad_global

def mostrar_alertas_compactas(titulo, lista_alertas, icono="⚠️"):
    if lista_alertas:
        with st.expander(f"{icono} {titulo}"):
            html_content = ""
            for alerta in lista_alertas:
                html_content += f"""
                <div style="background-color: #fff3cd; color: #856404; border: 1px solid #ffeeba; 
                            padding: 6px 10px; border-radius: 4px; margin-bottom: 5px; font-size: 14px;">
                    <strong>{icono}</strong> {alerta}
                </div>
                """
            st.markdown(html_content, unsafe_allow_html=True)


# --- 3. INTERFAZ DE USUARIO (SIDEBAR Y MAIN) ---
st.sidebar.header("Parámetros de Supervisión RER")
rango_fechas = st.sidebar.date_input("Intervalo de Fechas (IEOD)", value=(datetime(2024, 2, 19), datetime(2024, 2, 22)))

if st.sidebar.button("Extraer Datos RER", key="btn_procesar"):
    if isinstance(rango_fechas, tuple) and len(rango_fechas) == 2:
        start_date, end_date = rango_fechas
        status_text = st.empty()
        progress_bar = st.progress(0)
        
        df_prim, df_pot, alertas_extr, alertas_cal = procesar_rango_fechas(start_date, end_date, progress_bar, status_text)
        
        if df_prim is not None and not df_prim.empty:
            st.session_state['df_rer_prim'] = df_prim
            st.session_state['df_rer_pot'] = df_pot
            st.session_state['alertas_extr_rer'] = alertas_extr
            st.session_state['alertas_cal_rer'] = alertas_cal
            
        status_text.empty()
        progress_bar.empty()

if 'df_rer_prim' in st.session_state:
    df_prim = st.session_state['df_rer_prim']
    df_pot = st.session_state['df_rer_pot']
    alertas_extr = st.session_state['alertas_extr_rer']
    alertas_cal = st.session_state['alertas_cal_rer']
    
    mostrar_alertas_compactas("Bitácora de Alertas de Extracción / Match de Potencia", alertas_extr, icono="⚠️")
    mostrar_alertas_compactas("Bitácora de Calidad de Datos (Valores Descartados)", alertas_cal, icono="🚨")
                
    st.success("✅ Extracción algorítmica y cruce de variables (Primaria + Potencia) completados.")
    st.markdown("---")
    
    # ==========================================
    # FILTROS INTELIGENTES
    # ==========================================
    todas_las_columnas = [col for col in df_prim.columns if col != 'Fecha_Hora']
    lista_empresas = sorted(list(set([col.split(" | ")[1].strip() for col in todas_las_columnas])))
    lista_centrales_todas = sorted(list(set([col.split(" | ")[0].strip() for col in todas_las_columnas])))

    col1, col2 = st.columns(2)
    with col1:
        empresas_seleccionadas = st.multiselect("🏢 Empresa Concesionaria:", options=lista_empresas)
    with col2:
        if empresas_seleccionadas:
            columnas_filtradas_por_empresa = [c for c in todas_las_columnas if c.split(" | ")[1].strip() in empresas_seleccionadas]
            lista_centrales_filtrada = sorted(list(set([c.split(" | ")[0].strip() for c in columnas_filtradas_por_empresa])))
        else:
            lista_centrales_filtrada = lista_centrales_todas

        centrales_seleccionadas = st.multiselect("⚡ Central de Generación:", options=lista_centrales_filtrada)

    columnas_a_mostrar = []
    for col in todas_las_columnas:
        emp = col.split(" | ")[1].strip()
        cen = col.split(" | ")[0].strip()
        match_empresa = (not empresas_seleccionadas) or (emp in empresas_seleccionadas)
        match_central = (not centrales_seleccionadas) or (cen in centrales_seleccionadas)
        if match_empresa and match_central:
            columnas_a_mostrar.append(col)

    if not columnas_a_mostrar:
        st.warning("No hay datos que coincidan con los filtros seleccionados.")
    else:
        df_filtrado_prim = df_prim[['Fecha_Hora'] + columnas_a_mostrar].copy()
        df_filtrado_pot = df_pot[['Fecha_Hora'] + columnas_a_mostrar].copy()
        
        cols_eolicas = [c for c in df_filtrado_prim.columns if c.startswith("C.E")]
        cols_solares = [c for c in df_filtrado_prim.columns if c.startswith("C.S")]
        
        if len(df_filtrado_prim) > 0:
            dias_totales = (df_filtrado_prim['Fecha_Hora'].max() - df_filtrado_prim['Fecha_Hora'].min()).days + 1
            rango_fechas_x = [df_filtrado_prim['Fecha_Hora'].min() - pd.Timedelta(hours=12), 
                              df_filtrado_prim['Fecha_Hora'].max() + pd.Timedelta(hours=12)]
        else:
            dias_totales = 0
            rango_fechas_x = None
            
        mostrar_mensual = dias_totales > 30

        # ==========================================
        # SECCIÓN EÓLICA
        # ==========================================
        if cols_eolicas:
            st.markdown("### 📈 Análisis Operativo - Parques Eólicos")
            
            # 1. Gráfica Primaria (Viento) - Apilada verticalmente
            fig_eol_prim = go.Figure()
            for col in cols_eolicas:
                fig_eol_prim.add_trace(go.Scatter(x=df_filtrado_prim['Fecha_Hora'], y=df_filtrado_prim[col], mode='lines', name=col.split("|")[0].strip()))
            fig_eol_prim.update_layout(title="Energía Primaria (Velocidad m/s)", xaxis_title="Fecha / Hora", yaxis_title="m/s", hovermode="x unified", height=400, xaxis_range=rango_fechas_x, showlegend=True)
            st.plotly_chart(fig_eol_prim, use_container_width=True)
            
            # 2. Gráfica de Potencia Individual (MW) - Apilada verticalmente
            fig_eol_pot = go.Figure()
            for col in cols_eolicas:
                fig_eol_pot.add_trace(go.Scatter(x=df_filtrado_pot['Fecha_Hora'], y=df_filtrado_pot[col], mode='lines', name=col.split("|")[0].strip()))
            fig_eol_pot.update_layout(title="Generación Activa Individual (Potencia MW)", xaxis_title="Fecha / Hora", yaxis_title="MW", hovermode="x unified", height=400, xaxis_range=rango_fechas_x, showlegend=True)
            st.plotly_chart(fig_eol_pot, use_container_width=True)
                
            # Gráfica de Potencia Agregada Eólica
            st.markdown("#### ⚡ Potencia Eólica Total Generada (MW)")
            
            cols_pl = [c for c in cols_eolicas if "PUNTA LOMITAS" in c.upper()]
            cols_otros = [c for c in cols_eolicas if "PUNTA LOMITAS" not in c.upper()]
            
            if len(cols_pl) > 0:
                potencia_total_eolica = df_filtrado_pot[cols_otros].sum(axis=1) + df_filtrado_pot[cols_pl[0]].fillna(0)
            else:
                potencia_total_eolica = df_filtrado_pot[cols_eolicas].sum(axis=1)

            fig_eol_total = go.Figure()
            fig_eol_total.add_trace(go.Scatter(
                x=df_filtrado_pot['Fecha_Hora'], 
                y=potencia_total_eolica, 
                mode='lines', 
                name='Total Eólica', 
                fill='tozeroy', 
                line=dict(color='#2ca02c', width=2)
            ))
            fig_eol_total.update_layout(
                title="Suma Agregada de Parques Eólicos Filtrados (MW)", 
                xaxis_title="Fecha / Hora", 
                yaxis_title="Total MW", 
                hovermode="x unified", 
                height=350, 
                xaxis_range=rango_fechas_x
            )
            st.plotly_chart(fig_eol_total, use_container_width=True)
            
            # Gráficas de Promedios (Primaria)
            if mostrar_mensual: col_eol_1, col_eol_2 = st.columns(2)
            else: col_eol_1 = st.container() 
            
            with col_eol_1:
                df_dia_eol = df_filtrado_prim.copy()
                df_dia_eol['Día'] = pd.to_datetime((df_dia_eol['Fecha_Hora'] - pd.Timedelta(minutes=1)).dt.date)
                df_dia_eol_grp = df_dia_eol.groupby('Día')[cols_eolicas].mean().reset_index()
                fig_eol_dia = go.Figure()
                for col in cols_eolicas: fig_eol_dia.add_trace(go.Bar(x=df_dia_eol_grp['Día'], y=df_dia_eol_grp[col], name=col.split("|")[0].strip()))
                fig_eol_dia.update_layout(title="Promedio Diario (m/s)", barmode='group', xaxis_title="Fecha Operativa", yaxis_title="m/s", xaxis_range=rango_fechas_x, showlegend=True)
                st.plotly_chart(fig_eol_dia, use_container_width=True)

            if mostrar_mensual:
                with col_eol_2:
                    df_mes_eol = df_filtrado_prim.copy()
                    df_mes_eol['Mes'] = df_mes_eol['Fecha_Hora'].dt.strftime('%Y-%m')
                    df_mes_eol_grp = df_mes_eol.groupby('Mes')[cols_eolicas].mean().reset_index()
                    fig_eol_mes = go.Figure()
                    for col in cols_eolicas: fig_eol_mes.add_trace(go.Bar(x=df_mes_eol_grp['Mes'], y=df_mes_eol_grp[col], name=col.split("|")[0].strip()))
                    fig_eol_mes.update_layout(title="Promedio Mensual (m/s)", barmode='group', xaxis_title="Mes", yaxis_title="m/s", showlegend=True)
                    st.plotly_chart(fig_eol_mes, use_container_width=True)
            st.markdown("---")

        # ==========================================
        # SECCIÓN SOLAR
        # ==========================================
        if cols_solares:
            st.markdown("### ☀️ Análisis Operativo - Centrales Solares")
            
            # 1. Gráfica Primaria (Irradiancia) - Apilada verticalmente
            fig_sol_prim = go.Figure()
            for col in cols_solares:
                fig_sol_prim.add_trace(go.Scatter(x=df_filtrado_prim['Fecha_Hora'], y=df_filtrado_prim[col], mode='lines', name=col.split("|")[0].strip()))
            fig_sol_prim.update_layout(title="Energía Primaria (Irradiancia W/m2)", xaxis_title="Fecha / Hora", yaxis_title="W/m2", hovermode="x unified", height=400, xaxis_range=rango_fechas_x, showlegend=True)
            st.plotly_chart(fig_sol_prim, use_container_width=True)
            
            # 2. Gráfica de Potencia Individual (MW) - Apilada verticalmente
            fig_sol_pot = go.Figure()
            for col in cols_solares:
                fig_sol_pot.add_trace(go.Scatter(x=df_filtrado_pot['Fecha_Hora'], y=df_filtrado_pot[col], mode='lines', name=col.split("|")[0].strip()))
            fig_sol_pot.update_layout(title="Generación Activa Individual (Potencia MW)", xaxis_title="Fecha / Hora", yaxis_title="MW", hovermode="x unified", height=400, xaxis_range=rango_fechas_x, showlegend=True)
            st.plotly_chart(fig_sol_pot, use_container_width=True)
                
            # Gráfica de Potencia Agregada Solar
            st.markdown("#### ⚡ Potencia Solar Total Generada (MW)")
            fig_sol_total = go.Figure()
            fig_sol_total.add_trace(go.Scatter(
                x=df_filtrado_pot['Fecha_Hora'], 
                y=df_filtrado_pot[cols_solares].sum(axis=1), 
                mode='lines', 
                name='Total Solar', 
                fill='tozeroy', 
                line=dict(color='#ff7f0e', width=2)
            ))
            fig_sol_total.update_layout(
                title="Suma Agregada de Centrales Solares Filtradas (MW)", 
                xaxis_title="Fecha / Hora", 
                yaxis_title="Total MW", 
                hovermode="x unified", 
                height=350, 
                xaxis_range=rango_fechas_x
            )
            st.plotly_chart(fig_sol_total, use_container_width=True)
            
            # Gráficas de Promedios (Primaria)
            if mostrar_mensual: col_sol_1, col_sol_2 = st.columns(2)
            else: col_sol_1 = st.container()
            
            with col_sol_1:
                df_dia_sol = df_filtrado_prim.copy()
                df_dia_sol['Día'] = pd.to_datetime((df_dia_sol['Fecha_Hora'] - pd.Timedelta(minutes=1)).dt.date)
                df_dia_sol_grp = df_dia_sol.groupby('Día')[cols_solares].mean().reset_index()
                fig_sol_dia = go.Figure()
                for col in cols_solares: fig_sol_dia.add_trace(go.Bar(x=df_dia_sol_grp['Día'], y=df_dia_sol_grp[col], name=col.split("|")[0].strip()))
                fig_sol_dia.update_layout(title="Promedio Diario (W/m2)", barmode='group', xaxis_title="Fecha Operativa", yaxis_title="W/m2", xaxis_range=rango_fechas_x, showlegend=True)
                st.plotly_chart(fig_sol_dia, use_container_width=True)

            if mostrar_mensual:
                with col_sol_2:
                    df_mes_sol = df_filtrado_prim.copy()
                    df_mes_sol['Mes'] = df_mes_sol['Fecha_Hora'].dt.strftime('%Y-%m')
                    df_mes_sol_grp = df_mes_sol.groupby('Mes')[cols_solares].mean().reset_index()
                    fig_sol_mes = go.Figure()
                    for col in cols_solares: fig_sol_mes.add_trace(go.Bar(x=df_mes_sol_grp['Mes'], y=df_mes_sol_grp[col], name=col.split("|")[0].strip()))
                    fig_sol_mes.update_layout(title="Promedio Mensual (W/m2)", barmode='group', xaxis_title="Mes", yaxis_title="W/m2", showlegend=True)
                    st.plotly_chart(fig_sol_mes, use_container_width=True)

        # ==========================================
        # TABLAS DE DATOS COMBINADAS (EÓLICAS Y SOLARES)
        # ==========================================
        st.markdown("### 🗄️ Trazabilidad de Datos Crudos Consolidada")
        st.info("Comparativa detallada entre el Recurso Primario extraído y su Potencia (MW) equivalente despachada. *Nota: Si una central despacha bajo un solo nodo comercial (ej. Punta Lomitas), su potencia se muestra Totalizada.*")
        
        # 1. Tabla Eólica
        if cols_eolicas:
            st.markdown("#### 🌬️ Datos Crudos - Parques Eólicos")
            df_eol_combined = pd.DataFrame()
            df_eol_combined['Fecha_Hora'] = df_filtrado_prim['Fecha_Hora'].dt.strftime('%d/%m/%Y %H:%M')
            
            for col in cols_eolicas:
                nombre_corto = col.split("|")[0].strip()
                df_eol_combined[f"{nombre_corto} [m/s]"] = df_filtrado_prim[col]
                
            for col in cols_eolicas:
                nombre_corto = col.split("|")[0].strip()
                if "PUNTA LOMITAS" in nombre_corto.upper():
                    df_eol_combined[f"{nombre_corto} [MW Totalizado]"] = df_filtrado_pot[col]
                else:
                    df_eol_combined[f"{nombre_corto} [MW]"] = df_filtrado_pot[col]
                
            st.dataframe(df_eol_combined, use_container_width=True, hide_index=True)
            
        # 2. Tabla Solar
        if cols_solares:
            st.markdown("#### ☀️ Datos Crudos - Centrales Solares")
            df_sol_combined = pd.DataFrame()
            df_sol_combined['Fecha_Hora'] = df_filtrado_prim['Fecha_Hora'].dt.strftime('%d/%m/%Y %H:%M')
            
            for col in cols_solares:
                nombre_corto = col.split("|")[0].strip()
                df_sol_combined[f"{nombre_corto} [W/m2]"] = df_filtrado_prim[col]
                
            for col in cols_solares:
                nombre_corto = col.split("|")[0].strip()
                df_sol_combined[f"{nombre_corto} [MW]"] = df_filtrado_pot[col]
                
            st.dataframe(df_sol_combined, use_container_width=True, hide_index=True)

else:
    st.info("👈 Seleccione un rango de fechas y presione 'Extraer Datos RER' en el panel lateral para iniciar la fiscalización.")