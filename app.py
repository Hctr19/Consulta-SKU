import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime, time

# --- CONFIGURACIÓN ---
st.set_page_config(page_title="SICAR BI - MultiDB", layout="wide")

def get_engine(db_choice):
    try:
        prefix = "arizone" if "Arizone" in db_choice else "josivna"
        user = st.secrets[f"{prefix}_user"]
        password = st.secrets[f"{prefix}_password"]
        host = st.secrets[f"{prefix}_host"]
        return create_engine(f"mysql+mysqlconnector://{user}:{password}@{host}/", pool_pre_ping=True)
    except Exception as e:
        st.error(f"Error conexión: {e}")
        return None

db_seleccionada = st.sidebar.selectbox("Base de Datos:", ["Database Arizone", "Database Josivna"])
engine = get_engine(db_seleccionada)

@st.cache_data(ttl=600)
def descubrir_esquema(db_choice):
    for schema in ["sicar", "SICAR"]:
        try:
            with engine.connect() as conn:
                conn.execute(text(f"SELECT 1 FROM {schema}.cliente LIMIT 1"))
                return schema
        except: continue
    return "sicar"

esquema = descubrir_esquema(db_seleccionada)

def ejecutar_consulta(query_template):
    try:
        final_query = query_template.replace("{db}", esquema)
        return pd.read_sql(final_query, engine)
    except Exception as e:
        return pd.DataFrame()

@st.cache_data(ttl=300)
def cargar_clientes(db_choice, esquema_act):
    df = ejecutar_consulta("SELECT nombre FROM {db}.cliente WHERE status = 1 ORDER BY nombre ASC")
    return ["Selecciona cliente..."] + df['nombre'].tolist() if not df.empty else ["Sin clientes"]

lista_clientes = cargar_clientes(db_seleccionada, esquema)

# --- NAVEGACIÓN ---
st.sidebar.divider()
modo = st.sidebar.radio("Pantalla:", ["Historial por SKU", "Historial por Cliente", "Reporte de ventas por Cliente"])

with st.sidebar:
    st.divider()
    st.header("Filtros Globales")
    col1, col2 = st.columns(2)
    inicio_dt = col1.date_input("Desde", datetime(2020, 1, 1))
    fin_dt = col2.date_input("Hasta", datetime.now())
    
    # AJUSTE DE FECHAS: Convertimos a string incluyendo el último segundo del día final
    inicio = f"{inicio_dt} 00:00:00"
    fin = f"{fin_dt} 23:59:59"
    
    cliente_sel = st.selectbox("Selecciona Cliente:", lista_clientes) if modo != "Historial por SKU" else None

if not engine: st.stop()

# --- MOTOR DE CONSULTA UNIFICADA ---
def obtener_ventas_totales(cliente=None, sku=None):
    q_tickets = f"""
    SELECT v.fecha, 'VENTA (T)' as TIPO, c.nombre as CLIENTE, TRIM(dv.clave) as clave, dv.descripcion, dv.cantidad,
           dv.PrecioCompra as COSTO_U, dv.PrecioCon as VENTA_U, dv.ImporteCompra as TOTAL_C, dv.ImporteCon as TOTAL_V
    FROM {{db}}.venta v
    INNER JOIN {{db}}.detallev dv ON v.ven_id = dv.ven_id
    INNER JOIN {{db}}.ticket t ON v.tic_id = t.tic_id
    INNER JOIN {{db}}.cliente c ON t.cli_id = c.cli_id
    WHERE v.status = 1 AND v.fecha BETWEEN '{inicio}' AND '{fin}'
    """
    if cliente: q_tickets += f" AND c.nombre = '{cliente}'"
    if sku: q_tickets += f" AND TRIM(dv.clave) = '{sku}'"

    q_notas = f"""
    SELECT v.fecha, 'VENTA (N)' as TIPO, c.nombre as CLIENTE, TRIM(dv.clave) as clave, dv.descripcion, dv.cantidad,
           dv.PrecioCompra as COSTO_U, dv.PrecioCon as VENTA_U, dv.ImporteCompra as TOTAL_C, dv.ImporteCon as TOTAL_V
    FROM {{db}}.venta v
    INNER JOIN {{db}}.detallev dv ON v.ven_id = dv.ven_id
    INNER JOIN {{db}}.nota n ON v.not_id = n.not_id
    INNER JOIN {{db}}.cliente c ON n.cli_id = c.cli_id
    WHERE v.status = 1 AND v.fecha BETWEEN '{inicio}' AND '{fin}'
    """
    if cliente: q_notas += f" AND c.nombre = '{cliente}'"
    if sku: q_notas += f" AND TRIM(dv.clave) = '{sku}'"

    df_t = ejecutar_consulta(q_tickets)
    df_n = ejecutar_consulta(q_notas)
    return pd.concat([df_t, df_n])

# --- PANTALLAS ---

if modo == "Historial por SKU":
    st.header(f"🔍 SKU - {db_seleccionada}")
    sku_input = st.sidebar.text_input("Ingresa SKU:").upper().strip()
    if sku_input:
        df_v = obtener_ventas_totales(sku=sku_input)
        q_c = f"""
        SELECT c.fecha, 'COMPRA' as TIPO, 'PROVEEDOR' as CLIENTE, TRIM(dc.clave) as clave, dc.cantidad 
        FROM {{db}}.detallec dc 
        INNER JOIN {{db}}.compra c ON dc.com_id = c.com_id 
        WHERE TRIM(dc.clave) = '{sku_input}' AND c.status = 1 AND c.fecha BETWEEN '{inicio}' AND '{fin}'
        """
        df_c = ejecutar_consulta(q_c)
        if not df_v.empty or not df_c.empty:
            v_res = df_v[['fecha', 'TIPO', 'CLIENTE', 'cantidad']] if not df_v.empty else pd.DataFrame()
            c_res = df_c[['fecha', 'TIPO', 'CLIENTE', 'cantidad']] if not df_c.empty else pd.DataFrame()
            res = pd.concat([v_res, c_res]).sort_values('fecha', ascending=False)
            st.dataframe(res, use_container_width=True, hide_index=True)
        else:
            st.warning("No se encontraron movimientos.")

elif modo == "Historial por Cliente" and cliente_sel != "Selecciona cliente...":
    st.header(f"👤 Historial - {db_seleccionada}")
    df = obtener_ventas_totales(cliente=cliente_sel)
    st.dataframe(df[['fecha', 'TIPO', 'clave', 'descripcion', 'cantidad']], use_container_width=True, hide_index=True)

elif modo == "Reporte de ventas por Cliente" and cliente_sel != "Selecciona cliente...":
    st.header(f"📊 Reporte - {db_seleccionada}")
    df_v = obtener_ventas_totales(cliente=cliente_sel)
    q_nc = f"""
    SELECT nc.fecha, 'DEVOLUCION' as TIPO, c.nombre as CLIENTE, dn.clave, dn.descripcion, -dn.cantidad, 
           dn.PrecioCompra as COSTO_U, dn.PrecioCon as VENTA_U, -dn.ImporteCompra as TOTAL_C, -dn.ImporteCon as TOTAL_V 
    FROM {{db}}.notacredito nc 
    INNER JOIN {{db}}.detallen dn ON nc.ncr_id = dn.ncr_id 
    LEFT JOIN {{db}}.ticket t ON nc.tic_id = t.tic_id 
    LEFT JOIN {{db}}.cliente c ON t.cli_id = c.cli_id 
    WHERE nc.status = 1 AND nc.fecha BETWEEN '{inicio}' AND '{fin}' AND c.nombre = '{cliente_sel}'
    """
    df_nc = ejecutar_consulta(q_nc)
    df = pd.concat([df_v, df_nc])
    if not df.empty:
        v_n, c_n = df['TOTAL_V'].sum(), df['TOTAL_C'].sum()
        c1, c2, c3 = st.columns(3)
        c1.metric("Venta Neta", f"${v_n:,.2f}"); c2.metric("Costo Neto", f"${c_n:,.2f}"); c3.metric("Utilidad", f"${v_n-c_n:,.2f}")
        st.dataframe(df.style.format({'COSTO_U': '{:,.2f}', 'VENTA_U': '{:,.2f}', 'TOTAL_C': '{:,.2f}', 'TOTAL_V': '{:,.2f}'}), use_container_width=True, hide_index=True)
