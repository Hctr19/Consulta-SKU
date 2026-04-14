import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime

# --- CONFIGURACIÓN ---
st.set_page_config(page_title="SICAR SKU Tracker", layout="wide")

# Conexión segura usando st.secrets (se configuran en el panel de Streamlit Cloud)
def get_engine():
    user = st.secrets["db_user"]
    password = st.secrets["db_password"]
    host = st.secrets["db_host"]
    db = st.secrets["db_name"]
    return create_engine(f"mysql+mysqlconnector://{user}:{password}@{host}/{db}")

st.title("📊 Monitor de Movimientos SKU")

# --- INTERFAZ ---
with st.sidebar:
    sku = st.text_input("SKU a consultar:").upper().strip()
    col_f1, col_f2 = st.columns(2)
    inicio = col_f1.date_input("Desde", datetime(2026, 1, 1))
    fin = col_f2.date_input("Hasta", datetime.now())

if sku:
    engine = get_engine()
    
    # Queries simplificadas para el dashboard
    q_ventas = f"SELECT fecha, 'VENTA' as Tipo, cantidad as Cant, precioCompra * 1.16 as Costo_IVA FROM sicar.detallev dv INNER JOIN sicar.venta v ON dv.ven_id = v.ven_id WHERE dv.clave = '{sku}' AND v.status = 1 AND v.fecha BETWEEN '{inicio}' AND '{fin}'"
    q_compras = f"SELECT fecha, 'COMPRA' as Tipo, cantidad as Cant, precioCompra * 1.16 as Costo_IVA FROM sicar.detallec dc INNER JOIN sicar.compra c ON dc.com_id = c.com_id WHERE dc.clave = '{sku}' AND c.status = 1 AND c.fecha BETWEEN '{inicio}' AND '{fin}'"
    q_notas = f"SELECT fecha, 'NOTA_CREDITO' as Tipo, -cantidad as Cant, -(precioCompra * 1.16) as Costo_IVA FROM sicar.detallen dn INNER JOIN sicar.notacredito nc ON dn.ncr_id = nc.ncr_id WHERE dn.clave = '{sku}' AND nc.status = 1 AND nc.fecha BETWEEN '{inicio}' AND '{fin}'"

    df_v = pd.read_sql(q_ventas, engine)
    df_c = pd.read_sql(q_compras, engine)
    df_n = pd.read_sql(q_notas, engine)
    
    df_resumen = pd.concat([df_v, df_c, df_n]).sort_values('fecha', ascending=False)

    if not df_resumen.empty:
        # Métricas de cabecera
        m1, m2, m3 = st.columns(3)
        m1.metric("Ventas (Netas)", int(df_v['Cant'].sum() + df_n['Cant'].sum()))
        m2.metric("Compras", int(df_c['Cant'].sum()))
        m3.metric("Costo Total Movido", f"${df_resumen['Costo_IVA'].sum():,.2f}")
        
        st.dataframe(df_resumen, use_container_width=True)
    else:
        st.info("Sin movimientos para este SKU en el rango seleccionado.")
