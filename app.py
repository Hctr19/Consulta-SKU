import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta

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
        except:
            continue
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
    return ["Selecciona cliente..."] + df["nombre"].tolist() if not df.empty else ["Sin clientes"]

lista_clientes = cargar_clientes(db_seleccionada, esquema)

# --- NAVEGACIÓN ---
st.sidebar.divider()
# ELIMINADO: "Historial por Cliente"
modo = st.sidebar.radio("Pantalla:", ["Historial por SKU", "Reporte de ventas por Cliente"])

with st.sidebar:
    st.divider()
    col1, col2 = st.columns(2)
    inicio_dt = col1.date_input("Desde", datetime.now() - timedelta(days=365))
    fin_dt    = col2.date_input("Hasta", datetime.now())
    inicio = f"{inicio_dt} 00:00:00"
    fin    = f"{fin_dt} 23:59:59"
    cliente_sel = st.selectbox("Selecciona Cliente:", lista_clientes) if modo != "Historial por SKU" else None

if not engine:
    st.stop()

# --- MOTOR DE CONSULTA UNIFICADA ---
def obtener_ventas_totales(cliente=None, sku=None):
    q_tickets = (
        f"SELECT v.fecha, 'VENTA (T)' as TIPO, c.nombre as CLIENTE, TRIM(dv.clave) as clave, "
        f"dv.descripcion, dv.cantidad, dv.PrecioCompra as COSTO_U, dv.PrecioCon as VENTA_U, "
        f"dv.ImporteCompra as TOTAL_C, dv.ImporteCon as TOTAL_V "
        f"FROM {{db}}.venta v "
        f"INNER JOIN {{db}}.detallev dv ON v.ven_id = dv.ven_id "
        f"INNER JOIN {{db}}.ticket t ON v.tic_id = t.tic_id "
        f"INNER JOIN {{db}}.cliente c ON t.cli_id = c.cli_id "
        f"WHERE v.status = 1 AND v.fecha BETWEEN '{inicio}' AND '{fin}'"
    )
    if cliente:
        q_tickets += f" AND c.nombre = '{cliente}'"
    if sku:
        q_tickets += f" AND TRIM(dv.clave) = '{sku}'"

    q_notas = (
        f"SELECT v.fecha, 'VENTA (N)' as TIPO, c.nombre as CLIENTE, TRIM(dv.clave) as clave, "
        f"dv.descripcion, dv.cantidad, dv.PrecioCompra as COSTO_U, dv.PrecioCon as VENTA_U, "
        f"dv.ImporteCompra as TOTAL_C, dv.ImporteCon as TOTAL_V "
        f"FROM {{db}}.venta v "
        f"INNER JOIN {{db}}.detallev dv ON v.ven_id = dv.ven_id "
        f"INNER JOIN {{db}}.nota n ON v.not_id = n.not_id "
        f"INNER JOIN {{db}}.cliente c ON n.cli_id = c.cli_id "
        f"WHERE v.status = 1 AND v.fecha BETWEEN '{inicio}' AND '{fin}'"
    )
    if cliente:
        q_notas += f" AND c.nombre = '{cliente}'"
    if sku:
        q_notas += f" AND TRIM(dv.clave) = '{sku}'"

    df_tickets = ejecutar_consulta(q_tickets)
    df_notas   = ejecutar_consulta(q_notas)

    if df_tickets.empty and df_notas.empty:
        return pd.DataFrame()

    return pd.concat([df_tickets, df_notas]).sort_values("fecha", ascending=False)

def obtener_devoluciones(cliente=None):
    q = f"""
    SELECT nc.fecha, 'DEVOLUCION' as TIPO, c.nombre as CLIENTE, dn.clave, dn.descripcion, dn.cantidad,
    dn.PrecioCompra as COSTO_U, dn.PrecioCon as VENTA_U, dn.ImporteCompra as TOTAL_C, dn.ImporteCon as TOTAL_V
    FROM {{db}}.notacredito nc
    INNER JOIN {{db}}.detallen dn ON nc.ncr_id = dn.ncr_id
    LEFT JOIN {{db}}.ticket t ON nc.tic_id = t.tic_id
    LEFT JOIN {{db}}.nota n ON nc.not_id = n.not_id
    LEFT JOIN {{db}}.cliente c ON (t.cli_id = c.cli_id OR n.cli_id = c.cli_id)
    WHERE nc.status = 1 AND nc.fecha BETWEEN '{inicio}' AND '{fin}'
    """
    if cliente:
        q += f" AND c.nombre = '{cliente}'"
    return ejecutar_consulta(q).sort_values("fecha", ascending=False)

def obtener_historial_completo_sku(sku, inicio, fin):
    q = f"""
    -- 1. Compras Directas (Entrada)
    SELECT 
        c.fecha AS fecha,
        'ENTRADA' AS TIPO,
        'Compra Directa' AS CONCEPTO,
        d.cantidad AS cantidad
    FROM {{db}}.compra c
    INNER JOIN {{db}}.detallec d ON c.com_id = d.com_id
    INNER JOIN {{db}}.articulo art ON d.art_id = art.art_id
    WHERE c.status = 1 
      AND art.status = 1
      AND TRIM(d.clave) = '{sku}'
      AND c.fecha BETWEEN '{inicio}' AND '{fin}'

    UNION ALL

    -- 2. Ajustes Positivos de Stock (Entrada)
    SELECT 
        aj.fecha AS fecha,
        'ENTRADA' AS TIPO,
        'Ajuste Positivo' AS CONCEPTO,
        aja.diferencia AS cantidad
    FROM {{db}}.ajusteinventario aj
    INNER JOIN {{db}}.ajusteinventarioarticulo aja ON aj.ain_id = aja.ain_id
    INNER JOIN {{db}}.articulo art ON aja.art_id = art.art_id
    WHERE aja.diferencia > 0 
      AND art.status = 1
      AND TRIM(art.clave) = '{sku}'
      AND aj.fecha BETWEEN '{inicio}' AND '{fin}'

    UNION ALL

    -- 3. Devoluciones de Clientes (Entrada)
    SELECT 
        nc.fecha AS fecha,
        'ENTRADA' AS TIPO,
        'Devolución Cliente' AS CONCEPTO,
        dn.cantidad AS cantidad
    FROM {{db}}.notacredito nc
    INNER JOIN {{db}}.detallen dn ON nc.ncr_id = dn.ncr_id
    INNER JOIN {{db}}.articulo art ON dn.art_id = art.art_id
    WHERE nc.status = 1 
      AND art.status = 1
      AND TRIM(dn.clave) = '{sku}'
      AND nc.fecha BETWEEN '{inicio}' AND '{fin}'

    UNION ALL

    -- 4. Ventas (Tickets y Notas) (Salida)
    SELECT 
        v.fecha AS fecha,
        'SALIDA' AS TIPO,
        CASE 
            WHEN v.tic_id IS NOT NULL THEN 'Venta (Ticket)'
            ELSE 'Venta (Nota)'
        END AS CONCEPTO,
        dv.cantidad AS cantidad
    FROM {{db}}.venta v
    INNER JOIN {{db}}.detallev dv ON v.ven_id = dv.ven_id
    INNER JOIN {{db}}.articulo art ON dv.art_id = art.art_id
    LEFT JOIN {{db}}.ticket t ON v.tic_id = t.tic_id
    LEFT JOIN {{db}}.nota n ON v.not_id = n.not_id
    WHERE v.status = 1 
      AND art.status = 1
      AND (t.tic_id IS NOT NULL OR n.not_id IS NOT NULL)
      AND TRIM(dv.clave) = '{sku}'
      AND v.fecha BETWEEN '{inicio}' AND '{fin}'

    UNION ALL

    -- 5. Ajustes Negativos / Mermas (Salida)
    SELECT 
        aj.fecha AS fecha,
        'SALIDA' AS TIPO,
        'Ajuste Negativo (Merma)' AS CONCEPTO,
        ABS(aja.diferencia) AS cantidad
    FROM {{db}}.ajusteinventario aj
    INNER JOIN {{db}}.ajusteinventarioarticulo aja ON aj.ain_id = aja.ain_id
    INNER JOIN {{db}}.articulo art ON aja.art_id = art.art_id
    WHERE aja.diferencia < 0 
      AND art.status = 1
      AND TRIM(art.clave) = '{sku}'
      AND aj.fecha BETWEEN '{inicio}' AND '{fin}'

    UNION ALL

    -- 6. Traspasos Salientes (Salida)
    SELECT 
        t.fechaApl AS fecha,
        'SALIDA' AS TIPO,
        'Traspaso Saliente' AS CONCEPTO,
        dt.cantidad AS cantidad
    FROM {{db}}.traspaso t
    INNER JOIN {{db}}.detallet dt ON t.tra_id = dt.tra_id
    INNER JOIN {{db}}.articulo art ON dt.art_id = art.art_id
    WHERE t.fechaApl IS NOT NULL 
      AND art.status = 1
      AND TRIM(dt.clave) = '{sku}'
      AND t.fechaApl BETWEEN '{inicio}' AND '{fin}'
    """
    return ejecutar_consulta(q).sort_values("fecha", ascending=False)

# --- PANTALLAS ---

if modo == "Historial por SKU":
    st.header(f"🔍 SKU - {db_seleccionada}")
    sku_input = st.sidebar.text_input("Ingresa SKU:").upper().strip()
    if sku_input:
        df_historial = obtener_historial_completo_sku(
            sku=sku_input,
            inicio=inicio,
            fin=fin
        )

        if not df_historial.empty:
            total_entradas = df_historial[df_historial["TIPO"] == "ENTRADA"]["cantidad"].sum()
            total_salidas = df_historial[df_historial["TIPO"] == "SALIDA"]["cantidad"].sum()
            balance_neto = total_entradas - total_salidas

            col1, col2, col3 = st.columns(3)
            col1.metric("Total Entradas", f"{total_entradas:,.2f}")
            col2.metric("Total Salidas", f"{total_salidas:,.2f}")
            col3.metric("Balance Neto (Stock)", f"{balance_neto:,.2f}")

            st.divider()
            st.dataframe(df_historial, use_container_width=True, hide_index=True)
        else:
            st.info("No se encontraron registros para este SKU en el periodo seleccionado.")

elif modo == "Reporte de ventas por Cliente" and cliente_sel != "Selecciona cliente...":
    df_ventas = obtener_ventas_totales(cliente=cliente_sel)
    df_devs   = obtener_devoluciones(cliente=cliente_sel)

    st.header(f"📊 Reporte Financiero - {cliente_sel}")

    v_n = df_ventas["TOTAL_V"].sum() if not df_ventas.empty else 0.0
    c_n = df_ventas["TOTAL_C"].sum() if not df_ventas.empty else 0.0
    d_v = df_devs["TOTAL_V"].sum()   if not df_devs.empty   else 0.0
    d_c = df_devs["TOTAL_C"].sum()   if not df_devs.empty   else 0.0

    total_v = v_n - d_v
    total_c = c_n - d_c

    col1, col2, col3 = st.columns(3)
    col1.metric("Venta Neta (Ventas - Dev)", f"${total_v:,.2f}")
    col2.metric("Costo Neto",                f"${total_c:,.2f}")
    col3.metric("Utilidad",                  f"${total_v - total_c:,.2f}")

    st.subheader("🛒 Ventas (Tickets y Notas)")
    if not df_ventas.empty:
        st.dataframe(df_ventas, use_container_width=True, hide_index=True)
    else:
        st.info("No se encontraron ventas para este cliente en el periodo seleccionado.")

    st.subheader("↩️ Devoluciones (Notas de Crédito)")
    if not df_devs.empty:
        st.dataframe(df_devs, use_container_width=True, hide_index=True)
    else:
        st.info("No se encontraron notas de crédito para este cliente en el periodo seleccionado.")