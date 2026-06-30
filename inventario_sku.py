import os
import argparse
import pandas as pd
from sqlalchemy import create_engine, text

def load_secrets():
    """Carga las credenciales del archivo secrets.toml."""
    secrets = {}
    path = os.path.join(".streamlit", "secrets.toml")
    if not os.path.exists(path):
        path = os.path.join("C:\\Users\\User\\Documents\\GitHub\\Consulta-SKU\\.streamlit", "secrets.toml")
    
    if not os.path.exists(path):
        raise FileNotFoundError(f"No se encontró el archivo secrets.toml en: {path}")

    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" in line:
                key, val = line.split("=", 1)
                secrets[key.strip()] = val.strip().strip('"').strip("'")
    return secrets

def get_engine(db_choice, secrets):
    """Crea la conexión a la base de datos MySQL."""
    prefix = "arizone" if "arizone" in db_choice.lower() else "josivna"
    user = secrets.get(f"{prefix}_user")
    password = secrets.get(f"{prefix}_password")
    host = secrets.get(f"{prefix}_host")
    port = secrets.get(f"{prefix}_port", 3306)
    if not all([user, password, host]):
        raise ValueError(f"Faltan credenciales en secrets.toml para el prefijo: {prefix}")
    return create_engine(f"mysql+mysqlconnector://{user}:{password}@{host}:{port}/sicar", pool_pre_ping=True)

def construir_query(db_schema, sku=None, months=12, start_date=None, end_date=None):
    """
    Construye la consulta SQL consolidada (UNION ALL) de 6 orígenes de movimientos.
    """
    # Filtro de fecha
    if start_date and end_date:
        date_filter = f"BETWEEN '{start_date}' AND '{end_date}'"
    else:
        date_filter = f">= DATE_SUB(NOW(), INTERVAL {months} MONTH)"

    # Filtro de SKU
    sku_filter_dc = f"AND TRIM(d.clave) = '{sku}'" if sku else ""
    sku_filter_aja = f"AND TRIM(art.clave) = '{sku}'" if sku else ""
    sku_filter_dn = f"AND TRIM(dn.clave) = '{sku}'" if sku else ""
    sku_filter_dv = f"AND TRIM(dv.clave) = '{sku}'" if sku else ""
    sku_filter_dt = f"AND TRIM(dt.clave) = '{sku}'" if sku else ""

    query = f"""
    -- 1. Compras Directas (Entrada)
    SELECT 
        TRIM(d.clave) AS sku,
        c.fecha AS fecha,
        'ENTRADA' AS tipo,
        'Compra Directa' AS concepto,
        COALESCE(p.nombre, 'PROVEEDOR DESCONOCIDO') AS detalle,
        d.cantidad AS cantidad
    FROM `{db_schema}`.compra c
    INNER JOIN `{db_schema}`.detallec d ON c.com_id = d.com_id
    INNER JOIN `{db_schema}`.articulo art ON d.art_id = art.art_id
    LEFT JOIN `{db_schema}`.proveedor p ON c.pro_id = p.pro_id
    WHERE c.status = 1 
      AND art.status = 1
      AND c.fecha {date_filter}
      {sku_filter_dc}

    UNION ALL

    -- 2. Ajustes Positivos de Stock (Entrada)
    SELECT 
        TRIM(art.clave) AS sku,
        aj.fecha AS fecha,
        'ENTRADA' AS tipo,
        'Ajuste Positivo' AS concepto,
        COALESCE(aj.comentario, 'Ajuste de Stock') AS detalle,
        aja.diferencia AS cantidad
    FROM `{db_schema}`.ajusteinventario aj
    INNER JOIN `{db_schema}`.ajusteinventarioarticulo aja ON aj.ain_id = aja.ain_id
    INNER JOIN `{db_schema}`.articulo art ON aja.art_id = art.art_id
    WHERE aja.diferencia > 0 
      AND art.status = 1
      AND aj.fecha {date_filter}
      {sku_filter_aja}

    UNION ALL

    -- 3. Devoluciones de Clientes (Entrada)
    SELECT 
        TRIM(dn.clave) AS sku,
        nc.fecha AS fecha,
        'ENTRADA' AS tipo,
        'Devolución Cliente' AS concepto,
        COALESCE(cli.nombre, 'CLIENTE DESCONOCIDO') AS detalle,
        dn.cantidad AS cantidad
    FROM `{db_schema}`.notacredito nc
    INNER JOIN `{db_schema}`.detallen dn ON nc.ncr_id = dn.ncr_id
    INNER JOIN `{db_schema}`.articulo art ON dn.art_id = art.art_id
    LEFT JOIN `{db_schema}`.ticket t ON nc.tic_id = t.tic_id
    LEFT JOIN `{db_schema}`.nota n ON nc.not_id = n.not_id
    LEFT JOIN `{db_schema}`.cliente cli ON (t.cli_id = cli.cli_id OR n.cli_id = cli.cli_id)
    WHERE nc.status = 1 
      AND art.status = 1
      AND nc.fecha {date_filter}
      {sku_filter_dn}

    UNION ALL

    -- 4. Ventas (Tickets y Notas) (Salida)
    SELECT 
        TRIM(dv.clave) AS sku,
        v.fecha AS fecha,
        'SALIDA' AS tipo,
        CASE 
            WHEN v.tic_id IS NOT NULL THEN 'Venta (Ticket)'
            ELSE 'Venta (Nota)'
        END AS concepto,
        COALESCE(cli.nombre, 'CLIENTE DESCONOCIDO') AS detalle,
        dv.cantidad AS cantidad
    FROM `{db_schema}`.venta v
    INNER JOIN `{db_schema}`.detallev dv ON v.ven_id = dv.ven_id
    INNER JOIN `{db_schema}`.articulo art ON dv.art_id = art.art_id
    LEFT JOIN `{db_schema}`.ticket t ON v.tic_id = t.tic_id
    LEFT JOIN `{db_schema}`.nota n ON v.not_id = n.not_id
    LEFT JOIN `{db_schema}`.cliente cli ON (t.cli_id = cli.cli_id OR n.cli_id = cli.cli_id)
    WHERE v.status = 1 
      AND art.status = 1
      AND (t.tic_id IS NOT NULL OR n.not_id IS NOT NULL)
      AND v.fecha {date_filter}
      {sku_filter_dv}

    UNION ALL

    -- 5. Ajustes Negativos / Mermas (Salida)
    SELECT 
        TRIM(art.clave) AS sku,
        aj.fecha AS fecha,
        'SALIDA' AS tipo,
        'Ajuste Negativo (Merma)' AS concepto,
        COALESCE(aj.comentario, 'Ajuste de Stock') AS detalle,
        ABS(aja.diferencia) AS cantidad
    FROM `{db_schema}`.ajusteinventario aj
    INNER JOIN `{db_schema}`.ajusteinventarioarticulo aja ON aj.ain_id = aja.ain_id
    INNER JOIN `{db_schema}`.articulo art ON aja.art_id = art.art_id
    WHERE aja.diferencia < 0 
      AND art.status = 1
      AND aj.fecha {date_filter}
      {sku_filter_aja}

    UNION ALL

    -- 6. Traspasos Salientes (Salida)
    SELECT 
        TRIM(dt.clave) AS sku,
        COALESCE(t.fechaApl, t.fecha) AS fecha,
        'SALIDA' AS tipo,
        'Traspaso Saliente' AS concepto,
        CONCAT('Hacia: ', COALESCE(t.aliasDes, 'DESTINO DESCONOCIDO')) AS detalle,
        dt.cantidad AS cantidad
    FROM `{db_schema}`.traspaso t
    INNER JOIN `{db_schema}`.detallet dt ON t.tra_id = dt.tra_id
    INNER JOIN `{db_schema}`.articulo art ON dt.art_id = art.art_id
    WHERE t.fechaCan IS NULL 
      AND art.status = 1
      AND COALESCE(t.fechaApl, t.fecha) {date_filter}
      {sku_filter_dt}
    """
    return query

def main():
    parser = argparse.ArgumentParser(description="Consulta consolidada de movimientos de inventario SICAR ERP.")
    parser.add_argument("--db", default="arizone", choices=["arizone", "josivna"], help="Base de datos a consultar.")
    parser.add_argument("--sku", default=None, help="SKU específico a consultar.")
    parser.add_argument("--months", type=int, default=12, help="Rango de meses para el reporte.")
    parser.add_argument("--output", default=None, help="Guardar el resultado en formato CSV con la ruta especificada.")

    args = parser.parse_args()

    print(f"Cargando configuración de base de datos...")
    secrets = load_secrets()
    engine = get_engine(args.db, secrets)

    # Descubrir esquema
    db_schema = "sicar"
    for schema in ["sicar", "SICAR"]:
        try:
            with engine.connect() as conn:
                conn.execute(text(f"SELECT 1 FROM `{schema}`.cliente LIMIT 1"))
            db_schema = schema
            break
        except Exception:
            continue

    print(f"Esquema detectado: {db_schema}")
    print(f"Generando consulta para DB: {args.db}, SKU: {args.sku or 'Todos'}, Meses: {args.months}")
    
    query = construir_query(
        db_schema=db_schema,
        sku=args.sku,
        months=args.months
    )

    # Agregar ordenamiento global (descendente para procesar la reconstrucción)
    final_query = f"{query} ORDER BY fecha DESC"

    try:
        current_stock = 0.0
        if args.sku:
            try:
                with engine.connect() as conn:
                    df_stock = pd.read_sql(
                        text(f"SELECT existencia FROM `{db_schema}`.articulo WHERE TRIM(clave) = '{args.sku}' AND status = 1"),
                        conn
                    )
                current_stock = float(df_stock.iloc[0]["existencia"]) if not df_stock.empty else 0.0
                print(f"Existencia actual en base de datos: {current_stock}")
            except Exception as e:
                print(f"Advertencia: No se pudo obtener la existencia actual para la reconstrucción: {e}")

        with engine.connect() as conn:
            df = pd.read_sql(text(final_query), conn)

        if df.empty:
            print("No se encontraron movimientos para los filtros indicados.")
            return

        if args.sku:
            # Reconstrucción de stock
            stock_after = []
            stock_before = []
            running_stock = current_stock

            for idx, row in df.iterrows():
                qty = float(row['cantidad'])
                tipo = row['tipo']

                sa = running_stock
                if tipo.upper() == 'ENTRADA':
                    sb = sa - qty
                else:  # SALIDA
                    sb = sa + qty

                stock_after.append(sa)
                stock_before.append(sb)
                running_stock = sb

            df['Stock Previo'] = stock_before
            df['Stock Resultante'] = stock_after
            
            # Volver a ordenar cronológicamente de forma ascendente para mostrarlo al usuario
            df = df.sort_values("fecha", ascending=True)

        print(f"\nSe encontraron {len(df)} registros de movimientos:")
        print(df.to_string(index=False, max_rows=50))

        if args.output:
            df.to_csv(args.output, index=False, encoding="utf-8-sig")
            print(f"\nDatos guardados exitosamente en: {args.output}")

    except Exception as e:
        print(f"Error al ejecutar la consulta: {e}")

if __name__ == "__main__":
    main()
