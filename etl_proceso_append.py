
import pandas as pd
import mysql.connector
from sqlalchemy import create_engine

# Conexión a MySQL usando SQLAlchemy
engine_trans = create_engine("mysql+mysqlconnector://root:@localhost:3306/megastore_transaccional")
engine_dw = create_engine("mysql+mysqlconnector://root:@localhost:3306/megastore_dw_V2")

# 1. EXTRACCIÓN: Leer datos desde la base transaccional
ventas = pd.read_sql("SELECT * FROM ventas", engine_trans)
clientes = pd.read_sql("SELECT * FROM clientes", engine_trans)
productos = pd.read_sql("SELECT * FROM productos", engine_trans)
tiendas = pd.read_sql("SELECT * FROM tiendas", engine_trans)
empleados = pd.read_sql("SELECT * FROM empleados", engine_trans)

# 2. TRANSFORMACIÓN: Procesar fechas y calcular medidas
ventas['fecha'] = pd.to_datetime(ventas['fecha'])
ventas['dia'] = ventas['fecha'].dt.day
ventas['mes'] = ventas['fecha'].dt.month
ventas['año'] = ventas['fecha'].dt.year
ventas['trimestre'] = ventas['fecha'].dt.quarter
ventas['nombre_mes'] = ventas['fecha'].dt.month_name()
ventas['temporada'] = ventas['mes'].apply(lambda m: 'Verano' if m in [6,7,8] else 'Invierno' if m in [12,1,2] else 'Primavera' if m in [3,4,5] else 'Otoño')

# Crear Dim_Tiempo considerando datos existentes
print("🔄 Procesando dimensión tiempo...")
try:
    # Leer datos existentes de Dim_Tiempo
    existing_tiempo = pd.read_sql("SELECT * FROM Dim_Tiempo", engine_dw)
    max_id_tiempo = existing_tiempo['id_tiempo'].max() if not existing_tiempo.empty else 0
    print(f"ℹ️  Máximo ID existente en Dim_Tiempo: {max_id_tiempo}")
except Exception as e:
    print(f"ℹ️  Tabla Dim_Tiempo no existe o está vacía: {e}")
    existing_tiempo = pd.DataFrame()
    max_id_tiempo = 0

# Crear nuevos registros de tiempo
dim_tiempo_new = ventas[['fecha', 'dia', 'mes', 'trimestre', 'año', 'nombre_mes', 'temporada']].drop_duplicates().reset_index(drop=True)

# Si existe tabla, verificar cuáles son nuevos registros
if not existing_tiempo.empty:
    # Convertir fecha existente para comparación
    existing_tiempo['fecha'] = pd.to_datetime(existing_tiempo['fecha'])
    
    # Encontrar registros que no existen
    merged = dim_tiempo_new.merge(
        existing_tiempo[['fecha', 'dia', 'mes', 'trimestre', 'año', 'nombre_mes', 'temporada']], 
        on=['fecha', 'dia', 'mes', 'trimestre', 'año', 'nombre_mes', 'temporada'], 
        how='left', 
        indicator=True
    )
    dim_tiempo_new = merged[merged['_merge'] == 'left_only'].drop('_merge', axis=1)

# Asignar nuevos IDs solo a registros nuevos
if not dim_tiempo_new.empty:
    dim_tiempo_new['id_tiempo'] = range(max_id_tiempo + 1, max_id_tiempo + 1 + len(dim_tiempo_new))
    print(f"ℹ️  Se procesarán {len(dim_tiempo_new)} registros nuevos de tiempo")
else:
    print("ℹ️  No hay registros nuevos de tiempo para procesar")

# Crear tabla completa de tiempo para el merge con ventas
if existing_tiempo.empty:
    dim_tiempo_complete = dim_tiempo_new.copy()
else:
    dim_tiempo_complete = pd.concat([existing_tiempo, dim_tiempo_new], ignore_index=True)

# Añadir id_tiempo a ventas
ventas = ventas.merge(dim_tiempo_complete[['fecha', 'dia', 'mes', 'trimestre', 'año', 'nombre_mes', 'temporada', 'id_tiempo']], 
                     on=['fecha', 'dia', 'mes', 'trimestre', 'año', 'nombre_mes', 'temporada'])

# Calcular ingreso total
ventas = ventas.merge(productos[['id_producto', 'precio']], on='id_producto')
ventas['ingreso_total'] = ventas['precio'] * ventas['cantidad'] * (1 - ventas['descuento'])

# 3. CARGA: Insertar en el DW con verificación de duplicados
print("🔄 Iniciando proceso de carga...")

# Función para insertar solo registros nuevos
def insert_new_records(df, table_name, engine, id_column):
    try:
        # Leer datos existentes
        existing_df = pd.read_sql(f"SELECT {id_column} FROM {table_name}", engine)
        existing_ids = existing_df[id_column].tolist()
        
        # Filtrar solo registros nuevos
        new_records = df[~df[id_column].isin(existing_ids)]
        
        if len(new_records) > 0:
            new_records.to_sql(table_name, engine, if_exists="append", index=False)
            print(f"✅ {len(new_records)} registros nuevos insertados en {table_name}")
        else:
            print(f"ℹ️  No hay registros nuevos para insertar en {table_name}")
            
    except Exception as e:
        # Si la tabla no existe, insertar todos los registros
        if "doesn't exist" in str(e) or "Table" in str(e) or "1146" in str(e):
            df.to_sql(table_name, engine, if_exists="replace", index=False)
            print(f"✅ Tabla {table_name} creada con {len(df)} registros")
        else:
            print(f"❌ Error al procesar {table_name}: {e}")

# Cargar dimensión tiempo (solo registros nuevos)
if not dim_tiempo_new.empty:
    insert_new_records(
        dim_tiempo_new[['id_tiempo', 'fecha', 'dia', 'mes', 'trimestre', 'año', 'nombre_mes', 'temporada']], 
        "Dim_Tiempo", engine_dw, "id_tiempo"
    )

# Cargar otras dimensiones
insert_new_records(
    productos[['id_producto', 'nombre', 'categoria', 'precio']], 
    "Dim_Producto", engine_dw, "id_producto"
)

clientes['edad'] = pd.Timestamp.now().year - pd.to_datetime(clientes['fecha_nacimiento']).dt.year
insert_new_records(
    clientes[['id_cliente', 'nombre', 'correo', 'genero', 'edad']], 
    "Dim_Cliente", engine_dw, "id_cliente"
)

insert_new_records(tiendas, "Dim_Tienda", engine_dw, "id_tienda")
insert_new_records(empleados, "Dim_Empleado", engine_dw, "id_empleado")

# Cargar hechos
fact_ventas = ventas[['id_venta', 'id_tiempo', 'id_producto', 'id_cliente', 'id_empleado', 'id_tienda', 'cantidad', 'ingreso_total', 'descuento']]
insert_new_records(fact_ventas, "Fact_Ventas", engine_dw, "id_venta")

print("✅ Proceso ETL completado con éxito.")
