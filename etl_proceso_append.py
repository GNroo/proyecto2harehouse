
import pandas as pd
import mysql.connector
from sqlalchemy import create_engine

# Conexión a MySQL usando SQLAlchemy
engine_trans = create_engine("mysql+mysqlconnector://root:@localhost:3306/megastore_transaccional")
engine_dw = create_engine("mysql+mysqlconnector://root:@localhost:3306/megastore_dw")

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

# Crear Dim_Tiempo
dim_tiempo = ventas[['fecha', 'dia', 'mes', 'trimestre', 'año', 'nombre_mes', 'temporada']].drop_duplicates().reset_index(drop=True)
dim_tiempo['id_tiempo'] = dim_tiempo.index + 1

# Añadir id_tiempo a ventas
ventas = ventas.merge(dim_tiempo, on=['fecha', 'dia', 'mes', 'trimestre', 'año', 'nombre_mes', 'temporada'])

# Calcular ingreso total
ventas = ventas.merge(productos[['id_producto', 'precio']], on='id_producto')
ventas['ingreso_total'] = ventas['precio'] * ventas['cantidad'] * (1 - ventas['descuento'])

# 3. CARGA: Insertar en el DW
# Cargar dimensiones
dim_tiempo[['id_tiempo', 'fecha', 'dia', 'mes', 'trimestre', 'año', 'nombre_mes', 'temporada']].to_sql("Dim_Tiempo", engine_dw, if_exists="append", index=False)
productos[['id_producto', 'nombre', 'categoria', 'precio']].to_sql("Dim_Producto", engine_dw, if_exists="append", index=False)
clientes['edad'] = pd.Timestamp.now().year - pd.to_datetime(clientes['fecha_nacimiento']).dt.year
clientes[['id_cliente', 'nombre', 'correo', 'genero', 'edad']].to_sql("Dim_Cliente", engine_dw, if_exists="append", index=False)
tiendas.to_sql("Dim_Tienda", engine_dw, if_exists="append", index=False)
empleados.to_sql("Dim_Empleado", engine_dw, if_exists="append", index=False)

# Cargar hechos
fact_ventas = ventas[['id_venta', 'id_tiempo', 'id_producto', 'id_cliente', 'id_empleado', 'id_tienda', 'cantidad', 'ingreso_total', 'descuento']]
fact_ventas.to_sql("Fact_Ventas", engine_dw, if_exists="append", index=False)

print("✅ Proceso ETL completado con éxito.")
