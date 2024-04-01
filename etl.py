import oracledb
# CONEXCION BD OLTP
cnx = oracledb.connect(
    user="C##OT",
    password="0000",
    dsn="localhost/xe"
)
cursor = cnx.cursor()

# DROP ALL TABLES
DROP_TBL_EMPLEADOS_ETL = """
    DROP TABLE EMPLEADOS_ETL
"""
DROP_TBL_ALMACEN_ETL = """
    DROP TABLE ALMACEN_ETL
"""
DROP_TBL_REGION_ETL = """
    DROP TABLE REGION_ETL
"""
DROP_TBL_PRODUCTOS_ETL = """
    DROP TABLE PRODUCTOS_ETL
"""
DROP_TBL_TIEMPO_ETL = """
    DROP TABLE TIEMPO_ETL
"""

# FUNCION PARA VERIFICAR SI LA TABLA EXISTE
def tabla_existe(nombre_tabla):
    try:
        # Ejecutar una consulta para verificar la existencia de la tabla
        cursor.execute("SELECT COUNT(*) FROM user_tables WHERE table_name = :nombre_tabla", nombre_tabla=nombre_tabla.upper())
        # Obtener el resultado de la consulta
        result = cursor.fetchone()
        # Si el resultado es mayor que cero, la tabla existe
        tabla_existe = result[0] > 0
    finally:
        print('')
    return tabla_existe

# Ejemplo de uso
if tabla_existe("EMPLEADOS_ETL"):
    print("La tabla 'EMPLEADOS_ETL' existe en la base de datos, SE REEMPLAZARA.")
    cursor.execute(DROP_TBL_EMPLEADOS_ETL)
else:
    print("La tabla 'EMPLEADOS_ETL' no existe en la base de datos, se creara una")

if tabla_existe("ALMACEN_ETL"):
    print("La tabla 'ALMACEN_ETL' existe en la base de datos, SE REEMPLAZARA.")
    cursor.execute(DROP_TBL_ALMACEN_ETL)
else:
    print("La tabla 'ALMACEN_ETL' no existe en la base de datos, se creara una")

if tabla_existe("REGION_ETL"):
    print("La tabla 'REGION_ETL' existe en la base de datos, SE REEMPLAZARA.")
    cursor.execute(DROP_TBL_REGION_ETL)
else:
    print("La tabla 'REGION_ETL' no existe en la base de datos, se creara una")

if tabla_existe("PRODUCTOS_ETL"):
    print("La tabla 'PRODUCTOS_ETL' existe en la base de datos, SE REEMPLAZARA.")
    cursor.execute(DROP_TBL_PRODUCTOS_ETL)
else:
    print("La tabla 'PRODUCTOS_ETL' no existe en la base de datos, se creara una")

if tabla_existe("TIEMPO_ETL"):
    print("La tabla 'TIEMPO_ETL' existe en la base de datos, SE REEMPLAZARA.")
    cursor.execute(DROP_TBL_TIEMPO_ETL)
else:
    print("La tabla 'TIEMPO_ETL' no existe en la base de datos, se creara una")

# CREACION DE DATAMARTS
CREACION_TBL_EMPLEADOS_ETL = """
    CREATE TABLE EMPLEADOS_ETL(
        CODIGO_EMPLEADO NUMBER,
        NOMBRE_COMPLETO VARCHAR2(200)
    )
"""
CREACION_TBL_ALMACEN_ETL = """
    CREATE TABLE ALMACEN_ETL(
        ALMACEN_ID NUMBER PRIMARY KEY,
        NOMBRE_ALMACEN VARCHAR2(200)
    )
"""
CREACION_TBL_REGION_ETL = """
    CREATE TABLE REGION_ETL(
        REGION_ID NUMBER PRIMARY KEY,
        NOMBRE_REGION VARCHAR2 (200)
    )
"""
CREACION_TBL_PRODUCTOS_ETL = """
    CREATE TABLE PRODUCTOS_ETL(
        PRODUCTO_ID NUMBER PRIMARY KEY,
        NOMBRE_PRODUCTO VARCHAR2 (200)
    )
"""
CREACION_TBL_TIEMPO_ETL = """
    CREATE TABLE TIEMPO_ETL(
        CODIGO_TIEMPO NUMBER PRIMARY KEY,
        MES VARCHAR2(20),
        TRIMESTRE VARCHAR2(20),
        ANYO NUMBER
    )
"""

cursor.execute(CREACION_TBL_EMPLEADOS_ETL)
cursor.execute(CREACION_TBL_ALMACEN_ETL)
cursor.execute(CREACION_TBL_REGION_ETL)
cursor.execute(CREACION_TBL_PRODUCTOS_ETL)
cursor.execute(CREACION_TBL_TIEMPO_ETL)

EXTRACT_EMPLOYEES_FROM_OLTP = """
    SELECT EMPLOYEE_ID, CONCAT(CONCAT(EMPLOYEES.FIRST_NAME, ' '), EMPLOYEES.LAST_NAME) AS NOMBRE_COMPLETO FROM EMPLOYEES
"""
EXTRACT_ALMACENES_FROM_OLTP = """
    SELECT WAREHOUSE_ID, WAREHOUSE_NAME FROM WAREHOUSES
"""
EXTRACT_REGIONES_FROM_OLTP = """
    SELECT REGION_ID, REGION_NAME FROM REGIONS
"""
EXTRACT_PRODUCTOS_FROM_OLTP = """
    SELECT PRODUCT_ID, PRODUCT_NAME FROM PRODUCTS
"""
EXTRACT_TIEMPOS_FROM_OLTP = """
    SELECT ORDER_ID, TO_CHAR(ORDER_DATE, 'MONTH') AS MES, 
    TO_CHAR(TO_DATE(ORDER_DATE, 'DD-MON-YY'), 'Q') AS TRIMESTRE, 
    EXTRACT(YEAR FROM TO_DATE(ORDER_DATE, 'DD-MON-YY')) AS ANYO FROM ORDERS
"""


cursor.execute(EXTRACT_EMPLOYEES_FROM_OLTP)
record_employees = cursor.fetchall()

cursor.execute(EXTRACT_ALMACENES_FROM_OLTP)
record_almacen = cursor.fetchall()

cursor.execute(EXTRACT_REGIONES_FROM_OLTP)
record_regiones = cursor.fetchall()

cursor.execute(EXTRACT_PRODUCTOS_FROM_OLTP)
record_productos = cursor.fetchall()

cursor.execute(EXTRACT_TIEMPOS_FROM_OLTP)
record_tiempos = cursor.fetchall()

INSERT_TBL_EMPLEADOS_ETL = """
    INSERT INTO EMPLEADOS_ETL (CODIGO_EMPLEADO, NOMBRE_COMPLETO) VALUES (:CODIGO_EMPLEADO, :NOMBRE_COMPLETO)
"""
INSERT_TBL_ALMACENES_ETL = """
    INSERT INTO ALMACEN_ETL (ALMACEN_ID, NOMBRE_ALMACEN) VALUES (:ALMACEN_ID, :NOMBRE_ALMACEN)
"""
INSERT_TBL_REGIONES_ETL = """
    INSERT INTO REGION_ETL (REGION_ID, NOMBRE_REGION) VALUES (:REGION_ID, :NOMBRE_REGION)
"""
INSERT_TBL_PRODUCTOS_ETL = """
    INSERT INTO PRODUCTOS_ETL (PRODUCTO_ID, NOMBRE_PRODUCTO) VALUES (:PRODUCTO_ID, :NOMBRE_PRODUCTO)
"""
INSERT_TBL_TIEMPOS_ETL = """
    INSERT INTO TIEMPO_ETL (CODIGO_TIEMPO, MES, TRIMESTRE, ANYO) VALUES (:CODIGO_TIEMPO, :MES, :TRIMESTRE, :ANYO)
"""

index = 0
for x in record_employees:
    cursor.execute(INSERT_TBL_EMPLEADOS_ETL, record_employees[index])
    cnx.commit()
    index = index+1
index = 0
for x in record_almacen:
    cursor.execute(INSERT_TBL_ALMACENES_ETL, record_almacen[index])
    cnx.commit()
    index = index+1
index = 0
for x in record_regiones:
    cursor.execute(INSERT_TBL_REGIONES_ETL, record_regiones[index])
    cnx.commit()
    index = index+1
index = 0
for x in record_productos:
    cursor.execute(INSERT_TBL_PRODUCTOS_ETL, record_productos[index])
    cnx.commit()
    index = index+1
index = 0
for x in record_tiempos:
    cursor.execute(INSERT_TBL_TIEMPOS_ETL, record_tiempos[index])
    cnx.commit()
    index = index+1

