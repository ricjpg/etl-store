
# ETL PYTHON WITH ORACLE

#### PREGUNTAS DEL NEGOCIO

1. Saber cual es el total de ventas por producto, por categoría y por empleado
2. Se desea analizar las ventas por mes, trimestre y año
3. Se desea conocer las ventas por almacén
4. Se desea conocer las ventas por region

**Métrica:**

1. Total de ventas por producto

**Tablas de dimension:**

1. Empleados
2. Tiempo
3. Almacén
4. Region
5. Producto


------------

#### Para instalar el proyecto, sigue estos pasos:

1. Instala Python 3:
2. Instala las dependencias con: `python -m pip install oracledb --upgrade`.
3. Clona este repositorio
4. Asegurate de tener lista la base de datos OLTP **usa el mismo usuario y contraseña de la bd proporcionada**
5. Ejecuta el archivo etl con: `python etl.py`
