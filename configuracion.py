"""
configuracion.py

Bases de Datos - IMAT
ICAI, Universidad Pontificia Comillas

Integrantes del grupo:
    - Carlos Martínez
    - Lydia Ruiz

Descripción:
En este fichero se indican el nombre de la base de datos de MySQL y de MongoDB, así como las credenciales 
necesarias para conectarse a la base de datos y la ruta de la carpeta empleada.
"""

# CREDENCIALES MYSQL
host = "localhost"
user = ""
password = ""

# CONEXIÓN MONGODB
CONNECTION_STRING = "mongodb://localhost:27017"

# NOMBRES BASES DE DATOS
database_name_SQL = "Reviews"
database_name_MongoDB = "Reviews"
database_name_MongoDB_PBi = "Reviews_PBi"

# NEO4J

uri_neo4j = "neo4j://localhost:7687"
user_neo4j = ""
contrasena_neo4j = ""

# RUTA CARPETA
folder_path = "Datos_proyecto/"  # Ruta de la carpeta que contiene los archivos JSON

# NÚMERO DE USUARIOS CON MÁS REVIEWS (4.1 Neo4J)
top_n = 30