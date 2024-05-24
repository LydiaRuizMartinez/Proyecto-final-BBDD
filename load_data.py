"""
load_data.py

Bases de Datos - IMAT
ICAI, Universidad Pontificia Comillas

Integrantes del grupo:
    - Carlos Martínez
    - Lydia Ruiz

Descripción:
Programa que carga toda la infraestructura de los datos tanto en MySQL como en MongoDB.
"""

from configuracion import (
    host,
    user,
    password,
    database_name_SQL,
    database_name_MongoDB,
    CONNECTION_STRING,
    folder_path,
)

import os
import json
import pymysql
from datetime import datetime
from pymongo import MongoClient


from typing import List


def create_database(host: str, user: str, password: str, database_name: str, collections_columns: List):
    """
    Crea una base de datos MySQL y las tablas especificadas si no existen.

    Args:
        host (str): Dirección del servidor MySQL.
        user (str): Nombre de usuario de MySQL.
        password (str): Contraseña de MySQL.
        database_name (str): Nombre de la base de datos a crear.
        collections_columns (List): Lista de tuplas que contienen la información de las tablas.
                                    Cada tupla debe tener el nombre de la tabla, una lista de tuplas
                                    con los nombres y tipos de columnas, y el nombre de la columna
                                    primaria y su tipo.

    Returns:
        None
    """
    connection_mysql = pymysql.connect(host=host, user=user, password=password)
    with connection_mysql:
        cursor = connection_mysql.cursor()

        # Crea la base de datos
        cursor.execute(f"CREATE DATABASE {database_name}")
        cursor.execute(f"USE {database_name}")

        # Crea cada tabla 
        for collection in collections_columns:
            table_name, columns, keys = collection
            table_keys = ", ".join(keys)

            # Define la estructura de la tabla
            column_definitions = ", ".join(f"{name} {type}" for name, type in columns)
            sql = f"""CREATE TABLE {table_name} ({column_definitions}, {table_keys});"""

            cursor.execute(sql)

def insert_data(host: str, user: str, password: str, database_name: str, collections_columns: List, data: List[List]):
    """
    Inserta datos en las tablas de la base de datos.

    Args:
        host (str): Dirección del servidor MySQL.
        user (str): Nombre de usuario de MySQL.
        password (str): Contraseña de MySQL.
        database_name (str): Nombre de la base de datos.
        collections_columns (List): Lista de tuplas que contienen la información de las tablas.
                                    Cada tupla debe tener el nombre de la tabla y una lista de tuplas
                                    con los nombres y tipos de columnas.
        data (List[List]): Lista de datos a insertar en las tablas. Cada lista interna contiene
                            los datos correspondientes a una tabla.

    Returns:
        None
    """
    connection_mysql = pymysql.connect(host=host, user=user, password=password, database=database_name)
    with connection_mysql:
        cursor = connection_mysql.cursor()

        # Inserta los datos en las tablas
        for collection, collection_data in zip(collections_columns, data):
            table_name, columns, _ = collection

            # Construye la consulta SQL para insertar los datos
            column_names = ", ".join(name for name, _ in columns)
            placeholders = ", ".join(["%s"] * len(columns))

            sql = f"INSERT INTO {table_name} ({column_names}) VALUES ({placeholders});"
            
            # Ejecuta la consulta SQL con los datos correspondientes
            cursor.executemany(sql, collection_data)

        # Confirma los cambios en la base de datos
        connection_mysql.commit()
        cursor.close()

def obtain_data_sql(folder_path: str):
    """
    Función para obtener los tipos, nombres e IDs de varios ficheros.

    Args:
        folder_path (str): Ruta de la carpeta que contiene los ficheros.

    Returns:
        tuple: Una tupla conteniendo tres listas: la lista de IDs, la lista de tipos de productos 
              y la lista de ASINs.
    """
    
    ids = []
    asins = []

    seen_ids = set()    # Conjunto para rastrear IDs únicos
    cont_ids_asins = 1  # Contador para rastrear asins únicos
    
    # Obtener los nombres de los archivos en la carpeta y 
    # crear diccionario para mapear el tipo de cada archivo 
    files_list = os.listdir(folder_path)
    types_dict = {count: value.replace("_5.json", "") for count, value in enumerate(files_list)}
    types_list = list(types_dict.items())

    # Iterar sobre cada archivo en la lista
    for count, file in enumerate(files_list):
        path_file = os.path.join(folder_path, file)

        file_asins = []

        seen_asins = set()

        # Abrir el archivo y leer cada línea
        with open(path_file, mode="r") as fp:
            for line in fp:
                line_json = json.loads(line)

                # Obtener el ID y el ASIN de la línea
                id = line_json.get("reviewerID", "")
                name = line_json.get("reviewerName", "")
                asin = line_json.get("asin", "")
                
                # Si el ID es nuevo, agregarlo a la lista de IDs
                if id not in seen_ids and name != "":
                    ids.append((id, name))
                    seen_ids.add(id)

                # Si el ASIN es nuevo, agregarlo a la lista de ASINs
                if asin not in seen_asins:
                    file_asins.append((cont_ids_asins, asin, count))
                    seen_asins.add(asin)
                    cont_ids_asins += 1

        # Agregar los ASINs únicos del archivo a la lista de ASINs total
        asins += file_asins

    return ids, types_list, asins

def insert_collection_data(
    file_path, database_name_MongoDB, collection_name, columns, batch_size=1000
):
    """
    Inserta los datos de un archivo JSON en una colección MongoDB.

    Args:
        file_path (str): Ruta del archivo JSON.
        database_name_MongoDB (pymongo.database.Database): Base de datos de MongoDB.
        collection_name (str): Nombre de la colección en la que insertar los datos.
        columns (list): Lista de nombres de columnas a extraer del archivo JSON.
        batch_size (int): Tamaño del lote para las inserciones por lotes.

    Returns:
        None
    """
    # Obtiene la colección en la base de datos
    collection = database_name_MongoDB[collection_name]
    batch = []
    batch_counter = 0

    # Abre el archivo JSON y procesa cada línea
    with open(file_path, "r") as fp:
        for line in fp:
            # Carga la línea JSON y extrae las columnas especificadas
            line_json = json.loads(line)
            info_json = {column: line_json.get(column, "") for column in columns}
            info_json["reviewTime"] = datetime.strptime(
                info_json["reviewTime"], "%m %d, %Y"
            )
            batch.append(info_json)
            batch_counter += 1

            # Insertar lote en la base de datos cuando se alcanza el tamaño del lote
            if batch_counter >= batch_size:
                collection.insert_many(batch)
                batch = []  # Reiniciar lote
                batch_counter = 0  # Reiniciar contador

        # Insertar documentos restantes en el último lote
        if batch:
            collection.insert_many(batch)


if __name__ == "__main__":

   

    collections_columns = [
    ("Reviewers", [("ID", "VARCHAR(100)"), ("Name", "VARCHAR(100)")], ["PRIMARY KEY (ID)"]),
    ("Products", [("ID", "INT"), ("Type", "VARCHAR(100)")], ["PRIMARY KEY (ID)"]),
    ("Items", [("ID", "INT"), ("Asin", "VARCHAR(100)"), ("Type", "INT")], ["PRIMARY KEY (ID)", "FOREIGN KEY (Type) REFERENCES PRODUCTS(ID)"]),
]

    create_database(host, user, password, database_name_SQL, collections_columns)

    data = obtain_data_sql(folder_path)
    insert_data(host, user, password, database_name_SQL, collections_columns, data)

    # Configuración de la conexión a la base de datos MongoDB
    client = MongoClient(CONNECTION_STRING)

    database = client[database_name_MongoDB]

    files_list = os.listdir(folder_path)

    # Columnas que se van a extraer de los archivos JSON
    columns = [
        "reviewerID",
        "asin",
        "helpful",
        "overall",
        "summary",
        "reviewText",
        "reviewTime",
        "unixReviewTime",
    ]

    # Itera sobre cada archivo JSON en la carpeta
    for file in files_list:
        file_path = os.path.join(folder_path, file)
        collection_name = os.path.basename(file_path).replace("_5.json", "")

        # Inserta los datos del archivo en la colección correspondiente
        insert_collection_data(file_path, database, collection_name, columns)
