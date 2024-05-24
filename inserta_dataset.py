"""
inserta_dataset.py

Bases de Datos - IMAT
ICAI, Universidad Pontificia Comillas

Integrantes del grupo:
    - Carlos Martínez
    - Lydia Ruiz

Descripción:
Programa que carga la información de un nuevo fichero en las bases de datos previamente existentes.
"""

import os
import json
import pymysql
from datetime import datetime
from pymongo import MongoClient

from configuracion import (
    host,
    user,
    password,
    database_name_SQL,
    database_name_MongoDB,
    CONNECTION_STRING
)

def get_data(file_path):
    """
    Lee un archivo JSON y carga los datos en una lista.

    Args:
        file_path (str): Ruta del archivo JSON.

    Returns:
        list: Lista de diccionarios que representan los datos cargados desde el archivo JSON.
    """
    data = []
    with open(file_path, "r") as fp: 
        data = [json.loads(line) for line in fp]
    return data

def get_table_data(host, user, password, database_name, table_name):
    """
    Obtiene los datos de una tabla específica en la base de datos MySQL.

    Args:
        host (str): Dirección del host de la base de datos MySQL.
        user (str): Nombre de usuario de la base de datos MySQL.
        password (str): Contraseña de la base de datos MySQL.
        database_name (str): Nombre de la base de datos MySQL.
        table_name (str): Nombre de la tabla de la que seleccionar los datos.

    Returns:
        list: Lista de tuplas representando las filas recuperadas de la tabla.
    """
    connection_mysql = pymysql.connect(host=host, user=user, password=password, database=database_name)
    with connection_mysql:
        cursor = connection_mysql.cursor()
        sql = f"""SELECT * 
                  FROM {table_name};"""
        cursor.execute(sql)
        result = cursor.fetchall()
        cursor.close()

    return result

def get_new_ids_names(new_data, id_column_name="ID", name_column_name="Name"):
    """
    Obtiene los nuevos IDs y nombres de una lista de datos.

    Args:
        new_data (list): Lista de diccionarios representando los nuevos datos.
        id_column_name (str, optional): Nombre de la columna que contiene los IDs. Defaults to "ID".
        name_column_name (str, optional): Nombre de la columna que contiene los nombres. Defaults to "Name".

    Returns:
        set: Conjunto de tuplas (ID, Nombre) representando los nuevos IDs y nombres.
    """
    ids_names_new = {(entry.get(id_column_name, ""), entry.get(name_column_name, "")) for entry in new_data if entry.get(name_column_name)}
    reviewer_ids_with_name = {reviewer_id for reviewer_id, reviewer_name in ids_names_new if reviewer_name}
    ids_names_new = {(reviewer_id, reviewer_name) for reviewer_id, reviewer_name in ids_names_new if reviewer_name or reviewer_id not in reviewer_ids_with_name}
    return ids_names_new

def get_unique_ids_names(old_ids_names, new_ids_names):
    """
    Obtiene los IDs y nombres únicos que deben insertarse.

    Args:
        old_ids_names (list): Lista de tuplas representando los IDs y nombres existentes.
        new_ids_names (set): Conjunto de tuplas representando los nuevos IDs y nombres.

    Returns:
        list: Lista de tuplas representando los IDs y nombres únicos que deben insertarse.
    """
    old_ids, _ = zip(*old_ids_names)
    unique_ids_names = [(id, name) for id, name in new_ids_names if id not in old_ids]
    return unique_ids_names

def get_last_product_id(host, user, password, database_name, table_name):
    """
    Obtiene el número de filas de la tabla

    Args:
        host (str): Dirección del host de la base de datos MySQL.
        user (str): Nombre de usuario de la base de datos MySQL.
        password (str): Contraseña de la base de datos MySQL.
        database_name (str): Nombre de la base de datos MySQL.
        table_name (str): Nombre de la tabla de la que seleccionar los datos.

    Returns:
        int: Número de filas de la tabla.
    """
    connection_mysql = pymysql.connect(host=host, user=user, password=password, database=database_name)
    with connection_mysql:
        cursor = connection_mysql.cursor()
        sql = f"""SELECT * 
                  FROM {table_name};"""
        result = cursor.execute(sql)
        cursor.close()

    return result

def get_new_asins_types(host, user, password, database_name, table_name, new_data, column_name, type):
    """
    Obtiene los nuevos ASINs y tipos de una lista de datos.

    Args:
        host (str): Dirección del host de la base de datos MySQL.
        user (str): Nombre de usuario de la base de datos MySQL.
        password (str): Contraseña de la base de datos MySQL.
        database_name (str): Nombre de la base de datos MySQL.
        table_name (str): Nombre de la tabla de la que seleccionar los datos.
        new_data (list): Lista de diccionarios representando los nuevos datos.
        column_name (str): Nombre de la columna que contiene los ASINs.
        type (str): Tipo asociado a los nuevos datos.

    Returns:
        list: Lista de tuplas representando los nuevos ASINs y tipos.
    """
    connection_mysql = pymysql.connect(host=host, user=user, password=password, database=database_name)
    with connection_mysql:
        cursor = connection_mysql.cursor()
        sql = f"""SELECT ID FROM {table_name}"""
        max_id = cursor.execute(sql)
        cursor.close()
   
    asins_types = list(set((entry.get(column_name, ""), type) for entry in new_data))
    new_asins_types = []
    for asin_type in asins_types:
        max_id += 1
        new_asin_type = (max_id,) + asin_type
        new_asins_types.append(new_asin_type)

    return new_asins_types

def insert_new_data_table_sql(host, user, password, database_name, table_name, data_to_insert, column_names):
    """
    Inserta datos en una tabla específica de la base de datos MySQL.

    Args:
        host (str): Dirección del host de la base de datos MySQL.
        user (str): Nombre de usuario de la base de datos MySQL.
        password (str): Contraseña de la base de datos MySQL.
        database_name (str): Nombre de la base de datos MySQL.
        table_name (str): Nombre de la tabla en la que insertar los datos.
        data_to_insert (list): Lista de tuplas representando los datos a insertar.
        column_names (list): Lista de nombres de columnas en la tabla.

    Returns:
        None
    """
    column_names_str = ", ".join(column_names)
    placeholders = ", ".join(["%s" for _ in range(len(column_names))])
    connection_mysql = pymysql.connect(host=host, user=user, password=password, database=database_name)
    with connection_mysql:
        cursor = connection_mysql.cursor()
        sql = f"INSERT INTO {table_name} ({column_names_str}) VALUES({placeholders});"
        cursor.executemany(sql, data_to_insert)
        connection_mysql.commit()
        cursor.close()


def insert_new_data_sql(host, user, password, data, database_name):
    """
    Inserta nuevos datos en una base de datos MySQL.

    Args:
        host (str): Dirección del host de la base de datos MySQL.
        user (str): Nombre de usuario de la base de datos MySQL.
        password (str): Contraseña de la base de datos MySQL.
        data (list): Lista de diccionarios que representan los datos cargados desde el archivo JSON.
        database_name (str): Nombre de la base de datos MySQL.

    Returns:
        None
    """
    global REVIEWERS_TABLE, ITEMS_TABLE, PRODUCTS_TABLE, collection_name

    # Obtiene los datos existentes de la tabla de revisores
    old_ids_names = get_table_data(host, user, password, database_name, REVIEWERS_TABLE)
    # Obtiene los nuevos IDs y nombres del archivo JSON
    new_ids_names = get_new_ids_names(data)

    # Obtiene los IDs y nombres únicos que deben insertarse
    ids_names_to_insert = get_unique_ids_names(old_ids_names, new_ids_names)
    # Inserta los nuevos IDs y nombres en la tabla de revisores
    insert_new_data_table_sql(host, user, password, database_name, REVIEWERS_TABLE, ids_names_to_insert, ["ID", "Name"])

    # Obtiene el ID del nuevo tipo de producto
    new_product_id = get_last_product_id(host, user, password, database_name, PRODUCTS_TABLE)
    # Inserta el nuevo ID y el nombre del tipo de producto
    insert_new_data_table_sql(host, user, password, database_name, PRODUCTS_TABLE, [(new_product_id, collection_name)], ["ID", "Type"])

    # Obtiene los nuevos ASINs y tipos del archivo JSON
    asins_types_to_insert = get_new_asins_types(host, user, password, database_name, ITEMS_TABLE, data, "asin", new_product_id)
    # Inserta los nuevos ASINs y tipos en la tabla de artículos
    insert_new_data_table_sql(host, user, password, database_name, ITEMS_TABLE, asins_types_to_insert, ["ID", "asin", "type"])
    
def insert_new_data_mongo(data, database, collection_name, columns, batch_size= 1000): 
    """
    Inserta los datos de un archivo JSON en una colección MongoDB.

    Args:
        data (list): Lista de diccionarios que representan los datos cargados desde el archivo JSON.
        database (pymongo.database.Database): Base de datos de MongoDB.
        collection_name (str): Nombre de la colección en la que insertar los datos.
        columns (list): Lista de nombres de columnas a extraer del archivo JSON.
        batch_size (int): Tamaño del lote para las inserciones por lotes.

    Returns:
        None
    """
    collection = database[collection_name]

    batch = []
    batch_counter = 0


    for line in data:
        info_json = {column: line.get(column, "") for column in columns}
        # Convierte la cadena de tiempo a un objeto de fecha
        info_json["reviewTime"] = datetime.strptime(info_json['reviewTime'], '%m %d, %Y')
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

    # Ruta del archivo y datos que almacena
    new_file_path = "Pet_Supplies_5.json"
    data = get_data(new_file_path)

    # Nombre de base de datos y de la colección
    collection_name = os.path.basename(new_file_path).replace("_5.json", "")

    # Nombres de tablas de MySQL
    REVIEWERS_TABLE = "reviewers"
    ITEMS_TABLE = "items"
    PRODUCTS_TABLE = "products"

    # Introducir datos en MySQL
    insert_new_data_sql(host, user, password, data, database_name_SQL)

    # Configuración de la conexión a la base de datos MongoDB
    client = MongoClient(CONNECTION_STRING)
    database = client[database_name_MongoDB]

    # Columnas que se van a extraer de los archivos JSON
    mongo_columns = ["reviewerID", "asin", "helpful", "overall", "summary", "reviewText", "reviewTime", "unixReviewTime"]

    # Introducir datos en MongoDB
    insert_new_data_mongo(data, database, collection_name, mongo_columns)