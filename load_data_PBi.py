"""
load_data_PBi.py

Bases de Datos - IMAT
ICAI, Universidad Pontificia Comillas

Integrantes del grupo:
    - Carlos Martínez
    - Lydia Ruiz

Descripción:
Programa que carga toda la infraestructura de los datos de MongoDB para su visualización en PowerBI.
"""

from configuracion import (
    database_name_MongoDB_PBi,
    CONNECTION_STRING,
    folder_path,
)

import os
import json

from datetime import datetime
from pymongo import MongoClient


def insert_collection_data(
    file_path, database_name_MongoDB, collection_name, columns, batch_size=1000
):
    """
    Inserta los datos de un archivo JSON en una colección MongoDB, incluyendo una nueva columna 'type'.

    Args:
        file_path (str): Ruta del archivo JSON.
        database_name_MongoDB (pymongo.database.Database): Base de datos de MongoDB.
        collection_name (str): Nombre de la colección en la que insertar los datos.
        columns (list): Lista de nombres de columnas a extraer del archivo JSON.
        batch_size (int): Tamaño del lote para las inserciones por lotes.

    Returns:
        None
    """
    # Obtiene el nombre del tipo de documento quitando '_5.json'
    file_type = os.path.basename(file_path).replace('_5.json', '')

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
            # Añadir la columna 'type' con el nombre del archivo de origen modificado
            info_json['type'] = file_type
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

    # Configuración de la conexión a la base de datos MongoDB
    client = MongoClient(CONNECTION_STRING)

    database = client[database_name_MongoDB_PBi]

    files_list = os.listdir(folder_path)

    # Nombre fijo de la colección donde se insertarán todos los datos
    collection_name = 'reviews_collection'  # Puedes cambiar este nombre según tus necesidades

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

        # Inserta los datos del archivo en la colección fija
        insert_collection_data(file_path, database, collection_name, columns)