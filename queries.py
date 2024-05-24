"""
queries.py

Bases de Datos - IMAT
ICAI, Universidad Pontificia Comillas

Integrantes del grupo:
    - Carlos Martínez
    - Lydia Ruiz

Descripción:
Programa para obtener diferentes plots de visualización de diferentes datos usados en el menú.
"""

import matplotlib.pyplot as plt
from collections import defaultdict
from wordcloud import WordCloud


def first_query(database, collection_names):
    """
    Realiza una consulta a la base de datos MongoDB para obtener el recuento de revisiones
    por año para cada producto en las colecciones especificadas.

    Parameters:
        - database (pymongo.database.Database): Objeto de base de datos MongoDB.
        - collection_names (list): Lista de nombres de colecciones en la base de datos.

    Returns:
        - reviews_counts_by_year: Un diccionario defaultdict donde las claves son los
          años y los valores son los recuentos de revisiones de ese año.
    """
    reviews_counts_by_year = defaultdict(int)

    for collection_name in collection_names:
        collection = database[collection_name]

        # Realizar una consulta de agregación para calcular el recuento de revisiones por año
        query = collection.aggregate(
            [
                {"$group": {"_id": {"$year": "$reviewTime"}, "count": {"$sum": 1}}},
                {"$sort": {"_id": 1}},
            ]
        )

        # Procesar los resultados de la consulta
        for result in query:
            year = result["_id"]
            count = result["count"]
            reviews_counts_by_year[year] = count

    return reviews_counts_by_year


def plot_reviews_year(reviews_counts_by_year, product_type="todos los productos"):
    """
    Grafica la evolución del número de revisiones por año para los productos especificados.

    Parameters:
        - reviews_counts_by_year: Un diccionario que contiene los recuentos de revisiones
          por año para cada producto.
        - product_type: El tipo de producto para el que se están graficando las revisiones.
          Por defecto, se establece en "todos los productos".
    """
    # Desempaquetar las claves (años) y los valores (recuentos de revisiones) del diccionario
    years, review_counts = zip(*reviews_counts_by_year.items())

    # Crear el gráfico de barras
    plt.figure(figsize=(10, 6))
    plt.bar(years, review_counts)

    # Añadir título y etiquetas de ejes al gráfico
    plt.title(f"Reviews por año de {product_type}")
    plt.xlabel("Años")
    plt.ylabel("Número de reviews")

    # Establecer los años como etiquetas en el eje x
    plt.xticks(range(min(years), max(years) + 1))

    # Mostrar gráfico
    plt.grid(axis="y", linestyle="--", alpha=0.7)
    plt.show()


def second_query(database, collection_names):
    """
    Realiza una consulta a la base de datos MongoDB para obtener el recuento de revisiones
    por mes para cada producto en las colecciones especificadas.

    Parameters:
        - database (pymongo.database.Database): Objeto de base de datos MongoDB.
        - collection_names (list): Lista de nombres de colecciones en la base de datos.

    Returns:
        - reviews_counts_by_month: Un diccionario defaultdict donde las claves son los
          códigos ASIN de los productos y los valores son los recuentos de revisiones.
    """
    reviews_counts_by_month = defaultdict(int)

    for collection_name in collection_names:
        collection = database[collection_name]

        # Realizar una consulta de agregación para calcular el recuento de revisiones
        query = collection.aggregate(
            [
                {"$group": {"_id": "$asin", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}},
            ]
        )

        # Procesar los resultados de la consulta
        for result in query:
            asin = result["_id"]
            count = result["count"]
            reviews_counts_by_month[asin] = count

    return reviews_counts_by_month


def plot_reviews_asin(reviews_counts_by_month, product_type="todos los productos"):
    """
    Grafica la evolución de la popularidad de los productos a lo largo del tiempo,
    utilizando el recuento de revisiones por mes.

    Parameters:
        - reviews_counts_by_month: Un diccionario que contiene los recuentos de revisiones
          por mes para cada producto.
        - product_type: El tipo de producto para el que se están graficando las revisiones.
          Por defecto, se establece en "todos los productos".
    """
    asin_list, count_list = zip(*reviews_counts_by_month.items())

    # Graficar los recuentos de revisiones
    plt.figure(figsize=(8, 6))
    plt.plot(list(range(len(asin_list))), count_list)

    # Configuración de la apariencia de la gráfica
    plt.title(f"Evolución de la popularidad de {product_type}")
    plt.xlabel("Artículos")
    plt.ylabel("Número de reviews")
    plt.xticks([])
    plt.show()


def third_query(database, collection_options, user_option="Everything"):
    score_counts = {}

    collections_to_query = (
        [user_option] if user_option in collection_options else collection_options
    )

    for option in collections_to_query:
        collection = database[option]
        query = collection.aggregate(
            [
                {"$group": {"_id": "$overall", "count": {"$sum": 1}}},
                {"$sort": {"_id": 1}},
            ]
        )
        for doc in query:
            if doc["_id"] in score_counts:
                score_counts[doc["_id"]] += doc["count"]
            else:
                score_counts[doc["_id"]] = doc["count"]

    scores = list(score_counts.keys())
    counts = [score_counts[score] for score in scores]

    plt.figure(figsize=(10, 6))
    plt.bar(scores, counts, width=0.4)
    plt.title(
        f"Reviews por nota {'de todos los productos' if user_option == 'Everything' else 'de ' + user_option}"
    )
    plt.xlabel("Nota")
    plt.ylabel("Número de reviews")
    plt.xticks(scores)
    plt.grid(axis="y", linestyle="--", alpha=0.7)
    plt.show()


def fourth_query(database, collection_names):
    """
    Realiza una consulta a la base de datos y devuelve una lista de timestamps de reviews.

    Parameters:
        database (pymongo.database.Database): La base de datos MongoDB.
        collection_names (list): Lista de nombres de colecciones a consultar.

    Returns:
        list: Lista de timestamps de reviews.
    """

    time_stamps = []
    for collection in collection_names:
        collection = database[collection]

        # Consultar la colección y extraer los timestamps de las reviews
        query = collection.find({}, {"_id": 0, "unixReviewTime": 1})
        time_stamps.extend([result["unixReviewTime"] for result in query])

    # Ordenar los timestamps en orden ascendente
    time_stamps.sort()

    return time_stamps


def plot_reviews_evolution(time_stamp, product_type="todos los productos"):
    """
    Grafica la evolución de las reviews a lo largo del tiempo.

    Parameters:
        timestamps (list): Lista de timestamps de reviews.
        product_type (str): Tipo de producto. Por defecto, "Todos los productos".
    """

    # Crear el gráfico
    plt.figure(figsize=(8, 6))
    plt.plot(time_stamp, range(len(time_stamp)))

    # Añadir título y etiquetas de ejes al gráfico
    plt.title(f"Evolución de las reviews a lo largo del tiempo de {product_type}")
    plt.xlabel("Tiempo")
    plt.ylabel("Número de reviews hasta ese momento")

    # Añadir una cuadrícula al gráfico para mayor claridad
    plt.grid(axis="y", linestyle="--", alpha=0.7)

    # Mostrar el gráfico
    plt.show()


def fifth_query(database, collection_names):
    """
    Cuenta el número de revisiones por usuario en las colecciones especificadas.

    Parameters:
        - database (pymongo.database.Database): Objeto de base de datos MongoDB.
        - collection_names (list): Lista de nombres de colecciones en la base de datos.

    Returns:
        - reviews_by_user: Un diccionario donde las claves son los IDs de los revisores
          y los valores son el número de revisiones que ha realizado cada usuario.
    """
    reviews_by_user = defaultdict(int)

    for collection_name in collection_names:
        collection = database[collection_name]

        # Realizar una consulta de agregación para calcular el recuento de revisiones por usuario
        query = collection.aggregate(
            [{"$group": {"_id": "$reviewerID", "count": {"$sum": 1}}}]
        )

        # Procesar los resultados de la consulta
        for result in query:
            user = result["_id"]
            count = result["count"]
            reviews_by_user[user] += count

    return reviews_by_user


def plot_reviews_user(reviews_by_user):
    """
    Grafica el histograma del número de revisiones por usuario.

    Parameters:
        - reviews_by_user: Un diccionario que contiene los IDs de los revisores
          y el número de revisiones que ha realizado cada usuario.
    """
    counts_histogram = defaultdict(int)

    # Contar las frecuencias de los recuentos de revisiones por usuario
    for count in reviews_by_user.values():
        counts_histogram[count] += 1

    # Ordenar el histograma por los recuentos de revisiones
    counts_histogram = dict(sorted(counts_histogram.items()))

    # Graficar el histograma
    plt.figure(figsize=(8, 6))
    plt.bar(counts_histogram.keys(), counts_histogram.values())
    plt.xticks([])
    plt.xlabel("Número de reviews")
    plt.ylabel("Número de usuarios")
    plt.title("Histograma del número de reviews por usuario")
    plt.show()


def sixth_query(database, collection_options):
    """
    Consulta la base de datos para extraer el texto de las reviews de una colección específica.

    Parameters:
        database (pymongo.database.Database): La base de datos MongoDB.
        collection_options (str): El nombre de la colección a consultar.

    Returns:
        list: Lista de textos de las reviews.
    """
    collection = database[collection_options]
    query = collection.find({}, {"_id": 0, "reviewText": 1})
    review_texts = [result["reviewText"] for result in query if "reviewText" in result]

    return review_texts


def create_wordcloud(texts):
    """
    Crea y muestra una nube de palabras a partir de una lista de textos de reviews.

    Parameters:
        texts (list): Lista de textos de las reviews.
    """
    combined_text = " ".join(texts)

    wordcloud = WordCloud(
        width=800,
        height=400,
        min_word_length=4,
        background_color="white",
        colormap="magma",
    ).generate(combined_text)

    # Visualizamos la nube de palabras
    plt.figure(figsize=(10, 5))
    plt.imshow(wordcloud, interpolation="bilinear")
    plt.axis("off")
    plt.show()


def seventh_query(database, collection_names):
    """
    Realiza una consulta a la base de datos y devuelve el recuento de revisiones por mes.

    Parameters:
        database (pymongo.database.Database): La base de datos MongoDB.
        collection_names (list): Lista de nombres de colecciones a consultar.

    Returns:
        dict: Diccionario con el recuento de revisiones por mes.
    """

    # Diccionario para almacenar el recuento de revisiones por mes
    review_counts_by_month = defaultdict(int)

    # Iterar sobre las colecciones proporcionadas
    for collection_name in collection_names:
        collection = database[collection_name]

        # Consulta de agregación para contar las revisiones por mes
        query = collection.aggregate(
            [
                {"$group": {"_id": {"$month": "$reviewTime"}, "count": {"$sum": 1}}},
                {"$sort": {"_id": 1}},
            ]
        )

        # Procesar los resultados de la consulta
        for result in query:
            month = result["_id"]
            count = result["count"]
            review_counts_by_month[month] += count

    return review_counts_by_month


def plot_reviews_month(reviews_counts_by_month, product_type="todos los productos"):
    """
    Grafica el número de revisiones por mes.

    Parameters:
        reviews_counts_by_month (dict): Diccionario con el recuento de revisiones por mes.
        product_type (str): Tipo de producto. Por defecto, "todos los productos".
    """

    # Desempaquetar las claves (meses) y los valores (recuentos de revisiones) del diccionario
    months, review_counts = zip(*reviews_counts_by_month.items())

    # Crear el gráfico de barras
    plt.bar(months, review_counts)

    # Añadir título y etiquetas de ejes al gráfico
    plt.title(f"Número de revisiones por mes - {product_type}")
    plt.xlabel("Mes")
    plt.ylabel("Número de revisiones")

    # Establecer los meses como etiquetas en el eje x
    plt.xticks(months)

    # Mostrar el gráfico
    plt.show()
