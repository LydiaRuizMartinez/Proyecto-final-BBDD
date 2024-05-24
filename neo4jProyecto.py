"""
neo4jProyecto.py

Bases de Datos - IMAT
ICAI, Universidad Pontificia Comillas

Integrantes del grupo:
    - Carlos Martínez
    - Lydia Ruiz

Descripción:
En este fichero se resuelven todos los ejercicios de la aplicación Python junto con Neo4J.
"""

import random
import json
import os
from neo4j import GraphDatabase
from configuracion import folder_path
from collections import defaultdict
from datetime import datetime

from configuracion import (
    uri_neo4j,
    user_neo4j,
    contrasena_neo4j,
    top_n
)

def menu():
    
    print(""" ******** MENU ********
          
    1. Obtener similitudes entre usuarios.
    2. Obtener enlaces entre usuarios y artículos.
    3. Obtener algunos usuarios que han visto más de un determinado tipo de artículo.
    4. Artículos populares y artículos en común entre usuarios
    5. Salir.
          
************************
    """)

# 4.1 Obtener similitudes entre usuarios y mostrar los enlaces en Neo4J

def get_neo4j_session(uri, user, password):
    """
    Establece una conexión con una base de datos Neo4j y abre una sesión.
    """
    driver = GraphDatabase.driver(uri, auth=(user, password))
    return driver.session()

def read_json_data_from_folder(folder_path):
    """
    Lee archivos JSON desde un directorio especificado, asumiendo que cada archivo 
    contiene varias líneas, cada una representando un objeto JSON separado.
    """
    data = []
    for filename in os.listdir(folder_path):
        file_path = os.path.join(folder_path, filename)
        product_type = filename.replace("_5.json", "")
        with open(file_path, "r") as file:
            for line in file:
                item = json.loads(line)
                item["article_type"] = product_type
                data.append(item)
    return data

def get_top_users(data, top_n):
    """
    Identifica los usuarios con la mayor cantidad de productos únicos revisados.
    """
    user_reviews = {}
    for item in data:
        user = item["reviewerID"]
        product = item["asin"]
        if user not in user_reviews:
            user_reviews[user] = set()
        user_reviews[user].add(product)

    user_review_counts = {
        user: len(products) for user, products in user_reviews.items()
    }
    sorted_users = sorted(user_review_counts.items(), key=lambda x: x[1], reverse=True)[
        :top_n
    ]
    return {user: user_reviews[user] for user, _ in sorted_users}

def calculate_jaccard_similarity(top_users):
    """
    Calcula la similitud de Jaccard entre pares de usuarios basándose en los productos que han revisado.
    """
    similarities = {}
    for user1, items1 in top_users.items():
        for user2, items2 in top_users.items():
            if user1 != user2:
                intersection = len(items1.intersection(items2))
                union = len(items1.union(items2))
                similarity = intersection / union if union else 0
                if similarity > 0:
                    similarities[(user1, user2)] = similarity
    return similarities

def load_similarities_into_neo4j(session, similarities):
    """
    Carga las similitudes calculadas entre usuarios en una base de datos Neo4j.
    """
    session.run("MATCH (n) DETACH DELETE n")
    for (user1, user2), similarity in similarities.items():
        session.run(
            "MERGE (u1:User {id: $user1}) "
            "MERGE (u2:User {id: $user2}) "
            "MERGE (u1)-[:SIMILAR {similarity: $similarity}]->(u2)",
            user1=user1,
            user2=user2,
            similarity=similarity,
        )

def find_user_with_most_neighbors(uri, user, password):
    """
    Encuentra el usuario con el mayor número de conexiones ('vecinos') en la base de datos Neo4j.

    """
    driver = GraphDatabase.driver(uri, auth=(user, password))

    with driver.session() as session:
        result = session.run(
            "MATCH (u:User)-[:SIMILAR]-() "
            "RETURN u.id AS user, count(*) AS neighbors "
            "ORDER BY neighbors DESC "
            "LIMIT 1"
        )
        record = result.single()
        print(f"User with most neighbors: {record['user']} with {record['neighbors']} neighbors")


# 4.2 Obtener enlaces entre usuarios y artículos

def get_random_articles(data, article_type, n):
    """
    Selecciona una cantidad específica de artículos aleatorios de un tipo dado dentro de un conjunto de datos.
    """
    filtered_articles = [article for article in data if article['article_type'] == article_type]
    if len(filtered_articles) < n:
        print(f"Solo se encontraron {len(filtered_articles)} artículos de tipo {article_type}, seleccionando todos.")
        return filtered_articles
    else:
        return random.sample(filtered_articles, n)
    
def load_articles_and_reviews(session, data, selected_articles):
    """
    Cargar artículos seleccionados y sus reseñas correspondientes en una base de datos Neo4j.
    """
    session.run("MATCH (n) DETACH DELETE n")

    selected_article_ids = {article['asin'] for article in selected_articles}

    for article in selected_articles:
        article_id = article['asin'].replace("'", "\\'")
        session.run(
            f"""
            MERGE (a:Article {{id: '{article_id}'}})
            ON CREATE SET a.firstSeen = timestamp()
            """
        )

    for review in data:
        article_id = review['asin'].replace("'", "\\'")
        if article_id in selected_article_ids:
            user_id = review['reviewerID'].replace("'", "\\'")
            user_name = review.get('reviewerName', 'Unknown').replace("'", "\\'")
            review_text = review.get('reviewText', '').replace("'", "\\'")
            review_date = review.get('reviewTime', 'Unknown Date')
            review_score = review.get('overall', 0)
            session.run(
                f"""
                MERGE (u:User {{id: '{user_id}', name: '{user_name}'}})
                ON CREATE SET u.firstSeen = timestamp()
                MERGE (a:Article {{id: '{article_id}'}})
                MERGE (u)-[r:REVIEWED {{text: '{review_text}', score: {review_score}, date: '{review_date}'}}]->(a)
                """
            )

    print("Ha finalizado la carga en Neo4J")

# 4.3 Obtener algunos usuarios que han visto más de un determinado tipo de artículo

def get_user_article_types(data):
    """
    Recopila la cantidad de diferentes tipos de artículos que cada usuario ha revisado.
    """
    user_article_types = defaultdict(lambda: defaultdict(int))

    for item in data:
        user_id = item["reviewerID"]
        user_name = item.get("reviewerName", "Unknown")
        article_type = item["article_type"]
        user_article_types[(user_name, user_id)][article_type] += 1

    first_400_users = dict(sorted(user_article_types.items(), key=lambda x: x[0][0])[:400])

    users_with_multiple_types = {
        (user[1], user[0]): types for user, types in first_400_users.items() if len(types) > 1
    }

    return users_with_multiple_types

def load_users_and_article_types(session, sorted_users):
    """
    Cargar la relación entre usuarios y tipos de artículos que han revisado en Neo4j.
    """
    session.run("MATCH (n) DETACH DELETE n")

    for user_id, article_types in sorted_users.items():
        for article_type, count in article_types.items():
            session.run(
                """
                MERGE (u:User {id: $user_id})
                MERGE (t:ArticleType {type: $article_type})
                MERGE (u)-[r:REVIEWED]->(t)
                SET r.count = $count
                """,
                parameters={
                    "user_id": user_id,
                    "article_type": article_type,
                    "count": count,
                },
            )
    print("Ha finalizado la carga en Neo4J")


# 4.4 Artículos populares y artículos en común entre usuarios

def find_popular_articles_with_few_reviews(data, max_reviews=40, top_n=5):
    """
    Identificar artículos populares que tienen relativamente pocas reseñas.
    """
    article_review_counts = defaultdict(int)
    article_reviews = defaultdict(list)

    for item in data:
        article_id = item["asin"]
        article_review_counts[article_id] += 1
        article_reviews[article_id].append(item)

    filtered_articles = {
        k: v for k, v in article_review_counts.items() if v < max_reviews
    }

    popular_articles = sorted(
        filtered_articles, key=filtered_articles.get, reverse=True
    )[:top_n]

    selected_articles_reviews = {
        article_id: article_reviews[article_id] for article_id in popular_articles
    }

    return selected_articles_reviews

def load_articles_and_users(session, articles_reviews):
    """
    Cargar artículos y las reseñas correspondientes de los usuarios en Neo4j.
    """
    session.run("MATCH (n) DETACH DELETE n")
    for article_id, reviews in articles_reviews.items():
        for review in reviews:
            review_time = datetime.strptime(review["reviewTime"], "%m %d, %Y").strftime("%Y-%m-%d")
            
            session.run(
                """
                MERGE (a:Article {id: $articleId})
                MERGE (u:User {id: $userId})
                MERGE (u)-[r:REVIEWED]->(a)
                ON CREATE SET r.rating = $rating, r.reviewTime = $reviewTime
                """,
                parameters={
                    "articleId": article_id,
                    "userId": review["reviewerID"],
                    "rating": review["overall"],
                    "reviewTime": review_time,
                },
            )
    print("Ha finalizado la carga en Neo4J")

def calculate_and_load_user_connections(session):
    """
    Calcular y cargar conexiones entre usuarios basadas en la cantidad de artículos que ambos han revisado.
    """
    session.run(
        """
        MATCH (u1:User)-[:REVIEWED]->(a:Article)<-[:REVIEWED]-(u2:User)
        WITH u1, u2, COUNT(a) AS sharedArticles
        WHERE u1 <> u2 AND sharedArticles > 1
        MERGE (u1)-[r:SHARED_REVIEWS]->(u2)
        SET r.count = sharedArticles
    """
    )


if __name__ == "__main__":

    menu()

    response = input("Introduzca la opción que desee: ")

    session = get_neo4j_session(uri_neo4j, user_neo4j, contrasena_neo4j)
    data = read_json_data_from_folder(folder_path)

    # 4.1
    if response == "1":
        top_users = get_top_users(data, top_n)
        similarities = calculate_jaccard_similarity(top_users)
        load_similarities_into_neo4j(session, similarities)
        find_user_with_most_neighbors(uri_neo4j, user_neo4j, contrasena_neo4j)

    # 4.2
    elif response == "2":
        article_type = input("Ingrese el tipo de artículo (Video_Games, Digital_Music, Musical_Instruments o Toys_and_Games): ")
        n = int(input("Ingrese el número de artículos aleatorios que desea seleccionar: "))
        random_articles = get_random_articles(data, article_type, n)
        load_articles_and_reviews(session, data, random_articles)

    # 4.3
    elif response == "3":
        sorted_users = get_user_article_types(data)
        load_users_and_article_types(session, sorted_users)

    # 4.4
    elif response == "4":
        popular_articles_reviews = find_popular_articles_with_few_reviews(data)
        load_articles_and_users(session, popular_articles_reviews)
        calculate_and_load_user_connections(session)

    # Opción de salir
    elif response == "5":
        print("Ha salido del menú.")
        
    else:
        print("Introduzca una opción válida")
        menu()

    session.close()

