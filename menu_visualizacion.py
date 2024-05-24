"""
menu_visualizacion.py

Bases de Datos - IMAT
ICAI, Universidad Pontificia Comillas

Integrantes del grupo:
    - Carlos Martínez
    - Lydia Ruiz

Descripción:
Programa para obtener un menú con diferentes plots de visualización de diferentes datos.
"""

from configuracion import CONNECTION_STRING, database_name_MongoDB
import tkinter as tk
from tkinter import ttk
from PIL import Image, ImageTk
from pymongo import MongoClient
import queries as q

# Definición de las opciones de categoría disponibles para realizar consultas en la base de datos.
collection_options = [
    "Digital_Music",
    "Musical_Instruments",
    "Toys_and_Games",
    "Video_Games",
]

# Esta función cierra la ventana principal de la aplicación, terminando el programa.
def close_window():
    root.destroy()


# Esta función gestiona la apertura de gráficos basados en la categoría y el tipo de consulta seleccionados por el usuario.
# Se conecta a MongoDB usando la cadena de conexión, selecciona la base de datos y realiza consultas específicas.
def open_chart(category, query):
    # Establecimiento de la conexión con la base de datos MongoDB.
    client = MongoClient(CONNECTION_STRING)
    database = client[database_name_MongoDB]

    # "user_option" representa la categoría seleccionada por el usuario.
    user_option = category
    # Dependiendo del tipo de consulta (`query`), se ejecuta una función diferente de "queries".
    # Cada consulta obtiene datos específicos y luego llama a otra función para graficar esos datos.
    if query == 1:
        if user_option in collection_options:
            # Consultar y graficar las revisiones para la opción del usuario
            reviews_years = q.first_query(database, [user_option])
            q.plot_reviews_year(reviews_years, user_option)
        else:
            # Consultar y graficar las revisiones para todas las opciones de colección
            reviews_years = q.first_query(database, collection_options)

            # Graficar los resultados
            q.plot_reviews_year(reviews_years)

    elif query == 2:
        if user_option in collection_options:
            # Consultar y graficar las revisiones para la opción del usuario
            reviews_asins = q.second_query(database, [user_option])
            q.plot_reviews_asin(reviews_asins, user_option)
        else:
            # Consultar y graficar las revisiones para todas las opciones de colección
            reviews_asins = q.second_query(database, collection_options)

            # Ordenar los resultados por recuento de revisiones de manera descendente
            reviews_asins = dict(
                sorted(reviews_asins.items(), key=lambda kv: kv[1], reverse=True)
            )

            # Graficar los resultados
            q.plot_reviews_asin(reviews_asins)
    elif query == 3:
        q.third_query(database, collection_options, user_option)
    elif query == 4:
        if user_option in collection_options:
            time_stamps = q.fourth_query(database, [user_option])
            q.plot_reviews_evolution(time_stamps, user_option)
        else:
            time_stamps = q.fourth_query(database, collection_options)
            q.plot_reviews_evolution(time_stamps)
    elif query == 6:
        review_texts = q.sixth_query(database, user_option)
        q.create_wordcloud(review_texts)
    elif query == 7:
        if user_option in collection_options:
            reviews_month = q.seventh_query(database, [user_option])
            q.plot_reviews_month(reviews_month, user_option)
        else:
            reviews_month = q.seventh_query(database, collection_options)
            q.plot_reviews_month(reviews_month)


# Función para abrir un gráfico que no requiere la selección de una categoría específica por el usuario.
def open_chart_no_opt():
    # Similar a "open_chart", pero específicamente para una consulta que grafica el histograma de reviews por usuario.
    client = MongoClient(CONNECTION_STRING)
    database = client[database_name_MongoDB]

    reviews_by_user = q.fifth_query(database, collection_options)
    # Graficar el histograma del número de revisiones por usuario
    q.plot_reviews_user(reviews_by_user)


# Inicio de la interfaz gráfica de usuario usando Tkinter.
root = tk.Tk()
root.title("Reviews de Amazon")

# Creación y configuración de widgets (elementos de la interfaz) como etiquetas, botones, menús.
# Estos widgets permiten al usuario interactuar con la aplicación, seleccionando categorías y tipos de consulta.
author_label = ttk.Label(
    root,
    text="Carlos Martínez Cuenca | Lydia Ruiz Martínez",
    font=("Times New Roman", 16),
    anchor="w",
)
author_label.pack(side=tk.TOP, fill=tk.X, padx=10)

title_label = ttk.Label(
    root, text="REVIEWS DE AMAZON", font=("Times New Roman", 18), foreground="black"
)
title_label.pack(side=tk.TOP, pady=20)

image_path = "fondo.png"
image = Image.open(image_path)
photo = ImageTk.PhotoImage(image)
image_label = tk.Label(root, image=photo)
image_label.pack(side=tk.TOP, pady=10)

# Se configuran estilos para los botones, se cargan imágenes y se definen las acciones al seleccionar opciones del menú.
style = ttk.Style()
style.configure(
    "TButton",
    font=("Times New Roman", 12),
    background="sky blue",
    width=60,
    height=2,
    padding=[20, 10],
)
style.configure(
    "Exit.TButton", font=("Times New Roman", 12), background="pink", foreground="salmon"
)

categories = [
    "Digital_Music",
    "Musical_Instruments",
    "Toys_and_Games",
    "Video_Games",
    "Everything",
]

review_button = ttk.Menubutton(
    root, text="Evolución de reviews por años", style="TButton"
)
review_menu = tk.Menu(review_button, tearoff=0)
review_button["menu"] = review_menu
for category in categories:
    review_menu.add_command(
        label=category, command=lambda cat=category: open_chart(cat, query=1)
    )
review_button.pack(side=tk.TOP, pady=10)

popularity_button = ttk.Menubutton(
    root, text="Evolución de la popularidad de los artículos", style="TButton"
)
popularity_menu = tk.Menu(popularity_button, tearoff=0)
popularity_button["menu"] = popularity_menu
for category in categories:
    popularity_menu.add_command(
        label=category, command=lambda cat=category: open_chart(cat, query=2)
    )
popularity_button.pack(side=tk.TOP, pady=10)

mark_button = ttk.Menubutton(root, text="Histograma por nota", style="TButton")
mark_menu = tk.Menu(mark_button, tearoff=0)
mark_button["menu"] = mark_menu
for category in categories:
    mark_menu.add_command(
        label=category, command=lambda cat=category: open_chart(cat, query=3)
    )
mark_button.pack(side=tk.TOP, pady=10)

reviews_time_button = ttk.Menubutton(
    root, text="Evolución de las reviews por año", style="TButton"
)
reviews_time_menu = tk.Menu(reviews_time_button, tearoff=0)
reviews_time_button["menu"] = reviews_time_menu
for category in categories:
    reviews_time_menu.add_command(
        label=category, command=lambda cat=category: open_chart(cat, query=4)
    )
reviews_time_button.pack(side=tk.TOP, pady=10)

reviews_user_button = ttk.Button(
    root,
    text="Histograma de reviews por usuario",
    command=lambda: open_chart_no_opt(),
    style="TButton",
)
reviews_user_button.pack(side=tk.TOP, pady=10)

categories_cloud = [
    "Digital_Music",
    "Musical_Instruments",
    "Toys_and_Games",
    "Video_Games",
]
word_cloud_button = ttk.Menubutton(
    root, text="Nube de palabras en función de la categoría", style="TButton"
)
word_cloud_menu = tk.Menu(word_cloud_button, tearoff=0)
word_cloud_button["menu"] = word_cloud_menu
for category in categories_cloud:
    word_cloud_menu.add_command(
        label=category, command=lambda cat=category: open_chart(cat, query=6)
    )
word_cloud_button.pack(side=tk.TOP, pady=10)

reviews_month_button = ttk.Menubutton(
    root, text="Evolución de las reviews por mes", style="TButton"
)
reviews_month_menu = tk.Menu(reviews_month_button, tearoff=0)
reviews_month_button["menu"] = reviews_month_menu
for category in categories:
    reviews_month_menu.add_command(
        label=category, command=lambda cat=category: open_chart(cat, query=7)
    )
reviews_month_button.pack(side=tk.TOP, pady=10)

exit_button = ttk.Button(root, text="Salir", command=close_window, style="Exit.TButton")
exit_button.pack(side=tk.TOP, pady=10)

# Configuración para que la ventana se abra en pantalla completa y el bucle principal que mantiene abierta la interfaz.
root.attributes("-fullscreen", True)

if __name__ == "__main__":
    root.mainloop()
