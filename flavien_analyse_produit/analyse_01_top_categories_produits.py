"""
analyse_01_top_categories_produits.py
--------------------------------------
Top 10 catégories avec le plus de produits.
Prérequis : MongoDB chargé via import_mongo.py
"""

from pymongo import MongoClient
import matplotlib.pyplot as plt
from tabulate import tabulate

# Connexion
client = MongoClient("mongodb://mongo_user:mongo_pass@localhost:27017/?authSource=admin")
db = client["mamionmiam"]

# Requête
pipeline = [
    {"$group": {"_id": "$Categorie", "nb_produits": {"$sum": 1}}},
    {"$sort": {"nb_produits": -1}},
    {"$limit": 10}
]

resultats = list(db["produits"].aggregate(pipeline))

# Affichage tableau
print("\nTop 10 catégories avec le plus de produits\n")
print(tabulate(
    [(r["_id"], r["nb_produits"]) for r in resultats],
    headers=["Catégorie", "Nb produits"],
    tablefmt="pretty"
))

# Graphique
categories = [r["_id"] for r in resultats]
valeurs = [r["nb_produits"] for r in resultats]

plt.figure(figsize=(10, 6))
plt.barh(categories[::-1], valeurs[::-1])
plt.xlabel("Nombre de produits")
plt.title("Top 10 catégories avec le plus de produits")
plt.tight_layout()
plt.savefig("_out/analyse_01_top_categories_produits.png")
plt.show()
print("\n<ok> Graph save")