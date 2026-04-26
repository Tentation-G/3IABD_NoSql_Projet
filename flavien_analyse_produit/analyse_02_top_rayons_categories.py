"""
analyse_02_top_rayons_categories.py
-------------------------------------
Top 10 rayons avec le plus de catégories.
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
    {"$group": {"_id": {"rayon": "$Rayon", "categorie": "$Categorie"}}},
    {"$group": {"_id": "$_id.rayon", "nb_categories": {"$sum": 1}}},
    {"$sort": {"nb_categories": -1}},
    {"$limit": 10}
]

resultats = list(db["produits"].aggregate(pipeline))

# Affichage tableau
print("\nTop 10 rayons avec le plus de catégories\n")
print(tabulate(
    [(r["_id"], r["nb_categories"]) for r in resultats],
    headers=["Rayon", "Nb catégories"],
    tablefmt="pretty"
))

# Graphique
rayons = [r["_id"] for r in resultats]
valeurs = [r["nb_categories"] for r in resultats]

plt.figure(figsize=(10, 6))
plt.barh(rayons[::-1], valeurs[::-1])
plt.xlabel("Nombre de catégories")
plt.title("Top 10 rayons avec le plus de catégories")
plt.tight_layout()
plt.savefig("_out/analyse_02_top_rayons_categories.png")
plt.show()
print("\n<ok> Graph save")