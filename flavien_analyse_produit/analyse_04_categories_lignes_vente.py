"""
analyse_04_categories_lignes_vente.py
---------------------------------------
Catégories avec le plus de lignes de vente.
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
    {"$unwind": "$detail"},
    {"$lookup": {
        "from": "produits",
        "localField": "detail.SKU",
        "foreignField": "SKU",
        "as": "produit_info"
    }},
    {"$unwind": "$produit_info"},
    {"$group": {
        "_id": "$produit_info.Categorie",
        "nb_lignes": {"$sum": 1}
    }},
    {"$sort": {"nb_lignes": -1}},
    {"$limit": 10}
]

resultats = list(db["achats"].aggregate(pipeline))

# Affichage tableau
print("\nTop 10 catégories avec le plus de lignes de vente\n")
print(tabulate(
    [(r["_id"], r["nb_lignes"]) for r in resultats],
    headers=["Catégorie", "Nb lignes de vente"],
    tablefmt="pretty"
))

# Graphique
categories = [r["_id"] for r in resultats]
valeurs = [r["nb_lignes"] for r in resultats]

plt.figure(figsize=(10, 6))
plt.barh(categories[::-1], valeurs[::-1])
plt.xlabel("Nombre de lignes de vente")
plt.title("Top 10 catégories avec le plus de lignes de vente")
plt.tight_layout()
plt.savefig("_out/analyse_04_categories_lignes_vente.png")
plt.show()
print("\n<ok> Graph save")