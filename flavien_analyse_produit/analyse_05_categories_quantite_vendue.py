"""
analyse_05_categories_quantite_vendue.py
-----------------------------------------
Catégories avec le plus de quantité vendue.
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
        "total_qte": {"$sum": "$detail.qte"}
    }},
    {"$sort": {"total_qte": -1}},
    {"$limit": 10}
]

resultats = list(db["achats"].aggregate(pipeline))

# Affichage tableau
print("\nTop 10 catégories avec le plus de quantité vendue\n")
print(tabulate(
    [(r["_id"], r["total_qte"]) for r in resultats],
    headers=["Catégorie", "Quantité vendue"],
    tablefmt="pretty"
))

# Graphique
categories = [r["_id"] for r in resultats]
valeurs = [r["total_qte"] for r in resultats]

plt.figure(figsize=(10, 6))
plt.barh(categories[::-1], valeurs[::-1])
plt.xlabel("Quantité vendue")
plt.title("Top 10 catégories avec le plus de quantité vendue")
plt.tight_layout()
plt.savefig("_out/analyse_05_categories_quantite_vendue.png")
plt.show()
print("\n<ok> Graph save")