"""
import_mongo.py
---------------
Charge les 6 fichiers JSON dans MongoDB.
Prérequis : MongoDB lancé via docker-compose up -d
Usage : python import_mongo.py
"""

import json
import os
from pymongo import MongoClient

# Connexion
client = MongoClient("mongodb://mongo_user:mongo_pass@localhost:27017/?authSource=admin")
db = client["mamionmiam"]

fichiers = {
    "shops":       "data/shops.json",
    "clients":     "data/clients.json",
    "parrainages": "data/parrainages.json",
    "entreprises": "data/entreprises.json",
    "achats":      "data/achats.json",
    "produits":    "data/produits.json",
}

for collection_name, filepath in fichiers.items():
    with open(filepath, encoding="utf-8") as f:
        data = json.load(f)

    # Vider la collection avant de recharger
    db[collection_name].drop()

    # Insérer
    db[collection_name].insert_many(data)
    print(f"<ok> {collection_name} : {len(data)} documents insérés")

# Index utiles pour les analyses
db["produits"].create_index("Rayon")
db["produits"].create_index("Categorie")
db["produits"].create_index("SKU")
db["achats"].create_index("acheteur")
db["clients"].create_index("id")

print("\n<ok> Index créés")
print("<ok> Import terminé — MongoDB est prêt")