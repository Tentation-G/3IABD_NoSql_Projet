import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from pymongo import MongoClient
from math import radians, sin, cos, sqrt, atan2
import os

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "_out")
os.makedirs(OUTPUT_DIR, exist_ok=True)

RAYON_KM = 4.0

def haversine(lat1, lng1, lat2, lng2):
    R = 6371.0
    dlat = radians(lat2 - lat1)
    dlng = radians(lng2 - lng1)
    a = sin(dlat / 2)**2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlng / 2)**2
    return R * 2 * atan2(sqrt(a), sqrt(1 - a))

mongo = MongoClient("mongodb://mongo_user:mongo_pass@localhost:27017/?authSource=admin")
db = mongo["mamionmiam"]

shops = list(db.shops.find({}, {"_id": 0}))
clients = list(db.clients.find(
    {"coords.lat": {"$exists": True}, "coords.lng": {"$exists": True}},
    {"_id": 0, "coords": 1}
))

mongo.close()

print(f"\n{len(shops)} magasins  |  {len(clients)} clients avec coordonnees")
print(f"Rayon de recherche : {RAYON_KM} km\n")

results = []
for shop in shops:
    slat = float(shop.get("lat", 0))
    slng = float(shop.get("lng", 0))
    nb = sum(
        1 for c in clients
        if haversine(slat, slng, float(c["coords"]["lat"]), float(c["coords"]["lng"])) <= RAYON_KM
    )
    results.append({"nom": shop.get("name", shop.get("id", "?")), "nb": nb})
    print(f"  {shop.get('name', shop.get('id', '?')):<35} -> {nb} abonnes dans {RAYON_KM} km")

# Graphique
noms = [r["nom"] for r in results]
vals = [r["nb"] for r in results]

fig, ax = plt.subplots(figsize=(10, 5))
bars = ax.bar(noms, vals, color="#5bba8a", edgecolor="white")
for bar in bars:
    h = bar.get_height()
    ax.text(bar.get_x() + bar.get_width() / 2, h + 1, str(int(h)),
            ha="center", va="bottom", fontsize=10)
ax.set_title(f"Abonnes habitant dans un rayon de {RAYON_KM} km par magasin", fontsize=13, fontweight="bold")
ax.set_ylabel("Nombre d'abonnes")
ax.set_ylim(0, max(vals) * 1.15 if vals else 10)
ax.set_xticks(range(len(noms)))
ax.set_xticklabels(noms, rotation=20, ha="right")
plt.tight_layout()
out = os.path.join(OUTPUT_DIR, "analyse_08_abonnes_rayon.png")
plt.savefig(out, dpi=150, bbox_inches="tight")
print(f"\nGraphique sauvegarde : {out}")
