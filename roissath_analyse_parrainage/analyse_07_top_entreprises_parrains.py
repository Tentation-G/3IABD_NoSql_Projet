import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from pymongo import MongoClient
import os

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "_out")
os.makedirs(OUTPUT_DIR, exist_ok=True)

mongo = MongoClient("mongodb://mongo_user:mongo_pass@localhost:27017/?authSource=admin")
db = mongo["mamionmiam"]

# IDs des parrains
parrain_ids = list(db.parrainages.distinct("idParrain"))

# Clients-parrains avec leur entreprise
pipeline = [
    {"$match": {
        "id": {"$in": parrain_ids},
        "entreprise.siret": {"$exists": True, "$ne": None}
    }},
    {"$group": {
        "_id": "$entreprise.siret",
        "nom": {"$first": "$entreprise.nom"},
        "nb_parrains": {"$sum": 1}
    }},
    {"$sort": {"nb_parrains": -1}},
    {"$limit": 10}
]

top = list(db.clients.aggregate(pipeline))
mongo.close()

print("\nTop 10 entreprises dont les employes sont le plus parrains :")
print(f"  {'Rang':<5} {'Entreprise':<40} {'Parrains'}")
print("  " + "-" * 55)
for i, e in enumerate(top, 1):
    nom = (e.get("nom") or "Inconnue")[:38]
    print(f"  {i:<5} {nom:<40} {e['nb_parrains']}")

noms = [(e.get("nom") or e["_id"])[:32] for e in top]
vals = [e["nb_parrains"] for e in top]

fig, ax = plt.subplots(figsize=(10, 6))
colors = ["#e07b8a" if i < 3 else "#f0b4bb" for i in range(len(noms))]
bars = ax.bar(range(len(noms)), vals, color=colors, edgecolor="white")
for bar in bars:
    h = bar.get_height()
    ax.text(bar.get_x() + bar.get_width() / 2, h + 0.2, str(int(h)),
            ha="center", va="bottom", fontsize=9)
ax.set_xticks(range(len(noms)))
ax.set_xticklabels(noms, rotation=35, ha="right", fontsize=8)
ax.set_title("Top 10 entreprises — employes les plus actifs en parrainage", fontsize=12, fontweight="bold")
ax.set_ylabel("Nombre de parrainages generes")
ax.set_ylim(0, max(vals) * 1.15)
plt.tight_layout()
out = os.path.join(OUTPUT_DIR, "analyse_07_top_entreprises_parrains.png")
plt.savefig(out, dpi=150, bbox_inches="tight")
print(f"\nGraphique sauvegarde : {out}")
