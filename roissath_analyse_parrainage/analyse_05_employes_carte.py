import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from pymongo import MongoClient
import os

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "_out")
os.makedirs(OUTPUT_DIR, exist_ok=True)

mongo = MongoClient("mongodb://mongo_user:mongo_pass@localhost:27017/?authSource=admin")
db = mongo["mamionmiam"]

pipeline = [
    {"$match": {"entreprise.siret": {"$exists": True, "$ne": None}}},
    {"$group": {
        "_id": "$entreprise.siret",
        "nom": {"$first": "$entreprise.nom"},
        "nb_employes": {"$sum": 1}
    }},
    {"$sort": {"nb_employes": -1}},
    {"$limit": 15}
]

top = list(db.clients.aggregate(pipeline))
mongo.close()

print(f"\nTop 15 entreprises par nombre d'employes-clients :")
print(f"  {'Rang':<5} {'Entreprise':<40} {'Employes'}")
print("  " + "-" * 55)
for i, e in enumerate(top, 1):
    nom = (e.get("nom") or "Inconnue")[:38]
    print(f"  {i:<5} {nom:<40} {e['nb_employes']}")

noms = [(e.get("nom") or e["_id"])[:30] for e in top]
vals = [e["nb_employes"] for e in top]

fig, ax = plt.subplots(figsize=(11, 7))
colors = ["#4a90d9" if i < 3 else "#9ec8f0" for i in range(len(noms))]
bars = ax.barh(noms[::-1], vals[::-1], color=colors[::-1], edgecolor="white")
for bar in bars:
    w = bar.get_width()
    ax.text(w + 0.3, bar.get_y() + bar.get_height() / 2,
            str(int(w)), va="center", fontsize=9)
ax.set_title("Top 15 entreprises — nombre d'employes clients", fontsize=13, fontweight="bold")
ax.set_xlabel("Nombre d'employes-clients")
ax.set_xlim(0, max(vals) * 1.12)
plt.tight_layout()
out = os.path.join(OUTPUT_DIR, "analyse_05_employes_carte.png")
plt.savefig(out, dpi=150, bbox_inches="tight")
print(f"\nGraphique sauvegarde : {out}")
