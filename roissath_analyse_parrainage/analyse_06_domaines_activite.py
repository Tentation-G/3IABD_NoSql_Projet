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
    {"$match": {"domain_label": {"$exists": True, "$ne": None}}},
    {"$group": {"_id": "$domain_label", "nb": {"$sum": 1}}},
    {"$sort": {"nb": -1}},
    {"$limit": 12}
]

data = list(db.entreprises.aggregate(pipeline))
mongo.close()

print("\nDomaines d'activite — nombre d'entreprises :")
print(f"  {'Domaine':<50} {'Entreprises'}")
print("  " + "-" * 62)
for d in data:
    print(f"  {str(d['_id'])[:48]:<50} {d['nb']}")

labels = [str(d["_id"])[:35] for d in data]
vals = [d["nb"] for d in data]

cmap = plt.get_cmap("tab20")
colors = [cmap(i / len(labels)) for i in range(len(labels))]

fig, ax = plt.subplots(figsize=(10, 6))
bars = ax.barh(labels[::-1], vals[::-1], color=colors[::-1], edgecolor="white")
for bar in bars:
    w = bar.get_width()
    ax.text(w + 0.1, bar.get_y() + bar.get_height() / 2,
            str(int(w)), va="center", fontsize=9)
ax.set_title("Domaines d'activite — concentration des entreprises", fontsize=13, fontweight="bold")
ax.set_xlabel("Nombre d'entreprises")
ax.set_xlim(0, max(vals) * 1.15)
plt.tight_layout()
out = os.path.join(OUTPUT_DIR, "analyse_06_domaines_activite.png")
plt.savefig(out, dpi=150, bbox_inches="tight")
print(f"\nGraphique sauvegarde : {out}")
