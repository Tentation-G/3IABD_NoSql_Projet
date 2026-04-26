import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from pymongo import MongoClient
from tabulate import tabulate
from datetime import date
import os

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "_out")
os.makedirs(OUTPUT_DIR, exist_ok=True)

mongo = MongoClient("mongodb://mongo_user:mongo_pass@localhost:27017/?authSource=admin")
db = mongo["mamionmiam"]

# IDs des parrains
parrain_ids = set(db.parrainages.distinct("idParrain"))

parrains = list(db.clients.find(
    {"id": {"$in": list(parrain_ids)}},
    {"_id": 0, "naissance": 1}
))

today = date.today()

def tranche(naissance_str):
    if not naissance_str:
        return None
    try:
        naissance = date.fromisoformat(naissance_str[:10])
        age = today.year - naissance.year - (
            (today.month, today.day) < (naissance.month, naissance.day)
        )
    except ValueError:
        return None
    if   18 <= age <= 27: return "18-27"
    elif 28 <= age <= 37: return "28-37"
    elif 38 <= age <= 47: return "38-47"
    elif 48 <= age <= 57: return "48-57"
    elif 58 <= age <= 67: return "58-67"
    elif age >= 68:        return "68+"
    return None

tranches = ["18-27", "28-37", "38-47", "48-57", "58-67", "68+"]
counts = {t: 0 for t in tranches}

for p in parrains:
    t = tranche(p.get("naissance"))
    if t:
        counts[t] += 1

mongo.close()

rows = [[t, counts[t]] for t in tranches]
print("\n" + tabulate(rows, headers=["Tranche d'age", "Nb parrains"], tablefmt="pretty"))

fig, ax = plt.subplots(figsize=(9, 5))
bars = ax.bar(tranches, [counts[t] for t in tranches], color="#4a90d9", edgecolor="white")
for bar in bars:
    h = bar.get_height()
    ax.text(bar.get_x() + bar.get_width() / 2, h + 1, str(int(h)),
            ha="center", va="bottom", fontsize=10)
ax.set_title("Nombre de parrains par tranche d'age", fontsize=13, fontweight="bold")
ax.set_xlabel("Tranche d'age")
ax.set_ylabel("Nombre de parrains")
ax.set_ylim(0, max(counts.values()) * 1.15)
plt.tight_layout()
out = os.path.join(OUTPUT_DIR, "analyse_03_tranches_age.png")
plt.savefig(out, dpi=150, bbox_inches="tight")
print(f"\nGraphique sauvegarde : {out}")
