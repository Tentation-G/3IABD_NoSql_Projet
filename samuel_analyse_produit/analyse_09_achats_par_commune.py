from pymongo import MongoClient
import pandas as pd
import matplotlib.pyplot as plt
import os

client = MongoClient("mongodb://mongo_user:mongo_pass@localhost:27017/?authSource=admin")
db = client["mamionmiam"]
os.makedirs("exports", exist_ok=True)

clients_raw = list(db["clients"].find({}, {"id": 1, "commune": 1, "_id": 0}))
client_commune = {c["id"]: c.get("commune", "Inconnue") for c in clients_raw}

achats_raw = list(db["achats"].find({}, {"acheteur": 1, "total": 1, "_id": 0}))

rows = []
for achat in achats_raw:
    rows.append({
        "commune": client_commune.get(achat.get("acheteur"), "Inconnue"),
        "total":   achat.get("total", 0)
    })

df = pd.DataFrame(rows)
result = (df.groupby("commune")
            .agg(nb_achats=("total", "count"), depense_totale=("total", "sum"))
            .reset_index()
            .sort_values("depense_totale", ascending=False))
result["depense_totale"] = result["depense_totale"].round(2)

print("  Achats et dépenses par commune (top 20)")
print(result.head(20).to_string(index=False))

top15 = result.head(15)
fig, ax = plt.subplots(figsize=(12, 7))
ax.barh(top15["commune"][::-1], top15["depense_totale"][::-1])
ax.set_xlabel("Dépense totale (€)")
ax.set_title("Top 15 communes par dépense totale")
plt.tight_layout()
plt.savefig("exports/analyse_09_achats_par_commune.png", dpi=150)
plt.close()
print("exports/analyse_09_achats_par_commune.png")

client.close()
