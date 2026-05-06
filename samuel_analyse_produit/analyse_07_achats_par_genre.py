from pymongo import MongoClient
import pandas as pd
import matplotlib.pyplot as plt
import os

client = MongoClient("mongodb://mongo_user:mongo_pass@localhost:27017/?authSource=admin")
db = client["mamionmiam"]
os.makedirs("exports", exist_ok=True)

clients_raw = list(db["clients"].find({}, {"id": 1, "genre": 1, "_id": 0}))
client_genre = {c["id"]: c.get("genre", "?") for c in clients_raw}

achats_raw = list(db["achats"].find({}, {"acheteur": 1, "total": 1, "_id": 0}))

rows = []
for achat in achats_raw:
    rows.append({
        "genre": client_genre.get(achat.get("acheteur"), "?"),
        "total": achat.get("total", 0)
    })

df = pd.DataFrame(rows)
result = (df.groupby("genre")
            .agg(nb_achats=("total", "count"), depense_totale=("total", "sum"))
            .reset_index()
            .sort_values("depense_totale", ascending=False))
result["depense_totale"] = result["depense_totale"].round(2)

print("Achats et dépenses par genre")
print(result.to_string(index=False))

fig, axes = plt.subplots(1, 2, figsize=(10, 5))

axes[0].bar(result["genre"], result["nb_achats"])
axes[0].set_title("Nombre d'achats par genre")
axes[0].set_ylabel("Nb achats")
for i, v in enumerate(result["nb_achats"]):
    axes[0].text(i, v + 5, str(v), ha="center", fontsize=9)

axes[1].bar(result["genre"], result["depense_totale"])
axes[1].set_title("Dépense totale par genre (€)")
axes[1].set_ylabel("€")
for i, v in enumerate(result["depense_totale"]):
    axes[1].text(i, v + 50, f"{v:.0f}€", ha="center", fontsize=9)

plt.suptitle("Comportement d'achat par genre", fontsize=13, fontweight="bold")
plt.tight_layout()
plt.savefig("exports/analyse_07_achats_par_genre.png", dpi=150)
plt.close()
print("exports/analyse_07_achats_par_genre.png")

client.close()
