from pymongo import MongoClient
import pandas as pd
import matplotlib.pyplot as plt
import os

client = MongoClient("mongodb://mongo_user:mongo_pass@localhost:27017/?authSource=admin")
db = client["mamionmiam"]
os.makedirs("exports", exist_ok=True)

clients_raw = list(db["clients"].find({}, {"id": 1, "genre": 1, "_id": 0}))
client_genre = {c["id"]: c.get("genre", "?") for c in clients_raw}

produits_raw = list(db["produits"].find({}, {"SKU": 1, "Rayon": 1, "_id": 0}))
sku_to_rayon = {str(p["SKU"]): p.get("Rayon", "Inconnu") for p in produits_raw}

achats_raw = list(db["achats"].find({}, {"acheteur": 1, "detail": 1, "_id": 0}))

rows = []
for achat in achats_raw:
    genre = client_genre.get(achat.get("acheteur"), "?")
    for article in achat.get("detail", []):
        rows.append({
            "genre":       genre,
            "rayon":       sku_to_rayon.get(str(article.get("SKU")), "Inconnu"),
            "total_ligne": article.get("total", 0)
        })

df = pd.DataFrame(rows)
result = (df.groupby(["genre", "rayon"])
            .agg(nb_lignes=("total_ligne", "count"), depense=("total_ligne", "sum"))
            .reset_index()
            .sort_values(["genre", "depense"], ascending=[True, False]))
result["depense"] = result["depense"].round(2)

print("  Achats et dépenses par genre et par rayon")
print(result.to_string(index=False))

top_rayons = (df.groupby("rayon")["total_ligne"].sum()
                .sort_values(ascending=False)
                .head(10).index.tolist())

pivot = (result[result["rayon"].isin(top_rayons)]
         .pivot_table(index="rayon", columns="genre", values="depense", fill_value=0))
pivot["_total"] = pivot.sum(axis=1)
pivot = pivot.sort_values("_total", ascending=True).drop(columns="_total")

pivot.plot(kind="barh", figsize=(12, 7))
plt.title("Dépense par rayon et genre (top 10 rayons)")
plt.xlabel("Dépense (€)")
plt.tight_layout()
plt.savefig("exports/analyse_08_achats_genre_rayon.png", dpi=150)
plt.close()
print("exports/analyse_08_achats_genre_rayon.png")

client.close()
