from pymongo import MongoClient
import pandas as pd
import matplotlib.pyplot as plt
import os

client = MongoClient("mongodb://mongo_user:mongo_pass@localhost:27017/?authSource=admin")
db = client["mamionmiam"]
os.makedirs("exports", exist_ok=True)

clients_raw = list(db["clients"].find({}, {"id": 1, "genre": 1, "commune": 1, "_id": 0}))
client_info = {
    c["id"]: {"genre": c.get("genre", "?"), "commune": c.get("commune", "Inconnue")}
    for c in clients_raw
}

achats_raw = list(db["achats"].find({}, {"acheteur": 1, "total": 1, "_id": 0}))

rows = []
for achat in achats_raw:
    info = client_info.get(achat.get("acheteur"), {})
    rows.append({
        "commune": info.get("commune", "Inconnue"),
        "genre":   info.get("genre", "?"),
        "total":   achat.get("total", 0)
    })

df = pd.DataFrame(rows)
result = (df.groupby(["commune", "genre"])["total"]
            .sum()
            .reset_index(name="depense")
            .sort_values("depense", ascending=False))
result["depense"] = result["depense"].round(2)

print("  Dépense par commune et genre (top 20)")
print(result.head(20).to_string(index=False))

top10_communes = (df.groupby("commune")["total"].sum()
                    .sort_values(ascending=False)
                    .head(10).index.tolist())

pivot = (result[result["commune"].isin(top10_communes)]
         .pivot_table(index="commune", columns="genre", values="depense", fill_value=0))
pivot["_total"] = pivot.sum(axis=1)
pivot = pivot.sort_values("_total", ascending=True).drop(columns="_total")

pivot.plot(kind="barh", figsize=(12, 7))
plt.title("Dépense par commune et genre (top 10 communes)")
plt.xlabel("Dépense (€)")
plt.tight_layout()
plt.savefig("exports/analyse_10_depenses_commune_genre.png", dpi=150)
plt.close()
print("exports/analyse_10_depenses_commune_genre.png")

client.close()
