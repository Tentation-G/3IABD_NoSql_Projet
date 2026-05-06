from pymongo import MongoClient
from itertools import combinations
from collections import Counter
import pandas as pd
import matplotlib.pyplot as plt
from tabulate import tabulate
import os

client = MongoClient("mongodb://mongo_user:mongo_pass@localhost:27017/?authSource=admin")
db = client["mamionmiam"]
os.makedirs("exports", exist_ok=True)

produits_raw = list(db["produits"].find({}, {"SKU": 1, "Label": 1, "_id": 0}))
sku_to_label = {str(p["SKU"]): p.get("Label", str(p["SKU"])) for p in produits_raw}

achats_raw = list(db["achats"].find({}, {"detail": 1, "_id": 0}))

paires = Counter()
for achat in achats_raw:
    skus = list({str(a.get("SKU")) for a in achat.get("detail", [])})
    for sku1, sku2 in combinations(sorted(skus), 2):
        paires[(sku1, sku2)] += 1

rows = []
for (sku1, sku2), nb in paires.most_common(20):
    rows.append({
        "produit_1":  sku_to_label.get(sku1, sku1),
        "produit_2":  sku_to_label.get(sku2, sku2),
        "nb_tickets": nb
    })
df = pd.DataFrame(rows)

print("\nTop 20 paires de produits achetés ensemble")
print(tabulate(df, headers="keys", tablefmt="outline", showindex=False))

df_top = df.head(10).copy()
df_top["paire"] = df_top["produit_1"].str[:22] + "\n+ " + df_top["produit_2"].str[:22]

fig, ax = plt.subplots(figsize=(11, 7))
ax.barh(df_top["paire"][::-1], df_top["nb_tickets"][::-1])
ax.set_xlabel("Nombre de tickets communs")
ax.set_title("Top 10 paires de produits achetés ensemble")
plt.tight_layout()
plt.savefig("exports/analyse_04_market_basket.png")
plt.close()
print("exports/analyse_04_market_basket.png")

client.close()