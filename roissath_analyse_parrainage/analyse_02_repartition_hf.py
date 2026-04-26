import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from neo4j import GraphDatabase
import os

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "_out")
os.makedirs(OUTPUT_DIR, exist_ok=True)

driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "neo4j_pass"))

with driver.session() as s:

    parrains = s.run("""
        MATCH (p:Client)-[:PARRAINE]->(:Client)
        WITH DISTINCT p
        RETURN p.genre AS genre, COUNT(*) AS nb
    """).data()

    filleuls = s.run("""
        MATCH (:Client)-[:PARRAINE]->(f:Client)
        WITH DISTINCT f
        RETURN f.genre AS genre, COUNT(*) AS nb
    """).data()

driver.close()

def to_hf(rows):
    d = {r["genre"]: r["nb"] for r in rows}
    return d.get("H", 0), d.get("F", 0)

ph, pf = to_hf(parrains)
fh, ff = to_hf(filleuls)

fig, axes = plt.subplots(1, 2, figsize=(10, 5))
fig.suptitle("Repartition H/F — Parrains vs Filleuls", fontsize=14, fontweight="bold")

for ax, vals, titre in [
    (axes[0], [ph, pf], f"Parrains ({ph+pf})"),
    (axes[1], [fh, ff], f"Filleuls ({fh+ff})"),
]:
    wedges, texts, autotexts = ax.pie(
        vals,
        labels=["Hommes", "Femmes"],
        autopct="%1.1f%%",
        colors=["#4a90d9", "#e07b8a"],
        startangle=90,
    )
    ax.set_title(titre)

plt.tight_layout()
out = os.path.join(OUTPUT_DIR, "analyse_02_repartition_hf.png")
plt.savefig(out, dpi=150, bbox_inches="tight")
print(f"Graphique sauvegarde : {out}")

print(f"\nParrains  -> H:{ph}  F:{pf}")
print(f"Filleuls  -> H:{fh}  F:{ff}")
