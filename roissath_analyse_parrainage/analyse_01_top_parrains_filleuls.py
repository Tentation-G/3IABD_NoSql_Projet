from neo4j import GraphDatabase
from tabulate import tabulate
import os

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "_out")
os.makedirs(OUTPUT_DIR, exist_ok=True)

driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "neo4j_pass"))

with driver.session() as s:

    # Top 10 parrains
    top_parrains = s.run("""
        MATCH (parrain:Client)-[:PARRAINE]->(filleul:Client)
        WITH parrain, COUNT(filleul) AS nb_filleuls
        ORDER BY nb_filleuls DESC
        LIMIT 10
        RETURN parrain.id AS id, parrain.prenom AS prenom,
               parrain.nom AS nom, parrain.genre AS genre,
               nb_filleuls
    """).data()

    rows = [[i, p['prenom'], p['nom'], p['genre'], p['nb_filleuls']]
             for i, p in enumerate(top_parrains, 1)]
    print("\n" + tabulate(rows, headers=["Rang", "Prenom", "Nom", "Genre", "Filleuls"], tablefmt="pretty"))

    # Filleuls du top 1
    top1 = top_parrains[0]
    filleuls = s.run("""
        MATCH (parrain:Client {id: $id})-[:PARRAINE]->(filleul:Client)
        RETURN filleul.prenom AS prenom, filleul.nom AS nom,
               filleul.genre AS genre, filleul.commune AS commune
        ORDER BY filleul.nom
    """, id=top1["id"]).data()

    print(f"\nFilleuls de {top1['prenom']} {top1['nom']} ({len(filleuls)} filleuls) :")
    rows_f = [[f['prenom'], f['nom'], f['genre'], f['commune']] for f in filleuls]
    print(tabulate(rows_f, headers=["Prenom", "Nom", "Genre", "Commune"], tablefmt="pretty"))

driver.close()
