from neo4j import GraphDatabase
from tabulate import tabulate
import os

driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "neo4j_pass"))

with driver.session() as s:

    # Longueur max de chaine (depth-first, acyclique)
    result = s.run("""
        MATCH path = (debut:Client)-[:PARRAINE*]->(fin:Client)
        WHERE NOT ()-[:PARRAINE]->(debut)
          AND NOT (fin)-[:PARRAINE]->()
        WITH path, length(path) AS longueur
        ORDER BY longueur DESC
        LIMIT 1
        RETURN [n IN nodes(path) | n.prenom + ' ' + n.nom] AS chaine,
               longueur
    """).single()

    if result:
        print(f"\nChaine de parrainage la plus longue : {result['longueur']} niveaux\n")
        chaine = result["chaine"]
        for i, personne in enumerate(chaine):
            indent = "  " * i
            fleche = "" if i == 0 else "-> "
            print(f"{indent}{fleche}{personne}")
    else:
        print("Aucune chaine de parrainage trouvee.")

    # Statistiques de profondeur
    stats = s.run("""
        MATCH path = (debut:Client)-[:PARRAINE*]->(fin:Client)
        WHERE NOT ()-[:PARRAINE]->(debut)
          AND NOT (fin)-[:PARRAINE]->()
        RETURN length(path) AS longueur, COUNT(*) AS nb_chaines
        ORDER BY longueur DESC
        LIMIT 10
    """).data()

    if stats:
        rows = [[r['longueur'], r['nb_chaines']] for r in stats]
        print("\nDistribution des longueurs de chaines :")
        print(tabulate(rows, headers=["Longueur", "Nb chaines"], tablefmt="pretty"))

driver.close()
