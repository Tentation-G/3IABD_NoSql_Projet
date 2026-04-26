"""
import_neo4j.py
---------------
Importe les données depuis MongoDB vers Neo4j pour la Personne 3.

Nœuds créés  : (:Client), (:Shop), (:Entreprise)
Relations     : (:Client)-[:PARRAINE]->(:Client)
                (:Client)-[:TRAVAILLE_DANS]->(:Entreprise)
                (:Client)-[:CARTE_FIDELITE {shop_id}]->(:Shop)

Prérequis : docker-compose up -d  +  import_mongo.py exécuté
Usage     : python import_neo4j.py
"""

import time
from pymongo import MongoClient
from neo4j import GraphDatabase

#  Connexions 
mongo = MongoClient("mongodb://mongo_user:mongo_pass@localhost:27017/?authSource=admin")
db    = mongo["mamionmiam"]

NEO4J_URI  = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASS = "neo4j_pass"

print(" Connexion à Neo4j")
driver = None
for attempt in range(12):
    try:
        driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))
        driver.verify_connectivity()
        print(" Connecté à Neo4j")
        break
    except Exception as e:
        print(f"   Tentative {attempt+1}/12  {e}")
        time.sleep(5)
if driver is None:
    raise RuntimeError("Impossible de se connecter à Neo4j.")

#  Import 
with driver.session() as s:

    # Nettoyage
    s.run("MATCH (n) DETACH DELETE n")
    print("  Graphe nettoyé")

    # --- Nœuds Client ---------------------------------------------------------
    clients = list(db.clients.find({}))
    for c in clients:
        naissance = c.get("naissance", "")
        age = None
        if naissance:
            try:
                from datetime import date
                y = int(naissance[:4])
                age = date.today().year - y
            except Exception:
                pass
        s.run(
            """
            MERGE (cl:Client {id: $id})
            SET cl.nom      = $nom,
                cl.prenom   = $prenom,
                cl.genre    = $genre,
                cl.commune  = $commune,
                cl.insee    = $insee,
                cl.lat      = $lat,
                cl.lng      = $lng,
                cl.age      = $age,
                cl.naissance = $naissance
            """,
            id=c["id"],
            nom=c.get("nom", ""),
            prenom=c.get("prenom", ""),
            genre=c.get("genre", ""),
            commune=c.get("commune", ""),
            insee=c.get("insee", ""),
            lat=c.get("coords", {}).get("lat"),
            lng=c.get("coords", {}).get("lng"),
            age=age,
            naissance=naissance,
        )
    print(f" {len(clients)} nœuds Client")

    # --- Nœuds Entreprise -----------------------------------------------------
    entreprises = list(db.entreprises.find({}))
    for e in entreprises:
        s.run(
            """
            MERGE (en:Entreprise {siret: $siret})
            SET en.nom            = $nom,
                en.ville          = $ville,
                en.domain_label   = $domain_label,
                en.domain_code    = $domain_code,
                en.naf_label      = $naf_label,
                en.lat            = $lat,
                en.lng            = $lng
            """,
            siret=e["siret"],
            nom=e.get("nom", ""),
            ville=e.get("ville", ""),
            domain_label=e.get("domain_label", ""),
            domain_code=e.get("domain_code", ""),
            naf_label=e.get("naf_label", ""),
            lat=e.get("coords", {}).get("lat"),
            lng=e.get("coords", {}).get("lng"),
        )
    print(f" {len(entreprises)} nœuds Entreprise")

    # --- Nœuds Shop -----------------------------------------------------------
    shops = list(db.shops.find({}))
    for sh in shops:
        s.run(
            """
            MERGE (sh:Shop {id: $id})
            SET sh.name    = $name,
                sh.adresse = $adresse,
                sh.lat     = $lat,
                sh.lng     = $lng
            """,
            id=sh["id"],
            name=sh.get("name", ""),
            adresse=sh.get("adresse", ""),
            lat=sh.get("lat"),
            lng=sh.get("lng"),
        )
    print(f" {len(shops)} nœuds Shop")

    # --- Relations PARRAINE ---------------------------------------------------
    parrainages = list(db.parrainages.find({}))
    for p in parrainages:
        s.run(
            """
            MATCH (parrain:Client {id: $id_parrain})
            MATCH (filleul:Client {id: $id_filleul})
            MERGE (parrain)-[:PARRAINE {date: $date}]->(filleul)
            """,
            id_parrain=p["idParrain"],
            id_filleul=p["idFilleul"],
            date=p.get("dateParrainage", ""),
        )
    print(f" {len(parrainages)} relations PARRAINE")

    # --- Relations TRAVAILLE_DANS (via entreprise dans clients) ---------------
    nb_travaille = 0
    for c in clients:
        entreprise = c.get("entreprise")
        if entreprise and entreprise.get("siret"):
            s.run(
                """
                MATCH (cl:Client {id: $id})
                MATCH (en:Entreprise {siret: $siret})
                MERGE (cl)-[:TRAVAILLE_DANS]->(en)
                """,
                id=c["id"],
                siret=entreprise["siret"],
            )
            nb_travaille += 1
    print(f" {nb_travaille} relations TRAVAILLE_DANS")

    # --- Contraintes / Index --------------------------------------------------
    s.run("CREATE INDEX client_id IF NOT EXISTS FOR (c:Client) ON (c.id)")
    s.run("CREATE INDEX entreprise_siret IF NOT EXISTS FOR (e:Entreprise) ON (e.siret)")
    s.run("CREATE INDEX shop_id IF NOT EXISTS FOR (s:Shop) ON (s.id)")
    print(" Index créés")

print("\n Import Neo4j terminé !")
driver.close()
mongo.close()
