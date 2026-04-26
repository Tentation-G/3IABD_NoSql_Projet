# Mamion Miam — NoSQL & Big Data

## Répartition des taches
Flavien  : Infrastructure, analyse catalogue produits
Samuel   : Analyse comportement d'achat clients
Roïssath : Analyse parrainage & entreprises

## Prérequis
- Docker Desktop
- Python 3.10+

## Installation

```bash
docker-compose up -d
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
```

## Charger les données
```bash
python import_mongo.py
```

## Lancer les analyses
```bash
python analyse_01_top_categories_produits.py
python analyse_02_top_rayons_categories.py
python analyse_03_top_rayons_produits.py
python analyse_04_categories_lignes_vente.py
python analyse_05_categories_quantite_vendue.py
```
Les graphiques sont exportés dans `flavien_analyse_produit/_out`.