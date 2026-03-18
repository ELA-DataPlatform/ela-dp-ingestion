# Plan de migration : Garmin → nouvelle architecture

## Contexte

Le connecteur Garmin tourne depuis 7 mois dans l'ancien repo et a accumulé de la data de valeur dans BigQuery. L'objectif est une migration **as-is** vers la nouvelle architecture (celle utilisée par Spotify), en conservant la continuité des données : mêmes noms de tables, même format de fichiers, même logique de fetch. Le backfill sera une phase 2 séparée.

La nouvelle architecture est plus simple et linéaire :
```
API → src/fetch/{source}.py → writer.py (JSONL + _ingested_at) → GCS /landing/
→ src/load/{source}.py → BigQuery → GCS /archive/
```

## Décisions prises

- **CLI** : Nouvel argument `--days` dédié (défaut 30), distinct de `--limit`
- **Bucket GCS** : `gs://ela-source-{env}/garmin/landing/` et `gs://ela-source-{env}/garmin/archive/`
- **Backfill** : Reporté à plus tard

## Fichiers créés / modifiés

| Fichier | Action | Description |
|---|---|---|
| `src/fetch/garmin.py` | **Créé** | GarminConnector + DataType enum + fetch logic + flatten_nested_arrays |
| `src/load/garmin.py` | **Créé** | Fonction `load()` (copie quasi identique de spotify) |
| `config/loading.yaml` | **Modifié** | Ajout section `garmin:` avec 29 data types |
| `run.py` | **Modifié** | Ajout garmin au SOURCE_MAP + généralisation du load + `--days` |
| `Dockerfile` | **Modifié** | Ajout `garminconnect` aux dépendances |
| `Makefile` | **Modifié** | Ajout GARMIN_USERNAME/PASSWORD en env Docker |

## Continuité des données

| Aspect | Ancien | Nouveau | Status |
|---|---|---|---|
| Dataset BQ | `dp_normalized_{env}` | `dp_normalized_{env}` (via loading.yaml) | Identique |
| Tables BQ | `normalized_garmin__{metric}` | `normalized_garmin__{metric}` (via loading.yaml) | Identique |
| Noms de fichiers | `{ts}_garmin_{type}.jsonl` | `{ts}_garmin_{type}.jsonl` (via pattern) | Identique |
| Bucket GCS | `gs://ela-source-{env}/garmin/` | Passé via `--output-dir` / `--gcs-dir` | Compatible |
| Champ `data_type` | Ajouté par le fetcher | Ajouté par le fetcher | Identique |
| Transformations | `flatten_nested_arrays` | Même fonction portée | Identique |
| Champ `_ingested_at` | Absent | Ajouté par `writer.py` | Nouveau — non-breaking |

**Note `_ingested_at`** : L'ancien code ne l'ajoutait pas. `writer.py` l'injecte automatiquement. C'est un simple ajout de colonne — `ALLOW_FIELD_ADDITION` le gère.
