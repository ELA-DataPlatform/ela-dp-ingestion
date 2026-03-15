# ela-dp-ingestion

Pipeline d'ingestion de données personnelles vers Google BigQuery. Fetch depuis des APIs externes, écrit en JSONL sur GCS, charge en BQ, puis archive les fichiers traités.
# ⚠️ Bloc à ajouter TOUT EN HAUT du fichier, avant "# ela-dp-ingestion"

## Règles de travail

- **Langue** : code, comments, docstrings, noms de variables → anglais. Échanges, explications, commits → français.
- **Explique ton raisonnement avant de coder.** Décris ton approche, attends mon feu vert, puis implémente.
- **Changements minimaux.** Ne modifie que ce qui est nécessaire. Pas de refacto non demandée.
- **Pas de sur-ingénierie.** Privilégie le simple et lisible. IMPORTANT : ne propose pas d'abstractions, patterns ou dépendances supplémentaires sauf si je le demande explicitement.
- **Pas de nouvelles dépendances** sans mon accord.

## GCP — exploration en lecture seule

**Projets GCP** : toujours utiliser `--project=ela-dp-dev` (dev) ou `--project=ela-dp-prd` (prd). Ne JAMAIS utiliser d'autres project IDs (ex: `polar-scene-*`).

Tu as accès à `gcloud` et `bq` dans le terminal. Utilise-les librement pour explorer :

```bash
# ✅ Autorisé sans demander :
gcloud storage ls gs://ela-source-dev/
bq query --nouse_legacy_sql --project_id=ela-dp-dev 'SELECT * FROM dp_lake_spotify_dev.INFORMATION_SCHEMA.TABLES'
bq show --project_id=ela-dp-dev dp_lake_spotify_dev.normalized_recently_played
gcloud run jobs list --project=ela-dp-dev

# 🚫 INTERDIT sans mon accord explicite :
# - Toute opération d'écriture/suppression (bq load, gcloud storage rm, gcloud deploy...)
# - Toute opération sur les projets/buckets *-prd
```

## Git

- **Conventional Commits** en français : `feat:`, `fix:`, `refactor:`, `docs:`, `chore:`
- **GitHub Flow** : toujours travailler sur une branche, jamais de commit direct sur `main`
- Nommage branches : `feat/<description>`, `fix/<description>`

## Ce repo dans le contexte ELA DATAPLATFORM

Ce repo gère UNIQUEMENT le fetch API + ingestion GCS → BigQuery.
- L'infra (Cloud Run, IAM, buckets) est dans `ela-infra-as-code` (Terraform) — NE PAS créer d'infra ici
- La transformation est dans `ela-dbt-core` (dbt) — NE PAS faire de SQL analytique ici
- Le frontend est dans `ela-dp-frontend` (Next.js) — NE PAS ajouter de route ou d'API ici
- Ne PAS ajouter de framework web (Flask, FastAPI) — c'est un Cloud Run Job

## Architecture

```
API externe
    │
    ▼
src/fetch/{source}.py       ← appels API, retourne list[dict]
    │
    ▼
src/writer.py               ← sérialise en JSONL + injecte _ingested_at
    │
    ▼
GCS /landing/               ← stockage brut
    │
    ▼
src/load/{source}.py        ← load GCS → BigQuery (WRITE_APPEND)
    │
    ▼
BigQuery dataset.table
    │
    ▼
GCS /archive/               ← fichier déplacé après ingestion réussie
```

## Sources supportées

### Spotify (`src/fetch/spotify.py`)
Data types disponibles : `recently_played`, `saved_tracks`, `saved_albums`, `followed_artists`, `playlists`, `user_profile`, `top_tracks`, `top_artists`

Authentification via refresh token (pas de flow OAuth interactif). Variables d'environnement requises :
```
SPOTIFY_CLIENT_ID
SPOTIFY_CLIENT_SECRET
SPOTIFY_REDIRECT_URI
SPOTIFY_REFRESH_TOKEN
```

## Nom des fichiers GCS

Pattern : `{YYYY_MM_DD_HH_MM}_{source}_{data_type}.jsonl`

Exemple : `2026_03_13_10_00_spotify_recently_played.jsonl`

Ce pattern est utilisé en mode `load` pour auto-détecter le `data_type` sans qu'il soit passé en argument.

## Colonne `_ingested_at`

Chaque record reçoit automatiquement un champ `_ingested_at` (ISO 8601 UTC) au moment de l'écriture dans `src/writer.py`. Utilisé pour la déduplication côté BQ :

```sql
SELECT * FROM `project.dataset.table`
QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY _ingested_at DESC) = 1
```

## Config d'ingestion (`config/ingestion.yaml`)

Mappe chaque `source.data_type` vers un `dataset.table` BigQuery. Le placeholder `{env}` est résolu au runtime selon `--env dev|prd`.

```yaml
spotify:
  recently_played:
    dataset: dp_lake_spotify_{env}   # → dp_lake_spotify_dev ou dp_lake_spotify_prd
    table: normalized_recently_played
```

Pour ajouter une destination ou renommer une table : modifier uniquement ce fichier, sans toucher au code.

## Commandes Make

```bash
# Fetch API Spotify → GCS /landing/
make fetch OUTPUT=gs://bucket/spotify/landing/ TYPES="recently_played top_tracks"

# Ingestion GCS /landing/ → BigQuery + déplacement vers /archive/
make load GCS_DIR=gs://bucket/spotify/landing/

# Fetch + load en une seule commande (mode all)
make run OUTPUT=gs://bucket/spotify/landing/
```

Variables Make disponibles :
| Variable | Défaut | Description |
|---|---|---|
| `ENV` | `dev` | Environnement GCP (`dev` ou `prd`) |
| `SOURCE` | `spotify` | Source de données |
| `TYPES` | `recently_played` | Data types (espace-séparé pour plusieurs) |
| `OUTPUT` | chemin local | Destination fetch (local ou `gs://`) |
| `GCS_DIR` | — | Dossier GCS à ingérer (mode `load`) |

## Modes d'exécution (`--mode`)

| Mode | Comportement |
|---|---|
| `fetch` | API → JSONL sur GCS/local uniquement |
| `load` | JSONL GCS → BigQuery + archive (pas de fetch) |
| `all` | fetch + load en séquence (si `OUTPUT` est GCS) |

## Gestion des credentials GCP

En local via Docker, les ADC sont montés depuis `~/.config/gcloud/` :
```
-v ~/.config/gcloud:/root/.config/gcloud:ro
-e GOOGLE_APPLICATION_CREDENTIALS=/root/.config/gcloud/application_default_credentials.json
```

En production (Cloud Run / GKE), utiliser un Service Account avec les rôles :
- `roles/storage.objectAdmin` sur le bucket source
- `roles/bigquery.dataEditor` sur les datasets cibles
- `roles/bigquery.jobUser` sur le projet

## Ajouter une nouvelle source

1. Créer `src/fetch/{source}.py` avec un `DataType` enum et un connecteur `.from_env()`
2. Créer `src/load/{source}.py` avec une fonction `load(gcs_uri, data_type, project, dataset, table)`
3. Référencer la source dans `SOURCE_MAP` de `run.py`
4. Ajouter les mappings dans `config/ingestion.yaml`
5. Ajouter les variables d'env nécessaires dans `.env`
