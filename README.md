# POC - Pipeline Temps Réel avec Redpanda & PySpark

##  Contexte

Dans le cadre de la modernisation de l’infrastructure data, ce projet simule un pipeline de traitement de données en temps réel basé sur :

- Redpanda (streaming)
- PySpark (traitement)
- Stockage en fichiers (output)

L’objectif est de démontrer la capacité à :
- ingérer des données en continu
- les transformer
- produire des analyses exploitables

---

## 📂 Structure du projet

Tous les fichiers du projet sont regroupés dans un même dossier :

```text
redpanda_project_dockerized/
├── .dockerignore
├── create_topic.sh
├── docker-compose.yml
├── Dockerfile.producer
├── Dockerfile.redpanda
├── Dockerfile.spark
├── producer_tickets.py
├── README.md
├── requirements_producer.txt
├── run_spark.sh
├── spark_redpanda_analysis.py
├── wait_for_redpanda.sh
├── output/             # généré à l’exécution
└── checkpoints/        # généré à l’exécution

---

## Partie lancement corrigée

```markdown
## ▶️ Exécution du projet

### 1. Démarrer les services

Depuis le dossier `redpanda_project_dockerized`, lancer :

```bash
docker compose up --build

---

## Partie résultats attendus plus juste

```markdown
## 📊 Résultats produits

Le pipeline génère deux types de sorties :

### 1. Données enrichies
Les tickets enrichis sont enregistrés en **Parquet** dans :

- `output/enriched_tickets`

Chaque ticket contient notamment :
- `ticket_id`
- `client_id`
- `created_at`
- `request`
- `request_type`
- `priority`
- `support_team`

### 2. Agrégats par batch
Des statistiques sont générées en **JSON** dans :

- `output/tickets_by_type`
- `output/tickets_by_priority`
- `output/tickets_by_team`

Chaque batch est stocké dans un sous-dossier du type :

```text
batch_0/
batch_1/
batch_2/
...



```markdown
## Architecture du pipeline

```mermaid
flowchart LR
    A[producer_tickets.py<br/>Génération continue de tickets JSON] --> B[Redpanda<br/>Topic client_tickets]
    B --> C[spark_redpanda_analysis.py<br/>PySpark Structured Streaming]

    C --> D1[Flux enrichi<br/>ajout support_team]
    C --> D2[Agrégation par request_type]
    C --> D3[Agrégation par priority]
    C --> D4[Agrégation par support_team]

    D1 --> E1[output/enriched_tickets<br/>Parquet]
    D2 --> E2[output/tickets_by_type<br/>JSON par batch]
    D3 --> E3[output/tickets_by_priority<br/>JSON par batch]
    D4 --> E4[output/tickets_by_team<br/>JSON par batch]

flowchart LR

    A@{ shape: stadium, label: "Démarrage du pipeline" }

    subgraph SRC["Génération et ingestion"]
        B@{ shape: rect, label: "producer_tickets.py\nGénération continue de tickets JSON" }
        C@{ shape: rect, label: "create_topic.sh\nCréation du topic client_tickets" }
        D@{ shape: cyl, label: "Redpanda\nTopic : client_tickets" }
    end

    subgraph PROC["Traitement temps réel"]
        E@{ shape: rect, label: "spark_redpanda_analysis.py\nLecture streaming depuis Redpanda" }
        F@{ shape: rect, label: "Parsing JSON\nSchéma des tickets" }
        G@{ shape: rect, label: "Enrichissement\nAjout de support_team" }
        H@{ shape: diamond, label: "Agrégations" }
        I@{ shape: rect, label: "Comptage par request_type" }
        J@{ shape: rect, label: "Comptage par priority" }
        K@{ shape: rect, label: "Comptage par support_team" }
    end

    subgraph OUT["Sorties du pipeline"]
        L@{ shape: doc, label: "output/enriched_tickets\nParquet" }
        M@{ shape: doc, label: "output/tickets_by_type\nJSON par batch" }
        N@{ shape: doc, label: "output/tickets_by_priority\nJSON par batch" }
        O@{ shape: doc, label: "output/tickets_by_team\nJSON par batch" }
    end

    P@{ shape: dbl-circ, label: "Résultats exploitables" }

    A --> C
    A --> B
    C --> D
    B --> D
    D --> E
    E --> F
    F --> G
    G --> L
    G --> H
    H --> I
    H --> J
    H --> K
    I --> M
    J --> N
    K --> O
    L --> P
    M --> P
    N --> P
    O --> P
```

## 🏗️ Architecture du pipeline

```mermaid
flowchart LR
    A[producer_tickets.py<br/>Génération de tickets JSON] --> B[Redpanda<br/>Topic client_tickets]
    B --> C[PySpark Streaming<br/>spark_redpanda_analysis.py]

    C --> D[Enrichissement des tickets<br/>Ajout support_team]
    
    D --> E[output/enriched_tickets<br/>Parquet]
    D --> F[Agrégation par type]
    D --> G[Agrégation par priorité]
    D --> H[Agrégation par équipe]

    F --> I[output/tickets_by_type]
    G --> J[output/tickets_by_priority]
    H --> K[output/tickets_by_team]
```
