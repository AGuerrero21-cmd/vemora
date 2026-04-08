#  VEMORA (Volcanic Energy Modeling and Research Analysis)
We introduce VEMORA (Volcanic Energy Modeling and Research Analysis), a software tool designed to automate the collection, organization, and probabilistic analysis of volcanic eruption data using an energy‑based approach. VEMORA integrates several key capabilities:

- Automatic downloading of Global Volcanism Program (GVP) data

- Integration of additional information via user‑provided CSV files

- Completeness analysis of eruptive timelines

- Computation of volumetric energy‑based probability density functions (ePDFs)

- Optional temporal clustering of events

By focusing on total thermal energy and partitioning it into lava‑ and tephra‑related components, VEMORA ensures coherence across single‑ and multi‑hazard analyses. This provides a consistent physical basis for probabilistic modeling and supports the development of robust, reproducible hazard assessments.

In this software, we apply VEMORA to Holocene eruptions from the GVP database, complemented by literature data to fill gaps. We present the workflow—from data ingestion to completeness analysis, energy computation, and PDF derivation—and present energy‑based.


## Main workflow

Entry point: `vemora.py`

What it does:
1. Fetches volcano + eruption data from Smithsonian GVP.
2. Stores/updates data in SQLite (`DB/volcanic_data.db` inside your `--project-path`).
3. Applies optional CSV corrections.
4. Computes eruption energies.
5. Runs optional completeness + temporal analysis.
6. Computes total/marginal/absolute PDFs.
7. Runs **automatic PDF normalization DB checks** (fail-fast).
8. Runs optional clustering.

### Steps description

1. **Fetches volcano + eruption data**  
	Establishes a standardized observational baseline (chronology, VEI, event metadata) needed for reproducible hazard and recurrence studies.

2. **Stores/updates data in SQLite**  
	Ensures traceability and reproducibility of analyses; every model output can be linked back to a persistent, queryable dataset state.

3. **Applies optional CSV corrections**  
	Incorporates expert curation and local knowledge, reducing catalog bias and improving physical realism of downstream inference.

4. **Computes eruption energies**  
	Converts heterogeneous descriptors (e.g., volume/VEI) into a comparable physical metric (energy), enabling cross-event and cross-volcano scaling analysis.

5. **Completeness + temporal analysis**  
	Identifies recording bias/change points and estimates inter-eruption time behavior, which is essential for robust recurrence/probabilistic forecasting.

6. **Computes total/marginal/absolute PDFs**  
	Characterizes the statistical structure of eruptive energy, including partitioning between lava and tephra, to support scenario-based hazard quantification.

7. **Automatic PDF normalization DB checks**  
	Validates probabilistic consistency (area ≈ 1) and persistence of key records, preventing mathematically invalid distributions from propagating into interpretation.

8. **Optional clustering**  
	Detects natural groupings in eruption behavior, useful for defining analog classes, comparing eruptive regimes, and hypothesis generation.

### Detailed execution flow (with inputs/outputs)

1. **Initialize project/database context**
	- **Input:** `--project-path`
	- **Action:** sets SQLite root and initializes schema (`volcanoes`, `eruptions`, `epdfs`) if needed.
	- **Output:** database file at `<project-path>/DB/volcanic_data.db`.

2. **Select target volcano**
	- **Input:** `--volcano` (required).
	- **Action:** validates target ID and clears previous run log for that volcano if present.
	- **Output:** clean run context for one volcano.

3. **Ingest Smithsonian records**
	- **Input:** volcano ID.
	- **Action:** fetches volcano + eruption data from GVP and upserts into local DB.
	- **Output:** updated `volcanoes` and `eruptions` records for that volcano.

4. **Apply optional curated corrections**
	- **Input:** `--biblio` CSV path.
	- **Action:** updates eruption metadata and rock-related fields from user-provided corrections.
	- **Output:** corrected DB records; improved data quality for physical/statistical modeling.

5. **Compute physical eruption energy**
	- **Input:** eruption volume/VEI + thermophysical parameters from eruption/volcano defaults.
	- **Action:** computes `e_tp`, `e_tl`, and total `energy` for eruptions.
	- **Output:** energy fields persisted in `eruptions` table.

6. **Completeness and temporal modeling**
	- **Input:** `--completeness`, `--period`.
	- **Action:** optionally detects catalog change points; fits temporal recurrence model (Weibull/exponweib) and computes probability for `period` when requested.
	- **Output:** temporal fit artifacts/plots/DB EPDF (`type='temporal'`).

7. **Energy PDF modeling**
	- **Input:** computed energies + optional `--phi` override.
	- **Action:** fits total-energy PDF, optionally fits phi-distribution, derives marginal lava/tephra PDFs, and stores absolute PDFs.
	- **Output:** EPDF records in DB (`E_total`, `phi`, `marginal`, `absolute_total`, `absolute_lava`, `absolute_tephra`) + plots/text outputs.

8. **Automatic probabilistic consistency checks (fail-fast)**
	- **Input:** stored EPDF records from DB.
	- **Action:** verifies area under `total`, `lava`, `tephra` PDFs is approximately 1 and absolute records exist.
	- **Output:** pass/fail report from `DB_Tools/pdf_normalization_check.py`; pipeline raises error if any check fails.

9. **Optional clustering stage**
	- **Input:** `--clustering`.
	- **Action:** clusters cumulative eruption-energy behavior.
	- **Output:** clustering plots and summary stats.

## Run VEMORA

Basic run:

```bash
python vemora.py --volcano 383030
```

Full run example:

```bash
python vemora.py \
	--volcano 383030 \
	--biblio Example/Correction_DB_Teide_example.csv \
	--completeness \
	--period 1000 \
	--clustering \
	--phi 0.45 \
	--project-path /Users/aleja/Documents/PhD/Data/Data_Analysis
```

## Outputs and files created

VEMORA writes outputs under your `--project-path` (default:
`/Users/aleja/Documents/PhD/Data/Data_Analysis`) using one folder per volcano.

Expected structure after a full run for volcano `383030`:

```text
<project-path>/
├── DB/
│   └── volcanic_data.db
└── 383030/
    ├── Logs/
    │   └── analysis.log
    ├── PDFs/
    │   ├── 383030_Total_energy_per_event.png
    │   ├── 383030_PDF_Best_Fit_Total_Energy.png
    │   ├── PDF_energy_383030.txt
    │   ├── Partitioned_pdf_383030.png
    │   ├── Phi_PDF_383030.png
    │   ├── PHI_parameters_383030.txt
    │   ├── Temporal_distribution_383030.png
    │   ├── Temporal_parameters_383030.txt
    │   ├── Absolute_pdf_total_383030.png
    │   ├── Absolute_pdf_lava_383030.png
    │   ├── Absolute_pdf_tephra_383030.png
    │   ├── marginal_pdf_lava_383030_ePDFs.png
    │   ├── marginal_pdf_lava_data_383030_ePDFs.txt
    │   ├── marginal_pdf_tephra_383030_ePDFs.png
    │   └── marginal_pdf_tephra_data_383030_ePDFs.txt
    ├── Change_Point/
    │   ├── regression_383030.txt
    │   ├── linear_regression_383030.png
    │   ├── exponential_regression_383030.png
    │   ├── change_point_stats_383030.txt
    │   └── change_point_detection_383030.png
    ├── Clustering/
    │   ├── 383030_elbow_test.png
    │   ├── 383030_silhouette_analysis.png
    │   ├── 383030_hierarchical_k<N>.png
    │   ├── 383030_kmeans_k<N>.png
    │   └── 383030_clustering_stats.txt
    └── Reports/
        ├── volcano_report_383030.json
        └── volcano_report_383030.txt
```

Notes:
- Some files are conditional.
	- `Change_Point/*` only if `--completeness` finds enough data and a significant change-point pattern.
	- `Clustering/*` only with `--clustering`.
	- `Temporal_*` only when temporal analysis runs.
	- `Phi_*` and marginal files depend on phi availability or `--phi` override.
- Besides files, VEMORA also writes database records in `epdfs` for:
	`E_total`, `phi`, `marginal`, `absolute_total`, `absolute_lava`, `absolute_tephra`, and optional `temporal`.
- The script removes the previous volcano log at startup:
	`<project-path>/<volcano>/Logs/analysis.log`.

### What the main outputs mean

- `volcanic_data.db`:
	Persistent SQLite catalog with `volcanoes`, `eruptions`, and `epdfs` tables.
- `analysis.log`:
	Timestamped execution trace (data fetch, fitting status, warnings, storage status).
- `PDFs/*.png`:
	Visual diagnostics for energy distributions (total, marginal lava/tephra, temporal, absolute PDFs).
- `PDFs/*.txt`:
	Numerical fit parameters and model quality metrics (KS, p-value, AIC, etc.).
- `Change_Point/*`:
	Completeness/change-point diagnostics and regression comparison results.
- `Clustering/*`:
	Cluster-selection diagnostics and clustered cumulative-energy patterns.
- `Reports/*`:
	Exportable snapshot of one-volcano DB content and summary statistics.

## Input variables (`vemora.py`)

### Required

- `--volcano` (`str`)  
	Volcano identifier in Smithsonian/GVP format (example: `383030`). The full pipeline runs for this ID only.

### Optional

- `--project-path` (`str`, default: `/Users/aleja/Documents/PhD/Data/Data_Analysis`)  
	Root directory used for SQLite DB, logs, figures, and text outputs. Change this to isolate experiments or datasets.

- `--biblio` (`str` path, optional)  
	CSV file with curated corrections and bibliography/event adjustments. If omitted, no correction file is applied.

- `--completeness` (`flag`)  
	Enables change-point/completeness analysis before recurrence and PDF interpretation.

- `--period` (`float`, years, optional)  
	Forecast horizon used in temporal analysis probability query. Example: `--period 50` computes probability within 50 years.

- `--clustering` (`flag`)  
	Enables clustering analysis on cumulative eruption-energy behavior.

- `--phi` (`float`, optional, expected between 0 and 1 for fraction meaning)  
	Overrides fitted lava fraction model with a user-defined value. Useful for sensitivity analysis and scenario testing.

### Notes on combinations

- `--period` is most meaningful with `--completeness`; without completeness, temporal analysis still runs using available intervals.
- If `--phi` is provided, fitted phi distribution is bypassed for marginal PDF partitioning.
- PDF normalization checks always run after PDF computation and can stop execution on failure.



## DB_Tools

This folder contains utilities for database setup, validation, migration, and reporting.

### 1) `DB_Tools/db_config.py`

**Purpose**
- Backend selector that exposes a common DB API.
- Lets code import one module and switch between `sqlite` and `supabase` using `DATABASE_BACKEND`.

**Inputs**
- Environment variable: `DATABASE_BACKEND` (`sqlite` or `supabase`; default is `sqlite`).

**Outputs**
- Re-exported database functions (`add_volcanoSmith`, `volcano_data`, `eruptions_energy`, etc.) from the selected backend.

**When to use**
- When you want backend-agnostic scripts/modules.

---

### 2) `DB_Tools/test_sqlite.py`

**Purpose**
- Sanity/integration test suite for local SQLite operations.
- Verifies schema presence, permissions, CRUD operations, and energy calculation path.

**Inputs**
- CLI flags:
	- `--quick`: minimal checks
	- `--verbose`: detailed output

**Outputs**
- Terminal pass/fail summary with failed test details.
- No analysis artifacts are created; temporary test rows are inserted and removed from
	`<project-path>/DB/volcanic_data.db` during tests.

**When to use**
- After first setup, after schema changes, or before running a large batch of analyses.

**Typical run**
```bash
python DB_Tools/test_sqlite.py
python DB_Tools/test_sqlite.py --quick
python DB_Tools/test_sqlite.py --verbose
```

---

### 3) `DB_Tools/migrate_db.py`

**Purpose**
- Migration utility between `supabase` and `sqlite` backends.
- Also provides backup, validation, and summary statistics commands.

**Inputs**
- CLI options such as:
	- `--from`, `--to`
	- `--volcano-id` or `--all`
	- `--backup`
	- `--stats`
	- `--validate`

**Outputs**
- Migrated records, migration validation messages, backup `.db` file (SQLite mode), and DB stats.
- For `--backup sqlite`, creates:
	`<project-path>/DB/volcanic_data_backup_YYYYMMDD_HHMMSS.db`

**When to use**
- Transitioning projects between local and remote DBs.
- Creating migration snapshots/backups before major edits.

**Typical run**
```bash
python DB_Tools/migrate_db.py --from supabase --to sqlite --volcano-id 211060
python DB_Tools/migrate_db.py --backup sqlite
python DB_Tools/migrate_db.py --stats sqlite
```

---

### 4) `DB_Tools/count_eruptions.py`

**Purpose**
- Independent exploratory script that queries all Holocene eruptions directly from GVP WFS.
- Builds eruption count summaries and plots volcanoes by number of eruptions.

**Inputs**
- No CLI arguments; uses Smithsonian WFS endpoint and VEI/year filters internally.

**Outputs**
- In-memory summary DataFrame and two plots:
	- all volcanoes sorted by eruption count
	- volcanoes with ≥20 eruptions
- No files are written by default (plots are displayed interactively).

**When to use**
- Global catalog exploration and quick context on eruption-rich systems.

**Typical run**
```bash
python DB_Tools/count_eruptions.py
```

---

### 5) `DB_Tools/pdf_normalization_check.py`

**Purpose**
- Validates probabilistic consistency of stored PDFs in DB.
- Ensures areas under curves are approximately 1 and absolute PDF records exist.

**Inputs**
- Function-level input:
	- volcano ID
	- optional numerical tolerance (default `1e-2`)

**Outputs**
- Structured validation report with per-check pass/fail and area errors.
- Human-readable terminal report via `print_pdf_normalization_report(...)`.
- No files are created; checks run against EPDF records already stored in
	`<project-path>/DB/volcanic_data.db`.

**When to use**
- Automatically runs inside `vemora.py` after PDF generation.
- Can be used manually for QA on previously generated EPDFs.

---

### 6) `DB_Tools/generate_volcano_report.py`

**Purpose**
- Generates a complete volcano-level DB report if the volcano exists.
- Aggregates volcano metadata, all eruptions, all EPDF records, and summary statistics.

**Inputs**
- `--volcano` (required)
- `--project-path` (optional)
- `--output-dir` (optional)

**Outputs**
- `volcano_report_<id>.json`
- `volcano_report_<id>.txt`
- Default output directory (if `--output-dir` is not provided):
	`<project-path>/<id>/Reports/`

**When to use**
- Archiving one-volcano analysis state, sharing reproducible data snapshots, or audit/report preparation.

**Typical run**
```bash
python DB_Tools/generate_volcano_report.py --volcano 383030 --project-path /Users/aleja/Documents/PhD/Data/Data_Analysis
```

## Relevant files

- `vemora.py` – main CLI pipeline
- `Data_Analysis_VEMORA.py` – analysis functions
- `SQLite_connection.py` – SQLite CRUD + EPDF storage
- `DB_Tools/pdf_normalization_check.py` – PDF area/persistence checks
- `DB_Tools/generate_volcano_report.py` – full volcano DB report generator
