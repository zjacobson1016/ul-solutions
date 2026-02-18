# UL Solutions: From PDFs to Conversational Intelligence

## The Demo Story (Start With the End)

> **Narrative arc**: Show the customer what their engineers and compliance teams will experience *first*, then peel back the layers to reveal how Databricks makes it possible — entirely serverless, entirely governed, entirely deployed as code.

---

## Scene 1: The Two Interfaces Your Teams Will Use Every Day

### 1A — Genie Space: "Ask Your Certification Data Anything"

**Open the Genie Space: "UL Solutions Equipment Catalog"**

A compliance manager sits down Monday morning. Instead of opening a spreadsheet or filing a Jira ticket to the data team, she types:

> *"What is our certification pass rate by equipment type?"*

Genie returns an instant, governed answer — pulled from a Metric View that defines "Certification Pass Rate" exactly once, consistently, for the entire organization.

Follow up live:

- *"Which manufacturers have conditional certifications?"*
- *"Show me all equipment at the Chicago Manufacturing Complex with overdue inspections"*
- *"What's the total maintenance cost by facility?"*
- *"Which equipment has conditional certifications AND open work orders?"*

**Key message**: No SQL. No waiting for reports. No ambiguity about what "pass rate" means — it's defined once, governed in Unity Catalog, and consistent everywhere. And because we've joined certification data with operational inventory, Genie can answer questions that span both worlds.

---

### 1B — Knowledge Assistant: "Ask Your Documents Anything"

**Open the Knowledge Assistant**

Now the same compliance manager needs to go deeper. Genie told her there's a conditional certification. She needs to understand *why*. She asks the Knowledge Assistant:

> *"Why are the conditional certs for ABB Power Solutions Power Distribution Unit?"*

The Knowledge Assistant retrieves the answer directly from the source PDF — the actual certification report sitting in a Unity Catalog Volume — and cites the specific section.

Follow up live - Don't demo unless needed:

- *"Describe the GD&T tolerances for mounting surfaces"*
- *"What compliance standards does this model need to meet?"*
- *"Summarize the test results from the most recent PLC certification"*

**Key message**: Genie answers *what* your data looks like in aggregate. The Knowledge Assistant answers *why* — pulling specific test procedures, GD&T specs, and compliance details from the source documents. Both are governed, both are self-service, both are available right now.

---

### Transition: "So how did we get here?"

> *"Everything you just saw — the conversational analytics, the document Q&A — is powered by a single pipeline that turns raw PDFs into governed, queryable intelligence. Let me show you how it works, layer by layer."*

---

## Scene 2: The 360° View — Structured Meets Unstructured

**Open**: `gold_equipment_360.sql` and `gold_maintenance_insights.sql`

Before diving into how the data was built, show *what* the pipeline produces — a complete picture that couldn't exist without joining structured and unstructured data.

The **`gold_equipment_360`** materialized view joins three worlds:

| Source | Type | What It Provides |
|--------|------|-----------------|
| `equipment_inventory` | Structured (ERP) | Asset IDs, facility assignments, purchase history, operational status, inspection schedules |
| `gold_equipment_catalog` | Unstructured (AI-extracted from PDFs) | Certification status, safety ratings, IP ratings, compliance standards |
| `facilities` | Structured (ERP) | Plant locations, regions, employee counts |
| `manufacturer_contracts` | Structured (procurement) | Contract terms, SLA response times, contract status |

The **`gold_maintenance_insights`** view adds work order history — aggregating labor hours, parts costs, downtime, and emergency repair frequency per asset — then flags risk levels:

```sql
CASE
  WHEN certification_status = 'CONDITIONAL' AND open_work_orders > 0
    THEN 'HIGH RISK — Conditional cert with open work orders'
  WHEN certification_status = 'CONDITIONAL'
    THEN 'MEDIUM RISK — Conditional certification'
  WHEN next_inspection_due < current_date()
    THEN 'MEDIUM RISK — Overdue inspection'
  WHEN emergency_repairs >= 3
    THEN 'ELEVATED — Frequent emergency repairs'
  ELSE 'NORMAL'
END AS risk_level
```

**What to highlight**:

- **Structured + unstructured in one view** — ERP inventory data joined with AI-extracted certification data from PDFs, all in SQL, all in one pipeline.
- **Risk flags computed automatically** — conditional certifications combined with open work orders surface high-risk equipment without manual review.
- **Facility-level visibility** — "How many Siemens PLCs are at the Houston plant, and what's their certification status?" is now a single query.
- The join is on `manufacturer` + `equipment_type` — shared reference data between ERP systems and certification documents.

> *"This is the view that doesn't exist today. It combines what you know operationally — where equipment is, its maintenance history, its warranty status — with what's buried in certification PDFs — safety ratings, compliance standards, test results. One table, one query, one answer."*

---

## Scene 3: Structured Data Layer (ERP / CMMS Data)

**Open**: `02_generate_structured_data.py`

The 360° view is powered by four structured operational tables that represent what would come from ERP, CMMS, and procurement systems:

| Table | Rows | What It Represents |
|-------|------|--------------------|
| `facilities` | 8 | Plant locations across North America and EMEA — manufacturing plants, distribution centers, R&D labs |
| `equipment_inventory` | 120 | Asset registry — every piece of equipment with its facility, purchase date, price, warranty, inspection schedule, and operational status |
| `work_orders` | 500 | Two years of maintenance history — preventive maintenance, corrective repairs, emergency callouts, calibrations |
| `manufacturer_contracts` | 10 | Supplier agreements with SLA terms, contract values, and expiration dates |

**What to highlight**:

- Uses the **exact same manufacturers and equipment types** as the certification PDFs — Siemens, ABB, Schneider, Eaton, Rockwell, etc. This is what makes the join work.
- **Realistic distributions** — larger plants have more equipment; transformers and switchgear cost more than relay modules; emergency repairs are always high priority.
- Equipment prices, maintenance costs, and work order volumes are realistic for industrial equipment operations.
- These tables represent data that **already exists** in the customer's ERP (SAP, Oracle, Maximo) — Databricks just needs to ingest it.

> *"This data already lives in your ERP and CMMS systems today. We're bringing it into Databricks alongside the AI-extracted certification data so you can ask questions that span both worlds."*

---

## Scene 4: Governed Business Metrics (The Foundation for Genie)

**Open**: `metric_view_equipment.sql`

The reason Genie can answer questions consistently is **Metric Views** — a declarative way to define business metrics once and consume them everywhere.

```sql
CREATE OR REPLACE VIEW equipment_certification_metrics
WITH METRICS
LANGUAGE YAML
AS $$
version: "1.1"
source: mfg_mc_se_sa.ul_solutions.gold_equipment_catalog
dimensions:
  - name: Equipment Type
    expr: equipment_type
  - name: Manufacturer
    expr: manufacturer
  - name: Certification Status
    expr: certification_status
  ...
measures:
  - name: Certification Pass Rate
    expr: ROUND(COUNT_IF(certification_status = 'PASS') * 100.0 / COUNT(*), 1)
  - name: Total Equipment
    expr: COUNT(*)
  ...
$$;
```

**What to highlight**:

- **9 dimensions** (Equipment Type, Manufacturer, IP Rating, Voltage Rating, etc.) and **14 measures** (pass rates, weight stats, temperature ranges, document processing metrics).
- Defined in **YAML** — declarative, version-controlled, auditable.
- **Single source of truth**: when the definition of "Certification Pass Rate" changes, it changes for Genie, dashboards, notebooks — everywhere.
- This is what makes Genie's answers *trustworthy*. The metric isn't invented by the LLM — it's governed.

> *"Instead of every analyst writing their own definition of pass rate, it's defined once. Genie consumes this. Dashboards consume this. Everyone gets the same number."*

---

## Scene 5: AI-Powered Data Extraction (How We Built the Gold Table)

**Open**: `gold_equipment_catalog.sql`

The Metric View sits on top of `gold_equipment_catalog` — a materialized view where a Foundation Model (Llama 3.3 70B) extracts 14 structured fields from unstructured document text using `ai_query()`:

```sql
ai_query(
  'databricks-meta-llama-3-3-70b-instruct',
  concat(
    'Extract these fields as a JSON object from this industrial equipment
     certification document...',
    'Fields: equipment_name, model_number, manufacturer, equipment_type,
     certification_id, certification_status, safety_rating, material_type,
     weight_kg, voltage_rating, ip_rating, operating_temp_min_c,
     operating_temp_max_c, compliance_standards',
    left(document_text, 8000)
  )
)
```

The LLM output is a JSON string, parsed with `from_json()` into strongly-typed columns — queryable with standard SQL.

**What to highlight**:

- **Foundation Model as a SQL function** — no Python, no model deployment, no API keys. `ai_query()` calls Llama 3.3 70B directly from SQL.
- **14 typed columns** extracted: equipment name, model number, manufacturer, certification status, IP rating, operating temperature range, compliance standards, and more.
- **Materialized view** — pre-computed, automatically refreshed when upstream data changes.
- Two additional gold views explode **table specifications** (`gold_table_specifications`) and **figure descriptions** (`gold_figure_descriptions`) for detailed analysis.

> *"We're using a foundation model as a SQL function to turn free-form certification text into 14 strongly-typed columns. No notebooks, no custom ML. Standard SQL."*

---

## Scene 6: AI Document Parsing (How We Understood the PDFs)

**Open**: `silver_parsed_equipment.sql`

Before the LLM can extract fields, we need to understand the document's structure. `ai_parse_document()` handles this — a built-in Databricks function that understands layout:

```sql
ai_parse_document(
  file_content,
  map('version', '2.0', 'descriptionElementTypes', '*')
) AS parsed_doc
```

From the parsed output, we extract:

| Output | Description |
|--------|-------------|
| `document_text` | All text content concatenated from layout elements |
| `num_pages` | Page count |
| `num_tables` / `table_contents` | Count and HTML content of specification tables |
| `num_figures` / `figure_descriptions` | Count and AI-generated descriptions of diagrams |
| `num_text_blocks` | Number of distinct text blocks |

**What to highlight**:

- **Not just OCR** — `ai_parse_document()` understands document *structure*: text blocks, tables (as HTML), figures (with AI-generated descriptions), and page layout.
- **Single SQL function call** — no third-party OCR service, no API keys, no model hosting.
- Handles **GD&T drawings**, specification tables, and multi-page layouts.
- Still a **streaming table** — processes incrementally as new documents arrive.

> *"With one SQL function, a raw PDF binary becomes structured text, extracted tables, and AI-described engineering diagrams. This handles GD&T drawings and multi-page layouts — no custom model training required."*

---

## Scene 7: Data Ingestion (Where It All Starts)

**Open**: `bronze_equipment_docs.sql`

The entire pipeline starts when a certification PDF is dropped into a **Unity Catalog Volume**:

```sql
CREATE OR REFRESH STREAMING TABLE bronze_equipment_docs
AS SELECT
  path          AS file_path,
  content       AS file_content,
  length        AS file_size_bytes,
  ...
FROM STREAM read_files(
  '${volume_path}',
  format => 'binaryFile'
)
WHERE path LIKE '%.pdf';
```

**What to highlight**:

- **UC Volumes** are governed, cloud-native file storage — no separate blob storage accounts to configure.
- **Auto Loader** (`read_files` with `STREAM`) detects new PDFs automatically. No file-polling scripts, no cron jobs.
- **Streaming table** — drop a new PDF and it flows through Bronze > Silver > Gold automatically.
- Binary content is preserved as-is for downstream AI processing.

> *"When a new certification report comes in, it's dropped into a Unity Catalog Volume. Auto Loader picks it up, and it flows through the entire pipeline — parsed, extracted, metrified — with zero manual intervention."*

---

## Scene 8: The Live Pipeline (Tie It All Together)

**Open**: The SDP pipeline UI in Databricks

Show the full DAG — both the unstructured path (PDFs → AI extraction) and the structured data joining at the gold layer:

```
  UNSTRUCTURED PATH                           STRUCTURED PATH
  ─────────────────                           ───────────────

  PDF drops into UC Volume                    ERP / CMMS Systems
         │                                          │
         ▼                                          ▼
  ┌─────────────────────────┐          ┌─────────────────────────────┐
  │  bronze_equipment_docs  │          │  facilities                 │
  │  (Streaming Table)      │          │  equipment_inventory        │
  └───────────┬─────────────┘          │  work_orders                │
              │                        │  manufacturer_contracts     │
              ▼                        └──────────┬──────────────────┘
  ┌──────────────────────────┐                    │
  │ silver_parsed_equipment  │                    │
  │ (Streaming Table)        │                    │
  └───────────┬──────────────┘                    │
              │                                   │
              ▼                                   │
  ┌──────────────────────────┐                    │
  │  gold_equipment_catalog  │                    │
  │  (Materialized View)     │                    │
  └───────────┬──────────────┘                    │
              │                                   │
              └──────────┬────────────────────────┘
                         │
              ┌──────────┴──────────┐
              ▼                     ▼
  ┌────────────────────┐  ┌───────────────────────────┐
  │ gold_equipment_360 │  │ gold_maintenance_insights  │
  │ (Materialized View)│  │ (Materialized View)        │
  └────────┬───────────┘  └───────────────────────────┘
           │
    ┌──────┴──────┐
    ▼              ▼
┌──────────┐  ┌──────────────────┐
│  Metric  │  │  Knowledge       │
│  View    │  │  Assistant (RAG) │
└────┬─────┘  │  over source     │
     │        │  PDFs in Volume  │
     ▼        └──────────────────┘
┌──────────┐
│  Genie   │
│  Space   │
└──────────┘
```

**What to highlight**:

- **Structured + unstructured in one pipeline** — the same SDP pipeline that processes PDFs also joins with ERP data at the gold layer.
- **100% serverless** — no clusters to size, no infrastructure to manage.
- **100% SQL** — no Python notebooks in the pipeline (the AI functions are SQL, the joins are SQL).
- **Spark Declarative Pipeline** — just declare the tables you want; Databricks handles orchestration, lineage, error handling, and restarts.
- **Incremental by default** — streaming tables process only new data on each run.

> *"The entire pipeline is SQL. Unstructured PDFs flow through AI extraction on the left. Structured ERP data feeds in from the right. They join at the gold layer to give you a complete 360° view — all declarative, all serverless."*

---

## Scene 9: Deployed as Code (Asset Bundles)

**Open**: `databricks.yml` and `resources/` folder

Everything demonstrated is packaged as a **Databricks Asset Bundle**:

```yaml
bundle:
  name: ul-solutions-equipment-certification

variables:
  catalog:     { default: "mfg_mc_se_sa" }
  schema:      { default: "ul_solutions" }
  volume_name: { default: "raw_data" }

targets:
  dev:   { mode: development, ... }
  prod:  { mode: production,  ... }
```

Two resource definitions power everything:

| Resource | What It Deploys |
|----------|----------------|
| `ul_solutions_pipeline.yml` | The SDP pipeline (Bronze → Silver → Gold + 360° joins) with serverless compute and Photon |
| `ul_solutions_setup_job.yml` | A setup job with 3 tasks: generate PDFs → generate structured data → create Metric View |

**What to highlight**:

- **One command deploys everything**: `databricks bundle deploy -t prod`
- **Parameterized** — same code, different catalog/schema per environment. Dev and prod use the same bundle with different variable values.
- **Version-controlled** — Git tracks every change to pipeline definitions, metric views, and job configs.
- **Reproducible** — no manual clicks, no configuration drift between environments.

> *"This isn't a one-off demo. Everything is infrastructure-as-code. One command deploys the pipeline, jobs, and all resources. Same code goes from dev to prod — no manual configuration."*

---

## The Full Value Story

| Layer | Traditional Approach | What Databricks Does |
|-------|---------------------|---------------------|
| **File Storage** | Blob storage + custom access controls | UC Volumes — governed, cloud-native, ACL'd by Unity Catalog |
| **PDF Ingestion** | Cron jobs, polling scripts, custom connectors | Auto Loader — streaming, incremental, zero config |
| **Document Parsing** | Third-party OCR APIs, custom ML models | `ai_parse_document()` — one SQL function, structural understanding |
| **Data Extraction** | Manual data entry, regex, custom NLP | `ai_query()` — Foundation Model as a SQL function |
| **Structured + Unstructured** | Separate systems, manual reconciliation, spreadsheets | Gold-layer SQL joins in the same pipeline — 360° view |
| **Business Metrics** | Scattered SQL, inconsistent definitions, tribal knowledge | Metric Views — governed, declarative, single source of truth |
| **Self-Service Analytics** | Ticket-driven report requests, weeks of turnaround | Genie Space — natural language, instant, governed answers |
| **Document Q&A** | Read the PDF manually, call the engineer who wrote it | Knowledge Assistant — RAG over source documents in UC Volumes |
| **Deployment** | Manual config, wiki docs, environment drift | Asset Bundles — one command, version-controlled, reproducible |
| **Infrastructure** | Cluster sizing, cost management, capacity planning | 100% serverless — zero infrastructure overhead |

---

## Closing Line

> *"Databricks turns your certification PDFs from static files buried on a file share into a governed, queryable, AI-powered intelligence layer — joined with your operational data to give you a 360° view that doesn't exist today. From raw document to conversational analytics, from ERP data to risk-flagged maintenance insights — in a single platform, deployed as code, running entirely serverless."*

---

## Demo Run Order (Quick Reference)

| Step | What to Show | Time |
|------|-------------|------|
| 1 | **Genie Space** — ask 2-3 questions spanning structured + unstructured data | 3 min |
| 2 | **Knowledge Assistant** — ask a document-specific question Genie can't answer | 2 min |
| 3 | **Gold 360° view** — show the joined table, highlight risk flags and facility context | 2 min |
| 4 | **Structured source tables** — briefly show facilities, inventory, work orders | 2 min |
| 5 | **Metric View** — show the YAML definition, run a `SELECT MEASURE(...)` query | 2 min |
| 6 | **Gold catalog** — show the 14 AI-extracted columns, explain `ai_query()` | 2 min |
| 7 | **Silver table** — show `ai_parse_document()` output, highlight structure | 2 min |
| 8 | **Bronze table** — show Auto Loader + UC Volume, optionally drop a new PDF | 1 min |
| 9 | **Pipeline DAG** — show the full flow (both paths merging) in the SDP UI | 1 min |
| 10 | **Asset Bundle** — show `databricks.yml`, explain deploy flow | 2 min |
| | **Total** | **~19 min** |
