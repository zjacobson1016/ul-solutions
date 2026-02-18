# UL Solutions - Equipment Certification Intelligence Demo

## Demo Talking Points: End-to-End Unstructured Data to AI-Powered Insights

---

## Opening / Scene Setting

- **Customer pain point**: UL Solutions processes thousands of industrial equipment certification reports (PDFs) containing GD&T drawings, material specifications, test results, and compliance data. Today this is largely manual -- engineers read PDFs, transcribe key data points, and enter them into spreadsheets or systems.
- **What we'll show**: Databricks can ingest raw PDF documents, use AI to automatically parse and extract structured data, build governed business metrics, and deliver self-service analytics and conversational Q&A -- all serverless, all governed through Unity Catalog.

---

## Act 1: Data Ingestion (Unity Catalog Volumes + Auto Loader)

**Show**: `bronze_equipment_docs.sql`

### Key Talking Points

- PDF certification reports are landed into a **Unity Catalog Volume** -- a governed, cloud-native file storage layer. No separate blob storage accounts to manage.
- **Auto Loader** (`read_files` with `STREAM`) picks up new PDFs automatically as they arrive -- no file polling scripts, no cron jobs.
- Binary content is ingested as-is, preserving the original document for downstream AI processing.
- This is a **streaming table** -- incremental by default. Drop a new PDF into the volume and it flows through the entire pipeline automatically.

### Demo Action

> "When a new certification report comes in, it's simply dropped into a Unity Catalog Volume. Auto Loader detects it and ingests the raw binary -- no ETL code, no connectors to configure."

---

## Act 2: AI Document Parsing (ai_parse_document)

**Show**: `silver_parsed_equipment.sql`

### Key Talking Points

- **`ai_parse_document()`** is a built-in Databricks AI Function -- no external OCR services, no API keys, no model hosting to manage. It's a SQL function call.
- It understands document **layout**: text blocks, tables (as HTML), figures (with AI-generated descriptions), and page structure. This isn't just OCR -- it's structural understanding.
- We extract **document structure metrics** (page count, table count, figure count) alongside the content itself -- giving visibility into what the AI parsed.
- Figure descriptions are AI-generated -- the model describes what it "sees" in engineering diagrams and GD&T drawings.
- Still streaming -- this layer processes incrementally as bronze data arrives.

### Demo Action

> "With a single SQL function call, we go from a raw PDF binary to structured text, extracted tables, and AI-described figures. Notice this handles GD&T drawings, specification tables, and multi-page layouts -- all without any custom model training."

---

## Act 3: AI-Powered Data Extraction (ai_query + LLM)

**Show**: `gold_equipment_catalog.sql`

### Key Talking Points

- **`ai_query()`** calls a Foundation Model (Llama 3.3 70B) directly from SQL to extract 14 structured fields from the parsed document text.
- Fields extracted: equipment name, model number, manufacturer, certification status, safety rating, IP rating, voltage, weight, temperature range, compliance standards, and more.
- The LLM output (JSON string) is parsed with **`from_json()`** into a strongly-typed struct -- giving you clean, queryable columns in your gold table.
- This is a **materialized view** -- it automatically refreshes when upstream data changes, and query results are pre-computed for fast reads.
- Two additional gold views explode **table specifications** and **figure descriptions** for detailed analysis.

### Demo Action

> "We're using a foundation model as a SQL function to turn unstructured certification text into 14 typed columns. Equipment name, certification status, IP rating, operating temperature range -- all extracted by AI, all queryable with standard SQL. No Python, no notebooks, no model deployment."

---

## Act 4: Governed Business Metrics (Metric Views)

**Show**: `metric_view_equipment.sql`

### Key Talking Points

- **Metric Views** define reusable, governed business metrics in a declarative YAML format. Dimensions and measures are defined once, then queryable from any tool.
- 9 dimensions (Equipment Type, Manufacturer, Certification Status, IP Rating, etc.) and 14 measures (pass rates, weight statistics, temperature ranges, document processing stats).
- Metrics are **centrally governed** -- a single source of truth. When the definition of "Certification Pass Rate" changes, it changes everywhere.
- Any downstream consumer (Genie, dashboards, notebooks, BI tools) gets consistent metric definitions without copy-pasting SQL logic.

### Demo Action

> "Instead of every analyst writing their own definition of 'Certification Pass Rate,' it's defined once as a governed metric. Dimensions and measures are declared in YAML, version-controlled, and consistent across every tool."

---

## Act 5: Conversational Analytics (Genie Space)

**Show**: Genie Space UI (live demo)

### Key Talking Points

- **Genie Space** lets business users ask natural language questions about their equipment certification data -- no SQL knowledge required.
- Backed by the Metric View, so answers are always consistent with the governed metric definitions.
- Sample questions to demo:
  - *"What is the certification pass rate by manufacturer?"*
  - *"Show me all equipment with IP67 rating"*
  - *"Which equipment types have the widest operating temperature range?"*
  - *"How many conditional certifications are there?"*
- Engineers and compliance teams can self-serve without waiting for a data team to build a report.

### Demo Action

> "A compliance manager can walk up to Genie and ask 'What's our certification pass rate by equipment type?' in plain English. The answer comes from governed metrics, not from someone's one-off spreadsheet."

---

## Act 6: Document Q&A (Knowledge Assistant)

**Show**: Knowledge Assistant UI (live demo)

### Key Talking Points

- The **Knowledge Assistant** is built on the same source PDFs stored in Unity Catalog Volumes -- a RAG-based conversational agent for document-level Q&A.
- While Genie answers structured data questions, the Knowledge Assistant answers questions about the actual document content -- specific test procedures, GD&T callouts, material certifications.
- Sample questions to demo:
  - *"What dielectric withstand test thresholds are required?"*
  - *"Describe the GD&T tolerances for mounting surfaces"*
  - *"What compliance standards does the VFD-4521 need to meet?"*
- Both interfaces are available side-by-side -- structured analytics through Genie, deep document Q&A through the Knowledge Assistant.

### Demo Action

> "Genie tells you *what* your certification data looks like in aggregate. The Knowledge Assistant tells you *why* -- pulling specific test results, GD&T specs, and compliance details directly from the source documents."

---

## Act 7: Production-Ready Deployment (Databricks Asset Bundles)

**Show**: `databricks.yml` + `resources/` folder

### Key Talking Points

- Everything shown today is packaged as a **Databricks Asset Bundle (DAB)** -- infrastructure-as-code for the entire solution.
- One `databricks bundle deploy` command deploys the pipeline, jobs, and all resources to any environment.
- **Parameterized** with variables for catalog, schema, and volume -- the same code promotes from dev to staging to prod by changing a target.
- **100% serverless** -- no clusters to size, no infrastructure to manage. The pipeline, jobs, and warehouse all run on serverless compute.
- Version-controlled in Git -- full auditability of what changed, when, and by whom.

### Demo Action

> "This isn't a one-off demo. Everything is packaged as code. We deploy to dev with one command, validate, then promote to production with the same bundle. No manual configuration, no snowflake environments."

---

## Closing / Value Summary

| Capability | Traditional Approach | Databricks Approach |
|---|---|---|
| PDF Ingestion | Custom scripts, cron jobs, blob storage | Auto Loader + UC Volumes (streaming, governed) |
| Document Parsing | Third-party OCR APIs, custom ML models | `ai_parse_document()` -- single SQL function |
| Data Extraction | Manual data entry, regex, custom NLP | `ai_query()` with Foundation Models in SQL |
| Business Metrics | Scattered SQL, inconsistent definitions | Metric Views -- governed, reusable, version-controlled |
| Self-Service Analytics | Ticket-driven report requests | Genie Space -- natural language, instant answers |
| Document Q&A | Read the PDF manually | Knowledge Assistant -- RAG over source documents |
| Deployment | Manual config, environment drift | Asset Bundles -- one command, any environment |
| Infrastructure | Cluster sizing, cost management | 100% serverless -- zero infrastructure overhead |

### The Bottom Line

> "Databricks turns your certification PDFs from static files into a governed, queryable, AI-powered intelligence layer -- from raw document to conversational analytics in a single platform, deployed as code, running entirely serverless."

---

## Appendix: Demo Execution Order

1. **Show the source PDFs** -- open one in the UC Volume browser to show the complexity (GD&T drawings, tables, multi-page layout)
2. **Walk the pipeline DAG** -- Bronze > Silver > Gold in the SDP pipeline UI
3. **Query the gold table** -- show the 14 AI-extracted columns alongside the original document text
4. **Open the Metric View** -- show the YAML definition and run a `SELECT MEASURE(...)` query
5. **Demo Genie Space** -- ask 2-3 natural language questions live
6. **Demo Knowledge Assistant** -- ask a document-specific question that Genie can't answer
7. **Show the Asset Bundle** -- `databricks.yml` and the `databricks bundle validate` output
