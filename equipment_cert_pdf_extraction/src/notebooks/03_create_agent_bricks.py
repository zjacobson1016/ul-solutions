# Databricks notebook source
# MAGIC %md
# MAGIC # UL Solutions - Create Agent Bricks
# MAGIC
# MAGIC Creates the conversational AI layer for the UL Solutions demo:
# MAGIC 1. **Knowledge Assistant** — RAG over equipment certification PDFs in UC Volume
# MAGIC 2. **Genie Space** — Natural language SQL over the equipment metric view (looked up, not created here)
# MAGIC 3. **Multi-Agent Supervisor** — Orchestrates both agents with intelligent routing

# COMMAND ----------

dbutils.widgets.text("catalog", "mfg_mc_se_sa", "Catalog")
dbutils.widgets.text("schema", "ul_solutions", "Schema")
dbutils.widgets.text("volume_name", "raw_data", "Volume Name")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
volume_name = dbutils.widgets.get("volume_name")

VOLUME_PATH = f"/Volumes/{catalog}/{schema}/{volume_name}/equipment_docs"
KA_NAME = "UL_Solutions_Equipment_Docs"
MAS_NAME = "UL_Solutions_Equipment_Intelligence"
GENIE_SPACE_NAME = "UL Solutions Equipment Catalog"

print(f"Catalog:     {catalog}")
print(f"Schema:      {schema}")
print(f"Volume Path: {VOLUME_PATH}")
print(f"KA Name:     {KA_NAME}")
print(f"MAS Name:    {MAS_NAME}")
print(f"Genie Space: {GENIE_SPACE_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Initialize Agent Bricks Manager

# COMMAND ----------

import json
import time
import requests
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Create or Update Knowledge Assistant

# COMMAND ----------

# MAGIC %md
# MAGIC The Knowledge Assistant provides RAG-based Q&A over the equipment
# MAGIC certification PDFs stored in the UC Volume. Engineers and compliance
# MAGIC managers can ask about specific test results, GD&T tolerances,
# MAGIC compliance standards, and certification details.

# COMMAND ----------

def find_ka_by_name(w, name):
    """Find a Knowledge Assistant by exact name using the tiles API."""
    headers = w.config.authenticate()
    url = f"{w.config.host}/api/2.0/tiles"
    params = {"filter": f"name_contains={name}&&tile_type=KA"}
    resp = requests.get(url, headers=headers, params=params, timeout=30)
    resp.raise_for_status()
    for tile in resp.json().get("tiles", []):
        if tile.get("name") == name:
            return tile
    return None


def create_knowledge_assistant(w, name, volume_path, description, instructions):
    """Create a Knowledge Assistant with a UC Volume knowledge source."""
    headers = w.config.authenticate()
    headers["Content-Type"] = "application/json"
    url = f"{w.config.host}/api/2.0/knowledge-assistants"

    source_name = volume_path.rstrip("/").split("/")[-1]
    payload = {
        "name": name,
        "description": description,
        "instructions": instructions,
        "knowledge_sources": [
            {
                "files_source": {
                    "name": source_name,
                    "type": "files",
                    "files": {"path": volume_path},
                }
            }
        ],
    }
    resp = requests.post(url, headers=headers, json=payload, timeout=300)
    resp.raise_for_status()
    return resp.json()


def get_ka(w, tile_id):
    """Get Knowledge Assistant details by tile ID."""
    headers = w.config.authenticate()
    url = f"{w.config.host}/api/2.0/knowledge-assistants/{tile_id}"
    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()
    return resp.json()

# COMMAND ----------

existing_ka = find_ka_by_name(w, KA_NAME)

if existing_ka:
    ka_tile_id = existing_ka["tile_id"]
    ka_details = get_ka(w, ka_tile_id)
    ka_status = ka_details.get("knowledge_assistant", {}).get("status", {}).get("endpoint_status", "UNKNOWN")
    print(f"Knowledge Assistant already exists:")
    print(f"  Tile ID: {ka_tile_id}")
    print(f"  Name:    {KA_NAME}")
    print(f"  Status:  {ka_status}")
else:
    print(f"Creating Knowledge Assistant '{KA_NAME}'...")
    ka_result = create_knowledge_assistant(
        w,
        name=KA_NAME,
        volume_path=VOLUME_PATH,
        description=(
            "Answers questions about UL Solutions industrial equipment certification "
            "reports. Retrieves specific test results, GD&T tolerances, compliance "
            "standards, material specifications, and certification details directly "
            "from source PDF documents stored in Unity Catalog Volumes."
        ),
        instructions=(
            "You are a technical assistant for UL Solutions equipment certification "
            "documents. When answering questions:\n"
            "1. Always cite the specific certification report (by certification ID "
            "and model number) when providing information\n"
            "2. Provide exact values from the documents (test measurements, tolerances, "
            "ratings) rather than summaries\n"
            "3. If a question asks about conditional certifications, explain the specific "
            "failed tests and corrective action requirements\n"
            "4. Use proper engineering terminology (GD&T, IP ratings, UL standards)\n"
            "5. If information is not found in the documents, clearly state that rather "
            "than speculating"
        ),
    )

    ka_tile_id = (
        ka_result.get("knowledge_assistant", {})
        .get("tile", {})
        .get("tile_id")
    )
    ka_status = (
        ka_result.get("knowledge_assistant", {})
        .get("status", {})
        .get("endpoint_status", "PROVISIONING")
    )
    print(f"Knowledge Assistant created:")
    print(f"  Tile ID: {ka_tile_id}")
    print(f"  Name:    {KA_NAME}")
    print(f"  Status:  {ka_status}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Look Up Existing Genie Space

# COMMAND ----------

# MAGIC %md
# MAGIC The Genie Space `UL Solutions Equipment Catalog` is created separately
# MAGIC (via the Databricks UI or API) and configured with the
# MAGIC `equipment_certification_metrics` metric view. Here we look it up
# MAGIC by name so we can wire it into the Multi-Agent Supervisor.

# COMMAND ----------

def find_genie_space_by_name(w, display_name):
    """Find a Genie Space by display name."""
    headers = w.config.authenticate()
    url = f"{w.config.host}/api/2.0/data-rooms"
    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()
    for space in resp.json().get("spaces", []):
        if space.get("display_name") == display_name:
            return space
    return None

# COMMAND ----------

genie_space = find_genie_space_by_name(w, GENIE_SPACE_NAME)

if genie_space:
    genie_space_id = genie_space["space_id"]
    print(f"Found Genie Space:")
    print(f"  Space ID: {genie_space_id}")
    print(f"  Name:     {genie_space['display_name']}")
else:
    raise ValueError(
        f"Genie Space '{GENIE_SPACE_NAME}' not found. "
        "Create it in the Databricks UI first with the "
        "equipment_certification_metrics metric view."
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Create or Update Multi-Agent Supervisor

# COMMAND ----------

# MAGIC %md
# MAGIC The MAS routes user queries to the right specialized agent:
# MAGIC - **Document questions** (test results, GD&T specs, compliance details) → Knowledge Assistant
# MAGIC - **Data/analytics questions** (pass rates, inventory, facility metrics) → Genie Space

# COMMAND ----------

def find_mas_by_name(w, name):
    """Find a Multi-Agent Supervisor by exact name using the tiles API."""
    headers = w.config.authenticate()
    url = f"{w.config.host}/api/2.0/tiles"
    params = {"filter": f"name_contains={name}&&tile_type=MAS"}
    resp = requests.get(url, headers=headers, params=params, timeout=30)
    resp.raise_for_status()
    for tile in resp.json().get("tiles", []):
        if tile.get("name") == name:
            return tile
    return None


def create_multi_agent_supervisor(w, name, agents, description, instructions):
    """Create a Multi-Agent Supervisor."""
    headers = w.config.authenticate()
    headers["Content-Type"] = "application/json"
    url = f"{w.config.host}/api/2.0/multi-agent-supervisors"

    payload = {
        "name": name,
        "agents": agents,
        "description": description,
        "instructions": instructions,
    }
    resp = requests.post(url, headers=headers, json=payload, timeout=300)
    resp.raise_for_status()
    return resp.json()


def get_mas(w, tile_id):
    """Get Multi-Agent Supervisor details by tile ID."""
    headers = w.config.authenticate()
    url = f"{w.config.host}/api/2.0/multi-agent-supervisors/{tile_id}"
    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()
    return resp.json()


def add_mas_examples(w, tile_id, examples):
    """Add example questions to a Multi-Agent Supervisor."""
    headers = w.config.authenticate()
    headers["Content-Type"] = "application/json"
    created = []
    for ex in examples:
        url = f"{w.config.host}/api/2.0/multi-agent-supervisors/{tile_id}/examples"
        payload = {"tile_id": tile_id, "question": ex["question"]}
        if ex.get("guideline"):
            payload["guidelines"] = [ex["guideline"]]
        try:
            resp = requests.post(url, headers=headers, json=payload, timeout=30)
            resp.raise_for_status()
            created.append(resp.json())
            print(f"  Added example: {ex['question'][:60]}...")
        except Exception as e:
            print(f"  Failed to add example: {e}")
    return created

# COMMAND ----------

MAS_AGENTS = [
    {
        "name": "equipment_docs_agent",
        "description": (
            "Answers questions about equipment certification documents including "
            "test results, GD&T tolerances, compliance standards, material "
            "specifications, and certification details from source PDF reports"
        ),
        "agent_type": "ka",
        "serving_endpoint": {"name": f"ka-{ka_tile_id.split('-')[0]}-endpoint"},
    },
    {
        "name": "equipment_catalog_agent",
        "description": (
            "Answers data questions about equipment certifications, inventory, "
            "facilities, maintenance, and contracts using SQL against structured "
            "tables and metric views"
        ),
        "agent_type": "genie",
        "genie_space": {"id": genie_space_id},
    },
]

MAS_INSTRUCTIONS = (
    "You are a supervisor agent for UL Solutions equipment intelligence. "
    "Route questions as follows:\n\n"
    "1. Route to the equipment_docs_agent (Knowledge Assistant) when the user asks about:\n"
    "   - Specific certification report details (test results, measurements, thresholds)\n"
    "   - GD&T tolerances and engineering specifications\n"
    "   - Why a certification is conditional or what failed\n"
    "   - Material specifications and compliance standards from documents\n"
    "   - Specific equipment model details from certification reports\n\n"
    "2. Route to the equipment_catalog_agent (Genie Space) when the user asks about:\n"
    "   - Aggregate metrics (certification pass rates, counts, averages)\n"
    "   - Equipment inventory across facilities\n"
    "   - Facility-level questions (which equipment is at which plant)\n"
    "   - Maintenance and work order analytics\n"
    "   - Contract and warranty status\n"
    "   - Comparisons across manufacturers or equipment types\n\n"
    "If a question spans both domains, break it into parts and route appropriately."
)

MAS_DESCRIPTION = (
    "Multi-Agent Supervisor for UL Solutions equipment intelligence. Routes "
    "document-specific questions (test results, GD&T specs, compliance details) "
    "to the Knowledge Assistant and data/analytics questions (pass rates, "
    "inventory counts, facility metrics) to the Genie Space."
)

MAS_EXAMPLES = [
    {
        "question": "What is the certification pass rate by equipment type?",
        "guideline": "Route to equipment_catalog_agent — aggregate metric over structured data",
    },
    {
        "question": "Why does the ABB Power Solutions Power Distribution Unit have a conditional certification?",
        "guideline": "Route to equipment_docs_agent — requires reading the specific certification PDF",
    },
    {
        "question": "How many pieces of equipment are at the Chicago Manufacturing Complex?",
        "guideline": "Route to equipment_catalog_agent — inventory count against structured data",
    },
    {
        "question": "What are the GD&T tolerances for the mounting surfaces on model IMC-3456-B2?",
        "guideline": "Route to equipment_docs_agent — engineering specifications from the certification document",
    },
    {
        "question": "Which manufacturers have conditional certifications and what were the specific failures?",
        "guideline": "Route to both agents — catalog agent for manufacturer list, docs agent for failure details",
    },
]

# COMMAND ----------

existing_mas = find_mas_by_name(w, MAS_NAME)

if existing_mas:
    mas_tile_id = existing_mas["tile_id"]
    mas_details = get_mas(w, mas_tile_id)
    mas_status = (
        mas_details.get("multi_agent_supervisor", {})
        .get("status", {})
        .get("endpoint_status", "UNKNOWN")
    )
    print(f"Multi-Agent Supervisor already exists:")
    print(f"  Tile ID: {mas_tile_id}")
    print(f"  Name:    {MAS_NAME}")
    print(f"  Status:  {mas_status}")
else:
    print(f"Creating Multi-Agent Supervisor '{MAS_NAME}'...")
    mas_result = create_multi_agent_supervisor(
        w,
        name=MAS_NAME,
        agents=MAS_AGENTS,
        description=MAS_DESCRIPTION,
        instructions=MAS_INSTRUCTIONS,
    )

    mas_tile_id = (
        mas_result.get("multi_agent_supervisor", {})
        .get("tile", {})
        .get("tile_id")
    )
    mas_status = (
        mas_result.get("multi_agent_supervisor", {})
        .get("status", {})
        .get("endpoint_status", "PROVISIONING")
    )
    print(f"Multi-Agent Supervisor created:")
    print(f"  Tile ID: {mas_tile_id}")
    print(f"  Name:    {MAS_NAME}")
    print(f"  Status:  {mas_status}")

    print(f"\nAdding {len(MAS_EXAMPLES)} routing examples...")
    add_mas_examples(w, mas_tile_id, MAS_EXAMPLES)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("=" * 60)
print("UL Solutions Agent Bricks — Deployment Summary")
print("=" * 60)
print()
print(f"Knowledge Assistant: {KA_NAME}")
print(f"  Tile ID:     {ka_tile_id}")
print(f"  Volume Path: {VOLUME_PATH}")
print(f"  Purpose:     RAG over certification PDFs")
print()
print(f"Genie Space: {GENIE_SPACE_NAME}")
print(f"  Space ID:  {genie_space_id}")
print(f"  Purpose:   Natural language SQL over metric views")
print()
print(f"Multi-Agent Supervisor: {MAS_NAME}")
print(f"  Tile ID:  {mas_tile_id}")
print(f"  Agents:   equipment_docs_agent (KA), equipment_catalog_agent (Genie)")
print(f"  Purpose:  Intelligent routing between document Q&A and data analytics")
print()
print("Provisioning typically takes 2-5 minutes. Check status in the")
print("Databricks UI under AI Playground or Compound AI Systems.")
