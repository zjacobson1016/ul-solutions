# Databricks notebook source
# MAGIC %md
# MAGIC # UL Solutions - Generate Structured Operational Data
# MAGIC
# MAGIC Generates realistic structured data that would exist in an ERP / CMMS system:
# MAGIC - **Facilities**: Plant locations where equipment is deployed
# MAGIC - **Equipment Inventory**: Asset registry with facility assignments, purchase history, operational status
# MAGIC - **Work Orders**: Maintenance and service history for deployed equipment
# MAGIC - **Manufacturer Contracts**: Supplier agreements, SLAs, and contract terms
# MAGIC
# MAGIC This data joins with the AI-extracted certification data (from the unstructured PDF pipeline)
# MAGIC to create a 360-degree view of equipment across the organization.

# COMMAND ----------

dbutils.widgets.text("catalog", "mfg_mc_se_sa", "Catalog")
dbutils.widgets.text("schema", "ul_solutions", "Schema")
dbutils.widgets.text("volume_name", "raw_data", "Volume Name")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
volume_name = dbutils.widgets.get("volume_name")

volume_base = f"/Volumes/{catalog}/{schema}/{volume_name}/structured_data"

print(f"Catalog: {catalog}")
print(f"Schema:  {schema}")
print(f"Volume:  {volume_name}")
print(f"Output:  {volume_base}/")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Shared Reference Data
# MAGIC
# MAGIC These lists match the PDF generation notebook exactly so that structured and
# MAGIC unstructured data share the same manufacturers, equipment types, and model formats.

# COMMAND ----------

import random
import hashlib
from datetime import datetime, timedelta, date
from pyspark.sql.types import *
from pyspark.sql import functions as F

random.seed(42)

MANUFACTURERS = [
    "Siemens Industrial Systems", "ABB Power Solutions", "Schneider Electric",
    "Eaton Corporation", "Rockwell Automation", "Honeywell Process Solutions",
    "Emerson Electric", "GE Industrial", "Mitsubishi Electric", "Yokogawa Electric"
]

EQUIPMENT_TYPES = [
    ("Industrial Motor Controller", "IMC"),
    ("Power Distribution Unit", "PDU"),
    ("Programmable Logic Controller", "PLC"),
    ("Variable Frequency Drive", "VFD"),
    ("Circuit Breaker Assembly", "CBA"),
    ("Transformer Unit", "TRU"),
    ("Motor Starter Panel", "MSP"),
    ("Switchgear Assembly", "SGA"),
    ("Uninterruptible Power Supply", "UPS"),
    ("Industrial Relay Module", "IRM"),
]

VOLTAGES = ["120V AC", "240V AC", "480V AC", "24V DC", "48V DC", "600V AC"]
IP_RATINGS = ["IP20", "IP44", "IP54", "IP55", "IP65", "IP66", "IP67"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table 1: Facilities
# MAGIC
# MAGIC Manufacturing plants, distribution centers, and R&D labs where equipment is deployed.

# COMMAND ----------

facilities_data = [
    ("FAC-001", "Chicago Manufacturing Complex",   "Chicago",        "IL", "United States", "North America", "Manufacturing Plant",  285000, 1420, "1998-03-15"),
    ("FAC-002", "Houston Energy Center",           "Houston",        "TX", "United States", "North America", "Manufacturing Plant",  342000, 1850, "2002-07-22"),
    ("FAC-003", "Detroit Automation Hub",           "Detroit",        "MI", "United States", "North America", "Manufacturing Plant",  198000, 980,  "2005-11-01"),
    ("FAC-004", "Charlotte Distribution Center",   "Charlotte",      "NC", "United States", "North America", "Distribution Center",  156000, 420,  "2010-04-18"),
    ("FAC-005", "San Jose R&D Laboratory",         "San Jose",       "CA", "United States", "North America", "R&D Laboratory",       78000,  310,  "2012-09-30"),
    ("FAC-006", "Toronto Systems Integration",     "Toronto",        "ON", "Canada",        "North America", "Manufacturing Plant",  165000, 720,  "2008-01-10"),
    ("FAC-007", "Monterrey Assembly Plant",        "Monterrey",      "NL", "Mexico",        "North America", "Manufacturing Plant",  210000, 1100, "2015-06-25"),
    ("FAC-008", "Frankfurt European Operations",   "Frankfurt",      "HE", "Germany",       "EMEA",          "Manufacturing Plant",  230000, 1280, "2001-08-14"),
]

facilities_schema = StructType([
    StructField("facility_id",     StringType(), False),
    StructField("facility_name",   StringType(), False),
    StructField("city",            StringType(), False),
    StructField("state_province",  StringType(), False),
    StructField("country",         StringType(), False),
    StructField("region",          StringType(), False),
    StructField("facility_type",   StringType(), False),
    StructField("square_footage",  IntegerType(), False),
    StructField("employee_count",  IntegerType(), False),
    StructField("opened_date",     StringType(), False),
])

df_facilities = spark.createDataFrame(facilities_data, schema=facilities_schema) \
    .withColumn("opened_date", F.to_date("opened_date"))

facilities_path = f"{volume_base}/facilities"
df_facilities.write.mode("overwrite").parquet(facilities_path)
print(f"Wrote {df_facilities.count()} rows to {facilities_path}/")
df_facilities.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table 2: Equipment Inventory
# MAGIC
# MAGIC Asset registry spanning all facilities. Uses the same manufacturers, equipment types,
# MAGIC and model number format as the certification PDFs so the gold-layer join works.

# COMMAND ----------

def generate_model_number(prefix):
    return f"{prefix}-{random.randint(1000, 9999)}-{random.choice('ABCDEFGH')}{random.randint(1, 9)}"

def generate_serial(manufacturer, idx):
    mfr_code = "".join(w[0] for w in manufacturer.split()[:2]).upper()
    return f"{mfr_code}-{random.randint(2020, 2026)}-{idx:06d}"

facility_ids = [f[0] for f in facilities_data]

# Weight by facility size — larger plants have more equipment
facility_weights = {
    "FAC-001": 20, "FAC-002": 24, "FAC-003": 14, "FAC-004": 8,
    "FAC-005": 5,  "FAC-006": 10, "FAC-007": 15, "FAC-008": 18,
}

INSTALL_LOCATIONS = [
    "Production Line 1", "Production Line 2", "Production Line 3",
    "Assembly Area A", "Assembly Area B", "Utility Room",
    "Control Room", "Shipping Dock", "Quality Lab",
    "Motor Control Center", "Substation", "Roof Mechanical",
    "Maintenance Shop", "Boiler Room", "Compressor Room"
]

OPERATIONAL_STATUSES = ["Active", "Active", "Active", "Active", "Active",
                         "Under Maintenance", "Standby", "Decommissioned"]

inventory_rows = []
for i in range(120):
    eq_type_name, eq_prefix = random.choice(EQUIPMENT_TYPES)
    manufacturer = random.choice(MANUFACTURERS)
    model = generate_model_number(eq_prefix)
    fac_id = random.choices(facility_ids, weights=[facility_weights[f] for f in facility_ids])[0]

    purchase_date = date(2018, 1, 1) + timedelta(days=random.randint(0, 2800))
    warranty_years = random.choice([1, 2, 3, 5])
    warranty_exp = purchase_date + timedelta(days=warranty_years * 365)
    last_inspection = purchase_date + timedelta(days=random.randint(30, min(2800, (date(2026, 2, 17) - purchase_date).days)))
    next_inspection = last_inspection + timedelta(days=random.choice([90, 180, 365]))

    # Price varies by equipment type — transformers and switchgear are expensive
    base_prices = {
        "TRU": (18000, 85000), "SGA": (25000, 120000), "PDU": (8000, 35000),
        "VFD": (3000, 22000),  "PLC": (2000, 15000),   "UPS": (5000, 40000),
        "CBA": (1500, 12000),  "IMC": (2500, 18000),   "MSP": (4000, 25000),
        "IRM": (800, 6000),
    }
    price_range = base_prices.get(eq_prefix, (2000, 20000))
    purchase_price = round(random.uniform(*price_range), 2)

    voltage = random.choice(VOLTAGES)
    ip_rating = random.choice(IP_RATINGS)
    op_status = random.choice(OPERATIONAL_STATUSES)

    inventory_rows.append((
        f"AST-{i+1:06d}",
        model,
        eq_type_name,
        eq_prefix,
        manufacturer,
        fac_id,
        generate_serial(manufacturer, i+1),
        str(purchase_date),
        purchase_price,
        str(warranty_exp),
        op_status,
        voltage,
        ip_rating,
        str(last_inspection),
        str(next_inspection),
        random.choice(INSTALL_LOCATIONS),
    ))

inventory_schema = StructType([
    StructField("asset_id",              StringType(), False),
    StructField("model_number",          StringType(), False),
    StructField("equipment_type",        StringType(), False),
    StructField("equipment_type_code",   StringType(), False),
    StructField("manufacturer",          StringType(), False),
    StructField("facility_id",           StringType(), False),
    StructField("serial_number",         StringType(), False),
    StructField("purchase_date",         StringType(), False),
    StructField("purchase_price_usd",    DoubleType(), False),
    StructField("warranty_expiration",   StringType(), False),
    StructField("operational_status",    StringType(), False),
    StructField("voltage_rating",        StringType(), False),
    StructField("ip_rating",             StringType(), False),
    StructField("last_inspection_date",  StringType(), False),
    StructField("next_inspection_due",   StringType(), False),
    StructField("install_location",      StringType(), False),
])

df_inventory = spark.createDataFrame(inventory_rows, schema=inventory_schema) \
    .withColumn("purchase_date",        F.to_date("purchase_date")) \
    .withColumn("warranty_expiration",  F.to_date("warranty_expiration")) \
    .withColumn("last_inspection_date", F.to_date("last_inspection_date")) \
    .withColumn("next_inspection_due",  F.to_date("next_inspection_due"))

inventory_path = f"{volume_base}/equipment_inventory"
df_inventory.write.mode("overwrite").parquet(inventory_path)
print(f"Wrote {df_inventory.count()} rows to {inventory_path}/")
df_inventory.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table 3: Work Orders
# MAGIC
# MAGIC Maintenance and service history. Linked to equipment inventory via `asset_id`.
# MAGIC Includes preventive maintenance, corrective repairs, inspections, and calibrations.

# COMMAND ----------

WORK_ORDER_TYPES = [
    "Preventive Maintenance", "Preventive Maintenance", "Preventive Maintenance",
    "Corrective Repair", "Corrective Repair",
    "Inspection", "Inspection",
    "Calibration",
    "Emergency Repair",
]

PRIORITIES = ["Critical", "High", "High", "Medium", "Medium", "Medium", "Low", "Low"]
WO_STATUSES = ["Completed", "Completed", "Completed", "Completed",
               "In Progress", "Open", "Cancelled"]

TECHNICIANS = [
    "Mike Rodriguez", "Sarah Chen", "James Kowalski", "Lisa Patel",
    "Carlos Mendez", "Emily Thornton", "David Kim", "Amanda Foster",
    "Robert Okonkwo", "Jennifer Wu", "Thomas Schmidt", "Maria Gonzalez",
    "Kevin O'Brien", "Rachel Nakamura", "Antonio Rossi"
]

WORK_DESCRIPTIONS = {
    "Preventive Maintenance": [
        "Quarterly PM — lubrication, filter replacement, visual inspection of connections",
        "Annual preventive maintenance — full teardown, bearing inspection, torque checks",
        "Semi-annual PM — thermal scan, vibration analysis, firmware check",
        "Monthly PM — clean enclosure, check indicator lights, verify alarm functions",
        "Scheduled maintenance — replace cooling fans, clean heat sinks, test interlocks",
    ],
    "Corrective Repair": [
        "Replaced failed contactor — intermittent tripping reported by operators",
        "Repaired communication module — Modbus timeout errors on SCADA",
        "Replaced blown fuse and inspected for root cause — overcurrent event logged",
        "Fixed overheating issue — replaced thermal paste and cleaned air vents",
        "Replaced damaged display panel — cracked during adjacent equipment installation",
    ],
    "Inspection": [
        "Annual safety inspection per NFPA 70E — all clearances verified",
        "Quarterly thermographic inspection — no hot spots detected",
        "Pre-shutdown inspection — verified isolation points and lockout procedures",
        "Insurance compliance inspection — all labels and markings verified current",
    ],
    "Calibration": [
        "Annual calibration of current transformers — within ±0.5% tolerance",
        "Recalibrated analog outputs — drift detected during routine checks",
        "Temperature sensor calibration — 3-point verification against NIST standard",
    ],
    "Emergency Repair": [
        "Emergency callout — unit tripped on ground fault, production line down",
        "Emergency repair — power supply failure causing cascading alarms",
        "Emergency response — smoke detected from enclosure, isolated and inspected",
    ],
}

asset_ids = [row[0] for row in inventory_rows]

work_order_rows = []
for i in range(500):
    asset_id = random.choice(asset_ids)
    wo_type = random.choice(WORK_ORDER_TYPES)
    priority = random.choice(PRIORITIES)
    if wo_type == "Emergency Repair":
        priority = random.choice(["Critical", "Critical", "High"])

    status = random.choice(WO_STATUSES)

    created = datetime(2024, 1, 1) + timedelta(
        days=random.randint(0, 775),
        hours=random.randint(6, 18),
        minutes=random.randint(0, 59),
    )

    scheduled = (created + timedelta(days=random.randint(0, 14))).date()
    if wo_type == "Emergency Repair":
        scheduled = created.date()

    completed_date = None
    labor_hours = None
    parts_cost = None
    downtime_hours = None

    if status == "Completed":
        completed_date = str(scheduled + timedelta(days=random.randint(0, 3)))
        labor_hours = round(random.uniform(0.5, 16.0), 1)
        parts_cost = round(random.uniform(0, 4500), 2) if wo_type in ("Corrective Repair", "Emergency Repair", "Preventive Maintenance") else 0.0
        downtime_hours = round(random.uniform(0, 8.0), 1) if wo_type in ("Corrective Repair", "Emergency Repair") else 0.0
    elif status == "In Progress":
        labor_hours = round(random.uniform(0.5, 4.0), 1)

    description = random.choice(WORK_DESCRIPTIONS.get(wo_type, ["General maintenance activity"]))

    work_order_rows.append((
        f"WO-{i+1:06d}",
        asset_id,
        wo_type,
        priority,
        status,
        str(created),
        str(scheduled),
        completed_date,
        random.choice(TECHNICIANS),
        description,
        labor_hours,
        parts_cost,
        downtime_hours,
    ))

wo_schema = StructType([
    StructField("work_order_id",    StringType(), False),
    StructField("asset_id",         StringType(), False),
    StructField("work_order_type",  StringType(), False),
    StructField("priority",         StringType(), False),
    StructField("status",           StringType(), False),
    StructField("created_at",       StringType(), False),
    StructField("scheduled_date",   StringType(), False),
    StructField("completed_date",   StringType(), True),
    StructField("technician_name",  StringType(), False),
    StructField("description",      StringType(), False),
    StructField("labor_hours",      DoubleType(), True),
    StructField("parts_cost_usd",   DoubleType(), True),
    StructField("downtime_hours",   DoubleType(), True),
])

df_work_orders = spark.createDataFrame(work_order_rows, schema=wo_schema) \
    .withColumn("created_at",     F.to_timestamp("created_at")) \
    .withColumn("scheduled_date", F.to_date("scheduled_date")) \
    .withColumn("completed_date", F.to_date("completed_date"))

work_orders_path = f"{volume_base}/work_orders"
df_work_orders.write.mode("overwrite").parquet(work_orders_path)
print(f"Wrote {df_work_orders.count()} rows to {work_orders_path}/")
df_work_orders.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table 4: Manufacturer Contracts
# MAGIC
# MAGIC Supplier agreements covering service, parts supply, and extended warranties.
# MAGIC One contract per manufacturer, using the same manufacturer names as the certification PDFs.

# COMMAND ----------

CONTRACT_TYPES = ["Service Agreement", "Parts Supply", "Extended Warranty"]

contracts_data = []
for i, manufacturer in enumerate(MANUFACTURERS):
    contract_type = random.choice(CONTRACT_TYPES)
    start = date(2023, 1, 1) + timedelta(days=random.randint(0, 365))
    duration_years = random.choice([1, 2, 3, 5])
    end = start + timedelta(days=duration_years * 365)

    today = date(2026, 2, 17)
    if end < today:
        status = "Expired"
    elif (end - today).days < 90:
        status = "Expiring Soon"
    else:
        status = "Active"

    annual_value = round(random.uniform(25000, 350000), 2)
    sla_hours = random.choice([2, 4, 4, 8, 8, 24])

    first_names = ["John", "Maria", "David", "Sarah", "Kenji", "Hans", "Priya", "Wei", "Carlos", "Anna"]
    last_names = ["Mueller", "Santos", "Park", "Williams", "Tanaka", "Fischer", "Sharma", "Chang", "Rodriguez", "Johansson"]
    contact_name = f"{first_names[i]} {last_names[i]}"
    domain = manufacturer.lower().replace(" ", "").replace(".", "")[:12]
    contact_email = f"{contact_name.lower().replace(' ', '.')}@{domain}.com"

    contracts_data.append((
        f"CTR-{i+1:04d}",
        manufacturer,
        contract_type,
        str(start),
        str(end),
        annual_value,
        sla_hours,
        status,
        contact_name,
        contact_email,
    ))

contracts_schema = StructType([
    StructField("contract_id",       StringType(), False),
    StructField("manufacturer",      StringType(), False),
    StructField("contract_type",     StringType(), False),
    StructField("start_date",        StringType(), False),
    StructField("end_date",          StringType(), False),
    StructField("annual_value_usd",  DoubleType(), False),
    StructField("sla_response_hours", IntegerType(), False),
    StructField("contract_status",   StringType(), False),
    StructField("primary_contact",   StringType(), False),
    StructField("contact_email",     StringType(), False),
])

df_contracts = spark.createDataFrame(contracts_data, schema=contracts_schema) \
    .withColumn("start_date", F.to_date("start_date")) \
    .withColumn("end_date",   F.to_date("end_date"))

contracts_path = f"{volume_base}/manufacturer_contracts"
df_contracts.write.mode("overwrite").parquet(contracts_path)
print(f"Wrote {df_contracts.count()} rows to {contracts_path}/")
df_contracts.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC Four structured operational tables created:

# COMMAND ----------

tables = ["facilities", "equipment_inventory", "work_orders", "manufacturer_contracts"]
for t in tables:
    path = f"{volume_base}/{t}"
    count = spark.read.parquet(path).count()
    print(f"  {path}: {count:,} rows")

print(f"\nParquet files written to {volume_base}/")
print("The SDP pipeline will ingest these via read_files() into streaming tables,")
print("then join with AI-extracted gold_equipment_catalog for a 360-degree equipment view.")
