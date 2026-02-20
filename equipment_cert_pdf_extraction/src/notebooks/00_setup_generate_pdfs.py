# Databricks notebook source
# MAGIC %md
# MAGIC # UL Solutions - Generate & Upload Equipment Certification PDFs
# MAGIC
# MAGIC This notebook:
# MAGIC 1. Creates the UC schema and volume (if they don't exist)
# MAGIC 2. Generates synthetic industrial equipment certification PDFs using reportlab
# MAGIC 3. Uploads them to the UC Volume for pipeline ingestion

# COMMAND ----------

dbutils.widgets.text("catalog", "mfg_mc_se_sa", "Catalog")
dbutils.widgets.text("schema", "ul_solutions", "Schema")
dbutils.widgets.text("volume_name", "raw_data", "Volume Name")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
volume_name = dbutils.widgets.get("volume_name")

print(f"Catalog:  {catalog}")
print(f"Schema:   {schema}")
print(f"Volume:   {volume_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Create Schema and Volume

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.{volume_name}")

volume_path = f"/Volumes/{catalog}/{schema}/{volume_name}/equipment_docs"
dbutils.fs.mkdirs(volume_path)
print(f"Volume path ready: {volume_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Generate Synthetic Equipment Certification PDFs

# COMMAND ----------

import os
import io
import random
import math
from pathlib import Path

from reportlab.lib.pagesizes import letter
from reportlab.lib.units import inch
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
    PageBreak, HRFlowable
)
from reportlab.graphics.shapes import Drawing, Rect, Circle, Line, String
from reportlab.lib.enums import TA_CENTER

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
NUM_DOCS = 10

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

MATERIALS = [
    "Aluminum 6061-T6", "Stainless Steel 304", "Carbon Steel ASTM A36",
    "Copper C11000", "Brass C36000", "Polycarbonate (Lexan)",
    "Glass-Filled Nylon (PA66-GF30)", "HDPE", "Fiberglass FR-4", "Cast Iron Class 30"
]

SAFETY_RATINGS = ["UL 508A", "UL 891", "UL 1558", "UL 67", "UL 489", "UL 508", "UL 61010-1"]
IP_RATINGS = ["IP20", "IP44", "IP54", "IP55", "IP65", "IP66", "IP67"]
COMPLIANCE_STANDARDS = [
    "UL", "CSA", "IEC 61439", "NEMA 250", "IEEE C37",
    "NEC Article 409", "EN 60204-1", "ISO 13849-1"
]

GDT_TOLERANCE_TYPES = [
    ("Flatness", "⏥"), ("Straightness", "—"), ("Circularity", "○"),
    ("Cylindricity", "⌭"), ("Position", "⊕"), ("Concentricity", "◎"),
    ("Symmetry", "⌯"), ("Parallelism", "∥"), ("Perpendicularity", "⊥"),
    ("Angularity", "∠"), ("Total Runout", "↗↗"), ("Profile of Surface", "⌓")
]

DATUM_REFS = ["A", "B", "C", "A|B", "A|B|C", "B|C"]

TEST_NAMES = [
    "Dielectric Withstand Test", "Insulation Resistance Test",
    "Ground Continuity Test", "Short-Circuit Current Rating Test",
    "Temperature Rise Test", "Overload Protection Test",
    "Environmental Stress Screening", "Vibration Endurance Test",
    "Salt Spray Corrosion Test", "Humidity Resistance Test",
    "Thermal Shock Test", "EMC Immunity Test",
    "Arc Flash Hazard Test", "IP Enclosure Integrity Test"
]


def random_model_number(prefix):
    return f"{prefix}-{random.randint(1000, 9999)}-{random.choice('ABCDEFGH')}{random.randint(1, 9)}"


def random_cert_id():
    return f"UL-{random.randint(2024, 2026)}-{random.randint(100000, 999999)}"


def random_tolerance():
    val = round(random.uniform(0.001, 0.250), 3)
    return f"±{val} mm"


def create_gdt_drawing(width=400, height=250):
    d = Drawing(width, height)
    d.add(Rect(0, 0, width, height, fillColor=colors.white, strokeColor=colors.black, strokeWidth=1))
    cx, cy = width / 2, height / 2
    d.add(Rect(cx - 120, cy - 60, 240, 120, fillColor=colors.Color(0.92, 0.92, 0.95),
               strokeColor=colors.black, strokeWidth=1.5))
    d.add(Rect(cx - 80, cy - 35, 160, 70, fillColor=colors.white,
               strokeColor=colors.black, strokeWidth=1))
    for dx, dy in [(-100, -45), (100, -45), (-100, 45), (100, 45)]:
        d.add(Circle(cx + dx, cy + dy, 8, fillColor=colors.Color(0.85, 0.85, 0.85),
                     strokeColor=colors.black, strokeWidth=1))
    d.add(Line(cx - 120, cy + 80, cx + 120, cy + 80, strokeColor=colors.blue, strokeWidth=0.5))
    d.add(Line(cx - 120, cy + 60, cx - 120, cy + 85, strokeColor=colors.blue, strokeWidth=0.5))
    d.add(Line(cx + 120, cy + 60, cx + 120, cy + 85, strokeColor=colors.blue, strokeWidth=0.5))
    d.add(String(cx - 15, cy + 83, "240.00", fontSize=7, fillColor=colors.blue))
    d.add(Line(cx + 140, cy - 60, cx + 140, cy + 60, strokeColor=colors.blue, strokeWidth=0.5))
    d.add(Line(cx + 120, cy - 60, cx + 145, cy - 60, strokeColor=colors.blue, strokeWidth=0.5))
    d.add(Line(cx + 120, cy + 60, cx + 145, cy + 60, strokeColor=colors.blue, strokeWidth=0.5))
    d.add(String(cx + 143, cy - 3, "120.00", fontSize=7, fillColor=colors.blue, textAnchor="start"))
    d.add(Rect(cx - 130, cy - 95, 110, 25, fillColor=colors.white, strokeColor=colors.black, strokeWidth=1))
    d.add(Line(cx - 130, cy - 95, cx - 130, cy - 70, strokeColor=colors.black))
    d.add(Line(cx - 95, cy - 95, cx - 95, cy - 70, strokeColor=colors.black))
    d.add(Line(cx - 60, cy - 95, cx - 60, cy - 70, strokeColor=colors.black))
    d.add(String(cx - 125, cy - 88, "⊕", fontSize=10, fillColor=colors.black))
    d.add(String(cx - 90, cy - 88, "0.05", fontSize=7, fillColor=colors.black))
    d.add(String(cx - 55, cy - 88, "A | B", fontSize=7, fillColor=colors.black))
    d.add(String(cx - 135, cy + 60, "▼ A", fontSize=8, fillColor=colors.red))
    d.add(String(cx + 125, cy - 60, "▼ B", fontSize=8, fillColor=colors.red))
    d.add(Rect(10, 5, width - 20, 20, fillColor=colors.Color(0.95, 0.95, 0.95),
               strokeColor=colors.black, strokeWidth=0.5))
    d.add(String(15, 10, "CROSS SECTION VIEW — ALL DIMENSIONS IN MM — THIRD ANGLE PROJECTION",
                 fontSize=6, fillColor=colors.black))
    return d


def create_equipment_diagram(width=400, height=200, eq_type=""):
    d = Drawing(width, height)
    d.add(Rect(0, 0, width, height, fillColor=colors.white, strokeColor=colors.black, strokeWidth=1))
    d.add(Rect(50, 30, 300, 140, fillColor=colors.Color(0.9, 0.93, 0.96),
               strokeColor=colors.black, strokeWidth=2, rx=5, ry=5))
    d.add(Rect(70, 100, 80, 50, fillColor=colors.Color(0.7, 0.85, 0.7),
               strokeColor=colors.black, strokeWidth=1))
    d.add(String(80, 120, "POWER", fontSize=8, fillColor=colors.black))
    d.add(String(80, 108, "MODULE", fontSize=8, fillColor=colors.black))
    d.add(Rect(170, 100, 80, 50, fillColor=colors.Color(0.7, 0.75, 0.9),
               strokeColor=colors.black, strokeWidth=1))
    d.add(String(180, 120, "CONTROL", fontSize=8, fillColor=colors.black))
    d.add(String(180, 108, "LOGIC", fontSize=8, fillColor=colors.black))
    d.add(Rect(270, 100, 60, 50, fillColor=colors.Color(0.9, 0.8, 0.7),
               strokeColor=colors.black, strokeWidth=1))
    d.add(String(275, 120, "I/O", fontSize=8, fillColor=colors.black))
    d.add(String(275, 108, "PANEL", fontSize=8, fillColor=colors.black))
    d.add(Line(150, 125, 170, 125, strokeColor=colors.red, strokeWidth=1.5))
    d.add(Line(250, 125, 270, 125, strokeColor=colors.red, strokeWidth=1.5))
    for i in range(4):
        d.add(Circle(80 + i * 25, 55, 5,
                     fillColor=random.choice([colors.green, colors.red, colors.yellow]),
                     strokeColor=colors.black, strokeWidth=0.5))
    d.add(String(60, 160, eq_type, fontSize=9, fillColor=colors.black))
    d.add(String(60, 37, "STATUS INDICATORS", fontSize=6, fillColor=colors.gray))
    return d


def generate_equipment_pdf(doc_index):
    eq_type_name, eq_prefix = random.choice(EQUIPMENT_TYPES)
    manufacturer = random.choice(MANUFACTURERS)
    model = random_model_number(eq_prefix)
    cert_id = random_cert_id()
    material = random.choice(MATERIALS)
    safety = random.choice(SAFETY_RATINGS)
    ip = random.choice(IP_RATINGS)
    standards = ", ".join(random.sample(COMPLIANCE_STANDARDS, k=random.randint(3, 5)))
    weight = round(random.uniform(2.5, 150.0), 1)
    voltage = random.choice(["120V AC", "240V AC", "480V AC", "24V DC", "48V DC", "600V AC"])
    temp_min = random.choice([-40, -25, -20, -10, 0])
    temp_max = random.choice([50, 55, 60, 70, 85])
    cert_status = random.choices(["PASS", "CONDITIONAL", "PASS", "PASS"], weights=[6, 2, 1, 1])[0]

    filename = f"UL_Cert_{cert_id.replace('-', '_')}_{model.replace('-', '_')}.pdf"

    pdf_buffer = io.BytesIO()
    doc = SimpleDocTemplate(
        pdf_buffer, pagesize=letter,
        leftMargin=0.75 * inch, rightMargin=0.75 * inch,
        topMargin=0.75 * inch, bottomMargin=0.75 * inch
    )

    styles = getSampleStyleSheet()
    title_style = ParagraphStyle('Title2', parent=styles['Title'], fontSize=18, spaceAfter=6,
                                  textColor=colors.HexColor('#1a3a5c'))
    subtitle_style = ParagraphStyle('Subtitle2', parent=styles['Normal'], fontSize=12, spaceAfter=12,
                                     textColor=colors.HexColor('#4a6a8c'), alignment=TA_CENTER)
    heading_style = ParagraphStyle('Heading2', parent=styles['Heading2'], fontSize=14, spaceBefore=16,
                                    spaceAfter=8, textColor=colors.HexColor('#1a3a5c'))
    body_style = ParagraphStyle('Body2', parent=styles['Normal'], fontSize=10, spaceAfter=8, leading=14)
    small_style = ParagraphStyle('Small', parent=styles['Normal'], fontSize=8, textColor=colors.gray)

    story = []

    # Page 1: Cover & General Info
    story.append(Spacer(1, 0.3 * inch))
    story.append(Paragraph("UL SOLUTIONS", ParagraphStyle('UL', parent=styles['Title'],
                            fontSize=28, textColor=colors.HexColor('#c41230'), alignment=TA_CENTER)))
    story.append(Paragraph("Equipment Certification Report", title_style))
    story.append(HRFlowable(width="100%", thickness=2, color=colors.HexColor('#c41230')))
    story.append(Spacer(1, 0.2 * inch))
    story.append(Paragraph(f"Certification ID: <b>{cert_id}</b>", subtitle_style))
    story.append(Paragraph(f"{eq_type_name} — Model {model}", subtitle_style))
    story.append(Paragraph(f"Manufacturer: {manufacturer}", subtitle_style))
    story.append(Spacer(1, 0.3 * inch))

    general_data = [
        ["Field", "Value"],
        ["Equipment Type", eq_type_name],
        ["Model Number", model],
        ["Manufacturer", manufacturer],
        ["Certification ID", cert_id],
        ["Certification Status", cert_status],
        ["Safety Standard", safety],
        ["IP Rating", ip],
        ["Compliance Standards", standards],
        ["Material (Primary)", material],
        ["Weight", f"{weight} kg"],
        ["Voltage Rating", voltage],
        ["Operating Temp Range", f"{temp_min}°C to {temp_max}°C"],
        ["Report Date", f"2026-{random.randint(1,2):02d}-{random.randint(1,28):02d}"],
    ]
    t = Table(general_data, colWidths=[2.5 * inch, 4.5 * inch])
    t.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#1a3a5c')),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, -1), 9),
        ('BACKGROUND', (0, 1), (-1, -1), colors.HexColor('#f5f7fa')),
        ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor('#cccccc')),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('LEFTPADDING', (0, 0), (-1, -1), 8),
        ('RIGHTPADDING', (0, 0), (-1, -1), 8),
        ('TOPPADDING', (0, 0), (-1, -1), 5),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
    ]))
    story.append(t)
    story.append(PageBreak())

    # Page 2: Equipment Diagram & GD&T Drawing
    story.append(Paragraph("2. Equipment Overview & Engineering Drawings", heading_style))
    story.append(Paragraph(
        f"The {eq_type_name} model {model} manufactured by {manufacturer} is designed for "
        f"industrial applications requiring {voltage} power supply with {ip} environmental protection. "
        f"The unit is constructed primarily from {material} with a total weight of {weight} kg.",
        body_style
    ))
    story.append(Spacer(1, 0.15 * inch))
    story.append(Paragraph("<b>Figure 1: Equipment Block Diagram</b>", body_style))
    story.append(create_equipment_diagram(450, 180, eq_type_name))
    story.append(Spacer(1, 0.2 * inch))
    story.append(Paragraph("<b>Figure 2: GD&T Cross-Section Drawing</b>", body_style))
    story.append(create_gdt_drawing(450, 230))
    story.append(Spacer(1, 0.15 * inch))
    story.append(Paragraph(
        "All dimensions are in millimeters per ASME Y14.5-2018 standard. "
        "Third-angle projection is used throughout.",
        body_style
    ))
    story.append(PageBreak())

    # Page 3: GD&T Specifications
    story.append(Paragraph("3. Geometric Dimensioning & Tolerancing (GD&T)", heading_style))
    story.append(Paragraph(
        "The following GD&T specifications apply to critical features of the equipment "
        "housing and mounting interfaces per ASME Y14.5-2018.",
        body_style
    ))

    num_gdt = random.randint(6, 10)
    gdt_data = [["Feature", "Tolerance Type", "Symbol", "Value", "Datum Ref", "Status"]]
    features = [
        "Mounting Surface A", "Bore Diameter", "Shaft Centerline", "Face Plate",
        "Terminal Block Surface", "Conduit Hub", "DIN Rail Channel",
        "Heat Sink Fins", "Cover Plate", "Gasket Groove", "Cable Entry Port",
        "Enclosure Corner Radius"
    ]
    for _ in range(num_gdt):
        tol_type, symbol = random.choice(GDT_TOLERANCE_TYPES)
        status = random.choices(["PASS", "FAIL"], weights=[9, 1])[0]
        gdt_data.append([
            random.choice(features), tol_type, symbol,
            random_tolerance(), random.choice(DATUM_REFS), status
        ])

    gt = Table(gdt_data, colWidths=[1.5 * inch, 1.2 * inch, 0.6 * inch, 1.0 * inch, 0.8 * inch, 0.7 * inch])
    gt.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#1a3a5c')),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, -1), 8),
        ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor('#cccccc')),
        ('BACKGROUND', (0, 1), (-1, -1), colors.HexColor('#f5f7fa')),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('ALIGN', (2, 0), (2, -1), 'CENTER'),
        ('ALIGN', (5, 0), (5, -1), 'CENTER'),
        ('LEFTPADDING', (0, 0), (-1, -1), 6),
        ('TOPPADDING', (0, 0), (-1, -1), 4),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 4),
    ]))
    for row_idx in range(1, len(gdt_data)):
        if gdt_data[row_idx][5] == "FAIL":
            gt.setStyle(TableStyle([
                ('TEXTCOLOR', (5, row_idx), (5, row_idx), colors.red),
                ('FONTNAME', (5, row_idx), (5, row_idx), 'Helvetica-Bold'),
            ]))
    story.append(gt)
    story.append(Spacer(1, 0.2 * inch))

    # Material Specifications
    story.append(Paragraph("4. Material Specifications", heading_style))
    num_mats = random.randint(3, 6)
    mat_data = [["Component", "Material", "Grade/Spec", "Thickness (mm)", "Hardness"]]
    components = [
        "Enclosure Body", "Cover Plate", "Terminal Block", "Mounting Bracket",
        "Heat Sink", "Bus Bar", "DIN Rail", "Gasket", "Cable Gland", "Internal Bracket"
    ]
    for _ in range(num_mats):
        mat_data.append([
            random.choice(components), random.choice(MATERIALS),
            f"ASTM {random.choice(['A36', 'A572', 'B209', 'B152', 'D3935'])}",
            f"{round(random.uniform(0.5, 12.0), 1)}",
            f"{random.randint(40, 95)} HRB"
        ])
    mt = Table(mat_data, colWidths=[1.4 * inch, 1.5 * inch, 1.2 * inch, 1.1 * inch, 0.8 * inch])
    mt.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#1a3a5c')),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, -1), 8),
        ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor('#cccccc')),
        ('BACKGROUND', (0, 1), (-1, -1), colors.HexColor('#f5f7fa')),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('LEFTPADDING', (0, 0), (-1, -1), 6),
        ('TOPPADDING', (0, 0), (-1, -1), 4),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 4),
    ]))
    story.append(mt)
    story.append(PageBreak())

    # Page 4: Test Results
    story.append(Paragraph("5. Certification Test Results", heading_style))
    story.append(Paragraph(
        f"The following tests were conducted in accordance with {safety} and "
        f"applicable {standards} standards.",
        body_style
    ))

    num_tests = random.randint(7, 12)
    test_data = [["Test Name", "Measured Value", "Threshold", "Result"]]
    selected_tests = random.sample(TEST_NAMES, k=min(num_tests, len(TEST_NAMES)))
    for test_name in selected_tests:
        if "Dielectric" in test_name:
            measured, threshold = f"{random.randint(1800, 2500)} VAC", "≥ 1500 VAC"
        elif "Insulation" in test_name:
            measured, threshold = f"{random.randint(80, 500)} MΩ", "≥ 50 MΩ"
        elif "Ground" in test_name:
            measured, threshold = f"{round(random.uniform(0.01, 0.15), 3)} Ω", "≤ 0.1 Ω"
        elif "Temperature" in test_name:
            measured = f"{random.randint(35, 75)}°C rise"
            threshold = f"≤ {random.choice([65, 70, 75])}°C rise"
        elif "Vibration" in test_name:
            measured = f"{round(random.uniform(0.5, 5.0), 1)}g @ {random.randint(10, 500)} Hz"
            threshold = f"≤ {round(random.uniform(3.0, 10.0), 1)}g"
        elif "Salt" in test_name:
            measured = f"{random.randint(200, 1000)} hours"
            threshold = f"≥ {random.choice([200, 500, 720])} hours"
        elif "Humidity" in test_name:
            measured = f"{random.randint(85, 98)}% RH, {random.randint(24, 168)}h"
            threshold = "95% RH, 48h min"
        elif "Short-Circuit" in test_name:
            measured = f"{random.randint(10, 100)} kA"
            threshold = f"≥ {random.choice([10, 25, 50])} kA"
        elif "Thermal Shock" in test_name:
            measured = f"-{random.randint(20, 40)}°C to +{random.randint(60, 85)}°C"
            threshold = f"-40°C to +{random.choice([70, 85])}°C"
        else:
            measured = f"{round(random.uniform(0.1, 100.0), 2)} units"
            threshold = f"≤ {round(random.uniform(50, 200), 1)} units"
        result = random.choices(["PASS", "FAIL"], weights=[9, 1])[0]
        test_data.append([test_name, measured, threshold, result])

    tt = Table(test_data, colWidths=[2.2 * inch, 1.5 * inch, 1.5 * inch, 0.8 * inch])
    tt.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#1a3a5c')),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, -1), 9),
        ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor('#cccccc')),
        ('BACKGROUND', (0, 1), (-1, -1), colors.HexColor('#f5f7fa')),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('ALIGN', (3, 0), (3, -1), 'CENTER'),
        ('LEFTPADDING', (0, 0), (-1, -1), 6),
        ('TOPPADDING', (0, 0), (-1, -1), 4),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 4),
    ]))
    for row_idx in range(1, len(test_data)):
        if test_data[row_idx][3] == "FAIL":
            tt.setStyle(TableStyle([
                ('TEXTCOLOR', (3, row_idx), (3, row_idx), colors.red),
                ('FONTNAME', (3, row_idx), (3, row_idx), 'Helvetica-Bold'),
            ]))
        else:
            tt.setStyle(TableStyle([
                ('TEXTCOLOR', (3, row_idx), (3, row_idx), colors.HexColor('#228B22')),
            ]))
    story.append(tt)
    story.append(Spacer(1, 0.3 * inch))

    # Summary
    total_tests = len(test_data) - 1
    passed = sum(1 for row in test_data[1:] if row[3] == "PASS")
    failed = total_tests - passed

    story.append(Paragraph("6. Certification Summary", heading_style))
    story.append(Paragraph(
        f"<b>Overall Status: {cert_status}</b><br/>"
        f"Total Tests Performed: {total_tests}<br/>"
        f"Tests Passed: {passed}<br/>"
        f"Tests Failed: {failed}<br/>"
        f"Pass Rate: {round(passed / total_tests * 100, 1)}%",
        body_style
    ))
    story.append(Spacer(1, 0.15 * inch))

    if cert_status == "PASS":
        story.append(Paragraph(
            f"Based on the test results, the {eq_type_name} model {model} manufactured by "
            f"{manufacturer} meets all requirements of {safety} and is hereby certified "
            f"for use in industrial environments rated up to {ip}.",
            body_style
        ))
    elif cert_status == "CONDITIONAL":
        story.append(Paragraph(
            f"The {eq_type_name} model {model} has received conditional certification pending "
            f"resolution of {failed} failed test(s). The manufacturer must submit corrective "
            f"action documentation within 90 days.",
            body_style
        ))

    story.append(Spacer(1, 0.3 * inch))
    story.append(HRFlowable(width="100%", thickness=1, color=colors.HexColor('#c41230')))
    story.append(Paragraph(
        "UL Solutions — Confidential Certification Document — Unauthorized reproduction prohibited",
        small_style
    ))

    doc.build(story)
    pdf_buffer.seek(0)
    return {"filename": filename, "pdf_bytes": pdf_buffer.getvalue(),
            "equipment_name": eq_type_name, "model_number": model,
            "manufacturer": manufacturer, "certification_id": cert_id,
            "certification_status": cert_status}


# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Generate PDFs and Upload to Volume

# COMMAND ----------

volume_dest = f"/Volumes/{catalog}/{schema}/{volume_name}/equipment_docs"
print(f"Generating {NUM_DOCS} equipment certification PDFs directly to {volume_dest}/...")

docs = []
for i in range(NUM_DOCS):
    meta = generate_equipment_pdf(i)
    dest_path = f"{volume_dest}/{meta['filename']}"
    with open(dest_path, "wb") as f:
        f.write(meta["pdf_bytes"])
    del meta["pdf_bytes"]
    docs.append(meta)
    print(f"  [{i + 1}/{NUM_DOCS}] {meta['filename']} — {meta['equipment_name']} ({meta['manufacturer']})")

print(f"\nWrote {NUM_DOCS} PDFs to {volume_dest}")

# COMMAND ----------

# Verify uploads
files = dbutils.fs.ls(volume_dest)
print(f"Files in volume ({len(files)}):")
for f in files:
    print(f"  {f.name} ({f.size:,} bytes)")
