"""
Generates realistic, synthetic multi-source raw data covering ERP and
CRM exports and Excel files. The data is synthetic, modeled on a
plausible B2B subscription/sales business, and deliberately built with
the kind of real messiness a genuine multi-source ETL job has to handle --
inconsistent keys, missing fields, duplicate records, mismatched date
formats across sources -- rather than three already-clean tables that
would make the "integration" step trivial.
"""

import csv
import json
import os
import random

random.seed(42)

OUT_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "raw")
os.makedirs(OUT_DIR, exist_ok=True)

CUSTOMERS = [
    ("C1001", "Klinikum Nordstadt GmbH", "Healthcare"),
    ("C1002", "Universitaet Erlangen Bibliothek", "Education"),
    ("C1003", "MedCare Consulting AG", "Healthcare"),
    ("C1004", "Fachhochschule Franken", "Education"),
    ("C1005", "St. Elisabeth Krankenhaus", "Healthcare"),
    ("C1006", "PharmaLogix Vertrieb GmbH", "Pharma"),
    ("C1007", "Bayerische Aerztekammer", "Association"),
    ("C1008", "Reha-Zentrum Sued", "Healthcare"),
    ("C1009", "MedTech Solutions Erlangen", "Pharma"),
    ("C1010", "Landesbibliothek Bayern", "Education"),
]

PRODUCTS = [
    ("P01", "Digital Subscription - Clinical Reference", 890.00),
    ("P02", "Print + Digital Bundle - Medical Journal", 1250.00),
    ("P03", "E-Learning Platform License", 450.00),
    ("P04", "Institutional Database Access", 3200.00),
    ("P05", "Patient Education Toolkit", 320.00),
]


def generate_erp_orders_csv():
    """ERP-style export: order/revenue records, German-locale
    formatting (comma decimal separator, semicolon delimiter -- a
    real, common SAP/ERP export quirk), with a real data-quality
    problem deliberately injected (duplicate order IDs, a few rows
    with a missing customer ID) so the ETL has genuine cleaning to do."""
    path = os.path.join(OUT_DIR, "erp_orders_export.csv")
    rows = []
    order_id_counter = 5000
    for _ in range(400):
        order_id_counter += 1
        customer = random.choice(CUSTOMERS)
        product = random.choice(PRODUCTS)
        quantity = random.randint(1, 8)
        # German-locale number formatting, as a real SAP export would produce
        amount = round(product[2] * quantity, 2)
        amount_str = f"{amount:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        month = random.randint(1, 12)
        day = random.randint(1, 28)
        order_date = f"{day:02d}.{month:02d}.2026"  # DD.MM.YYYY, real SAP-style date format
        customer_id = customer[0] if random.random() > 0.03 else ""  # ~3% missing customer ID, a real data-quality issue
        rows.append({
            "BestellNr": f"ORD-{order_id_counter}",
            "KundenNr": customer_id,
            "ProduktCode": product[0],
            "Menge": quantity,
            "Betrag_EUR": amount_str,
            "Bestelldatum": order_date,
        })
    # inject real duplicate order rows (same order appearing twice, a
    # common export-glitch pattern), so dedup is genuinely necessary
    for _ in range(15):
        rows.append(dict(random.choice(rows)))

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys(), delimiter=";")
        writer.writeheader()
        writer.writerows(rows)
    print(f"Wrote {len(rows)} rows to {path}")


def generate_crm_contacts_json():
    """CRM-style export: customer contact/engagement records as JSON
    (mirroring a CRM API export), with a genuinely different customer
    key naming convention than the ERP source (crm_customer_id vs
    KundenNr) and inconsistent casing -- a real integration problem the
    ETL has to resolve via an explicit mapping, not just a naive join
    on identical column names."""
    path = os.path.join(OUT_DIR, "crm_contacts_export.json")
    records = []
    for cust_id, name, segment in CUSTOMERS:
        records.append({
            "crm_customer_id": cust_id.lower(),  # deliberately lowercase, unlike ERP's uppercase KundenNr
            "account_name": name,
            "segment": segment,
            "account_owner": random.choice(["A. Weber", "M. Schmidt", "J. Klein", "S. Wagner"]),
            "last_contact_date": f"2026-{random.randint(1,12):02d}-{random.randint(1,28):02d}",  # ISO format, different from ERP's DD.MM.YYYY
            "renewal_risk_score": round(random.uniform(0.05, 0.85), 2),
        })
    with open(path, "w", encoding="utf-8") as f:
        json.dump(records, f, indent=2, ensure_ascii=False)
    print(f"Wrote {len(records)} records to {path}")


def generate_product_catalog_excel_style_csv():
    """Excel-style source: a product catalog maintained manually in
    Excel (represented here as CSV rather than a real .xlsx). Includes a real,
    common Excel-export problem: trailing whitespace and inconsistent
    capitalization in the product code column, which breaks a naive
    exact-match join if not cleaned."""
    path = os.path.join(OUT_DIR, "product_catalog.csv")
    rows = []
    for code, name, price in PRODUCTS:
        # inject inconsistent casing/whitespace on ~40% of rows, a real
        # manual-Excel-entry problem
        messy_code = f" {code.lower()} " if random.random() < 0.4 else code
        rows.append({"Product Code": messy_code, "Product Name": name, "List Price EUR": price, "Active": "Yes"})
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    print(f"Wrote {len(rows)} rows to {path}")


if __name__ == "__main__":
    generate_erp_orders_csv()
    generate_crm_contacts_json()
    generate_product_catalog_excel_style_csv()
