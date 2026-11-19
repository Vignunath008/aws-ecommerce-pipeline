"""Regenerates the README diagrams (SVG) into docs/images/.
Usage: python docs/generate_diagrams.py"""
import os
import sys
from xml.sax.saxutils import escape as esc

ROOT = sys.argv[1] if len(sys.argv) > 1 else os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
OUT = os.path.join(ROOT, "docs", "images")
os.makedirs(OUT, exist_ok=True)

FONT = "Arial, Helvetica, sans-serif"
C = {  # service colours (border, fill)
    "src": ("#6b7280", "#f3f4f6"),
    "s3": ("#3f8624", "#ecf7e8"),
    "lambda": ("#ed7100", "#fff3e6"),
    "glue": ("#8c4fff", "#f3edff"),
    "redshift": ("#2e73b8", "#e8f1fa"),
    "dq": ("#0f766e", "#e6f6f4"),
    "airflow": ("#017cee", "#e8f3fe"),
    "bad": ("#c0392b", "#fdecea"),
}


def head(w, h, title):
    return [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{w}" height="{h}" viewBox="0 0 {w} {h}" '
        f'font-family="{FONT}">',
        f"<title>{esc(title)}</title>",
        "<defs>"
        '<marker id="a" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="8" markerHeight="8" orient="auto-start-reverse">'
        '<path d="M0,0 L10,5 L0,10 z" fill="#374151"/></marker>'
        '<marker id="ab" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="8" markerHeight="8" orient="auto-start-reverse">'
        '<path d="M0,0 L10,5 L0,10 z" fill="#017cee"/></marker>'
        "</defs>",
        f'<rect width="{w}" height="{h}" fill="#ffffff"/>',
    ]


def box(x, y, w, h, kind, title, lines=(), tag=None):
    stroke, fill = C[kind]
    out = [f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="10" fill="{fill}" stroke="{stroke}" stroke-width="2"/>']
    if tag:
        out.append(f'<rect x="{x}" y="{y}" width="{w}" height="22" rx="10" fill="{stroke}"/>'
                   f'<rect x="{x}" y="{y+11}" width="{w}" height="11" fill="{stroke}"/>'
                   f'<text x="{x+w/2}" y="{y+15.5}" text-anchor="middle" font-size="11" font-weight="700" '
                   f'fill="#ffffff" letter-spacing="0.6">{esc(tag)}</text>')
        ty = y + 44
    else:
        ty = y + 28
    out.append(f'<text x="{x+w/2}" y="{ty}" text-anchor="middle" font-size="15" font-weight="700" fill="#111827">{esc(title)}</text>')
    for i, ln in enumerate(lines):
        out.append(f'<text x="{x+w/2}" y="{ty+20+i*17}" text-anchor="middle" font-size="12" fill="#4b5563">{esc(ln)}</text>')
    return out


def arrow(points, label=None, lx=None, ly=None, color="#374151", dashed=False, anchor="middle"):
    d = "M" + " L".join(f"{x},{y}" for x, y in points)
    m = "ab" if color == "#017cee" else "a"
    dash = ' stroke-dasharray="6 5"' if dashed else ""
    out = [f'<path d="{d}" fill="none" stroke="{color}" stroke-width="2"{dash} marker-end="url(#{m})"/>']
    if label:
        out.append(f'<text x="{lx}" y="{ly}" text-anchor="{anchor}" font-size="12" font-style="italic" fill="{color}">{esc(label)}</text>')
    return out


def save(name, parts):
    parts.append("</svg>")
    with open(os.path.join(OUT, name), "w") as f:
        f.write("\n".join(parts) + "\n")


# --------------------------------------------------------------- 1. architecture
W, H = 1080, 640
p = head(W, H, "AWS e-commerce pipeline architecture")
p.append(f'<text x="30" y="36" font-size="20" font-weight="700" fill="#111827">End-to-end architecture</text>')
p.append(f'<text x="30" y="58" font-size="13" fill="#6b7280">Files flow left to right on the top row, then right to left on the bottom row.</text>')
bw, bh = 200, 96
X = [30, 310, 590, 850]
Y1, Y2 = 90, 360
p += box(X[0], Y1, bw, bh, "src", "Order files", ["CSV exports from the", "e-commerce store"], "SOURCE")
p += box(X[1], Y1, bw, bh, "s3", "S3 raw bucket", ["incoming/*.csv"], "AMAZON S3")
p += box(X[2], Y1, bw, bh, "lambda", "validate_and_trigger", ["checks type, size, header"], "AWS LAMBDA")
p += box(X[3], Y1, bw, bh, "s3", "S3 raw bucket", ["validated/<date>/*.csv"], "AMAZON S3")
p += box(X[2], 230, bw, 80, "bad", "Quarantine", ["quarantine/<date>/ + reason"])
p += box(X[3], Y2, bw, bh, "glue", "transform_orders", ["clean, type, dedupe", "(PySpark, Glue 4.0)"], "AWS GLUE")
p += box(X[2], Y2, bw, bh, "s3", "S3 curated bucket", ["orders/load_date=<ds>/", "*.parquet"], "AMAZON S3")
p += box(X[1], Y2, bw, bh, "redshift", "Star schema", ["staging → dim_* +", "fact_order_lines"], "AMAZON REDSHIFT")
p += box(X[0], Y2, bw, bh, "dq", "Quality checks", ["8 SQL checks;", "run fails on any error"], "DATA QUALITY")
cy1, cy2 = Y1 + bh / 2, Y2 + bh / 2
p += arrow([(X[0]+bw, cy1), (X[1]-4, cy1)], "upload", (X[0]+bw+X[1])/2, cy1-8)
p += arrow([(X[1]+bw, cy1), (X[2]-4, cy1)], "S3 event", (X[1]+bw+X[2])/2, cy1-8)
p += arrow([(X[2]+bw, cy1), (X[3]-4, cy1)], "valid", (X[2]+bw+X[3])/2, cy1-8)
p += arrow([(X[2]+bw/2, Y1+bh), (X[2]+bw/2, 226)], "invalid", X[2]+bw/2+8, 212, color="#c0392b", anchor="start")
p += arrow([(X[3]+bw/2, Y1+bh), (X[3]+bw/2, Y2-4)], "daily batch", X[3]+bw/2+8, 290, anchor="start")
p += arrow([(X[3], cy2), (X[2]+bw+4, cy2)], "Parquet", (X[2]+bw+X[3])/2, cy2-8)
p += arrow([(X[2], cy2), (X[1]+bw+4, cy2)], "COPY", (X[1]+bw+X[2])/2, cy2-8)
p += arrow([(X[1], cy2), (X[0]+bw+4, cy2)], "validate", (X[0]+bw+X[1])/2, cy2-8)
# Airflow bar
ay = 530
p.append(f'<rect x="30" y="{ay}" width="{X[3]+bw-30}" height="74" rx="10" fill="{C["airflow"][1]}" stroke="{C["airflow"][0]}" stroke-width="2"/>')
p.append(f'<text x="50" y="{ay+30}" font-size="15" font-weight="700" fill="#0b4f9c">Apache Airflow — ecommerce_pipeline DAG (daily)</text>')
p.append(f'<text x="50" y="{ay+52}" font-size="12" fill="#1f4f80">wait for validated/&lt;ds&gt;/ files  →  run Glue job  →  load Redshift (one transaction)  →  run quality checks</text>')
for xx in (X[3], X[1], X[0]):
    p += arrow([(xx+bw/2, ay), (xx+bw/2, Y2+bh+4)], color="#017cee", dashed=True)
p.append(f'<text x="{X[2]+bw/2}" y="{ay-12}" text-anchor="middle" font-size="12" font-style="italic" fill="#017cee">orchestrates</text>')
save("architecture.svg", p)

# --------------------------------------------------------------- 2. airflow DAG
W, H = 1080, 300
p = head(W, H, "Airflow DAG tasks")
p.append('<text x="30" y="36" font-size="20" font-weight="700" fill="#111827">Airflow DAG: ecommerce_pipeline</text>')
p.append('<text x="30" y="58" font-size="13" fill="#6b7280">Runs daily. Each run handles the files that arrived on its logical date ({{ ds }}). Example below: ds = 2026-11-18.</text>')
tasks = [
    ("wait_for_orders_file", "S3KeySensor", ["validated/2026-11-18/*.csv", "skips the run if no files"]),
    ("glue_transform_orders", "GlueJobOperator", ["writes curated/orders/", "load_date=2026-11-18/"]),
    ("load_redshift", "SQLExecuteQueryOperator", ["COPY + dedupe + upsert", "8 statements, 1 transaction"]),
    ("data_quality_checks", "PythonOperator", ["8 checks from checks.sql", "fails the run on any error"]),
]
bw, gap = 225, 40
for i, (tid, op, lines) in enumerate(tasks):
    x = 30 + i * (bw + gap)
    p.append(f'<rect x="{x}" y="90" width="{bw}" height="150" rx="10" fill="#ffffff" stroke="#2e7d32" stroke-width="2.5"/>')
    p.append(f'<rect x="{x}" y="90" width="8" height="150" rx="4" fill="#2e7d32"/>')
    p.append(f'<text x="{x+22}" y="120" font-size="11" font-weight="700" fill="#6b7280">STEP {i+1}</text>')
    p.append(f'<text x="{x+22}" y="144" font-size="15" font-weight="700" fill="#111827">{esc(tid)}</text>')
    p.append(f'<text x="{x+22}" y="166" font-size="12" fill="#017cee">{esc(op)}</text>')
    for j, ln in enumerate(lines):
        p.append(f'<text x="{x+22}" y="{196+j*18}" font-size="12" fill="#4b5563">{esc(ln)}</text>')
    if i < 3:
        p += arrow([(x+bw, 165), (x+bw+gap-4, 165)])
p.append('<text x="30" y="278" font-size="12" fill="#6b7280">Retries: 2 (5 min apart)  ·  max_active_runs = 1  ·  catchup = False</text>')
save("airflow_dag.svg", p)

# --------------------------------------------------------------- 3. star schema
W, H = 1080, 540
p = head(W, H, "Redshift star schema")
p.append('<text x="30" y="36" font-size="20" font-weight="700" fill="#111827">Redshift data model</text>')
p.append('<text x="30" y="58" font-size="13" fill="#6b7280">Parquet is copied into staging, deduplicated, then upserted into one fact table and two dimensions.</text>')
RH = 24


def table(x, y, w, name, cols, kind, note=None):
    stroke, fill = C[kind]
    h = 38 + RH * len(cols)
    out = [f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="8" fill="#ffffff" stroke="{stroke}" stroke-width="2"/>',
           f'<rect x="{x}" y="{y}" width="{w}" height="38" rx="8" fill="{stroke}"/>',
           f'<rect x="{x}" y="{y+30}" width="{w}" height="8" fill="{stroke}"/>',
           f'<text x="{x+w/2}" y="{y+25}" text-anchor="middle" font-size="14" font-weight="700" fill="#ffffff">{esc(name)}</text>']
    for i, (cname, typ, key) in enumerate(cols):
        ry = y + 38 + i * RH
        if i % 2:
            out.append(f'<rect x="{x+2}" y="{ry}" width="{w-4}" height="{RH}" fill="{fill}"/>')
        out.append(f'<text x="{x+14}" y="{ry+16}" font-size="12" font-weight="{700 if key else 400}" fill="#111827">{esc(cname)}</text>')
        out.append(f'<text x="{x+w-78}" y="{ry+16}" text-anchor="end" font-size="11" fill="#6b7280">{esc(typ)}</text>')
        if key:
            col = "#b45309" if key.startswith("PK") else ("#2e73b8" if key == "FK" else "#0f766e")
            out.append(f'<text x="{x+w-12}" y="{ry+16}" text-anchor="end" font-size="11" font-weight="700" fill="{col}">{esc(key)}</text>')
    if note:
        out.append(f'<text x="{x+w/2}" y="{y+h+20}" text-anchor="middle" font-size="11" fill="#6b7280">{esc(note)}</text>')
    return out, h


fact_cols = [
    ("order_id", "VARCHAR(64)", "PK"), ("product_id", "VARCHAR(64)", "PK, FK"),
    ("customer_id", "VARCHAR(64)", "FK"), ("order_ts", "TIMESTAMP", ""),
    ("order_date", "DATE", "SORTKEY"), ("quantity", "INTEGER", ""),
    ("unit_price", "DECIMAL(12,2)", ""), ("line_total", "DECIMAL(14,2)", ""),
    ("currency", "VARCHAR(3)", ""), ("order_status", "VARCHAR(32)", ""), ("load_ts", "TIMESTAMP", ""),
]
fx, fy, fw = 380, 170, 320
t, fh = table(fx, fy, fw, "analytics.fact_order_lines", fact_cols, "redshift", "DISTKEY customer_id  ·  SORTKEY order_date")
p += t
dw, dy = 260, 230
dc, dh = table(30, dy, dw, "analytics.dim_customer",
               [("customer_key", "BIGINT", "PK"), ("customer_id", "VARCHAR(64)", ""), ("first_seen_ts", "TIMESTAMP", "")],
               "dq", "DISTSTYLE ALL")
p += dc
dp, _ = table(790, dy, dw, "analytics.dim_product",
              [("product_key", "BIGINT", "PK"), ("product_id", "VARCHAR(64)", ""), ("first_seen_ts", "TIMESTAMP", "")],
              "dq", "DISTSTYLE ALL")
p += dp
cy_dim = dy + 38 + RH + RH / 2
cy_cust = fy + 38 + RH * 2 + RH / 2
cy_prod = fy + 38 + RH + RH / 2
p.append(f'<path d="M{30+dw},{cy_dim} L335,{cy_dim} L335,{cy_cust} L{fx},{cy_cust}" fill="none" stroke="#2e73b8" stroke-width="2"/>')
p.append(f'<path d="M790,{cy_dim} L745,{cy_dim} L745,{cy_prod} L{fx+fw},{cy_prod}" fill="none" stroke="#2e73b8" stroke-width="2"/>')
for lx, ly in ((335, (cy_dim + cy_cust) / 2), (745, (cy_dim + cy_prod) / 2)):
    p.append(f'<rect x="{lx-28}" y="{ly-10}" width="56" height="18" rx="4" fill="#ffffff" stroke="#2e73b8"/>')
    p.append(f'<text x="{lx}" y="{ly+3.5}" text-anchor="middle" font-size="11" fill="#2e73b8">1 : many</text>')
# staging
p.append(f'<rect x="{fx}" y="80" width="{fw}" height="56" rx="10" fill="{C["s3"][1]}" stroke="{C["s3"][0]}" stroke-width="2"/>')
p.append(f'<text x="{fx+fw/2}" y="103" text-anchor="middle" font-size="15" font-weight="700" fill="#111827">staging.orders_stg</text>')
p.append(f'<text x="{fx+fw/2}" y="123" text-anchor="middle" font-size="12" fill="#4b5563">11 columns, same order as the Glue output</text>')
p += arrow([(fx+fw/2, 136), (fx+fw/2, fy-4)])
p.append(f'<text x="{fx+fw/2+10}" y="{(136+fy)/2+4}" font-size="12" font-style="italic" fill="#374151">dedupe + upsert</text>')
p.append(f'<text x="30" y="{H-20}" font-size="12" fill="#6b7280">PK = primary key  ·  FK = links to a dimension  ·  SORTKEY speeds up date filters  ·  DISTSTYLE ALL copies small tables to every node</text>')
save("star_schema.svg", p)

# --------------------------------------------------------------- 4. data cleaning
rows = [
    ("O1001", "P100", "2", "19.99", "kept as is", True),
    ("O1001", "P200", "1", "5.50", "kept as is", True),
    ("O1002", "P100", "1", "19.99", "older version of the same order line (10:02)", False),
    ("O1002", "P100", "3", "19.99", "latest version (11:45) wins the dedupe", True),
    ("O1003", "P300", "0", "49.00", "quantity is 0", False),
    ("(blank)", "P100", "1", "19.99", "missing order_id", False),
    ("O1004", "P100", "1", "19.99", "order_ts is \"not-a-date\"", False),
    ("O1005", "P400", "1", "-3.00", "negative unit_price", False),
    ("O1006", "P500", "4", "12.25", "\" C006 \" trimmed, \"eur\" → EUR", True),
]
W = 1080
RH = 34
H = 150 + RH * len(rows) + 60
p = head(W, H, "Glue transform on the sample data")
p.append('<text x="30" y="36" font-size="20" font-weight="700" fill="#111827">What the Glue job does to the sample file</text>')
p.append('<text x="30" y="58" font-size="13" fill="#6b7280">sample_data/orders_sample.csv  →  output/transformed_orders_sample.csv</text>')
cols = [("order_id", 30), ("product_id", 140), ("quantity", 260), ("unit_price", 360), ("What happened", 480), ("Result", 920)]
ty = 90
p.append(f'<rect x="20" y="{ty}" width="{W-40}" height="36" rx="6" fill="#1f2937"/>')
for name, x in cols:
    p.append(f'<text x="{x+10}" y="{ty+23}" font-size="13" font-weight="700" fill="#ffffff">{esc(name)}</text>')
for i, (oid, pid, q, price, what, kept) in enumerate(rows):
    y = ty + 36 + i * RH
    fill = "#ecf7e8" if kept else "#fdecea"
    p.append(f'<rect x="20" y="{y}" width="{W-40}" height="{RH}" fill="{fill}"/>')
    p.append(f'<line x1="20" y1="{y+RH}" x2="{W-20}" y2="{y+RH}" stroke="#ffffff" stroke-width="2"/>')
    vals = [oid, pid, q, price, what]
    for (name, x), v in zip(cols[:5], vals):
        p.append(f'<text x="{x+10}" y="{y+22}" font-size="13" fill="#111827">{esc(v)}</text>')
    label, col = ("kept", "#2e7d32") if kept else ("dropped", "#c0392b")
    ix, iy = cols[5][1] + 18, y + RH / 2
    p.append(f'<circle cx="{ix}" cy="{iy}" r="9" fill="{col}"/>')
    if kept:
        p.append(f'<path d="M{ix-4.5},{iy} L{ix-1},{iy+3.5} L{ix+5},{iy-3.5}" fill="none" stroke="#fff" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"/>')
    else:
        p.append(f'<path d="M{ix-3.5},{iy-3.5} L{ix+3.5},{iy+3.5} M{ix+3.5},{iy-3.5} L{ix-3.5},{iy+3.5}" stroke="#fff" stroke-width="2.2" stroke-linecap="round"/>')
    p.append(f'<text x="{ix+16}" y="{y+22}" font-size="13" font-weight="700" fill="{col}">{label}</text>')
sy = ty + 36 + RH * len(rows) + 34
p.append(f'<text x="30" y="{sy}" font-size="15" font-weight="700" fill="#111827">9 rows in  →  4 clean rows out</text>')
p.append(f'<text x="290" y="{sy}" font-size="13" fill="#4b5563">The job also adds line_total (quantity × unit_price) and load_ts, and lower-cases order_status.</text>')
save("data_cleaning.svg", p)

print("wrote", sorted(os.listdir(OUT)))
