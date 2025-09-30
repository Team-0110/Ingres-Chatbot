import pandas as pd, re, sqlite3, glob, os, yaml

# Optional Silver (Parquet). If pyarrow isn't installed, we'll just skip it.
try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    HAVE_ARROW = True
except Exception:
    HAVE_ARROW = False

RAW_GLOB   = os.environ.get("RAW_GLOB", "data/raw/*.xlsx")
DB_PATH    = os.environ.get("DB_PATH", "data/ingres_proto.sqlite")
SILVER_DIR = os.environ.get("SILVER_DIR", "data/silver/assessment")
MAP_PATH   = os.environ.get("MAP_PATH", "scripts/mapping.yaml")
PRINT_HEADERS = os.environ.get("PRINT_HEADERS", "0") == "1"

# ---------------- helpers ----------------

def parse_year_from_name(path: str):
    """
    Extract assessment year from filename like '2019-2020' -> returns 2020.
    (Fix: capture full 4-digit years)
    """
    m = re.search(r"((?:19|20)\d{2})-((?:19|20)\d{2})", os.path.basename(path))
    if not m:
        return None
    return int(m.group(2))

def detect_headers(df: pd.DataFrame):
    """
    Find the first row that contains both STATE and DISTRICT, assume next row is subheader.
    """
    header_idx = None
    for i in range(min(200, len(df))):
        row = df.iloc[i].astype(str).str.upper().tolist()
        if any("STATE" in c for c in row) and any("DISTRICT" in c for c in row):
            header_idx = i
            break
    if header_idx is None:
        raise RuntimeError("Could not locate header row with both STATE and DISTRICT")
    return header_idx, header_idx + 1

def normalize_headers(df: pd.DataFrame, header_idx: int, sub_idx: int):
    header = df.iloc[header_idx].ffill().astype(str)
    subhdr = df.iloc[sub_idx].fillna("").astype(str)
    cols = [(h.strip() + " " + s.strip()).strip() for h, s in zip(header, subhdr)]
    data = df.iloc[sub_idx + 1:].copy()
    data.columns = cols
    data = data.dropna(how="all", axis=1).dropna(how="all")
    if PRINT_HEADERS:
        print("---- HEADER PREVIEW ----")
        for j, (h, s) in enumerate(zip(header, subhdr)):
            print(f"{j:3d}: {str(h)} | {str(s)}")
        print("------------------------")
    return data

def first_col_idx(data: pd.DataFrame, patterns):
    pats = [p.upper() for p in patterns]
    for i, c in enumerate(data.columns):
        u = str(c).upper()
        if any(p in u for p in pats):
            return i
    return None

def pick_metric_idx(data: pd.DataFrame, mapping: dict, metric_key: str):
    configs = mapping["metrics"][metric_key]
    for conf in configs:
        idx = first_col_idx(data, [conf["header_like"]])
        if idx is not None:
            return idx, conf
    return None, None

def to_mcm(series: pd.Series, conf: dict | None):
    if conf and conf.get("factor"):
        return pd.to_numeric(series, errors="coerce") * float(conf["factor"])
    return pd.to_numeric(series, errors="coerce")

def stage_to_category(s):
    if pd.isna(s):
        return None
    try:
        s = float(s)
    except Exception:
        return None
    if s < 70:
        return "Safe"
    if s < 90:
        return "Semi-Critical"
    if s <= 100:
        return "Critical"
    return "Over-Exploited"

def norm_key(s):
    """
    Normalize state/district/block cells:
    - strip whitespace
    - treat '', 'nan', 'na', 'none' as missing
    """
    if s is None:
        return None
    s = str(s).strip()
    if s == "" or s.lower() in ("nan", "na", "none"):
        return None
    return s

# ---------------- ingest one file ----------------

def load_one(path: str, mapping: dict) -> pd.DataFrame:
    x = pd.ExcelFile(path)
    sheet = next((s for s in x.sheet_names if "GEC" in s.upper()), x.sheet_names[0])
    df = x.parse(sheet, header=None)

    header_idx, sub_idx = detect_headers(df)
    data = normalize_headers(df, header_idx, sub_idx)

    # Keys
    i_state    = first_col_idx(data, mapping["keys"]["state"])
    i_district = first_col_idx(data, mapping["keys"]["district"])
    i_block    = None
    if "block" in mapping.get("keys", {}):
        i_block = first_col_idx(data, mapping["keys"]["block"])

    # Metrics
    i_recharge,    recharge_map    = pick_metric_idx(data, mapping, "recharge_mcm")
    i_extractable, extractable_map = pick_metric_idx(data, mapping, "extractable_mcm")
    i_extraction,  extraction_map  = pick_metric_idx(data, mapping, "extraction_mcm")
    i_stage,       stage_map       = pick_metric_idx(data, mapping, "stage_pct")

    idxs = [i_state, i_district, i_recharge, i_extractable, i_extraction, i_stage]
    missing_names = ["state", "district", "recharge", "extractable", "extraction", "stage"]
    if any(v is None for v in idxs):
        miss = [missing_names[i] for i, v in enumerate(idxs) if v is None]
        raise RuntimeError(f"Missing required columns: {miss} in {os.path.basename(path)} (sheet: {sheet})")

    # Select by POSITION to avoid duplicate-label expansion
    sub = data.iloc[:, idxs].copy()
    sub.columns = ["state", "district", "recharge_raw", "extractable_raw", "extraction_raw", "stage_pct"]

    if i_block is not None:
        sub.insert(2, "block", data.iloc[:, i_block])

    # Normalize keys early
    sub["state"] = sub["state"].apply(norm_key)
    sub["district"] = sub["district"].apply(norm_key)
    if "block" in sub.columns:
        sub["block"] = sub["block"].apply(norm_key)

    # Numeric & unit conversions
    sub["recharge_mcm"]    = to_mcm(sub["recharge_raw"],    recharge_map)
    sub["extractable_mcm"] = to_mcm(sub["extractable_raw"], extractable_map)
    sub["extraction_mcm"]  = to_mcm(sub["extraction_raw"],  extraction_map)
    sub["stage_pct"]       = pd.to_numeric(sub["stage_pct"], errors="coerce")

    # Year
    sub["year"] = parse_year_from_name(path)

    # If stage missing but extractable>0 and extraction present, compute stage
    mask = sub["stage_pct"].isna() & sub["extractable_mcm"].gt(0) & sub["extraction_mcm"].notna()
    sub.loc[mask, "stage_pct"] = (sub.loc[mask, "extraction_mcm"] / sub.loc[mask, "extractable_mcm"]) * 100.0

    # Derived category
    sub["category"] = sub["stage_pct"].apply(stage_to_category)

    # Standardize columns
    cols = ["state", "district", "year", "recharge_mcm", "extractable_mcm", "extraction_mcm", "stage_pct", "category"]
    if "block" in sub.columns:
        cols = ["state", "district", "block", "year", "recharge_mcm", "extractable_mcm", "extraction_mcm", "stage_pct", "category"]
    sub = sub[cols]

    # Drop rows missing keys or year
    key_cols = ["state", "district", "year"]
    sub = sub.dropna(subset=key_cols)

    return sub

# ---------------- outputs ----------------

def write_silver(df: pd.DataFrame):
    if not HAVE_ARROW:
        return
    years = sorted(df["year"].dropna().unique().tolist())
    for yr in years:
        part = df[df["year"] == yr].copy()
        outdir = os.path.join(SILVER_DIR, f"year={int(yr)}")
        os.makedirs(outdir, exist_ok=True)
        pq.write_table(pa.Table.from_pandas(part), os.path.join(outdir, "part.parquet"))

def upsert_gold(df: pd.DataFrame):
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.executescript("""
    PRAGMA journal_mode = WAL;
    CREATE TABLE IF NOT EXISTS gw_assessment_core (
      state TEXT NOT NULL,
      district TEXT NOT NULL,
      block TEXT,
      year INTEGER NOT NULL,
      recharge_mcm REAL,
      extractable_mcm REAL,
      extraction_mcm REAL,
      stage_pct REAL,
      category TEXT,
      PRIMARY KEY (state, district, year)
    );
    CREATE INDEX IF NOT EXISTS idx_core_state_year ON gw_assessment_core(state, year);
    CREATE INDEX IF NOT EXISTS idx_core_cat_state_year ON gw_assessment_core(category, state, year);
    """)
    cols = ["state", "district", "block", "year", "recharge_mcm", "extractable_mcm", "extraction_mcm", "stage_pct", "category"]
    for c in cols:
        if c not in df.columns:
            df[c] = None
    rows = df[cols].values.tolist()
    try:
        cur.executemany("""
        INSERT INTO gw_assessment_core
          (state, district, block, year, recharge_mcm, extractable_mcm, extraction_mcm, stage_pct, category)
        VALUES (?,?,?,?,?,?,?,?,?)
        ON CONFLICT(state, district, year) DO UPDATE SET
          block=excluded.block,
          recharge_mcm=excluded.recharge_mcm,
          extractable_mcm=excluded.extractable_mcm,
          extraction_mcm=excluded.extraction_mcm,
          stage_pct=excluded.stage_pct,
          category=excluded.category;
        """, rows)
    except sqlite3.OperationalError:
        cur.executemany("""
        INSERT OR REPLACE INTO gw_assessment_core
          (state, district, block, year, recharge_mcm, extractable_mcm, extraction_mcm, stage_pct, category)
        VALUES (?,?,?,?,?,?,?,?,?);
        """, rows)
    con.commit()
    con.close()

# ---------------- main ----------------

def main():
    if not os.path.exists(MAP_PATH):
        raise SystemExit(f"mapping.yaml not found at {MAP_PATH}")
    mapping = yaml.safe_load(open(MAP_PATH, "r", encoding="utf-8"))

    frames = []
    paths = glob.glob(RAW_GLOB)
    if not paths:
        print(f"No files matched RAW_GLOB={RAW_GLOB}")
        return

    for path in paths:
        try:
            df = load_one(path, mapping)
            frames.append(df)
            print("Parsed:", path)
        except Exception as e:
            print("Skip:", path, e)

    if not frames:
        print("No files parsed.")
        return

    all_df = pd.concat(frames, ignore_index=True)

    write_silver(all_df)
    upsert_gold(all_df)

    print(f"Loaded rows: {len(all_df)} into {DB_PATH}")
    try:
        yr_min, yr_max = int(all_df["year"].min()), int(all_df["year"].max())
        print(f"Year range: {yr_min}–{yr_max}")
        print("Sample:", all_df.head(3).to_dict(orient="records"))
    except Exception:
        pass

if __name__ == "__main__":
    main()
