# INGRES Chatbot (Deterministic Prototype)

This is a no-LLM, auditable chatbot API over INGRES Central Reports.

## Structure
- `scripts/ingest.py` — parses Excel files in `data/raw/*.xlsx`, writes:
  - Gold: `data/ingres_proto.sqlite` with `gw_assessment_core`
  - Silver: Parquet partitions per year in `data/silver/assessment/year=YYYY/` (optional, requires pyarrow)
- `scripts/mapping.yaml` — header patterns + unit conversions
- `server/` — Express + SQLite deterministic API

## Quickstart

1) Put your Excel files in `data/raw/` (e.g., `CentralReport2019-2020.xlsx`).

2) Create a venv and run ingestion:
```bash
python3 -m venv .venv && source .venv/bin/activate
pip install pandas openpyxl pyyaml pyarrow
python scripts/ingest.py
```

3) Start the API:
```bash
cd server
npm install
npx ts-node src/index.ts
# API at http://localhost:3001
```

### Endpoints
- `POST /api/parse` — { text } → { intent, years, place, category }
- `POST /api/query` — pass the parsed object → rows + meta.sql + yearUsed
- `GET  /api/explain` — formula + thresholds (static)

### Example cURL
```bash
curl -s http://localhost:3001/api/parse -H 'Content-Type: application/json' -d '{"text":"list over-exploited districts in Gujarat 2020"}' | jq
curl -s http://localhost:3001/api/query -H 'Content-Type: application/json' -d '{"intent":"LIST","years":{"y1":2020,"y2":2020},"category":"Over-Exploited","place":{"level":"state","state":"Gujarat"}}' | jq
```

## Notes
- District-level is supported out of the box. For block-level, extend mapping with `"BLOCK"|"TALUK"|"MANDAL"` and add centroids.
- This prototype prefers **determinism**. You can optionally add Grok for translation if you wish; keep deterministic parsing as default.

