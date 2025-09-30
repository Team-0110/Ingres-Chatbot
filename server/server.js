
// server/index.js
// INGRES chatbot backend: Express + better-sqlite3 + optional Gemini summarization + map APIs
// Node 20+
//
// Install deps:  npm i express cors better-sqlite3
// Run:           node index.js
// DB file:       ../data/ingres_proto.sqlite  (or ./data/ingres_proto.sqlite, or env DB_PATH)
// Gemini: set GEMINI_KEY in env; if absent, the app still works using deterministic summaries.

const GEMINI_KEY   = process.env.GEMINI_KEY || "AIzaSyBulJycrBjs5nsRWwAICwMopvPw3CuR66I"; // ← do NOT hardcode secrets
const GEMINI_MODEL = "gemini-2.0-flash";
const GEMINI_URL   = (model, key) =>
  `https://generativelanguage.googleapis.com/v1beta/models/${encodeURIComponent(model)}:generateContent?key=${encodeURIComponent(key)}`;

const express  = require("express");
const cors     = require("cors");
const Database = require("better-sqlite3");
const fs       = require("fs");
const path     = require("path");
const serverless = require("serverless-http");

const PORT = process.env.PORT || 3001;

// ---------- DB path resolution ----------
const ENV_DB   = process.env.DB_PATH;
const ROOT_DB  = path.resolve(__dirname, "../data/ingres_proto.sqlite");
const LOCAL_DB = path.resolve(__dirname, "./data/ingres_proto.sqlite");
const DB_PATH  = (ENV_DB && fs.existsSync(ENV_DB)) ? ENV_DB : (fs.existsSync(ROOT_DB) ? ROOT_DB : LOCAL_DB);

if (!fs.existsSync(DB_PATH)) {
  console.error("SQLite DB not found at:", DB_PATH);
  console.error('Set DB_PATH or place "ingres_proto.sqlite" under ../data or ./data');
  process.exit(1);
}

const app = express();
app.use(cors());
app.use(express.json({ limit: "2mb" }));

const db = new Database(DB_PATH, { readonly: false });
console.log("Using DB:", DB_PATH);

// ---------- helpers ----------
const esc = (s) => (s || "").replaceAll("'", "''");
const fmt1 = (n) => (n == null || isNaN(n)) ? "—" : (+n).toFixed(1);
const collapseWs = (s) => (s || "").replace(/\s+/g, " ").trim();

const STATE_SYNONYM = {
  TN: "TAMIL NADU", TAMILNADU: "TAMIL NADU", UP: "UTTAR PRADESH", MP: "MADHYA PRADESH",
  MH: "MAHARASHTRA", TS: "TELANGANA", AP: "ANDHRA PRADESH",
};

function nameRegex(name) {
  const cleaned = String(name).trim();
  if (!cleaned) return null;
  const wordish = cleaned.replace(/[^A-Za-z0-9]+/g, "\\s+");
  const spaced = new RegExp(`\\b${wordish}\\b`, "i");
  const nospace = new RegExp(`(?<![A-Za-z0-9])${cleaned.replace(/[^A-Za-z0-9]/g, "")}(?![A-Za-z0-9])`, "i");
  return { spaced, nospace };
}
function containsName(text, name) {
  const n = String(name || "");
  if (n.replace(/[^A-Za-z0-9]/g, "").length < 4) return false;
  const { spaced, nospace } = nameRegex(n);
  return spaced.test(text) || nospace.test(text.replace(/\s+/g, ""));
}

function parseYearsAbsolute(t) {
  const range = t.match(/(19|20)\d{2}\s*(?:-|to|–)\s*(19|20)\d{2}/i);
  if (range) {
    const [a, b] = range[0].split(/-|to|–/i).map((s) => parseInt(s.trim(), 10));
    return { y1: Math.min(a, b), y2: Math.max(a, b) };
  }
  const ys = (t.match(/(19|20)\d{2}/g) || []).map((x) => parseInt(x, 10));
  if (ys.length === 1) return { y1: ys[0], y2: ys[0] };
  if (ys.length >= 2) return { y1: Math.min(...ys), y2: Math.max(...ys) };
  return {};
}
function wordsToNum(w) {
  const m = { one:1,two:2,three:3,four:4,five:5,six:6,seven:7,eight:8,nine:9,ten:10,
              1:1,2:2,3:3,4:4,5:5,6:6,7:7,8:8,9:9,10:10 };
  return m[w.toLowerCase()];
}
function parseYearsRelative(t, latestYear) {
  const s = t.toLowerCase();
  const m = s.match(/\b(last|past|previous|recent)\s+(\d+|one|two|three|four|five|six|seven|eight|nine|ten)\s+years?\b/);
  if (!m || !latestYear) return null;
  const n = wordsToNum(m[2]);
  if (!n) return null;
  const y2 = latestYear;
  const y1 = y2 - (n - 1);
  return { y1, y2 };
}
function intentOf(t) {
  const s = t.toLowerCase();
  if (/(^|\b)(vs|compare|के मुकाबले)(\b|$)/.test(s)) return "COMPARE";
  if (/\b(since|between|trend|from|to|से|तक|trend of|trends?|change)\b/.test(s)) return "TREND";
  if (/\b(over|critical|semi|safe|अति|गंभीर|सुरक्षित)\b/.test(s) && /\b(list|show|दिखाओ)\b/.test(s)) return "LIST";
  return "DATA";
}

function resolvePlaceSmart(question) {
  const text = collapseWs(question);
  const textNoSpace = text.replace(/\s+/g, "");
  for (const k of Object.keys(STATE_SYNONYM)) {
    const syn = new RegExp(`\\b${k}\\b`, "i");
    if (syn.test(text)) return { level: "state", state: STATE_SYNONYM[k] };
  }
  const districts = db.prepare("SELECT DISTINCT state,district FROM gw_assessment_core").all();
  let bestD = null;
  for (const r of districts) if (containsName(text, r.district)) if (!bestD || String(r.district).length > String(bestD.district).length) bestD = r;
  if (bestD) return { level: "district", state: bestD.state, district: bestD.district };
  const states = db.prepare("SELECT DISTINCT state FROM gw_assessment_core").all().map(r => r.state);
  let bestS = null;
  for (const s of states) if (containsName(text, s)) if (!bestS || String(s).length > String(bestS).length) bestS = s;
  if (bestS) return { level: "state", state: bestS };
  for (const s of states) {
    const ns = String(s).replace(/\s+/g, "");
    if (new RegExp(`(?<![A-Za-z0-9])${ns}(?![A-Za-z0-9])`, "i").test(textNoSpace)) return { level: "state", state: s };
  }
  return { level: "unknown" };
}
function latestYearForState(state) {
  if (!state) return (db.prepare("SELECT MAX(year) y FROM gw_assessment_core").get() || {}).y;
  return (db.prepare("SELECT MAX(year) y FROM gw_assessment_core WHERE state=?").get(state) || {}).y;
}

// ---------- SQL builder ----------
function buildSQL(parsed) {
  const { intent, place, years, category } = parsed;
  const latest = latestYearForState(place?.state);
  const byYear = (years?.y1 && years?.y2)
    ? `AND year BETWEEN ${years.y1} AND ${years.y2}`
    : `AND year = ${latest ?? "(SELECT MAX(year) FROM gw_assessment_core)"}`;

  if (place?.level === "district") {
    const where = `state='${esc(place.state)}' AND district='${esc(place.district)}'`;
    return `SELECT year, recharge_mcm, extractable_mcm, extraction_mcm, stage_pct, category
            FROM gw_assessment_core WHERE ${where} ${byYear} ORDER BY year;`;
  }
  if (place?.level === "state") {
    const where = `state='${esc(place.state)}'`;
    if (intent === "LIST") {
      return `SELECT state, district, year, extractable_mcm, extraction_mcm, stage_pct, category
              FROM gw_assessment_core WHERE ${where} ${category ? `AND category='${esc(category)}'` : ""} ${byYear}
              ORDER BY stage_pct DESC LIMIT 500;`;
    }
    return `SELECT state, district, year, stage_pct, category
            FROM gw_assessment_core WHERE ${where} ${byYear}
            ORDER BY stage_pct DESC LIMIT 500;`;
  }
  const ly = latestYearForState(undefined);
  return `SELECT state, district, year, stage_pct, category
          FROM gw_assessment_core WHERE year=${ly} ORDER BY stage_pct DESC LIMIT 50;`;
}

// ---------- Gemini wrappers (optional) ----------
async function geminiGenerate(model, text, timeoutMs = 12000) {
  if (!GEMINI_KEY) throw new Error("No GEMINI_KEY set");
  const controller = new AbortController();
  const to = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const res = await fetch(GEMINI_URL(model, GEMINI_KEY), {
      method: "POST",
      signal: controller.signal,
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ contents: [{ role: "user", parts: [{ text }] }], generationConfig: { temperature: 0.2 } })
    });
    if (!res.ok) throw new Error(`Gemini error ${res.status}`);
    const data = await res.json();
    const parts = data?.candidates?.[0]?.content?.parts || [];
    const out = parts.map(p => p.text || "").join("").trim();
    if (!out) throw new Error("Empty Gemini response");
    return out;
  } finally { clearTimeout(to); }
}
async function geminiParse(question) {
  const prompt = `Convert India's groundwater question into STRICT JSON (no extra text).\nSchema:{\n  \"intent\": \"LIST\"|\"DATA\"|\"TREND\"|\"COMPARE\",\n  \"place\":{\"level\":\"national\"|\"state\"|\"district\",\"state\":string|null,\"district\":string|null},\n  \"years\":{\"y1\":number|null,\"y2\":number|null},\n  \"category\":\"Safe\"|\"Semi-Critical\"|\"Critical\"|\"Over-Exploited\"|null}\nRules: If 'latest' leave years null. If one year, y1=y2. Prefer exact names.\nQuestion: \"\"\"${question}\"\"\"`;
  const raw = await geminiGenerate(GEMINI_MODEL, prompt);
  let out = {};
  try { out = JSON.parse(raw); } catch {}
  const intent   = out.intent || "DATA";
  const place    = out.place || { level: "unknown" };
  const years    = out.years || {};
  const category = out.category || undefined;
  return { intent, place, years, category, engine: "gemini" };
}

function trimmedRows(rows) {
  return rows.slice(0, 80).map(r => ({
    state: r.state, district: r.district, year: r.year,
    recharge_mcm: r.recharge_mcm == null ? null : +fmt1(r.recharge_mcm),
    extractable_mcm: r.extractable_mcm == null ? null : +fmt1(r.extractable_mcm),
    extraction_mcm:  r.extraction_mcm  == null ? null : +fmt1(r.extraction_mcm),
    stage_pct:       r.stage_pct       == null ? null : +fmt1(r.stage_pct),
    category: r.category
  }));
}
function simpleSummary(_question, parsed, rows, _meta) {
  if (!rows.length) return "I could not find any rows for that question. Please try another place or year.";
  const placeText = parsed?.place?.level === "district"
    ? `${parsed.place.district}, ${parsed.place.state}`
    : (parsed?.place?.state || "India");
  const years = Array.from(new Set(rows.map(r=>r.year).filter(Boolean))).sort();
  const yrSpan = years.length ? (years[0]===years[years.length-1] ? `${years[0]}` : `${years[0]}–${years[years.length-1]}`) : "latest";
  if (parsed?.place?.level === "district" && rows.length >= 2) {
    const last = rows[rows.length - 1];
    const pct  = fmt1(last.stage_pct);
    return `Here is the stage trend for ${placeText} (${yrSpan}). The latest stage is ${pct}%.`;
  }
  const top = [...rows]
    .filter(r => r.stage_pct != null)
    .sort((a,b)=>(b.stage_pct)-(a.stage_pct))
    .slice(0,3)
    .map(r => `${r.district} (${fmt1(r.stage_pct)}%)`);
  return `Here are the latest results for ${placeText} (${yrSpan}). Worst-affected districts include ${top.join(", ")}.`;
}
async function geminiSummarize(question, parsed, rows, meta) {
  if (!GEMINI_KEY) return simpleSummary(question, parsed, rows, meta);
  const sr = trimmedRows(rows);
  const placeText = parsed?.place?.level === "district"
    ? `${parsed.place.district}, ${parsed.place.state}`
    : (parsed?.place?.state || "India");
  const years = Array.from(new Set(sr.map(r=>r.year).filter(Boolean))).sort();
  const yrSpan = years.length ? (years[0]===years[years.length-1] ? `${years[0]}` : `${years[0]}–${years[years.length-1]}`) : "latest";
  const prompt = `You are a helpful assistant for India's groundwater (INGRES/CGWB data).\nWrite a short, clear answer. 2–6 sentences. If listing, use \"- \" bullets.\nMention the place and the year(s) used: ${placeText} (${yrSpan}).\nINPUT:${JSON.stringify({ question, parsed, meta, sample_rows: sr }, null, 2)}`;
  try { return await geminiGenerate(GEMINI_MODEL, prompt, 15000); } catch { return simpleSummary(question, parsed, rows, meta); }
}

// ---------- KPIs + chart payload ----------
function buildPayload(parsed, rows, meta) {
  const payload = {};
  const years = Array.from(new Set(rows.map(r=>r.year).filter(Boolean))).sort();
  const singleYear = years.length === 1 ? years[0] : null;

  // include place + year for frontend auto-map
  payload.place = parsed.place || { level: "unknown" };
  payload.yearUsed = meta?.yearUsed || (singleYear ? { min: singleYear, max: singleYear } : null);

  if (parsed?.place?.level === "state" && singleYear) {
    const total = rows.length;
    const byCat = rows.reduce((acc, r)=>{ const k = r.category || "Unknown"; acc[k]=(acc[k]||0)+1; return acc;}, {});
    payload.kpis = [
      { label: `Districts (${singleYear})`, value: total },
      { label: "Over-Exploited", value: byCat["Over-Exploited"]||0 },
      { label: "Critical", value: byCat["Critical"]||0 },
      { label: "Semi-Critical", value: byCat["Semi-Critical"]||0 },
    ];
    payload.catBreakdown = byCat; // for stacked bar
  }

  if (parsed?.place?.level === "district" && rows.length >= 2) {
    payload.chart = {
      label: "Stage %",
      labels: rows.map(r=>r.year),
      values: rows.map(r=>Number(fmt1(r.stage_pct))),
      xLabel: "Year", yLabel: "Stage (%)"
    };
  }

  payload.source = singleYear ? `INGRES/CGWB (year ${singleYear})` : "INGRES/CGWB (latest available)";
  return payload;
}

// ---------- ROUTES ----------
app.get("/health", (_req,res)=>{
  const row = db.prepare("SELECT COUNT(*) c FROM gw_assessment_core").get();
  res.json({ ok:true, rows: row.c });
});

// Chat answer (existing)
app.post("/api/answer", async (req, res) => {
  try {
    const question = collapseWs((req.body?.text || "").toString());
    if (!question) return res.status(400).json({ error: "Missing text" });

    // 1) deterministic parse
    let parsed = {
      intent: intentOf(question),
      years: parseYearsAbsolute(question),
      place: resolvePlaceSmart(question),
      category: (()=>{
        const s = question.toLowerCase();
        if (s.includes("over-exploited") || s.includes("overexploited") || s.includes("अति")) return "Over-Exploited";
        if (s.includes("critical") || s.includes("गंभीर")) return "Critical";
        if (s.includes("semi") || s.includes("अर्ध")) return "Semi-Critical";
        if (s.includes("safe") || s.includes("सुरक्षित")) return "Safe";
        return undefined;
      })(),
      engine: "deterministic"
    };

    // 2) relative year handling
    const latestForRel = latestYearForState(parsed.place?.state);
    const rel = parseYearsRelative(question, latestForRel);
    if (!parsed.years?.y1 && rel) parsed.years = rel;

    // 3) If place unknown -> try Gemini parse (optional)
    if (parsed.place.level === "unknown" && GEMINI_KEY) {
      try {
        const llm = await geminiParse(question);
        parsed = { ...parsed, ...llm, engine: "gemini" };
        const latest2 = latestYearForState(parsed.place?.state);
        const rel2 = parseYearsRelative(question, latest2);
        if (!parsed.years?.y1 && rel2) parsed.years = rel2;
      } catch {}
    }

    // 4) Build + run SQL
    let sql = buildSQL(parsed);
    if (/;.*\S/.test(sql)) return res.status(400).json({ error: "Invalid SQL" });
    let rows = db.prepare(sql).all();

    // 5) Fallbacks if no rows
    if (!rows.length) {
      try {
        if (GEMINI_KEY) {
          const llm = await geminiParse(question);
          if (llm.place?.level === "state") {
            const hit = db.prepare("SELECT 1 FROM gw_assessment_core WHERE state=? LIMIT 1").get(llm.place.state);
            if (hit) {
              parsed = { ...parsed, ...llm, engine: "gemini" };
              const latest3 = latestYearForState(parsed.place?.state);
              const rel3 = parseYearsRelative(question, latest3);
              if (!parsed.years?.y1 && rel3) parsed.years = rel3;
              sql = buildSQL(parsed);
              rows = db.prepare(sql).all();
            }
          } else if (llm.place?.level === "district") {
            const hit = db.prepare("SELECT 1 FROM gw_assessment_core WHERE state=? AND district=? LIMIT 1")
                          .get(llm.place.state, llm.place.district);
            if (hit) {
              parsed = { ...parsed, ...llm, engine: "gemini" };
              const latest3 = latestYearForState(parsed.place?.state);
              const rel3 = parseYearsRelative(question, latest3);
              if (!parsed.years?.y1 && rel3) parsed.years = rel3;
              sql = buildSQL(parsed);
              rows = db.prepare(sql).all();
            }
          }
        }
      } catch {}
      if (!rows.length && rel) {
        const latest = latestYearForState(parsed.place?.state);
        parsed.years = { y1: latest, y2: latest };
        sql = buildSQL(parsed);
        rows = db.prepare(sql).all();
      }
    }

    const years = rows.map(r=>r.year).filter(y=>y!=null);
    const meta  = { sql, yearUsed: years.length ? { min: Math.min(...years), max: Math.max(...years) } : null };

    const payload = buildPayload(parsed, rows, meta);
    const answer  = await geminiSummarize(question, parsed, rows, meta);
    res.json({ answer, payload, debug: req.query.debug ? { parsed, sql, rows: rows.slice(0,10) } : undefined });
  } catch (e) {
    res.status(500).json({ error: String(e?.message || e) });
  }
});

// --- NEW: Map/Chart helper APIs ---
// District snapshot for a state + year (or latest)
app.get("/api/state-map", (req, res) => {
  try {
    const state = String(req.query.state||"").trim();
    if (!state) return res.status(400).json({ error: "Missing state" });
    let year = req.query.year ? parseInt(req.query.year,10) : latestYearForState(state);
    if (!year) return res.status(404).json({ error: "No year found for state" });
    const rows = db.prepare("SELECT district, category, stage_pct, year FROM gw_assessment_core WHERE state=? AND year=?")
                   .all(state, year)
                   .map(r => ({ district: r.district, category: r.category, stage_pct: +fmt1(r.stage_pct), year: r.year }));
    res.json({ state, year, rows });
  } catch(e){ res.status(500).json({ error: String(e?.message||e) }); }
});

// Category counts for stacked bar
app.get("/api/state-cats", (req,res)=>{
  try {
    const state = String(req.query.state||"").trim();
    if (!state) return res.status(400).json({ error: "Missing state" });
    let year = req.query.year ? parseInt(req.query.year,10) : latestYearForState(state);
    if (!year) return res.status(404).json({ error: "No year found for state" });
    const rows = db.prepare("SELECT category, COUNT(*) c FROM gw_assessment_core WHERE state=? AND year=? GROUP BY category").all(state, year);
    const out = { state, year, counts: { "Safe":0, "Semi-Critical":0, "Critical":0, "Over-Exploited":0 } };
    for (const r of rows) if (out.counts[r.category]!=null) out.counts[r.category] = r.c;
    res.json(out);
  } catch(e){ res.status(500).json({ error: String(e?.message||e) }); }
});

// District trend (for tooltip click)
app.get("/api/district-trend", (req,res)=>{
  try {
    const state = String(req.query.state||"").trim();
    const district = String(req.query.district||"").trim();
    if (!state || !district) return res.status(400).json({ error: "Missing state or district" });
    const rows = db.prepare("SELECT year, stage_pct FROM gw_assessment_core WHERE state=? AND district=? ORDER BY year")
                   .all(state, district)
                   .map(r => ({ year: r.year, stage_pct: +fmt1(r.stage_pct) }));
    res.json({ state, district, rows });
  } catch(e){ res.status(500).json({ error: String(e?.message||e) }); }
});

module.exports = app;
module.exports.handler = serverless(app);
