/* Compare a Barak price export against what the system actually holds and shows.
 *
 *   node compare_export_to_db.js <path-to-export.csv> [--api <base-url>]
 *
 * Answers three questions in one pass:
 *
 *   1. Did the file land in the database intact?
 *      Every price in the export is compared with the stored value. Any
 *      difference means the import chain altered a price, which is exactly the
 *      failure that caused coloured stones to be sold at a quarter of their
 *      value.
 *
 *   2. What does the sales floor show now, and what did it show before?
 *      The old rule halved every coloured stone that was not an emerald. The
 *      new rule shows the stored price for every category. The report lists
 *      which stones changed, by how much, and what the totals come to.
 *
 *   3. Does the export itself hold together?
 *      Runs the same integrity checks the importer now runs automatically
 *      (see utils/priceIntegrity.js) directly against the file.
 *
 * Accepts CSV or tab-separated exports. For .xlsx, save as CSV first.
 */

const fs = require("fs");
const path = require("path");
const https = require("https");
const http = require("http");
const { parse: parseCsv } = require("csv-parse/sync");
const { runPriceIntegrityChecks } = require("./utils/priceIntegrity");

const DEFAULT_API = "https://gems-dna-be.onrender.com";
const MONEY_TOLERANCE = 0.01; // absolute dollars — guards against float noise

/* -------------------------------------------------------------------------
 * Argument handling
 * ---------------------------------------------------------------------- */
const args = process.argv.slice(2);
const filePath = args.find((a) => !a.startsWith("--"));
const apiIdx = args.indexOf("--api");
const apiBase = apiIdx >= 0 ? args[apiIdx + 1] : DEFAULT_API;

if (!filePath) {
  console.error("Usage: node compare_export_to_db.js <path-to-export.csv> [--api <base-url>]");
  process.exit(1);
}
if (!fs.existsSync(filePath)) {
  console.error(`File not found: ${path.resolve(filePath)}`);
  process.exit(1);
}

/* -------------------------------------------------------------------------
 * Column resolution — export headers drift over time, so match loosely
 * ---------------------------------------------------------------------- */
const normaliseHeader = (h) => String(h || "").toLowerCase().replace(/[^a-z0-9]/g, "");

const COLUMN_ALIASES = {
  sku: ["sku", "stoneid", "stone", "itemid"],
  category: ["category", "type"],
  weight: ["weight", "weightct", "carat", "carats", "ct"],
  ppc: ["pricepercarat", "priceperct", "ppc", "pricect", "usdct"],
  total: ["totalprice", "total", "price", "amount"],
  grouping: ["groupingtype", "grouping"],
};

function resolveColumns(headers) {
  const map = {};
  const normalised = headers.map((h) => ({ raw: h, key: normaliseHeader(h) }));

  for (const [field, aliases] of Object.entries(COLUMN_ALIASES)) {
    // Exact alias match first, then a prefix match, so "Price Per Carat " and
    // "Weight (ct)" both resolve without hand-maintaining every variant.
    let hit = normalised.find((h) => aliases.includes(h.key));
    if (!hit) hit = normalised.find((h) => aliases.some((a) => h.key.startsWith(a)));
    if (hit) map[field] = hit.raw;
  }
  return map;
}

/* The Barak export ships "Rap Price % " (the discount) and "Rap. Price" (the
 * list price). Both collapse to the same key once punctuation is stripped, so
 * they are resolved separately: by the '%' marker when it survives the export,
 * and otherwise by magnitude — a discount lives in the tens, a list price in
 * the thousands. Getting these two backwards would silently invert the single
 * strongest check we have, so the magnitude test also runs as a correction
 * even when the headers looked unambiguous. */
function resolveRapColumns(headers, rows) {
  const rapHeaders = headers.filter((h) => normaliseHeader(h).startsWith("rap"));
  if (rapHeaders.length < 2) {
    return rapHeaders.length === 1 ? { rapPct: rapHeaders[0] } : {};
  }

  let pct =
    rapHeaders.find((h) => String(h).includes("%")) ||
    rapHeaders.find((h) => normaliseHeader(h).includes("percent")) ||
    rapHeaders[0];
  let list = rapHeaders.find((h) => h !== pct) || rapHeaders[1];

  const typicalMagnitude = (col) => {
    const vals = rows
      .map((r) => num(r[col]))
      .filter((n) => n != null && n !== 0)
      .map(Math.abs)
      .sort((a, b) => a - b);
    return vals.length ? vals[Math.floor(vals.length / 2)] : 0;
  };

  if (typicalMagnitude(pct) > typicalMagnitude(list)) {
    [pct, list] = [list, pct];
  }
  return { rapPct: pct, rapList: list };
}

const num = (v) => {
  if (v == null) return null;
  const cleaned = String(v).replace(/[$,\s]/g, "");
  if (cleaned === "") return null;
  const n = Number(cleaned);
  return Number.isFinite(n) ? n : null;
};

/* -------------------------------------------------------------------------
 * The sales rule as it stood BEFORE the fix.
 *
 * Every category the frontend's category map resolved to Diamond, Fancy or
 * Emerald was shown at its stored price; everything else was halved. This is a
 * frozen copy of that behaviour on purpose — the live rule no longer exists in
 * the codebase, and this list is only here so the report can say what each
 * stone used to display.
 * ---------------------------------------------------------------------- */
const OLD_SHOWN_AS_IS = new Set([
  "Diamond", "Diamond O", "Diamonds MEMO", "Diamonds Virtual",
  "Fancy",
  "Emerald", "Emerald M", "Emerald Memo", "Emerald N", "Emerald O",
  "Emerald Virtual", "Aquamarine+Emerald O", "EM+AQ+PT O",
]);

const oldSalesDivisor = (category) =>
  OLD_SHOWN_AS_IS.has(String(category ?? "").trim()) ? 1 : 2;

/* -------------------------------------------------------------------------
 * Fetch the live catalog
 * ---------------------------------------------------------------------- */
function fetchJson(url) {
  const client = url.startsWith("https") ? https : http;
  return new Promise((resolve, reject) => {
    client
      .get(url, (res) => {
        let body = "";
        res.on("data", (c) => (body += c));
        res.on("end", () => {
          try {
            resolve(JSON.parse(body));
          } catch (e) {
            reject(new Error(`Bad JSON from ${url}: ${e.message}`));
          }
        });
      })
      .on("error", reject);
  });
}

const money = (n) =>
  n == null ? "-" : `$${Math.round(n).toLocaleString("en-US")}`;

const line = (char = "-") => console.log(char.repeat(78));

/* -------------------------------------------------------------------------
 * Main
 * ---------------------------------------------------------------------- */
(async () => {
  const raw = fs.readFileSync(filePath, "utf8");
  const delimiter = raw.split("\n")[0].includes("\t") ? "\t" : ",";

  const rows = parseCsv(raw, {
    columns: true,
    skip_empty_lines: true,
    relax_quotes: true,
    relax_column_count: true,
    bom: true,
    delimiter,
    trim: true,
  });

  if (!rows.length) {
    console.error("The export parsed to zero rows — check the delimiter/encoding.");
    process.exit(1);
  }

  const headers = Object.keys(rows[0]);
  const cols = { ...resolveColumns(headers), ...resolveRapColumns(headers, rows) };
  console.log(`\nParsed ${rows.length.toLocaleString()} rows from ${path.basename(filePath)}`);
  console.log(`Detected columns: ${JSON.stringify(cols)}\n`);

  for (const required of ["sku", "ppc", "total"]) {
    if (!cols[required]) {
      console.error(`Could not find a "${required}" column. Headers seen:`);
      console.error(Object.keys(rows[0]).join(" | "));
      process.exit(1);
    }
  }

  const fileStones = new Map();
  for (const r of rows) {
    const sku = String(r[cols.sku] ?? "").trim();
    if (!sku) continue;
    fileStones.set(sku, {
      sku,
      category: cols.category ? String(r[cols.category] ?? "").trim() : "",
      grouping_type: cols.grouping ? String(r[cols.grouping] ?? "").trim() : "",
      weight: cols.weight ? num(r[cols.weight]) : null,
      price_per_carat: num(r[cols.ppc]),
      total_price: num(r[cols.total]),
      rap_price: cols.rapPct ? num(r[cols.rapPct]) : null,
      rap_list_price: cols.rapList ? num(r[cols.rapList]) : null,
    });
  }

  console.log("Fetching the live catalog…");
  const payload = await fetchJson(`${apiBase}/api/soap-stones`);
  const dbRows = Array.isArray(payload.stones) ? payload.stones : payload;
  const dbStones = new Map();
  for (const s of dbRows) {
    if (s.sku) dbStones.set(String(s.sku).trim(), s);
  }
  console.log(`Live catalog holds ${dbStones.size.toLocaleString()} stones.\n`);

  /* ---- 1. Coverage -------------------------------------------------- */
  line("=");
  console.log("1. COVERAGE");
  line("=");
  const onlyInFile = [];
  const onlyInDb = [];
  const matched = [];
  for (const [sku, f] of fileStones) {
    if (dbStones.has(sku)) matched.push({ file: f, db: dbStones.get(sku) });
    else onlyInFile.push(f);
  }
  for (const sku of dbStones.keys()) {
    if (!fileStones.has(sku)) onlyInDb.push(sku);
  }
  console.log(`  in export        : ${fileStones.size.toLocaleString()}`);
  console.log(`  in database      : ${dbStones.size.toLocaleString()}`);
  console.log(`  matched by SKU   : ${matched.length.toLocaleString()}`);
  console.log(`  only in export   : ${onlyInFile.length.toLocaleString()}${onlyInFile.length ? "  e.g. " + onlyInFile.slice(0, 5).map((s) => s.sku).join(", ") : ""}`);
  console.log(`  only in database : ${onlyInDb.length.toLocaleString()}${onlyInDb.length ? "  e.g. " + onlyInDb.slice(0, 5).join(", ") : ""}`);

  /* ---- 2. File vs database ------------------------------------------ */
  line("=");
  console.log("2. EXPORT vs DATABASE  (any difference means the import altered a price)");
  line("=");
  const priceDiffs = [];
  for (const { file, db } of matched) {
    const dbPpc = num(db.pricePerCt);
    const dbTotal = num(db.priceTotal);
    const ppcDiff = file.price_per_carat != null && dbPpc != null
      ? Math.abs(file.price_per_carat - dbPpc) : 0;
    const totalDiff = file.total_price != null && dbTotal != null
      ? Math.abs(file.total_price - dbTotal) : 0;

    if (ppcDiff > MONEY_TOLERANCE || totalDiff > MONEY_TOLERANCE) {
      priceDiffs.push({
        sku: file.sku,
        category: file.category || db.category,
        filePpc: file.price_per_carat,
        dbPpc,
        ratio: file.price_per_carat && dbPpc ? dbPpc / file.price_per_carat : null,
      });
    }
  }

  if (!priceDiffs.length) {
    console.log("  ✅ Every matched stone holds exactly the price from the export.");
    console.log("     The import chain does not alter prices.");
  } else {
    console.log(`  ⚠️  ${priceDiffs.length.toLocaleString()} stone(s) differ between the export and the database.\n`);
    const ratios = {};
    for (const d of priceDiffs) {
      const key = d.ratio ? d.ratio.toFixed(2) : "n/a";
      ratios[key] = (ratios[key] || 0) + 1;
    }
    console.log("  db / file ratio distribution (2.00 or 0.50 = systematic scaling):");
    Object.entries(ratios)
      .sort((a, b) => b[1] - a[1])
      .slice(0, 8)
      .forEach(([r, n]) => console.log(`     x${r.padEnd(6)} ${n.toLocaleString()} stone(s)`));
    console.log("\n  first 15:");
    console.log("     SKU            category         export      database    ratio");
    priceDiffs.slice(0, 15).forEach((d) =>
      console.log(
        `     ${String(d.sku).padEnd(14)} ${String(d.category || "").padEnd(16)} ${money(d.filePpc).padStart(10)} ${money(d.dbPpc).padStart(11)}   ${d.ratio ? "x" + d.ratio.toFixed(2) : "-"}`
      )
    );
  }

  /* ---- 3. Sales floor: before vs after ------------------------------ */
  line("=");
  console.log("3. SALES FLOOR  —  before the fix vs now");
  line("=");
  const changed = [];
  let beforeSum = 0;
  let afterSum = 0;
  for (const [, db] of dbStones) {
    const total = num(db.priceTotal);
    if (!total) continue;
    const divisor = oldSalesDivisor(db.category);
    const before = total / divisor;
    const after = total;
    beforeSum += before;
    afterSum += after;
    if (divisor !== 1) {
      changed.push({ sku: db.sku, category: db.category, before, after, ppc: num(db.pricePerCt) });
    }
  }

  console.log(`  stones whose displayed price changed : ${changed.length.toLocaleString()}`);
  console.log(`  catalog value shown before           : ${money(beforeSum)}`);
  console.log(`  catalog value shown now              : ${money(afterSum)}`);
  console.log(`  value that had been hidden           : ${money(afterSum - beforeSum)}`);

  if (changed.length) {
    const byCategory = {};
    for (const c of changed) {
      const key = c.category || "(none)";
      if (!byCategory[key]) byCategory[key] = { n: 0, before: 0, after: 0 };
      byCategory[key].n++;
      byCategory[key].before += c.before;
      byCategory[key].after += c.after;
    }
    console.log("\n  affected categories:");
    console.log("     category               count        was            now");
    Object.entries(byCategory)
      .sort((a, b) => b[1].n - a[1].n)
      .forEach(([cat, v]) =>
        console.log(`     ${cat.padEnd(22)} ${String(v.n).padStart(5)} ${money(v.before).padStart(13)} ${money(v.after).padStart(14)}`)
      );

    console.log("\n  highest-value corrections:");
    console.log("     SKU            category          was/ct        now/ct");
    changed
      .sort((a, b) => b.after - a.after)
      .slice(0, 10)
      .forEach((c) =>
        console.log(`     ${String(c.sku).padEnd(14)} ${String(c.category || "").padEnd(16)} ${money(c.ppc / 2).padStart(10)} ${money(c.ppc).padStart(13)}`)
      );
  }

  /* ---- 4. Integrity of the export itself ----------------------------- */
  line("=");
  console.log("4. INTEGRITY CHECKS RUN DIRECTLY ON THE EXPORT");
  line("=");
  const report = runPriceIntegrityChecks([...fileStones.values()], null);
  console.log(`  overall: ${report.status.toUpperCase()}\n`);
  for (const c of report.checks) {
    console.log(`  [${c.status.toUpperCase()}] ${c.name}`);
    console.log(`         ${c.detail}`);
    if (c.worstOffenders && c.worstOffenders.length) {
      console.log(`         worst: ${JSON.stringify(c.worstOffenders.slice(0, 3))}`);
    }
  }

  line("=");
  console.log(
    priceDiffs.length
      ? "VERDICT: the database does NOT match the export — see section 2."
      : "VERDICT: the database matches the export exactly. Prices are stored verbatim."
  );
  line("=");
  console.log();
})().catch((e) => {
  console.error("\nComparison failed:", e.message);
  process.exit(1);
});
