/* Import the DNADNA export (Barak "Parcel" CSV format) into soap_stones.
 *
 * This export differs from the legacy Diamonds CSV (importCsv.js) and the SOAP
 * feed (importFromSoap.js): columns are "Parcel", "Price $/Ct.", "Gemology
 * Institute", "cost_per_carat", "Holder", etc. It also carries the two new
 * sales-page fields — cost_per_carat and holder.
 *
 * SAFETY: defaults to a DRY RUN (parse + map + report, no DB writes). Pass
 * --write to actually upsert. Upsert is keyed on sku and uses COALESCE so we
 * never clobber an existing value with a blank from this file (and we never
 * touch rows/columns this export doesn't carry).
 *
 * Pricing: prices are stored exactly as they appear in the CSV (no ×2). The
 * Sales Inventory does its own per-category adjustment on the FE. Pass --double
 * only if you explicitly want the legacy ×2 (bruto) behaviour.
 *
 * Usage:
 *   node importDnaCsv.js "C:/path/to/DNADNA.csv"            # dry run
 *   node importDnaCsv.js "C:/path/to/DNADNA.csv" --write    # apply
 *   node importDnaCsv.js "C:/path/to/DNADNA.csv" --write --double
 */

const fs = require("fs");
const { parse } = require("csv-parse/sync");
const { pool } = require("./db/client");

const BRANCH_MAP = {
  IL: "Israel", EM: "Israel", JI: "Israel", ISR: "Israel",
  LA: "Los Angeles", EL: "Los Angeles",
  HK: "Hong Kong", ES: "Hong Kong", HS: "Hong Kong", JH: "Hong Kong",
  JS: "Hong Kong", EH: "Hong Kong", HKG: "Hong Kong",
  NY: "New York", EN: "New York", ET: "New York", DT: "New York",
  JT: "New York", EG: "New York", EV: "New York", GN: "New York",
  VG: "New York", JG: "New York", JV: "New York", EY: "New York", NYC: "New York",
};

const mapBranch = (b) => {
  if (!b) return null;
  const clean = String(b).trim();
  if (!clean || clean.includes("http") || clean.length > 20) return null;
  return BRANCH_MAP[clean.toUpperCase()] || clean;
};

const safeNum = (v) => {
  if (v === undefined || v === null || String(v).trim() === "") return null;
  const n = parseFloat(String(v).replace(/,/g, ""));
  return Number.isFinite(n) ? n : null;
};

const str = (v) => {
  if (v === undefined || v === null) return null;
  const s = String(v).trim();
  return s === "" ? null : s;
};

// Columns we touch from this export. Anything not here is left untouched on
// existing rows (and defaults to NULL on brand-new rows).
const COLUMNS = [
  "sku", "weight", "certificate_number", "total_price", "cost_per_carat",
  "origin", "pair_stone", "shape", "color", "clarity", "cut", "symmetry",
  "depth_percent", "fluorescence", "polish", "table_percent", "measurements",
  "ratio", "lab", "cert_comments", "image", "certificate_image",
  "additional_pictures", "branch", "additional_videos", "certificate_image_jpg",
  "price_per_carat", "location", "holder",
];

const mapRow = (r, { double }) => {
  const tp = safeNum(r["Price"]);
  const ppc = safeNum(r["Price $/Ct."]);
  return {
    sku: str(r["Parcel"]),
    weight: safeNum(r["Weight"]),
    certificate_number: str(r["Certificate Number"]),
    total_price: tp !== null ? (double ? tp * 2 : tp) : null,
    cost_per_carat: safeNum(r["cost_per_carat"]),
    origin: str(r["Country"]),
    pair_stone: str(r["Pair Parcel"]),
    shape: str(r["Model"]),
    color: str(r["Color"]),
    clarity: str(r["Clarity"]),
    cut: str(r["Cut"]),
    symmetry: str(r["Symmetry"]),
    depth_percent: safeNum(r["Depth"]),
    fluorescence: str(r["Fluorescence"]),
    polish: str(r["Polish"]),
    table_percent: safeNum(r["Table"]),
    measurements: str(r["Measurements (- delimiter)"]),
    ratio: safeNum(r["ratio"]),
    lab: str(r["Gemology Institute "]) || str(r["Gemology Institute"]),
    cert_comments: str(r["Cert. Comments"]),
    image: str(r["Picture Url"]),
    certificate_image: str(r["Certificate Url"]),
    additional_pictures: str(r["additional_pictures"]),
    branch: mapBranch(r["Location Region"]),
    additional_videos: str(r["additional_videos"]),
    certificate_image_jpg: str(r["certificateImageJPG"]),
    price_per_carat: ppc !== null ? (double ? ppc * 2 : ppc) : null,
    location: str(r["Location"]),
    holder: str(r["Holder"]),
  };
};

(async () => {
  const csvPath = process.argv[2] || "c:/Users/yarden/Desktop/DNADNA_2026-06-09 11-32-30.csv";
  const write = process.argv.includes("--write");
  // DB stores the real CSV price as-is. Pass --double for legacy ×2 (bruto).
  const double = process.argv.includes("--double");

  console.log(`File: ${csvPath}`);
  console.log(`Mode: ${write ? "WRITE (upsert)" : "DRY RUN (no writes)"} | price ×2: ${double}`);

  const csv = fs.readFileSync(csvPath, "utf8");
  const rows = parse(csv, { columns: true, skip_empty_lines: true, relax_quotes: true, relax_column_count: true, trim: true });
  console.log(`Parsed rows: ${rows.length}`);

  const mapped = rows.map((r) => mapRow(r, { double })).filter((m) => m.sku);
  console.log(`Rows with a SKU: ${mapped.length}`);

  // SKU prefix distribution (helps spot whether this is the full inventory).
  const prefixes = {};
  for (const m of mapped) {
    const p = (m.sku.match(/^[A-Za-z]+/) || ["?"])[0].toUpperCase();
    prefixes[p] = (prefixes[p] || 0) + 1;
  }
  console.log("SKU prefixes:", Object.entries(prefixes).sort((a, b) => b[1] - a[1]).map(([k, v]) => `${k}:${v}`).join("  "));

  const withCost = mapped.filter((m) => m.cost_per_carat != null).length;
  const withHolder = mapped.filter((m) => m.holder != null).length;
  console.log(`cost_per_carat populated: ${withCost}/${mapped.length}`);
  console.log(`holder populated: ${withHolder}/${mapped.length}`);

  console.log("\nSample mapped rows:");
  for (const m of mapped.slice(0, 3)) {
    console.log(`  ${m.sku} | ${m.shape} | ${m.weight}ct | ppc=${m.price_per_carat} total=${m.total_price} cost/ct=${m.cost_per_carat} | lab=${m.lab} | holder=${m.holder} | branch=${m.branch}`);
  }

  if (!write) {
    console.log("\nDRY RUN complete — no DB writes. Re-run with --write to apply.");
    await pool.end().catch(() => {});
    return;
  }

  // Upsert in chunks. COALESCE(EXCLUDED.col, soap_stones.col) keeps existing
  // values when this file leaves a field blank.
  const updateSet = COLUMNS.filter((c) => c !== "sku")
    .map((c) => `${c} = COALESCE(EXCLUDED.${c}, soap_stones.${c})`)
    .join(", ");

  const CHUNK = 300;
  let done = 0;
  for (let i = 0; i < mapped.length; i += CHUNK) {
    const chunk = mapped.slice(i, i + CHUNK);
    const ph = chunk
      .map((_, ri) => `(${COLUMNS.map((__, ci) => `$${ri * COLUMNS.length + ci + 1}`).join(",")})`)
      .join(",");
    const flat = chunk.flatMap((m) => COLUMNS.map((c) => m[c]));
    await pool.query(
      `INSERT INTO soap_stones (${COLUMNS.join(",")}) VALUES ${ph}
       ON CONFLICT (sku) DO UPDATE SET ${updateSet}, updated_at = NOW()`,
      flat
    );
    done += chunk.length;
    console.log(`  upserted ${done}/${mapped.length}`);
  }

  console.log(`DONE — upserted ${done} rows.`);
  await pool.end().catch(() => {});
})().catch((e) => {
  console.error("ERR", e);
  process.exit(1);
});
