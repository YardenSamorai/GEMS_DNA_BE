// One-time loader for jewelry_products.real_unit_cost.
//
// The daily Jewelry_Web.csv declares a real_unit_cost column but ships it
// empty on every row, so the figures have to come from a separate file that
// Barak keeps by hand. The nightly sync is written to leave an existing cost
// alone when the feed sends nothing (see importJewelryFromFtp.js), which is
// what makes a load like this durable rather than something the next sync
// erases.
//
// Usage:
//   node api/stones/backfillJewelryCost.js <file.csv>              # dry run
//   node api/stones/backfillJewelryCost.js <file.csv> --commit     # write
//
//   --model=<header>   force the model-number column (default: auto-detected)
//   --cost=<header>    force the cost column          (default: auto-detected)
//
// A dry run is the default on purpose: it prints exactly which pieces would
// change and what from, so the file can be checked before anything is written.

const fs = require("fs");
const path = require("path");
const { parse: parseCsv } = require("csv-parse/sync");
const { pool } = require("../../db/client");

const norm = (v) => String(v ?? "").trim().toUpperCase();

/* Money as people type it: "$1,234.50", "1234.5", "1 234". Anything that isn't
 * a positive number comes back null so a stray "N/A" is skipped rather than
 * silently stored as zero. */
const parseCost = (v) => {
  if (v === null || v === undefined) return null;
  const cleaned = String(v).replace(/[^0-9.-]/g, "");
  if (cleaned === "" || cleaned === "-" || cleaned === ".") return null;
  const n = Number(cleaned);
  return Number.isFinite(n) && n > 0 ? n : null;
};

/* Which column holds the model number is settled by evidence, not by the
 * header text: whichever column's values match the most rows already in the
 * catalog wins. A file that labels the column "Item", "Style #" or nothing at
 * all still lands correctly. */
const detectModelColumn = (rows, headers, known) => {
  let best = { header: null, hits: 0 };
  for (const h of headers) {
    let hits = 0;
    for (const r of rows) if (known.has(norm(r[h]))) hits++;
    if (hits > best.hits) best = { header: h, hits };
  }
  return best;
};

/* The cost column is chosen by name — there is nothing in the values that
 * distinguishes a cost from a price, so guessing from the numbers would be a
 * good way to import the wrong figure. Most specific name wins. */
const COST_PATTERNS = [
  /^real[_ ]?unit[_ ]?cost$/i,
  /^jewelry[_ ]?cost$/i,
  /^unit[_ ]?cost$/i,
  /^total[_ ]?cost$/i,
  /\bcost\b/i,
];
const detectCostColumn = (headers) => {
  for (const pattern of COST_PATTERNS) {
    const hit = headers.find((h) => pattern.test(String(h).trim()));
    if (hit) return hit;
  }
  return null;
};

const flagValue = (name) => {
  const hit = process.argv.find((a) => a.startsWith(`--${name}=`));
  return hit ? hit.slice(name.length + 3) : null;
};

const main = async () => {
  const file = process.argv[2];
  const commit = process.argv.includes("--commit");

  if (!file || file.startsWith("--")) {
    console.error("Usage: node api/stones/backfillJewelryCost.js <file.csv> [--commit]");
    process.exit(1);
  }
  if (!fs.existsSync(file)) {
    console.error(`File not found: ${path.resolve(file)}`);
    process.exit(1);
  }
  if (/\.xlsx?$/i.test(file)) {
    console.error("This reads CSV. Save the sheet as CSV (UTF-8) and re-run.");
    process.exit(1);
  }

  const rows = parseCsv(fs.readFileSync(file), {
    columns: true,
    skip_empty_lines: true,
    relax_quotes: true,
    relax_column_count: true,
    bom: true,
  });
  if (!rows.length) {
    console.error("The file has no data rows.");
    process.exit(1);
  }
  const headers = Object.keys(rows[0]);
  console.log(`Read ${rows.length} rows from ${path.basename(file)}`);
  console.log(`Columns: ${headers.join(" | ")}\n`);

  const catalog = await pool.query(
    `SELECT model_number, title, price, real_unit_cost FROM jewelry_products`
  );
  const byModel = new Map(catalog.rows.map((r) => [norm(r.model_number), r]));
  console.log(`Catalog holds ${catalog.rows.length} pieces\n`);

  const forcedModel = flagValue("model");
  const forcedCost = flagValue("cost");

  const modelCol = forcedModel || detectModelColumn(rows, headers, byModel).header;
  const costCol = forcedCost || detectCostColumn(headers);

  if (!modelCol) {
    console.error("No column in this file matches any model number in the catalog.");
    console.error("Point at it explicitly with --model=<header>.");
    process.exit(1);
  }
  if (!costCol) {
    console.error(`No cost column found. Pass one with --cost=<header>.`);
    process.exit(1);
  }
  console.log(`Model number ← "${modelCol}"${forcedModel ? " (forced)" : ""}`);
  console.log(`Cost         ← "${costCol}"${forcedCost ? " (forced)" : ""}\n`);

  const updates = new Map(); // model_number → cost
  const unmatched = [];
  const unparseable = [];
  const conflicts = [];

  for (const r of rows) {
    const key = norm(r[modelCol]);
    if (!key) continue;
    const piece = byModel.get(key);
    if (!piece) {
      unmatched.push({ model: r[modelCol], cost: r[costCol] });
      continue;
    }
    const cost = parseCost(r[costCol]);
    if (cost === null) {
      if (String(r[costCol] ?? "").trim() !== "") unparseable.push({ model: key, raw: r[costCol] });
      continue;
    }
    // The same piece listed twice with two different costs is a question for
    // whoever keeps the file, not something to resolve by taking the last row.
    const seen = updates.get(piece.model_number);
    if (seen !== undefined && seen !== cost) {
      conflicts.push({ model: piece.model_number, costs: [seen, cost] });
    }
    updates.set(piece.model_number, cost);
  }

  const sample = [...updates.entries()].slice(0, 15);
  console.log(`Would set a cost on ${updates.size} of ${catalog.rows.length} catalog pieces.`);
  if (sample.length) {
    console.log("\nSample:");
    for (const [model, cost] of sample) {
      const piece = byModel.get(norm(model));
      const was = piece.real_unit_cost === null ? "—" : `$${Number(piece.real_unit_cost).toLocaleString()}`;
      const price = piece.price === null ? "—" : `$${Number(piece.price).toLocaleString()}`;
      console.log(
        `  ${model.padEnd(18)} price ${price.padStart(12)}   cost ${was} → $${cost.toLocaleString()}`
      );
    }
    if (updates.size > sample.length) console.log(`  … and ${updates.size - sample.length} more`);
  }

  // A cost above the asking price is legal (a piece can be underwater) but is
  // far more often a column mix-up, so it gets called out before the write.
  const overPrice = [...updates.entries()].filter(([model, cost]) => {
    const p = byModel.get(norm(model))?.price;
    return p !== null && p !== undefined && cost > Number(p);
  });
  if (overPrice.length) {
    console.log(`\n⚠️  ${overPrice.length} piece(s) would get a cost above their asking price:`);
    for (const [model, cost] of overPrice.slice(0, 10)) {
      const p = Number(byModel.get(norm(model)).price);
      console.log(`  ${model.padEnd(18)} price $${p.toLocaleString()} < cost $${cost.toLocaleString()}`);
    }
  }
  if (conflicts.length) {
    console.log(`\n⚠️  ${conflicts.length} piece(s) appear twice with different costs:`);
    for (const c of conflicts.slice(0, 10)) console.log(`  ${c.model}: ${c.costs.join(" vs ")}`);
  }
  if (unparseable.length) {
    console.log(`\n⚠️  ${unparseable.length} row(s) had a cost that isn't a number, skipped:`);
    for (const u of unparseable.slice(0, 10)) console.log(`  ${u.model}: ${JSON.stringify(u.raw)}`);
  }
  if (unmatched.length) {
    console.log(`\nℹ️  ${unmatched.length} row(s) name a piece that isn't in the catalog, skipped:`);
    for (const u of unmatched.slice(0, 10)) console.log(`  ${u.model}`);
  }

  const missing = catalog.rows.filter((r) => !updates.has(r.model_number) && r.real_unit_cost === null);
  console.log(`\n${missing.length} catalog piece(s) would still have no cost.`);

  if (!commit) {
    console.log("\nDry run — nothing written. Re-run with --commit to apply.");
    await pool.end();
    return;
  }

  let written = 0;
  for (const [model, cost] of updates) {
    const r = await pool.query(
      `UPDATE jewelry_products SET real_unit_cost = $2 WHERE model_number = $1`,
      [model, cost]
    );
    written += r.rowCount || 0;
  }
  console.log(`\n✅ Wrote a cost onto ${written} piece(s).`);
  await pool.end();
};

main().catch(async (e) => {
  console.error("FAILED:", e.message);
  await pool.end().catch(() => {});
  process.exit(1);
});
