// One-off: clear stale HOLD tags. A stone is stale when our DB has a holder
// but the fresh Barak export shows its Holder column empty (hold released).
// Stones absent from the export are left untouched.
//
// Usage: node fix_stale_holds.js "C:\path\to\Diamonds....csv"

const fs = require("fs");
const { parse: parseCsv } = require("csv-parse/sync");
const { pool } = require("./db/client");

(async () => {
  const file = process.argv[2];
  if (!file || !fs.existsSync(file)) {
    console.error("Usage: node fix_stale_holds.js <fresh Barak CSV export>");
    process.exit(1);
  }

  const rows = parseCsv(fs.readFileSync(file, "utf8"), {
    columns: true,
    skip_empty_lines: true,
    relax_quotes: true,
    relax_column_count: true,
  });
  const holderBySku = new Map();
  for (const r of rows) {
    const sku = String(r["SKU"] || "").trim().toUpperCase();
    if (sku) holderBySku.set(sku, String(r["Holder"] || "").trim());
  }
  console.log(`Export: ${rows.length} rows, ${holderBySku.size} SKUs`);

  const db = await pool.query(
    `SELECT sku, holder FROM soap_stones WHERE holder IS NOT NULL AND TRIM(holder) <> ''`
  );
  console.log(`DB stones currently marked HOLD: ${db.rowCount}`);

  const stale = [];
  const real = [];
  const notInFile = [];
  for (const r of db.rows) {
    const key = String(r.sku || "").trim().toUpperCase();
    if (!holderBySku.has(key)) notInFile.push(r);
    else if (holderBySku.get(key) === "") stale.push(r);
    else real.push(r);
  }

  console.log(`\nReal holds (export agrees): ${real.length}`);
  console.log(`Not in export (left untouched): ${notInFile.length}`);
  for (const r of notInFile) console.log(`   ? ${r.sku}: ${r.holder}`);
  console.log(`\nSTALE holds to clear (export shows released): ${stale.length}`);
  for (const r of stale) console.log(`   x ${r.sku}: ${r.holder}`);

  if (stale.length) {
    const res = await pool.query(
      `UPDATE soap_stones SET holder = NULL WHERE sku = ANY($1::text[])`,
      [stale.map((r) => r.sku)]
    );
    console.log(`\nCleared holder on ${res.rowCount} stones.`);
  }

  const t = await pool.query(`SELECT sku, holder FROM soap_stones WHERE sku = 'T9577'`);
  console.log(`\nT9577 now: holder = ${t.rows[0]?.holder ?? "(none)"}`);

  await pool.end();
})().catch((e) => { console.error("FAILED:", e); process.exit(1); });
