// One-off: tag jewelry_products with security_level from the Excel/CSV Liran
// provided (the list of level-2 = website-approved items).
//   - Items in the file  → security_level = 2
//   - Items NOT in file  → security_level = 1
//   - Future items added by the nightly sync arrive with NULL (unclassified)
//     until a fresh list is applied — rerun this script with a new file.
//
// Usage: node apply_security_levels.js "C:\path\to\Jewelry_Web.csv"

const fs = require("fs");
const { parse: parseCsv } = require("csv-parse/sync");
const { pool } = require("./db/client");

(async () => {
  const file = process.argv[2];
  if (!file || !fs.existsSync(file)) {
    console.error("Usage: node apply_security_levels.js <csv file with Model Number column>");
    process.exit(1);
  }

  const rows = parseCsv(fs.readFileSync(file, "utf8"), {
    columns: true,
    skip_empty_lines: true,
    relax_quotes: true,
    relax_column_count: true,
  });
  const models = [...new Set(rows.map((r) => String(r["Model Number"] || "").trim()).filter(Boolean))];
  console.log(`File: ${rows.length} rows, ${models.length} distinct model numbers (level 2)`);

  await pool.query(`ALTER TABLE jewelry_products ADD COLUMN IF NOT EXISTS security_level SMALLINT`);

  const up2 = await pool.query(
    `UPDATE jewelry_products SET security_level = 2 WHERE model_number = ANY($1::text[])`,
    [models]
  );
  const up1 = await pool.query(
    `UPDATE jewelry_products SET security_level = 1 WHERE NOT (model_number = ANY($1::text[]))`,
    [models]
  );
  console.log(`Tagged level 2: ${up2.rowCount} items`);
  console.log(`Tagged level 1: ${up1.rowCount} items`);

  // Which file entries have no matching row in the DB (sold / renamed)?
  const found = await pool.query(
    `SELECT model_number FROM jewelry_products WHERE model_number = ANY($1::text[])`,
    [models]
  );
  const inDb = new Set(found.rows.map((r) => r.model_number));
  const missing = models.filter((m) => !inDb.has(m));
  if (missing.length) {
    console.log(`\n${missing.length} model number(s) from the file are not in the system (sold / renamed?):`);
    for (const m of missing) console.log(` - ${m}`);
  } else {
    console.log("\nEvery model number in the file exists in the system.");
  }

  const dist = await pool.query(
    `SELECT COALESCE(security_level::text, 'unclassified') AS lvl, COUNT(*)::int AS n
       FROM jewelry_products GROUP BY 1 ORDER BY 1`
  );
  console.log("\nFinal distribution:");
  for (const r of dist.rows) console.log(` - level ${r.lvl}: ${r.n}`);

  await pool.end();
})().catch((e) => {
  console.error("FAILED:", e);
  process.exit(1);
});
