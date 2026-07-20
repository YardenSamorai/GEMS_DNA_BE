// Update manually-maintained sales fields on soap_stones from a DNADNA CSV
// export, matched by SKU (the CSV's "Parcel" column).
//
// These three fields do NOT come from the SOAP feed, so they live only here:
//   cost_per_carat  ← CSV "cost_per_carat"  (internal cost basis, NOT doubled)
//   location        ← CSV "Location"        (e.g. "Office")
//   holder          ← CSV "Holder"          (who physically holds the stone)
//
// This is an UPDATE-by-SKU (no TRUNCATE): it never wipes SOAP-synced columns
// like category/type/color/clarity. A non-empty CSV value overwrites; an empty
// CSV cell leaves the existing value intact — EXCEPT holder, where the export
// is authoritative: an empty Holder means the hold was RELEASED, so it clears
// the field (keeping the old name showed dead HOLD tags on the site forever).
//
// Usage:  node importSalesFields.js ["C:\\path\\to\\export.csv"]

const fs = require('fs');
const { parse } = require('csv-parse/sync');
const { pool } = require('./db/client');

const safeNum = (v) => {
  const n = parseFloat(v);
  return Number.isFinite(n) ? n : null;
};
const txt = (v) => {
  if (v == null) return null;
  const s = String(v).trim();
  return s === '' ? null : s;
};

const CHUNK = 300;

(async () => {
  const csvPath =
    process.argv[2] || 'C:/Users/yarden/Desktop/DNADNA_2026-06-09 11-32-30.csv';
  console.log('Reading CSV:', csvPath);
  const csv = fs.readFileSync(csvPath, 'utf8');
  const rows = parse(csv, {
    columns: (header) => header.map((h) => h.trim()),
    skip_empty_lines: true,
    relax_quotes: true,
    relax_column_count: true,
    trim: true,
  });
  console.log('Parsed rows:', rows.length);
  console.log(`${rows[0]}, Location: ${rows[0]['Location']}, Holder: ${rows[0]['Holder']},Holder: ${rows[0]['Holder']}`);

  // [sku, cost_per_carat, location, holder]
  const records = [];
  for (const r of rows) {
    const sku = txt(r['Parcel']);
    if (!sku) continue;
    records.push([sku, safeNum(r['cost_per_carat']), txt(r['Location']), txt(r['Holder'])]);
  }
  console.log('Records with SKU:', records.length);

  let matched = 0;
  const totalChunks = Math.ceil(records.length / CHUNK);
  for (let i = 0; i < records.length; i += CHUNK) {
    const chunk = records.slice(i, i + CHUNK);
    const ph = chunk
      .map((_, ri) => {
        const b = ri * 4;
        return `($${b + 1}::text,$${b + 2}::numeric,$${b + 3}::text,$${b + 4}::text)`;
      })
      .join(',');
    const sql = `
      UPDATE soap_stones AS s SET
        cost_per_carat = COALESCE(v.cpc, s.cost_per_carat),
        location       = COALESCE(v.loc, s.location),
        holder         = v.hold
      FROM (VALUES ${ph}) AS v(sku, cpc, loc, hold)
      WHERE s.sku = v.sku`;
    const res = await pool.query(sql, chunk.flat());
    matched += res.rowCount || 0;
    console.log(`  Chunk ${Math.floor(i / CHUNK) + 1}/${totalChunks} → ${res.rowCount} rows updated`);
  }

  const stats = await pool.query(`
    SELECT
      COUNT(*) FILTER (WHERE cost_per_carat IS NOT NULL)::int AS with_cost,
      COUNT(*) FILTER (WHERE location IS NOT NULL)::int        AS with_location,
      COUNT(*) FILTER (WHERE holder IS NOT NULL)::int          AS with_holder
    FROM soap_stones`);

  console.log(`\nDONE. CSV records: ${records.length}, rows matched/updated: ${matched}`);
  console.log('soap_stones now populated:', stats.rows[0]);
  await pool.end().catch(() => {});
})();
