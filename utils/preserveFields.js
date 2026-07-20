// Shared field-preservation logic used by BOTH stone importers
// (SOAP sync in importFromSoap.js and the CSV upload in /api/import-csv).
//
// Both importers TRUNCATE soap_stones and re-insert the whole table, and both
// feed the same /api/soap-stones endpoint (regular inventory AND sales
// inventory). To make them behave identically and never drop data:
//
//   • Snapshot the value-bearing fields BEFORE the truncate.
//   • After re-inserting, restore any field the new import left empty.
//
// The incoming import always WINS when it provides a non-empty value; the
// snapshot only fills the gaps. This means a CSV that carries data the live
// SOAP feed lacks (e.g. the "U-V" colour) survives the next SOAP sync, and a
// SOAP sync that carries data a partial CSV lacks survives the next CSV import.

// Text columns: a blank/empty incoming value falls back to the snapshot.
const PRESERVE_TEXT = [
  'color', 'clarity', 'lab', 'fluorescence', 'cut', 'polish', 'symmetry',
  'measurements', 'origin', 'comment', 'type', 'cert_comments',
  'certificate_number', 'certificate_image', 'certificate_image_jpg',
  'image', 'additional_pictures', 'video', 'additional_videos',
  'fancy_intensity', 'fancy_color', 'fancy_overtone', 'fancy_color_2',
  'fancy_overtone_2', 'trade_show', 'grouping_type', 'location', 'branch',
  'holder', 'jewelry_model',
];

// Numeric columns: a NULL incoming value falls back to the snapshot.
const PRESERVE_NUM = ['ratio', 'table_percent', 'depth_percent', 'cost_per_carat'];

const ALL_COLS = [...PRESERVE_TEXT, ...PRESERVE_NUM];

/* Normalise any imported text value: trim whitespace, collapse empties to NULL.
 * Used by both importers so trailing spaces (e.g. "U-V ") never reach the DB. */
const cleanText = (v) => {
  if (v == null) return null;
  // xml2js can hand back { _: 'text', $: {...} } for elements with attributes.
  const raw = typeof v === 'object' && v._ !== undefined ? v._ : v;
  const s = String(raw).trim();
  return s === '' ? null : s;
};

/* Snapshot the preserved fields for every stone that has at least one of them
 * set, keyed by SKU. Call this BEFORE the TRUNCATE. */
async function snapshotPreserved(dbPool) {
  const cols = ['sku', ...ALL_COLS].join(', ');
  const conds = ALL_COLS.map((c) => `${c} IS NOT NULL`).join(' OR ');
  const { rows } = await dbPool.query(
    `SELECT ${cols} FROM soap_stones WHERE ${conds}`
  );
  return rows;
}

/* Re-apply the snapshot AFTER the new rows are inserted. The freshly-imported
 * value wins whenever it is non-empty; otherwise the snapshot value is kept.
 * Returns the number of rows touched.
 *
 * `excludeCols` — columns the CURRENT import is authoritative for, i.e. an
 * empty incoming value MEANS empty (don't fall back to the snapshot). The CSV
 * export carries a real Holder column, so a blank there means "hold released"
 * — restoring the old name kept dead HOLD tags alive forever (T9577 bug).
 * The SOAP feed has no holder at all, so the sync still preserves it. */
async function restorePreserved(dbPool, rows, chunkSize = 300, excludeCols = []) {
  if (!rows || !rows.length) return 0;

  const setClause = [
    ...PRESERVE_TEXT.filter((c) => !excludeCols.includes(c)).map(
      (c) => `${c} = COALESCE(NULLIF(s.${c}, ''), v.${c})`
    ),
    ...PRESERVE_NUM.filter((c) => !excludeCols.includes(c)).map(
      (c) => `${c} = COALESCE(s.${c}, v.${c})`
    ),
  ].join(',\n           ');

  const valCols = ['sku', ...ALL_COLS];
  const castFor = (col, idx) => {
    if (idx === 0) return '::text'; // sku
    return PRESERVE_NUM.includes(col) ? '::numeric' : '::text';
  };

  let restored = 0;
  for (let i = 0; i < rows.length; i += chunkSize) {
    const chunk = rows.slice(i, i + chunkSize);
    const width = valCols.length;
    const placeholders = chunk
      .map((_, ri) => {
        const base = ri * width;
        return (
          '(' +
          valCols.map((c, ci) => `$${base + ci + 1}${castFor(c, ci)}`).join(',') +
          ')'
        );
      })
      .join(', ');
    const flat = chunk.flatMap((r) => valCols.map((c) => (r[c] ?? null)));
    const res = await dbPool.query(
      `UPDATE soap_stones AS s SET
           ${setClause}
         FROM (VALUES ${placeholders}) AS v(${valCols.join(', ')})
         WHERE s.sku = v.sku`,
      flat
    );
    restored += res.rowCount || 0;
  }
  return restored;
}

module.exports = {
  PRESERVE_TEXT,
  PRESERVE_NUM,
  ALL_COLS,
  cleanText,
  snapshotPreserved,
  restorePreserved,
};
