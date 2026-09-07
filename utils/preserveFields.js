// Import-time value handling shared by BOTH stone importers (the SOAP sync in
// importFromSoap.js and the CSV upload in /api/import-csv). Both TRUNCATE
// soap_stones and re-insert the whole table.
//
// This module used to snapshot every value-bearing column and restore any one
// the incoming import left empty, so that no data was ever dropped. The cost
// was that deletion became impossible to express: Barak clearing a stone's
// video, or a rep releasing a hold, was undone on the very next import — the
// old value came straight back out of the snapshot. The hold case was patched
// by hand (the T9577 bug) and the video case surfaced the same way months
// later, which is the shape of a rule that is wrong rather than incomplete.
//
// Both feeds carry every one of these columns today, so an empty value is a
// statement and not a gap: the inventory system is saying there is nothing
// there. Each importer is therefore authoritative for every column it carries.
//
// One case survives, and it is the case where empty genuinely means "no
// opinion": a CSV export that does not contain the column at all. There is
// nothing to be authoritative with, so the stored value is kept. That keeps a
// truncated or older export from silently emptying half the table.

// Text columns: the ones an import can carry a value for.
const PRESERVE_TEXT = [
  'color', 'clarity', 'lab', 'fluorescence', 'cut', 'polish', 'symmetry',
  'measurements', 'origin', 'comment', 'type', 'cert_comments',
  'certificate_number', 'certificate_image', 'certificate_image_jpg',
  'image', 'additional_pictures', 'video', 'additional_videos',
  'fancy_intensity', 'fancy_color', 'fancy_overtone', 'fancy_color_2',
  'fancy_overtone_2', 'trade_show', 'grouping_type', 'location', 'branch',
  'holder', 'jewelry_model',
];

// Numeric columns. Note that these never round-tripped through the old
// restore anyway: the feed writes "0.00" for an absent measurement and
// parseFloat turns that into 0, which is a value, not a gap.
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

/* Snapshot the given columns for every stone that has at least one of them
 * set, keyed by SKU. Call this BEFORE the TRUNCATE. */
async function snapshotColumns(dbPool, cols) {
  if (!cols || !cols.length) return [];
  const conds = cols.map((c) => `${c} IS NOT NULL`).join(' OR ');
  const { rows } = await dbPool.query(
    `SELECT sku, ${cols.join(', ')} FROM soap_stones WHERE ${conds}`
  );
  return rows;
}

/* Re-apply the snapshot AFTER the new rows are inserted, for the columns the
 * import could say nothing about. Returns the number of rows touched. */
async function restoreColumns(dbPool, rows, cols, chunkSize = 300) {
  if (!rows || !rows.length || !cols || !cols.length) return 0;

  const setClause = cols
    .map((c) =>
      PRESERVE_NUM.includes(c)
        ? `${c} = COALESCE(s.${c}, v.${c})`
        : `${c} = COALESCE(NULLIF(s.${c}, ''), v.${c})`
    )
    .join(',\n           ');

  const valCols = ['sku', ...cols];
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
  snapshotColumns,
  restoreColumns,
};
