/* =============================================================================
 * WEBSITE LIST (eTrade replacement)
 * =============================================================================
 *
 * The stones an admin chose to publish on Eshed.com. Kept in its own table
 * so the SOAP/CSV truncate of soap_stones cannot touch it; a stone that
 * drops out of the Barak feed (sold) stays listed but is left out of the
 * export and flagged on the review screen.
 *
 * Pairs travel together. Adding or removing one half always takes its
 * partner with it — a half pair on the site is what this list replaces.
 * ========================================================================== */

const { toCsv } = require("./etradeExport");

const MAX_SKUS = 5000;

async function ensureTable(pool) {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS web_listings (
      sku       TEXT PRIMARY KEY,
      added_by  TEXT,
      added_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);
}

const cleanSkus = (input) => {
  const list = Array.isArray(input) ? input : [];
  const out = new Set();
  for (const raw of list) {
    const s = String(raw ?? "").trim();
    if (s && s.length <= 80) out.add(s);
    if (out.size >= MAX_SKUS) break;
  }
  return [...out];
};

/* The SKUs plus every pair partner either side points at. With
 * `catalogOnly`, anything not in the current Barak feed is dropped (adding);
 * without it the requested SKUs are kept so a sold stone can be removed. */
async function withPartners(pool, skus, { catalogOnly = false } = {}) {
  if (!skus.length) return [];
  const { rows } = await pool.query(
    `SELECT sku, pair_stone FROM soap_stones
      WHERE sku = ANY($1::text[]) OR pair_stone = ANY($1::text[])`,
    [skus]
  );
  const all = new Set(skus);
  for (const r of rows) {
    all.add(r.sku);
    const partner = String(r.pair_stone || "").trim();
    if (partner && skus.includes(r.sku)) all.add(partner);
  }
  const { rows: known } = await pool.query(
    `SELECT sku FROM soap_stones WHERE sku = ANY($1::text[])`,
    [[...all]]
  );
  const inCatalog = new Set(known.map((r) => r.sku));
  return [...all].filter((s) => inCatalog.has(s) || (!catalogOnly && skus.includes(s)));
}

async function listSkus(pool) {
  await ensureTable(pool);
  const { rows } = await pool.query(`SELECT sku FROM web_listings ORDER BY sku`);
  return rows.map((r) => r.sku);
}

async function addSkus(pool, input, actorId) {
  await ensureTable(pool);
  const skus = await withPartners(pool, cleanSkus(input), { catalogOnly: true });
  if (!skus.length) return { added: [], skus: [] };
  const { rows } = await pool.query(
    `INSERT INTO web_listings (sku, added_by)
     SELECT s, $2 FROM unnest($1::text[]) AS s
     ON CONFLICT (sku) DO NOTHING
     RETURNING sku`,
    [skus, actorId || null]
  );
  return { added: rows.map((r) => r.sku), skus };
}

async function removeSkus(pool, input) {
  await ensureTable(pool);
  const requested = cleanSkus(input);
  const skus = await withPartners(pool, requested);
  if (!skus.length) return { removed: [] };
  const { rows } = await pool.query(
    `DELETE FROM web_listings WHERE sku = ANY($1::text[]) RETURNING sku`,
    [skus]
  );
  return { removed: rows.map((r) => r.sku) };
}

const hasFile = (v) => {
  const s = String(v || "").trim();
  return Boolean(s) && !/\/$/.test(s);
};

/* Everything the review screen needs, warnings included. */
async function listDetailed(pool) {
  await ensureTable(pool);
  const { rows } = await pool.query(
    `SELECT w.sku, w.added_at, w.added_by,
            s.sku IS NOT NULL AS in_catalog,
            s.category, s.shape, s.weight, s.color, s.clarity, s.lab,
            s.price_per_carat, s.total_price, s.branch, s.image, s.video,
            s.certificate_image, s.pair_stone, s.home_page,
            p.sku AS partner_listed
       FROM web_listings w
       LEFT JOIN soap_stones s ON s.sku = w.sku
       LEFT JOIN web_listings p ON p.sku = NULLIF(TRIM(s.pair_stone), '')
      ORDER BY w.added_at DESC, w.sku`
  );
  return rows.map((r) => {
    const warnings = [];
    if (!r.in_catalog) warnings.push("not_in_inventory");
    else {
      if (!hasFile(r.image)) warnings.push("no_image");
      if (!hasFile(r.video)) warnings.push("no_video");
      if (!hasFile(r.certificate_image)) warnings.push("no_certificate");
      if (/memo/i.test(r.category || "")) warnings.push("memo_category");
      if (String(r.pair_stone || "").trim() && !r.partner_listed) warnings.push("half_pair");
    }
    return {
      sku: r.sku,
      inCatalog: Boolean(r.in_catalog),
      category: r.category,
      shape: r.shape,
      weightCt: r.weight != null ? Number(r.weight) : null,
      color: r.color,
      clarity: r.clarity,
      lab: r.lab,
      pricePerCt: r.price_per_carat != null ? Number(r.price_per_carat) : null,
      priceTotal: r.total_price != null ? Number(r.total_price) : null,
      branch: r.branch,
      image: r.image,
      pairSku: r.pair_stone || null,
      homePage: r.home_page || null,
      addedAt: r.added_at,
      addedBy: r.added_by,
      warnings,
    };
  });
}

/* The eTrade CSV: every listed stone still in the Barak feed. */
async function exportCsv(pool) {
  await ensureTable(pool);
  const { rows } = await pool.query(
    `SELECT s.* FROM web_listings w
       JOIN soap_stones s ON s.sku = w.sku
      ORDER BY s.category, s.sku`
  );
  return { csv: toCsv(rows), count: rows.length };
}

module.exports = {
  ensureTable,
  cleanSkus,
  listSkus,
  addSkus,
  removeSkus,
  listDetailed,
  exportCsv,
};
