/* =============================================================================
 * ADMIN STONE OVERRIDES
 * =============================================================================
 *
 * Barak is the catalog source, but some lots arrive with a wrong Weight (or
 * another spec). An admin can correct one stone; the correction lives in
 * `stone_overrides` so the next SOAP/CSV truncate cannot wipe it.
 *
 * `fields` is JSON keyed by the frontend names (weightCt, groupingType, …).
 * Only those keys are written onto soap_stones after every import.
 * ========================================================================== */

const FIELDS = {
  weightCt: { col: "weight", kind: "num", min: 0.001 },
  stones: { col: "stones", kind: "int", min: 0 },
  ratio: { col: "ratio", kind: "num", min: 0 },
  shape: { col: "shape", kind: "text" },
  color: { col: "color", kind: "text" },
  clarity: { col: "clarity", kind: "text" },
  measurements: { col: "measurements", kind: "text" },
  groupingType: { col: "grouping_type", kind: "text" },
  pairSku: { col: "pair_stone", kind: "text" },
  origin: { col: "origin", kind: "text" },
  treatment: { col: "comment", kind: "text" },
  lab: { col: "lab", kind: "text" },
  fluorescence: { col: "fluorescence", kind: "text" },
};

const API_KEYS = Object.keys(FIELDS);
const MAX_TEXT = 200;

const isPlainObject = (v) => v != null && typeof v === "object" && !Array.isArray(v);

const sanitizeText = (v) => {
  const s = String(v).trim();
  if (!s) return null;
  return s.length > MAX_TEXT ? s.slice(0, MAX_TEXT) : s;
};

/* Turn a PATCH body into { set, unset }. Unknown keys are ignored. A null or
 * blank value on a known key means "stop overriding — go back to Barak". */
const sanitizePatch = (body) => {
  const src = isPlainObject(body?.fields) ? body.fields : isPlainObject(body) ? body : {};
  const set = {};
  const unset = [];

  for (const key of API_KEYS) {
    if (!Object.prototype.hasOwnProperty.call(src, key)) continue;
    const raw = src[key];
    if (raw === null || raw === undefined || raw === "") {
      unset.push(key);
      continue;
    }
    const spec = FIELDS[key];
    if (spec.kind === "text") {
      const t = sanitizeText(raw);
      if (t == null) unset.push(key);
      else set[key] = t;
      continue;
    }
    const n = Number(String(raw).replace(/,/g, ""));
    if (!Number.isFinite(n) || n < spec.min) {
      const err = new Error(`Invalid ${key}`);
      err.status = 400;
      throw err;
    }
    set[key] = spec.kind === "int" ? Math.round(n) : n;
  }

  return { set, unset };
};

const toApiFields = (fields) => {
  const src = isPlainObject(fields) ? fields : {};
  const out = {};
  for (const key of API_KEYS) {
    if (!Object.prototype.hasOwnProperty.call(src, key)) continue;
    out[key] = src[key];
  }
  return out;
};

const appliedFromRow = (row) => {
  if (!row) return null;
  return {
    weightCt: row.weight != null ? Number(row.weight) : null,
    stones: row.stones != null ? Number(row.stones) : null,
    ratio: row.ratio != null && row.ratio !== "" ? Number(row.ratio) : null,
    shape: row.shape || "",
    color: row.color || "",
    clarity: row.clarity || "",
    measurements: row.measurements || "",
    groupingType: row.grouping_type || "",
    pairSku: row.pair_stone || null,
    origin: row.origin && String(row.origin).toUpperCase() !== "N/A" ? row.origin : "",
    treatment: row.comment || "",
    lab: row.lab && String(row.lab).toUpperCase() !== "N/A" ? row.lab : "",
    fluorescence: row.fluorescence || "",
  };
};

const SET_CLAUSE = API_KEYS.map((key) => {
  const { col, kind } = FIELDS[key];
  const cast = kind === "text" ? "text" : "numeric";
  return `${col} = CASE WHEN o.fields ? '${key}' THEN NULLIF(o.fields->>'${key}', '')::${cast} ELSE s.${col} END`;
}).join(",\n           ");

async function ensureTable(pool) {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS stone_overrides (
      sku        TEXT PRIMARY KEY,
      fields     JSONB NOT NULL DEFAULT '{}'::jsonb,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_by TEXT
    )
  `);
}

async function getOverrides(pool, sku) {
  await ensureTable(pool);
  const { rows } = await pool.query(
    `SELECT sku, fields, updated_at, updated_by FROM stone_overrides WHERE sku = $1`,
    [sku]
  );
  if (!rows[0]) return { sku, fields: {}, updatedAt: null, updatedBy: null };
  return {
    sku: rows[0].sku,
    fields: toApiFields(rows[0].fields),
    updatedAt: rows[0].updated_at,
    updatedBy: rows[0].updated_by,
  };
}

async function applyOverrides(pool, sku = null) {
  await ensureTable(pool);
  const res = await pool.query(
    `UPDATE soap_stones AS s SET
           ${SET_CLAUSE}
       FROM stone_overrides o
      WHERE s.sku = o.sku
        AND ($1::text IS NULL OR s.sku = $1)
        AND o.fields <> '{}'::jsonb`,
    [sku]
  );
  return res.rowCount || 0;
}

async function upsertOverrides(pool, sku, body, actorId) {
  const { set, unset } = sanitizePatch(body);
  if (!set && !unset.length) {
    /* fall through — still return current row */
  }
  await ensureTable(pool);

  const current = await getOverrides(pool, sku);
  const next = { ...current.fields };
  for (const key of unset) delete next[key];
  Object.assign(next, set);

  if (!Object.keys(next).length) {
    await pool.query(`DELETE FROM stone_overrides WHERE sku = $1`, [sku]);
  } else {
    await pool.query(
      `INSERT INTO stone_overrides (sku, fields, updated_by)
       VALUES ($1, $2::jsonb, $3)
       ON CONFLICT (sku) DO UPDATE SET
         fields = EXCLUDED.fields,
         updated_by = EXCLUDED.updated_by,
         updated_at = NOW()`,
      [sku, JSON.stringify(next), actorId || null]
    );
  }

  const appliedCount = await applyOverrides(pool, sku);
  const { rows } = await pool.query(
    `SELECT weight, stones, ratio, shape, color, clarity, measurements,
            grouping_type, pair_stone, origin, comment, lab, fluorescence
       FROM soap_stones WHERE sku = $1`,
    [sku]
  );
  const saved = await getOverrides(pool, sku);
  return {
    ...saved,
    applied: appliedFromRow(rows[0]),
    appliedCount,
  };
}

module.exports = {
  FIELDS,
  API_KEYS,
  sanitizePatch,
  toApiFields,
  appliedFromRow,
  ensureTable,
  getOverrides,
  applyOverrides,
  upsertOverrides,
};
