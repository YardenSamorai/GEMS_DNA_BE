/* =============================================================================
 * FANTASY → fantasy_lots (read-only mirror)
 * =============================================================================
 *
 * Fantasy is replacing Barak as the inventory system. This keeps a local copy
 * of every Fantasy lot (all statuses — the whole system is ~8k lots) so DNA can
 * compare it with the Barak feed without calling Fantasy on every page load.
 * Nothing here writes to Fantasy.
 * ========================================================================== */

const fantasy = require("./fantasyClient");

const FIELDS = [
  "LotID", "Stock_ID", "LotName", "LotStatusID", "LotStatusDB", "CompanyID", "ItemBaseTypeID", "ItemName",
  "ParcelOrStone", "Quantity", "Weight", "ShapeID", "ColorID", "ClarityID", "GemTypeID", "MetalID",
  "Cost", "TotalCost", "Sale", "TotalSale",
  "LocationAccountName", "DepartmentAccountName", "OwnerAccountName", "VendorAccountName", "BoxLotName",
  "LabAccountName", "CertificateNo", "Image", "LastUpdate",
];

// A Fantasy reply this small means something went wrong upstream; keep the
// previous copy instead of replacing it.
const MIN_LOTS = 1000;
const STALE_MS = 12 * 60 * 60 * 1000;
const CHUNK = 500;

const ready = new Map();
function ensureTable(pool) {
  if (!ready.has(pool)) {
    const p = (async () => {
      await pool.query(`
        CREATE TABLE IF NOT EXISTS fantasy_lots (
          lot_id         INTEGER PRIMARY KEY,
          stock_id       INTEGER,
          lot_name       TEXT,
          name_key       TEXT,
          status         TEXT,
          status_db      TEXT,
          company_id     INTEGER,
          item_base_type INTEGER,
          item_name      TEXT,
          parcel_or_stone TEXT,
          quantity       NUMERIC,
          weight         NUMERIC,
          shape          TEXT,
          color          TEXT,
          clarity        TEXT,
          gem_type       TEXT,
          metal          TEXT,
          cost           NUMERIC,
          total_cost     NUMERIC,
          sale           NUMERIC,
          total_sale     NUMERIC,
          location       TEXT,
          department     TEXT,
          owner          TEXT,
          vendor         TEXT,
          box_lot_name   TEXT,
          lab            TEXT,
          certificate_no TEXT,
          image          TEXT,
          last_update    TIMESTAMP,
          synced_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
      `);
      await pool.query(`CREATE INDEX IF NOT EXISTS fantasy_lots_name_key ON fantasy_lots (name_key)`);
      await pool.query(`
        CREATE TABLE IF NOT EXISTS fantasy_syncs (
          id          SERIAL PRIMARY KEY,
          source      TEXT,
          success     BOOLEAN NOT NULL,
          lots        INTEGER,
          message     TEXT,
          duration_ms INTEGER,
          ran_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
      `);
    })();
    p.catch(() => ready.delete(pool));
    ready.set(pool, p);
  }
  return ready.get(pool);
}

const nameKey = (s) => String(s ?? "").trim().toUpperCase();
const num = (v) => (v === null || v === undefined || v === "" || !Number.isFinite(Number(v)) ? null : Number(v));
const text = (v) => {
  const s = String(v ?? "").trim();
  return s || null;
};

const toRow = (l) => [
  l.LotID, num(l.Stock_ID), text(l.LotName), nameKey(l.LotName), text(l.LotStatusID), text(l.LotStatusDB),
  num(l.CompanyID), num(l.ItemBaseTypeID), text(l.ItemName), text(l.ParcelOrStone),
  num(l.Quantity), num(l.Weight), text(l.ShapeID), text(l.ColorID), text(l.ClarityID), text(l.GemTypeID), text(l.MetalID),
  num(l.Cost), num(l.TotalCost), num(l.Sale), num(l.TotalSale),
  text(l.LocationAccountName), text(l.DepartmentAccountName), text(l.OwnerAccountName), text(l.VendorAccountName),
  text(l.BoxLotName), text(l.LabAccountName), text(l.CertificateNo), text(l.Image), text(l.LastUpdate),
];
const COLUMNS = [
  "lot_id", "stock_id", "lot_name", "name_key", "status", "status_db", "company_id", "item_base_type", "item_name",
  "parcel_or_stone", "quantity", "weight", "shape", "color", "clarity", "gem_type", "metal",
  "cost", "total_cost", "sale", "total_sale", "location", "department", "owner", "vendor",
  "box_lot_name", "lab", "certificate_no", "image", "last_update",
];

async function logSync(pool, entry) {
  await pool.query(
    `INSERT INTO fantasy_syncs (source, success, lots, message, duration_ms) VALUES ($1, $2, $3, $4, $5)`,
    [entry.source, entry.success, entry.lots ?? null, entry.message ?? null, entry.durationMs ?? null]
  );
}

/* Replaces the mirror in one transaction, so readers never see it half full. */
async function replaceLots(pool, lots) {
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    await client.query("DELETE FROM fantasy_lots");
    for (let i = 0; i < lots.length; i += CHUNK) {
      const chunk = lots.slice(i, i + CHUNK).map(toRow);
      const values = chunk
        .map((row, r) => `(${row.map((_, c) => `$${r * COLUMNS.length + c + 1}`).join(", ")})`)
        .join(", ");
      await client.query(`INSERT INTO fantasy_lots (${COLUMNS.join(", ")}) VALUES ${values}`, chunk.flat());
    }
    await client.query("COMMIT");
  } catch (err) {
    await client.query("ROLLBACK").catch(() => {});
    throw err;
  } finally {
    client.release();
  }
}

let running = null;

function runSync(pool, { source = "manual" } = {}) {
  if (running) return running;
  running = (async () => {
    const started = Date.now();
    await ensureTable(pool);
    try {
      if (!fantasy.isConfigured()) throw new Error("Fantasy API is not configured");
      const all = await fantasy.getAllStock({ select: FIELDS.join(", ") });
      const lots = all.filter((l) => Number.isInteger(l.LotID));
      if (lots.length < MIN_LOTS) throw new Error(`Fantasy returned only ${lots.length} lots — kept the previous copy`);
      await replaceLots(pool, lots);
      const durationMs = Date.now() - started;
      await logSync(pool, { source, success: true, lots: lots.length, durationMs });
      console.log(`🧾 Fantasy sync: ${lots.length} lots in ${(durationMs / 1000).toFixed(1)}s`);
      return { success: true, lots: lots.length, durationMs };
    } catch (err) {
      console.error("❌ Fantasy sync failed:", err.message);
      await logSync(pool, { source, success: false, message: err.message, durationMs: Date.now() - started }).catch(() => {});
      return { success: false, message: err.message };
    }
  })().finally(() => {
    running = null;
  });
  return running;
}

async function lastSync(pool, { successOnly = false } = {}) {
  await ensureTable(pool);
  const { rows } = await pool.query(
    `SELECT source, success, lots, message, duration_ms, ran_at FROM fantasy_syncs
      ${successOnly ? "WHERE success" : ""} ORDER BY id DESC LIMIT 1`
  );
  const r = rows[0];
  return r
    ? { source: r.source, success: r.success, lots: r.lots, message: r.message, durationMs: r.duration_ms, ranAt: r.ran_at }
    : null;
}

async function syncIfStale(pool, options) {
  if (!fantasy.isConfigured()) return null;
  const last = await lastSync(pool, { successOnly: true });
  if (last && Date.now() - new Date(last.ranAt).getTime() < STALE_MS) return null;
  return runSync(pool, options);
}

async function loadLots(pool) {
  await ensureTable(pool);
  const { rows } = await pool.query(`SELECT ${COLUMNS.join(", ")} FROM fantasy_lots`);
  return rows;
}

module.exports = { FIELDS, nameKey, ensureTable, runSync, syncIfStale, lastSync, loadLots };
