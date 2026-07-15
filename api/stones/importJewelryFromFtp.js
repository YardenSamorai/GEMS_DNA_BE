// Automated jewelry sync — pulls Jewelry_Web.csv that Barak uploads daily to
// our FTP (same server that receives the Diamonds feed) and refreshes the
// jewelry_products table. Mirrors the manual /api/jewelry/import-csv flow:
// upsert by model_number (preserving first_seen_at) and prune rows that
// disappeared from the feed. Every run is recorded in sync_log with
// sync_type='jewelry' so the Dashboard's "Inventory sync" window shows it.
//
// Run directly (Render Cron Job): node api/stones/importJewelryFromFtp.js

const ftp = require("basic-ftp");
const { Writable } = require("stream");
const { parse: parseCsv } = require("csv-parse/sync");
const { pool } = require("../../db/client");

const FTP_HOST = process.env.BARAK_FTP_HOST || "ftp.eshed.art";
const FTP_USER = process.env.BARAK_FTP_USER || "eshedftp@eshed.art";
const FTP_PASSWORD = process.env.BARAK_FTP_PASSWORD || "Eshed131223!@#";
const JEWELRY_FILE = process.env.BARAK_JEWELRY_FILE || "Jewelry_Web.csv";

const safeNum = (v) => {
  if (v === undefined || v === null) return null;
  const n = parseFloat(String(v).replace(/,/g, ""));
  return Number.isFinite(n) ? n : null;
};

const txt = (v) => {
  const s = (v === undefined || v === null) ? "" : String(v).trim();
  return s === "" ? null : s;
};

const downloadCsv = async () => {
  const client = new ftp.Client(30000);
  try {
    await client.access({ host: FTP_HOST, user: FTP_USER, password: FTP_PASSWORD });
    const chunks = [];
    const sink = new Writable({
      write(chunk, _enc, cb) { chunks.push(chunk); cb(); },
    });
    await client.downloadTo(sink, JEWELRY_FILE);
    return Buffer.concat(chunks).toString("utf8");
  } finally {
    client.close();
  }
};

const COLUMNS = [
  "model_number", "stock_number", "jewelry_type", "style", "collection",
  "price", "video_link", "all_pictures_link", "certificate_link", "certificate_number",
  "title", "description", "jewelry_weight", "total_carat", "stone_type",
  "center_stone_carat", "center_stone_shape", "center_stone_color", "center_stone_clarity",
  "metal_type", "currency", "availability", "shipping_from", "category",
  "full_description", "jewelry_size", "instructions_main", "location",
];

const rowToValues = (r) => [
  txt(r["Model Number"]),
  txt(r["Stock Number"]),
  txt(r["Jewelry Type"]),
  txt(r["Style"]),
  txt(r["Collection"]),
  safeNum(r["Price"]),
  txt(r["Video_Link"]),
  txt(r["All_Pictures_Link"]),
  txt(r["Certificate_Link"]),
  txt(r["Certificate Number"]),
  txt(r["Title"]),
  txt(r["Description"]),
  safeNum(r["Jewelry_Weight"]),
  safeNum(r["Total_Carat"]),
  txt(r["Stone_Type"]),
  safeNum(r["Center_Stone_Carat"]),
  txt(r["Center_Stone_Shape"]),
  txt(r["Center_Stone_Color"]),
  txt(r["Center_Stone_Clarity"]),
  txt(r["Metal_Type"]),
  txt(r["Currency"]),
  txt(r["Availability"]),
  txt(r["Shipping_From"]),
  txt(r["Category"]),
  txt(r["full_description"]),
  txt(r["jewelry_size"]),
  txt(r["Instructions_main"]),
  txt(r["Location"]),
];

/**
 * Fetch the jewelry CSV from FTP and refresh jewelry_products.
 * @param {object} options
 * @param {object} options.dbPool - Optional pool (when called from server.js)
 * @returns {Promise<{success: boolean, count: number, message: string}>}
 */
const runImport = async (options = {}) => {
  const dbPool = options.dbPool || pool;

  try {
    console.log(`🚀 [1/4] Downloading ${JEWELRY_FILE} from ${FTP_HOST}...`);
    const csvContent = await downloadCsv();
    if (!csvContent || !csvContent.trim()) {
      return { success: false, count: 0, message: "Jewelry CSV is empty or missing on FTP" };
    }

    console.log("📦 [2/4] Parsing CSV...");
    const rows = parseCsv(csvContent, {
      columns: true,
      skip_empty_lines: true,
      relax_quotes: true,
      relax_column_count: true,
    });
    console.log(`📊 Parsed ${rows.length} jewelry rows`);

    const rawValues = rows.map(rowToValues).filter((v) => v[0] !== null);

    // Safety valve: a truncated/broken feed must never wipe the catalog.
    if (rawValues.length < 50) {
      return {
        success: false,
        count: rawValues.length,
        message: `Feed too small (${rawValues.length} items) — skipped to protect existing data`,
      };
    }

    // De-duplicate by model_number (last occurrence wins) — the upsert can't
    // touch the same target row twice in one statement.
    const dedupMap = new Map();
    for (const row of rawValues) dedupMap.set(row[0], row);
    const values = Array.from(dedupMap.values());
    const dupCount = rawValues.length - values.length;
    if (dupCount > 0) console.warn(`⚠️  Collapsed ${dupCount} duplicate Model Number row(s)`);

    console.log("🧱 [3/4] Upserting into jewelry_products...");
    // Keep parity with the manual import: make sure trailing columns exist.
    await dbPool.query(`ALTER TABLE jewelry_products ADD COLUMN IF NOT EXISTS location VARCHAR(150)`);
    await dbPool.query(`ALTER TABLE jewelry_products ADD COLUMN IF NOT EXISTS first_seen_at TIMESTAMP DEFAULT NOW()`);

    const CHUNK = 100;
    for (let i = 0; i < values.length; i += CHUNK) {
      const chunk = values.slice(i, i + CHUNK);
      const ph = chunk
        .map((row, ri) => "(" + COLUMNS.map((_, ci) => "$" + (ri * COLUMNS.length + ci + 1)).join(",") + ")")
        .join(",");
      await dbPool.query(
        "INSERT INTO jewelry_products (" + COLUMNS.join(",") + ") VALUES " + ph +
          " ON CONFLICT (model_number) DO UPDATE SET " +
          COLUMNS.slice(1).map((c) => `${c} = EXCLUDED.${c}`).join(", "),
        chunk.flat()
      );
    }

    console.log("🧹 [4/4] Pruning items no longer in the feed...");
    const importedModels = values.map((v) => v[0]);
    const pruneRes = await dbPool.query(
      "DELETE FROM jewelry_products WHERE NOT (model_number = ANY($1::text[]))",
      [importedModels]
    );
    const pruned = pruneRes.rowCount || 0;

    const message = `Successfully synced ${values.length} jewelry items` +
      (pruned > 0 ? ` (removed ${pruned} sold/discontinued)` : "");
    console.log(`🎉 DONE! ${message}`);
    return { success: true, count: values.length, message };
  } catch (err) {
    console.error("❌ Jewelry sync error:", err);
    return { success: false, count: 0, message: err.message || "Unknown error during jewelry sync" };
  }
};

// Same sync_log table the stones sync writes to; sync_type distinguishes the
// two feeds in the Dashboard window. Logging must never fail the sync itself.
const logSyncRun = async (dbPool, entry) => {
  try {
    await dbPool.query(`
      CREATE TABLE IF NOT EXISTS sync_log (
        id           SERIAL PRIMARY KEY,
        source       TEXT NOT NULL DEFAULT 'manual',
        success      BOOLEAN NOT NULL,
        stones_count INTEGER NOT NULL DEFAULT 0,
        message      TEXT,
        duration_ms  INTEGER,
        started_at   TIMESTAMPTZ NOT NULL,
        finished_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
      )
    `);
    await dbPool.query(`ALTER TABLE sync_log ADD COLUMN IF NOT EXISTS sync_type TEXT NOT NULL DEFAULT 'stones'`);
    await dbPool.query(
      `INSERT INTO sync_log (source, success, stones_count, message, duration_ms, started_at, sync_type)
       VALUES ($1, $2, $3, $4, $5, $6, 'jewelry')`,
      [entry.source, entry.success, entry.count, entry.message, entry.durationMs, entry.startedAt]
    );
    console.log(`📝 sync_log recorded (jewelry, ${entry.source}, success=${entry.success}, count=${entry.count})`);
  } catch (e) {
    console.warn("⚠️  Could not write sync_log entry:", e.message);
  }
};

/**
 * Run the jewelry FTP import and record the outcome in sync_log.
 * @param {object} options
 * @param {object} options.dbPool - Optional pool (when called from server.js)
 * @param {boolean} options.closePool - Close the pool when done (default: true)
 * @param {string} options.source - 'manual' or 'cron'
 */
const run = async (options = {}) => {
  const dbPool = options.dbPool || pool;
  const closePool = options.closePool !== undefined ? options.closePool : true;
  const source = options.source || "manual";
  const startedAt = new Date();

  let result;
  try {
    result = await runImport(options);
  } catch (err) {
    result = { success: false, count: 0, message: err.message || "Unknown error during jewelry sync" };
  }

  await logSyncRun(dbPool, {
    source,
    success: result.success,
    count: result.count,
    message: result.message,
    durationMs: Date.now() - startedAt.getTime(),
    startedAt,
  });

  if (closePool) {
    await pool.end().catch(() => {});
  }
  return result;
};

module.exports = { run };

// Only auto-run when executed directly (the Render Cron Job runs
// `node api/stones/importJewelryFromFtp.js`)
if (require.main === module) {
  run({ source: "cron" }).then((result) => {
    console.log("📋 Result:", result);
    process.exit(result.success ? 0 : 1);
  });
}
