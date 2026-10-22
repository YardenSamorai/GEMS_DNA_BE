/* =============================================================================
 * BARAK eTRADE → WEBSITE LIST
 * =============================================================================
 *
 * Barak writes the stones marked for the web to our FTP every three hours as
 * Diamonds<YYYYMMDD_HHMMSS>.csv. That file decides what is on Eshed.com:
 * each run makes the Barak-managed part of web_listings match the newest
 * file. Stones an admin added by hand in DNA are left alone.
 * ========================================================================== */

const ftp = require("basic-ftp");
const { Writable } = require("stream");
const { parse: parseCsv } = require("csv-parse/sync");
const webListings = require("./webListings");

const WEB_FILE_RE = /^Diamonds\d{8}_\d{6}\.csv$/i;
const MIN_STONES = 100;
const MAX_DROP = 0.3;
const STALE_MS = 3 * 60 * 60 * 1000 + 15 * 60 * 1000;

const latestWebFile = (names) => names.filter((n) => WEB_FILE_RE.test(n)).sort().pop() || null;

const skusFromCsv = (text) => {
  const rows = parseCsv(String(text || "").replace(/^\uFEFF/, ""), {
    columns: (header) => header.map((h) => String(h).trim()),
    skip_empty_lines: true,
    relax_quotes: true,
    relax_column_count: true,
  });
  return [...new Set(rows.map((r) => String(r.SKU ?? "").trim()).filter(Boolean))];
};

/* A broken or half-written file must never empty the website. A large but
 * real clear-out is confirmed by an admin pressing Sync now (allowDrop). */
const guardReason = (fileCount, managedCount, removing, { allowDrop = false } = {}) => {
  if (fileCount < MIN_STONES) return `Only ${fileCount} stones in the file`;
  if (!allowDrop && managedCount >= 50 && removing / managedCount > MAX_DROP) {
    return `Would remove ${removing} of ${managedCount} stones — press Sync now to confirm`;
  }
  return null;
};

async function downloadLatest(config) {
  const client = new ftp.Client(60000);
  try {
    await client.access(config);
    const name = latestWebFile((await client.list()).map((f) => f.name));
    if (!name) throw new Error("No Diamonds file on the FTP");
    const chunks = [];
    await client.downloadTo(
      new Writable({ write(c, _e, cb) { chunks.push(c); cb(); } }),
      name
    );
    return { name, text: Buffer.concat(chunks).toString("utf8") };
  } finally {
    client.close();
  }
}

let running = null;

/* force: re-apply even when this file was already synced. */
function runSync(pool, { ftpConfig, source = "manual", force = false } = {}) {
  if (running) return running;
  running = (async () => {
    let fileName = null;
    try {
      const file = await downloadLatest(ftpConfig);
      fileName = file.name;
      const last = await webListings.lastSync(pool, { successOnly: true });
      if (!force && last && last.fileName === fileName) {
        await webListings.logSync(pool, { fileName, source, success: true, stones: last.stones, added: 0, removed: 0, message: "No new file" });
        return { success: true, skipped: true, fileName };
      }
      const skus = skusFromCsv(file.text);
      const result = await webListings.syncManaged(pool, skus, (removing, managed) =>
        guardReason(skus.length, managed, removing, { allowDrop: force })
      );
      await webListings.logSync(pool, { fileName, source, success: !result.blocked, stones: skus.length, added: result.added.length, removed: result.removed.length, message: result.blocked || null });
      if (result.blocked) {
        console.warn(`⚠️  Website sync skipped (${fileName}): ${result.blocked}`);
        return { success: false, fileName, message: result.blocked };
      }
      console.log(`🌐 Website sync ${fileName}: ${skus.length} stones, +${result.added.length} -${result.removed.length}`);
      return { success: true, fileName, stones: skus.length, added: result.added, removed: result.removed };
    } catch (err) {
      console.error("❌ Website sync failed:", err.message);
      await webListings
        .logSync(pool, { fileName, source, success: false, message: err.message })
        .catch(() => {});
      return { success: false, fileName, message: err.message };
    }
  })().finally(() => {
    running = null;
  });
  return running;
}

async function syncIfStale(pool, options) {
  const last = await webListings.lastSync(pool);
  if (last && Date.now() - new Date(last.ranAt).getTime() < STALE_MS) return null;
  return runSync(pool, options);
}

module.exports = { latestWebFile, skusFromCsv, guardReason, runSync, syncIfStale };
