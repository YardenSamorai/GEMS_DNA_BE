/* =============================================================================
 * PRICE INTEGRITY CHECKS
 * =============================================================================
 *
 * Runs after every stone import (SOAP sync and CSV/Excel upload) and can be
 * triggered on demand via GET /api/price-audit.
 *
 * WHY THIS EXISTS
 * ---------------
 * The importers store the supplier's price verbatim. That convention is not
 * written down anywhere in the data itself — there is no "neto"/"bruto" flag in
 * the feed — so when it silently changed once before, the display code kept
 * dividing by two and the sales floor showed coloured stones at a quarter of
 * their real price for weeks before anyone noticed.
 *
 * These checks turn that invisible assumption into something the system
 * verifies on its own after every single import.
 *
 * WHAT IS CHECKED
 * ---------------
 *  1. Rap identity      — the feed sends the price and the Rap discount in two
 *                         independent fields, so they must agree:
 *                             pricePerCt = rapListPrice × (1 + rapPrice / 100)
 *                         This is a mathematical proof that prices were not
 *                         scaled on the way in. It covers diamonds only, but
 *                         the importers apply no per-category branching, so a
 *                         global scaling change shows up here first.
 *  2. Total consistency — total_price must equal price_per_carat × weight.
 *                         Catches shifted columns and mis-parsed rows.
 *  3. Category drift    — median price per carat per category, compared with
 *                         the previous import. Protects coloured stones, which
 *                         have no Rap to validate against. A median that moves
 *                         by ~2× or ~0.5× is a convention change, not a market
 *                         move, and is reported as a failure.
 *  4. Outliers          — negative prices, and rows priced per carat but with
 *                         no total (or the reverse).
 *
 * Nothing here ever aborts an import. A bad report is recorded and surfaced;
 * the data still lands, so a false positive can never take the catalog down.
 * ========================================================================== */

/** Rap implied vs stated percentage may differ by this many points. */
const RAP_TOLERANCE_POINTS = 1;
/** total_price may differ from price_per_carat × weight by this fraction. */
const TOTAL_TOLERANCE = 0.01;
/** Below this share of matching Rap rows the convention is considered broken. */
const MIN_RAP_MATCH_RATE = 0.9;
/** Above this share of inconsistent totals the feed is considered corrupt. */
const MAX_TOTAL_MISMATCH_RATE = 0.02;
/** A category median moving more than this fraction is worth a warning. */
const DRIFT_WARN_RATIO = 0.35;
/** Categories smaller than this are too noisy for a median comparison. */
const MIN_CATEGORY_SIZE = 5;

const num = (v) => {
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
};

const median = (values) => {
  if (!values.length) return null;
  const sorted = [...values].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  return sorted.length % 2
    ? sorted[mid]
    : (sorted[mid - 1] + sorted[mid]) / 2;
};

/* Read one stone row in either the DB shape (snake_case) or the API shape
 * (camelCase) so the checks can run against a query result or a payload. */
const readRow = (r) => ({
  sku: r.sku ?? null,
  category: r.category ?? null,
  weight: num(r.weight ?? r.weightCt),
  ppc: num(r.price_per_carat ?? r.pricePerCt),
  total: num(r.total_price ?? r.priceTotal),
  rapPct: num(r.rap_price ?? r.rapPrice),
  rapList: num(r.rap_list_price ?? r.rapListPrice),
});

/* ---------------------------------------------------------------------------
 * 1. Rap identity — pricePerCt = rapListPrice × (1 + rapPrice / 100)
 * ------------------------------------------------------------------------ */
function checkRapIdentity(rows) {
  const samples = [];
  let matched = 0;
  const worst = [];

  for (const raw of rows) {
    const r = readRow(raw);
    if (!r.ppc || !r.rapList || r.rapList <= 0 || !r.rapPct) continue;

    const impliedPct = (r.ppc / r.rapList - 1) * 100;
    const deltaPoints = Math.abs(impliedPct - r.rapPct);
    samples.push(r.sku);
    if (deltaPoints <= RAP_TOLERANCE_POINTS) matched++;
    else worst.push({ sku: r.sku, statedPct: r.rapPct, impliedPct: Number(impliedPct.toFixed(1)) });
  }

  const checked = samples.length;
  const rate = checked ? matched / checked : 1;
  const status = !checked ? "skipped" : rate >= MIN_RAP_MATCH_RATE ? "ok" : "fail";

  return {
    name: "rap_identity",
    status,
    checked,
    matched,
    matchRate: Number(rate.toFixed(4)),
    detail:
      status === "fail"
        ? `Only ${matched}/${checked} diamonds satisfy pricePerCt = rapList x (1 + rap%). ` +
          "Prices were most likely scaled on import — the stored value is no longer the supplier price."
        : `${matched}/${checked} diamonds satisfy the Rap identity.`,
    worstOffenders: worst
      .sort((a, b) => Math.abs(b.impliedPct - b.statedPct) - Math.abs(a.impliedPct - a.statedPct))
      .slice(0, 10),
  };
}

/* ---------------------------------------------------------------------------
 * 2. total_price must equal price_per_carat × weight
 *
 * Only single stones drive the verdict. On parcels and sets the feed's weight
 * describes the whole lot while the price columns describe one representative
 * unit, so ~15% of them never satisfy the identity and always have not — those
 * are counted and reported, but they must not be allowed to hold the check in
 * a permanent failure, or the alert becomes noise everyone learns to ignore.
 * A scaling regression breaks every single stone at once, which is exactly
 * what this still catches.
 * ------------------------------------------------------------------------ */
const isSingleStone = (raw) => {
  const grouping = String(raw.grouping_type ?? raw.groupingType ?? "").trim();
  return grouping === "" || grouping.toLowerCase() === "single";
};

function checkTotalConsistency(rows) {
  let checked = 0;
  let mismatched = 0;
  let multiChecked = 0;
  let multiMismatched = 0;
  const offenders = [];

  for (const raw of rows) {
    const r = readRow(raw);
    if (!r.ppc || !r.total || !r.weight || r.total <= 0) continue;

    const expected = r.ppc * r.weight;
    const drift = Math.abs(expected - r.total) / r.total;
    const bad = drift > TOTAL_TOLERANCE;

    if (!isSingleStone(raw)) {
      multiChecked++;
      if (bad) multiMismatched++;
      continue;
    }

    checked++;
    if (bad) {
      mismatched++;
      offenders.push({
        sku: r.sku,
        expected: Math.round(expected),
        actual: Math.round(r.total),
        driftPct: Number((drift * 100).toFixed(1)),
      });
    }
  }

  const rate = checked ? mismatched / checked : 0;
  const status = !checked ? "skipped" : rate <= MAX_TOTAL_MISMATCH_RATE ? "ok" : "fail";

  return {
    name: "total_consistency",
    status,
    checked,
    mismatched,
    mismatchRate: Number(rate.toFixed(4)),
    multiStoneChecked: multiChecked,
    multiStoneMismatched: multiMismatched,
    detail:
      `${mismatched}/${checked} single stones where total_price != price_per_carat x weight` +
      ` (${multiMismatched}/${multiChecked} parcels/sets, not counted towards the verdict).`,
    worstOffenders: offenders.sort((a, b) => b.driftPct - a.driftPct).slice(0, 10),
  };
}

/* ---------------------------------------------------------------------------
 * 3. Per-category median price per carat, compared with the previous import
 * ------------------------------------------------------------------------ */
function categoryMedians(rows) {
  const buckets = new Map();
  for (const raw of rows) {
    const r = readRow(raw);
    if (!r.ppc || r.ppc <= 0) continue;
    const key = r.category || "(none)";
    if (!buckets.has(key)) buckets.set(key, []);
    buckets.get(key).push(r.ppc);
  }

  const out = {};
  for (const [category, values] of buckets) {
    out[category] = { count: values.length, medianPpc: median(values) };
  }
  return out;
}

function checkCategoryDrift(current, previous) {
  if (!previous || !Object.keys(previous).length) {
    return {
      name: "category_drift",
      status: "skipped",
      detail: "No previous import on record yet — this run becomes the baseline.",
      movements: [],
    };
  }

  const movements = [];
  for (const [category, now] of Object.entries(current)) {
    const before = previous[category];
    if (!before || !before.medianPpc || !now.medianPpc) continue;
    if (now.count < MIN_CATEGORY_SIZE || before.count < MIN_CATEGORY_SIZE) continue;

    const ratio = now.medianPpc / before.medianPpc;
    const moved = Math.abs(ratio - 1);
    if (moved < DRIFT_WARN_RATIO) continue;

    // A ratio sitting on 2 or 0.5 is the signature of a scaling change rather
    // than a price update, so it is escalated from a warning to a failure.
    const looksLikeScaling =
      Math.abs(ratio - 2) < 0.15 || Math.abs(ratio - 0.5) < 0.0375;

    movements.push({
      category,
      previousMedian: before.medianPpc,
      currentMedian: now.medianPpc,
      ratio: Number(ratio.toFixed(3)),
      severity: looksLikeScaling ? "fail" : "warn",
    });
  }

  const status = movements.some((m) => m.severity === "fail")
    ? "fail"
    : movements.length
      ? "warn"
      : "ok";

  return {
    name: "category_drift",
    status,
    detail: movements.length
      ? `${movements.length} category median(s) moved sharply since the previous import.`
      : "All category medians are stable since the previous import.",
    movements: movements.sort((a, b) => Math.abs(b.ratio - 1) - Math.abs(a.ratio - 1)),
  };
}

/* ---------------------------------------------------------------------------
 * 4. Structurally impossible prices
 * ------------------------------------------------------------------------ */
function checkOutliers(rows) {
  const offenders = [];
  for (const raw of rows) {
    const r = readRow(raw);
    if (r.ppc != null && r.ppc < 0) offenders.push({ sku: r.sku, issue: "negative price_per_carat", value: r.ppc });
    else if (r.total != null && r.total < 0) offenders.push({ sku: r.sku, issue: "negative total_price", value: r.total });
    else if (r.ppc > 0 && r.weight > 0 && !r.total) offenders.push({ sku: r.sku, issue: "price per carat without a total", value: r.ppc });
  }

  return {
    name: "outliers",
    status: offenders.length ? "warn" : "ok",
    count: offenders.length,
    detail: offenders.length
      ? `${offenders.length} stone(s) carry structurally impossible prices.`
      : "No impossible prices found.",
    worstOffenders: offenders.slice(0, 10),
  };
}

/* ---------------------------------------------------------------------------
 * Orchestration
 * ------------------------------------------------------------------------ */
function runPriceIntegrityChecks(rows, previousMedians) {
  const medians = categoryMedians(rows);
  const checks = [
    checkRapIdentity(rows),
    checkTotalConsistency(rows),
    checkCategoryDrift(medians, previousMedians),
    checkOutliers(rows),
  ];

  const status = checks.some((c) => c.status === "fail")
    ? "fail"
    : checks.some((c) => c.status === "warn")
      ? "warn"
      : "ok";

  return { status, stonesChecked: rows.length, medians, checks };
}

/* ---------------------------------------------------------------------------
 * Persistence
 * ------------------------------------------------------------------------ */
async function ensurePriceAuditTable(pool) {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS price_audit_log (
      id SERIAL PRIMARY KEY,
      checked_at TIMESTAMP DEFAULT NOW(),
      source TEXT,
      status TEXT,
      stones_checked INTEGER,
      medians JSONB,
      checks JSONB
    )
  `);
}

async function loadPreviousMedians(pool) {
  try {
    const { rows } = await pool.query(
      `SELECT medians FROM price_audit_log
        WHERE status <> 'fail'
        ORDER BY id DESC LIMIT 1`
    );
    return rows[0]?.medians || null;
  } catch (_) {
    return null;
  }
}

async function recordPriceAudit(pool, source, report) {
  await pool.query(
    `INSERT INTO price_audit_log (source, status, stones_checked, medians, checks)
     VALUES ($1, $2, $3, $4::jsonb, $5::jsonb)`,
    [
      source,
      report.status,
      report.stonesChecked,
      JSON.stringify(report.medians),
      JSON.stringify(report.checks),
    ]
  );
}

const AUDIT_QUERY = `
  SELECT sku, category, grouping_type, weight, price_per_carat, total_price,
         rap_price, rap_list_price
    FROM soap_stones
`;

/**
 * Read the freshly imported catalog, run every check, persist the report and
 * log a human-readable summary. Never throws — an import must not fail because
 * its self-check could not run.
 */
async function auditPrices(pool, source = "manual") {
  try {
    await ensurePriceAuditTable(pool);
    const previousMedians = await loadPreviousMedians(pool);
    const { rows } = await pool.query(AUDIT_QUERY);
    const report = runPriceIntegrityChecks(rows, previousMedians);

    await recordPriceAudit(pool, source, report);

    const icon = report.status === "ok" ? "✅" : report.status === "warn" ? "⚠️ " : "🚨";
    console.log(`${icon} Price audit (${source}): ${report.status.toUpperCase()} over ${report.stonesChecked} stones`);
    for (const check of report.checks) {
      if (check.status === "ok" || check.status === "skipped") continue;
      console.log(`   ${check.status === "fail" ? "🚨" : "⚠️ "} ${check.name}: ${check.detail}`);
    }

    return report;
  } catch (e) {
    console.warn("⚠️  Price audit could not run:", e.message);
    return null;
  }
}

module.exports = {
  RAP_TOLERANCE_POINTS,
  TOTAL_TOLERANCE,
  MIN_RAP_MATCH_RATE,
  MAX_TOTAL_MISMATCH_RATE,
  DRIFT_WARN_RATIO,
  checkRapIdentity,
  checkTotalConsistency,
  checkCategoryDrift,
  checkOutliers,
  categoryMedians,
  runPriceIntegrityChecks,
  ensurePriceAuditTable,
  loadPreviousMedians,
  recordPriceAudit,
  auditPrices,
};
