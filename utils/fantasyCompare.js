/* =============================================================================
 * Barak feed ↔ Fantasy mirror comparison (pure — no I/O)
 * =============================================================================
 *
 * Lots match Barak by name: Fantasy's Lot Name is the Barak SKU. Generic names
 * ("LOT PS") are shared by many lots, so a name is compared against the sum of
 * its live lots.
 *
 * Field mapping verified on 26/10/2026 (LA0006, T9097, T7379, M104-0127):
 *   Barak total_price      = Fantasy TotalSale
 *   Barak cost_per_carat   = Fantasy Cost (per carat); × weight = TotalCost
 * ========================================================================== */

// Lots that are still Eshed's in some form (in stock, out on memo, set in a
// piece or a box, or in production).
const LIVE = new Set(["STOCK", "MEMO", "OUT", "BOXED", "JOBORDER", "CONSUMED"]);
const SOLD = "INVOICE";

const WEIGHT_TOLERANCE = 0.011;
const MONEY_TOLERANCE = 0.01;

const key = (s) => String(s ?? "").trim().toUpperCase();
const n = (v) => (v === null || v === undefined || v === "" || !Number.isFinite(Number(v)) ? null : Number(v));
const sum = (rows, f) => rows.reduce((a, r) => a + (n(r[f]) || 0), 0);
const round = (v, d = 2) => (v === null ? null : Math.round(v * 10 ** d) / 10 ** d);
const uniq = (arr) => [...new Set(arr.filter(Boolean))];

const moneyDiffers = (a, b) => Math.abs(a - b) > Math.max(1, MONEY_TOLERANCE * Math.max(Math.abs(a), Math.abs(b)));

const countBy = (rows, f) =>
  rows.reduce((m, r) => {
    const k = f(r) || "—";
    m[k] = (m[k] || 0) + 1;
    return m;
  }, {});

const fantasySide = (lots) => {
  if (!lots.length) return null;
  const live = lots.filter((l) => LIVE.has(l.status));
  const shown = live.length ? live : lots;
  return {
    lots: shown.length,
    lotIds: shown.map((l) => l.lot_id).slice(0, 20),
    statuses: uniq(lots.map((l) => l.status)),
    weight: round(sum(shown, "weight")),
    quantity: round(sum(shown, "quantity"), 0),
    sale: round(sum(shown, "total_sale")),
    cost: round(sum(shown, "total_cost")),
    location: uniq(shown.map((l) => l.location)).join(", ") || null,
    department: uniq(shown.map((l) => l.department)).join(", ") || null,
    owner: uniq(shown.map((l) => l.owner)).join(", ") || null,
    itemName: uniq(shown.map((l) => l.item_name)).join(", ") || null,
  };
};

/**
 * @param {object} input
 * @param {Array} input.lots    fantasy_lots rows
 * @param {Array} input.stones  soap_stones rows (sku, category, branch, weight, total_price, cost_per_carat, location, holder)
 * @param {Array} input.jewelry jewelry_products rows (model_number, jewelry_type, price, location)
 */
function compare({ lots = [], stones = [], jewelry = [] }) {
  const byName = new Map();
  for (const l of lots) {
    const k = l.name_key || key(l.lot_name);
    if (!k) continue;
    if (!byName.has(k)) byName.set(k, []);
    byName.get(k).push(l);
  }

  const issues = [];
  const counts = { matched: 0, ok: 0, missing: 0, sold: 0, deleted: 0, weight: 0, price: 0, cost: 0, shared: 0, fantasyOnly: 0 };
  const barakKeys = new Set();

  for (const s of stones) {
    const k = key(s.sku);
    if (!k) continue;
    barakKeys.add(k);
    const group = byName.get(k) || [];
    const live = group.filter((l) => LIVE.has(l.status));
    const kinds = [];

    if (!group.length) kinds.push("missing");
    else if (!live.length) kinds.push(group.some((l) => l.status === SOLD) ? "sold" : "deleted");
    else {
      counts.matched++;
      const bWeight = n(s.weight);
      const fWeight = sum(live, "weight");
      if (bWeight !== null && Math.abs(fWeight - bWeight) > WEIGHT_TOLERANCE) kinds.push("weight");

      const bPrice = n(s.total_price);
      const fSale = sum(live, "total_sale");
      if (bPrice && moneyDiffers(bPrice, fSale)) kinds.push("price");

      const bCostPerCt = n(s.cost_per_carat);
      const fCost = sum(live, "total_cost");
      if (bCostPerCt && bWeight && moneyDiffers(bCostPerCt * bWeight, fCost)) kinds.push("cost");

      if (live.length > 1) kinds.push("shared");
      if (!kinds.length) counts.ok++;
    }

    for (const kind of kinds) counts[kind]++;
    if (!kinds.length) continue;

    const bWeight = n(s.weight);
    const bCostPerCt = n(s.cost_per_carat);
    issues.push({
      sku: s.sku,
      kinds,
      category: s.category || null,
      barak: {
        branch: s.branch || null,
        weight: bWeight,
        price: n(s.total_price),
        cost: bCostPerCt && bWeight ? round(bCostPerCt * bWeight) : null,
        location: s.location || null,
        holder: s.holder || null,
      },
      fantasy: fantasySide(group),
    });
  }

  const jewelryKeys = new Set(jewelry.map((j) => key(j.model_number)).filter(Boolean));
  const jewelryRows = jewelry.map((j) => {
    const group = (byName.get(key(j.model_number)) || []).filter((l) => l.status !== "DELETE");
    return {
      sku: j.model_number,
      type: j.jewelry_type || null,
      price: n(j.price),
      location: j.location || null,
      fantasy: fantasySide(group),
    };
  });

  for (const [k, group] of byName) {
    if (barakKeys.has(k) || jewelryKeys.has(k)) continue;
    const live = group.filter((l) => LIVE.has(l.status));
    if (!live.length) continue;
    counts.fantasyOnly++;
    issues.push({
      sku: live[0].lot_name,
      kinds: ["fantasy_only"],
      category: live[0].item_name || null,
      barak: null,
      fantasy: fantasySide(group),
    });
  }

  const liveLots = lots.filter((l) => LIVE.has(l.status));
  const summary = {
    fantasy: {
      lots: lots.length,
      live: liveLots.length,
      byStatus: countBy(lots, (l) => l.status),
      byDepartment: countBy(liveLots, (l) => l.department),
      byOwner: countBy(liveLots, (l) => l.owner),
      byType: countBy(liveLots, (l) => l.item_name),
      totalCost: round(sum(liveLots, "total_cost"), 0),
      totalSale: round(sum(liveLots, "total_sale"), 0),
    },
    barak: { stones: stones.length, jewelry: jewelry.length },
    counts,
    jewelryInFantasy: jewelryRows.filter((j) => j.fantasy).length,
  };

  return { summary, issues, jewelry: jewelryRows };
}

module.exports = { compare, LIVE };
