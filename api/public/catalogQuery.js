/* =========================================================
   Catalog query engine for the API-key surface (/api/v1, MCP)
   ---------------------------------------------------------
   Pure functions over the already-masked lists produced by
   listSoapStonesFor / listJewelryFor: they never widen what a
   viewer may see, they only filter, sort, page and aggregate.
   ========================================================= */

// Same mapping the frontend uses (src/utils/categoryMap.js) to turn the
// supplier's category strings into gem names.
const CATEGORY_MAP = {
  'Alexandrite': ['Alexandrite'],
  'Amethyst O': ['Amethyst'],
  'Aquamarine': ['Aquamarine'],
  'Aquamarine O': ['Aquamarine'],
  'Aquamarine+Emerald O': ['Aquamarine', 'Emerald'],
  'Blue Topaz O': ['Topaz'],
  'chrome diopside O': ['Chrome diopside'],
  'Diamond': ['Diamond'],
  'Diamond O': ['Diamond'],
  'Diamonds MEMO': ['Diamond'],
  'Diamonds Virtual': ['Diamond'],
  'EM+AQ+PT O': ['Emerald', 'Aquamarine', 'Tourmaline'],
  'Emerald': ['Emerald'],
  'Emerald M': ['Emerald'],
  'Emerald Memo': ['Emerald'],
  'Emerald N': ['Emerald'],
  'Emerald O': ['Emerald'],
  'Emerald Virtual': ['Emerald'],
  'Garnet O': ['Garnet'],
  'Green Tourmaline O': ['Tourmaline'],
  'Kanari O': ['Kanari'],
  'Kunzite O': ['Kunzite'],
  'Morganite O': ['Morganite'],
  'Onyx O': ['Onyx'],
  'Opal O': ['Opal'],
  'Orange Topaz O': ['Topaz'],
  'Pink Tourmaline': ['Tourmaline'],
  'Pink Tourmaline Memo': ['Tourmaline'],
  'Pink Tourmaline O': ['Tourmaline'],
  'Pink Tourmaline+Aquamarine O': ['Tourmaline', 'Aquamarine'],
  'Pink Tourmaline+Kunzite O': ['Tourmaline', 'Kunzite'],
  'Red Tourmaline O': ['Tourmaline'],
  'Rubellite O': ['Rubellite'],
  'Ruby': ['Ruby'],
  'Ruby Memo': ['Ruby'],
  'Ruby O': ['Ruby'],
  'Sapphire': ['Sapphire'],
  'Sapphire Memo': ['Sapphire'],
  'Sapphire O': ['Sapphire'],
  'Sapphires Virtual': ['Sapphire'],
  'Spinel': ['Spinel'],
  'Spinel Memo': ['Spinel'],
  'Spinel O': ['Spinel'],
  'Fancy': ['Diamond', 'Fancy'],
  'Tanzanite': ['Tanzanite'],
  'Tanzanite O': ['Tanzanite'],
  'Tourmaline Memo': ['Tourmaline'],
  'Tourmaline O': ['Tourmaline'],
  'Tsavorite O': ['Tsavorite'],
};

const gemsOf = (category) => CATEGORY_MAP[String(category || '').trim()] || [];

// The feed stores shape codes; callers (and AIs) ask in words. Mirrors the
// frontend's SHAPE_MATCH (src/pages/sales/SalesInventory.js).
const SHAPE_NAMES = {
  RD: 'Round', BR: 'Round', OV: 'Oval', MOVAL: 'Oval', EC: 'Emerald', EM: 'Emerald',
  SQEC: 'Emerald', PS: 'Pear', SQ: 'Square', CARRE: 'Square', HS: 'Heart', MQ: 'Marquise',
  CU: 'Cushion', CB: 'Cushion Brilliant', CMB: 'Cushion Modified', OCT: 'Octagonal',
  RAD: 'Radiant', SQRAD: 'Square Radiant', ASH: 'Asscher', CAB: 'Cabochon', SUGAR: 'Sugarloaf',
  SUGER: 'Sugarloaf', TR: 'Trilliant', BGT: 'Baguette', TPR: 'Tapered Baguette',
  TRPZ: 'Trapezoid', HM: 'Half Moon', PR: 'Princess', HEX: 'Hexagonal', SHI: 'Shield',
  BRIO: 'Briolette', OM: 'Old Mine', OMB: 'Old Mine', FAN: 'Fan', DROPS: 'Drops', MIX: 'Mixed',
  CAD: 'Cadillac', KITE: 'Kite', LOZENGE: 'Lozenge', 'RD-CAB': 'Round Cabochon',
  'PS-CAB': 'Pear Cabochon', 'OV-CAB': 'Oval Cabochon', 'OV ROSE': 'Oval Rose Cut',
  'ROSE CUT': 'Rose Cut', 'EC+CU': 'Emerald / Cushion', 'RD+PS': 'Round / Pear',
};
const shapeNameOf = (code) => {
  const raw = String(code || '').trim();
  if (!raw) return null;
  return SHAPE_NAMES[raw.toUpperCase()] || raw;
};

const STONE_TYPES = ['Diamond', 'Fancy Diamond', 'Emerald', 'Gemstone'];

const stoneTypeOf = (category) => {
  const gems = gemsOf(category);
  if (gems.includes('Fancy')) return 'Fancy Diamond';
  if (gems.includes('Diamond')) return 'Diamond';
  if (gems.includes('Emerald')) return 'Emerald';
  return 'Gemstone';
};

const SIZE_BUCKETS = [
  [0, 0.5, '<0.50ct'],
  [0.5, 1, '0.50-0.99ct'],
  [1, 2, '1.00-1.99ct'],
  [2, 3, '2.00-2.99ct'],
  [3, 5, '3.00-4.99ct'],
  [5, 10, '5.00-9.99ct'],
  [10, Infinity, '10ct+'],
];
const PRICE_BUCKETS = [
  [0, 1000, '<$1K'],
  [1000, 5000, '$1K-5K'],
  [5000, 10000, '$5K-10K'],
  [10000, 25000, '$10K-25K'],
  [25000, 50000, '$25K-50K'],
  [50000, 100000, '$50K-100K'],
  [100000, Infinity, '$100K+'],
];
const bucketOf = (buckets, v) => {
  if (v == null || !Number.isFinite(v)) return 'Unknown';
  const b = buckets.find(([lo, hi]) => v >= lo && v < hi);
  return b ? b[2] : 'Unknown';
};

/* ---------- public record shapes ---------- */

const toApiStone = (s, { canSeeCost }) => {
  const out = {
    sku: s.sku,
    type: stoneTypeOf(s.category),
    gems: gemsOf(s.category),
    category: s.category || null,
    shape: s.shape || null,
    shapeName: shapeNameOf(s.shape),
    weightCt: s.weightCt,
    color: s.color || null,
    clarity: s.clarity || null,
    fancyIntensity: s.fancyIntensity || null,
    fancyColor: s.fancyColor || null,
    fancyOvertone: s.fancyOvertone || null,
    cut: s.cut || null,
    polish: s.polish || null,
    symmetry: s.symmetry || null,
    fluorescence: s.fluorescence || null,
    lab: s.lab && s.lab !== 'N/A' ? s.lab : null,
    certificateNumber: s.certificateNumber || null,
    origin: s.origin && s.origin !== 'N/A' ? s.origin : null,
    treatment: s.treatment || null,
    measurements: s.measurements || null,
    ratio: s.ratio,
    tablePercent: s.tablePercent,
    depthPercent: s.depthPercent,
    priceTotal: s.priceTotal,
    pricePerCt: s.pricePerCt,
    rapListPrice: s.rapListPrice,
    rapDiscount: s.rapPrice,
    pairSku: s.pairSku || null,
    groupingType: s.groupingType || null,
    stones: s.stones,
    jewelryModel: s.jewelryModel || null,
    branch: s.branch,
    exactLocation: s.exactLocation,
    holder: s.holder,
    onMemo: !!s.onMemo,
    onHold: !!s.onHold,
    imageUrl: s.imageUrl || null,
    videoUrl: s.videoUrl || null,
    certificateUrl: s.certificateUrl || null,
    updatedAt: s.updatedAt,
  };
  if (canSeeCost) out.costPerCt = s.costPerCt;
  return out;
};

const num = (v) => (v == null || v === '' || !Number.isFinite(Number(v)) ? null : Number(v));

const toApiJewelry = (j, { canSeeCost }) => {
  const out = {
    modelNumber: j.model_number,
    stockNumber: j.stock_number || null,
    title: j.title || null,
    jewelryType: j.jewelry_type || null,
    style: j.style || null,
    collection: j.collection || null,
    category: j.category || null,
    metalType: j.metal_type || null,
    stoneType: j.stone_type || null,
    price: num(j.price),
    currency: j.currency || null,
    totalCarat: num(j.total_carat),
    centerStoneCarat: num(j.center_stone_carat),
    centerStoneShape: j.center_stone_shape || null,
    centerStoneColor: j.center_stone_color || null,
    centerStoneClarity: j.center_stone_clarity || null,
    jewelryWeight: num(j.jewelry_weight),
    jewelrySize: j.jewelry_size || null,
    availability: j.availability || null,
    certificateNumber: j.certificate_number || null,
    certificateUrl: j.certificate_link || null,
    videoUrl: j.video_link || null,
    picturesUrl: j.all_pictures_link || null,
    description: j.description || null,
    branch: j.branch,
    exactLocation: j.exact_location,
    holder: j.holder,
    onMemo: !!j.on_memo,
    onHold: !!j.on_hold,
    firstSeenAt: j.first_seen_at || null,
  };
  if (canSeeCost) out.cost = num(j.real_unit_cost);
  return out;
};

/* ---------- parameter parsing ---------- */

const listParam = (v) => {
  if (v == null || v === '') return [];
  const arr = Array.isArray(v) ? v : String(v).split(',');
  return arr.map((x) => String(x).trim()).filter(Boolean);
};
const numParam = (v) => (v == null || v === '' || !Number.isFinite(Number(v)) ? null : Number(v));
const boolParam = (v) => {
  if (v === true || v === false) return v;
  if (v == null || v === '') return null;
  const s = String(v).toLowerCase();
  if (['1', 'true', 'yes'].includes(s)) return true;
  if (['0', 'false', 'no'].includes(s)) return false;
  return null;
};
const lc = (v) => String(v == null ? '' : v).trim().toLowerCase();

// Each filter kind returns a predicate, or null when the param is absent.
const anyEq = (list, field) => {
  const want = new Set(list.map(lc));
  return want.size ? (r) => want.has(lc(field(r))) : null;
};
const anyContains = (list, field) => {
  const want = list.map(lc);
  return want.length ? (r) => want.some((w) => lc(field(r)).includes(w)) : null;
};
const range = (min, max, field) => {
  if (min == null && max == null) return null;
  return (r) => {
    const v = field(r);
    if (v == null || !Number.isFinite(v)) return false;
    return (min == null || v >= min) && (max == null || v <= max);
  };
};
const flag = (want, field) => (want == null ? null : (r) => !!field(r) === want);
const textSearch = (q, fields) => {
  const terms = lc(q).split(/\s+/).filter(Boolean);
  if (!terms.length) return null;
  return (r) => {
    const hay = fields.map((f) => lc(r[f])).join(' ');
    return terms.every((t) => hay.includes(t));
  };
};

const applyFilters = (rows, preds) => {
  const active = preds.filter(Boolean);
  return active.length ? rows.filter((r) => active.every((p) => p(r))) : rows;
};

/* ---------- stones ---------- */

const STONE_SORTS = {
  weight: (r) => r.weightCt,
  price: (r) => r.priceTotal,
  ppc: (r) => r.pricePerCt,
  ratio: (r) => r.ratio,
  sku: (r) => r.sku,
  updated: (r) => r.updatedAt,
};

const STONE_TEXT_FIELDS = [
  'sku', 'type', 'gems', 'category', 'shape', 'shapeName', 'color', 'clarity', 'fancyColor', 'fancyIntensity',
  'lab', 'origin', 'treatment', 'certificateNumber', 'branch', 'groupingType', 'jewelryModel',
];

function filterStones(rows, p = {}) {
  const typeWant = listParam(p.type).map((t) => {
    const s = lc(t);
    if (s.startsWith('fancy')) return 'fancy diamond';
    if (s.startsWith('diamond')) return 'diamond';
    if (s.startsWith('emerald')) return 'emerald';
    if (s.startsWith('gem')) return 'gemstone';
    return s;
  });
  const gemWant = listParam(p.gem).map(lc);
  return applyFilters(rows, [
    textSearch(p.q, STONE_TEXT_FIELDS),
    anyEq(listParam(p.sku), (r) => r.sku),
    typeWant.length ? (r) => typeWant.includes(lc(r.type)) : null,
    gemWant.length ? (r) => r.gems.some((g) => gemWant.includes(lc(g))) : null,
    anyContains(listParam(p.category), (r) => r.category),
    (() => {
      const want = new Set(listParam(p.shape).map(lc));
      return want.size ? (r) => want.has(lc(r.shape)) || want.has(lc(r.shapeName)) : null;
    })(),
    anyEq(listParam(p.color), (r) => r.color),
    anyEq(listParam(p.clarity), (r) => r.clarity),
    anyEq(listParam(p.lab), (r) => r.lab),
    anyEq(listParam(p.cut), (r) => r.cut),
    anyEq(listParam(p.polish), (r) => r.polish),
    anyEq(listParam(p.symmetry), (r) => r.symmetry),
    anyEq(listParam(p.fluorescence), (r) => r.fluorescence),
    anyEq(listParam(p.branch), (r) => r.branch),
    anyEq(listParam(p.groupingType), (r) => r.groupingType),
    anyContains(listParam(p.origin), (r) => r.origin),
    anyContains(listParam(p.treatment), (r) => r.treatment),
    anyContains(listParam(p.fancyColor), (r) => r.fancyColor),
    range(numParam(p.minCt), numParam(p.maxCt), (r) => r.weightCt),
    range(numParam(p.minPrice), numParam(p.maxPrice), (r) => r.priceTotal),
    range(numParam(p.minPpc), numParam(p.maxPpc), (r) => r.pricePerCt),
    range(numParam(p.minRatio), numParam(p.maxRatio), (r) => r.ratio),
    flag(boolParam(p.onMemo), (r) => r.onMemo),
    flag(boolParam(p.onHold), (r) => r.onHold),
    flag(boolParam(p.hasImage), (r) => r.imageUrl),
    flag(boolParam(p.hasVideo), (r) => r.videoUrl),
    flag(boolParam(p.hasCertificate), (r) => r.certificateUrl || r.certificateNumber),
    flag(boolParam(p.isPair), (r) => r.pairSku),
  ]);
}

/* ---------- jewelry ---------- */

const JEWELRY_SORTS = {
  price: (r) => r.price,
  carat: (r) => r.totalCarat,
  model: (r) => r.modelNumber,
  newest: (r) => r.firstSeenAt,
};

const JEWELRY_TEXT_FIELDS = [
  'modelNumber', 'stockNumber', 'title', 'jewelryType', 'style', 'collection', 'category',
  'metalType', 'stoneType', 'centerStoneShape', 'description', 'branch',
];

function filterJewelry(rows, p = {}) {
  return applyFilters(rows, [
    textSearch(p.q, JEWELRY_TEXT_FIELDS),
    anyEq(listParam(p.model), (r) => r.modelNumber),
    anyContains(listParam(p.jewelryType), (r) => r.jewelryType),
    anyContains(listParam(p.collection), (r) => r.collection),
    anyContains(listParam(p.metal), (r) => r.metalType),
    anyContains(listParam(p.stoneType), (r) => r.stoneType),
    anyContains(listParam(p.category), (r) => r.category),
    anyContains(listParam(p.centerShape), (r) => r.centerStoneShape),
    anyEq(listParam(p.branch), (r) => r.branch),
    anyContains(listParam(p.availability), (r) => r.availability),
    range(numParam(p.minPrice), numParam(p.maxPrice), (r) => r.price),
    range(numParam(p.minCt), numParam(p.maxCt), (r) => r.totalCarat),
    flag(boolParam(p.onMemo), (r) => r.onMemo),
    flag(boolParam(p.onHold), (r) => r.onHold),
  ]);
}

/* ---------- sort, page, project ---------- */

function sortRows(rows, sorts, key, order) {
  const get = sorts[key];
  if (!get) return rows;
  const dir = lc(order) === 'asc' ? 1 : -1;
  // Rows missing the sort value always go last, whatever the direction.
  return [...rows].sort((a, b) => {
    const va = get(a);
    const vb = get(b);
    const na = va == null || va === '';
    const nb = vb == null || vb === '';
    if (na || nb) return na === nb ? 0 : na ? 1 : -1;
    if (typeof va === 'number' && typeof vb === 'number') return (va - vb) * dir;
    return String(va).localeCompare(String(vb), undefined, { numeric: true }) * dir;
  });
}

const MAX_PAGE = 500;

function page(rows, p, sorts, defaultSort) {
  const sorted = sortRows(rows, sorts, p.sort || defaultSort, p.order);
  const limit = Math.min(MAX_PAGE, Math.max(1, Math.floor(numParam(p.limit) ?? 50)));
  const offset = Math.max(0, Math.floor(numParam(p.offset) ?? 0));
  const fields = listParam(p.fields);
  const items = sorted.slice(offset, offset + limit).map((r) => {
    if (!fields.length) return r;
    const o = {};
    for (const f of fields) if (f in r) o[f] = r[f];
    return o;
  });
  return {
    total: rows.length,
    offset,
    limit,
    hasMore: offset + items.length < rows.length,
    items,
  };
}

/* ---------- aggregation ---------- */

const STONE_GROUPS = {
  type: (r) => [r.type],
  gem: (r) => (r.gems.length ? r.gems : ['Other']),
  category: (r) => [r.category],
  shape: (r) => [r.shapeName],
  color: (r) => [r.color],
  clarity: (r) => [r.clarity],
  lab: (r) => [r.lab],
  origin: (r) => [r.origin],
  treatment: (r) => [r.treatment],
  fluorescence: (r) => [r.fluorescence],
  branch: (r) => [r.branch],
  groupingType: (r) => [r.groupingType],
  onMemo: (r) => [r.onMemo ? 'On memo' : 'In house'],
  sizeBucket: (r) => [bucketOf(SIZE_BUCKETS, r.weightCt)],
  priceBucket: (r) => [bucketOf(PRICE_BUCKETS, r.priceTotal)],
};

const JEWELRY_GROUPS = {
  jewelryType: (r) => [r.jewelryType],
  collection: (r) => [r.collection],
  category: (r) => [r.category],
  metalType: (r) => [r.metalType],
  stoneType: (r) => [r.stoneType],
  branch: (r) => [r.branch],
  availability: (r) => [r.availability],
  onMemo: (r) => [r.onMemo ? 'On memo' : 'In house'],
  priceBucket: (r) => [bucketOf(PRICE_BUCKETS, r.price)],
};

const round = (v, d = 2) => (Number.isFinite(v) ? Math.round(v * 10 ** d) / 10 ** d : null);

// Group rows by up to two dimensions and total them. `measure` reads the
// numbers each row contributes; a row with several gems counts in each gem.
function summarize(rows, groupSpec, groups, measure) {
  const dims = listParam(groupSpec).slice(0, 2);
  const bad = dims.filter((d) => !groups[d]);
  if (!dims.length || bad.length) {
    const err = new Error(
      `groupBy must be one or two of: ${Object.keys(groups).join(', ')}`
    );
    err.status = 400;
    throw err;
  }
  const buckets = new Map();
  const totals = { count: 0, totalCt: 0, totalValue: 0, totalCost: 0, costKnown: 0 };
  const add = (acc, m) => {
    acc.count += 1;
    if (m.ct != null) acc.totalCt += m.ct;
    if (m.value != null) {
      acc.totalValue += m.value;
      acc.minPrice = acc.minPrice == null ? m.value : Math.min(acc.minPrice, m.value);
      acc.maxPrice = acc.maxPrice == null ? m.value : Math.max(acc.maxPrice, m.value);
    }
    if (m.cost != null) {
      acc.totalCost += m.cost;
      acc.costKnown += 1;
    }
  };
  for (const r of rows) {
    const m = measure(r);
    add(totals, m);
    const keys = dims.map((d) => groups[d](r).map((v) => (v == null || v === '' ? 'Unknown' : String(v))));
    const combos = keys.length === 1 ? keys[0].map((a) => [a]) : keys[0].flatMap((a) => keys[1].map((b) => [a, b]));
    for (const combo of combos) {
      const id = combo.join('\u0000');
      if (!buckets.has(id)) {
        const g = { count: 0, totalCt: 0, totalValue: 0, totalCost: 0, costKnown: 0 };
        dims.forEach((d, i) => { g[d] = combo[i]; });
        buckets.set(id, g);
      }
      add(buckets.get(id), m);
    }
  }
  const finish = (g) => {
    const out = { ...g };
    out.totalCt = round(g.totalCt);
    out.totalValue = round(g.totalValue);
    out.avgPrice = g.count ? round(g.totalValue / g.count) : null;
    out.avgPricePerCt = g.totalCt > 0 ? round(g.totalValue / g.totalCt) : null;
    if (g.costKnown) out.totalCost = round(g.totalCost);
    else delete out.totalCost;
    delete out.costKnown;
    return out;
  };
  return {
    groupBy: dims,
    totals: finish(totals),
    groups: [...buckets.values()].map(finish).sort((a, b) => b.count - a.count),
  };
}

const stoneMeasure = (canSeeCost) => (r) => ({
  ct: r.weightCt,
  value: r.priceTotal,
  cost: canSeeCost && r.costPerCt != null && r.weightCt != null ? r.costPerCt * r.weightCt : null,
});
const jewelryMeasure = (canSeeCost) => (r) => ({
  ct: r.totalCarat,
  value: r.price,
  cost: canSeeCost ? r.cost ?? null : null,
});

// Distinct values (with counts) per dimension, so a caller — or an AI — can
// discover the vocabulary before filtering.
function facets(rows, groups) {
  const out = {};
  for (const [dim, get] of Object.entries(groups)) {
    const counts = new Map();
    for (const r of rows) {
      for (const v of get(r)) {
        if (v == null || v === '') continue;
        counts.set(String(v), (counts.get(String(v)) || 0) + 1);
      }
    }
    out[dim] = [...counts.entries()]
      .sort((a, b) => b[1] - a[1])
      .slice(0, 60)
      .map(([value, count]) => ({ value, count }));
  }
  return out;
}

module.exports = {
  STONE_TYPES,
  STONE_SORTS,
  JEWELRY_SORTS,
  STONE_GROUPS,
  JEWELRY_GROUPS,
  MAX_PAGE,
  gemsOf,
  shapeNameOf,
  stoneTypeOf,
  toApiStone,
  toApiJewelry,
  filterStones,
  filterJewelry,
  sortRows,
  page,
  summarize,
  stoneMeasure,
  jewelryMeasure,
  facets,
};
