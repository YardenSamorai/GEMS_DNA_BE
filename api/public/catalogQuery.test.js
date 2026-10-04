/* Run with:  node api/public/catalogQuery.test.js
 *
 * Dependency-free, like the other *.test.js files. Exits non-zero on the
 * first failure. */

const assert = require('assert');
const q = require('./catalogQuery');

const raw = [
  { sku: 'E1', category: 'Emerald O', shape: 'Oval', weightCt: 2.1, priceTotal: 21000, pricePerCt: 10000, origin: 'Colombia', lab: 'GRS', branch: 'New York', onMemo: false, costPerCt: 6000, updatedAt: '2026-01-02' },
  { sku: 'E2', category: 'Emerald', shape: 'Cushion', weightCt: 0.8, priceTotal: 4000, pricePerCt: 5000, origin: 'Zambia', lab: 'N/A', branch: 'Israel', onMemo: true, costPerCt: 3000, updatedAt: '2026-01-03' },
  { sku: 'D1', category: 'Diamond', shape: 'Round', weightCt: 1.5, priceTotal: 15000, pricePerCt: 10000, color: 'F', clarity: 'VS1', lab: 'GIA', branch: 'New York', updatedAt: '2026-01-01' },
  { sku: 'F1', category: 'Fancy', shape: 'Radiant', weightCt: 3, priceTotal: null, pricePerCt: null, fancyColor: 'Yellow', lab: 'GIA', branch: null, updatedAt: '2026-01-04' },
  { sku: 'S1', category: 'Sapphire O', shape: 'OV', weightCt: 5.2, priceTotal: 52000, pricePerCt: 10000, origin: 'Sri Lanka', treatment: 'No Heat', branch: 'Hong Kong', updatedAt: '2026-01-05' },
];

const stones = raw.map((s) => q.toApiStone(s, { canSeeCost: true }));
const hidden = raw.map((s) => q.toApiStone(s, { canSeeCost: false }));

// Cost leaves only for viewers allowed to see it.
assert.strictEqual(stones[0].costPerCt, 6000);
assert.ok(!('costPerCt' in hidden[0]));

// Families follow the frontend's category map.
assert.deepStrictEqual(stones.map((s) => s.type), ['Emerald', 'Emerald', 'Diamond', 'Fancy Diamond', 'Gemstone']);
assert.deepStrictEqual(stones[4].gems, ['Sapphire']);
assert.strictEqual(stones[1].lab, null, '"N/A" lab reads as missing');

const skus = (rows) => rows.map((r) => r.sku);

assert.deepStrictEqual(skus(q.filterStones(stones, { type: 'emerald' })), ['E1', 'E2']);
assert.deepStrictEqual(skus(q.filterStones(stones, { type: 'diamond,fancy' })), ['D1', 'F1']);
assert.deepStrictEqual(skus(q.filterStones(stones, { gem: 'sapphire' })), ['S1']);
assert.deepStrictEqual(skus(q.filterStones(stones, { minCt: '1', maxCt: 3 })), ['E1', 'D1', 'F1']);
assert.deepStrictEqual(skus(q.filterStones(stones, { maxPrice: 20000 })), ['E2', 'D1'], 'unpriced stones fall out of a price range');
assert.deepStrictEqual(skus(q.filterStones(stones, { onMemo: 'true' })), ['E2']);
assert.deepStrictEqual(skus(q.filterStones(stones, { onMemo: false })), ['E1', 'D1', 'F1', 'S1']);
assert.deepStrictEqual(skus(q.filterStones(stones, { origin: 'colom' })), ['E1']);
assert.deepStrictEqual(skus(q.filterStones(stones, { q: 'oval new york' })), ['E1']);
assert.deepStrictEqual(skus(q.filterStones(stones, { shape: ['oval', 'Round'] })), ['E1', 'D1', 'S1']);

// Shape codes answer to their names, in filters and in free text.
assert.strictEqual(stones[4].shapeName, 'Oval');
assert.deepStrictEqual(skus(q.filterStones(stones, { shape: 'ov' })), ['S1']);
assert.deepStrictEqual(skus(q.filterStones(stones, { q: 'oval sapphire' })), ['S1']);
assert.strictEqual(q.shapeNameOf('EM'), 'Emerald');
assert.strictEqual(q.shapeNameOf('Kite'), 'Kite');
assert.strictEqual(q.shapeNameOf(''), null);

// Sorting keeps rows without the value last in both directions.
const byPriceAsc = q.page(stones, { sort: 'price', order: 'asc' }, q.STONE_SORTS, 'updated');
assert.deepStrictEqual(skus(byPriceAsc.items), ['E2', 'D1', 'E1', 'S1', 'F1']);
const byPriceDesc = q.page(stones, { sort: 'price' }, q.STONE_SORTS, 'updated');
assert.deepStrictEqual(skus(byPriceDesc.items), ['S1', 'E1', 'D1', 'E2', 'F1']);

// Paging and field projection.
const p = q.page(stones, { limit: 2, offset: 1, fields: 'sku,weightCt', sort: 'sku', order: 'asc' }, q.STONE_SORTS, 'updated');
assert.strictEqual(p.total, 5);
assert.strictEqual(p.hasMore, true);
assert.deepStrictEqual(p.items, [{ sku: 'E1', weightCt: 2.1 }, { sku: 'E2', weightCt: 0.8 }]);
assert.strictEqual(q.page(stones, { limit: 99999 }, q.STONE_SORTS, 'updated').limit, q.MAX_PAGE);

// One-dimension summary.
const byType = q.summarize(stones, 'type', q.STONE_GROUPS, q.stoneMeasure(true));
const em = byType.groups.find((g) => g.type === 'Emerald');
assert.strictEqual(em.count, 2);
assert.strictEqual(em.totalCt, 2.9);
assert.strictEqual(em.totalValue, 25000);
assert.strictEqual(em.avgPricePerCt, 8620.69);
assert.strictEqual(em.totalCost, 6000 * 2.1 + 3000 * 0.8);
assert.strictEqual(byType.totals.count, 5);

// Cost totals disappear for viewers who can't see cost.
const noCost = q.summarize(hidden, 'type', q.STONE_GROUPS, q.stoneMeasure(false));
assert.ok(noCost.groups.every((g) => !('totalCost' in g)));

// Two-dimension pivot and missing values bucketed as Unknown.
const pivot = q.summarize(stones, 'branch,type', q.STONE_GROUPS, q.stoneMeasure(false));
assert.ok(pivot.groups.some((g) => g.branch === 'New York' && g.type === 'Diamond' && g.count === 1));
assert.ok(pivot.groups.some((g) => g.branch === 'Unknown' && g.type === 'Fancy Diamond'));

const sizes = q.summarize(stones, 'sizeBucket', q.STONE_GROUPS, q.stoneMeasure(false));
assert.ok(sizes.groups.some((g) => g.sizeBucket === '5.00-9.99ct' && g.count === 1));

assert.throws(() => q.summarize(stones, 'nonsense', q.STONE_GROUPS, q.stoneMeasure(false)), /groupBy must be/);
assert.throws(() => q.summarize(stones, '', q.STONE_GROUPS, q.stoneMeasure(false)), (e) => e.status === 400);

// Jewelry.
const jewelry = [
  { model_number: 'R100', jewelry_type: 'Ring', metal_type: 'Platinum', price: '12000', total_carat: '2.5', real_unit_cost: '7000', branch: 'New York', on_memo: false },
  { model_number: 'N200', jewelry_type: 'Necklace', metal_type: '18K White Gold', price: '30000', total_carat: '8', real_unit_cost: null, branch: 'Israel', on_memo: true },
].map((j) => q.toApiJewelry(j, { canSeeCost: true }));
assert.strictEqual(jewelry[0].price, 12000);
assert.strictEqual(jewelry[0].cost, 7000);
assert.deepStrictEqual(q.filterJewelry(jewelry, { metal: 'gold' }).map((j) => j.modelNumber), ['N200']);
assert.deepStrictEqual(q.filterJewelry(jewelry, { jewelryType: 'ring', minPrice: 10000 }).map((j) => j.modelNumber), ['R100']);
const jSum = q.summarize(jewelry, 'jewelryType', q.JEWELRY_GROUPS, q.jewelryMeasure(true));
assert.strictEqual(jSum.totals.totalValue, 42000);
assert.strictEqual(jSum.totals.totalCost, 7000);

// Facets list values with counts, most common first.
const f = q.facets(stones, q.STONE_GROUPS);
assert.deepStrictEqual(f.shape[0], { value: 'Oval', count: 2 });

console.log('catalogQuery: all tests passed');
