/* Run with:  node utils/priceIntegrity.test.js
 *
 * Deliberately dependency-free so it can run anywhere, including on a box that
 * has never installed dev tooling. Exits non-zero on the first failure. */

const assert = require("assert");
const {
  checkRapIdentity,
  checkTotalConsistency,
  checkCategoryDrift,
  checkOutliers,
  categoryMedians,
  runPriceIntegrityChecks,
} = require("./priceIntegrity");

let passed = 0;
const test = (name, fn) => {
  try {
    fn();
    passed++;
    console.log(`  ok  ${name}`);
  } catch (e) {
    console.error(`  FAIL  ${name}`);
    console.error(`        ${e.message}`);
    process.exitCode = 1;
  }
};

/* Real rows from the catalog: price, Rap list and Rap percentage as the
 * supplier sends them. These satisfy ppc = rapList x (1 + rap/100). */
const diamonds = [
  { sku: "T1015", category: "Diamond", weight: 1, price_per_carat: 5590, total_price: 5590, rap_list_price: 8600, rap_price: -35 },
  { sku: "T9312", category: "Diamond", weight: 1, price_per_carat: 4690, total_price: 4690, rap_list_price: 6700, rap_price: -30 },
  { sku: "T9098", category: "Diamond", weight: 1, price_per_carat: 16500, total_price: 16500, rap_list_price: 22000, rap_price: -25 },
  { sku: "T7244", category: "Diamond", weight: 1, price_per_carat: 58750, total_price: 58750, rap_list_price: 23500, rap_price: 150 },
  { sku: "T8404", category: "Diamond", weight: 1, price_per_carat: 47250, total_price: 47250, rap_list_price: 63000, rap_price: -25 },
];

const doubled = diamonds.map((d) => ({
  ...d,
  price_per_carat: d.price_per_carat * 2,
  total_price: d.total_price * 2,
}));

console.log("\nRap identity");
test("passes on prices stored exactly as the supplier sends them", () => {
  const r = checkRapIdentity(diamonds);
  assert.strictEqual(r.status, "ok");
  assert.strictEqual(r.matched, 5);
});

test("catches an importer that starts doubling prices again", () => {
  const r = checkRapIdentity(doubled);
  assert.strictEqual(r.status, "fail");
  assert.strictEqual(r.matched, 0);
});

test("catches an importer that starts halving prices", () => {
  const halved = diamonds.map((d) => ({ ...d, price_per_carat: d.price_per_carat / 2 }));
  assert.strictEqual(checkRapIdentity(halved).status, "fail");
});

test("skips cleanly when no stone carries Rap data", () => {
  const r = checkRapIdentity([{ sku: "ME505", category: "Sapphire", price_per_carat: 60000 }]);
  assert.strictEqual(r.status, "skipped");
});

console.log("\nTotal consistency");
test("accepts totals equal to price per carat x weight", () => {
  const rows = [{ sku: "ME505", weight: 10.09, price_per_carat: 60000, total_price: 605400 }];
  assert.strictEqual(checkTotalConsistency(rows).status, "ok");
});

test("catches a total that was scaled without its price per carat", () => {
  const rows = Array.from({ length: 10 }, (_, i) => ({
    sku: `X${i}`, weight: 2, price_per_carat: 1000, total_price: 1000,
  }));
  const r = checkTotalConsistency(rows);
  assert.strictEqual(r.status, "fail");
  assert.strictEqual(r.mismatched, 10);
});

test("does not let parcels and sets decide the verdict", () => {
  // Parcels legitimately break the identity: the feed's weight covers the whole
  // lot while the price columns describe one unit.
  const rows = Array.from({ length: 50 }, (_, i) => ({
    sku: `P${i}`, grouping_type: "Parcel", weight: 35, price_per_carat: 1000, total_price: 4167,
  }));
  const r = checkTotalConsistency(rows);
  assert.strictEqual(r.status, "skipped");
  assert.strictEqual(r.multiStoneMismatched, 50);
  assert.strictEqual(r.checked, 0);
});

test("still fails on singles even when parcels are noisy", () => {
  const rows = [
    ...Array.from({ length: 50 }, (_, i) => ({
      sku: `P${i}`, grouping_type: "Parcel", weight: 35, price_per_carat: 1000, total_price: 4167,
    })),
    ...Array.from({ length: 20 }, (_, i) => ({
      sku: `S${i}`, grouping_type: "Single", weight: 2, price_per_carat: 1000, total_price: 1000,
    })),
  ];
  assert.strictEqual(checkTotalConsistency(rows).status, "fail");
});

console.log("\nCategory drift");
const baseline = categoryMedians([
  ...Array.from({ length: 20 }, (_, i) => ({ sku: `S${i}`, category: "Sapphire", price_per_carat: 3500 })),
  ...Array.from({ length: 20 }, (_, i) => ({ sku: `E${i}`, category: "Emerald", price_per_carat: 2750 })),
]);

test("stays quiet when medians hold steady", () => {
  const now = categoryMedians([
    ...Array.from({ length: 20 }, (_, i) => ({ sku: `S${i}`, category: "Sapphire", price_per_carat: 3600 })),
    ...Array.from({ length: 20 }, (_, i) => ({ sku: `E${i}`, category: "Emerald", price_per_carat: 2700 })),
  ]);
  assert.strictEqual(checkCategoryDrift(now, baseline).status, "ok");
});

test("fails when a category median doubles — the coloured-stone safety net", () => {
  const now = categoryMedians([
    ...Array.from({ length: 20 }, (_, i) => ({ sku: `S${i}`, category: "Sapphire", price_per_carat: 7000 })),
    ...Array.from({ length: 20 }, (_, i) => ({ sku: `E${i}`, category: "Emerald", price_per_carat: 2750 })),
  ]);
  const r = checkCategoryDrift(now, baseline);
  assert.strictEqual(r.status, "fail");
  assert.strictEqual(r.movements[0].category, "Sapphire");
});

test("fails when a category median halves", () => {
  const now = categoryMedians(
    Array.from({ length: 20 }, (_, i) => ({ sku: `S${i}`, category: "Sapphire", price_per_carat: 1750 }))
  );
  assert.strictEqual(checkCategoryDrift(now, baseline).status, "fail");
});

test("treats the first ever run as a baseline instead of a failure", () => {
  assert.strictEqual(checkCategoryDrift(baseline, null).status, "skipped");
});

console.log("\nOutliers");
test("flags negative and structurally impossible prices", () => {
  const r = checkOutliers([
    { sku: "A", price_per_carat: -5 },
    { sku: "B", weight: 2, price_per_carat: 1000, total_price: 0 },
    { sku: "C", weight: 2, price_per_carat: 1000, total_price: 2000 },
  ]);
  assert.strictEqual(r.count, 2);
});

console.log("\nEnd to end");
test("a clean catalog reports ok", () => {
  assert.strictEqual(runPriceIntegrityChecks(diamonds, null).status, "ok");
});

test("a doubled catalog reports fail", () => {
  assert.strictEqual(runPriceIntegrityChecks(doubled, null).status, "fail");
});

console.log(`\n${passed} passed${process.exitCode ? " (with failures)" : ""}\n`);
