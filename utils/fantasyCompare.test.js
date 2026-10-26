/* Run with:  node utils/fantasyCompare.test.js */

const assert = require("assert");
const { compare } = require("./fantasyCompare");

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

const lot = (o) => ({
  lot_id: o.id, lot_name: o.name, name_key: o.name.toUpperCase(), status: o.status || "STOCK",
  weight: o.weight ?? 1, quantity: 1, total_sale: o.sale ?? 0, total_cost: o.cost ?? 0,
  location: o.location || "IL OFFICE", department: "IL OFFICE", owner: "Gemstar", item_name: o.item || "Gemstone",
});
const stone = (o) => ({ sku: o.sku, category: "Emerald", branch: "Israel", weight: o.weight ?? 1, total_price: o.price ?? 0, cost_per_carat: o.cost ?? null, location: "Office" });

// LA0006 as seen in both systems on 26/10/2026.
const la0006 = { lot: lot({ id: 144297, name: "LA0006", status: "OUT", weight: 9.51, sale: 144789.75, cost: 115004.43, location: "Dubai Show" }),
                 stone: stone({ sku: "LA0006", weight: 9.51, price: 144789.75, cost: 12093 }) };

test("a stone that agrees on weight, price and cost has no issue", () => {
  const r = compare({ lots: [la0006.lot], stones: [la0006.stone] });
  assert.strictEqual(r.issues.length, 0);
  assert.strictEqual(r.summary.counts.matched, 1);
  assert.strictEqual(r.summary.counts.ok, 1);
});

test("missing, sold and deleted are told apart", () => {
  const r = compare({
    lots: [lot({ id: 1, name: "S1", status: "INVOICE" }), lot({ id: 2, name: "D1", status: "DELETE" })],
    stones: [stone({ sku: "M1" }), stone({ sku: "s1" }), stone({ sku: "D1" })],
  });
  const kinds = Object.fromEntries(r.issues.map((i) => [i.sku, i.kinds]));
  assert.deepStrictEqual(kinds, { M1: ["missing"], s1: ["sold"], D1: ["deleted"] });
});

test("weight, price and cost gaps are flagged; tiny rounding is not", () => {
  const r = compare({
    lots: [lot({ id: 1, name: "A", weight: 1.96, sale: 9.75, cost: 9.75 }), lot({ id: 2, name: "B", weight: 2.005, sale: 1000.4, cost: 500 })],
    stones: [stone({ sku: "A", weight: 1.96, price: 8820, cost: 4.97 }), stone({ sku: "B", weight: 2, price: 1000, cost: 250 })],
  });
  assert.deepStrictEqual(r.issues.find((i) => i.sku === "A").kinds, ["price"]);
  assert.strictEqual(r.issues.find((i) => i.sku === "B"), undefined);
});

test("a shared lot name is compared against the sum of its live lots", () => {
  const r = compare({
    lots: [lot({ id: 1, name: "LOT PS", weight: 3 }), lot({ id: 2, name: "LOT PS", weight: 5 }), lot({ id: 3, name: "LOT PS", weight: 9, status: "INVOICE" })],
    stones: [stone({ sku: "LOT PS", weight: 8 })],
  });
  assert.deepStrictEqual(r.issues[0].kinds, ["shared"]);
  assert.strictEqual(r.issues[0].fantasy.weight, 8);
  assert.strictEqual(r.issues[0].fantasy.lots, 2);
});

test("live Fantasy lots unknown to Barak are listed once per name; jewelry is matched by model number", () => {
  const r = compare({
    lots: [lot({ id: 1, name: "2602" }), lot({ id: 2, name: "OLD", status: "INVOICE" }), lot({ id: 3, name: "RI-HJ-044", item: "Ring Three Stone" })],
    stones: [],
    jewelry: [{ model_number: "RI-HJ-044", jewelry_type: "Rings", price: 65000 }, { model_number: "ER-1", jewelry_type: "Earrings" }],
  });
  assert.deepStrictEqual(r.issues.map((i) => [i.sku, i.kinds[0]]), [["2602", "fantasy_only"]]);
  assert.strictEqual(r.summary.jewelryInFantasy, 1);
  assert.strictEqual(r.jewelry.find((j) => j.sku === "ER-1").fantasy, null);
});

console.log(`\n${passed} passed${process.exitCode ? ", some failed" : ""}`);
