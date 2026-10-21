/* Run with:  node utils/stoneOverrides.test.js */

const assert = require("assert");
const { sanitizePatch, toApiFields } = require("./stoneOverrides");

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

test("sets weightCt from a number", () => {
  const { set, unset } = sanitizePatch({ weightCt: 24.24 });
  assert.strictEqual(set.weightCt, 24.24);
  assert.deepStrictEqual(unset, []);
});

test("accepts fields wrapper and clears with null", () => {
  const { set, unset } = sanitizePatch({ fields: { weightCt: null, color: "Green" } });
  assert.strictEqual(set.color, "Green");
  assert.deepStrictEqual(unset, ["weightCt"]);
});

test("rejects a non-positive weight", () => {
  assert.throws(() => sanitizePatch({ weightCt: 0 }), /Invalid weightCt/);
});

test("rounds stones to an integer", () => {
  const { set } = sanitizePatch({ stones: "15.4" });
  assert.strictEqual(set.stones, 15);
});

test("ignores unknown keys and actor metadata", () => {
  const { set, unset } = sanitizePatch({ sku: "X", priceTotal: 9, weightCt: 1.2 });
  assert.deepStrictEqual(set, { weightCt: 1.2 });
  assert.deepStrictEqual(unset, []);
});

test("toApiFields drops junk keys", () => {
  assert.deepStrictEqual(toApiFields({ weightCt: 1, hack: true }), { weightCt: 1 });
});

if (!process.exitCode) console.log(`\n${passed} tests passed`);
