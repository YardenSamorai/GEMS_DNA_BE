/* Run with:  node utils/stoneOverrides.test.js */

const assert = require("assert");
const { sanitizePatch, toApiFields, nextOverrideState, parseRawBarak } = require("./stoneOverrides");

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

test("first override remembers the live Barak value", () => {
  const { fields, originals } = nextOverrideState({}, {}, { weightCt: 24.24 }, [], { weightCt: 0.71 });
  assert.strictEqual(fields.weightCt, 24.24);
  assert.strictEqual(originals.weightCt, 0.71);
});

test("later edits keep the first original", () => {
  const { originals } = nextOverrideState(
    { weightCt: 24.24 },
    { weightCt: 0.71 },
    { weightCt: 25 },
    [],
    { weightCt: 24.24 }
  );
  assert.strictEqual(originals.weightCt, 0.71);
});

test("unset drops the field and its original", () => {
  const { fields, originals } = nextOverrideState(
    { weightCt: 24.24, color: "Green" },
    { weightCt: 0.71, color: "" },
    {},
    ["weightCt"],
    {}
  );
  assert.deepStrictEqual(fields, { color: "Green" });
  assert.deepStrictEqual(originals, { color: "" });
});

test("reads Weight out of the Barak snapshot", () => {
  const barak = parseRawBarak(JSON.stringify({ SKU: "MTBG-0001", Weight: "0.71", Stones: "15" }));
  assert.strictEqual(barak.weightCt, 0.71);
  assert.strictEqual(barak.stones, 15);
});

if (!process.exitCode) console.log(`\n${passed} tests passed`);
