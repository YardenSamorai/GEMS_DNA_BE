/* Run with:  node utils/barakRow.test.js */

const assert = require("assert");
const { commentSpill, realignCertComments } = require("./barakRow");

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

// Shapes copied from the live feed (T9097, P9397, NY159).
const oneComma = () => ({
  SKU: "T9097",
  "Cert.Comments": '"""Clouds',
  certificateImageJPG: ' pinpoints and surface graining are not shown."""',
  Origin: "", TradeShow: "", GroupingType: "JCK 2022", Box: "Single", Stones: "",
  cost_per_carat: "1", Holder: "37662.34", Location: "", JewelryModel: "Dubai Show",
});

const twoCommas = () => ({
  SKU: "P9397",
  "Cert.Comments": '"""Internal laser drilling',
  certificateImageJPG: " clouds",
  Origin: ' pinpoints and surface graining are not shown."""',
  TradeShow: "", GroupingType: "", Box: "JCK 2022", Stones: "Single",
  cost_per_carat: "", Holder: "1", Location: "89.67", JewelryModel: "",
});

const intact = () => ({
  SKU: "NY159",
  "Cert.Comments": '"""Pinpoints are not shown."""',
  certificateImageJPG: "", Origin: "", TradeShow: "JCK 2022", GroupingType: "Single",
  Box: "", Stones: "1", cost_per_carat: "13500", Holder: "", Location: "Itamar Jewelry - JN",
  JewelryModel: "RI-HJ-032",
});

test("one comma: location, holder and cost return to their fields", () => {
  const s = oneComma();
  assert.strictEqual(realignCertComments(s), 1);
  assert.strictEqual(s["Cert.Comments"], '"""Clouds, pinpoints and surface graining are not shown."""');
  assert.strictEqual(s.certificateImageJPG, "");
  assert.strictEqual(s.TradeShow, "JCK 2022");
  assert.strictEqual(s.GroupingType, "Single");
  assert.strictEqual(s.Stones, "1");
  assert.strictEqual(s.cost_per_carat, "37662.34");
  assert.strictEqual(s.Holder, "");
  assert.strictEqual(s.Location, "Dubai Show");
  assert.strictEqual(s.JewelryModel, null);
});

test("two commas: numbers never land in Location; lost tail is null", () => {
  const s = twoCommas();
  assert.strictEqual(realignCertComments(s), 2);
  assert.strictEqual(s["Cert.Comments"], '"""Internal laser drilling, clouds, pinpoints and surface graining are not shown."""');
  assert.strictEqual(s.TradeShow, "JCK 2022");
  assert.strictEqual(s.GroupingType, "Single");
  assert.strictEqual(s.Stones, "1");
  assert.strictEqual(s.cost_per_carat, "89.67");
  assert.strictEqual(s.Holder, "");
  assert.strictEqual(s.Location, null);
  assert.strictEqual(s.JewelryModel, null);
});

test("a holder name pushed into Location moves back to Holder", () => {
  const s = { ...oneComma(), Location: "Meirav", JewelryModel: "Eshed New York" };
  realignCertComments(s);
  assert.strictEqual(s.Holder, "Meirav");
  assert.strictEqual(s.Location, "Eshed New York");
});

test("intact rows are untouched", () => {
  for (const make of [intact, () => ({ ...intact(), "Cert.Comments": "" }), () => ({ ...intact(), "Cert.Comments": "Minor" })]) {
    const s = make();
    const before = JSON.stringify(s);
    assert.strictEqual(realignCertComments(s), 0);
    assert.strictEqual(JSON.stringify(s), before);
  }
});

test("an opening quote that never closes is left alone", () => {
  const s = { ...intact(), "Cert.Comments": '"""Clouds' };
  assert.strictEqual(commentSpill(s), 0);
});

console.log(`\n${passed} passed${process.exitCode ? ", some failed" : ""}`);
