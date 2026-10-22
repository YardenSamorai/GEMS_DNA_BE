/* Run with:  node utils/etradeExport.test.js */

const assert = require("assert");
const { HEADERS, rowToCells, toCsv, fileName, branchCode, category, lab, price } = require("./etradeExport");

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

const cell = (row, header) => rowToCells(row)[HEADERS.indexOf(header)];

test("has Barak's 40 web columns, trailing space included", () => {
  assert.strictEqual(HEADERS.length, 40);
  assert.strictEqual(HEADERS[9], "Rap Price % ");
  assert.strictEqual(HEADERS[39], "TradeShow");
});

test("branch display names go back to the web codes", () => {
  assert.strictEqual(branchCode("Israel"), "IL");
  assert.strictEqual(branchCode("New York"), "NY");
  assert.strictEqual(branchCode("Hong Kong"), "HK");
  assert.strictEqual(branchCode("Los Angeles"), "LA");
  assert.strictEqual(branchCode(null), "");
});

test("category drops Barak's internal ' O' suffix only", () => {
  assert.strictEqual(category("Pink Tourmaline O"), "Pink Tourmaline");
  assert.strictEqual(category("Emerald"), "Emerald");
  assert.strictEqual(category("Spinel Memo"), "Spinel Memo");
});

test("lab ESH is spelled out", () => {
  assert.strictEqual(lab("ESH"), "Eshed");
  assert.strictEqual(lab("GIA"), "GIA");
});

test("prices keep every digit and drop padding", () => {
  assert.strictEqual(price("7500.0000"), "7500");
  assert.strictEqual(price("4423.500"), "4423.5");
  assert.strictEqual(price("20751.500998003992015968063872"), "20751.500998003992015968063872");
  assert.strictEqual(price(9075), "9075");
  assert.strictEqual(price(null), "");
});

test("location, trade show and internal fields never leave", () => {
  const row = {
    sku: "T-1", location: "Emerald office", trade_show: "JCK 2022",
    cost_per_carat: 10, holder: "Liran", box: "B1",
  };
  assert.strictEqual(cell(row, "Location"), "");
  assert.strictEqual(cell(row, "TradeShow"), "");
  assert.ok(!toCsv([row]).includes("Liran"));
  assert.ok(!toCsv([row]).includes("Emerald office"));
});

test("quotes inside a value are doubled like Barak's file", () => {
  const csv = toCsv([{ sku: "P1", cert_comments: '"Additional clouds are not shown."' }]);
  assert.ok(csv.includes('"""Additional clouds are not shown."""'));
});

test("CRLF lines with a trailing newline", () => {
  const csv = toCsv([{ sku: "A" }, { sku: "B" }]);
  assert.strictEqual(csv.split("\r\n").length, 4);
  assert.ok(csv.endsWith("\r\n"));
});

test("file name follows Barak's DiamondsYYYYMMDD_HHMMSS pattern in Israel time", () => {
  assert.strictEqual(fileName(new Date("2026-10-22T08:45:55Z")), "Diamonds20261022_114555.csv");
});

console.log(`\n${passed} passed`);
