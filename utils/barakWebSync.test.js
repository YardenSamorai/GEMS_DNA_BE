/* Run with:  node utils/barakWebSync.test.js */

const assert = require("assert");
const { latestWebFile, skusFromCsv, guardReason } = require("./barakWebSync");
const { syncManaged } = require("./webListings");

let passed = 0;
const test = async (name, fn) => {
  try {
    await fn();
    passed++;
    console.log(`  ok  ${name}`);
  } catch (e) {
    console.error(`  FAIL  ${name}`);
    console.error(`        ${e.message}`);
    process.exitCode = 1;
  }
};

/* Just enough of pg for syncManaged: one table, the statements it sends. */
const fakePool = (initial) => {
  const table = new Map(initial.map(([sku, source]) => [sku, source]));
  let tx = null;
  const query = async (sql, params = []) => {
    const s = sql.replace(/\s+/g, " ").trim();
    if (/^BEGIN/.test(s)) tx = new Map(table);
    else if (/^ROLLBACK/.test(s)) tx = null;
    else if (/^COMMIT/.test(s)) {
      table.clear();
      tx.forEach((v, k) => table.set(k, v));
      tx = null;
    } else if (/^SELECT sku, source FROM web_listings/.test(s)) {
      return { rows: [...tx.entries()].map(([sku, source]) => ({ sku, source })) };
    } else if (/^DELETE FROM web_listings/.test(s)) {
      params[0].forEach((k) => tx.delete(k));
    } else if (/^INSERT INTO web_listings/.test(s)) {
      params[0].forEach((k) => tx.set(k, "barak"));
    }
    return { rows: [] };
  };
  return { table, query, connect: async () => ({ query, release() {} }) };
};

(async () => {
  await test("picks the newest Diamonds file and ignores everything else", () => {
    assert.strictEqual(
      latestWebFile(["Jewelry_Web.csv", "Diamonds20261022_110009.csv", "Diamonds20261021_230006.csv", ".ftpquota"]),
      "Diamonds20261022_110009.csv"
    );
    assert.strictEqual(latestWebFile(["Jewelry_Web.csv"]), null);
  });

  await test("reads unique SKUs, BOM and quoted line breaks included", () => {
    const csv =
      "\uFEFFCategory,SKU,Comment\r\n" +
      'Fancy,P7961,"""clouds, not shown"""\r\n' +
      'Emerald,E1,"a\r\nb"\r\n' +
      "Emerald,E1,\r\n";
    assert.deepStrictEqual(skusFromCsv(csv), ["P7961", "E1"]);
  });

  await test("blocks a tiny file or a mass removal, allows normal churn", () => {
    assert.ok(guardReason(40, 900, 0));
    assert.ok(guardReason(930, 930, 400));
    assert.strictEqual(guardReason(924, 930, 6), null);
    assert.strictEqual(guardReason(500, 10, 10), null);
    assert.strictEqual(guardReason(500, 930, 430, { allowDrop: true }), null);
    assert.ok(guardReason(40, 930, 0, { allowDrop: true }));
  });

  await test("sync mirrors Barak rows, adopts manual ones it lists, keeps other manual ones", async () => {
    const pool = fakePool([["A", "barak"], ["B", "barak"], ["M1", "manual"], ["M2", "manual"]]);
    const r = await syncManaged(pool, ["A", "C", "M1"]);
    assert.deepStrictEqual(r.added, ["C"]);
    assert.deepStrictEqual(r.removed, ["B"]);
    assert.deepStrictEqual(
      Object.fromEntries(pool.table),
      { A: "barak", C: "barak", M1: "barak", M2: "manual" }
    );
  });

  await test("a blocked sync writes nothing", async () => {
    const pool = fakePool([["A", "barak"], ["B", "barak"]]);
    const r = await syncManaged(pool, ["C"], () => "too many");
    assert.strictEqual(r.blocked, "too many");
    assert.deepStrictEqual(Object.fromEntries(pool.table), { A: "barak", B: "barak" });
  });

  console.log(`\n${passed} passed`);
})();
