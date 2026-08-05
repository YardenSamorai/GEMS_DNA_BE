/* Run with:  node utils/assistantFilterSchema.test.js
 *
 * Dependency-free, same style as priceIntegrity.test.js. Exits non-zero on
 * the first failure. */

const assert = require("assert");
const {
  buildFilterTool,
  buildNavigateTool,
  validateFilters,
  validateSort,
  sanitizeVocabulary,
  sanitizeNavTargets,
  sanitizeShortlist,
} = require("./assistantFilterSchema");

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

const vocab = {
  category: ["Diamond", "Emerald", "Sapphire"],
  shape: ["Round", "Oval", "Emerald"],
  location: ["New York", "Hong Kong"],
  lab: ["GIA", "GRS"],
  treatment: ["No Oil", "Minor"],
};

console.log("\nvalidateFilters");

test("keeps a plain range and returns it as a string for the inputs", () => {
  const { filters } = validateFilters({ minCarat: 5 }, vocab);
  assert.strictEqual(filters.minCarat, "5");
});

test("snaps multi-select values back to the inventory's own casing", () => {
  const { filters } = validateFilters({ category: ["emerald"], lab: ["gia"] }, vocab);
  assert.deepStrictEqual(filters.category, ["Emerald"]);
  assert.deepStrictEqual(filters.lab, ["GIA"]);
});

test("drops a value that does not exist in the inventory", () => {
  const { filters, dropped } = validateFilters({ category: ["Emerald", "Unobtainium"] }, vocab);
  assert.deepStrictEqual(filters.category, ["Emerald"]);
  assert.ok(dropped.some((d) => d.includes("Unobtainium")));
});

test("drops a field the model invented outright", () => {
  const { filters, dropped } = validateFilters({ somethingMadeUp: "x" }, vocab);
  assert.strictEqual(filters.somethingMadeUp, undefined);
  assert.ok(dropped.includes("somethingMadeUp"));
});

// A NaN threshold reads as "no filter" in one place and "match nothing" in
// another, so it must never survive validation.
test("rejects non-numeric and negative ranges", () => {
  const { filters } = validateFilters({ minPrice: "abc", maxPrice: -10 }, vocab);
  assert.strictEqual(filters.minPrice, undefined);
  assert.strictEqual(filters.maxPrice, undefined);
});

test("swaps an inverted range instead of returning nothing", () => {
  const { filters } = validateFilters({ minCarat: 10, maxCarat: 2 }, vocab);
  assert.strictEqual(filters.minCarat, "2");
  assert.strictEqual(filters.maxCarat, "10");
});

test("returns only the keys the model set, so callers can merge", () => {
  const { filters } = validateFilters({ minCarat: 3 }, vocab);
  assert.deepStrictEqual(Object.keys(filters), ["minCarat"]);
});

test("accepts a bare string where an array was expected", () => {
  const { filters } = validateFilters({ category: "Diamond" }, vocab);
  assert.deepStrictEqual(filters.category, ["Diamond"]);
});

test("trims free text and ignores an empty string", () => {
  const { filters } = validateFilters({ sku: "  T9099 ", box: "   " }, vocab);
  assert.strictEqual(filters.sku, "T9099");
  assert.strictEqual(filters.box, undefined);
});

console.log("\nbuildFilterTool");

test("offers only values present in the caller's inventory", () => {
  const tool = buildFilterTool(vocab, "gemstones");
  assert.deepStrictEqual(
    tool.function.parameters.properties.category.items.enum,
    ["Diamond", "Emerald", "Sapphire"]
  );
});

// Masked tiers null out branch, so location arrives empty and must not be
// offered as a filter the model can guess at.
test("omits a field the viewer has no values for", () => {
  const tool = buildFilterTool({ category: ["Diamond"] }, "diamonds");
  assert.strictEqual(tool.function.parameters.properties.location, undefined);
});

// The jewelry filter block ignores lab, branch and price-per-carat entirely.
// Offering them would produce a filter the UI claims to have applied.
test("hides fields the jewelry tab cannot filter on", () => {
  const props = buildFilterTool(vocab, "jewelry").function.parameters.properties;
  assert.strictEqual(props.lab, undefined);
  assert.strictEqual(props.location, undefined);
  assert.strictEqual(props.minPricePerCt, undefined);
  assert.strictEqual(props.box, undefined);
  assert.ok(props.category);
  assert.ok(props.minCarat);
});

test("drops jewelry-unsupported fields even if the model returns them", () => {
  const { filters, dropped } = validateFilters(
    { lab: ["GIA"], minPricePerCt: 500, minCarat: 1 },
    vocab,
    "jewelry"
  );
  assert.strictEqual(filters.lab, undefined);
  assert.strictEqual(filters.minPricePerCt, undefined);
  assert.strictEqual(filters.minCarat, "1");
  assert.ok(dropped.includes("lab") && dropped.includes("minPricePerCt"));
});

test("relabels the reused keys when the jewelry tab is active", () => {
  const stones = buildFilterTool(vocab, "gemstones");
  const jewelry = buildFilterTool(vocab, "jewelry");
  assert.notStrictEqual(
    stones.function.parameters.properties.shape.description,
    jewelry.function.parameters.properties.shape.description
  );
});

console.log("\nsanitizeVocabulary");

test("de-duplicates, trims and ignores unknown keys", () => {
  const clean = sanitizeVocabulary({ category: [" Diamond ", "Diamond", ""], nope: ["x"] });
  assert.deepStrictEqual(clean.category, ["Diamond"]);
  assert.strictEqual(clean.nope, undefined);
});

test("caps a runaway list", () => {
  const many = Array.from({ length: 500 }, (_, i) => `tag${i}`);
  const clean = sanitizeVocabulary({ tag: many }, 120);
  assert.strictEqual(clean.tag.length, 120);
});

console.log("\nvalidateSort");

test("accepts a known field and defaults a bad direction to ascending", () => {
  assert.deepStrictEqual(validateSort({ field: "pricePerCt", direction: "sideways" }, "gemstones"), {
    field: "pricePerCt",
    direction: "asc",
  });
});

test("rejects a field that is not sortable, and any junk", () => {
  assert.strictEqual(validateSort({ field: "costPerCt", direction: "asc" }, "gemstones"), null);
  assert.strictEqual(validateSort({ field: "pricePerCt" }, "jewelry"), null);
  assert.strictEqual(validateSort("nonsense", "gemstones"), null);
});

console.log("\nnavigation");

// An absolute URL here would turn the assistant into an open redirect.
test("keeps in-app paths and rejects anything that could redirect off-site", () => {
  const clean = sanitizeNavTargets([
    { path: "/sales/diamonds", label: "Diamonds" },
    { path: "https://evil.example.com", label: "Nope" },
    { path: "//evil.example.com", label: "Nope" },
    { path: "/sales/diamonds", label: "Duplicate" },
    { path: "/no-label", label: "" },
  ]);
  assert.deepStrictEqual(clean, [{ path: "/sales/diamonds", label: "Diamonds" }]);
});

test("offers no navigation tool when the user may open nothing", () => {
  assert.strictEqual(buildNavigateTool([]), null);
});

test("limits the destination enum to the paths supplied", () => {
  const tool = buildNavigateTool([{ path: "/inventory", label: "Inventory" }]);
  assert.deepStrictEqual(tool.function.parameters.properties.path.enum, ["/inventory"]);
});

console.log("\nsanitizeShortlist");

// Our buying price is not needed to compare stones and must never be sent.
test("strips cost and any field outside the allow-list", () => {
  const [row] = sanitizeShortlist([
    { sku: "T1", weightCt: 2, pricePerCt: 100, costPerCt: 40, holder: "Dan", rawXml: "<x/>" },
  ]);
  assert.strictEqual(row.costPerCt, undefined);
  assert.strictEqual(row.holder, undefined);
  assert.strictEqual(row.rawXml, undefined);
  assert.strictEqual(row.pricePerCt, 100);
});

test("caps the payload and drops rows with no SKU", () => {
  const many = Array.from({ length: 50 }, (_, i) => ({ sku: `T${i}`, weightCt: 1 }));
  assert.strictEqual(sanitizeShortlist(many).length, 20);
  assert.strictEqual(sanitizeShortlist([{ weightCt: 3 }]).length, 0);
  assert.strictEqual(sanitizeShortlist("nonsense").length, 0);
});

console.log(`\n${passed} passed\n`);
