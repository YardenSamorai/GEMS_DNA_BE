/* Live check of the inventory assistant against the real model.
 *
 * Run with:  node utils/assistantQuery.live.js
 * Needs OPENAI_API_KEY. Costs a fraction of a cent per run.
 *
 * Separate from assistantFilterSchema.test.js on purpose: that one is
 * deterministic and free, this one spends money and can wobble with model
 * behaviour, so it is a tool you reach for rather than a gate. */

require("dotenv").config();
const { runAssistantQuery, runAssistantAdvice, ASSISTANT_MODEL } = require("./assistantQuery");

const navTargets = [
  { path: "/sales/diamonds", label: "Sales catalog — diamonds" },
  { path: "/sales/gemstones", label: "Sales catalog — coloured gemstones" },
  { path: "/sales/jewelry", label: "Sales catalog — jewellery" },
  { path: "/dashboard", label: "Dashboard overview" },
];

/* Stand-in for the rows the browser sends back in phase two. */
const shortlist = [
  { sku: "K1188", category: "Emerald", weightCt: 5.1, pricePerCt: 12400, priceTotal: 63240, lab: "GRS", origin: "Colombia", treatment: "Minor", location: "New York" },
  { sku: "K1190", category: "Emerald", weightCt: 5.4, pricePerCt: 21000, priceTotal: 113400, lab: "GRS", origin: "Colombia", treatment: "No Oil", location: "New York" },
  { sku: "K1204", category: "Emerald", weightCt: 6.2, pricePerCt: 18800, priceTotal: 116560, lab: "SSEF", origin: "Zambia", treatment: "Insignificant", location: "New York" },
];

/* Aggregates for the whole match. Deliberately far larger than anything the
 * three sample rows add up to ($293,200), so a reply quoting the sample total
 * is unmistakable rather than plausible. */
const summary = {
  count: 189,
  totalValue: 24800000,
  totalCarats: 1240.5,
  avgPricePerCt: 19992,
  minPricePerCt: 4100,
  maxPricePerCt: 88000,
  priceMode: "neto",
  byCategory: [
    { key: "Emerald", count: 96, totalValue: 14100000, totalCarats: 700.2 },
    { key: "Sapphire", count: 61, totalValue: 7300000, totalCarats: 380.1 },
    { key: "Ruby", count: 32, totalValue: 3400000, totalCarats: 160.2 },
  ],
  byLocation: [
    { key: "New York", count: 120, totalValue: 16200000, totalCarats: 800.0 },
    { key: "Hong Kong", count: 69, totalValue: 8600000, totalCarats: 440.5 },
  ],
};

/* A realistic slice of the vocabulary the inventory page sends. */
const stoneVocab = {
  shape: ["Round", "Oval", "Cushion", "Emerald", "Pear", "Sugarloaf"],
  category: ["Diamond", "Emerald", "Ruby", "Sapphire", "Spinel", "Tourmaline"],
  treatment: ["No Oil", "Insignificant", "Minor", "Moderate", "Significant"],
  location: ["New York", "Hong Kong", "Israel"],
  lab: ["GIA", "GRS", "SSEF", "GUBELIN", "AGL"],
  groupingType: ["Single", "Pair", "Set", "Parcel"],
  diamondColor: ["D", "E", "F", "G", "H", "I", "J"],
  fancyColor: ["Blue", "Pink", "Green", "Yellow"],
  tag: ["Hot", "Reserved"],
};

const jewelryVocab = {
  category: ["Ring", "Necklace", "Earrings", "Bracelet"],
  shape: ["Solitaire", "Halo", "Three Stone"],
  treatment: ["Heritage", "Classic"],
  diamondColor: ["Diamond", "Emerald", "Ruby"],
  fancyColor: ["White Gold", "Yellow Gold", "Platinum"],
};

/* Each case asserts on the shape of the outcome rather than an exact filter,
 * so a differently-worded but equally correct answer still passes. */
const cases = [
  {
    name: "Hebrew: emeralds over 5 carats",
    input: { message: "תראה לי אמרלדים מעל 5 קראט", inventoryMode: "gemstones", vocabulary: stoneVocab },
    expect: (b) => b.filters.category?.includes("Emerald") && b.filters.minCarat === "5",
  },
  {
    name: "Hebrew: branch and lab",
    input: { message: "כל האבנים בניו יורק עם תעודת GRS", inventoryMode: "gemstones", vocabulary: stoneVocab },
    expect: (b) => b.filters.location?.includes("New York") && b.filters.lab?.includes("GRS"),
  },
  {
    name: "English: shape, lab and a weight range",
    input: { message: "round diamonds, GIA, between 1 and 2 carats", inventoryMode: "diamonds", vocabulary: stoneVocab },
    expect: (b) =>
      b.filters.shape?.includes("Round") &&
      b.filters.lab?.includes("GIA") &&
      b.filters.minCarat === "1" &&
      b.filters.maxCarat === "2",
  },
  {
    name: "English: price per carat is not confused with total price",
    input: { message: "sapphires under 8000 per carat", inventoryMode: "gemstones", vocabulary: stoneVocab },
    expect: (b) =>
      b.filters.category?.includes("Sapphire") &&
      b.filters.maxPricePerCt === "8000" &&
      b.filters.maxPrice === undefined,
  },
  {
    name: "Trade shorthand: 'clarity' means treatment on coloured stones",
    input: { message: "emeralds with no oil", inventoryMode: "gemstones", vocabulary: stoneVocab },
    expect: (b) => b.filters.treatment?.includes("No Oil"),
  },
  {
    name: "Ambiguous request asks a question instead of guessing",
    input: { message: "show me something nice", inventoryMode: "gemstones", vocabulary: stoneVocab },
    expect: (b) => b.needsClarification && Object.keys(b.filters).length === 0 && !!b.reply,
  },
  {
    name: "Absent stock is reported rather than filtered to nothing",
    input: { message: "show me alexandrite", inventoryMode: "gemstones", vocabulary: stoneVocab },
    expect: (b) => b.needsClarification && Object.keys(b.filters).length === 0,
  },
  {
    name: "Jewelry tab: never returns a field that tab cannot filter on",
    input: { message: "platinum rings under 20000 with a GIA certificate", inventoryMode: "jewelry", vocabulary: jewelryVocab },
    expect: (b) =>
      b.filters.category?.includes("Ring") &&
      b.filters.lab === undefined &&
      b.filters.minPricePerCt === undefined,
  },
  {
    name: "Follow-up narrows the previous turn",
    input: {
      message: "ורק אלה שמעל 3 קראט",
      inventoryMode: "gemstones",
      vocabulary: stoneVocab,
      history: [
        { role: "user", content: "תראה לי ספירים בהונג קונג" },
        { role: "assistant", content: "סיננתי ספירים בהונג קונג." },
      ],
    },
    expect: (b) => b.filters.minCarat === "3",
  },
  {
    name: "Switches tab when the request belongs to another one",
    input: { message: "show me emerald gemstones over 4ct", inventoryMode: "diamonds", vocabulary: stoneVocab },
    expect: (b) => b.inventoryMode === "gemstones",
  },
  {
    name: "Superlative becomes a sort, not a guess",
    input: { message: "\u05d4\u05d0\u05d1\u05e0\u05d9\u05dd \u05d4\u05d6\u05d5\u05dc\u05d5\u05ea \u05d1\u05d9\u05d5\u05ea\u05e8 \u05dc\u05e7\u05e8\u05d0\u05d8", inventoryMode: "gemstones", vocabulary: stoneVocab },
    expect: (b) => b.sort?.field === "pricePerCt" && b.sort.direction === "asc",
  },
  {
    name: "Biggest sorts by weight descending",
    input: { message: "show me the biggest rubies", inventoryMode: "gemstones", vocabulary: stoneVocab },
    expect: (b) => b.filters.category?.includes("Ruby") && b.sort?.field === "weightCt" && b.sort.direction === "desc",
  },
  {
    name: "Asking for advice raises the recommendation flag",
    input: { message: "\u05d0\u05d9\u05d6\u05d5 \u05d0\u05d6\u05de\u05e8\u05dc\u05d3 \u05d4\u05db\u05d9 \u05de\u05e9\u05ea\u05dc\u05de\u05ea \u05dc\u05dc\u05e7\u05d5\u05d7?", inventoryMode: "gemstones", vocabulary: stoneVocab },
    expect: (b) => b.wantsAnswer === true && Object.keys(b.filters).length > 0,
  },
  {
    name: "Plain filtering does not raise it",
    input: { message: "sapphires in Hong Kong", inventoryMode: "gemstones", vocabulary: stoneVocab },
    expect: (b) => b.wantsAnswer === false,
  },
  {
    name: "A question about total value also raises the flag",
    input: { message: "כמה שווה כל המלאי בניו יורק?", inventoryMode: "gemstones", vocabulary: stoneVocab },
    expect: (b) => b.wantsAnswer === true && b.filters.location?.includes("New York"),
  },
  {
    name: "A breakdown request raises the flag without inventing a filter",
    input: { message: "מה הפילוח של המלאי לפי קטגוריה?", inventoryMode: "gemstones", vocabulary: stoneVocab },
    expect: (b) => b.wantsAnswer === true,
  },
  {
    name: "Routes to another page when the request belongs there",
    input: { message: "\u05e7\u05d7 \u05d0\u05d5\u05ea\u05d9 \u05dc\u05e7\u05d8\u05dc\u05d5\u05d2 \u05d4\u05de\u05db\u05d9\u05e8\u05d5\u05ea \u05e9\u05dc \u05d4\u05ea\u05db\u05e9\u05d9\u05d8\u05d9\u05dd", inventoryMode: "gemstones", vocabulary: stoneVocab, navTargets },
    expect: (b) => b.navigateTo?.path === "/sales/jewelry",
  },
  {
    name: "Does not navigate away just to show inventory results",
    input: { message: "\u05ea\u05e8\u05d0\u05d4 \u05dc\u05d9 \u05d0\u05de\u05e8\u05dc\u05d3\u05d9\u05dd \u05de\u05e2\u05dc 5 \u05e7\u05e8\u05d0\u05d8", inventoryMode: "gemstones", vocabulary: stoneVocab, navTargets },
    expect: (b) => b.navigateTo === null && b.filters.category?.includes("Emerald"),
  },
];

/* Phase two runs against the rows the browser sends back. */
const adviceCases = [
  {
    name: "Names a specific SKU from the rows it was given",
    input: { message: "\u05d0\u05d9\u05d6\u05d5 \u05d4\u05db\u05d9 \u05de\u05e9\u05ea\u05dc\u05de\u05ea?", shortlist, totalCount: 3 },
    expect: (b) => b.skus.length > 0 && shortlist.some((s) => b.reply.includes(s.sku)),
  },
  {
    name: "Answers in Hebrew when asked in Hebrew",
    input: { message: "\u05de\u05d4 \u05d4\u05db\u05d9 \u05db\u05d1\u05d3\u05d4 \u05db\u05d0\u05df?", shortlist, totalCount: 3 },
    expect: (b) => /[\u0590-\u05FF]/.test(b.reply) && b.reply.includes("K1204"),
  },
  {
    name: "Says so plainly when there is nothing to recommend",
    input: { message: "which is best?", shortlist: [], totalCount: 0 },
    expect: (b) => b.skus.length === 0 && !!b.reply,
  },
  {
    name: "Totals the whole match, not the three rows it can see",
    input: { message: "מה השווי הכולל של המלאי הזה?", shortlist, summary, totalCount: 189 },
    // 24.8M in any spoken form; must not be the $293,200 sample total.
    expect: (b) => /24[.,]8|24,800,000|24800000/.test(b.reply) && !/293/.test(b.reply),
  },
  {
    name: "Reads the average per carat off the summary",
    input: { message: "what is the average price per carat here?", shortlist, summary, totalCount: 189 },
    expect: (b) => /19,?9\d\d|20k|~?\$?20,000/i.test(b.reply),
  },
  {
    name: "Breaks down by category from the summary groups",
    input: { message: "מה הפילוח לפי קטגוריה?", shortlist, summary, totalCount: 189 },
    expect: (b) => /96/.test(b.reply) && /61/.test(b.reply) && /Emerald|אמרלד|ברקת/i.test(b.reply),
  },
  {
    name: "Still names individual stones from the sample when asked to pick",
    input: { message: "איזו אבן הכי משתלמת מהאלה?", shortlist, summary, totalCount: 189 },
    expect: (b) => b.skus.length > 0 && shortlist.some((s) => b.reply.includes(s.sku)),
  },
];

const runSuite = async (label, list, fn) => {
  console.log(`\n${label}`);
  let passed = 0;

  for (const c of list) {
    let result;
    try {
      result = await fn(c.input);
    } catch (e) {
      console.error(`  FAIL  ${c.name}\n        threw: ${e.message}`);
      process.exitCode = 1;
      continue;
    }

    if (!result.ok) {
      console.error(`  FAIL  ${c.name}\n        ${result.status}: ${result.error}`);
      process.exitCode = 1;
      continue;
    }

    if (c.expect(result.body)) {
      passed++;
      console.log(`  ok    ${c.name}`);
    } else {
      console.error(`  FAIL  ${c.name}`);
      console.error(`        ${JSON.stringify(result.body)}`);
      process.exitCode = 1;
    }
  }

  return passed;
};

(async () => {
  console.log(`\nmodel: ${ASSISTANT_MODEL}`);
  const a = await runSuite("phase 1 — filter, sort, navigate", cases, runAssistantQuery);
  const b = await runSuite("phase 2 — advice on the matched rows", adviceCases, runAssistantAdvice);
  console.log(`\n${a + b}/${cases.length + adviceCases.length} passed\n`);
})();
