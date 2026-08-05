/* Live check of the inventory assistant against the real model.
 *
 * Run with:  node utils/assistantQuery.live.js
 * Needs OPENAI_API_KEY. Costs a fraction of a cent per run.
 *
 * Separate from assistantFilterSchema.test.js on purpose: that one is
 * deterministic and free, this one spends money and can wobble with model
 * behaviour, so it is a tool you reach for rather than a gate. */

require("dotenv").config();
const { runAssistantQuery, ASSISTANT_MODEL } = require("./assistantQuery");

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
];

(async () => {
  console.log(`\nmodel: ${ASSISTANT_MODEL}\n`);
  let passed = 0;

  for (const c of cases) {
    let result;
    try {
      result = await runAssistantQuery(c.input);
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

  console.log(`\n${passed}/${cases.length} passed\n`);
})();
