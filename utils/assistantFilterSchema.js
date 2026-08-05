/* Filter contract for the inventory AI assistant.
 *
 * The assistant never receives stone rows. It receives a question plus the
 * vocabulary of values that actually exist in the caller's current inventory,
 * and answers with a filter object that the browser applies locally. That
 * keeps prices, costs and locations out of the model entirely, and means the
 * per-viewer masking already done in /api/soap-stones needs no second
 * implementation here.
 *
 * The shape below mirrors `defaultFilters` in the frontend inventory page.
 * Anything the model invents outside this contract is dropped, so a
 * hallucinated field can never reach the UI.
 */

const INVENTORY_MODES = ["diamonds", "gemstones", "jewelry"];

/* Numeric ranges. Kept as strings on the way out because the inventory page
 * binds them straight to controlled inputs. */
const RANGE_FIELDS = [
  "minPrice", "maxPrice",
  "minPricePerCt", "maxPricePerCt",
  "minCarat", "maxCarat",
  "minLength", "maxLength",
  "minWidth", "maxWidth",
];

/* Multi-selects. The value list is supplied per request from the live
 * inventory, so the model can only choose things that exist right now.
 * `stoneLabel` / `jewelryLabel` differ because the jewelry tab reuses the
 * same filter keys for completely different columns. */
const MULTI_FIELDS = {
  shape: { stoneLabel: "shape", jewelryLabel: "jewellery style" },
  treatment: { stoneLabel: "treatment, shown in the UI as Clarity for coloured stones", jewelryLabel: "collection" },
  category: { stoneLabel: "stone category", jewelryLabel: "jewellery type" },
  tag: { stoneLabel: "user tag", jewelryLabel: "user tag" },
  location: { stoneLabel: "branch the stone sits in", jewelryLabel: "branch" },
  groupingType: { stoneLabel: "grouping (Single, Pair, Set, Parcel...)", jewelryLabel: "grouping" },
  diamondColor: { stoneLabel: "diamond colour grade group", jewelryLabel: "centre stone type" },
  fancyColor: { stoneLabel: "fancy colour", jewelryLabel: "metal type" },
  lab: { stoneLabel: "grading lab", jewelryLabel: "grading lab" },
};

const TEXT_FIELDS = {
  sku: { max: 2000, hint: "exact SKU, or several separated by commas" },
  box: { max: 100, hint: "physical box label, matched as a substring" },
};

const MULTI_KEYS = Object.keys(MULTI_FIELDS);
const TEXT_KEYS = Object.keys(TEXT_FIELDS);

/* The jewelry tab runs a much shorter filter block than the stone tabs — it
 * has no lab, branch, grouping, tag, box, price-per-carat or millimetre
 * filtering. Offering those anyway would let the model return a filter that
 * is silently ignored while the UI claims it was applied. */
const JEWELRY_SUPPORTED = {
  ranges: ["minPrice", "maxPrice", "minCarat", "maxCarat"],
  multi: ["category", "shape", "treatment", "diamondColor", "fancyColor"],
  text: ["sku"],
};

const supportedFields = (inventoryMode) =>
  inventoryMode === "jewelry"
    ? JEWELRY_SUPPORTED
    : { ranges: RANGE_FIELDS, multi: MULTI_KEYS, text: TEXT_KEYS };

/* Sorting is how "the cheapest" or "the biggest" gets answered without the
 * model ever ranking rows itself. These are keys on the stone object the
 * inventory page sorts by directly. */
const SORT_FIELDS = {
  stones: ["pricePerCt", "priceTotal", "weightCt", "sku", "color", "clarity", "lab", "shape", "ratio"],
  jewelry: ["priceTotal", "weightCt", "sku"],
};

const sortFieldsFor = (inventoryMode) =>
  inventoryMode === "jewelry" ? SORT_FIELDS.jewelry : SORT_FIELDS.stones;

/** @returns {{field: string, direction: 'asc'|'desc'}|null} */
const validateSort = (raw, inventoryMode = "diamonds") => {
  if (!raw || typeof raw !== "object") return null;
  const field = String(raw.field ?? "").trim();
  if (!sortFieldsFor(inventoryMode).includes(field)) return null;
  return { field, direction: raw.direction === "desc" ? "desc" : "asc" };
};

const rangeDescription = (field) => {
  if (field.includes("PricePerCt")) return "price per carat in USD";
  if (field.includes("Price")) return "total price in USD";
  if (field.includes("Carat")) return "weight in carats";
  if (field.includes("Length")) return "length in millimetres";
  return "width in millimetres";
};

/**
 * Build the OpenAI tool definition, with the multi-select values baked in as
 * enums so the model picks real values instead of paraphrasing them.
 * @param {object} vocabulary - { [field]: string[] } taken from live inventory
 * @param {string} inventoryMode - diamonds | gemstones | jewelry
 */
const buildFilterTool = (vocabulary = {}, inventoryMode = "diamonds") => {
  const isJewelry = inventoryMode === "jewelry";
  const supported = supportedFields(inventoryMode);
  const properties = {};

  for (const field of supported.ranges) {
    const bound = field.startsWith("min") ? "Minimum" : "Maximum";
    properties[field] = {
      type: "number",
      description: `${bound} ${rangeDescription(field)}.`,
    };
  }

  for (const field of supported.multi) {
    const labels = MULTI_FIELDS[field];
    const values = Array.isArray(vocabulary[field]) ? vocabulary[field] : [];
    // A field with no values in the current inventory is omitted rather than
    // offered as an empty enum — that also silently drops columns the viewer
    // is not entitled to see, such as branch on a masked location tier.
    if (values.length === 0) continue;
    properties[field] = {
      type: "array",
      items: { type: "string", enum: values },
      description: `Filter by ${isJewelry ? labels.jewelryLabel : labels.stoneLabel}. Multiple values are OR-ed.`,
    };
  }

  for (const field of supported.text) {
    properties[field] = { type: "string", description: `Filter by ${TEXT_FIELDS[field].hint}.` };
  }

  // Not a filter — the three tabs hold different item sets, so asking for
  // rings while the diamonds tab is open has to switch tab or match nothing.
  properties.inventoryMode = {
    type: "string",
    enum: INVENTORY_MODES,
    description:
      "Switch tab, but only when the request clearly belongs to another one: diamonds for white and fancy diamonds, gemstones for coloured stones, jewelry for finished pieces. Omit to stay on the current tab.",
  };

  // Superlatives are answered by ordering the list, never by the model
  // ranking rows in its head.
  properties.sort = {
    type: "object",
    properties: {
      field: { type: "string", enum: sortFieldsFor(inventoryMode) },
      direction: { type: "string", enum: ["asc", "desc"] },
    },
    required: ["field", "direction"],
    description:
      "Order the results. Use for requests like cheapest, most expensive, biggest or heaviest.",
  };

  properties.wantsRecommendation = {
    type: "boolean",
    description:
      "Set true only when the user wants you to choose, compare or advise on specific stones, rather than just narrow the list. When true you will be shown the matching stones and asked to answer about them.",
  };

  return {
    type: "function",
    function: {
      name: "build_inventory_filter",
      description:
        "Filter, sort and show the user's inventory list. Only include fields the user actually asked for; omit everything else.",
      parameters: { type: "object", properties, additionalProperties: false },
    },
  };
};

/**
 * Tool for sending the user to another page. Targets are supplied per request
 * from the frontend, which knows which sections this user may open — so the
 * model can never route someone into a section they are not permitted to see.
 * @param {Array<{path: string, label: string}>} targets
 */
const buildNavigateTool = (targets = []) => {
  if (!Array.isArray(targets) || targets.length === 0) return null;
  return {
    type: "function",
    function: {
      name: "navigate_to_page",
      description:
        "Send the user to a different page. Use only when the request belongs somewhere else in the app; do not use it to show inventory results, which appear in place.",
      parameters: {
        type: "object",
        properties: {
          path: {
            type: "string",
            enum: targets.map((t) => t.path),
            description: targets.map((t) => `${t.path} = ${t.label}`).join("; "),
          },
        },
        required: ["path"],
        additionalProperties: false,
      },
    },
  };
};

const sanitizeNavTargets = (raw, limit = 20) => {
  if (!Array.isArray(raw)) return [];
  const out = [];
  for (const t of raw) {
    const path = String(t?.path ?? "").trim();
    const label = String(t?.label ?? "").trim();
    // Relative in-app paths only — an absolute URL here would turn the
    // assistant into an open redirect.
    if (!/^\/[\w\-/]*$/.test(path) || !label) continue;
    if (out.some((o) => o.path === path)) continue;
    out.push({ path, label: label.slice(0, 60) });
    if (out.length >= limit) break;
  }
  return out;
};

/**
 * Coerce and whitelist whatever the model produced.
 * @returns {{ filters: object, applied: string[], dropped: string[] }}
 *   `filters` holds only the keys the model set — the caller merges it over
 *   the existing filter state rather than replacing it wholesale.
 */
const validateFilters = (raw, vocabulary = {}, inventoryMode = "diamonds") => {
  const filters = {};
  const applied = [];
  const dropped = [];
  const supported = supportedFields(inventoryMode);

  if (!raw || typeof raw !== "object") return { filters, applied, dropped };

  for (const [key, value] of Object.entries(raw)) {
    if (value === undefined || value === null) continue;

    if (supported.ranges.includes(key)) {
      const num = Number(value);
      // Negative prices and weights are meaningless here, and NaN would
      // silently filter everything out.
      if (!Number.isFinite(num) || num < 0) {
        dropped.push(key);
        continue;
      }
      filters[key] = String(num);
      applied.push(key);
      continue;
    }

    if (supported.multi.includes(key)) {
      const allowed = Array.isArray(vocabulary[key]) ? vocabulary[key] : [];
      const asArray = Array.isArray(value) ? value : [value];
      const matched = [];
      for (const entry of asArray) {
        const needle = String(entry ?? "").trim().toLowerCase();
        if (!needle) continue;
        // Snap back to the inventory's own casing so the equality checks in
        // the filtering memo line up.
        const hit = allowed.find((v) => String(v).trim().toLowerCase() === needle);
        if (hit != null && !matched.includes(hit)) matched.push(hit);
        else if (hit == null) dropped.push(`${key}:${entry}`);
      }
      if (matched.length > 0) {
        filters[key] = matched;
        applied.push(key);
      }
      continue;
    }

    if (supported.text.includes(key)) {
      const str = String(value).trim().slice(0, TEXT_FIELDS[key].max);
      if (str) {
        filters[key] = str;
        applied.push(key);
      }
      continue;
    }

    dropped.push(key);
  }

  // A range whose min exceeds its max returns nothing and looks like a bug to
  // the user, so swap them instead.
  const pairs = [
    ["minPrice", "maxPrice"], ["minPricePerCt", "maxPricePerCt"],
    ["minCarat", "maxCarat"], ["minLength", "maxLength"], ["minWidth", "maxWidth"],
  ];
  for (const [minKey, maxKey] of pairs) {
    if (filters[minKey] && filters[maxKey] && Number(filters[minKey]) > Number(filters[maxKey])) {
      const tmp = filters[minKey];
      filters[minKey] = filters[maxKey];
      filters[maxKey] = tmp;
    }
  }

  return { filters, applied, dropped };
};

/** Keep only the vocabulary keys we know about, and cap list sizes so a huge
 *  tag list can't blow up the prompt. */
const sanitizeVocabulary = (raw, limit = 120) => {
  const out = {};
  if (!raw || typeof raw !== "object") return out;
  for (const key of MULTI_KEYS) {
    const values = raw[key];
    if (!Array.isArray(values)) continue;
    const clean = [];
    for (const v of values) {
      const str = String(v ?? "").trim();
      if (str && !clean.includes(str)) clean.push(str);
      if (clean.length >= limit) break;
    }
    if (clean.length > 0) out[key] = clean;
  }
  return out;
};

/* The only stone fields ever allowed to reach the model, and only once the
 * user has asked for a recommendation. The rows come from the browser, which
 * already holds them masked per viewer, so nothing here can be more revealing
 * than what is on screen. costPerCt is absent deliberately: our buying price
 * is not needed to compare stones and must not leave the building. */
const SHORTLIST_FIELDS = [
  "sku", "category", "shape", "weightCt", "color", "clarity", "treatment",
  "lab", "origin", "fluorescence", "measurements", "ratio",
  "pricePerCt", "priceTotal", "location", "certificateNumber",
  "jewelryType", "style", "collection", "stoneType", "metalType", "title",
];
const SHORTLIST_MAX = 20;

const sanitizeShortlist = (raw) => {
  if (!Array.isArray(raw)) return [];
  const out = [];
  for (const row of raw.slice(0, SHORTLIST_MAX)) {
    if (!row || typeof row !== "object") continue;
    const clean = {};
    for (const field of SHORTLIST_FIELDS) {
      const v = row[field];
      if (v === undefined || v === null || v === "") continue;
      clean[field] = typeof v === "number" ? v : String(v).slice(0, 120);
    }
    if (clean.sku) out.push(clean);
  }
  return out;
};

module.exports = {
  INVENTORY_MODES,
  SHORTLIST_MAX,
  buildFilterTool,
  buildNavigateTool,
  validateFilters,
  validateSort,
  sanitizeVocabulary,
  sanitizeNavTargets,
  sanitizeShortlist,
};
