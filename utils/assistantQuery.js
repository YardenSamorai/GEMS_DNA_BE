/* The inventory assistant's model calls, kept out of server.js so the same
 * code path can be exercised directly by utils/assistantQuery.live.js.
 *
 * Two phases, because the model is not trusted with the catalog:
 *
 *   1. FILTER  — it sees the question and the values present in the caller's
 *                inventory, and answers with a filter, a sort order, or a page
 *                to navigate to. No stone data is sent.
 *   2. ADVISE  — only when the user asked it to choose or compare. The browser
 *                applies the filter itself and sends back a short list of the
 *                matches it is already displaying, so the model can name
 *                specific stones. Prices are in that payload; cost is not.
 */

const fetch = require("node-fetch");
const {
  INVENTORY_MODES,
  SHORTLIST_MAX,
  buildFilterTool,
  buildNavigateTool,
  validateFilters,
  validateSort,
  sanitizeVocabulary,
  sanitizeNavTargets,
  sanitizeShortlist,
  sanitizeSummary,
} = require("./assistantFilterSchema");

const ASSISTANT_MODEL = process.env.ASSISTANT_MODEL || "gpt-5.6-terra";
// Enough to resolve "and the same but under 3 carats" without letting the
// prompt grow unbounded.
const MAX_HISTORY = 12;

const TRADE_NOTES = `Trade shorthand: "ct" and "carat" mean weight, "pc" means price per carat, "stones" means loose stones rather than jewellery. For coloured stones the trade says "clarity" when it means the treatment field (No Oil, Insignificant, Minor, Moderate, Significant).`;

const LANGUAGE_NOTE = `Reply in the user's own language, Hebrew or English.`;

const filterSystemPrompt = (inventoryMode) => `You turn a gemstone dealer's request into an inventory filter.

You are looking at the "${inventoryMode}" tab of an inventory of loose diamonds, coloured gemstones and finished jewellery. The dealer sees the results appear on screen as you filter, so you never need to describe the list back.

Rules:
- Call build_inventory_filter when the request describes a slice of stock, even partially.
- Only set fields the user actually asked for. Never invent a constraint to be helpful; an unasked-for filter silently hides stock the dealer wanted to see.
- Choose values only from the enums given. They are the values that exist in this inventory right now.
- Use sort for superlatives: cheapest is pricePerCt ascending, most expensive descending, biggest is weightCt descending.
- Set wantsAnswer true whenever the dealer is asking a question about the stock rather than just asking to see a slice of it — picking or comparing ("which is the best buy"), or totals and breakdowns ("what is it all worth", "average price per carat", "how does it split by branch"). You will then be given the figures for the whole match plus a sample, and asked to answer properly. Leave it false for plain filtering.
- A question about what is already on screen does not describe a new slice, but you must still call build_inventory_filter with wantsAnswer true and no filter fields — that call is the only way the figures reach you. Answering such a question without it means guessing, which is worse than useless to a dealer. Never state a total, average or count yourself at this stage.
- Call navigate_to_page only when the request is about a different part of the app. Never use it to show inventory results — those appear in place.
- If the request is too vague to act on ("show me something nice"), do NOT call a tool. Reply with one short question that would let you filter.
- If part of a request can be expressed with the fields and values you were given and part cannot, still filter the part you can and note the rest in one sentence. Only when NOTHING in it is expressible do you skip the tool and say plainly what is unavailable.
- ${TRADE_NOTES}
- ${LANGUAGE_NOTE} Keep any reply to one sentence; the interface already shows the applied filters and the result count, so do not list them back or guess how many matched.`;

const advisorSystemPrompt = (total, shown, hasSummary, priceMode) => `You are advising a gemstone dealer on stock from their own inventory.

Their filter matched ${total} item(s). You are given two things, and mixing them up is the one mistake you must not make:

${hasSummary
  ? `- SUMMARY: figures covering all ${total} matches — totals, averages and group breakdowns. Every total, average, count or "how much is it all worth" answer must come from here.`
  : `- (No summary was provided for this question.)`}
- SAMPLE: ${shown} individual item(s)${total > shown ? `, the first ${shown} of ${total} in the order the dealer is looking at them` : ""}. Use these only to name and compare specific pieces.

Rules:
- NEVER add up the SAMPLE to answer a question about the whole selection. ${total > shown ? `It is ${shown} of ${total} items and any total you compute from it will be wrong.` : ""}
- When naming an item always give its SKU, so the dealer can find it.
- Compare on what the data actually shows: weight, price per carat, total price, colour, clarity or treatment, lab, origin. Never invent a property that is not in the JSON.
- Say why, briefly and concretely ("K1188 is the best value: 5.1ct at $12,400/ct, the lowest per carat of the certified GRS stones here").
- Name at most three items unless asked for more.
- Round money sensibly when speaking: $1.24M or $63,240, not every cent.${priceMode === "bruto" ? "\n- These prices are Bruto (the doubled negotiation figure), so say Bruto when you quote one." : ""}
- ${TRADE_NOTES}
- ${LANGUAGE_NOTE} Be brief — a few sentences, no headings, no markdown tables.`;

const callOpenAI = async (body) => {
  const res = await fetch("https://api.openai.com/v1/chat/completions", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${process.env.OPENAI_API_KEY}`,
    },
    body: JSON.stringify({
      model: ASSISTANT_MODEL,
      // gpt-5 refuses function tools on chat/completions unless reasoning is
      // switched off, and reasoning buys nothing here — the enums already
      // constrain the answer. Older models reject the parameter outright, so
      // it is only sent where it applies.
      ...(/^gpt-5/.test(ASSISTANT_MODEL) ? { reasoning_effort: "none" } : {}),
      ...body,
    }),
  });

  if (!res.ok) {
    const errText = await res.text();
    console.error("Assistant OpenAI error:", res.status, errText);
    let error = `Assistant unavailable (${res.status})`;
    if (res.status === 429) error = "OpenAI quota exceeded. Add credit at platform.openai.com.";
    if (res.status === 401) error = "OpenAI API key invalid.";
    return { ok: false, status: 502, error };
  }

  return { ok: true, message: (await res.json()).choices?.[0]?.message || {} };
};

const trimHistory = (history) =>
  (Array.isArray(history) ? history : [])
    .slice(-MAX_HISTORY)
    .filter((m) => m && (m.role === "user" || m.role === "assistant") && m.content)
    .map((m) => ({ role: m.role, content: String(m.content).slice(0, 1000) }));

const validateQuestion = (message) => {
  const question = String(message ?? "").trim();
  if (!question) return { error: { ok: false, status: 400, error: "message is required" } };
  if (question.length > 1000) return { error: { ok: false, status: 400, error: "message is too long" } };
  if (!process.env.OPENAI_API_KEY) {
    return { error: { ok: false, status: 500, error: "OPENAI_API_KEY is not configured" } };
  }
  return { question };
};

/**
 * Phase 1 — turn the question into a filter, a sort, or a page to open.
 *
 * @param {object} input
 * @param {string} input.message
 * @param {Array}  [input.history]        prior [{ role, content }] turns
 * @param {string} [input.inventoryMode]  diamonds | gemstones | jewelry
 * @param {object} [input.vocabulary]     { [filterField]: string[] } from live stock
 * @param {Array}  [input.navTargets]     [{ path, label }] this user may open
 * @returns {Promise<{ ok: boolean, status?: number, error?: string, body?: object }>}
 */
const runAssistantQuery = async ({ message, history, inventoryMode, vocabulary, navTargets }) => {
  const { question, error } = validateQuestion(message);
  if (error) return error;

  const mode = INVENTORY_MODES.includes(inventoryMode) ? inventoryMode : "diamonds";
  const vocab = sanitizeVocabulary(vocabulary);
  const targets = sanitizeNavTargets(navTargets);
  const tools = [buildFilterTool(vocab, mode), buildNavigateTool(targets)].filter(Boolean);

  const res = await callOpenAI({
    messages: [
      { role: "system", content: filterSystemPrompt(mode) },
      ...trimHistory(history),
      { role: "user", content: question },
    ],
    tools,
    tool_choice: "auto",
  });
  if (!res.ok) return res;

  const choice = res.message;
  const calls = choice.tool_calls || [];
  const filterCall = calls.find((c) => c.function?.name === "build_inventory_filter");
  const navCall = calls.find((c) => c.function?.name === "navigate_to_page");

  const empty = {
    filters: {},
    inventoryMode: null,
    sort: null,
    navigateTo: null,
    wantsAnswer: false,
    reply: null,
    needsClarification: true,
    dropped: [],
  };

  if (navCall && !filterCall) {
    let path = null;
    try {
      path = JSON.parse(navCall.function?.arguments || "{}").path;
    } catch (_) { /* fall through to the clarification below */ }
    const allowed = targets.find((t) => t.path === path);
    if (allowed) {
      return {
        ok: true,
        body: {
          ...empty,
          navigateTo: allowed,
          reply: choice.content || null,
          needsClarification: false,
        },
      };
    }
  }

  // No usable tool call means the model judged the request too vague, or asked
  // for something we don't stock — its text is the whole answer.
  if (!filterCall) {
    return {
      ok: true,
      body: {
        ...empty,
        reply: choice.content || "I couldn't turn that into a filter. Could you be more specific?",
      },
    };
  }

  let args;
  try {
    args = JSON.parse(filterCall.function?.arguments || "{}");
  } catch (e) {
    console.warn("Assistant returned unparsable tool arguments:", filterCall.function?.arguments);
    return { ok: false, status: 502, error: "Could not read the assistant's filter" };
  }

  const {
    inventoryMode: suggestedMode,
    sort: rawSort,
    wantsAnswer,
    ...filterArgs
  } = args;
  const { filters, applied, dropped } = validateFilters(filterArgs, vocab, mode);
  const targetMode = INVENTORY_MODES.includes(suggestedMode) ? suggestedMode : mode;
  const sort = validateSort(rawSort, targetMode);

  // Nothing survived validation, no ordering, and nothing being asked about the
  // list as it stands — report a miss rather than silently doing nothing and
  // looking broken. A bare wantsAnswer is not a miss: that is how a question
  // about what is already on screen arrives.
  if (applied.length === 0 && !sort && wantsAnswer !== true) {
    return {
      ok: true,
      body: {
        ...empty,
        reply: choice.content || "I couldn't match that to anything in your inventory.",
        dropped,
      },
    };
  }

  return {
    ok: true,
    body: {
      filters,
      inventoryMode: targetMode !== mode ? targetMode : null,
      sort,
      navigateTo: null,
      wantsAnswer: wantsAnswer === true,
      reply: choice.content || null,
      needsClarification: false,
      dropped,
    },
  };
};

/**
 * Phase 2 — answer about the stock the browser is now showing.
 *
 * @param {object} input
 * @param {string} input.message      the original question
 * @param {Array}  [input.history]
 * @param {Array}  input.shortlist    rows the browser is displaying, already masked
 * @param {object} [input.summary]    aggregates over every match, not just the sample
 * @param {number} [input.totalCount] how many matched in full
 * @returns {Promise<{ ok: boolean, status?: number, error?: string, body?: object }>}
 */
const runAssistantAdvice = async ({ message, history, shortlist, summary, totalCount }) => {
  const { question, error } = validateQuestion(message);
  if (error) return error;

  const rows = sanitizeShortlist(shortlist);
  const stats = sanitizeSummary(summary);

  if (rows.length === 0) {
    return {
      ok: true,
      body: { reply: "Nothing matched, so there is nothing to report.", skus: [] },
    };
  }

  const total = Number.isFinite(Number(totalCount))
    ? Number(totalCount)
    : stats?.count ?? rows.length;

  const payload = [
    stats ? `SUMMARY (all ${total} matches):\n${JSON.stringify(stats)}` : null,
    `SAMPLE (${rows.length} item(s)):\n${JSON.stringify(rows)}`,
  ].filter(Boolean).join("\n\n");

  const res = await callOpenAI({
    messages: [
      {
        role: "system",
        content: advisorSystemPrompt(total, rows.length, !!stats, stats?.priceMode),
      },
      ...trimHistory(history),
      { role: "user", content: `${question}\n\n${payload}` },
    ],
  });
  if (!res.ok) return res;

  const reply = res.message.content || "";
  // Pull out the SKUs the model actually named so the UI can surface those
  // cards first, instead of making the dealer hunt for them in the list.
  const named = rows.filter((r) => reply.includes(r.sku)).map((r) => r.sku);

  return { ok: true, body: { reply, skus: named } };
};

module.exports = { runAssistantQuery, runAssistantAdvice, ASSISTANT_MODEL, SHORTLIST_MAX };
