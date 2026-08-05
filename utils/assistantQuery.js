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
- Set wantsRecommendation true when the dealer asks you to pick, compare, or advise ("which is the best buy", "what would you show a customer looking for X"). You will then be shown the matches and asked to answer properly. Leave it false for plain filtering.
- Call navigate_to_page only when the request is about a different part of the app. Never use it to show inventory results — those appear in place.
- If the request is too vague to act on ("show me something nice"), do NOT call a tool. Reply with one short question that would let you filter.
- If the request names something absent from the enums, do NOT call a tool. Say plainly what is not available.
- ${TRADE_NOTES}
- ${LANGUAGE_NOTE} Keep any reply to one sentence; the interface already shows the applied filters and the result count, so do not list them back or guess how many matched.`;

const advisorSystemPrompt = (total, shown) => `You are advising a gemstone dealer on stones from their own inventory.

The filter you chose matched ${total} item(s). Below are ${shown} of them as JSON — this is the real stock, and the only stock you may talk about.

Rules:
- Recommend specific items and always name them by SKU, so the dealer can find them.
- Compare on what the data actually shows: weight, price per carat, total price, colour, clarity or treatment, lab, origin. Never invent a property that is not in the JSON.
- Say why, briefly and concretely ("K1188 is the best value: 5.1ct at $12,400/ct, the lowest per carat of the certified GRS stones here").
- Recommend at most three items unless asked for more.
${total > shown ? `- You are seeing only the first ${shown} of ${total} matches, ordered as the dealer sees them. Say so if it matters to the answer.` : ""}
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
    wantsRecommendation: false,
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
    wantsRecommendation,
    ...filterArgs
  } = args;
  const { filters, applied, dropped } = validateFilters(filterArgs, vocab, mode);
  const targetMode = INVENTORY_MODES.includes(suggestedMode) ? suggestedMode : mode;
  const sort = validateSort(rawSort, targetMode);

  // Nothing survived validation and there is no ordering to apply either —
  // report a miss rather than silently doing nothing and looking broken.
  if (applied.length === 0 && !sort) {
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
      wantsRecommendation: wantsRecommendation === true,
      reply: choice.content || null,
      needsClarification: false,
      dropped,
    },
  };
};

/**
 * Phase 2 — answer about the stones the browser is now showing.
 *
 * @param {object} input
 * @param {string} input.message      the original question
 * @param {Array}  [input.history]
 * @param {Array}  input.shortlist    rows the browser is displaying, already masked
 * @param {number} [input.totalCount] how many matched in full
 * @returns {Promise<{ ok: boolean, status?: number, error?: string, body?: object }>}
 */
const runAssistantAdvice = async ({ message, history, shortlist, totalCount }) => {
  const { question, error } = validateQuestion(message);
  if (error) return error;

  const rows = sanitizeShortlist(shortlist);
  if (rows.length === 0) {
    return {
      ok: true,
      body: { reply: "Nothing matched, so there is nothing I can recommend.", skus: [] },
    };
  }

  const total = Number.isFinite(Number(totalCount)) ? Number(totalCount) : rows.length;

  const res = await callOpenAI({
    messages: [
      { role: "system", content: advisorSystemPrompt(total, rows.length) },
      ...trimHistory(history),
      { role: "user", content: `${question}\n\nMatching stock:\n${JSON.stringify(rows)}` },
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
