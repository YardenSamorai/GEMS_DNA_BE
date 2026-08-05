/* The inventory assistant's model call, kept out of server.js so the same
 * code path can be exercised directly by utils/assistantQuery.live.js. */

const fetch = require("node-fetch");
const {
  INVENTORY_MODES,
  buildFilterTool,
  validateFilters,
  sanitizeVocabulary,
} = require("./assistantFilterSchema");

const ASSISTANT_MODEL = process.env.ASSISTANT_MODEL || "gpt-5.6-terra";
// Enough to resolve "and the same but under 3 carats" without letting the
// prompt grow unbounded.
const MAX_HISTORY = 12;

const systemPrompt = (inventoryMode) => `You turn a gemstone dealer's request into an inventory filter.

You are looking at the "${inventoryMode}" tab of an inventory of loose diamonds, coloured gemstones and finished jewellery.

Rules:
- Call build_inventory_filter when the request describes a slice of stock, even partially.
- Only set fields the user actually asked for. Never invent a constraint to be helpful; an unasked-for filter silently hides stock the dealer wanted to see.
- Choose values only from the enums given. They are the values that exist in this inventory right now.
- If the request is too vague to filter on ("show me something nice", "the good ones"), do NOT call the tool. Reply with one short question that would let you filter.
- If the request names something absent from the enums, do NOT call the tool. Say plainly what is not available.
- Trade shorthand: "ct" and "carat" mean weight, "pc" means price per carat, "stones" means loose stones rather than jewellery. For coloured stones the trade says "clarity" when it means the treatment field (No Oil, Insignificant, Minor, Moderate, Significant).
- Reply in the user's own language, Hebrew or English. Keep any reply to one sentence; the interface shows the applied filters and the result count, so do not list them back or guess how many matched.`;

/**
 * Ask the model for a filter and validate whatever comes back.
 *
 * @param {object} input
 * @param {string} input.message
 * @param {Array}  [input.history]        prior [{ role, content }] turns
 * @param {string} [input.inventoryMode]  diamonds | gemstones | jewelry
 * @param {object} [input.vocabulary]     { [filterField]: string[] } from live stock
 * @returns {Promise<{ ok: boolean, status?: number, error?: string, body?: object }>}
 */
const runAssistantQuery = async ({ message, history, inventoryMode, vocabulary }) => {
  const question = String(message ?? "").trim();
  if (!question) return { ok: false, status: 400, error: "message is required" };
  if (question.length > 1000) return { ok: false, status: 400, error: "message is too long" };
  if (!process.env.OPENAI_API_KEY) {
    return { ok: false, status: 500, error: "OPENAI_API_KEY is not configured" };
  }

  const mode = INVENTORY_MODES.includes(inventoryMode) ? inventoryMode : "diamonds";
  const vocab = sanitizeVocabulary(vocabulary);
  const tool = buildFilterTool(vocab, mode);

  const priorTurns = (Array.isArray(history) ? history : [])
    .slice(-MAX_HISTORY)
    .filter((m) => m && (m.role === "user" || m.role === "assistant") && m.content)
    .map((m) => ({ role: m.role, content: String(m.content).slice(0, 1000) }));

  const aiRes = await fetch("https://api.openai.com/v1/chat/completions", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${process.env.OPENAI_API_KEY}`,
    },
    body: JSON.stringify({
      model: ASSISTANT_MODEL,
      messages: [
        { role: "system", content: systemPrompt(mode) },
        ...priorTurns,
        { role: "user", content: question },
      ],
      tools: [tool],
      tool_choice: "auto",
      // gpt-5 refuses function tools on chat/completions unless reasoning is
      // switched off, and reasoning buys nothing here — the enums already
      // constrain the answer. Older models reject the parameter outright, so
      // it is only sent where it applies.
      ...(/^gpt-5/.test(ASSISTANT_MODEL) ? { reasoning_effort: "none" } : {}),
    }),
  });

  if (!aiRes.ok) {
    const errText = await aiRes.text();
    console.error("Assistant OpenAI error:", aiRes.status, errText);
    let error = `Assistant unavailable (${aiRes.status})`;
    if (aiRes.status === 429) error = "OpenAI quota exceeded. Add credit at platform.openai.com.";
    if (aiRes.status === 401) error = "OpenAI API key invalid.";
    return { ok: false, status: 502, error };
  }

  const data = await aiRes.json();
  const choice = data.choices?.[0]?.message || {};
  const call = choice.tool_calls?.find((c) => c.function?.name === "build_inventory_filter");

  // No tool call means the model judged the request too vague, or asked for
  // something we don't stock — its text is the whole answer.
  if (!call) {
    return {
      ok: true,
      body: {
        filters: {},
        inventoryMode: null,
        reply: choice.content || "I couldn't turn that into a filter. Could you be more specific?",
        needsClarification: true,
        dropped: [],
      },
    };
  }

  let args;
  try {
    args = JSON.parse(call.function?.arguments || "{}");
  } catch (e) {
    console.warn("Assistant returned unparsable tool arguments:", call.function?.arguments);
    return { ok: false, status: 502, error: "Could not read the assistant's filter" };
  }

  const { inventoryMode: suggestedMode, ...filterArgs } = args;
  const { filters, applied, dropped } = validateFilters(filterArgs, vocab, mode);

  // Everything the model asked for was rejected — report a miss rather than
  // silently applying nothing and looking broken.
  if (applied.length === 0) {
    return {
      ok: true,
      body: {
        filters: {},
        inventoryMode: null,
        reply: choice.content || "I couldn't match that to anything in your inventory.",
        needsClarification: true,
        dropped,
      },
    };
  }

  return {
    ok: true,
    body: {
      filters,
      inventoryMode:
        INVENTORY_MODES.includes(suggestedMode) && suggestedMode !== mode ? suggestedMode : null,
      reply: choice.content || null,
      needsClarification: false,
      dropped,
    },
  };
};

module.exports = { runAssistantQuery, ASSISTANT_MODEL };
