// Fantasy (inventory management) API client — server side only.
// Credentials come from FANTASY_URL / FANTASY_USER / FANTASY_PASSWORD and are
// never sent to the browser. API reference: Fantasy API Documentation (Postman
// collection 29605960/2sAYBPnF2T).

const TOKEN_SAFETY_MS = 60 * 60 * 1000;

let cached = null; // { token, expiresAt }
let pendingLogin = null;

const config = () => {
  const base = String(process.env.FANTASY_URL || "").trim().replace(/\/+$/, "");
  const username = process.env.FANTASY_USER;
  const password = process.env.FANTASY_PASSWORD;
  if (!base || !username || !password) {
    throw new Error("Fantasy API is not configured (FANTASY_URL, FANTASY_USER, FANTASY_PASSWORD)");
  }
  return { base, username, password };
};

const isConfigured = () => {
  try { config(); return true; } catch { return false; }
};

const login = async () => {
  const { base, username, password } = config();
  const res = await fetch(`${base}/token`, {
    method: "POST",
    headers: { Accept: "application/json", "Content-Type": "application/x-www-form-urlencoded" },
    body: new URLSearchParams({ grant_type: "password", username, password }).toString(),
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok || !data.access_token) {
    throw new Error(`Fantasy login failed (${res.status}): ${data.error_description || data.error || "no token"}`);
  }
  const ttlMs = Number(data.expires_in) > 0 ? Number(data.expires_in) * 1000 : 24 * 60 * 60 * 1000;
  cached = { token: data.access_token, expiresAt: Date.now() + ttlMs - TOKEN_SAFETY_MS };
  return cached.token;
};

const getToken = async () => {
  if (cached && cached.expiresAt > Date.now()) return cached.token;
  if (!pendingLogin) pendingLogin = login().finally(() => { pendingLogin = null; });
  return pendingLogin;
};

/** Authenticated call; logs in again once if the token was rejected. */
const call = async (method, path, body, { retried = false } = {}) => {
  const { base } = config();
  const token = await getToken();
  const res = await fetch(`${base}${path}`, {
    method,
    headers: {
      Accept: "application/json",
      Authorization: `Bearer ${token}`,
      ...(body !== undefined ? { "Content-Type": "application/json" } : {}),
    },
    body: body !== undefined ? JSON.stringify(body) : undefined,
  });
  if (res.status === 401 && !retried) {
    cached = null;
    return call(method, path, body, { retried: true });
  }
  const text = await res.text();
  if (!res.ok) throw new Error(`Fantasy ${method} ${path} → ${res.status}: ${text.slice(0, 300)}`);
  try { return text ? JSON.parse(text) : null; } catch { return text; }
};

/**
 * Read lots from /api/stock.
 * @param {object} q
 * @param {string} q.select  comma-separated field list (keep it short — a lot has 300+ fields)
 * @param {Array}  q.filters [{ fieldName, value | fromValue/toValue, StringMatch }]
 * @param {Array}  q.order   [{ fieldName, direction: "ascending" | "descending" }]
 */
const getStock = ({ select, filters, order, pageSize, pageNumber, loadAll } = {}) =>
  call("POST", "/api/stock", {
    requestType: "GET",
    request: {
      ...(select ? { select } : {}),
      ...(filters ? { filters } : {}),
      ...(order ? { order } : {}),
      ...(loadAll ? { IsLoadingAll: true } : { pageSize: pageSize ?? 100, pageNumber: pageNumber ?? 0 }),
    },
  });

/** Every lot matching the query in one call (IsLoadingAll). */
const getAllStock = async (q = {}) => {
  const rows = await getStock({ ...q, loadAll: true });
  return Array.isArray(rows) ? rows : [];
};

const searchItems = ({ select = "ItemID, ItemName, ItemTypeID", filters } = {}) =>
  call("POST", "/api/Items", { requestType: "GET", request: { select, ...(filters ? { filters } : {}) } });

module.exports = { isConfigured, getStock, getAllStock, searchItems, _call: call };
