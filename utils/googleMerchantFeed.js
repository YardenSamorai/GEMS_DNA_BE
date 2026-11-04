/* =============================================================================
 * Google Merchant Center product feed for Eshed.com
 * =============================================================================
 *
 * Without a feed of our own, Merchant Center builds its products by crawling
 * the site and keys each one on the last hyphen-separated piece of the SKU:
 * RI-HE-007 and RI-ONE-007 both became "007", and Google showed the 10.05ct
 * ring's photo next to the $22,000 price of an emerald ring ("83% off").
 * This feed gives every product its full SKU as the id.
 *
 * The source is the site's own WooCommerce Store API, so each price here is
 * the price on the page Google checks it against.
 *
 * What goes to Google:
 *   - every jewelry piece,
 *   - loose stones up to STONE_MAX_PRICE total.
 * ========================================================================== */

const fetch = require("node-fetch");

const STONE_MAX_PRICE = 50000;
const JEWELRY_CATEGORY = "jewelry";
const BRAND = "Eshed";

const PAGE_SIZE = 100;
const CACHE_MS = 30 * 60 * 1000;
/* A failed or half-answered fetch must never shrink the catalog Google holds:
 * products missing from the feed are dropped from Shopping. */
const MIN_PRODUCTS = 100;

const ENTITIES = { amp: "&", lt: "<", gt: ">", quot: '"', apos: "'", nbsp: " " };

const decodeEntities = (s) =>
  String(s ?? "").replace(/&(#x[0-9a-f]+|#\d+|[a-z]+);/gi, (m, code) => {
    if (code[0] === "#") {
      const n = code[1].toLowerCase() === "x" ? parseInt(code.slice(2), 16) : parseInt(code.slice(1), 10);
      return Number.isFinite(n) ? String.fromCodePoint(n) : m;
    }
    return ENTITIES[code.toLowerCase()] ?? m;
  });

const plainText = (html) =>
  decodeEntities(String(html ?? "").replace(/<br\s*\/?>/gi, " ").replace(/<[^>]*>/g, " "))
    .replace(/\s+/g, " ")
    .trim();

const escapeXml = (s) =>
  String(s ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&apos;");

/* Store API prices are integers in minor units ("2500000" with 2 decimals). */
const amount = (minor, decimals) => {
  const n = Number(minor);
  if (!Number.isFinite(n) || n <= 0) return null;
  return n / 10 ** (Number(decimals) || 0);
};

const money = (value, currency) => `${value.toFixed(2)} ${currency}`;

function toFeedItem(p) {
  const prices = p.prices || {};
  const decimals = prices.currency_minor_unit ?? 2;
  const regular = amount(prices.regular_price, decimals) ?? amount(prices.price, decimals);
  const current = amount(prices.price, decimals) ?? regular;
  const categories = (p.categories || []).map((c) => String(c.slug || "").toLowerCase());
  const title = plainText(p.name);
  return {
    id: String(p.sku || "").trim() || `wc-${p.id}`,
    wooId: p.id,
    title,
    description: plainText(p.short_description) || plainText(p.description) || title,
    link: p.permalink,
    images: (p.images || []).map((i) => i.src).filter(Boolean),
    currency: prices.currency_code || "USD",
    price: regular,
    salePrice: current != null && regular != null && current < regular ? current : null,
    inStock: p.is_in_stock !== false,
    isJewelry: categories.includes(JEWELRY_CATEGORY),
    productType: decodeEntities((p.categories || [])[0]?.name || ""),
  };
}

const sellingPrice = (item) => item.salePrice ?? item.price;

const includeInFeed = (item) =>
  item.price != null && !!item.link && (item.isJewelry || sellingPrice(item) <= STONE_MAX_PRICE);

/* Two products sharing a SKU would collide again in Merchant Center. */
function uniqueIds(items) {
  const seen = new Set();
  return items.map((item) => {
    const id = seen.has(item.id) ? `${item.id}-${item.wooId}` : item.id;
    seen.add(id);
    return id === item.id ? item : { ...item, id };
  });
}

function itemXml(item) {
  const tag = (name, value) => (value == null || value === "" ? "" : `      <g:${name}>${escapeXml(value)}</g:${name}>\n`);
  const [image, ...more] = item.images;
  return (
    "    <item>\n" +
    tag("id", item.id) +
    tag("title", item.title.slice(0, 150)) +
    tag("description", item.description.slice(0, 5000)) +
    tag("link", item.link) +
    tag("image_link", image) +
    more.slice(0, 10).map((src) => tag("additional_image_link", src)).join("") +
    tag("availability", item.inStock ? "in_stock" : "out_of_stock") +
    tag("price", money(item.price, item.currency)) +
    (item.salePrice != null ? tag("sale_price", money(item.salePrice, item.currency)) : "") +
    tag("condition", "new") +
    tag("brand", BRAND) +
    tag("identifier_exists", "no") +
    tag("product_type", item.productType) +
    "    </item>\n"
  );
}

function buildFeedXml(items, { siteUrl }) {
  return (
    '<?xml version="1.0" encoding="UTF-8"?>\n' +
    '<rss version="2.0" xmlns:g="http://base.google.com/ns/1.0">\n' +
    "  <channel>\n" +
    `    <title>${escapeXml(BRAND)}</title>\n` +
    `    <link>${escapeXml(siteUrl)}</link>\n` +
    `    <description>${escapeXml(`${BRAND} products for Google Merchant Center`)}</description>\n` +
    items.map(itemXml).join("") +
    "  </channel>\n" +
    "</rss>\n"
  );
}

async function fetchPage(siteUrl, page) {
  const url = `${siteUrl}/wp-json/wc/store/v1/products?per_page=${PAGE_SIZE}&page=${page}`;
  const res = await fetch(url, { timeout: 60000, headers: { Accept: "application/json" } });
  if (!res.ok) throw new Error(`WooCommerce Store API page ${page}: HTTP ${res.status}`);
  return { products: await res.json(), totalPages: Number(res.headers.get("x-wp-totalpages")) || 1 };
}

async function fetchStoreProducts(siteUrl) {
  const first = await fetchPage(siteUrl, 1);
  const rest = await Promise.all(
    Array.from({ length: Math.max(0, first.totalPages - 1) }, (_, i) => fetchPage(siteUrl, i + 2))
  );
  return [first, ...rest].flatMap((p) => p.products);
}

const siteUrlFromEnv = () => (process.env.WOOCOMMERCE_URL || "https://eshed.com").replace(/\/+$/, "");

let cached = null;
let inFlight = null;

async function build(siteUrl) {
  const products = await fetchStoreProducts(siteUrl);
  if (products.length < MIN_PRODUCTS) {
    throw new Error(`WooCommerce returned only ${products.length} products`);
  }
  const items = uniqueIds(products.map(toFeedItem).filter(includeInFeed));
  return { xml: buildFeedXml(items, { siteUrl }), count: items.length, builtAt: Date.now() };
}

/* Serves the last good feed when a rebuild fails, so a WooCommerce hiccup
 * during Google's daily fetch doesn't empty the catalog. */
async function getFeed({ siteUrl = siteUrlFromEnv() } = {}) {
  if (cached && Date.now() - cached.builtAt < CACHE_MS) return cached;
  if (!inFlight) {
    inFlight = build(siteUrl)
      .then((feed) => (cached = feed))
      .finally(() => {
        inFlight = null;
      });
  }
  try {
    return await inFlight;
  } catch (e) {
    if (cached) return cached;
    throw e;
  }
}

module.exports = {
  STONE_MAX_PRICE,
  decodeEntities,
  plainText,
  toFeedItem,
  includeInFeed,
  uniqueIds,
  buildFeedXml,
  getFeed,
};
