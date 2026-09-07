/* Supplier media proxy — translating Barak URLs to and from our own origin.
 *
 * Barak's IIS started answering every file under /Gemstones/Output/ with
 * `Cross-Origin-Resource-Policy: same-origin`. A browser on gems-dna.com
 * downloads the bytes, notices the page is a different origin, and throws
 * them away: a 200 response that still paints a broken <img>. That is what
 * emptied the sales grid, the DNA pages and every certificate preview. The
 * PDF export kept working precisely because it already pulled the files
 * through this server instead of the browser.
 *
 * Serving the same bytes from our origin removes the question entirely.
 *
 * The proxy keeps the supplier's path verbatim rather than stuffing the URL
 * into a query string, because a surprising amount of frontend code reads
 * meaning out of the URL's shape: a filename after the last slash is how
 * `usableImg` decides a stone has a photo at all (the export emits bare
 * folder paths for stones without one), and a `.pdf` tail is how the media
 * viewer decides to render a document rather than an image. A
 * `?url=`-style proxy would silently change what every one of those checks
 * sees. `/api/barak/Gemstones/Output/…/x.jpg` changes nothing.
 */

const SUPPLIER_ORIGIN = "https://app.barakdiamonds.com";

/* Every URL the API hands out — all 7,738 of them — lives under this folder.
 * Refusing anything else keeps the proxy from being pointed at the rest of
 * the supplier's site, including their admin UI. */
const SUPPLIER_MEDIA_PREFIX = "/gemstones/output/";

const PROXY_PREFIX = "/api/barak/";

/* Historic rows carry both schemes. */
const SUPPLIER_URL_RE = /https?:\/\/app\.barakdiamonds\.com\//gi;

/* Matches a proxy URL on any host, so a body posted from production, a
 * preview deploy or localhost all normalise back to the supplier. */
const PROXY_URL_RE = /https?:\/\/[^"'\s\\]+\/api\/barak\//gi;

/* The proxy only ever serves stone photos and certificates. Anything else
 * from that host — an error page, a login redirect — must not be relayed
 * from our origin. */
const ALLOWED_CONTENT_TYPE = /^(image\/|application\/pdf)/i;

/* Absolute, because the frontend sits on a different host to this API. */
const proxyBaseFor = (req) => `${req.protocol}://${req.get("host")}${PROXY_PREFIX}`;

/* Turns the tail of a proxy request back into the supplier URL it stands for,
 * or null when it points anywhere we refuse to fetch. `new URL` normalises
 * `..` segments before the prefix is checked, so traversal cannot escape the
 * media folder, and a tail that is itself an absolute URL lands harmlessly
 * inside the supplier's path instead of redirecting us elsewhere. */
const resolveSupplierUrl = (tail) => {
  if (!tail || typeof tail !== "string") return null;

  let parsed;
  try {
    parsed = new URL(`${SUPPLIER_ORIGIN}/${tail.replace(/^\/+/, "")}`);
  } catch (_) {
    return null;
  }

  if (parsed.origin !== SUPPLIER_ORIGIN) return null;
  if (!parsed.pathname.toLowerCase().startsWith(SUPPLIER_MEDIA_PREFIX)) return null;
  return parsed.href;
};

const rewriteToProxy = (text, proxyBase) =>
  typeof text === "string" ? text.replace(SUPPLIER_URL_RE, proxyBase) : text;

const rewriteToSupplier = (text) =>
  typeof text === "string" ? text.replace(PROXY_URL_RE, `${SUPPLIER_ORIGIN}/`) : text;

const isAllowedContentType = (type) => ALLOWED_CONTENT_TYPE.test(String(type || ""));

module.exports = {
  SUPPLIER_ORIGIN,
  SUPPLIER_MEDIA_PREFIX,
  PROXY_PREFIX,
  proxyBaseFor,
  resolveSupplierUrl,
  rewriteToProxy,
  rewriteToSupplier,
  isAllowedContentType,
};
