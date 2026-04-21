/**
 * Geo / country detection helpers.
 *
 * Combines three independent signals to infer a contact's country (and
 * normalise their city) from whatever info is available:
 *
 *   1. Phone number   →  libphonenumber-js (offline, instant, very reliable)
 *   2. City / address →  Mapbox Geocoding API (paid for high-volume but
 *                        100k req/month free, no credit card required)
 *   3. Email TLD      →  static lookup table (.co.il → IL, .de → DE, ...)
 *
 * The endpoint never blindly returns one signal — it returns ALL three
 * (when available) plus a single best-guess and a confidence score, so
 * the UI can let the user confirm before applying.
 */

const { parsePhoneNumberFromString } = require("libphonenumber-js");

/**
 * Tries to parse a phone number that may or may not start with a country
 * code, optionally biasing the parser with a known default country.
 *
 *   - If the number already starts with `+`, libphonenumber identifies the
 *     country directly and `defaultCountry` is ignored.
 *   - If it doesn't (e.g. "(212) 555-1234"), `defaultCountry` is used to
 *     interpret the number as a local one in that country.
 *   - Returns { country, e164, international } on success, null on failure.
 */
function tryParsePhone(phone, defaultCountry) {
  if (!phone) return null;
  try {
    const raw = String(phone).trim();
    const parsed = parsePhoneNumberFromString(raw, defaultCountry || undefined);
    if (parsed && parsed.isValid()) {
      return {
        country: parsed.country || null,
        e164: parsed.number,                    // +12125551234
        international: parsed.formatInternational(), // +1 212 555 1234
      };
    }
  } catch (_) { /* invalid */ }
  return null;
}

/* ---------- Country code → English name --------------------------------- */
// We deliberately keep this list small/explicit (the ones our customers
// actually use). Anything not on it falls back to the ISO code.
const COUNTRY_NAMES = {
  US: "United States", CA: "Canada", MX: "Mexico",
  GB: "United Kingdom", IE: "Ireland",
  IL: "Israel", AE: "United Arab Emirates", SA: "Saudi Arabia",
  TR: "Turkey", EG: "Egypt", JO: "Jordan", LB: "Lebanon",
  DE: "Germany", FR: "France", IT: "Italy", ES: "Spain",
  NL: "Netherlands", BE: "Belgium", CH: "Switzerland", AT: "Austria",
  PL: "Poland", CZ: "Czech Republic", SE: "Sweden", NO: "Norway",
  DK: "Denmark", FI: "Finland", PT: "Portugal", GR: "Greece",
  HU: "Hungary", RO: "Romania", RU: "Russia", UA: "Ukraine",
  IN: "India", PK: "Pakistan", CN: "China", JP: "Japan",
  KR: "South Korea", TH: "Thailand", VN: "Vietnam", PH: "Philippines",
  ID: "Indonesia", MY: "Malaysia", SG: "Singapore", HK: "Hong Kong",
  TW: "Taiwan", AU: "Australia", NZ: "New Zealand",
  BR: "Brazil", AR: "Argentina", CL: "Chile", CO: "Colombia", PE: "Peru",
  ZA: "South Africa", NG: "Nigeria", KE: "Kenya", MA: "Morocco",
};
const countryName = (cc) => (cc ? COUNTRY_NAMES[cc.toUpperCase()] || cc.toUpperCase() : null);

/* ---------- Country flag emoji ------------------------------------------ */
const flagEmoji = (cc) => {
  if (!cc || cc.length !== 2) return "";
  const A = 0x1f1e6;
  return String.fromCodePoint(A + cc.charCodeAt(0) - 65, A + cc.charCodeAt(1) - 65);
};

/* ---------- Email TLD → country ----------------------------------------- */
// Country-code TLDs only — generic TLDs (.com / .net / .org) carry no signal.
const TLD_COUNTRY = {
  il: "IL", uk: "GB", us: "US", de: "DE", fr: "FR", it: "IT", es: "ES",
  nl: "NL", be: "BE", ch: "CH", at: "AT", se: "SE", no: "NO", dk: "DK",
  fi: "FI", pt: "PT", gr: "GR", pl: "PL", cz: "CZ", hu: "HU", ro: "RO",
  ru: "RU", ua: "UA", tr: "TR", ae: "AE", sa: "SA", in: "IN", cn: "CN",
  jp: "JP", kr: "KR", th: "TH", sg: "SG", hk: "HK", tw: "TW", au: "AU",
  nz: "NZ", br: "BR", ar: "AR", mx: "MX", za: "ZA", ca: "CA", ie: "IE",
  // Common second-level patterns
  "co.il": "IL", "co.uk": "GB", "co.za": "ZA", "co.in": "IN",
  "com.br": "BR", "com.mx": "MX", "com.au": "AU", "com.tr": "TR",
};
const countryFromEmail = (email) => {
  if (!email || typeof email !== "string") return null;
  const at = email.toLowerCase().split("@")[1];
  if (!at) return null;
  const parts = at.split(".");
  // Try multi-level first (co.il, com.br, ...)
  if (parts.length >= 2) {
    const last2 = parts.slice(-2).join(".");
    if (TLD_COUNTRY[last2]) return TLD_COUNTRY[last2];
  }
  const tld = parts[parts.length - 1];
  return TLD_COUNTRY[tld] || null;
};

/* ---------- Phone → country -------------------------------------------- */
// `defaultCountry` lets us interpret bare local-format numbers like
// "(212) 555-1234" or "020 7946 0123" by saying "if no + prefix, assume US"
// or "...assume GB". Optional — caller can pass null for the original
// strict behaviour.
const countryFromPhone = (phone, defaultCountry = null) => {
  const parsed = tryParsePhone(phone, defaultCountry);
  return parsed?.country || null;
};

/* ---------- City normalisation ----------------------------------------- */
// User-visible cities arrive in many forms ("tel-aviv", "TEL AVIV",
// "ת"א", "N.Y", ...). We collapse known aliases and otherwise apply a
// gentle Title Case transformation.
const CITY_ALIASES = {
  "tel aviv": "Tel Aviv", "telaviv": "Tel Aviv", "tel-aviv": "Tel Aviv",
  'תל אביב': "Tel Aviv", 'ת"א': "Tel Aviv", "ta": "Tel Aviv",
  "jerusalem": "Jerusalem", "ירושלים": "Jerusalem",
  "haifa": "Haifa", "חיפה": "Haifa",
  "ramat gan": "Ramat Gan", "רמת גן": "Ramat Gan",
  "ny": "New York", "n.y": "New York", "n.y.": "New York", "nyc": "New York",
  "new york": "New York", "new york city": "New York",
  "la": "Los Angeles", "l.a": "Los Angeles", "l.a.": "Los Angeles",
  "los angeles": "Los Angeles",
  "sf": "San Francisco", "san francisco": "San Francisco",
  "hk": "Hong Kong", "hong kong": "Hong Kong",
  "uk": "London",   // bare "UK" in a city field most often means London
  "london": "London",
  "paris": "Paris", "milano": "Milan", "milan": "Milan",
  "roma": "Rome", "rome": "Rome",
  "antwerp": "Antwerp", "antwerpen": "Antwerp",
  "mumbai": "Mumbai", "bombay": "Mumbai",
  "dubai": "Dubai", "abu dhabi": "Abu Dhabi",
};
const titleCase = (s) =>
  String(s)
    .toLowerCase()
    .split(/(\s+|-)/)
    .map((w) => (/^[a-zA-Z\u00C0-\u017F]/.test(w) ? w.charAt(0).toUpperCase() + w.slice(1) : w))
    .join("");
const normaliseCity = (city) => {
  if (!city || typeof city !== "string") return null;
  const trimmed = city.trim();
  if (!trimmed) return null;
  const key = trimmed.toLowerCase().replace(/\.+$/, "");
  if (CITY_ALIASES[key]) return CITY_ALIASES[key];
  return titleCase(trimmed);
};

/* ---------- Mapbox geocoding ------------------------------------------- */
/**
 * Uses Mapbox's forward geocoding endpoint to turn a free-text place
 * (city + optional country hint) into structured location data.
 * Returns null on any failure — caller should fall back gracefully.
 */
async function mapboxLookup(query, opts = {}) {
  const token = process.env.MAPBOX_TOKEN;
  if (!token || !query) return null;
  try {
    const params = new URLSearchParams({
      access_token: token,
      limit: "1",
      // Bias toward populated places so "Antwerp" returns the city, not a street
      types: "place,locality,region,country",
    });
    if (opts.countryHint) params.set("country", String(opts.countryHint).toLowerCase());

    const url =
      `https://api.mapbox.com/geocoding/v5/mapbox.places/` +
      `${encodeURIComponent(query)}.json?${params.toString()}`;

    const resp = await fetch(url, { method: "GET" });
    if (!resp.ok) return null;
    const data = await resp.json();
    const feat = (data.features || [])[0];
    if (!feat) return null;

    // Mapbox returns [lng, lat]
    const [lng, lat] = feat.center || [];
    let country = null;
    let countryCode = null;
    let city = null;

    // The most precise place name lives in the feature itself if it's a
    // city; otherwise we walk the context array (which Mapbox sorts from
    // most specific to least specific).
    if (feat.place_type?.includes("country")) {
      country = feat.text;
      countryCode = (feat.properties?.short_code || "").toUpperCase();
    } else {
      city = feat.text;
    }
    for (const ctx of feat.context || []) {
      if (ctx.id?.startsWith("country")) {
        country = country || ctx.text;
        countryCode = countryCode || (ctx.short_code || "").toUpperCase();
      } else if (!city && (ctx.id?.startsWith("place") || ctx.id?.startsWith("locality"))) {
        city = ctx.text;
      }
    }

    return {
      country,
      countryCode,
      city,
      lat: typeof lat === "number" ? lat : null,
      lng: typeof lng === "number" ? lng : null,
      placeName: feat.place_name,
    };
  } catch (err) {
    console.warn("Mapbox lookup failed:", err.message);
    return null;
  }
}

/* ---------- Master orchestrator ---------------------------------------- */
/**
 * Combines all available signals into a single best guess.
 *
 * Confidence model (rough but useful):
 *   - "high"    : phone OR Mapbox returned a country
 *   - "medium"  : email TLD only
 *   - "low"     : nothing
 *
 * Conflict resolution: if Mapbox and phone disagree (e.g. an Israeli
 * number on a contact whose office is in NYC), we trust Mapbox for the
 * country (people travel; offices don't), but we keep phone as a
 * secondary "alternates" entry so the UI can offer both.
 */
async function detectGeo({ phone, city, address, country, email } = {}) {
  // ---- Pass 1: try to detect country from STRONG location signals only.
  //              We deliberately do NOT pass a default country to the phone
  //              parser yet, because if the phone really has a "+" prefix
  //              we want to discover the country from it independently.
  const signals = {
    phone: countryFromPhone(phone, null),  // strict: only +-prefixed numbers count
    email: countryFromEmail(email),
    mapbox: null,
  };

  // Build a sensible query: prefer the most specific location available.
  const query = [city, address, country].filter(Boolean).join(", ").trim();
  if (query) {
    signals.mapbox = await mapboxLookup(query, {
      countryHint: country ? null : signals.phone || signals.email || null,
    });
  }

  // Resolve the country winner using the strongest available signal.
  // Order of trust:
  //   1. Mapbox geocoding of the address (highest precision)
  //   2. Strict phone country (only valid if number had + prefix)
  //   3. Email TLD (weakest, but still useful)
  let bestCC = null;
  let bestCountry = null;
  let confidence = "low";
  let source = null;

  if (signals.mapbox?.countryCode) {
    bestCC = signals.mapbox.countryCode;
    bestCountry = signals.mapbox.country || countryName(bestCC);
    confidence = "high";
    source = "mapbox";
  } else if (signals.phone) {
    bestCC = signals.phone;
    bestCountry = countryName(bestCC);
    confidence = "high";
    source = "phone";
  } else if (signals.email) {
    bestCC = signals.email;
    bestCountry = countryName(bestCC);
    confidence = "medium";
    source = "email";
  }

  // ---- Pass 2: if we have a country guess BUT the phone has no country
  //              code (e.g. "(212) 555-1234" on a card with a NYC address),
  //              re-parse the phone with that country as a hint and produce
  //              the international form. This is the magic that lets a US
  //              local number on a US business card become "+1 212 555 1234"
  //              automatically.
  let formattedPhone = null;
  if (phone && bestCC) {
    const reparsed = tryParsePhone(phone, bestCC);
    if (reparsed?.international) {
      const original = String(phone).trim();
      // Only suggest the formatted version if it actually adds info
      // (i.e. the user didn't already type a +country-coded number).
      if (!original.startsWith("+") || original.replace(/[^\d+]/g, "") !== reparsed.e164) {
        formattedPhone = {
          e164: reparsed.e164,                 // +12125551234
          international: reparsed.international, // +1 212 555 1234
          inferredCountry: reparsed.country,
        };
      }
    }
  }

  const alternates = [];
  if (signals.phone && signals.phone !== bestCC) {
    alternates.push({ countryCode: signals.phone, country: countryName(signals.phone), source: "phone" });
  }
  if (signals.email && signals.email !== bestCC && signals.email !== signals.phone) {
    alternates.push({ countryCode: signals.email, country: countryName(signals.email), source: "email" });
  }

  return {
    country: bestCountry,
    countryCode: bestCC,
    flag: flagEmoji(bestCC),
    city: signals.mapbox?.city ? normaliseCity(signals.mapbox.city) : normaliseCity(city),
    lat: signals.mapbox?.lat ?? null,
    lng: signals.mapbox?.lng ?? null,
    placeName: signals.mapbox?.placeName || null,
    confidence,
    source,
    alternates,
    signals,
    formattedPhone,  // null if no improvement was possible
  };
}

module.exports = {
  detectGeo,
  countryFromPhone,
  countryFromEmail,
  normaliseCity,
  countryName,
  flagEmoji,
  mapboxLookup,
};
