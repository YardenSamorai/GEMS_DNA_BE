/* =============================================================================
 * eTrade (website) stone export
 * =============================================================================
 *
 * Builds the same CSV Barak's eTrade module produces for Eshed.com, from the
 * soap_stones rows an admin put on the website list. The sync that publishes
 * to the site reads this file, so the header row, column order and the
 * value rewrites below must match Barak's file exactly.
 *
 * Barak's web file is the full stock export minus the internal columns
 * (cost, holder, box, grouping, stones, jewelry model) with:
 *   - Location and TradeShow blanked,
 *   - Branch as a two-letter branch code (EM → IL, EY → NY, …),
 *   - the " O" suffix dropped from Category ("Tourmaline O" → "Tourmaline"),
 *   - Lab "ESH" spelled out as "Eshed".
 * ========================================================================== */

const HEADERS = [
  "Category", "SKU", "Shape", "Weight", "Color", "Clarity", "Lab", "Fluorescence",
  "Price Per Carat", "Rap Price % ", "Rap. Price", "Total Price", "Location", "Image",
  "Video", "Certificate image", "Certificate Number", "Cut", "Polish", "Symmetry",
  "Table", "Depth", "ratio", "Measurements (- delimiter)", "Pair Stone",
  "fancy_intensity", "fancy_color", "fancy_overtone", "fancy_color_2", "fancy_overtone_2",
  "home_page", "additional_pictures", "Branch", "additional_videos", "Comment", "Type",
  "Cert. Comments", "certificateImageJPG", "Origin", "TradeShow",
];

/* soap_stones keeps the branch as a display name; the web file wants the
 * code. Every company code Barak uses maps onto one of these four. */
const BRANCH_CODE = {
  israel: "IL",
  "new york": "NY",
  "hong kong": "HK",
  "los angeles": "LA",
};

const LAB_NAME = { ESH: "Eshed" };

const text = (v) => (v == null ? "" : String(v).trim());

const num = (v, digits = 2) => {
  if (v == null || v === "") return "";
  const n = Number(v);
  return Number.isFinite(n) ? n.toFixed(digits) : "";
};

/* Prices keep every digit we hold: some per-carat prices are derived
 * (total ÷ weight) and carry long fractions, and rounding them would
 * publish a different price than Barak does. pg hands numeric back as an
 * exact string, so trim padding zeros from that rather than going through
 * a float. */
const price = (v) => {
  if (v == null || v === "") return "";
  const s = String(v).trim();
  if (!/^-?\d+(\.\d+)?$/.test(s)) {
    const n = Number(s);
    return Number.isFinite(n) ? String(n) : "";
  }
  return s.includes(".") ? s.replace(/0+$/, "").replace(/\.$/, "") : s;
};

const branchCode = (v) => {
  const s = text(v);
  if (!s) return "";
  return BRANCH_CODE[s.toLowerCase()] || s;
};

const category = (v) => text(v).replace(/\s+O$/, "");

const lab = (v) => {
  const s = text(v);
  return LAB_NAME[s.toUpperCase()] || s;
};

/* One soap_stones row → the 40 eTrade cells, in HEADERS order. */
const rowToCells = (r) => [
  category(r.category),
  text(r.sku),
  text(r.shape),
  num(r.weight),
  text(r.color),
  text(r.clarity),
  lab(r.lab),
  text(r.fluorescence),
  price(r.price_per_carat),
  num(r.rap_price),
  num(r.rap_list_price),
  price(r.total_price),
  "",
  text(r.image),
  text(r.video),
  text(r.certificate_image),
  text(r.certificate_number),
  text(r.cut),
  text(r.polish),
  text(r.symmetry),
  num(r.table_percent),
  num(r.depth_percent),
  num(r.ratio),
  text(r.measurements),
  text(r.pair_stone),
  text(r.fancy_intensity),
  text(r.fancy_color),
  text(r.fancy_overtone),
  text(r.fancy_color_2),
  text(r.fancy_overtone_2),
  text(r.home_page),
  text(r.additional_pictures),
  branchCode(r.branch),
  text(r.additional_videos),
  text(r.comment),
  text(r.type),
  text(r.cert_comments),
  text(r.certificate_image_jpg),
  text(r.origin),
  "",
];

const csvCell = (v) => {
  const s = String(v ?? "");
  return /[",\r\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
};

/* CRLF line endings, no BOM, trailing newline — the same bytes Barak writes. */
const toCsv = (rows) =>
  [HEADERS, ...rows.map(rowToCells)].map((cells) => cells.map(csvCell).join(",")).join("\r\n") + "\r\n";

/* Named the way Barak names it, in Israel time — the server runs on UTC. */
const fileName = (date = new Date(), timeZone = "Asia/Jerusalem") => {
  const parts = Object.fromEntries(
    new Intl.DateTimeFormat("en-GB", {
      timeZone,
      year: "numeric", month: "2-digit", day: "2-digit",
      hour: "2-digit", minute: "2-digit", second: "2-digit",
      hourCycle: "h23",
    })
      .formatToParts(date)
      .map((p) => [p.type, p.value])
  );
  return `Diamonds${parts.year}${parts.month}${parts.day}_${parts.hour}${parts.minute}${parts.second}.csv`;
};

module.exports = { HEADERS, rowToCells, toCsv, fileName, branchCode, category, lab, price };
