// Barak builds each <Stone> from a comma-split row, so a certificate comment
// that contains commas ("""Clouds, pinpoints and … are not shown.""") is cut
// into several pieces and every later element moves right by one per comma.
// The last elements fall off the end of the row and are lost.

// Element order of the feed from Cert.Comments to the end of the row.
const COMMENT_TAIL = [
  "Cert.Comments", "certificateImageJPG", "Origin", "TradeShow", "GroupingType",
  "Box", "Stones", "cost_per_carat", "Holder", "Location", "JewelryModel",
];

const QUOTE = '"""';

const asText = (v) => {
  if (v == null) return "";
  if (typeof v === "object" && v._ !== undefined) return String(v._);
  return String(v);
};

/** How many extra elements the certificate comment spilled into (0 = intact). */
const commentSpill = (stone) => {
  const first = asText(stone["Cert.Comments"]).trim();
  if (!first.startsWith(QUOTE)) return 0;
  if (first.length >= QUOTE.length * 2 && first.endsWith(QUOTE)) return 0;
  for (let n = 1; n < COMMENT_TAIL.length; n++) {
    if (asText(stone[COMMENT_TAIL[n]]).trim().endsWith(QUOTE)) return n;
  }
  return 0;
};

/**
 * Join a comma-split certificate comment back together and move every later
 * element back to its own field. Elements pushed off the end of the row are
 * unknown and become null. Mutates `stone`; returns the spill count.
 */
const realignCertComments = (stone) => {
  const n = commentSpill(stone);
  if (!n) return 0;
  const values = COMMENT_TAIL.map((k) => stone[k]);
  stone["Cert.Comments"] = values.slice(0, n + 1).map(asText).join(",");
  for (let i = 1; i < COMMENT_TAIL.length; i++) {
    stone[COMMENT_TAIL[i]] = i + n < values.length ? values[i + n] : null;
  }
  return n;
};

module.exports = { COMMENT_TAIL, commentSpill, realignCertComments };
