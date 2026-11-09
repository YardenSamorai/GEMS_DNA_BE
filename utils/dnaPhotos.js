/* =============================================================================
 * DNA PHOTOS
 * =============================================================================
 *
 * A Photo Station shot a manager approved is a DNA photo of the item. It is
 * kept in stone_photo_captures (status 'approved', dna_image_url) rather than
 * in the catalog tables, because every Barak sync rebuilds soap_stones and
 * rewrites jewelry_products.all_pictures_link.
 *
 * applyDnaPhotos rebuilds the DNA part of the item's gallery after each import
 * and right after an approval: Barak's pictures first, then the DNA photos,
 * newest first. When the item has a Barak photo it stays the main image;
 * when it has none, the latest approved shot is.
 *
 * A photo that was also sent to Barak comes back from the feed once Barak's
 * Picture Mapping links it (as <ftp_filename>). From then on the DNA copy is
 * skipped so the gallery does not show the same shot twice.
 * ========================================================================== */

const TARGETS = {
  stone: { table: "soap_stones", key: "sku", gallery: "additional_pictures", barak: "concat_ws(';', x.image, b.base)", group: ", x.image" },
  jewelry: { table: "jewelry_products", key: "model_number", gallery: "all_pictures_link", barak: "COALESCE(b.base, '')", group: "" },
};

const ITEM_TYPES = Object.keys(TARGETS);

async function applyDnaPhotos(pool, itemType, sku = null) {
  const t = TARGETS[itemType];
  if (!t) throw new Error(`Unknown item type: ${itemType}`);
  const res = await pool.query(
    `UPDATE ${t.table} AS i
        SET ${t.gallery} = NULLIF(concat_ws(';', m.base, m.urls), '')
       FROM (
         SELECT x.${t.key} AS item_key, b.base,
                string_agg(c.dna_image_url, ';' ORDER BY c.reviewed_at DESC NULLS LAST, c.id DESC)
                  FILTER (WHERE c.status = 'approved'
                            AND (c.ftp_filename IS NULL
                                 OR strpos(${t.barak}, left(c.ftp_filename, length(c.ftp_filename) - 4)) = 0)) AS urls
           FROM ${t.table} x
           JOIN stone_photo_captures c
             ON c.sku = x.${t.key} AND c.item_type = $1 AND c.dna_image_url IS NOT NULL
           CROSS JOIN LATERAL (
             SELECT string_agg(btrim(p), ';' ORDER BY n) AS base
               FROM unnest(string_to_array(COALESCE(x.${t.gallery}, ''), ';')) WITH ORDINALITY AS g(p, n)
              WHERE btrim(p) <> ''
                AND NOT EXISTS (SELECT 1 FROM stone_photo_captures d
                                 WHERE d.item_type = $1 AND d.dna_image_url = btrim(p))
           ) b
          WHERE ($2::text IS NULL OR x.${t.key} = $2)
          GROUP BY x.${t.key}, b.base${t.group}
       ) m
      WHERE i.${t.key} = m.item_key
        AND i.${t.gallery} IS DISTINCT FROM NULLIF(concat_ws(';', m.base, m.urls), '')`,
    [itemType, sku]
  );
  return res.rowCount || 0;
}

/* After an import: never fails the sync, only reports. */
async function reapplyDnaPhotos(pool, itemType) {
  try {
    const n = await applyDnaPhotos(pool, itemType);
    if (n) console.log(`📷 Re-attached DNA photos to ${n} ${itemType === "stone" ? "stone(s)" : "jewelry item(s)"}`);
    return n;
  } catch (e) {
    console.warn(`⚠️  Could not re-attach DNA photos (${itemType}):`, e.message);
    return 0;
  }
}

module.exports = { ITEM_TYPES, applyDnaPhotos, reapplyDnaPhotos };
