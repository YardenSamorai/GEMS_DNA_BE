/* =============================================================================
 * DNA PHOTOS
 * =============================================================================
 *
 * A Photo Station shot a manager approved is a DNA photo of the item. It is
 * kept in stone_photo_captures (status 'approved', dna_image_url) rather than
 * in the catalog tables, because every Barak sync rebuilds soap_stones and
 * rewrites jewelry_products.all_pictures_link.
 *
 * applyDnaPhotos appends those photos to the item's gallery after each import
 * and right after an approval. Barak's pictures stay first, so when the item
 * already has a Barak photo it remains the main image.
 *
 * A photo that was also sent to Barak comes back from the feed once Barak's
 * Picture Mapping links it (as <ftp_filename>). From then on the DNA copy is
 * skipped so the gallery does not show the same shot twice.
 * ========================================================================== */

const TARGETS = {
  stone: { table: "soap_stones", key: "sku", gallery: "additional_pictures", all: "concat_ws(';', x.image, x.additional_pictures)" },
  jewelry: { table: "jewelry_products", key: "model_number", gallery: "all_pictures_link", all: "COALESCE(x.all_pictures_link, '')" },
};

const ITEM_TYPES = Object.keys(TARGETS);

async function applyDnaPhotos(pool, itemType, sku = null) {
  const t = TARGETS[itemType];
  if (!t) throw new Error(`Unknown item type: ${itemType}`);
  const res = await pool.query(
    `UPDATE ${t.table} AS i
        SET ${t.gallery} = concat_ws(';', NULLIF(i.${t.gallery}, ''), m.urls)
       FROM (
         SELECT x.${t.key} AS item_key,
                string_agg(c.dna_image_url, ';' ORDER BY c.reviewed_at, c.id) AS urls
           FROM stone_photo_captures c
           JOIN ${t.table} x ON x.${t.key} = c.sku
          WHERE c.status = 'approved'
            AND c.item_type = $1
            AND c.dna_image_url IS NOT NULL
            AND ($2::text IS NULL OR c.sku = $2)
            AND strpos(COALESCE(x.${t.gallery}, ''), c.dna_image_url) = 0
            AND (c.ftp_filename IS NULL
                 OR strpos(${t.all}, left(c.ftp_filename, length(c.ftp_filename) - 4)) = 0)
          GROUP BY x.${t.key}
       ) m
      WHERE i.${t.key} = m.item_key`,
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
