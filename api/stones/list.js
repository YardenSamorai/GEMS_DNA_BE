// api/stones/list.js
const express = require('express');
const router = express.Router();
const { pool } = require('../../db/client');

/**
 * GET /api/stones
 * מחזיר רשימת אבנים מהטבלה soap_stones בפורמט שמתאים ל-Frontend
 */
router.get('/', async (req, res) => {
  try {
    const limit = Number(req.query.limit) || 300; // אפשר להגביל
    const { rows } = await pool.query(
      `
      SELECT
        id,
        sku,
        shape,
        weight,
        total_price,
        comment,
        measurements,
        image,
        additional_pictures
      FROM soap_stones
      WHERE sku IS NOT NULL
      ORDER BY updated_at DESC
      LIMIT $1
    `,
      [limit]
    );

    const stones = rows.map((row) => {
      // בחירת תמונה
      let imageUrl = row.image;
      if (!imageUrl && row.additional_pictures) {
        const first = row.additional_pictures.split(';')[0];
        imageUrl = first ? first.trim() : null;
      }

      return {
        id: row.id,
        sku: row.sku,
        shape: row.shape,
        weightCt: row.weight ? Number(row.weight) : null,
        priceTotal: row.total_price ? Number(row.total_price) : null,
        treatment: row.comment || "", // כאן מגיע ה-Minor / Insignificant / No oil וכו'
        measurements: row.measurements || "",
        imageUrl,
      };
    });

    res.json({ stones });
  } catch (err) {
    console.error('❌ Error fetching stones:', err);
    res.status(500).json({ error: 'Failed to fetch stones' });
  }
});

module.exports = router;
