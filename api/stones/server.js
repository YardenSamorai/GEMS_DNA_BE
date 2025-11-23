const express = require("express");
const cors = require("cors");
const { Pool } = require("pg");
const dotenv = require("dotenv");
const path = require("path");
const CryptoJS = require("crypto-js");

dotenv.config({ path: path.resolve(__dirname, "../../.env") });

const ENCRYPT_SECRET = process.env.ENCRYPT_SECRET;
const app = express();
const port = process.env.PORT;
const dbUrl = process.env.DATABASE_URL;

const pool = new Pool({
  connectionString: dbUrl,
  ssl: { rejectUnauthorized: false },
});

console.log("🟢 Backend is running — This is the correct file.");
app.use(cors());

// 🔐 פונקציית הצפנה
const encrypt = (text) => {
  return CryptoJS.AES.encrypt(text, ENCRYPT_SECRET).toString();
};

/* =========================================================
   /api/stones – כל האבנים מטבלת stones (לא selector)
   ========================================================= */
app.get("/api/stones", async (req, res) => {
  try {
    const result = await pool.query("SELECT * FROM stones ORDER BY carat DESC");

    const formattedRows = result.rows.map((row) => ({
      ...row,
      carat: row.carat ? parseFloat(row.carat) : null,
      ratio: row.ratio ? parseFloat(row.ratio) : null,
      price_per_carat: row.price_per_carat
        ? parseFloat(row.price_per_carat)
        : null,
      total_price: row.total_price ? parseFloat(row.total_price) : null,
      measurements1: row.measurements1 || null,

      // ⭐ גם כאן נחזיר category אם קיים
      category: row.category || "",
    }));

    res.json(formattedRows);
  } catch (error) {
    console.error("❌ Error fetching stones:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

/* =========================================================
   /api/soap-stones – ל-Stone selector החדש
   ========================================================= */
app.get("/api/soap-stones", async (req, res) => {
  try {
    // ⚠️ בלי LIMIT – הכל, הפאגינציה ב-Frontend
    const result = await pool.query(`
      SELECT *
      FROM soap_stones
      WHERE sku IS NOT NULL
      ORDER BY updated_at DESC
    `);

    const stones = result.rows.map((row) => {
      // בחירת תמונה ראשית
      let imageUrl = row.image;
      if (!imageUrl && row.additional_pictures) {
        const first = row.additional_pictures.split(";")[0];
        imageUrl = first ? first.trim() : null;
      }

      return {
        id: row.id,
        sku: row.sku,
        shape: row.shape,

        // ⭐ קטגוריה (Emerald / Diamond / Fancy / Gemstone וכו')
        category: row.category || "",

        // משקל
        weightCt: row.weight ? Number(row.weight) : null,

        // מחירים
        priceTotal:
          row.total_price !== null && row.total_price !== undefined
            ? Number(row.total_price)
            : null,
        pricePerCt:
          row.price_per_carat !== null && row.price_per_carat !== undefined
            ? Number(row.price_per_carat)
            : null,

        // טיפול / Oil / Enhancement (מגיע מה־comment)
        treatment: row.comment || "",

        // מידות ויחס
        measurements: row.measurements || "",
        ratio:
          row.ratio !== undefined &&
          row.ratio !== null &&
          row.ratio !== ""
            ? Number(row.ratio)
            : null,

        // תמונות / וידאו / תעודה
        imageUrl,
        videoUrl: row.video || null,
        certificateUrl: row.certificate_image || row.certificate_url || null,
        certificateNumber: row.certificate_number || "",

        // מאפיינים נוספים
        lab: row.lab || "N/A",
        origin: row.origin || "N/A",
        color: row.color || "",
        clarity: row.clarity || "",
        luster: row.luster || "",
        fluorescence: row.fluorescence || "",
      };
    });

    res.json({ stones });
  } catch (error) {
    console.error("❌ Error fetching soap stones:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

/* =========================================================
   /api/stones/:stone_id – אבן ספציפית
   ========================================================= */
app.get("/api/stones/:stone_id", async (req, res) => {
  console.log("🚨 /api/stones/:stone_id CALLED");
  try {
    const { stone_id } = req.params;
    const result = await pool.query(
      "SELECT * FROM stones WHERE stone_id = $1",
      [stone_id]
    );

    if (result.rows.length === 0) {
      return res.status(404).json({ error: "Stone not found" });
    }

    const stone = result.rows[0];

    // Convert numeric fields
    const numericFields = ["carat", "ratio"];
    numericFields.forEach((field) => {
      if (stone[field] !== null && stone[field] !== undefined) {
        stone[field] = parseFloat(stone[field]);
      }
    });

    // Encrypt prices
    if (stone.price_per_carat !== null && stone.price_per_carat !== undefined) {
      const raw = stone.price_per_carat;
      stone.price_per_carat = encrypt(raw.toString());
      console.log(
        `💸 Encrypted price_per_carat: ${raw} → ${stone.price_per_carat}`
      );
    }

    if (stone.total_price !== null && stone.total_price !== undefined) {
      const raw = stone.total_price;
      stone.total_price = encrypt(raw.toString());
      console.log(`💰 Encrypted total_price: ${raw} → ${stone.total_price}`);
    }

    res.json(stone);
  } catch (error) {
    console.error("❌ Error fetching stone:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

/* =========================================================
   /api/jewelry/:modelNumber – תכשיט + הצפנה
   ========================================================= */
app.get("/api/jewelry/:modelNumber", async (req, res) => {
  const { modelNumber } = req.params;

  try {
    const result = await pool.query(
      "SELECT * FROM jewelry_products WHERE model_number = $1",
      [modelNumber]
    );

    if (result.rows.length === 0) {
      return res.status(404).json({ error: "Jewelry item not found" });
    }

    const item = result.rows[0];

    const numericFields = [
      "jewelry_weight",
      "total_carat",
      "center_stone_carat",
    ];
    numericFields.forEach((field) => {
      if (item[field] !== null && item[field] !== undefined) {
        item[field] = parseFloat(item[field]);
      }
    });

    if (item.price !== null && item.price !== undefined) {
      const originalPrice = item.price;
      item.price = encrypt(item.price.toString());
      console.log("🔐 Encrypted price:", originalPrice, "→", item.price);
    }

    res.json(item);
  } catch (error) {
    console.error("❌ Error fetching jewelry item:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

/* =========================================================
   Start server
   ========================================================= */
app.listen(port, () => {
  console.log(`🚀 Server is running on http://localhost:${port}`);
});
