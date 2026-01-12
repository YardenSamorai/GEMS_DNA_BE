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

/* =========================================================
   Encryption helper
   ========================================================= */
const encrypt = (text) => {
  return CryptoJS.AES.encrypt(text, ENCRYPT_SECRET).toString();
};

/* =========================================================
   /api/stones – כל האבנים מטבלת soap_stones (מיפוי לפורמט הישן)
   ========================================================= */
app.get("/api/stones", async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT * FROM soap_stones 
      WHERE sku IS NOT NULL 
      ORDER BY weight DESC
    `);

    // מיפוי מ-soap_stones לפורמט של stones (לתאימות עם Frontend)
    const formattedRows = result.rows.map((row) => ({
      id: row.id,
      stone_id: row.sku,                    // sku → stone_id
      carat: row.weight ? parseFloat(row.weight) : null,  // weight → carat
      clarity: row.clarity || null,
      shape: row.shape || null,
      lab: row.lab || null,
      origin: row.origin || null,
      ratio: row.ratio ? parseFloat(row.ratio) : null,
      price_per_carat: row.price_per_carat ? parseFloat(row.price_per_carat) : null,
      total_price: row.total_price ? parseFloat(row.total_price) : null,
      measurements1: row.measurements || null,  // measurements → measurements1
      certificate_number: row.certificate_number || null,
      cert_image: row.certificate_image || null,  // certificate_image → cert_image
      video: row.video || null,
      cert_pdf: null,  // לא קיים ב-soap_stones
      category: row.category || "",
      // שדות נוספים מ-soap_stones שאולי יעזרו ל-Frontend
      color: row.color || null,
      cut: row.cut || null,
      polish: row.polish || null,
      symmetry: row.symmetry || null,
      fluorescence: row.fluorescence || null,
      image: row.image || null,
      comment: row.comment || null,
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
   /api/stones/:stone_id – אבן ספציפית מ-soap_stones (לפי SKU)
   ========================================================= */
app.get("/api/stones/:stone_id", async (req, res) => {
  console.log("🚨 /api/stones/:stone_id CALLED");
  try {
    const { stone_id } = req.params;
    const result = await pool.query(
      "SELECT * FROM soap_stones WHERE sku = $1",
      [stone_id]
    );

    if (result.rows.length === 0) {
      return res.status(404).json({ error: "Stone not found" });
    }

    const row = result.rows[0];

    // חילוץ מספר תעודה מה-URL (למשל: 2024-087017 מתוך .../2024-087017.pdf)
    let certificateNumber = row.certificate_number || null;
    if (!certificateNumber && row.certificate_image) {
      const match = row.certificate_image.match(/\/([^\/]+)\.pdf$/i);
      if (match) {
        certificateNumber = match[1];
      }
    }

    // מיפוי מ-soap_stones לפורמט של stones (לתאימות עם Frontend)
    const stone = {
      id: row.id,
      stone_id: row.sku,                    // sku → stone_id
      carat: row.weight ? parseFloat(row.weight) : null,  // weight → carat
      clarity: row.clarity || null,
      shape: row.shape || null,
      lab: row.lab || null,
      origin: row.origin || null,
      ratio: row.ratio ? parseFloat(row.ratio) : null,
      measurements1: row.measurements || null,  // measurements → measurements1
      certificate_number: certificateNumber,   // מספר תעודה (מחולץ מה-URL אם צריך)
      cert_image: row.certificate_image || null,  // certificate_image → cert_image
      video: row.video || null,
      cert_pdf: null,  // לא קיים ב-soap_stones
      category: row.category || "",
      // שדות נוספים מ-soap_stones
      color: row.color || null,
      cut: row.cut || null,
      polish: row.polish || null,
      symmetry: row.symmetry || null,
      fluorescence: row.fluorescence || null,
      image: row.image || null,
      picture: row.image || null,  // Frontend מצפה ל-picture
      comment: row.comment || null,
    };

    // Encrypt prices
    if (row.price_per_carat !== null && row.price_per_carat !== undefined) {
      const raw = row.price_per_carat;
      stone.price_per_carat = encrypt(raw.toString());
      console.log(`💸 Encrypted price_per_carat: ${raw} → ${stone.price_per_carat}`);
    } else {
      stone.price_per_carat = null;
    }

    if (row.total_price !== null && row.total_price !== undefined) {
      const raw = row.total_price;
      stone.total_price = encrypt(raw.toString());
      console.log(`💰 Encrypted total_price: ${raw} → ${stone.total_price}`);
    } else {
      stone.total_price = null;
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
