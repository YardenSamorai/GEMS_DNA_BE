const express = require("express");
const cors = require("cors");
const { Pool } = require("pg");
const dotenv = require("dotenv");
const path = require("path");
const CryptoJS = require("crypto-js");
const { fetchSoapData } = require("../../utils/soapClient");
const { parseXml } = require("../../utils/xmlParser");

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
app.use(express.json());

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
        location: row.branch || "",  // branch מהDB מוצג כ-Location בUI
        
        // שדות ספציפיים ליהלומים
        cut: row.cut || "",
        polish: row.polish || "",
        symmetry: row.symmetry || "",
        tablePercent: row.table_percent ? Number(row.table_percent) : (row.table ? Number(row.table) : null),
        depthPercent: row.depth_percent ? Number(row.depth_percent) : (row.depth ? Number(row.depth) : null),
        rapPrice: row.rap_price ? Number(row.rap_price) : null,
        
        // שדות ספציפיים ל-Fancy
        fancyIntensity: row.fancy_intensity || "",
        fancyColor: row.fancy_color || "",
        fancyOvertone: row.fancy_overtone || "",
        fancyColor2: row.fancy_color_2 || "",
        fancyOvertone2: row.fancy_overtone_2 || "",
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
   /api/tags – ניהול תגיות לקוחות
   ========================================================= */

// Get all tags
app.get("/api/tags", async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT t.*, COUNT(st.id) as stone_count
      FROM tags t
      LEFT JOIN stone_tags st ON t.id = st.tag_id
      GROUP BY t.id
      ORDER BY t.name ASC
    `);
    res.json(result.rows);
  } catch (error) {
    console.error("❌ Error fetching tags:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

// Create a new tag
app.post("/api/tags", async (req, res) => {
  try {
    const { name, color } = req.body;
    
    if (!name || !name.trim()) {
      return res.status(400).json({ error: "Tag name is required" });
    }

    const result = await pool.query(
      `INSERT INTO tags (name, color) VALUES ($1, $2) RETURNING *`,
      [name.trim(), color || "#10b981"]
    );
    
    res.status(201).json(result.rows[0]);
  } catch (error) {
    if (error.code === "23505") { // unique_violation
      return res.status(409).json({ error: "Tag already exists" });
    }
    console.error("❌ Error creating tag:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

// Update a tag
app.put("/api/tags/:id", async (req, res) => {
  try {
    const { id } = req.params;
    const { name, color } = req.body;
    
    const result = await pool.query(
      `UPDATE tags SET name = COALESCE($1, name), color = COALESCE($2, color) WHERE id = $3 RETURNING *`,
      [name?.trim(), color, id]
    );
    
    if (result.rows.length === 0) {
      return res.status(404).json({ error: "Tag not found" });
    }
    
    res.json(result.rows[0]);
  } catch (error) {
    console.error("❌ Error updating tag:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

// Delete a tag
app.delete("/api/tags/:id", async (req, res) => {
  try {
    const { id } = req.params;
    
    // Delete associated stone_tags first
    await pool.query(`DELETE FROM stone_tags WHERE tag_id = $1`, [id]);
    
    const result = await pool.query(`DELETE FROM tags WHERE id = $1 RETURNING *`, [id]);
    
    if (result.rows.length === 0) {
      return res.status(404).json({ error: "Tag not found" });
    }
    
    res.json({ message: "Tag deleted successfully" });
  } catch (error) {
    console.error("❌ Error deleting tag:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

/* =========================================================
   /api/stone-tags – שיוך תגיות לאבנים
   ========================================================= */

// Get tags for a specific stone
app.get("/api/stones/:sku/tags", async (req, res) => {
  try {
    const { sku } = req.params;
    
    const result = await pool.query(`
      SELECT t.*
      FROM tags t
      INNER JOIN stone_tags st ON t.id = st.tag_id
      WHERE st.stone_sku = $1
      ORDER BY t.name ASC
    `, [sku]);
    
    res.json(result.rows);
  } catch (error) {
    console.error("❌ Error fetching stone tags:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

// Add tag to a stone
app.post("/api/stones/:sku/tags", async (req, res) => {
  try {
    const { sku } = req.params;
    const { tagId } = req.body;
    
    if (!tagId) {
      return res.status(400).json({ error: "Tag ID is required" });
    }

    // Check if already exists
    const existing = await pool.query(
      `SELECT * FROM stone_tags WHERE stone_sku = $1 AND tag_id = $2`,
      [sku, tagId]
    );
    
    if (existing.rows.length > 0) {
      return res.status(409).json({ error: "Tag already assigned to this stone" });
    }

    const result = await pool.query(
      `INSERT INTO stone_tags (stone_sku, tag_id) VALUES ($1, $2) RETURNING *`,
      [sku, tagId]
    );
    
    // Return the full tag info
    const tag = await pool.query(`SELECT * FROM tags WHERE id = $1`, [tagId]);
    
    res.status(201).json(tag.rows[0]);
  } catch (error) {
    console.error("❌ Error adding tag to stone:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

// Remove tag from a stone
app.delete("/api/stones/:sku/tags/:tagId", async (req, res) => {
  try {
    const { sku, tagId } = req.params;
    
    const result = await pool.query(
      `DELETE FROM stone_tags WHERE stone_sku = $1 AND tag_id = $2 RETURNING *`,
      [sku, tagId]
    );
    
    if (result.rows.length === 0) {
      return res.status(404).json({ error: "Tag assignment not found" });
    }
    
    res.json({ message: "Tag removed from stone" });
  } catch (error) {
    console.error("❌ Error removing tag from stone:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

// Get all stones with a specific tag
app.get("/api/tags/:id/stones", async (req, res) => {
  try {
    const { id } = req.params;
    
    const result = await pool.query(`
      SELECT ss.*
      FROM soap_stones ss
      INNER JOIN stone_tags st ON ss.sku = st.stone_sku
      WHERE st.tag_id = $1
      ORDER BY ss.weight DESC
    `, [id]);
    
    res.json(result.rows);
  } catch (error) {
    console.error("❌ Error fetching stones by tag:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

// Get all stone-tag mappings (for frontend to load all at once)
app.get("/api/stone-tags", async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT st.stone_sku, t.*
      FROM stone_tags st
      INNER JOIN tags t ON st.tag_id = t.id
      ORDER BY st.stone_sku
    `);
    
    // Group by stone_sku
    const grouped = {};
    result.rows.forEach(row => {
      if (!grouped[row.stone_sku]) {
        grouped[row.stone_sku] = [];
      }
      grouped[row.stone_sku].push({
        id: row.id,
        name: row.name,
        color: row.color
      });
    });
    
    res.json(grouped);
  } catch (error) {
    console.error("❌ Error fetching stone tags:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

/* =========================================================
   /api/sync - Sync SOAP data to database
   ========================================================= */
app.post("/api/sync", async (req, res) => {
  console.log("🔄 Starting SOAP sync...");
  
  try {
    // Step 1: Fetch SOAP data
    console.log("🚀 [1/5] Fetching SOAP data...");
    const rawXml = await fetchSoapData();
    
    if (!rawXml) {
      return res.status(500).json({ success: false, error: "No XML received from SOAP API" });
    }

    // Step 2: Parse XML
    console.log("📦 [2/5] Parsing XML...");
    const parsed = await parseXml(rawXml);
    const stones = parsed?.Stock?.Stone;

    if (!stones) {
      return res.status(500).json({ success: false, error: "No stones found in XML" });
    }

    const stoneArray = Array.isArray(stones) ? stones : [stones];
    console.log(`📊 [3/5] Total stones found: ${stoneArray.length}`);

    // Step 3: Build values for each row
    const safeNumber = (value) => {
      const n = parseFloat(value);
      return Number.isFinite(n) ? n : null;
    };

    const values = stoneArray.map((stone) => {
      const pricePerCarat = safeNumber(stone.PricePerCarat);
      const totalPrice = safeNumber(stone.TotalPrice);

      return [
        stone.Category || null,
        stone.SKU || null,
        stone.Shape || null,
        safeNumber(stone.Weight),
        stone.Color || null,
        stone.Clarity || null,
        stone.Lab || null,
        stone.Fluorescence || null,
        pricePerCarat !== null ? pricePerCarat * 2 : null,
        safeNumber(stone.RapPrice),
        safeNumber(stone["Rap.Price"]),
        totalPrice !== null ? totalPrice * 2 : null,
        stone.Location || null,
        stone.Branch || null,
        stone.Image || null,
        stone.additional_pictures || null,
        stone.Video || null,
        stone.additional_videos || null,
        stone.Certificateimage || null,
        stone.CertificateNumber || null,
        stone.certificateImageJPG || null,
        stone.Cut || null,
        stone.Polish || null,
        stone.Symmetry || null,
        safeNumber(stone.Table),
        safeNumber(stone.Depth),
        safeNumber(stone.ratio),
        stone["Measurements-delimiter"] || null,
        stone.fancy_intensity || null,
        stone.fancy_color || null,
        stone.fancy_overtone || null,
        stone.fancy_color_2 || null,
        stone.fancy_overtone_2 || null,
        stone.PairStone || null,
        stone.home_page || null,
        stone.TradeShow || null,
        stone.Comment || null,
        stone.Type || null,
        stone["Cert.Comments"] || null,
        stone.Origin || null,
        null,
      ];
    });

    const columns = [
      "category", "sku", "shape", "weight", "color", "clarity", "lab",
      "fluorescence", "price_per_carat", "rap_price", "rap_list_price",
      "total_price", "location", "branch", "image", "additional_pictures",
      "video", "additional_videos", "certificate_image", "certificate_number",
      "certificate_image_jpg", "cut", "polish", "symmetry", "table_percent",
      "depth_percent", "ratio", "measurements", "fancy_intensity",
      "fancy_color", "fancy_overtone", "fancy_color_2", "fancy_overtone_2",
      "pair_stone", "home_page", "trade_show", "comment", "type",
      "cert_comments", "origin", "raw_xml",
    ];

    // Step 4: Clear table and insert data
    console.log("🧹 [4/5] Clearing soap_stones table...");
    await pool.query("TRUNCATE TABLE soap_stones RESTART IDENTITY");

    console.log("🧱 [5/5] Inserting data in chunks...");
    const CHUNK_SIZE = 300;
    
    for (let i = 0; i < values.length; i += CHUNK_SIZE) {
      const chunk = values.slice(i, i + CHUNK_SIZE);

      const placeholders = chunk
        .map((row, rowIndex) => {
          const base = rowIndex * columns.length;
          return `(${columns.map((_, colIndex) => `$${base + colIndex + 1}`).join(", ")})`;
        })
        .join(", ");

      const insertQuery = `INSERT INTO soap_stones (${columns.join(", ")}) VALUES ${placeholders}`;
      const flatValues = chunk.flat();

      await pool.query(insertQuery, flatValues);
      console.log(`➡️ Inserted chunk ${Math.floor(i / CHUNK_SIZE) + 1}`);
    }

    console.log(`🎉 Sync complete! Inserted ${stoneArray.length} stones.`);
    
    res.json({
      success: true,
      message: `Successfully synced ${stoneArray.length} stones`,
      count: stoneArray.length,
      timestamp: new Date().toISOString()
    });

  } catch (error) {
    console.error("❌ Sync error:", error);
    res.status(500).json({ 
      success: false, 
      error: error.message || "Sync failed" 
    });
  }
});

/* =========================================================
   Start server
   ========================================================= */
app.listen(port, () => {
  console.log(`🚀 Server is running on http://localhost:${port}`);
});
