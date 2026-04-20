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
app.use(express.json({ limit: '50mb' }));

/* =========================================================
   Encryption helper
   ========================================================= */
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
        location: row.branch || null,  // branch מהDB מוצג כ-Location בUI (already mapped)
        
        // Diamond specific fields (camelCase for frontend)
        cut: row.cut || "",
        polish: row.polish || "",
        symmetry: row.symmetry || "",
        tablePercent: row.table_percent !== null && row.table_percent !== undefined ? Number(row.table_percent) : null,
        depthPercent: row.depth_percent !== null && row.depth_percent !== undefined ? Number(row.depth_percent) : null,
        rapPrice: row.rap_price !== null && row.rap_price !== undefined ? Number(row.rap_price) : null,
        
        // Fancy diamond specific fields (camelCase for frontend)
        fancyIntensity: row.fancy_intensity || "",
        fancyColor: row.fancy_color || "",
        fancyOvertone: row.fancy_overtone || "",
        fancyColor2: row.fancy_color_2 || "",
        fancyOvertone2: row.fancy_overtone_2 || "",
        
        // Pair stone
        pairSku: row.pair_stone || null,
        
        // Grouping
        groupingType: row.grouping_type || "",
        box: row.box || "",
        stones: row.stones != null ? Number(row.stones) : null,
      };
    });

    res.json({ stones });
  } catch (error) {
    console.error("❌ Error fetching soap stones:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

/* =========================================================
   /api/stones/:stone_id – אבן ספציפית (מ-soap_stones)
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
    
    // Select main image
    let imageUrl = row.image;
    if (!imageUrl && row.additional_pictures) {
      const first = row.additional_pictures.split(";")[0];
      imageUrl = first ? first.trim() : null;
    }

    // Extract certificate number from URL if not provided
    let certNumber = row.certificate_number || null;
    if (!certNumber && row.certificate_image) {
      // Extract from URL like: https://app.barakdiamonds.com/Gemstones/output/Certificates/2023-107020.pdf
      const match = row.certificate_image.match(/\/([^\/]+)\.pdf$/i);
      if (match) {
        certNumber = match[1];
      }
    }

    // Map to frontend format (compatible with old format)
    const stone = {
      id: row.id,
      stone_id: row.sku,
      sku: row.sku,
      category: row.category || null, // For determining stone type
      shape: row.shape || null,
      carat: row.weight ? parseFloat(row.weight) : null,
      clarity: row.clarity || null,
      color: row.color || null,
      lab: row.lab || null,
      origin: row.origin || null,
      ratio: row.ratio ? parseFloat(row.ratio) : null,
      measurements1: row.measurements || null,
      picture: imageUrl,
      video: row.video || null,
      certificate_number: certNumber,
      certificate_url: row.certificate_image || null,
      treatment: row.comment || null, // For emeralds
      
      // Diamond-specific fields
      cut: row.cut || null,
      polish: row.polish || null,
      symmetry: row.symmetry || null,
      table_percent: row.table_percent ? parseFloat(row.table_percent) : null,
      depth_percent: row.depth_percent ? parseFloat(row.depth_percent) : null,
      fluorescence: row.fluorescence || null,
      rap_price: row.rap_price ? parseFloat(row.rap_price) : null,
      
      // Fancy-specific fields
      fancy_intensity: row.fancy_intensity || null,
      fancy_color: row.fancy_color || null,
      fancy_overtone: row.fancy_overtone || null,
      fancy_color_2: row.fancy_color_2 || null,
      fancy_overtone_2: row.fancy_overtone_2 || null,
      
      // Pair stone
      pair_stone: row.pair_stone || null,
      
      // Prices (will be encrypted below)
      price_per_carat: row.price_per_carat ? parseFloat(row.price_per_carat) : null,
      total_price: row.total_price ? parseFloat(row.total_price) : null,
    };

    // Encrypt prices
    if (stone.price_per_carat !== null && stone.price_per_carat !== undefined) {
      const raw = stone.price_per_carat;
      stone.price_per_carat = encrypt(raw.toString());
    }

    if (stone.total_price !== null && stone.total_price !== undefined) {
      const raw = stone.total_price;
      stone.total_price = encrypt(raw.toString());
    }

    res.json(stone);
  } catch (error) {
    console.error("❌ Error fetching stone:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

/* =========================================================
   /api/jewelry – all jewelry items (inventory list)
   ========================================================= */
app.get("/api/jewelry", async (req, res) => {
  try {
    const result = await pool.query("SELECT * FROM jewelry_products ORDER BY model_number ASC");
    const items = result.rows.map(row => ({
      model_number: row.model_number,
      stock_number: row.stock_number,
      jewelry_type: row.jewelry_type,
      style: row.style,
      collection: row.collection,
      price: row.price !== null ? parseFloat(row.price) : null,
      video_link: row.video_link,
      all_pictures_link: row.all_pictures_link,
      certificate_link: row.certificate_link,
      certificate_number: row.certificate_number,
      title: row.title,
      description: row.description,
      jewelry_weight: row.jewelry_weight !== null ? parseFloat(row.jewelry_weight) : null,
      total_carat: row.total_carat !== null ? parseFloat(row.total_carat) : null,
      stone_type: row.stone_type,
      center_stone_carat: row.center_stone_carat !== null ? parseFloat(row.center_stone_carat) : null,
      center_stone_shape: row.center_stone_shape,
      center_stone_color: row.center_stone_color,
      center_stone_clarity: row.center_stone_clarity,
      metal_type: row.metal_type,
      currency: row.currency,
      availability: row.availability,
      shipping_from: row.shipping_from,
      category: row.category,
      full_description: row.full_description,
      jewelry_size: row.jewelry_size,
      instructions_main: row.instructions_main,
    }));
    res.json({ jewelry: items });
  } catch (error) {
    console.error("Error fetching jewelry:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

/* =========================================================
   /api/jewelry/import-csv – Upload jewelry CSV
   ========================================================= */
let jewelryImportProgress = { active: false, phase: 'idle', progress: 0, detail: '', total: 0, processed: 0 };

app.get("/api/jewelry/import-csv/progress", (req, res) => {
  res.json(jewelryImportProgress);
});

app.post("/api/jewelry/import-csv", async (req, res) => {
  if (jewelryImportProgress.active) {
    return res.status(409).json({ success: false, error: "A jewelry import is already in progress" });
  }

  try {
    const { csvContent } = req.body;
    if (!csvContent) {
      return res.status(400).json({ success: false, error: "No CSV content provided" });
    }

    console.log("Jewelry CSV import requested");
    jewelryImportProgress = { active: true, phase: 'parsing', progress: 10, detail: 'Parsing CSV...', total: 0, processed: 0 };

    const rows = parseCsv(csvContent, { columns: true, skip_empty_lines: true, relax_quotes: true, relax_column_count: true });
    console.log(`Parsed ${rows.length} jewelry rows from CSV`);
    jewelryImportProgress = { ...jewelryImportProgress, phase: 'processing', progress: 20, detail: `Parsed ${rows.length} items`, total: rows.length };

    const columns = [
      'model_number','stock_number','jewelry_type','style','collection',
      'price','video_link','all_pictures_link','certificate_link','certificate_number',
      'title','description','jewelry_weight','total_carat','stone_type',
      'center_stone_carat','center_stone_shape','center_stone_color','center_stone_clarity',
      'metal_type','currency','availability','shipping_from','category',
      'full_description','jewelry_size','instructions_main'
    ];

    const values = rows.map(r => [
      r['Model Number'] || null,
      r['Stock Number'] || null,
      r['Jewelry Type'] || null,
      r['Style'] || null,
      r['Collection'] || null,
      csvSafeNum(r['Price']),
      r['Video_Link'] || null,
      r['All_Pictures_Link'] || null,
      r['Certificate_Link'] || null,
      r['Certificate Number'] || null,
      r['Title'] || null,
      r['Description'] || null,
      csvSafeNum(r['Jewelry_Weight']),
      csvSafeNum(r['Total_Carat']),
      (r['Stone_Type'] || '').trim() || null,
      csvSafeNum(r['Center_Stone_Carat']),
      (r['Center_Stone_Shape'] || '').trim() || null,
      (r['Center_Stone_Color'] || '').trim() || null,
      (r['Center_Stone_Clarity'] || '').trim() || null,
      r['Metal_Type'] || null,
      r['Currency'] || null,
      r['Availability'] || null,
      r['Shipping_From'] || null,
      r['Category'] || null,
      r['full_description'] || null,
      r['jewelry_size'] || null,
      r['Instructions_main'] || null,
    ]).filter(v => v[0] !== null);

    jewelryImportProgress = { ...jewelryImportProgress, phase: 'clearing', progress: 40, detail: 'Preparing database...' };

    await pool.query(`
      CREATE TABLE IF NOT EXISTS jewelry_products (
        model_number VARCHAR(50) PRIMARY KEY,
        stock_number VARCHAR(50),
        jewelry_type VARCHAR(50),
        style VARCHAR(50),
        collection VARCHAR(100),
        price NUMERIC(10,2),
        video_link TEXT,
        all_pictures_link TEXT,
        certificate_link TEXT,
        certificate_number VARCHAR(100),
        title VARCHAR(250),
        description TEXT,
        jewelry_weight NUMERIC(10,2),
        total_carat NUMERIC(10,3),
        stone_type VARCHAR(50),
        center_stone_carat NUMERIC(10,3),
        center_stone_shape VARCHAR(50),
        center_stone_color VARCHAR(50),
        center_stone_clarity VARCHAR(50),
        metal_type VARCHAR(50),
        currency VARCHAR(10),
        availability VARCHAR(50),
        shipping_from VARCHAR(100),
        category VARCHAR(100),
        full_description TEXT,
        jewelry_size VARCHAR(50),
        instructions_main TEXT
      );
    `);

    await pool.query('DELETE FROM jewelry_products');

    jewelryImportProgress = { ...jewelryImportProgress, phase: 'inserting', progress: 50, detail: 'Saving jewelry to database...' };
    const CHUNK = 100;
    const totalChunks = Math.ceil(values.length / CHUNK);
    for (let i = 0; i < values.length; i += CHUNK) {
      const chunk = values.slice(i, i + CHUNK);
      const chunkIdx = Math.floor(i / CHUNK) + 1;
      const ph = chunk.map((row, ri) =>
        '(' + columns.map((_, ci) => '$' + (ri * columns.length + ci + 1)).join(',') + ')'
      ).join(',');
      await pool.query('INSERT INTO jewelry_products (' + columns.join(',') + ') VALUES ' + ph + ' ON CONFLICT (model_number) DO UPDATE SET ' +
        columns.slice(1).map(c => `${c} = EXCLUDED.${c}`).join(', '),
        chunk.flat()
      );
      const pct = 50 + Math.round((chunkIdx / totalChunks) * 45);
      jewelryImportProgress = { ...jewelryImportProgress, progress: pct, processed: Math.min(i + CHUNK, values.length), detail: `Inserted ${Math.min(i + CHUNK, values.length)} / ${values.length} items` };
    }

    jewelryImportProgress = { active: false, phase: 'complete', progress: 100, detail: `Successfully imported ${values.length} jewelry items!`, total: values.length, processed: values.length };
    console.log(`Jewelry CSV import completed: ${values.length} items`);

    res.json({ success: true, count: values.length, status: "completed" });
  } catch (error) {
    console.error("Jewelry CSV import error:", error);
    jewelryImportProgress = { active: false, phase: 'error', progress: 0, detail: error.message, total: 0, processed: 0 };
    res.status(500).json({ success: false, error: error.message });
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
   /api/tags - Get all tags
   ========================================================= */
app.get("/api/tags", async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT t.*, COUNT(st.tag_id) as stone_count
      FROM tags t
      LEFT JOIN stone_tags st ON t.id = st.tag_id
      GROUP BY t.id
      ORDER BY t.created_at DESC
    `);
    res.json(result.rows);
  } catch (error) {
    console.error("❌ Error fetching tags:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

/* =========================================================
   /api/tags - Create a new tag
   ========================================================= */
app.post("/api/tags", async (req, res) => {
  try {
    const { name, color } = req.body;
    
    if (!name || !name.trim()) {
      return res.status(400).json({ error: "Tag name is required" });
    }

    const result = await pool.query(
      "INSERT INTO tags (name, color) VALUES ($1, $2) RETURNING *",
      [name.trim(), color || "#10b981"]
    );
    
    res.status(201).json(result.rows[0]);
  } catch (error) {
    if (error.code === '23505') { // Unique violation
      return res.status(400).json({ error: "Tag name already exists" });
    }
    console.error("❌ Error creating tag:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

/* =========================================================
   /api/tags/:id - Update a tag
   ========================================================= */
app.put("/api/tags/:id", async (req, res) => {
  try {
    const { id } = req.params;
    const { name, color } = req.body;
    
    if (!name || !name.trim()) {
      return res.status(400).json({ error: "Tag name is required" });
    }

    const result = await pool.query(
      "UPDATE tags SET name = $1, color = $2 WHERE id = $3 RETURNING *",
      [name.trim(), color || "#10b981", id]
    );
    
    if (result.rows.length === 0) {
      return res.status(404).json({ error: "Tag not found" });
    }
    
    res.json(result.rows[0]);
  } catch (error) {
    if (error.code === '23505') { // Unique violation
      return res.status(400).json({ error: "Tag name already exists" });
    }
    console.error("❌ Error updating tag:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

/* =========================================================
   /api/tags/:id - Delete a tag
   ========================================================= */
app.delete("/api/tags/:id", async (req, res) => {
  try {
    const { id } = req.params;
    
    // Delete all stone associations first
    await pool.query("DELETE FROM stone_tags WHERE tag_id = $1", [id]);
    
    // Then delete the tag
    const result = await pool.query("DELETE FROM tags WHERE id = $1 RETURNING *", [id]);
    
    if (result.rows.length === 0) {
      return res.status(404).json({ error: "Tag not found" });
    }
    
    res.json({ success: true, message: "Tag deleted successfully" });
  } catch (error) {
    console.error("❌ Error deleting tag:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

/* =========================================================
   /api/stone-tags - Get all stone tags (grouped by stone SKU)
   ========================================================= */
app.get("/api/stone-tags", async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT st.stone_sku, t.id, t.name, t.color
      FROM stone_tags st
      JOIN tags t ON st.tag_id = t.id
      ORDER BY st.stone_sku, t.name
    `);
    
    // Group by stone SKU
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
   /api/stones/:sku/tags - Add tag to a stone
   ========================================================= */
app.post("/api/stones/:sku/tags", async (req, res) => {
  try {
    const { sku } = req.params;
    const { tagId } = req.body;
    
    if (!tagId) {
      return res.status(400).json({ error: "Tag ID is required" });
    }

    // Check if association already exists
    const existing = await pool.query(
      "SELECT * FROM stone_tags WHERE stone_sku = $1 AND tag_id = $2",
      [sku, tagId]
    );
    
    if (existing.rows.length > 0) {
      return res.status(400).json({ error: "Tag already associated with this stone" });
    }

    const result = await pool.query(
      "INSERT INTO stone_tags (stone_sku, tag_id) VALUES ($1, $2) RETURNING *",
      [sku, tagId]
    );
    
    // Get the tag details
    const tagResult = await pool.query("SELECT * FROM tags WHERE id = $1", [tagId]);
    
    res.status(201).json(tagResult.rows[0]);
  } catch (error) {
    console.error("❌ Error adding tag to stone:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

/* =========================================================
   /api/stones/:sku/tags/:tagId - Remove tag from a stone
   ========================================================= */
app.delete("/api/stones/:sku/tags/:tagId", async (req, res) => {
  try {
    const { sku, tagId } = req.params;
    
    const result = await pool.query(
      "DELETE FROM stone_tags WHERE stone_sku = $1 AND tag_id = $2 RETURNING *",
      [sku, tagId]
    );
    
    if (result.rows.length === 0) {
      return res.status(404).json({ error: "Tag association not found" });
    }
    
    res.json({ success: true, message: "Tag removed from stone" });
  } catch (error) {
    console.error("❌ Error removing tag from stone:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

/* =========================================================
   /api/sync – Trigger SOAP data sync (with progress tracking)
   ========================================================= */
const { run: runSoapImport } = require('./importFromSoap');

// In-memory sync progress state
let syncProgress = {
  active: false,
  phase: 'idle',
  progress: 0,
  detail: '',
  totalStones: 0,
  processedStones: 0,
  startedAt: null,
};

app.get("/api/sync/progress", (req, res) => {
  res.json(syncProgress);
});

app.post("/api/sync", async (req, res) => {
  if (syncProgress.active) {
    return res.status(409).json({ 
      success: false, 
      error: "A sync is already in progress",
      progress: syncProgress 
    });
  }

  try {
    console.log("🔄 SOAP sync requested via API");
    
    syncProgress = {
      active: true,
      phase: 'starting',
      progress: 0,
      detail: 'Starting sync...',
      totalStones: 0,
      processedStones: 0,
      startedAt: Date.now(),
    };

    const onProgress = (update) => {
      syncProgress = { ...syncProgress, ...update };
    };

    // Run the import directly (not via exec) using the server's db pool
    // closePool: false so we don't kill the server's connection pool
    const result = await runSoapImport({ dbPool: pool, closePool: false, onProgress });
    
    if (result.success) {
      console.log(`✅ Sync completed: ${result.count} stones`);
      syncProgress = { 
        ...syncProgress, 
        active: false, 
        phase: 'complete', 
        progress: 100, 
        detail: `Successfully synced ${result.count} stones!`,
        processedStones: result.count,
      };
      res.json({ 
        success: true, 
        message: result.message,
        count: result.count,
        status: "completed"
      });
    } else {
      console.error("❌ Sync failed:", result.message);
      syncProgress = { 
        ...syncProgress, 
        active: false, 
        phase: 'error', 
        progress: 0, 
        detail: result.message 
      };
      res.status(500).json({ 
        success: false, 
        error: result.message,
        status: "failed"
      });
    }
  } catch (error) {
    console.error("❌ Error during sync:", error);
    syncProgress = { 
      ...syncProgress, 
      active: false, 
      phase: 'error', 
      progress: 0, 
      detail: error.message 
    };
    res.status(500).json({ 
      success: false, 
      error: error.message 
    });
  }
});

/* =========================================================
   /api/image-proxy – Proxy for loading images (bypass CORS)
   ========================================================= */
const fetch = require('node-fetch');

app.get("/api/image-proxy", async (req, res) => {
  try {
    const { url } = req.query;
    
    if (!url) {
      return res.status(400).json({ error: "URL parameter required" });
    }

    console.log("📷 Proxying image:", url);

    // Fetch the image from the external URL
    const response = await fetch(url, {
      timeout: 15000,
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'image/*,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
      }
    });

    if (!response.ok) {
      console.log("❌ Image fetch failed:", response.status);
      return res.status(response.status).json({ error: "Failed to fetch image" });
    }

    // Get content type
    const contentType = response.headers.get('content-type') || 'image/jpeg';
    
    // Get the image buffer
    const buffer = await response.buffer();
    
    // Convert to base64
    const base64 = buffer.toString('base64');
    const dataUri = `data:${contentType};base64,${base64}`;
    
    console.log("✅ Image proxied successfully, size:", buffer.length);
    res.json({ image: dataUri });
  } catch (error) {
    console.error("❌ Error proxying image:", error.message);
    res.status(500).json({ error: "Failed to proxy image: " + error.message });
  }
});

/* =========================================================
   /api/import-csv – Import stones from CSV file upload
   ========================================================= */
const { parse: parseCsv } = require('csv-parse/sync');

const CSV_BRANCH_MAP = {
  IL:'Israel',EM:'Israel',JI:'Israel',
  LA:'Los Angeles',EL:'Los Angeles',
  HK:'Hong Kong',ES:'Hong Kong',HS:'Hong Kong',JH:'Hong Kong',JS:'Hong Kong',EH:'Hong Kong',
  NY:'New York',EN:'New York',ET:'New York',DT:'New York',JT:'New York',EG:'New York',
  EV:'New York',GN:'New York',VG:'New York',JG:'New York',JV:'New York',EY:'New York',
  HKG:'Hong Kong',ISR:'Israel',NYC:'New York'
};

const csvMapBranch = (b) => {
  if (!b) return null;
  const clean = b.trim();
  if (clean.includes('http://') || clean.includes('https://') || clean.length > 20) return null;
  return CSV_BRANCH_MAP[clean.toUpperCase()] || clean;
};

const csvSafeNum = (v) => {
  const n = parseFloat(v);
  return Number.isFinite(n) ? n : null;
};

let csvImportProgress = {
  active: false,
  phase: 'idle',
  progress: 0,
  detail: '',
  totalStones: 0,
  processedStones: 0,
};

app.get("/api/import-csv/progress", (req, res) => {
  res.json(csvImportProgress);
});

app.post("/api/import-csv", async (req, res) => {
  if (csvImportProgress.active) {
    return res.status(409).json({ success: false, error: "A CSV import is already in progress" });
  }

  try {
    const { csvContent } = req.body;
    if (!csvContent) {
      return res.status(400).json({ success: false, error: "No CSV content provided" });
    }

    console.log("📄 CSV import requested via API");
    csvImportProgress = { active: true, phase: 'parsing', progress: 10, detail: 'Parsing CSV...', totalStones: 0, processedStones: 0 };

    const rows = parseCsv(csvContent, { columns: true, skip_empty_lines: true, relax_quotes: true, relax_column_count: true });
    console.log(`📄 Parsed ${rows.length} rows from CSV`);
    csvImportProgress = { ...csvImportProgress, phase: 'processing', progress: 20, detail: `Parsed ${rows.length} stones`, totalStones: rows.length };

    const columns = [
      'category','sku','shape','weight','color','clarity','lab',
      'fluorescence','price_per_carat','rap_price','rap_list_price',
      'total_price','location','branch','image','additional_pictures',
      'video','additional_videos','certificate_image','certificate_number',
      'certificate_image_jpg','cut','polish','symmetry','table_percent',
      'depth_percent','ratio','measurements','fancy_intensity',
      'fancy_color','fancy_overtone','fancy_color_2','fancy_overtone_2',
      'pair_stone','home_page','trade_show','comment','type',
      'cert_comments','origin','grouping_type','box','stones','raw_xml'
    ];

    const values = rows.map(r => {
      const ppc = csvSafeNum(r['Price Per Carat']);
      const tp = csvSafeNum(r['Total Price']);
      return [
        r['Category'] || null,
        r['SKU'] || null,
        r['Shape'] || null,
        csvSafeNum(r['Weight']),
        r['Color'] || null,
        r['Clarity'] || null,
        r['Lab'] || null,
        r['Fluorescence'] || null,
        ppc !== null ? ppc * 2 : null,
        csvSafeNum(r['Rap Price % ']),
        csvSafeNum(r['Rap. Price']),
        tp !== null ? tp * 2 : null,
        r['Location'] || null,
        csvMapBranch(r['Branch']),
        r['Image'] || null,
        r['additional_pictures'] || null,
        r['Video'] || null,
        r['additional_videos'] || null,
        r['Certificate image'] || null,
        r['Certificate Number'] || null,
        r['certificateImageJPG'] || null,
        r['Cut'] || null,
        r['Polish'] || null,
        r['Symmetry'] || null,
        csvSafeNum(r['Table']),
        csvSafeNum(r['Depth']),
        csvSafeNum(r['ratio']),
        r['Measurements (- delimiter)'] || null,
        r['fancy_intensity'] || null,
        r['fancy_color'] || null,
        r['fancy_overtone'] || null,
        r['fancy_color_2'] || null,
        r['fancy_overtone_2'] || null,
        r['Pair Stone'] || null,
        r['home_page'] || null,
        r['TradeShow'] || null,
        r['Comment'] || null,
        r['Type'] || null,
        r['Cert. Comments'] || null,
        r['Origin'] || null,
        r['Grouping Type'] || null,
        r['Box'] || null,
        csvSafeNum(r['Stones']),
        'csv_import'
      ];
    });

    csvImportProgress = { ...csvImportProgress, phase: 'clearing', progress: 40, detail: 'Preparing database...' };
    await pool.query('TRUNCATE TABLE soap_stones RESTART IDENTITY');

    csvImportProgress = { ...csvImportProgress, phase: 'inserting', progress: 50, detail: 'Saving stones to database...' };
    const CHUNK = 300;
    const totalChunks = Math.ceil(values.length / CHUNK);
    for (let i = 0; i < values.length; i += CHUNK) {
      const chunk = values.slice(i, i + CHUNK);
      const chunkIdx = Math.floor(i / CHUNK) + 1;
      const ph = chunk.map((row, ri) =>
        '(' + columns.map((_, ci) => '$' + (ri * columns.length + ci + 1)).join(',') + ')'
      ).join(',');
      await pool.query('INSERT INTO soap_stones (' + columns.join(',') + ') VALUES ' + ph, chunk.flat());
      const pct = 50 + Math.round((chunkIdx / totalChunks) * 45);
      csvImportProgress = { ...csvImportProgress, progress: pct, processedStones: Math.min(i + CHUNK, values.length), detail: `Inserted ${Math.min(i + CHUNK, values.length)} / ${values.length} stones` };
    }

    csvImportProgress = { active: false, phase: 'complete', progress: 100, detail: `Successfully imported ${values.length} stones!`, totalStones: values.length, processedStones: values.length };
    console.log(`✅ CSV import completed: ${values.length} stones`);

    res.json({ success: true, count: values.length, status: "completed" });
  } catch (error) {
    console.error("❌ CSV import error:", error);
    csvImportProgress = { active: false, phase: 'error', progress: 0, detail: error.message, totalStones: 0, processedStones: 0 };
    res.status(500).json({ success: false, error: error.message });
  }
});

/* =========================================================
   Saved Filters
   ========================================================= */

// Auto-create table on startup
pool.query(`
  CREATE TABLE IF NOT EXISTS saved_filters (
    id SERIAL PRIMARY KEY,
    user_id TEXT NOT NULL,
    name TEXT NOT NULL,
    inventory_mode TEXT NOT NULL DEFAULT 'diamonds',
    filters JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMP DEFAULT NOW()
  )
`).catch(err => console.error("saved_filters table creation error:", err));

app.get("/api/saved-filters", async (req, res) => {
  try {
    const { userId } = req.query;
    if (!userId) return res.status(400).json({ error: "userId is required" });
    const result = await pool.query(
      "SELECT * FROM saved_filters WHERE user_id = $1 ORDER BY created_at DESC",
      [userId]
    );
    res.json(result.rows);
  } catch (error) {
    console.error("Error fetching saved filters:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

app.post("/api/saved-filters", async (req, res) => {
  try {
    const { userId, name, inventoryMode, filters } = req.body;
    if (!userId || !name?.trim()) {
      return res.status(400).json({ error: "userId and name are required" });
    }
    const result = await pool.query(
      "INSERT INTO saved_filters (user_id, name, inventory_mode, filters) VALUES ($1, $2, $3, $4) RETURNING *",
      [userId, name.trim(), inventoryMode || 'diamonds', JSON.stringify(filters || {})]
    );
    res.status(201).json(result.rows[0]);
  } catch (error) {
    console.error("Error creating saved filter:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

app.delete("/api/saved-filters/:id", async (req, res) => {
  try {
    const { id } = req.params;
    await pool.query("DELETE FROM saved_filters WHERE id = $1", [id]);
    res.json({ success: true });
  } catch (error) {
    console.error("Error deleting saved filter:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

/* =========================================================
   Label Templates (per user)
   ========================================================= */

pool.query(`
  CREATE TABLE IF NOT EXISTS label_templates (
    id SERIAL PRIMARY KEY,
    user_id TEXT NOT NULL,
    name TEXT NOT NULL DEFAULT 'Default',
    elements JSONB NOT NULL DEFAULT '[]',
    is_active BOOLEAN NOT NULL DEFAULT false,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
  )
`).catch(err => console.error("label_templates table creation error:", err));

app.get("/api/label-templates", async (req, res) => {
  try {
    const { userId } = req.query;
    if (!userId) return res.status(400).json({ error: "userId is required" });
    const result = await pool.query(
      "SELECT * FROM label_templates WHERE user_id = $1 ORDER BY created_at ASC",
      [userId]
    );
    res.json(result.rows);
  } catch (error) {
    console.error("Error fetching label templates:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

app.post("/api/label-templates", async (req, res) => {
  try {
    const { userId, name, elements, isActive } = req.body;
    if (!userId) return res.status(400).json({ error: "userId is required" });
    const result = await pool.query(
      "INSERT INTO label_templates (user_id, name, elements, is_active) VALUES ($1, $2, $3, $4) RETURNING *",
      [userId, (name || "New Template").trim(), JSON.stringify(elements || []), isActive || false]
    );
    res.status(201).json(result.rows[0]);
  } catch (error) {
    console.error("Error creating label template:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

app.put("/api/label-templates/:id", async (req, res) => {
  try {
    const { id } = req.params;
    const { name, elements, isActive } = req.body;
    const fields = [];
    const values = [];
    let idx = 1;

    if (name !== undefined) { fields.push(`name = $${idx++}`); values.push(name.trim()); }
    if (elements !== undefined) { fields.push(`elements = $${idx++}`); values.push(JSON.stringify(elements)); }
    if (isActive !== undefined) { fields.push(`is_active = $${idx++}`); values.push(isActive); }
    fields.push(`updated_at = NOW()`);

    if (fields.length === 1) return res.status(400).json({ error: "No fields to update" });

    values.push(id);
    const result = await pool.query(
      `UPDATE label_templates SET ${fields.join(", ")} WHERE id = $${idx} RETURNING *`,
      values
    );
    if (result.rows.length === 0) return res.status(404).json({ error: "Template not found" });
    res.json(result.rows[0]);
  } catch (error) {
    console.error("Error updating label template:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

app.put("/api/label-templates/set-active/:id", async (req, res) => {
  try {
    const { id } = req.params;
    const { userId } = req.body;
    if (!userId) return res.status(400).json({ error: "userId is required" });
    await pool.query("UPDATE label_templates SET is_active = false WHERE user_id = $1", [userId]);
    const result = await pool.query(
      "UPDATE label_templates SET is_active = true, updated_at = NOW() WHERE id = $1 AND user_id = $2 RETURNING *",
      [id, userId]
    );
    if (result.rows.length === 0) return res.status(404).json({ error: "Template not found" });
    res.json(result.rows[0]);
  } catch (error) {
    console.error("Error setting active template:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

app.delete("/api/label-templates/:id", async (req, res) => {
  try {
    const { id } = req.params;
    await pool.query("DELETE FROM label_templates WHERE id = $1", [id]);
    res.json({ success: true });
  } catch (error) {
    console.error("Error deleting label template:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

/* =========================================================
   CRM – Contacts, Interactions, Deals, Tasks, WhatsApp Log
   ========================================================= */

let crmReady = false;
const crmReadyPromise = (async () => {
  try {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS crm_contacts (
        id SERIAL PRIMARY KEY,
        user_id TEXT NOT NULL,
        name TEXT NOT NULL,
        type TEXT NOT NULL DEFAULT 'lead',
        company TEXT,
        phone TEXT,
        email TEXT,
        country TEXT,
        city TEXT,
        address TEXT,
        source TEXT,
        status TEXT DEFAULT 'active',
        tags JSONB DEFAULT '[]',
        preferences JSONB DEFAULT '{}',
        notes TEXT,
        avatar_url TEXT,
        last_contact_at TIMESTAMP,
        created_at TIMESTAMP DEFAULT NOW(),
        updated_at TIMESTAMP DEFAULT NOW()
      )
    `);
    await pool.query(`
      CREATE TABLE IF NOT EXISTS crm_deals (
        id SERIAL PRIMARY KEY,
        user_id TEXT NOT NULL,
        contact_id INTEGER NOT NULL REFERENCES crm_contacts(id) ON DELETE CASCADE,
        title TEXT NOT NULL,
        stage TEXT NOT NULL DEFAULT 'lead',
        value NUMERIC(14,2) DEFAULT 0,
        currency TEXT DEFAULT 'USD',
        probability INTEGER DEFAULT 0,
        expected_close DATE,
        actual_close DATE,
        notes TEXT,
        created_at TIMESTAMP DEFAULT NOW(),
        updated_at TIMESTAMP DEFAULT NOW()
      )
    `);
    await pool.query(`
      CREATE TABLE IF NOT EXISTS crm_interactions (
        id SERIAL PRIMARY KEY,
        user_id TEXT NOT NULL,
        contact_id INTEGER NOT NULL REFERENCES crm_contacts(id) ON DELETE CASCADE,
        deal_id INTEGER,
        type TEXT NOT NULL,
        direction TEXT DEFAULT 'outgoing',
        subject TEXT,
        content TEXT,
        metadata JSONB DEFAULT '{}',
        occurred_at TIMESTAMP DEFAULT NOW(),
        created_at TIMESTAMP DEFAULT NOW()
      )
    `);
    await pool.query(`
      CREATE TABLE IF NOT EXISTS crm_deal_items (
        id SERIAL PRIMARY KEY,
        deal_id INTEGER NOT NULL REFERENCES crm_deals(id) ON DELETE CASCADE,
        stone_id TEXT,
        sku TEXT,
        category TEXT,
        snapshot JSONB DEFAULT '{}',
        custom_price NUMERIC(14,2),
        quantity INTEGER DEFAULT 1,
        notes TEXT,
        created_at TIMESTAMP DEFAULT NOW()
      )
    `);
    await pool.query(`
      CREATE TABLE IF NOT EXISTS crm_tasks (
        id SERIAL PRIMARY KEY,
        user_id TEXT NOT NULL,
        contact_id INTEGER REFERENCES crm_contacts(id) ON DELETE CASCADE,
        deal_id INTEGER REFERENCES crm_deals(id) ON DELETE CASCADE,
        title TEXT NOT NULL,
        description TEXT,
        due_date TIMESTAMP,
        priority TEXT DEFAULT 'normal',
        status TEXT DEFAULT 'pending',
        completed_at TIMESTAMP,
        created_at TIMESTAMP DEFAULT NOW(),
        updated_at TIMESTAMP DEFAULT NOW()
      )
    `);
    await pool.query(`
      CREATE TABLE IF NOT EXISTS crm_whatsapp_log (
        id SERIAL PRIMARY KEY,
        user_id TEXT NOT NULL,
        contact_id INTEGER REFERENCES crm_contacts(id) ON DELETE SET NULL,
        phone TEXT,
        message TEXT NOT NULL,
        related_items JSONB DEFAULT '[]',
        sent_at TIMESTAMP DEFAULT NOW()
      )
    `);
    crmReady = true;
    console.log("✅ CRM tables ready");
  } catch (err) {
    console.error("❌ CRM table creation error:", err);
  }
})();

const ensureCrm = async (req, res, next) => {
  if (!crmReady) {
    try { await crmReadyPromise; } catch (_) {}
  }
  if (!crmReady) return res.status(503).json({ error: "CRM tables not ready" });
  next();
};
app.use("/api/crm", ensureCrm);

/* ---------- Contacts CRUD ---------- */

app.get("/api/crm/contacts", async (req, res) => {
  try {
    const { userId, search, type, status } = req.query;
    if (!userId) return res.status(400).json({ error: "userId is required" });

    const conditions = ["user_id = $1"];
    const values = [userId];
    let idx = 2;

    if (search) {
      conditions.push(`(name ILIKE $${idx} OR company ILIKE $${idx} OR phone ILIKE $${idx} OR email ILIKE $${idx})`);
      values.push(`%${search}%`);
      idx++;
    }
    if (type && type !== 'all') {
      conditions.push(`type = $${idx++}`);
      values.push(type);
    }
    if (status && status !== 'all') {
      conditions.push(`status = $${idx++}`);
      values.push(status);
    }

    const result = await pool.query(
      `SELECT c.*,
        (SELECT COUNT(*)::int FROM crm_deals d WHERE d.contact_id = c.id) AS deals_count,
        (SELECT COALESCE(SUM(value),0) FROM crm_deals d WHERE d.contact_id = c.id AND d.stage = 'won') AS total_won
       FROM crm_contacts c
       WHERE ${conditions.join(" AND ")}
       ORDER BY c.updated_at DESC`,
      values
    );
    res.json(result.rows);
  } catch (error) {
    console.error("Error fetching contacts:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

app.get("/api/crm/contacts/:id", async (req, res) => {
  try {
    const { id } = req.params;
    const { userId } = req.query;
    if (!userId) return res.status(400).json({ error: "userId is required" });

    const contact = await pool.query(
      "SELECT * FROM crm_contacts WHERE id = $1 AND user_id = $2",
      [id, userId]
    );
    if (contact.rows.length === 0) return res.status(404).json({ error: "Contact not found" });

    const interactions = await pool.query(
      "SELECT * FROM crm_interactions WHERE contact_id = $1 ORDER BY occurred_at DESC LIMIT 100",
      [id]
    );
    const deals = await pool.query(
      "SELECT * FROM crm_deals WHERE contact_id = $1 ORDER BY updated_at DESC",
      [id]
    );
    const tasks = await pool.query(
      "SELECT * FROM crm_tasks WHERE contact_id = $1 ORDER BY due_date ASC NULLS LAST",
      [id]
    );

    res.json({
      ...contact.rows[0],
      interactions: interactions.rows,
      deals: deals.rows,
      tasks: tasks.rows,
    });
  } catch (error) {
    console.error("Error fetching contact:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

app.post("/api/crm/contacts", async (req, res) => {
  try {
    const { userId, name, type, company, phone, email, country, city, address, source, status, tags, preferences, notes, avatarUrl } = req.body;
    if (!userId || !name) return res.status(400).json({ error: "userId and name are required" });

    const result = await pool.query(
      `INSERT INTO crm_contacts (user_id, name, type, company, phone, email, country, city, address, source, status, tags, preferences, notes, avatar_url)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15) RETURNING *`,
      [
        userId, name.trim(), type || 'lead', company || null, phone || null, email || null,
        country || null, city || null, address || null, source || null, status || 'active',
        JSON.stringify(tags || []), JSON.stringify(preferences || {}), notes || null, avatarUrl || null
      ]
    );
    res.status(201).json(result.rows[0]);
  } catch (error) {
    console.error("Error creating contact:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

app.put("/api/crm/contacts/:id", async (req, res) => {
  try {
    const { id } = req.params;
    const allowed = ['name','type','company','phone','email','country','city','address','source','status','tags','preferences','notes','avatar_url','last_contact_at'];
    const fields = [];
    const values = [];
    let idx = 1;

    for (const key of allowed) {
      const camel = key.replace(/_([a-z])/g, (_,c) => c.toUpperCase());
      if (req.body[camel] !== undefined) {
        if (key === 'tags' || key === 'preferences') {
          fields.push(`${key} = $${idx++}`);
          values.push(JSON.stringify(req.body[camel]));
        } else {
          fields.push(`${key} = $${idx++}`);
          values.push(req.body[camel]);
        }
      }
    }
    fields.push(`updated_at = NOW()`);
    if (fields.length === 1) return res.status(400).json({ error: "No fields to update" });

    values.push(id);
    const result = await pool.query(
      `UPDATE crm_contacts SET ${fields.join(", ")} WHERE id = $${idx} RETURNING *`,
      values
    );
    if (result.rows.length === 0) return res.status(404).json({ error: "Contact not found" });
    res.json(result.rows[0]);
  } catch (error) {
    console.error("Error updating contact:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

app.delete("/api/crm/contacts/:id", async (req, res) => {
  try {
    const { id } = req.params;
    await pool.query("DELETE FROM crm_contacts WHERE id = $1", [id]);
    res.json({ success: true });
  } catch (error) {
    console.error("Error deleting contact:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

/* ---------- Bulk operations on contacts ---------- */
app.post("/api/crm/contacts/bulk-delete", async (req, res) => {
  try {
    const { userId, ids } = req.body;
    if (!userId || !Array.isArray(ids) || ids.length === 0) {
      return res.status(400).json({ error: "userId and non-empty ids array are required" });
    }
    const result = await pool.query(
      "DELETE FROM crm_contacts WHERE user_id = $1 AND id = ANY($2::int[]) RETURNING id",
      [userId, ids.map(Number)]
    );
    res.json({ success: true, deleted: result.rowCount });
  } catch (error) {
    console.error("Bulk delete error:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

app.post("/api/crm/contacts/bulk-tag", async (req, res) => {
  // Action: 'add' | 'remove'
  try {
    const { userId, ids, tag, action } = req.body;
    if (!userId || !Array.isArray(ids) || !tag || !["add", "remove"].includes(action)) {
      return res.status(400).json({ error: "userId, ids, tag, and action(add|remove) are required" });
    }
    const safeTag = String(tag).trim();
    if (!safeTag) return res.status(400).json({ error: "Tag cannot be empty" });

    const sql =
      action === "add"
        ? `UPDATE crm_contacts
             SET tags = (
               CASE WHEN tags @> $1::jsonb THEN tags
                    ELSE tags || $1::jsonb END
             ),
             updated_at = NOW()
           WHERE user_id = $2 AND id = ANY($3::int[])
           RETURNING id`
        : `UPDATE crm_contacts
             SET tags = COALESCE((SELECT jsonb_agg(elem) FROM jsonb_array_elements(tags) elem WHERE elem <> $1::jsonb), '[]'::jsonb),
             updated_at = NOW()
           WHERE user_id = $2 AND id = ANY($3::int[])
           RETURNING id`;

    const result = await pool.query(sql, [JSON.stringify(safeTag), userId, ids.map(Number)]);
    res.json({ success: true, updated: result.rowCount });
  } catch (error) {
    console.error("Bulk tag error:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

app.get("/api/crm/tags", async (req, res) => {
  try {
    const { userId } = req.query;
    if (!userId) return res.status(400).json({ error: "userId is required" });
    const result = await pool.query(
      `SELECT tag, COUNT(*)::int AS count
         FROM crm_contacts c, jsonb_array_elements_text(c.tags) AS tag
         WHERE c.user_id = $1
         GROUP BY tag
         ORDER BY count DESC, tag ASC`,
      [userId]
    );
    res.json(result.rows);
  } catch (error) {
    console.error("Tags fetch error:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

/* ---------- Verify business online (OpenAI Web Search) ---------- */
app.post("/api/crm/verify-business", async (req, res) => {
  try {
    const { contact } = req.body;
    if (!contact || (!contact.name && !contact.company)) {
      return res.status(400).json({ error: "Contact name or company is required" });
    }
    if (!process.env.OPENAI_API_KEY) {
      return res.status(500).json({ error: "OPENAI_API_KEY is not configured" });
    }

    const detailsLines = [
      contact.name ? `Person: ${contact.name}` : null,
      contact.company ? `Company: ${contact.company}` : null,
      contact.email ? `Email: ${contact.email}` : null,
      contact.phone ? `Phone: ${contact.phone}` : null,
      contact.website ? `Website: ${contact.website}` : null,
      contact.country || contact.city ? `Location: ${[contact.city, contact.country].filter(Boolean).join(", ")}` : null,
    ].filter(Boolean).join("\n");

    const prompt = `You are a business verification assistant for a diamond/jewelry trading CRM.
Search the public web (Google, LinkedIn, official websites, business directories such as Rapaport, Polygon, IDEX, JCK, GIA, etc.) and verify the following contact.

Return a STRICT JSON object with this shape:
{
  "verified": true | false,
  "confidence": "high" | "medium" | "low",
  "summary": "1-2 sentence plain-language summary of what you found",
  "discoveredFields": {
    "company": "...optional, if found and improved...",
    "website": "...",
    "phone": "...",
    "email": "...",
    "country": "...",
    "city": "...",
    "address": "...",
    "linkedin": "...",
    "instagram": "...",
    "industry": "...",
    "yearsActive": "...",
    "notes": "interesting context (e.g. 'Listed on Rapaport member directory since 2015')"
  },
  "warnings": ["any red flags, e.g. inactive site, mismatched country, etc."],
  "sources": [{"label": "site name", "url": "https://..."}]
}

Only include fields you are reasonably confident about. Omit fields that are unknown rather than guessing.
If you cannot find anything credible, set verified=false, confidence="low" and explain in summary.

Contact data to verify:
${detailsLines}`;

    const aiRes = await fetch("https://api.openai.com/v1/chat/completions", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${process.env.OPENAI_API_KEY}`,
      },
      body: JSON.stringify({
        model: "gpt-4o-mini-search-preview",
        messages: [
          { role: "system", content: "You are a meticulous research assistant. You always reply with valid JSON only — no markdown, no commentary." },
          { role: "user", content: prompt },
        ],
        web_search_options: {},
        response_format: { type: "json_object" },
      }),
    });

    if (!aiRes.ok) {
      // Fallback to non-search model if search variant unavailable
      const errText = await aiRes.text();
      console.warn("Search model failed, trying fallback:", aiRes.status, errText);

      const fallback = await fetch("https://api.openai.com/v1/chat/completions", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${process.env.OPENAI_API_KEY}`,
        },
        body: JSON.stringify({
          model: "gpt-4o-mini",
          messages: [
            { role: "system", content: "You are a meticulous research assistant. Respond with valid JSON only." },
            { role: "user", content: prompt + "\n\nNote: Web search is unavailable — base your response only on widely-known public information you already have." },
          ],
          response_format: { type: "json_object" },
        }),
      });

      if (!fallback.ok) {
        const fbErr = await fallback.text();
        console.error("Fallback OpenAI error:", fallback.status, fbErr);
        let friendly = `Verification failed (${fallback.status})`;
        if (fallback.status === 429) friendly = "OpenAI quota exceeded. Add credit at platform.openai.com.";
        if (fallback.status === 401) friendly = "OpenAI API key invalid.";
        return res.status(502).json({ error: friendly });
      }
      const fbData = await fallback.json();
      try {
        const parsed = JSON.parse(fbData.choices?.[0]?.message?.content || "{}");
        return res.json({ ...parsed, _searchUsed: false });
      } catch (e) {
        return res.status(502).json({ error: "Could not parse verification response" });
      }
    }

    const data = await aiRes.json();
    let parsed;
    try {
      parsed = JSON.parse(data.choices?.[0]?.message?.content || "{}");
    } catch (e) {
      return res.status(502).json({ error: "Could not parse verification response" });
    }
    res.json({ ...parsed, _searchUsed: true });
  } catch (error) {
    console.error("Verify business error:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

/* ---------- Interactions ---------- */

app.post("/api/crm/interactions", async (req, res) => {
  try {
    const { userId, contactId, dealId, type, direction, subject, content, metadata, occurredAt } = req.body;
    if (!userId || !contactId || !type) return res.status(400).json({ error: "userId, contactId and type are required" });

    const result = await pool.query(
      `INSERT INTO crm_interactions (user_id, contact_id, deal_id, type, direction, subject, content, metadata, occurred_at)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,COALESCE($9, NOW())) RETURNING *`,
      [userId, contactId, dealId || null, type, direction || 'outgoing', subject || null, content || null, JSON.stringify(metadata || {}), occurredAt || null]
    );
    await pool.query("UPDATE crm_contacts SET last_contact_at = NOW(), updated_at = NOW() WHERE id = $1", [contactId]);
    res.status(201).json(result.rows[0]);
  } catch (error) {
    console.error("Error creating interaction:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

app.delete("/api/crm/interactions/:id", async (req, res) => {
  try {
    await pool.query("DELETE FROM crm_interactions WHERE id = $1", [req.params.id]);
    res.json({ success: true });
  } catch (error) {
    console.error("Error deleting interaction:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

/* ---------- Deals ---------- */

app.get("/api/crm/deals", async (req, res) => {
  try {
    const { userId, stage, contactId } = req.query;
    if (!userId) return res.status(400).json({ error: "userId is required" });

    const conditions = ["d.user_id = $1"];
    const values = [userId];
    let idx = 2;

    if (stage && stage !== 'all') { conditions.push(`d.stage = $${idx++}`); values.push(stage); }
    if (contactId) { conditions.push(`d.contact_id = $${idx++}`); values.push(contactId); }

    const result = await pool.query(
      `SELECT d.*, c.name AS contact_name, c.company AS contact_company, c.type AS contact_type,
        (SELECT COUNT(*)::int FROM crm_deal_items i WHERE i.deal_id = d.id) AS items_count
       FROM crm_deals d
       LEFT JOIN crm_contacts c ON c.id = d.contact_id
       WHERE ${conditions.join(" AND ")}
       ORDER BY d.updated_at DESC`,
      values
    );
    res.json(result.rows);
  } catch (error) {
    console.error("Error fetching deals:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

app.get("/api/crm/deals/:id", async (req, res) => {
  try {
    const { id } = req.params;
    const deal = await pool.query(
      `SELECT d.*, c.name AS contact_name, c.company AS contact_company, c.phone AS contact_phone, c.email AS contact_email
       FROM crm_deals d LEFT JOIN crm_contacts c ON c.id = d.contact_id WHERE d.id = $1`,
      [id]
    );
    if (deal.rows.length === 0) return res.status(404).json({ error: "Deal not found" });
    const items = await pool.query("SELECT * FROM crm_deal_items WHERE deal_id = $1 ORDER BY created_at ASC", [id]);
    const interactions = await pool.query("SELECT * FROM crm_interactions WHERE deal_id = $1 ORDER BY occurred_at DESC", [id]);
    res.json({ ...deal.rows[0], items: items.rows, interactions: interactions.rows });
  } catch (error) {
    console.error("Error fetching deal:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

app.post("/api/crm/deals", async (req, res) => {
  try {
    const { userId, contactId, title, stage, value, currency, probability, expectedClose, notes, items } = req.body;
    if (!userId || !contactId || !title) return res.status(400).json({ error: "userId, contactId and title are required" });

    const result = await pool.query(
      `INSERT INTO crm_deals (user_id, contact_id, title, stage, value, currency, probability, expected_close, notes)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9) RETURNING *`,
      [userId, contactId, title.trim(), stage || 'lead', value || 0, currency || 'USD', probability || 0, expectedClose || null, notes || null]
    );
    const deal = result.rows[0];

    if (Array.isArray(items) && items.length > 0) {
      for (const item of items) {
        await pool.query(
          `INSERT INTO crm_deal_items (deal_id, stone_id, sku, category, snapshot, custom_price, quantity, notes)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
          [deal.id, item.stoneId || null, item.sku || null, item.category || null, JSON.stringify(item.snapshot || {}), item.customPrice || null, item.quantity || 1, item.notes || null]
        );
      }
    }
    res.status(201).json(deal);
  } catch (error) {
    console.error("Error creating deal:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

app.put("/api/crm/deals/:id", async (req, res) => {
  try {
    const { id } = req.params;
    const allowed = ['title','stage','value','currency','probability','expected_close','actual_close','notes'];
    const fields = [];
    const values = [];
    let idx = 1;

    for (const key of allowed) {
      const camel = key.replace(/_([a-z])/g, (_,c) => c.toUpperCase());
      if (req.body[camel] !== undefined) {
        fields.push(`${key} = $${idx++}`);
        values.push(req.body[camel]);
      }
    }
    if (req.body.stage === 'won' && !req.body.actualClose) {
      fields.push(`actual_close = $${idx++}`);
      values.push(new Date().toISOString().slice(0,10));
    }
    fields.push(`updated_at = NOW()`);
    if (fields.length === 1) return res.status(400).json({ error: "No fields to update" });

    values.push(id);
    const result = await pool.query(
      `UPDATE crm_deals SET ${fields.join(", ")} WHERE id = $${idx} RETURNING *`,
      values
    );
    if (result.rows.length === 0) return res.status(404).json({ error: "Deal not found" });
    res.json(result.rows[0]);
  } catch (error) {
    console.error("Error updating deal:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

app.delete("/api/crm/deals/:id", async (req, res) => {
  try {
    await pool.query("DELETE FROM crm_deals WHERE id = $1", [req.params.id]);
    res.json({ success: true });
  } catch (error) {
    console.error("Error deleting deal:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

/* ---------- Deal items ---------- */

app.post("/api/crm/deals/:id/items", async (req, res) => {
  try {
    const { id } = req.params;
    const { items } = req.body;
    if (!Array.isArray(items)) return res.status(400).json({ error: "items array is required" });

    const inserted = [];
    for (const item of items) {
      const r = await pool.query(
        `INSERT INTO crm_deal_items (deal_id, stone_id, sku, category, snapshot, custom_price, quantity, notes)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8) RETURNING *`,
        [id, item.stoneId || null, item.sku || null, item.category || null, JSON.stringify(item.snapshot || {}), item.customPrice || null, item.quantity || 1, item.notes || null]
      );
      inserted.push(r.rows[0]);
    }
    await pool.query("UPDATE crm_deals SET updated_at = NOW() WHERE id = $1", [id]);
    res.status(201).json(inserted);
  } catch (error) {
    console.error("Error adding deal items:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

app.put("/api/crm/deal-items/:itemId", async (req, res) => {
  try {
    const { itemId } = req.params;
    const allowed = ['custom_price','quantity','notes'];
    const fields = [];
    const values = [];
    let idx = 1;
    for (const key of allowed) {
      const camel = key.replace(/_([a-z])/g, (_,c) => c.toUpperCase());
      if (req.body[camel] !== undefined) {
        fields.push(`${key} = $${idx++}`);
        values.push(req.body[camel]);
      }
    }
    if (fields.length === 0) return res.status(400).json({ error: "No fields to update" });
    values.push(itemId);
    const result = await pool.query(
      `UPDATE crm_deal_items SET ${fields.join(", ")} WHERE id = $${idx} RETURNING *`,
      values
    );
    res.json(result.rows[0]);
  } catch (error) {
    console.error("Error updating deal item:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

app.delete("/api/crm/deal-items/:itemId", async (req, res) => {
  try {
    await pool.query("DELETE FROM crm_deal_items WHERE id = $1", [req.params.itemId]);
    res.json({ success: true });
  } catch (error) {
    console.error("Error deleting deal item:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

/* ---------- Tasks ---------- */

app.get("/api/crm/tasks", async (req, res) => {
  try {
    const { userId, status, contactId, dealId } = req.query;
    if (!userId) return res.status(400).json({ error: "userId is required" });

    const conditions = ["t.user_id = $1"];
    const values = [userId];
    let idx = 2;
    if (status && status !== 'all') { conditions.push(`t.status = $${idx++}`); values.push(status); }
    if (contactId) { conditions.push(`t.contact_id = $${idx++}`); values.push(contactId); }
    if (dealId) { conditions.push(`t.deal_id = $${idx++}`); values.push(dealId); }

    const result = await pool.query(
      `SELECT t.*, c.name AS contact_name, d.title AS deal_title
       FROM crm_tasks t
       LEFT JOIN crm_contacts c ON c.id = t.contact_id
       LEFT JOIN crm_deals d ON d.id = t.deal_id
       WHERE ${conditions.join(" AND ")}
       ORDER BY t.status ASC, t.due_date ASC NULLS LAST`,
      values
    );
    res.json(result.rows);
  } catch (error) {
    console.error("Error fetching tasks:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

app.post("/api/crm/tasks", async (req, res) => {
  try {
    const { userId, contactId, dealId, title, description, dueDate, priority, status } = req.body;
    if (!userId || !title) return res.status(400).json({ error: "userId and title are required" });

    const result = await pool.query(
      `INSERT INTO crm_tasks (user_id, contact_id, deal_id, title, description, due_date, priority, status)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8) RETURNING *`,
      [userId, contactId || null, dealId || null, title.trim(), description || null, dueDate || null, priority || 'normal', status || 'pending']
    );
    res.status(201).json(result.rows[0]);
  } catch (error) {
    console.error("Error creating task:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

app.put("/api/crm/tasks/:id", async (req, res) => {
  try {
    const { id } = req.params;
    const allowed = ['title','description','due_date','priority','status'];
    const fields = [];
    const values = [];
    let idx = 1;
    for (const key of allowed) {
      const camel = key.replace(/_([a-z])/g, (_,c) => c.toUpperCase());
      if (req.body[camel] !== undefined) {
        fields.push(`${key} = $${idx++}`);
        values.push(req.body[camel]);
      }
    }
    if (req.body.status === 'done') {
      fields.push(`completed_at = NOW()`);
    }
    fields.push(`updated_at = NOW()`);
    if (fields.length === 1) return res.status(400).json({ error: "No fields to update" });
    values.push(id);
    const result = await pool.query(
      `UPDATE crm_tasks SET ${fields.join(", ")} WHERE id = $${idx} RETURNING *`,
      values
    );
    res.json(result.rows[0]);
  } catch (error) {
    console.error("Error updating task:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

app.delete("/api/crm/tasks/:id", async (req, res) => {
  try {
    await pool.query("DELETE FROM crm_tasks WHERE id = $1", [req.params.id]);
    res.json({ success: true });
  } catch (error) {
    console.error("Error deleting task:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

/* ---------- WhatsApp Log ---------- */

app.post("/api/crm/whatsapp-log", async (req, res) => {
  try {
    const { userId, contactId, phone, message, relatedItems } = req.body;
    if (!userId || !message) return res.status(400).json({ error: "userId and message are required" });

    const result = await pool.query(
      `INSERT INTO crm_whatsapp_log (user_id, contact_id, phone, message, related_items)
       VALUES ($1,$2,$3,$4,$5) RETURNING *`,
      [userId, contactId || null, phone || null, message, JSON.stringify(relatedItems || [])]
    );

    if (contactId) {
      await pool.query(
        `INSERT INTO crm_interactions (user_id, contact_id, type, direction, subject, content, metadata)
         VALUES ($1,$2,'whatsapp','outgoing','WhatsApp message',$3,$4)`,
        [userId, contactId, message, JSON.stringify({ phone, relatedItems: relatedItems || [] })]
      );
      await pool.query("UPDATE crm_contacts SET last_contact_at = NOW(), updated_at = NOW() WHERE id = $1", [contactId]);
    }
    res.status(201).json(result.rows[0]);
  } catch (error) {
    console.error("Error logging WhatsApp message:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

app.get("/api/crm/whatsapp-log", async (req, res) => {
  try {
    const { userId, contactId } = req.query;
    if (!userId) return res.status(400).json({ error: "userId is required" });

    const conditions = ["user_id = $1"];
    const values = [userId];
    let idx = 2;
    if (contactId) { conditions.push(`contact_id = $${idx++}`); values.push(contactId); }

    const result = await pool.query(
      `SELECT * FROM crm_whatsapp_log WHERE ${conditions.join(" AND ")} ORDER BY sent_at DESC LIMIT 200`,
      values
    );
    res.json(result.rows);
  } catch (error) {
    console.error("Error fetching whatsapp log:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

/* ---------- CRM Dashboard Stats ---------- */

app.get("/api/crm/stats", async (req, res) => {
  try {
    const { userId } = req.query;
    if (!userId) return res.status(400).json({ error: "userId is required" });

    const [contacts, deals, tasks, recentInteractions, topContacts, monthlyWon] = await Promise.all([
      pool.query(`
        SELECT
          COUNT(*)::int AS total,
          COUNT(*) FILTER (WHERE type = 'lead')::int AS leads,
          COUNT(*) FILTER (WHERE type = 'buyer')::int AS buyers,
          COUNT(*) FILTER (WHERE type = 'dealer')::int AS dealers,
          COUNT(*) FILTER (WHERE type = 'designer')::int AS designers,
          COUNT(*) FILTER (WHERE type = 'supplier')::int AS suppliers
        FROM crm_contacts WHERE user_id = $1
      `, [userId]),
      pool.query(`
        SELECT
          COUNT(*)::int AS total,
          COUNT(*) FILTER (WHERE stage NOT IN ('won','lost'))::int AS active,
          COUNT(*) FILTER (WHERE stage = 'won')::int AS won,
          COUNT(*) FILTER (WHERE stage = 'lost')::int AS lost,
          COALESCE(SUM(value) FILTER (WHERE stage NOT IN ('won','lost')),0) AS pipeline_value,
          COALESCE(SUM(value) FILTER (WHERE stage = 'won'),0) AS won_value
        FROM crm_deals WHERE user_id = $1
      `, [userId]),
      pool.query(`
        SELECT
          COUNT(*) FILTER (WHERE status = 'pending')::int AS pending,
          COUNT(*) FILTER (WHERE status = 'pending' AND due_date < NOW())::int AS overdue,
          COUNT(*) FILTER (WHERE status = 'pending' AND due_date::date = CURRENT_DATE)::int AS today
        FROM crm_tasks WHERE user_id = $1
      `, [userId]),
      pool.query(`
        SELECT i.*, c.name AS contact_name FROM crm_interactions i
        LEFT JOIN crm_contacts c ON c.id = i.contact_id
        WHERE i.user_id = $1
        ORDER BY i.occurred_at DESC LIMIT 10
      `, [userId]),
      pool.query(`
        SELECT c.id, c.name, c.company, c.type,
          COALESCE(SUM(d.value) FILTER (WHERE d.stage = 'won'),0) AS total_won,
          COUNT(d.id) FILTER (WHERE d.stage = 'won')::int AS deals_won
        FROM crm_contacts c
        LEFT JOIN crm_deals d ON d.contact_id = c.id
        WHERE c.user_id = $1
        GROUP BY c.id
        HAVING COUNT(d.id) FILTER (WHERE d.stage = 'won') > 0
        ORDER BY total_won DESC LIMIT 5
      `, [userId]),
      pool.query(`
        SELECT
          to_char(date_trunc('month', actual_close), 'YYYY-MM') AS month,
          COALESCE(SUM(value),0) AS value,
          COUNT(*)::int AS count
        FROM crm_deals
        WHERE user_id = $1 AND stage = 'won' AND actual_close >= NOW() - INTERVAL '12 months'
        GROUP BY 1 ORDER BY 1
      `, [userId]),
    ]);

    res.json({
      contacts: contacts.rows[0],
      deals: deals.rows[0],
      tasks: tasks.rows[0],
      recentInteractions: recentInteractions.rows,
      topContacts: topContacts.rows,
      monthlyWon: monthlyWon.rows,
    });
  } catch (error) {
    console.error("Error fetching CRM stats:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

/* =========================================================
   CRM – Business Card Scanner (OpenAI Vision)
   ========================================================= */

const normPhone = (p) => (p || "").replace(/[^\d]/g, "");

app.post("/api/crm/scan-card", async (req, res) => {
  try {
    const { userId, imageBase64 } = req.body;
    if (!userId) return res.status(400).json({ error: "userId is required" });
    if (!imageBase64) return res.status(400).json({ error: "imageBase64 is required" });
    if (!process.env.OPENAI_API_KEY) {
      return res.status(500).json({ error: "OPENAI_API_KEY is not configured on the server" });
    }

    const dataUrl = imageBase64.startsWith("data:")
      ? imageBase64
      : `data:image/jpeg;base64,${imageBase64}`;

    const prompt = `You are an OCR assistant for business cards used at gem and jewelry trade shows.
Extract structured contact details from the image. Return ONLY valid JSON, no prose.

Schema (use null for missing fields):
{
  "name": string|null,
  "jobTitle": string|null,
  "company": string|null,
  "phone": string|null,
  "phoneAlt": string|null,
  "email": string|null,
  "website": string|null,
  "country": string|null,
  "city": string|null,
  "address": string|null,
  "type": "buyer"|"dealer"|"designer"|"supplier"|"lead",
  "notes": string|null,
  "language": string|null
}

Rules:
- Choose "type" by guessing from job title/company (jeweler/designer = designer; wholesale/diamond dealer = dealer; supplier/manufacturer = supplier; retailer = buyer; otherwise lead).
- If multiple phones, put the main mobile in "phone" and the office in "phoneAlt".
- Keep phone numbers exactly as they appear (with + and country code if present).
- "notes" should contain any extra text on the card that does not fit the other fields (e.g. specialties, services).
- "language" is ISO code of the dominant language ("en", "he", etc.).
Output ONLY the JSON object.`;

    const aiRes = await fetch("https://api.openai.com/v1/chat/completions", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Authorization": `Bearer ${process.env.OPENAI_API_KEY}`,
      },
      body: JSON.stringify({
        model: "gpt-4o-mini",
        temperature: 0,
        response_format: { type: "json_object" },
        messages: [
          {
            role: "user",
            content: [
              { type: "text", text: prompt },
              { type: "image_url", image_url: { url: dataUrl, detail: "high" } },
            ],
          },
        ],
      }),
    });

    if (!aiRes.ok) {
      const errText = await aiRes.text();
      console.error("OpenAI error:", aiRes.status, errText);
      let friendly = `OCR provider error (${aiRes.status})`;
      if (aiRes.status === 429) {
        friendly =
          "OpenAI quota exceeded. Most likely no credit balance on the account. " +
          "Please add credit at platform.openai.com/settings/organization/billing/overview and try again.";
      } else if (aiRes.status === 401) {
        friendly = "OpenAI API key invalid or revoked. Update OPENAI_API_KEY in your server environment.";
      } else if (aiRes.status === 400) {
        friendly = "OpenAI rejected the image (it may be too large or in an unsupported format). Try a smaller, clearer photo.";
      }
      return res.status(502).json({ error: friendly, providerStatus: aiRes.status });
    }

    const aiData = await aiRes.json();
    let parsed;
    try {
      parsed = JSON.parse(aiData.choices?.[0]?.message?.content || "{}");
    } catch (e) {
      return res.status(500).json({ error: "Failed to parse OCR response" });
    }

    // Try to find matching contact by phone or email
    let matches = [];
    const phoneDigits = normPhone(parsed.phone);
    const altDigits = normPhone(parsed.phoneAlt);
    const email = (parsed.email || "").toLowerCase().trim();

    if (phoneDigits || altDigits || email) {
      const conditions = [];
      const values = [userId];
      let idx = 2;
      if (phoneDigits && phoneDigits.length >= 7) {
        conditions.push(`regexp_replace(coalesce(phone,''), '[^0-9]', '', 'g') LIKE $${idx++}`);
        values.push(`%${phoneDigits.slice(-9)}%`);
      }
      if (email) {
        conditions.push(`LOWER(email) = $${idx++}`);
        values.push(email);
      }
      if (conditions.length > 0) {
        const r = await pool.query(
          `SELECT id, name, type, company, phone, email, country, city, last_contact_at
           FROM crm_contacts
           WHERE user_id = $1 AND (${conditions.join(" OR ")})
           ORDER BY updated_at DESC LIMIT 5`,
          values
        );
        matches = r.rows;
      }
    }

    res.json({ extracted: parsed, matches });
  } catch (error) {
    console.error("Error scanning card:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

/* =========================================================
   Start server
   ========================================================= */
app.listen(port, () => {
  console.log(`🚀 Server is running on http://localhost:${port}`);
});
