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

const decrypt = (cipher) => {
  if (!cipher) return null;
  try {
    const bytes = CryptoJS.AES.decrypt(cipher, ENCRYPT_SECRET);
    return bytes.toString(CryptoJS.enc.Utf8) || null;
  } catch (e) {
    console.error("Decrypt failed:", e.message);
    return null;
  }
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

    // Folders (hierarchical)
    await pool.query(`
      CREATE TABLE IF NOT EXISTS crm_folders (
        id SERIAL PRIMARY KEY,
        user_id TEXT NOT NULL,
        name TEXT NOT NULL,
        parent_id INTEGER REFERENCES crm_folders(id) ON DELETE CASCADE,
        color TEXT,
        created_at TIMESTAMP DEFAULT NOW(),
        updated_at TIMESTAMP DEFAULT NOW()
      )
    `);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_crm_folders_user ON crm_folders(user_id)`);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_crm_folders_parent ON crm_folders(parent_id)`);

    // Per-user OAuth/integration tokens (Outlook, etc.)
    await pool.query(`
      CREATE TABLE IF NOT EXISTS crm_integrations (
        id SERIAL PRIMARY KEY,
        user_id TEXT NOT NULL,
        provider TEXT NOT NULL,
        account_email TEXT,
        account_name TEXT,
        access_token_enc TEXT,
        refresh_token_enc TEXT,
        expires_at TIMESTAMP,
        scope TEXT,
        last_sync_at TIMESTAMP,
        metadata JSONB DEFAULT '{}',
        created_at TIMESTAMP DEFAULT NOW(),
        updated_at TIMESTAMP DEFAULT NOW(),
        UNIQUE(user_id, provider)
      )
    `);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_crm_integrations_user ON crm_integrations(user_id)`);

    // Email broadcast log
    await pool.query(`
      CREATE TABLE IF NOT EXISTS crm_email_broadcasts (
        id SERIAL PRIMARY KEY,
        user_id TEXT NOT NULL,
        subject TEXT,
        body TEXT,
        recipients_count INTEGER DEFAULT 0,
        sent_count INTEGER DEFAULT 0,
        failed_count INTEGER DEFAULT 0,
        details JSONB DEFAULT '[]',
        sent_at TIMESTAMP DEFAULT NOW()
      )
    `);

    // Saved email templates (per-user, reusable HTML templates)
    await pool.query(`
      CREATE TABLE IF NOT EXISTS crm_email_templates (
        id SERIAL PRIMARY KEY,
        user_id TEXT NOT NULL,
        name TEXT NOT NULL,
        subject TEXT,
        html TEXT,
        thumbnail TEXT,
        created_at TIMESTAMP DEFAULT NOW(),
        updated_at TIMESTAMP DEFAULT NOW()
      )
    `);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_crm_email_templates_user ON crm_email_templates(user_id)`);

    // Add new columns to crm_contacts (idempotent)
    const newCols = [
      "ALTER TABLE crm_contacts ADD COLUMN IF NOT EXISTS title TEXT",
      "ALTER TABLE crm_contacts ADD COLUMN IF NOT EXISTS website TEXT",
      "ALTER TABLE crm_contacts ADD COLUMN IF NOT EXISTS phone_alt TEXT",
      "ALTER TABLE crm_contacts ADD COLUMN IF NOT EXISTS folder_id INTEGER",
      "ALTER TABLE crm_contacts ADD COLUMN IF NOT EXISTS linked_contact_ids JSONB DEFAULT '[]'",
      "ALTER TABLE crm_contacts ADD COLUMN IF NOT EXISTS card_back_notes TEXT",
      "ALTER TABLE crm_contacts ADD COLUMN IF NOT EXISTS outlook_contact_id TEXT",
      "ALTER TABLE crm_contacts ADD COLUMN IF NOT EXISTS outlook_synced_at TIMESTAMP",
      "ALTER TABLE crm_contacts ADD COLUMN IF NOT EXISTS card_image_front TEXT",
      "ALTER TABLE crm_contacts ADD COLUMN IF NOT EXISTS card_image_back TEXT",
      "ALTER TABLE crm_contacts ADD COLUMN IF NOT EXISTS card_image_thumb TEXT",
      // DNA-lead support: contacts visible to all users + which stone the lead is for
      "ALTER TABLE crm_contacts ADD COLUMN IF NOT EXISTS shared BOOLEAN DEFAULT FALSE",
      "ALTER TABLE crm_contacts ADD COLUMN IF NOT EXISTS dna_sku TEXT",
      "ALTER TABLE crm_deals ADD COLUMN IF NOT EXISTS shared BOOLEAN DEFAULT FALSE",
      "ALTER TABLE crm_deals ADD COLUMN IF NOT EXISTS dna_sku TEXT",
      "CREATE INDEX IF NOT EXISTS idx_crm_contacts_shared ON crm_contacts(shared) WHERE shared = TRUE",
      "CREATE INDEX IF NOT EXISTS idx_crm_deals_shared ON crm_deals(shared) WHERE shared = TRUE",
      "CREATE INDEX IF NOT EXISTS idx_crm_contacts_email_lower ON crm_contacts(LOWER(email))",
      "CREATE INDEX IF NOT EXISTS idx_crm_contacts_phone_norm ON crm_contacts(regexp_replace(COALESCE(phone,''),'[^0-9]','','g'))",
      // Performance indexes (huge speedup for the contacts list + drawer queries)
      "CREATE INDEX IF NOT EXISTS idx_crm_contacts_user_id ON crm_contacts(user_id)",
      "CREATE INDEX IF NOT EXISTS idx_crm_contacts_updated_at ON crm_contacts(updated_at DESC)",
      "CREATE INDEX IF NOT EXISTS idx_crm_contacts_folder_id ON crm_contacts(folder_id) WHERE folder_id IS NOT NULL",
      "CREATE INDEX IF NOT EXISTS idx_crm_contacts_type ON crm_contacts(type)",
      "CREATE INDEX IF NOT EXISTS idx_crm_deals_contact_id ON crm_deals(contact_id)",
      "CREATE INDEX IF NOT EXISTS idx_crm_deals_contact_stage ON crm_deals(contact_id, stage)",
      "CREATE INDEX IF NOT EXISTS idx_crm_deals_user_id ON crm_deals(user_id)",
      "CREATE INDEX IF NOT EXISTS idx_crm_interactions_contact ON crm_interactions(contact_id, occurred_at DESC)",
      "CREATE INDEX IF NOT EXISTS idx_crm_interactions_deal ON crm_interactions(deal_id) WHERE deal_id IS NOT NULL",
      "CREATE INDEX IF NOT EXISTS idx_crm_tasks_contact ON crm_tasks(contact_id)",
      "CREATE INDEX IF NOT EXISTS idx_crm_tasks_deal ON crm_tasks(deal_id) WHERE deal_id IS NOT NULL",
      "CREATE INDEX IF NOT EXISTS idx_crm_deal_items_deal ON crm_deal_items(deal_id)",
      "CREATE INDEX IF NOT EXISTS idx_crm_folders_parent ON crm_folders(parent_id)",
      "CREATE INDEX IF NOT EXISTS idx_crm_folders_user ON crm_folders(user_id)",
      // Unread DNA leads badge query (polled every 30s by every signed-in user)
      "CREATE INDEX IF NOT EXISTS idx_crm_contacts_dna_recent ON crm_contacts(created_at DESC) WHERE shared = TRUE AND source = 'dna_lead'",
    ];
    for (const sql of newCols) {
      try { await pool.query(sql); } catch (e) { console.warn("Migration warn:", e.message); }
    }
    // FK for folder_id (separate so it doesn't fail if column already added)
    try {
      await pool.query(`
        DO $$ BEGIN
          IF NOT EXISTS (
            SELECT 1 FROM information_schema.table_constraints
            WHERE constraint_name = 'crm_contacts_folder_fk'
          ) THEN
            ALTER TABLE crm_contacts
            ADD CONSTRAINT crm_contacts_folder_fk
            FOREIGN KEY (folder_id) REFERENCES crm_folders(id) ON DELETE SET NULL;
          END IF;
        END $$;
      `);
    } catch (e) { console.warn("FK migration warn:", e.message); }

    // One-time backfill: enrich existing DNA-lead deal_items with real inventory
    // (image, price, specs). Safe to re-run — only touches rows whose snapshot
    // is missing imageUrl. Caps at 200 rows per boot to avoid long startup blocks.
    try {
      const todo = await pool.query(`
        SELECT i.id, i.deal_id, i.sku
          FROM crm_deal_items i
          JOIN crm_deals d ON d.id = i.deal_id
         WHERE d.shared = TRUE
           AND i.sku IS NOT NULL
           AND COALESCE(i.snapshot->>'imageUrl', '') = ''
         LIMIT 200
      `);
      for (const row of todo.rows) {
        try {
          const stoneRes = await pool.query(
            `SELECT sku, category, shape, weight, color, clarity, lab, origin,
                    measurements, image, additional_pictures,
                    certificate_number, certificate_image, comment,
                    price_per_carat, total_price
               FROM soap_stones WHERE sku = $1 LIMIT 1`,
            [row.sku]
          );
          let snap = null;
          let bruto = 0;
          let category = null;
          if (stoneRes.rows.length) {
            const s = stoneRes.rows[0];
            let img = s.image;
            if (!img && s.additional_pictures) {
              img = String(s.additional_pictures).split(';')[0]?.trim() || null;
            }
            bruto = Number(s.total_price) || 0;
            category = s.category;
            snap = {
              sku: s.sku, category: s.category, shape: s.shape,
              weightCt: s.weight ? Number(s.weight) : null,
              color: s.color, clarity: s.clarity, lab: s.lab, origin: s.origin,
              measurements: s.measurements,
              certificateNumber: s.certificate_number,
              certificateUrl: s.certificate_image,
              treatment: s.comment, imageUrl: img,
              pricePerCarat: Number(s.price_per_carat) || null,
              priceTotal: bruto || null,
            };
          } else {
            const jewRes = await pool.query(
              `SELECT model_number, jewelry_type, style, collection, metal_type,
                      total_carat, jewelry_weight, stone_type,
                      all_pictures_link, video_link, price
                 FROM jewelry_products WHERE model_number = $1 LIMIT 1`,
              [row.sku]
            );
            if (jewRes.rows.length) {
              const j = jewRes.rows[0];
              const img = j.all_pictures_link
                ? String(j.all_pictures_link).split(';').map((x) => x.trim()).filter(Boolean)[0] || null
                : null;
              bruto = Number(j.price) || 0;
              category = 'Jewelry';
              snap = {
                sku: j.model_number, category: 'Jewelry',
                jewelryType: j.jewelry_type, style: j.style,
                collection: j.collection, metalType: j.metal_type,
                totalCarat: j.total_carat ? Number(j.total_carat) : null,
                weightG: j.jewelry_weight ? Number(j.jewelry_weight) : null,
                stoneType: j.stone_type, imageUrl: img,
                video: j.video_link,
                priceTotal: bruto || null,
              };
            }
          }
          if (snap) {
            const neto = bruto ? Math.round(bruto / 2) : null;
            await pool.query(
              `UPDATE crm_deal_items
                  SET snapshot = $2::jsonb,
                      category = COALESCE(category, $3),
                      custom_price = COALESCE(custom_price, $4)
                WHERE id = $1`,
              [row.id, JSON.stringify(snap), category, neto]
            );
            if (neto != null) {
              await pool.query(
                `UPDATE crm_deals SET value = $2, updated_at = NOW()
                  WHERE id = $1 AND COALESCE(value, 0) = 0`,
                [row.deal_id, neto]
              );
            }
          }
        } catch (rowErr) {
          console.warn(`DNA backfill row ${row.id} (${row.sku}) failed:`, rowErr.message);
        }
      }
      if (todo.rows.length) console.log(`DNA backfill: processed ${todo.rows.length} item(s)`);
    } catch (e) {
      console.warn("DNA backfill warn:", e.message);
    }

    crmReady = true;
    console.log("CRM tables ready");
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
    const {
      userId, search, type, status, folderId, country, city, company,
      hasEmail, hasPhone, hasWebsite, lastContactDays, createdSince, createdUntil, tag,
    } = req.query;
    if (!userId) return res.status(400).json({ error: "userId is required" });

    // Broadcast: each user sees their own contacts AND any contact flagged as shared
    // (currently used for DNA-lead inquiries that arrive from the public DNA page).
    const conditions = ["(user_id = $1 OR shared = TRUE)"];
    const values = [userId];
    let idx = 2;

    if (search) {
      conditions.push(`(name ILIKE $${idx} OR company ILIKE $${idx} OR phone ILIKE $${idx} OR email ILIKE $${idx} OR title ILIKE $${idx})`);
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
    if (folderId) {
      if (folderId === 'unfiled') {
        conditions.push(`folder_id IS NULL`);
      } else {
        // Include sub-folders recursively
        conditions.push(`folder_id IN (
          WITH RECURSIVE descendants AS (
            SELECT id FROM crm_folders WHERE id = $${idx} AND user_id = $1
            UNION ALL
            SELECT f.id FROM crm_folders f INNER JOIN descendants d ON f.parent_id = d.id
          )
          SELECT id FROM descendants
        )`);
        values.push(parseInt(folderId, 10));
        idx++;
      }
    }
    if (country) { conditions.push(`country ILIKE $${idx++}`); values.push(country); }
    if (city) { conditions.push(`city ILIKE $${idx++}`); values.push(city); }
    if (company) { conditions.push(`company ILIKE $${idx++}`); values.push(`%${company}%`); }
    if (hasEmail === 'true') conditions.push(`email IS NOT NULL AND email <> ''`);
    if (hasEmail === 'false') conditions.push(`(email IS NULL OR email = '')`);
    if (hasPhone === 'true') conditions.push(`phone IS NOT NULL AND phone <> ''`);
    if (hasPhone === 'false') conditions.push(`(phone IS NULL OR phone = '')`);
    if (hasWebsite === 'true') conditions.push(`website IS NOT NULL AND website <> ''`);
    if (hasWebsite === 'false') conditions.push(`(website IS NULL OR website = '')`);
    if (lastContactDays) {
      const d = parseInt(lastContactDays, 10);
      if (!Number.isNaN(d)) {
        conditions.push(`(last_contact_at IS NULL OR last_contact_at < NOW() - INTERVAL '${d} days')`);
      }
    }
    if (createdSince) { conditions.push(`created_at >= $${idx++}`); values.push(createdSince); }
    if (createdUntil) { conditions.push(`created_at <= $${idx++}`); values.push(createdUntil); }
    if (tag) {
      conditions.push(`tags @> $${idx++}::jsonb`);
      values.push(JSON.stringify([tag]));
    }

    let result;
    try {
      // Lean payload: only fields the list/cards/filters need.
      // Heavy fields (notes, address, preferences, linked_contact_ids, card images, dates other than updated_at)
      // are fetched on demand from /api/crm/contacts/:id.
      // Single LEFT JOIN with one aggregate query replaces 2 per-row correlated subqueries
      // (was O(N×M); now ~O(N+M)).
      // The list endpoint NEVER returns card_image_thumb (heavy base64 ~5-50KB each).
      // The FE fetches thumbnails in a separate, batched, background request via /api/crm/contacts/thumbs.
      result = await pool.query(
        `SELECT
            c.id, c.user_id, c.name, c.type, c.title, c.company,
            c.phone, c.email, c.website,
            c.country, c.city,
            c.source, c.status, c.tags,
            c.folder_id,
            c.dna_sku, c.shared,
            (c.card_image_front IS NOT NULL) AS has_card_front,
            (c.card_image_back IS NOT NULL) AS has_card_back,
            (c.card_image_thumb IS NOT NULL) AS has_card_thumb,
            c.last_contact_at, c.updated_at,
            f.name AS folder_name,
            COALESCE(da.deals_count, 0) AS deals_count,
            COALESCE(da.total_won, 0) AS total_won
         FROM crm_contacts c
         LEFT JOIN crm_folders f ON f.id = c.folder_id
         LEFT JOIN (
           SELECT contact_id,
                  COUNT(*)::int AS deals_count,
                  COALESCE(SUM(CASE WHEN stage = 'won' THEN value ELSE 0 END), 0) AS total_won
             FROM crm_deals
            GROUP BY contact_id
         ) da ON da.contact_id = c.id
         WHERE ${conditions.join(" AND ")}
         ORDER BY c.updated_at DESC
         LIMIT 2000`,
        values
      );
    } catch (joinErr) {
      // Self-heal: maybe the new columns/tables aren't present yet. Re-run migrations and fall back.
      console.warn("Contacts JOIN failed, attempting self-heal:", joinErr.message);
      try {
        await pool.query(`CREATE TABLE IF NOT EXISTS crm_folders (
          id SERIAL PRIMARY KEY,
          user_id TEXT NOT NULL,
          name TEXT NOT NULL,
          parent_id INTEGER REFERENCES crm_folders(id) ON DELETE CASCADE,
          color TEXT,
          created_at TIMESTAMP DEFAULT NOW(),
          updated_at TIMESTAMP DEFAULT NOW()
        )`);
        await pool.query(`ALTER TABLE crm_contacts ADD COLUMN IF NOT EXISTS title TEXT`);
        await pool.query(`ALTER TABLE crm_contacts ADD COLUMN IF NOT EXISTS website TEXT`);
        await pool.query(`ALTER TABLE crm_contacts ADD COLUMN IF NOT EXISTS phone_alt TEXT`);
        await pool.query(`ALTER TABLE crm_contacts ADD COLUMN IF NOT EXISTS folder_id INTEGER`);
        await pool.query(`ALTER TABLE crm_contacts ADD COLUMN IF NOT EXISTS linked_contact_ids JSONB DEFAULT '[]'`);
        await pool.query(`ALTER TABLE crm_contacts ADD COLUMN IF NOT EXISTS card_back_notes TEXT`);
        await pool.query(`ALTER TABLE crm_contacts ADD COLUMN IF NOT EXISTS card_image_front TEXT`);
        await pool.query(`ALTER TABLE crm_contacts ADD COLUMN IF NOT EXISTS card_image_back TEXT`);
        await pool.query(`ALTER TABLE crm_contacts ADD COLUMN IF NOT EXISTS card_image_thumb TEXT`);
      } catch (healErr) {
        console.error("Self-heal failed:", healErr.message);
      }
      // Fallback query without the folder join (still safe even if folder_id is missing).
      // Uses c.* so it works regardless of which optional columns exist.
      result = await pool.query(
        `SELECT c.*,
          NULL::text AS folder_name,
          COALESCE(da.deals_count, 0) AS deals_count,
          COALESCE(da.total_won, 0) AS total_won
         FROM crm_contacts c
         LEFT JOIN (
           SELECT contact_id,
                  COUNT(*)::int AS deals_count,
                  COALESCE(SUM(CASE WHEN stage = 'won' THEN value ELSE 0 END), 0) AS total_won
             FROM crm_deals
            GROUP BY contact_id
         ) da ON da.contact_id = c.id
         WHERE ${conditions.join(" AND ")}
         ORDER BY c.updated_at DESC
         LIMIT 2000`,
        values
      );
    }
    res.json(result.rows);
  } catch (error) {
    console.error("Error fetching contacts:", error.stack || error.message || error);
    res.status(500).json({ error: error.message || "Internal server error" });
  }
});

app.get("/api/crm/contacts/:id", async (req, res) => {
  try {
    const { id } = req.params;
    const { userId } = req.query;
    if (!userId) return res.status(400).json({ error: "userId is required" });

    const contact = await pool.query(
      "SELECT * FROM crm_contacts WHERE id = $1 AND (user_id = $2 OR shared = TRUE)",
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
    const {
      userId, name, type, title, company, phone, phoneAlt, email, website,
      country, city, address, source, status, tags, preferences, notes, avatarUrl,
      folderId, linkedContactIds, cardBackNotes,
      cardImageFront, cardImageBack, cardImageThumb,
    } = req.body;
    if (!userId || !name) return res.status(400).json({ error: "userId and name are required" });

    const result = await pool.query(
      `INSERT INTO crm_contacts (
         user_id, name, type, title, company, phone, phone_alt, email, website,
         country, city, address, source, status, tags, preferences, notes, avatar_url,
         folder_id, linked_contact_ids, card_back_notes,
         card_image_front, card_image_back, card_image_thumb
       )
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24)
       RETURNING id, user_id, name, type, title, company, phone, phone_alt, email, website,
                 country, city, address, source, status, tags, preferences, notes, avatar_url,
                 folder_id, linked_contact_ids, card_back_notes, card_image_thumb,
                 (card_image_front IS NOT NULL) AS has_card_front,
                 (card_image_back IS NOT NULL) AS has_card_back,
                 last_contact_at, created_at, updated_at`,
      [
        userId, name.trim(), type || 'lead', title || null, company || null,
        phone || null, phoneAlt || null, email || null, website || null,
        country || null, city || null, address || null, source || null, status || 'active',
        JSON.stringify(tags || []), JSON.stringify(preferences || {}),
        notes || null, avatarUrl || null,
        folderId || null, JSON.stringify(linkedContactIds || []), cardBackNotes || null,
        cardImageFront || null, cardImageBack || null, cardImageThumb || null,
      ]
    );
    res.status(201).json(result.rows[0]);
  } catch (error) {
    console.error("Error creating contact:", error);
    res.status(500).json({ error: error.message || "Internal server error" });
  }
});

app.put("/api/crm/contacts/:id", async (req, res) => {
  try {
    const { id } = req.params;
    const allowed = [
      'name','type','title','company','phone','phone_alt','email','website',
      'country','city','address','source','status','tags','preferences','notes','avatar_url','last_contact_at',
      'folder_id','linked_contact_ids','card_back_notes',
      'card_image_front','card_image_back','card_image_thumb'
    ];
    const fields = [];
    const values = [];
    let idx = 1;

    for (const key of allowed) {
      const camel = key.replace(/_([a-z])/g, (_,c) => c.toUpperCase());
      if (req.body[camel] !== undefined) {
        if (key === 'tags' || key === 'preferences' || key === 'linked_contact_ids') {
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
      "DELETE FROM crm_contacts WHERE (user_id = $1 OR shared = TRUE) AND id = ANY($2::int[]) RETURNING id",
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
           WHERE (user_id = $2 OR shared = TRUE) AND id = ANY($3::int[])
           RETURNING id`
        : `UPDATE crm_contacts
             SET tags = COALESCE((SELECT jsonb_agg(elem) FROM jsonb_array_elements(tags) elem WHERE elem <> $1::jsonb), '[]'::jsonb),
             updated_at = NOW()
           WHERE (user_id = $2 OR shared = TRUE) AND id = ANY($3::int[])
           RETURNING id`;

    const result = await pool.query(sql, [JSON.stringify(safeTag), userId, ids.map(Number)]);
    res.json({ success: true, updated: result.rowCount });
  } catch (error) {
    console.error("Bulk tag error:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

// GET /api/health — tiny wake-up endpoint used by FE to defrost cold Render instances
app.get("/api/health", (req, res) => {
  res.set("Cache-Control", "no-store");
  res.json({ ok: true, ts: Date.now() });
});

/* ---------- Lazy thumbnail loader (batched) ---------- */
// GET /api/crm/contacts/thumbs?ids=1,2,3
// Returns [{ id, thumb }] only for IDs that actually have a thumb.
// Used by the contacts list UI to render thumbnails in the background
// after the (lean) list payload has already painted.
app.get("/api/crm/contacts/thumbs", async (req, res) => {
  try {
    const ids = String(req.query.ids || "")
      .split(",")
      .map((x) => parseInt(x, 10))
      .filter((x) => Number.isFinite(x));
    if (ids.length === 0) return res.json([]);
    // Cap to keep payloads sane
    const capped = ids.slice(0, 200);
    const r = await pool.query(
      `SELECT id, card_image_thumb AS thumb
         FROM crm_contacts
        WHERE id = ANY($1::int[])
          AND card_image_thumb IS NOT NULL`,
      [capped]
    );
    // Cache hint: thumbs change rarely
    res.set("Cache-Control", "private, max-age=60");
    res.json(r.rows);
  } catch (error) {
    console.error("Thumbs batch error:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

/* =========================================================
   DNA → CRM bridge — public endpoints (no Clerk auth required)
   ========================================================= */

// Simple in-memory rate limiter: max 3 submissions per IP per minute
const dnaLeadHits = new Map(); // ip -> [timestamps]
const DNA_RATE_LIMIT = 3;
const DNA_RATE_WINDOW_MS = 60_000;

const checkDnaRateLimit = (ip) => {
  const now = Date.now();
  const arr = (dnaLeadHits.get(ip) || []).filter((t) => now - t < DNA_RATE_WINDOW_MS);
  if (arr.length >= DNA_RATE_LIMIT) return false;
  arr.push(now);
  dnaLeadHits.set(ip, arr);
  return true;
};

const normalisePhone = (s) => String(s || "").replace(/[^0-9]/g, "");
const normaliseEmail = (s) => String(s || "").trim().toLowerCase();
const cleanString = (s, max = 200) => String(s || "").trim().slice(0, max);
const titleCase = (s) => String(s || "")
  .split(/\s+/)
  .filter(Boolean)
  .map((w) => w[0]?.toUpperCase() + w.slice(1).toLowerCase())
  .join(" ");

// POST /api/crm/dna-lead — Public DNA "I'm interested" form
app.post("/api/crm/dna-lead", async (req, res) => {
  try {
    const ip = (req.headers["x-forwarded-for"] || req.socket?.remoteAddress || "unknown")
      .toString().split(",")[0].trim();

    if (!checkDnaRateLimit(ip)) {
      return res.status(429).json({ error: "Too many submissions. Please try again in a minute." });
    }

    const {
      firstName, lastName, email, phone, company, title, message, sku, snapshot, hp,
    } = req.body || {};

    // Honeypot — bots fill this hidden field; humans don't see it
    if (hp) return res.status(200).json({ success: true }); // pretend success, drop silently

    const cleanFirst = cleanString(firstName, 80);
    const cleanLast = cleanString(lastName, 80);
    const cleanEmail = normaliseEmail(email).slice(0, 200);
    const cleanPhone = cleanString(phone, 60);
    const cleanCompany = cleanString(company, 200);
    const cleanTitle = cleanString(title, 120);
    const cleanMessage = cleanString(message, 1000);
    const cleanSku = cleanString(sku, 60);

    if (!cleanFirst && !cleanLast) {
      return res.status(400).json({ error: "Name is required" });
    }
    if (!cleanEmail && !cleanPhone) {
      return res.status(400).json({ error: "Email or phone is required" });
    }
    if (cleanEmail && !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(cleanEmail)) {
      return res.status(400).json({ error: "Email is not valid" });
    }

    const fullName = titleCase([cleanFirst, cleanLast].filter(Boolean).join(" "));
    const phoneNorm = normalisePhone(cleanPhone);

    // ---- Find existing shared contact by email or normalised phone ----
    let contact = null;
    if (cleanEmail) {
      const r = await pool.query(
        `SELECT * FROM crm_contacts WHERE shared = TRUE AND LOWER(email) = $1 LIMIT 1`,
        [cleanEmail]
      );
      if (r.rows.length) contact = r.rows[0];
    }
    if (!contact && phoneNorm) {
      const r = await pool.query(
        `SELECT * FROM crm_contacts
           WHERE shared = TRUE
             AND regexp_replace(COALESCE(phone,''),'[^0-9]','','g') = $1
           LIMIT 1`,
        [phoneNorm]
      );
      if (r.rows.length) contact = r.rows[0];
    }

    let isNew = false;
    if (contact) {
      // Update missing fields only — never overwrite human-edited data
      contact = (await pool.query(
        `UPDATE crm_contacts SET
            name = CASE WHEN COALESCE(NULLIF(name,''),'') = '' THEN $2 ELSE name END,
            email = COALESCE(NULLIF(email,''), $3),
            phone = COALESCE(NULLIF(phone,''), $4),
            company = COALESCE(NULLIF(company,''), $5),
            title = COALESCE(NULLIF(title,''), $6),
            dna_sku = COALESCE(dna_sku, $7),
            last_contact_at = NOW(),
            updated_at = NOW()
          WHERE id = $1 RETURNING *`,
        [contact.id, fullName, cleanEmail || null, cleanPhone || null, cleanCompany || null, cleanTitle || null, cleanSku || null]
      )).rows[0];
    } else {
      isNew = true;
      contact = (await pool.query(
        `INSERT INTO crm_contacts
           (user_id, shared, name, type, email, phone, company, title, source, dna_sku, tags, last_contact_at)
         VALUES ($1, TRUE, $2, 'lead', $3, $4, $5, $6, 'dna_lead', $7, $8::jsonb, NOW())
         RETURNING *`,
        [
          'dna_public',                      // sentinel user_id; the row is shared anyway
          fullName,
          cleanEmail || null,
          cleanPhone || null,
          cleanCompany || null,
          cleanTitle || null,
          cleanSku || null,
          JSON.stringify(['DNA Lead']),
        ]
      )).rows[0];
    }

    // ---- Look the SKU up in real inventory so the deal item carries
    //      a real image, real bruto price, and trustworthy specs.
    //      We try stones first, then jewelry.
    let realCategory = snapshot?.category || null;
    let realSnapshot = { ...(snapshot || {}) };
    let brutoPrice = 0;

    if (cleanSku) {
      try {
        const stoneRes = await pool.query(
          `SELECT sku, category, shape, weight, color, clarity, lab, origin,
                  measurements, image, additional_pictures, video,
                  certificate_number, certificate_image, comment,
                  price_per_carat, total_price
             FROM soap_stones WHERE sku = $1 LIMIT 1`,
          [cleanSku]
        );

        if (stoneRes.rows.length) {
          const s = stoneRes.rows[0];
          let img = s.image;
          if (!img && s.additional_pictures) {
            const first = String(s.additional_pictures).split(';')[0];
            img = first ? first.trim() : null;
          }
          brutoPrice = Number(s.total_price) || 0;
          realCategory = s.category || realCategory;
          realSnapshot = {
            sku: s.sku,
            category: s.category,
            shape: s.shape,
            weightCt: s.weight ? Number(s.weight) : null,
            color: s.color,
            clarity: s.clarity,
            lab: s.lab,
            origin: s.origin,
            measurements: s.measurements,
            certificateNumber: s.certificate_number,
            certificateUrl: s.certificate_image,
            treatment: s.comment,
            imageUrl: img,
            video: s.video,
            pricePerCarat: Number(s.price_per_carat) || null,
            priceTotal: brutoPrice || null,
          };
        } else {
          // Try jewelry
          const jewRes = await pool.query(
            `SELECT model_number, jewelry_type, style, collection, metal_type,
                    total_carat, jewelry_weight, stone_type,
                    all_pictures_link, video_link, price
               FROM jewelry_products WHERE model_number = $1 LIMIT 1`,
            [cleanSku]
          );
          if (jewRes.rows.length) {
            const j = jewRes.rows[0];
            const firstImg = j.all_pictures_link
              ? String(j.all_pictures_link).split(';').map((x) => x.trim()).filter(Boolean)[0] || null
              : null;
            brutoPrice = Number(j.price) || 0;
            realCategory = realCategory || 'Jewelry';
            realSnapshot = {
              sku: j.model_number,
              category: 'Jewelry',
              jewelryType: j.jewelry_type,
              style: j.style,
              collection: j.collection,
              metalType: j.metal_type,
              totalCarat: j.total_carat ? Number(j.total_carat) : null,
              weightG: j.jewelry_weight ? Number(j.jewelry_weight) : null,
              stoneType: j.stone_type,
              imageUrl: firstImg,
              video: j.video_link,
              priceTotal: brutoPrice || null,
            };
          }
        }
      } catch (lookupErr) {
        console.warn('DNA lead inventory lookup failed:', lookupErr.message);
      }
    }

    // Net price (display default — same convention used everywhere else in the app)
    const netoPrice = brutoPrice ? Math.round(brutoPrice / 2) : 0;

    // ---- Create the deal in 'lead' stage ----
    const dealTitle = cleanSku
      ? `DNA inquiry · ${cleanSku}`
      : `DNA inquiry · ${fullName}`;

    const deal = (await pool.query(
      `INSERT INTO crm_deals
         (user_id, contact_id, title, stage, value, currency, notes, shared, dna_sku)
       VALUES ($1, $2, $3, 'lead', $4, 'USD', $5, TRUE, $6)
       RETURNING *`,
      [
        'dna_public',
        contact.id,
        dealTitle,
        netoPrice,
        cleanMessage || null,
        cleanSku || null,
      ]
    )).rows[0];

    // Attach the stone to the deal as a deal_item with real snapshot + a default custom price (neto)
    if (cleanSku) {
      await pool.query(
        `INSERT INTO crm_deal_items (deal_id, sku, category, snapshot, custom_price)
         VALUES ($1, $2, $3, $4::jsonb, $5)`,
        [deal.id, cleanSku, realCategory, JSON.stringify(realSnapshot), netoPrice || null]
      );
    }

    // Log an interaction so it appears in the timeline
    const subject = `Inquiry from DNA${cleanSku ? ` (${cleanSku})` : ''}`;
    const content = [
      cleanMessage,
      cleanSku ? `Stone: ${cleanSku}` : null,
      `IP: ${ip}`,
    ].filter(Boolean).join('\n');

    await pool.query(
      `INSERT INTO crm_interactions
         (user_id, contact_id, deal_id, type, direction, subject, content, metadata)
       VALUES ($1, $2, $3, 'dna_inquiry', 'incoming', $4, $5, $6::jsonb)`,
      ['dna_public', contact.id, deal.id, subject, content, JSON.stringify({ source: 'dna', sku: cleanSku, snapshot })]
    );

    res.status(201).json({
      success: true,
      isNew,
      contactId: contact.id,
      dealId: deal.id,
    });
  } catch (error) {
    console.error("DNA lead error:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

// GET /api/crm/dna-leads/unread-count?since=ISO  → { count, latest }
// Lets the CRM sidebar show a badge with new DNA leads
app.get("/api/crm/dna-leads/unread-count", async (req, res) => {
  try {
    const { since } = req.query;
    const sinceDate = since ? new Date(since) : new Date(Date.now() - 7 * 24 * 60 * 60 * 1000);
    if (isNaN(sinceDate.getTime())) {
      return res.status(400).json({ error: "Invalid 'since' timestamp" });
    }
    const r = await pool.query(
      `SELECT COUNT(*)::int AS count, MAX(created_at) AS latest
         FROM crm_contacts
         WHERE shared = TRUE AND source = 'dna_lead' AND created_at > $1`,
      [sinceDate.toISOString()]
    );
    res.json({ count: r.rows[0].count || 0, latest: r.rows[0].latest });
  } catch (error) {
    console.error("DNA unread count error:", error);
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

    // Broadcast: each user sees their own deals AND any deal flagged as shared (DNA leads)
    const conditions = ["(d.user_id = $1 OR d.shared = TRUE)"];
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
    const { userId, imageBase64, imageBase64Front, imageBase64Back } = req.body;
    if (!userId) return res.status(400).json({ error: "userId is required" });

    // Backward-compat: imageBase64 = single image; new: imageBase64Front + optional imageBase64Back
    const front = imageBase64Front || imageBase64;
    const back = imageBase64Back || null;

    if (!front) return res.status(400).json({ error: "Front image is required" });
    if (!process.env.OPENAI_API_KEY) {
      return res.status(500).json({ error: "OPENAI_API_KEY is not configured on the server" });
    }

    const toDataUrl = (b64) => b64.startsWith("data:") ? b64 : `data:image/jpeg;base64,${b64}`;

    const sideCount = back ? 2 : 1;
    const prompt = `You are an OCR assistant for business cards used at gem and jewelry trade shows.
You will be shown ${sideCount} image(s) of a business card${back ? " (front and back of the SAME physical card)" : ""}.

CRITICAL: Sometimes the two sides of a single card show TWO DIFFERENT PEOPLE (a partner / colleague, with their own name, title, phone and email). In that case you MUST return TWO contacts.
Otherwise (same person, or back contains only company info / extra phone / address / logo / language translation) return ONE contact and merge the data.

Return ONLY valid JSON in this exact shape:
{
  "contacts": [
    {
      "name": string|null,
      "title": string|null,
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
      "language": string|null,
      "side": "front"|"back"|"both"
    }
  ],
  "isTwoPeople": boolean,
  "reason": "1 short sentence explaining why one or two contacts"
}

Rules:
- "title" = job title (CEO, Sales Director, Designer, etc.). NEVER put the title inside "notes".
- Choose "type" by guessing from title/company (jeweler/designer = designer; wholesale/diamond dealer = dealer; supplier/manufacturer = supplier; retailer = buyer; otherwise lead).
- If multiple phones for the same person, put the main mobile in "phone" and the office in "phoneAlt".
- Keep phone numbers exactly as printed (with + and country code if present).
- "website" = the URL on the card (without http:// prefix is OK; we will normalise).
- "notes" = ONLY extra text that does not fit the other fields (e.g. "Specialises in Burmese rubies"). Never duplicate name/title/phone/email here.
- For ONE contact spanning both sides: set side="both" and merge fields (do not duplicate).
- For TWO different people: return two entries with side="front" and side="back".
- Output ONLY the JSON object, no markdown.`;

    const userContent = [{ type: "text", text: prompt }];
    userContent.push({ type: "image_url", image_url: { url: toDataUrl(front), detail: "high" } });
    if (back) {
      userContent.push({ type: "text", text: "Above is the FRONT side. Below is the BACK side of the same card:" });
      userContent.push({ type: "image_url", image_url: { url: toDataUrl(back), detail: "high" } });
    }

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
        messages: [{ role: "user", content: userContent }],
      }),
    });

    if (!aiRes.ok) {
      const errText = await aiRes.text();
      console.error("OpenAI error:", aiRes.status, errText);
      let friendly = `OCR provider error (${aiRes.status})`;
      if (aiRes.status === 429) {
        friendly = "OpenAI quota exceeded. Add credit at platform.openai.com/settings/organization/billing/overview.";
      } else if (aiRes.status === 401) {
        friendly = "OpenAI API key invalid or revoked.";
      } else if (aiRes.status === 400) {
        friendly = "OpenAI rejected the image. Try a smaller, clearer photo.";
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

    // Normalise -> always return contacts array
    let contacts = Array.isArray(parsed.contacts) ? parsed.contacts : [];
    if (contacts.length === 0) {
      // Backward-compat: maybe AI returned a flat object
      if (parsed.name || parsed.email || parsed.phone) {
        contacts = [{
          name: parsed.name || null,
          title: parsed.title || parsed.jobTitle || null,
          company: parsed.company || null,
          phone: parsed.phone || null,
          phoneAlt: parsed.phoneAlt || null,
          email: parsed.email || null,
          website: parsed.website || null,
          country: parsed.country || null,
          city: parsed.city || null,
          address: parsed.address || null,
          type: parsed.type || "lead",
          notes: parsed.notes || null,
          language: parsed.language || null,
          side: back ? "both" : "front",
        }];
      }
    }

    // Find matches per contact
    const enriched = [];
    for (const c of contacts) {
      let matches = [];
      const phoneDigits = normPhone(c.phone);
      const email = (c.email || "").toLowerCase().trim();
      if (phoneDigits || email) {
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
            `SELECT id, name, type, title, company, phone, email, country, city, last_contact_at
             FROM crm_contacts
             WHERE user_id = $1 AND (${conditions.join(" OR ")})
             ORDER BY updated_at DESC LIMIT 5`,
            values
          );
          matches = r.rows;
        }
      }
      enriched.push({ extracted: c, matches });
    }

    res.json({
      contacts: enriched,
      isTwoPeople: !!parsed.isTwoPeople && enriched.length > 1,
      reason: parsed.reason || null,
      // Backward-compat for old client
      extracted: enriched[0]?.extracted || null,
      matches: enriched[0]?.matches || [],
    });
  } catch (error) {
    console.error("Error scanning card:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

/* =========================================================
   CRM – Folders (hierarchical)
   ========================================================= */

app.get("/api/crm/folders", async (req, res) => {
  try {
    const { userId } = req.query;
    if (!userId) return res.status(400).json({ error: "userId is required" });
    const result = await pool.query(
      `SELECT f.*,
        (SELECT COUNT(*)::int FROM crm_contacts c WHERE c.folder_id = f.id) AS direct_count
       FROM crm_folders f
       WHERE f.user_id = $1
       ORDER BY f.name ASC`,
      [userId]
    );
    res.json(result.rows);
  } catch (error) {
    console.error("Folders fetch error:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

app.post("/api/crm/folders", async (req, res) => {
  try {
    const { userId, name, parentId, color } = req.body;
    if (!userId || !name) return res.status(400).json({ error: "userId and name are required" });
    const result = await pool.query(
      `INSERT INTO crm_folders (user_id, name, parent_id, color)
       VALUES ($1, $2, $3, $4) RETURNING *`,
      [userId, String(name).trim(), parentId || null, color || null]
    );
    res.status(201).json(result.rows[0]);
  } catch (error) {
    console.error("Folder create error:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

app.put("/api/crm/folders/:id", async (req, res) => {
  try {
    const { id } = req.params;
    const { name, parentId, color } = req.body;
    const fields = [];
    const values = [];
    let idx = 1;
    if (name !== undefined) { fields.push(`name = $${idx++}`); values.push(String(name).trim()); }
    if (parentId !== undefined) { fields.push(`parent_id = $${idx++}`); values.push(parentId || null); }
    if (color !== undefined) { fields.push(`color = $${idx++}`); values.push(color || null); }
    fields.push(`updated_at = NOW()`);
    if (fields.length === 1) return res.status(400).json({ error: "No fields to update" });

    // Prevent making a folder its own ancestor
    if (parentId) {
      const cyc = await pool.query(
        `WITH RECURSIVE descendants AS (
           SELECT id FROM crm_folders WHERE id = $1
           UNION ALL
           SELECT f.id FROM crm_folders f INNER JOIN descendants d ON f.parent_id = d.id
         )
         SELECT 1 FROM descendants WHERE id = $2`,
        [id, parentId]
      );
      if (cyc.rows.length > 0) return res.status(400).json({ error: "Cannot move a folder into its own descendant" });
    }

    values.push(id);
    const result = await pool.query(
      `UPDATE crm_folders SET ${fields.join(", ")} WHERE id = $${idx} RETURNING *`,
      values
    );
    if (result.rows.length === 0) return res.status(404).json({ error: "Folder not found" });
    res.json(result.rows[0]);
  } catch (error) {
    console.error("Folder update error:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

app.delete("/api/crm/folders/:id", async (req, res) => {
  try {
    const { id } = req.params;
    // ON DELETE CASCADE deletes children folders; ON DELETE SET NULL on contacts.folder_id moves contacts to root
    await pool.query("DELETE FROM crm_folders WHERE id = $1", [id]);
    res.json({ success: true });
  } catch (error) {
    console.error("Folder delete error:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

app.post("/api/crm/contacts/move-to-folder", async (req, res) => {
  try {
    const { userId, contactIds, folderId } = req.body;
    if (!userId || !Array.isArray(contactIds) || contactIds.length === 0) {
      return res.status(400).json({ error: "userId and non-empty contactIds required" });
    }
    const result = await pool.query(
      `UPDATE crm_contacts SET folder_id = $1, updated_at = NOW()
       WHERE user_id = $2 AND id = ANY($3::int[]) RETURNING id`,
      [folderId || null, userId, contactIds.map(Number)]
    );
    res.json({ success: true, updated: result.rowCount });
  } catch (error) {
    console.error("Move to folder error:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

/* =========================================================
   CRM – Title migration (extract titles from notes, one-time)
   ========================================================= */

app.post("/api/crm/contacts/migrate-titles", async (req, res) => {
  try {
    const { userId, dryRun } = req.body;
    if (!userId) return res.status(400).json({ error: "userId is required" });

    // Common job-title keywords (case-insensitive)
    const TITLE_PATTERNS = [
      /\b(CEO|CFO|COO|CTO|CMO|CIO|CSO|VP|SVP|EVP)\b/i,
      /\b(President|Vice President|Founder|Co[- ]?Founder|Owner|Partner|Managing Partner|Director|Managing Director|General Manager)\b/i,
      /\b(Sales (Director|Manager|Executive|Representative|Rep)|Account (Manager|Executive)|Business Development( Manager)?)\b/i,
      /\b(Designer|Senior Designer|Lead Designer|Creative Director|Art Director|Goldsmith|Jeweler|Gemologist|Appraiser|Polisher|Cutter|Setter)\b/i,
      /\b(Marketing (Director|Manager|Coordinator)|Brand Manager|PR Manager)\b/i,
      /\b(Buyer|Senior Buyer|Head Buyer|Procurement (Manager|Director))\b/i,
      /\b(Manager|Senior Manager|Head of [A-Za-z ]+|Chief [A-Za-z ]+)\b/i,
    ];

    const rows = await pool.query(
      `SELECT id, name, notes FROM crm_contacts
       WHERE user_id = $1 AND (title IS NULL OR title = '') AND notes IS NOT NULL AND notes <> ''`,
      [userId]
    );

    const updates = [];
    for (const r of rows.rows) {
      const lines = r.notes.split(/\r?\n/);
      let foundTitle = null;
      let remainingLines = [];
      for (const line of lines) {
        const trimmed = line.trim();
        if (!trimmed || foundTitle) { remainingLines.push(line); continue; }
        let matched = false;
        for (const pat of TITLE_PATTERNS) {
          const m = trimmed.match(pat);
          if (m) {
            // Use the whole line if short, else just the matched phrase
            foundTitle = trimmed.length <= 60 ? trimmed : m[0];
            matched = true;
            break;
          }
        }
        if (!matched) remainingLines.push(line);
      }
      if (foundTitle) {
        const newNotes = remainingLines.join("\n").replace(/\n{3,}/g, "\n\n").trim() || null;
        updates.push({ id: r.id, name: r.name, title: foundTitle, newNotes });
      }
    }

    if (!dryRun) {
      for (const u of updates) {
        await pool.query(
          `UPDATE crm_contacts SET title = $1, notes = $2, updated_at = NOW() WHERE id = $3`,
          [u.title, u.newNotes, u.id]
        );
      }
    }

    res.json({
      total: rows.rows.length,
      migrated: updates.length,
      preview: updates.slice(0, 20).map(u => ({ id: u.id, name: u.name, title: u.title })),
      dryRun: !!dryRun,
    });
  } catch (error) {
    console.error("Title migration error:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

/* =========================================================
   CRM – Import contacts (preview + execute)
   ========================================================= */

const normEmail = (e) => (e || "").toLowerCase().trim();

app.post("/api/crm/contacts/import-preview", async (req, res) => {
  try {
    const { userId, rows } = req.body;
    if (!userId || !Array.isArray(rows)) return res.status(400).json({ error: "userId and rows array required" });

    // Fetch all existing contacts once for fast in-memory matching
    const existing = await pool.query(
      `SELECT id, name, company, phone, email FROM crm_contacts WHERE user_id = $1`,
      [userId]
    );
    const byPhone = new Map();
    const byEmail = new Map();
    for (const e of existing.rows) {
      const ph = normPhone(e.phone);
      const em = normEmail(e.email);
      if (ph && ph.length >= 7) byPhone.set(ph.slice(-9), e);
      if (em) byEmail.set(em, e);
    }

    const preview = rows.map((r, idx) => {
      const ph = normPhone(r.phone);
      const em = normEmail(r.email);
      let match = null;
      if (em && byEmail.has(em)) match = byEmail.get(em);
      else if (ph && ph.length >= 7 && byPhone.has(ph.slice(-9))) match = byPhone.get(ph.slice(-9));
      return {
        rowIdx: idx,
        data: r,
        match,
        action: match ? "merge" : "create", // default suggestion
      };
    });

    res.json({
      total: rows.length,
      duplicates: preview.filter(p => p.match).length,
      newCount: preview.filter(p => !p.match).length,
      preview,
    });
  } catch (error) {
    console.error("Import preview error:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

app.post("/api/crm/contacts/import-execute", async (req, res) => {
  try {
    const { userId, rows, defaultFolderId } = req.body;
    // rows: [{ data, action: 'create'|'merge'|'skip', matchId? }, ...]
    if (!userId || !Array.isArray(rows)) return res.status(400).json({ error: "userId and rows array required" });

    let created = 0, merged = 0, skipped = 0, failed = 0;
    const errors = [];

    for (const r of rows) {
      try {
        if (r.action === "skip") { skipped++; continue; }
        const d = r.data || {};
        if (!d.name) { skipped++; continue; }

        if (r.action === "merge" && r.matchId) {
          // Update only fields that are currently empty on the existing contact
          const existing = await pool.query(`SELECT * FROM crm_contacts WHERE id = $1 AND user_id = $2`, [r.matchId, userId]);
          if (existing.rows.length === 0) { skipped++; continue; }
          const cur = existing.rows[0];
          const setFields = [];
          const setVals = [];
          let idx = 1;
          const fillIfEmpty = (col, camel) => {
            if (d[camel] && (cur[col] === null || cur[col] === "")) {
              setFields.push(`${col} = $${idx++}`);
              setVals.push(d[camel]);
            }
          };
          fillIfEmpty("title", "title");
          fillIfEmpty("company", "company");
          fillIfEmpty("phone", "phone");
          fillIfEmpty("phone_alt", "phoneAlt");
          fillIfEmpty("email", "email");
          fillIfEmpty("website", "website");
          fillIfEmpty("country", "country");
          fillIfEmpty("city", "city");
          fillIfEmpty("address", "address");
          if (d.notes) {
            const newNotes = (cur.notes || "") + (cur.notes ? "\n---\n" : "") + d.notes;
            setFields.push(`notes = $${idx++}`); setVals.push(newNotes);
          }
          if (defaultFolderId && !cur.folder_id) {
            setFields.push(`folder_id = $${idx++}`); setVals.push(defaultFolderId);
          }
          if (setFields.length > 0) {
            setFields.push(`updated_at = NOW()`);
            setVals.push(r.matchId);
            await pool.query(`UPDATE crm_contacts SET ${setFields.join(", ")} WHERE id = $${idx}`, setVals);
          }
          merged++;
        } else {
          await pool.query(
            `INSERT INTO crm_contacts (
               user_id, name, type, title, company, phone, phone_alt, email, website,
               country, city, address, source, status, tags, notes, folder_id
             ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)`,
            [
              userId, d.name, d.type || "lead", d.title || null, d.company || null,
              d.phone || null, d.phoneAlt || null, d.email || null, d.website || null,
              d.country || null, d.city || null, d.address || null, d.source || "import", "active",
              JSON.stringify(d.tags || []), d.notes || null, defaultFolderId || null,
            ]
          );
          created++;
        }
      } catch (e) {
        failed++;
        errors.push({ rowIdx: r.rowIdx, error: e.message });
      }
    }

    res.json({ created, merged, skipped, failed, errors });
  } catch (error) {
    console.error("Import execute error:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

/* =========================================================
   CRM – Email broadcast (Resend)
   ========================================================= */

app.post("/api/crm/email/send-broadcast", async (req, res) => {
  try {
    const { userId, contactIds, subject, html, text, fromName, replyTo, dryRun, provider } = req.body;
    if (!userId || !Array.isArray(contactIds) || contactIds.length === 0) {
      return res.status(400).json({ error: "userId and contactIds required" });
    }
    if (!subject || (!html && !text)) {
      return res.status(400).json({ error: "subject and (html or text) required" });
    }

    const sendProvider = provider === "outlook" ? "outlook" : "resend";

    let outlookAccessToken = null;
    if (sendProvider === "outlook") {
      try {
        outlookAccessToken = await getValidOutlookToken(userId);
      } catch (e) {
        return res.status(400).json({ error: e.message });
      }
    } else {
      if (!process.env.RESEND_API_KEY) {
        return res.status(500).json({
          error: "RESEND_API_KEY is not configured. Sign up at resend.com (free 3,000/month), add the API key to your server environment, and try again."
        });
      }
    }

    const fromEmail = process.env.RESEND_FROM_EMAIL || "onboarding@resend.dev";
    const senderName = (fromName || "GEMS DNA").replace(/[\r\n]/g, "");
    const fromHeader = `${senderName} <${fromEmail}>`;

    // Fetch recipients
    const recipientsRes = await pool.query(
      `SELECT id, name, email, company, title FROM crm_contacts
       WHERE user_id = $1 AND id = ANY($2::int[]) AND email IS NOT NULL AND email <> ''`,
      [userId, contactIds.map(Number)]
    );
    const recipients = recipientsRes.rows;

    if (recipients.length === 0) {
      return res.status(400).json({ error: "None of the selected contacts have an email address." });
    }

    if (dryRun) {
      return res.json({
        dryRun: true,
        provider: sendProvider,
        wouldSend: recipients.length,
        recipients: recipients.map(r => ({ id: r.id, name: r.name, email: r.email })),
      });
    }

    const personalize = (template, c) => {
      if (!template) return template;
      const firstName = (c.name || "").split(/\s+/)[0] || "";
      return template
        .replace(/\{\{?\s*name\s*\}?\}/gi, c.name || "")
        .replace(/\{\{?\s*firstName\s*\}?\}/gi, firstName)
        .replace(/\{\{?\s*company\s*\}?\}/gi, c.company || "")
        .replace(/\{\{?\s*title\s*\}?\}/gi, c.title || "");
    };

    const details = [];
    let sent = 0, failed = 0;

    for (const r of recipients) {
      try {
        const subj = personalize(subject, r);
        const personalizedHtml = html ? personalize(html, r) : null;
        const personalizedText = text ? personalize(text, r) : null;

        let ok = false, errTxt = "";

        if (sendProvider === "outlook") {
          const sendRes = await fetch("https://graph.microsoft.com/v1.0/me/sendMail", {
            method: "POST",
            headers: { Authorization: `Bearer ${outlookAccessToken}`, "Content-Type": "application/json" },
            body: JSON.stringify({
              message: {
                subject: subj,
                body: { contentType: personalizedHtml ? "HTML" : "Text", content: personalizedHtml || personalizedText },
                toRecipients: [{ emailAddress: { address: r.email, name: r.name } }],
              },
              saveToSentItems: true,
            }),
          });
          ok = sendRes.ok;
          if (!ok) errTxt = (await sendRes.text()).slice(0, 300);
        } else {
          const body = {
            from: fromHeader,
            to: [r.email],
            subject: subj,
            ...(personalizedHtml ? { html: personalizedHtml } : {}),
            ...(personalizedText ? { text: personalizedText } : {}),
            ...(replyTo ? { reply_to: replyTo } : {}),
          };
          const sendRes = await fetch("https://api.resend.com/emails", {
            method: "POST",
            headers: {
              "Content-Type": "application/json",
              "Authorization": `Bearer ${process.env.RESEND_API_KEY}`,
            },
            body: JSON.stringify(body),
          });
          ok = sendRes.ok;
          if (!ok) errTxt = (await sendRes.text()).slice(0, 300);
        }

        if (!ok) {
          failed++;
          details.push({ contactId: r.id, email: r.email, status: "failed", error: errTxt });
        } else {
          sent++;
          details.push({ contactId: r.id, email: r.email, status: "sent" });
          await pool.query(
            `INSERT INTO crm_interactions (user_id, contact_id, type, direction, subject, content, metadata)
             VALUES ($1, $2, 'email', 'outgoing', $3, $4, $5)`,
            [userId, r.id, subj, personalizedText, JSON.stringify({ broadcast: true, provider: sendProvider })]
          );
          await pool.query(`UPDATE crm_contacts SET last_contact_at = NOW(), updated_at = NOW() WHERE id = $1`, [r.id]);
        }
      } catch (e) {
        failed++;
        details.push({ contactId: r.id, email: r.email, status: "failed", error: e.message });
      }
    }

    // Save broadcast log
    await pool.query(
      `INSERT INTO crm_email_broadcasts (user_id, subject, body, recipients_count, sent_count, failed_count, details)
       VALUES ($1, $2, $3, $4, $5, $6, $7)`,
      [userId, subject, html || text, recipients.length, sent, failed, JSON.stringify({ provider: sendProvider, details })]
    );

    res.json({ sent, failed, total: recipients.length, provider: sendProvider, details });
  } catch (error) {
    console.error("Email broadcast error:", error);
    res.status(500).json({ error: error.message || "Internal server error" });
  }
});

app.get("/api/crm/email/broadcasts", async (req, res) => {
  try {
    const { userId } = req.query;
    if (!userId) return res.status(400).json({ error: "userId is required" });
    const result = await pool.query(
      `SELECT id, subject, recipients_count, sent_count, failed_count, sent_at
       FROM crm_email_broadcasts WHERE user_id = $1 ORDER BY sent_at DESC LIMIT 50`,
      [userId]
    );
    res.json(result.rows);
  } catch (error) {
    console.error("Broadcast log fetch error:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

/* =========================================================
   CRM – Email templates (saved HTML templates per user)
   ========================================================= */

app.get("/api/crm/email/templates", async (req, res) => {
  try {
    const { userId } = req.query;
    if (!userId) return res.status(400).json({ error: "userId is required" });
    const result = await pool.query(
      `SELECT id, name, subject, html, thumbnail, created_at, updated_at
       FROM crm_email_templates WHERE user_id = $1 ORDER BY updated_at DESC`,
      [userId]
    );
    res.json(result.rows);
  } catch (error) {
    console.error("Email templates fetch error:", error);
    res.status(500).json({ error: error.message || "Internal server error" });
  }
});

app.post("/api/crm/email/templates", async (req, res) => {
  try {
    const { userId, name, subject, html, thumbnail } = req.body || {};
    if (!userId || !name) return res.status(400).json({ error: "userId and name are required" });
    const result = await pool.query(
      `INSERT INTO crm_email_templates (user_id, name, subject, html, thumbnail)
       VALUES ($1, $2, $3, $4, $5)
       RETURNING id, name, subject, html, thumbnail, created_at, updated_at`,
      [userId, name, subject || "", html || "", thumbnail || null]
    );
    res.json(result.rows[0]);
  } catch (error) {
    console.error("Email template create error:", error);
    res.status(500).json({ error: error.message || "Internal server error" });
  }
});

app.put("/api/crm/email/templates/:id", async (req, res) => {
  try {
    const { id } = req.params;
    const { name, subject, html, thumbnail } = req.body || {};
    const fields = [];
    const values = [];
    let i = 1;
    if (name !== undefined) { fields.push(`name = $${i++}`); values.push(name); }
    if (subject !== undefined) { fields.push(`subject = $${i++}`); values.push(subject); }
    if (html !== undefined) { fields.push(`html = $${i++}`); values.push(html); }
    if (thumbnail !== undefined) { fields.push(`thumbnail = $${i++}`); values.push(thumbnail); }
    if (fields.length === 0) return res.status(400).json({ error: "No fields to update" });
    fields.push(`updated_at = NOW()`);
    values.push(id);
    const result = await pool.query(
      `UPDATE crm_email_templates SET ${fields.join(", ")} WHERE id = $${i}
       RETURNING id, name, subject, html, thumbnail, created_at, updated_at`,
      values
    );
    if (result.rows.length === 0) return res.status(404).json({ error: "Template not found" });
    res.json(result.rows[0]);
  } catch (error) {
    console.error("Email template update error:", error);
    res.status(500).json({ error: error.message || "Internal server error" });
  }
});

app.delete("/api/crm/email/templates/:id", async (req, res) => {
  try {
    const { id } = req.params;
    await pool.query(`DELETE FROM crm_email_templates WHERE id = $1`, [id]);
    res.json({ success: true });
  } catch (error) {
    console.error("Email template delete error:", error);
    res.status(500).json({ error: error.message || "Internal server error" });
  }
});

/* =========================================================
   CRM – Outlook / Microsoft Graph integration
   ========================================================= */

const OUTLOOK_TENANT = process.env.OUTLOOK_TENANT || "common";
const OUTLOOK_CLIENT_ID = process.env.OUTLOOK_CLIENT_ID;
const OUTLOOK_CLIENT_SECRET = process.env.OUTLOOK_CLIENT_SECRET;
const OUTLOOK_REDIRECT_URI = process.env.OUTLOOK_REDIRECT_URI; // e.g. https://gems-dna-be.onrender.com/api/crm/outlook/callback
const FRONTEND_URL = process.env.FRONTEND_URL || "https://gems-dna-fe.vercel.app";
const OUTLOOK_SCOPES = [
  "offline_access",
  "User.Read",
  "Contacts.ReadWrite",
  "Mail.Send",
  "Mail.Read",
];

const outlookConfigured = () => !!(OUTLOOK_CLIENT_ID && OUTLOOK_CLIENT_SECRET && OUTLOOK_REDIRECT_URI);

// Save (encrypted) integration record. Upsert by (user_id, provider).
const saveIntegration = async ({ userId, provider, accessToken, refreshToken, expiresIn, scope, accountEmail, accountName, metadata }) => {
  const expiresAt = expiresIn ? new Date(Date.now() + (expiresIn - 60) * 1000) : null;
  const accessEnc = accessToken ? encrypt(accessToken) : null;
  const refreshEnc = refreshToken ? encrypt(refreshToken) : null;
  await pool.query(
    `INSERT INTO crm_integrations (user_id, provider, account_email, account_name, access_token_enc, refresh_token_enc, expires_at, scope, metadata)
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
     ON CONFLICT (user_id, provider) DO UPDATE SET
       account_email = EXCLUDED.account_email,
       account_name = EXCLUDED.account_name,
       access_token_enc = EXCLUDED.access_token_enc,
       refresh_token_enc = COALESCE(EXCLUDED.refresh_token_enc, crm_integrations.refresh_token_enc),
       expires_at = EXCLUDED.expires_at,
       scope = EXCLUDED.scope,
       metadata = EXCLUDED.metadata,
       updated_at = NOW()`,
    [userId, provider, accountEmail || null, accountName || null, accessEnc, refreshEnc, expiresAt, scope || null, JSON.stringify(metadata || {})]
  );
};

const getIntegration = async (userId, provider) => {
  const r = await pool.query(
    `SELECT * FROM crm_integrations WHERE user_id = $1 AND provider = $2 LIMIT 1`,
    [userId, provider]
  );
  if (r.rows.length === 0) return null;
  const row = r.rows[0];
  return {
    ...row,
    access_token: decrypt(row.access_token_enc),
    refresh_token: decrypt(row.refresh_token_enc),
  };
};

// Refresh access token if expired
const getValidOutlookToken = async (userId) => {
  const integ = await getIntegration(userId, "outlook");
  if (!integ) throw new Error("Outlook is not connected for this user");
  if (!integ.access_token) throw new Error("Outlook tokens are missing — please reconnect");

  const expiresSoon = !integ.expires_at || new Date(integ.expires_at).getTime() < Date.now() + 30000;
  if (!expiresSoon) return integ.access_token;

  if (!integ.refresh_token) throw new Error("Outlook session expired — please reconnect");

  const tokenRes = await fetch(`https://login.microsoftonline.com/${OUTLOOK_TENANT}/oauth2/v2.0/token`, {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: new URLSearchParams({
      client_id: OUTLOOK_CLIENT_ID,
      client_secret: OUTLOOK_CLIENT_SECRET,
      grant_type: "refresh_token",
      refresh_token: integ.refresh_token,
      scope: OUTLOOK_SCOPES.join(" "),
    }).toString(),
  });
  if (!tokenRes.ok) {
    const err = await tokenRes.text();
    throw new Error(`Failed to refresh Outlook token: ${err.slice(0, 300)}`);
  }
  const td = await tokenRes.json();
  await saveIntegration({
    userId,
    provider: "outlook",
    accessToken: td.access_token,
    refreshToken: td.refresh_token || integ.refresh_token,
    expiresIn: td.expires_in,
    scope: td.scope || integ.scope,
    accountEmail: integ.account_email,
    accountName: integ.account_name,
  });
  return td.access_token;
};

// 1. Auth URL
app.get("/api/crm/outlook/auth-url", (req, res) => {
  if (!outlookConfigured()) {
    return res.status(500).json({
      error: "Outlook is not configured on the server. The administrator needs to set OUTLOOK_CLIENT_ID, OUTLOOK_CLIENT_SECRET and OUTLOOK_REDIRECT_URI."
    });
  }
  const { userId } = req.query;
  if (!userId) return res.status(400).json({ error: "userId is required" });

  const state = encrypt(JSON.stringify({ userId, ts: Date.now() }));
  const params = new URLSearchParams({
    client_id: OUTLOOK_CLIENT_ID,
    response_type: "code",
    redirect_uri: OUTLOOK_REDIRECT_URI,
    response_mode: "query",
    scope: OUTLOOK_SCOPES.join(" "),
    state,
    prompt: "select_account",
  });
  const url = `https://login.microsoftonline.com/${OUTLOOK_TENANT}/oauth2/v2.0/authorize?${params.toString()}`;
  res.json({ url });
});

// 2. Callback (Microsoft redirects here)
app.get("/api/crm/outlook/callback", async (req, res) => {
  const { code, state, error: authErr, error_description } = req.query;
  const redirectBack = (status, msg) => {
    const u = new URL(`${FRONTEND_URL}/crm/settings`);
    u.searchParams.set("outlook", status);
    if (msg) u.searchParams.set("msg", String(msg).slice(0, 200));
    res.redirect(u.toString());
  };

  if (authErr) return redirectBack("error", error_description || authErr);
  if (!code || !state) return redirectBack("error", "Missing code or state");

  try {
    const decoded = JSON.parse(decrypt(state));
    const userId = decoded.userId;
    if (!userId) return redirectBack("error", "Invalid state");
    if (Date.now() - decoded.ts > 10 * 60 * 1000) return redirectBack("error", "Auth link expired");

    // Exchange code for tokens
    const tokenRes = await fetch(`https://login.microsoftonline.com/${OUTLOOK_TENANT}/oauth2/v2.0/token`, {
      method: "POST",
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      body: new URLSearchParams({
        client_id: OUTLOOK_CLIENT_ID,
        client_secret: OUTLOOK_CLIENT_SECRET,
        grant_type: "authorization_code",
        code,
        redirect_uri: OUTLOOK_REDIRECT_URI,
        scope: OUTLOOK_SCOPES.join(" "),
      }).toString(),
    });
    if (!tokenRes.ok) {
      const t = await tokenRes.text();
      console.error("Outlook token exchange failed:", t);
      return redirectBack("error", `Token exchange failed (${tokenRes.status})`);
    }
    const td = await tokenRes.json();

    // Get user profile (email + name)
    let accountEmail = null, accountName = null;
    try {
      const meRes = await fetch("https://graph.microsoft.com/v1.0/me", {
        headers: { Authorization: `Bearer ${td.access_token}` },
      });
      if (meRes.ok) {
        const me = await meRes.json();
        accountEmail = me.mail || me.userPrincipalName || null;
        accountName = me.displayName || null;
      }
    } catch (_) {}

    await saveIntegration({
      userId,
      provider: "outlook",
      accessToken: td.access_token,
      refreshToken: td.refresh_token,
      expiresIn: td.expires_in,
      scope: td.scope,
      accountEmail,
      accountName,
    });

    return redirectBack("connected", accountEmail || "Outlook account connected");
  } catch (e) {
    console.error("Outlook callback error:", e);
    return redirectBack("error", e.message);
  }
});

// 3. Status
app.get("/api/crm/outlook/status", async (req, res) => {
  try {
    const { userId } = req.query;
    if (!userId) return res.status(400).json({ error: "userId is required" });
    const integ = await getIntegration(userId, "outlook");
    if (!integ) return res.json({ connected: false, configured: outlookConfigured() });
    res.json({
      connected: true,
      configured: outlookConfigured(),
      accountEmail: integ.account_email,
      accountName: integ.account_name,
      lastSyncAt: integ.last_sync_at,
      expiresAt: integ.expires_at,
    });
  } catch (e) {
    console.error("Outlook status error:", e);
    res.status(500).json({ error: e.message });
  }
});

// 4. Disconnect
app.post("/api/crm/outlook/disconnect", async (req, res) => {
  try {
    const { userId } = req.body;
    if (!userId) return res.status(400).json({ error: "userId is required" });
    await pool.query(`DELETE FROM crm_integrations WHERE user_id = $1 AND provider = 'outlook'`, [userId]);
    res.json({ success: true });
  } catch (e) {
    console.error("Outlook disconnect error:", e);
    res.status(500).json({ error: e.message });
  }
});

/* ----- Two-way contact sync ----- */
const outlookContactToCrm = (oc) => {
  const phones = [...(oc.businessPhones || []), ...(oc.homePhones || []), oc.mobilePhone].filter(Boolean);
  const emails = (oc.emailAddresses || []).map((e) => e.address).filter(Boolean);
  const addr = (oc.businessAddress || oc.homeAddress || {});
  return {
    name: oc.displayName || [oc.givenName, oc.surname].filter(Boolean).join(" ") || (emails[0] || "Unnamed"),
    title: oc.jobTitle || null,
    company: oc.companyName || null,
    phone: phones[0] || null,
    phoneAlt: phones[1] || null,
    email: emails[0] || null,
    website: oc.businessHomePage || (oc.websites && oc.websites[0]?.address) || null,
    country: addr.countryOrRegion || null,
    city: addr.city || null,
    address: [addr.street, addr.postalCode].filter(Boolean).join(", ") || null,
    notes: oc.personalNotes || null,
  };
};

const crmContactToOutlook = (c) => {
  const out = { displayName: c.name };
  const [given, ...rest] = (c.name || "").split(/\s+/);
  if (given) out.givenName = given;
  if (rest.length) out.surname = rest.join(" ");
  if (c.title) out.jobTitle = c.title;
  if (c.company) out.companyName = c.company;
  const businessPhones = [c.phone, c.phone_alt].filter(Boolean);
  if (businessPhones.length) out.businessPhones = businessPhones;
  if (c.email) out.emailAddresses = [{ address: c.email, name: c.name }];
  if (c.website) out.businessHomePage = c.website;
  if (c.notes) out.personalNotes = c.notes;
  if (c.country || c.city || c.address) {
    out.businessAddress = {
      ...(c.address ? { street: c.address } : {}),
      ...(c.city ? { city: c.city } : {}),
      ...(c.country ? { countryOrRegion: c.country } : {}),
    };
  }
  return out;
};

app.post("/api/crm/outlook/sync-contacts", async (req, res) => {
  try {
    const { userId, direction } = req.body;
    if (!userId) return res.status(400).json({ error: "userId is required" });
    const dir = direction || "two-way"; // 'pull' | 'push' | 'two-way'

    const accessToken = await getValidOutlookToken(userId);

    let pulledNew = 0, pulledUpdated = 0, pushedNew = 0, pushedUpdated = 0;

    /* ---- PULL Outlook -> CRM ---- */
    if (dir === "pull" || dir === "two-way") {
      let url = "https://graph.microsoft.com/v1.0/me/contacts?$top=100&$select=id,displayName,givenName,surname,jobTitle,companyName,businessPhones,homePhones,mobilePhone,emailAddresses,businessAddress,homeAddress,businessHomePage,personalNotes,lastModifiedDateTime";
      let pages = 0;
      while (url && pages < 20) {
        const r = await fetch(url, { headers: { Authorization: `Bearer ${accessToken}` } });
        if (!r.ok) {
          const t = await r.text();
          throw new Error(`Outlook contacts fetch failed (${r.status}): ${t.slice(0, 200)}`);
        }
        const data = await r.json();
        for (const oc of (data.value || [])) {
          const mapped = outlookContactToCrm(oc);
          // Find existing by outlook_contact_id, then by email/phone
          const existRes = await pool.query(
            `SELECT id, name, title, company, phone, phone_alt, email, website, country, city, address, notes
               FROM crm_contacts
              WHERE user_id = $1 AND (
                outlook_contact_id = $2
                OR (LOWER(email) = LOWER($3) AND $3 <> '' AND $3 IS NOT NULL)
              ) LIMIT 1`,
            [userId, oc.id, mapped.email || ""]
          );
          if (existRes.rows.length === 0) {
            await pool.query(
              `INSERT INTO crm_contacts (
                 user_id, name, type, title, company, phone, phone_alt, email, website,
                 country, city, address, source, status, notes, outlook_contact_id, outlook_synced_at
               ) VALUES ($1,$2,'lead',$3,$4,$5,$6,$7,$8,$9,$10,$11,'outlook','active',$12,$13,NOW())`,
              [userId, mapped.name, mapped.title, mapped.company, mapped.phone, mapped.phoneAlt,
               mapped.email, mapped.website, mapped.country, mapped.city, mapped.address, mapped.notes, oc.id]
            );
            pulledNew++;
          } else {
            // Fill empty fields only (non-destructive merge)
            const cur = existRes.rows[0];
            const sets = [];
            const vals = [];
            let i = 1;
            const fillIfEmpty = (col, val) => {
              if (val && (cur[col] === null || cur[col] === "")) {
                sets.push(`${col} = $${i++}`); vals.push(val);
              }
            };
            fillIfEmpty("title", mapped.title);
            fillIfEmpty("company", mapped.company);
            fillIfEmpty("phone", mapped.phone);
            fillIfEmpty("phone_alt", mapped.phoneAlt);
            fillIfEmpty("email", mapped.email);
            fillIfEmpty("website", mapped.website);
            fillIfEmpty("country", mapped.country);
            fillIfEmpty("city", mapped.city);
            fillIfEmpty("address", mapped.address);
            // Always update the link
            sets.push(`outlook_contact_id = $${i++}`); vals.push(oc.id);
            sets.push(`outlook_synced_at = NOW()`);
            sets.push(`updated_at = NOW()`);
            vals.push(cur.id);
            await pool.query(`UPDATE crm_contacts SET ${sets.join(", ")} WHERE id = $${i}`, vals);
            pulledUpdated++;
          }
        }
        url = data["@odata.nextLink"] || null;
        pages++;
      }
    }

    /* ---- PUSH CRM -> Outlook ---- */
    if (dir === "push" || dir === "two-way") {
      // Push CRM contacts that have an email and either no outlook_contact_id, or were updated since last sync
      const pushRows = await pool.query(
        `SELECT * FROM crm_contacts
          WHERE user_id = $1
            AND (
              outlook_contact_id IS NULL
              OR (outlook_synced_at IS NULL OR updated_at > outlook_synced_at)
            )
            AND name IS NOT NULL
          ORDER BY updated_at DESC
          LIMIT 200`,
        [userId]
      );
      for (const c of pushRows.rows) {
        const body = JSON.stringify(crmContactToOutlook(c));
        try {
          if (c.outlook_contact_id) {
            const u = `https://graph.microsoft.com/v1.0/me/contacts/${encodeURIComponent(c.outlook_contact_id)}`;
            const r = await fetch(u, {
              method: "PATCH",
              headers: { Authorization: `Bearer ${accessToken}`, "Content-Type": "application/json" },
              body,
            });
            if (r.ok) {
              await pool.query(`UPDATE crm_contacts SET outlook_synced_at = NOW() WHERE id = $1`, [c.id]);
              pushedUpdated++;
            }
          } else {
            const r = await fetch("https://graph.microsoft.com/v1.0/me/contacts", {
              method: "POST",
              headers: { Authorization: `Bearer ${accessToken}`, "Content-Type": "application/json" },
              body,
            });
            if (r.ok) {
              const created = await r.json();
              await pool.query(
                `UPDATE crm_contacts SET outlook_contact_id = $1, outlook_synced_at = NOW() WHERE id = $2`,
                [created.id, c.id]
              );
              pushedNew++;
            }
          }
        } catch (e) {
          console.warn("Push contact failed:", c.id, e.message);
        }
      }
    }

    await pool.query(
      `UPDATE crm_integrations SET last_sync_at = NOW(), updated_at = NOW() WHERE user_id = $1 AND provider = 'outlook'`,
      [userId]
    );

    res.json({ direction: dir, pulledNew, pulledUpdated, pushedNew, pushedUpdated });
  } catch (e) {
    console.error("Outlook sync error:", e);
    res.status(500).json({ error: e.message });
  }
});

/* ----- Send a single email through Outlook ----- */
app.post("/api/crm/outlook/send-email", async (req, res) => {
  try {
    const { userId, to, subject, html, text } = req.body;
    if (!userId || !to || !subject || (!html && !text)) {
      return res.status(400).json({ error: "userId, to, subject and html|text are required" });
    }
    const accessToken = await getValidOutlookToken(userId);
    const recipients = (Array.isArray(to) ? to : [to]).map((addr) => ({ emailAddress: { address: addr } }));
    const r = await fetch("https://graph.microsoft.com/v1.0/me/sendMail", {
      method: "POST",
      headers: { Authorization: `Bearer ${accessToken}`, "Content-Type": "application/json" },
      body: JSON.stringify({
        message: {
          subject,
          body: { contentType: html ? "HTML" : "Text", content: html || text },
          toRecipients: recipients,
        },
        saveToSentItems: true,
      }),
    });
    if (!r.ok) {
      const t = await r.text();
      return res.status(502).json({ error: `Outlook send failed (${r.status}): ${t.slice(0, 200)}` });
    }
    res.json({ success: true });
  } catch (e) {
    console.error("Outlook send error:", e);
    res.status(500).json({ error: e.message });
  }
});

/* ----- Import recent inbox emails as interactions ----- */
app.post("/api/crm/outlook/import-emails", async (req, res) => {
  try {
    const { userId, days } = req.body;
    if (!userId) return res.status(400).json({ error: "userId is required" });
    const lookbackDays = Math.min(Math.max(parseInt(days, 10) || 7, 1), 90);

    const accessToken = await getValidOutlookToken(userId);

    // Build email -> contact lookup map
    const contactsRes = await pool.query(
      `SELECT id, email FROM crm_contacts WHERE user_id = $1 AND email IS NOT NULL AND email <> ''`,
      [userId]
    );
    const emailToId = new Map();
    for (const c of contactsRes.rows) emailToId.set(c.email.toLowerCase().trim(), c.id);
    if (emailToId.size === 0) return res.json({ imported: 0, scanned: 0, message: "No contacts with email" });

    const since = new Date(Date.now() - lookbackDays * 86400 * 1000).toISOString();
    let scanned = 0, imported = 0;

    for (const folder of ["inbox", "sentitems"]) {
      let url = `https://graph.microsoft.com/v1.0/me/mailFolders/${folder}/messages?$top=50&$select=id,subject,bodyPreview,from,toRecipients,receivedDateTime,sentDateTime&$filter=receivedDateTime ge ${since}`;
      let pages = 0;
      while (url && pages < 5) {
        const r = await fetch(url, { headers: { Authorization: `Bearer ${accessToken}` } });
        if (!r.ok) break;
        const data = await r.json();
        for (const msg of (data.value || [])) {
          scanned++;
          const fromAddr = (msg.from?.emailAddress?.address || "").toLowerCase();
          const toAddrs = (msg.toRecipients || []).map((t) => (t.emailAddress?.address || "").toLowerCase());
          const direction = folder === "sentitems" ? "outgoing" : "incoming";
          const counterpart = direction === "outgoing" ? toAddrs : [fromAddr];
          for (const addr of counterpart) {
            const cId = emailToId.get(addr);
            if (!cId) continue;
            // Avoid duplicates by metadata.outlookMessageId
            const dupe = await pool.query(
              `SELECT 1 FROM crm_interactions WHERE contact_id = $1 AND metadata->>'outlookMessageId' = $2 LIMIT 1`,
              [cId, msg.id]
            );
            if (dupe.rows.length > 0) continue;
            await pool.query(
              `INSERT INTO crm_interactions (user_id, contact_id, type, direction, subject, content, metadata, occurred_at)
               VALUES ($1, $2, 'email', $3, $4, $5, $6, $7)`,
              [
                userId, cId, direction,
                msg.subject || null,
                msg.bodyPreview || null,
                JSON.stringify({ outlookMessageId: msg.id, source: "outlook" }),
                msg.receivedDateTime || msg.sentDateTime || new Date().toISOString(),
              ]
            );
            await pool.query(`UPDATE crm_contacts SET last_contact_at = NOW(), updated_at = NOW() WHERE id = $1`, [cId]);
            imported++;
          }
        }
        url = data["@odata.nextLink"] || null;
        pages++;
      }
    }

    res.json({ imported, scanned, lookbackDays });
  } catch (e) {
    console.error("Outlook import-emails error:", e);
    res.status(500).json({ error: e.message });
  }
});

/* =========================================================
   Start server
   ========================================================= */
app.listen(port, () => {
  console.log(`🚀 Server is running on http://localhost:${port}`);
});
