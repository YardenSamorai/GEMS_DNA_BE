// Upload DATA from BARAK SOAP API to Neon DB into soap_stones table

const { fetchSoapData } = require("../../utils/soapClient");
const { parseXml } = require("../../utils/xmlParser");
const { pool } = require("../../db/client");
const { cleanText, snapshotPreserved, restorePreserved } = require("../../utils/preserveFields");

const CHUNK_SIZE = 300; // ⭐ הכי יציב

// Branch mapping for consistent location names
const BRANCH_MAP = {
  // Israel
  IL: "Israel",
  EM: "Israel",
  JI: "Israel",
  
  // Los Angeles
  LA: "Los Angeles",
  EL: "Los Angeles",
  
  // Hong Kong
  HK: "Hong Kong",
  ES: "Hong Kong",
  HS: "Hong Kong",
  JH: "Hong Kong",
  JS: "Hong Kong",
  EH: "Hong Kong",
  
  // New York
  NY: "New York",
  EN: "New York",
  ET: "New York",
  DT: "New York",
  JT: "New York",
  EG: "New York",
  EV: "New York",
  GN: "New York",
  VG: "New York",
  JG: "New York",
  JV: "New York",
  EY: "New York",
  
  // Legacy mappings (for backwards compatibility)
  HKG: "Hong Kong",
  ISR: "Israel",
  NYC: "New York",
};

// Helper function to map branch
const mapBranch = (branch) => {
  if (!branch) return null;
  
  // Clean and trim
  const cleanBranch = branch.trim();
  
  // Validate: if it looks like a URL or is too long, it's probably wrong data
  if (cleanBranch.includes('http://') || cleanBranch.includes('https://') || cleanBranch.length > 20) {
    console.warn(`⚠️  Invalid branch value detected (likely URL or corrupted data): "${cleanBranch}"`);
    return null;
  }
  
  const upperBranch = cleanBranch.toUpperCase();
  return BRANCH_MAP[upperBranch] || cleanBranch; // Return mapped value or original if not found
};

/**
 * Run the SOAP import (internal — use `run` which also records the sync_log row)
 * @param {object} options
 * @param {object} options.dbPool - Optional database pool to use (if called from server)
 * @param {function} options.onProgress - Optional progress callback: ({ phase, progress, detail, totalStones, processedStones })
 * @returns {Promise<{success: boolean, count: number, message: string}>}
 */
const runImport = async (options = {}) => {
  const dbPool = options.dbPool || pool;
  const onProgress = options.onProgress || (() => {});
  
  try {
    onProgress({ phase: 'connecting', progress: 5, detail: 'Connecting to SOAP API...', totalStones: 0, processedStones: 0 });
    console.log("🚀 [1/6] Fetching SOAP data...");
    const rawXml = await fetchSoapData();

    if (!rawXml) {
      console.log("❌ No XML received");
      onProgress({ phase: 'error', progress: 0, detail: 'No XML received from SOAP API', totalStones: 0, processedStones: 0 });
      return { success: false, count: 0, message: "No XML received from SOAP API" };
    }

    onProgress({ phase: 'parsing', progress: 20, detail: 'Parsing stone data...', totalStones: 0, processedStones: 0 });
    console.log("📦 [2/6] Parsing XML...");
    const parsed = await parseXml(rawXml);
    const stones = parsed?.Stock?.Stone;

    if (!stones) {
      console.log("❌ No <Stone> elements found");
      onProgress({ phase: 'error', progress: 0, detail: 'No stone data found in response', totalStones: 0, processedStones: 0 });
      return { success: false, count: 0, message: "No stone data found in SOAP response" };
    }

    const stoneArray = Array.isArray(stones) ? stones : [stones];
    console.log(`📊 [3/6] Total stones found: ${stoneArray.length}`);
    onProgress({ phase: 'processing', progress: 30, detail: `Found ${stoneArray.length} stones, processing...`, totalStones: stoneArray.length, processedStones: 0 });

    // --------------------------
    // BUILD VALUES FOR EACH ROW
    // --------------------------
    // Known clarity grades for shift detection
    const CLARITY_GRADES = /^(FL|IF|VVS[12]|VS[12]|SI[123]|I[123])$/i;

    let shiftFixCount = 0;

    const fixShiftedFields = (stone) => {
      // Detect: Color contains a quote AND Lab looks like a clarity grade
      const colorVal = String(stone.Color || '');
      const labVal = String(stone.Lab || '');
      if (!colorVal.includes('"') || !CLARITY_GRADES.test(labVal.trim())) return;

      shiftFixCount++;
      console.warn(`⚠️  Fixing shifted fields for ${stone.SKU}: color="${stone.Color}" → lab="${stone.Lab}"`);

      // Merge the broken color
      const fixedColor = (colorVal + ', ' + (stone.Clarity || '')).replace(/"/g, '').trim();

      // Shift all fields back by one, following the SOAP XML element order:
      // Color,Clarity,Lab,Fluorescence,PricePerCarat,RapPrice,Rap.Price,TotalPrice,
      // Location,Image,Video,Certificateimage,CertificateNumber,Cut,Polish,Symmetry,
      // Table,Depth,ratio,Measurements-delimiter,PairStone,fancy_intensity,fancy_color,
      // fancy_overtone,fancy_color_2,fancy_overtone_2,home_page,additional_pictures,
      // Branch,additional_videos,Comment,Type,Cert.Comments,certificateImageJPG,
      // Origin,TradeShow,GroupingType,Box,Stones
      stone.Color = fixedColor;
      stone.Clarity = stone.Lab;
      stone.Lab = stone.Fluorescence;
      stone.Fluorescence = stone.PricePerCarat;
      stone.PricePerCarat = stone.RapPrice;
      stone.RapPrice = stone["Rap.Price"];
      stone["Rap.Price"] = stone.TotalPrice;
      stone.TotalPrice = stone.Location;
      stone.Location = stone.Image;
      stone.Image = stone.Video;
      stone.Video = stone.Certificateimage;
      stone.Certificateimage = stone.CertificateNumber;
      stone.CertificateNumber = stone.Cut;
      stone.Cut = stone.Polish;
      stone.Polish = stone.Symmetry;
      stone.Symmetry = stone.Table;
      stone.Table = stone.Depth;
      stone.Depth = stone.ratio;
      stone.ratio = stone["Measurements-delimiter"];
      stone["Measurements-delimiter"] = stone.PairStone;
      stone.PairStone = stone.fancy_intensity;
      stone.fancy_intensity = stone.fancy_color;
      stone.fancy_color = stone.fancy_overtone;
      stone.fancy_overtone = stone.fancy_color_2;
      stone.fancy_color_2 = stone.fancy_overtone_2;
      stone.fancy_overtone_2 = stone.home_page;
      stone.home_page = stone.additional_pictures;
      stone.additional_pictures = stone.Branch;
      stone.Branch = stone.additional_videos;
      stone.additional_videos = stone.Comment;
      stone.Comment = stone.Type;
      stone.Type = stone["Cert.Comments"];
      stone["Cert.Comments"] = stone.certificateImageJPG;
      stone.certificateImageJPG = stone.Origin;
      stone.Origin = stone.TradeShow;
      stone.TradeShow = stone.GroupingType;
      stone.GroupingType = stone.Box;
      stone.Box = stone.Stones;
      stone.Stones = null;
    };

    const values = stoneArray.map((stone) => {
      const safeNumber = (value) => {
        const n = parseFloat(value);
        return Number.isFinite(n) ? n : null;
      };

      // Save raw field snapshot BEFORE any fixes for debugging
      const rawSnapshot = JSON.stringify(stone);

      // 🛡️ Fix shifted fields (e.g. Color with comma breaks SOAP XML parsing)
      fixShiftedFields(stone);

      // Handle xml2js object values: extract text content if field is an object with _ property
      const textVal = (v) => {
        if (v == null) return null;
        if (typeof v === 'object' && v._ !== undefined) return String(v._);
        if (typeof v === 'string') return v;
        return String(v);
      };

      // 🛡️ Detect tail-field shifts: if Stones contains a valid GroupingType
      //    instead of a number, fields are shifted forward and need correction
      const VALID_GROUPING_TYPES = ['Single', 'Pair', 'Set', 'Parcel', 'Fancy', 'Side Stones', 'Melee'];
      const isValidGT = (v) => v && VALID_GROUPING_TYPES.some(g => g.toLowerCase() === String(v).trim().toLowerCase());
      const normalizeGT = (v) => VALID_GROUPING_TYPES.find(g => g.toLowerCase() === String(v).trim().toLowerCase()) || v;

      const stonesStr = String(stone.Stones || '').trim();
      const boxStr = String(stone.Box || '').trim();
      if (stonesStr && isValidGT(stonesStr)) {
        // Shift by 2: GroupingType is in Stones, TradeShow is in Box, Origin is in GroupingType
        console.warn(`⚠️  Tail shift (2) for ${stone.SKU}: Stones="${stonesStr}" → GroupingType`);
        const realGroupingType = stonesStr;
        const realTradeShow = boxStr;
        const realOrigin = String(stone.GroupingType || '').trim();
        stone.Stones = null;
        stone.Box = null;
        stone.GroupingType = realGroupingType;
        if (realTradeShow && !stone.TradeShow) stone.TradeShow = realTradeShow;
        if (realOrigin && !stone.Origin) stone.Origin = realOrigin;
      } else if (boxStr && isValidGT(boxStr) && !isValidGT(String(stone.GroupingType || '').trim())) {
        // Shift by 1: GroupingType is in Box, TradeShow is in GroupingType
        console.warn(`⚠️  Tail shift (1) for ${stone.SKU}: Box="${boxStr}" → GroupingType`);
        const realGroupingType = boxStr;
        const realTradeShow = String(stone.GroupingType || '').trim();
        stone.Box = String(stone.Stones || '').trim() || null;
        stone.Stones = null;
        stone.GroupingType = realGroupingType;
        if (realTradeShow && !stone.TradeShow) stone.TradeShow = realTradeShow;
      }

      // Handle xml2js object values
      if (stone.GroupingType && typeof stone.GroupingType === 'object') {
        stone.GroupingType = textVal(stone.GroupingType);
      }

      // 🛡️ Validate GroupingType - normalize to standard casing
      const gtVal = stone.GroupingType ? String(stone.GroupingType).trim() : null;
      if (gtVal && !isValidGT(gtVal)) {
        console.warn(`⚠️  Invalid GroupingType "${gtVal}" for ${stone.SKU} → set to null`);
        stone.GroupingType = null;
      } else if (gtVal) {
        stone.GroupingType = normalizeGT(gtVal);
      }

      // 💰 הכפלת מחירים x2
      const pricePerCarat = safeNumber(stone.PricePerCarat);
      const totalPrice = safeNumber(stone.TotalPrice);

      return [
        cleanText(stone.Category),
        cleanText(stone.SKU),
        cleanText(stone.Shape),
        safeNumber(stone.Weight),
        cleanText(stone.Color),
        cleanText(stone.Clarity),
        cleanText(stone.Lab),
        cleanText(stone.Fluorescence),
        pricePerCarat,   // store source price as-is (no ×2)
        safeNumber(stone.RapPrice),
        safeNumber(stone["Rap.Price"]),
        totalPrice,      // store source price as-is (no ×2)
        cleanText(stone.Location),
        mapBranch(stone.Branch),  // 🗺️ Map branch to consistent names
        cleanText(stone.Image),
        cleanText(stone.additional_pictures),
        cleanText(stone.Video),
        cleanText(stone.additional_videos),
        cleanText(stone.Certificateimage),
        cleanText(stone.CertificateNumber),
        cleanText(stone.certificateImageJPG),
        cleanText(stone.Cut),
        cleanText(stone.Polish),
        cleanText(stone.Symmetry),
        safeNumber(stone.Table),
        safeNumber(stone.Depth),
        safeNumber(stone.ratio),
        cleanText(stone["Measurements-delimiter"]),
        cleanText(stone.fancy_intensity),
        cleanText(stone.fancy_color),
        cleanText(stone.fancy_overtone),
        cleanText(stone.fancy_color_2),
        cleanText(stone.fancy_overtone_2),
        cleanText(stone.PairStone),
        cleanText(stone.home_page),
        cleanText(stone.TradeShow),
        cleanText(stone.Comment),
        cleanText(stone.Type),
        cleanText(stone["Cert.Comments"]),
        cleanText(stone.Origin),
        cleanText(stone.GroupingType),
        cleanText(stone.Box),
        safeNumber(stone.Stones),
        rawSnapshot,
      ];
    });

    if (shiftFixCount > 0) {
      console.log(`🛡️  Fixed ${shiftFixCount} stone(s) with shifted fields in this chunk`);
    }

    // Log GroupingType distribution for verification
    const gtCounts = {};
    const debugSkus = ['T-310H', 'T310H'];
    values.forEach(row => {
      const gt = row[40] || 'NULL';
      gtCounts[gt] = (gtCounts[gt] || 0) + 1;

      const sku = row[1];
      if (sku && debugSkus.some(d => sku.toUpperCase().includes(d.replace('-', '')))) {
        const rawData = row[43]; // raw_xml column
        let rawParsed;
        try { rawParsed = JSON.parse(rawData); } catch { rawParsed = null; }
        const allGroupingKeys = rawParsed ? Object.keys(rawParsed).filter(k => k.toLowerCase().includes('group')) : [];
        console.log(`🔍 DEBUG ${sku}: grouping_type=${gt}, raw GroupingType keys=[${allGroupingKeys}], raw values=[${allGroupingKeys.map(k => rawParsed[k])}]`);
      }
    });
    console.log('📊 GroupingType distribution:', gtCounts);

    // עמודות לטבלה:
    const columns = [
      "category", "sku", "shape", "weight", "color", "clarity", "lab",
      "fluorescence", "price_per_carat", "rap_price", "rap_list_price",
      "total_price", "location", "branch", "image", "additional_pictures",
      "video", "additional_videos", "certificate_image", "certificate_number",
      "certificate_image_jpg", "cut", "polish", "symmetry", "table_percent",
      "depth_percent", "ratio", "measurements", "fancy_intensity",
      "fancy_color", "fancy_overtone", "fancy_color_2", "fancy_overtone_2",
      "pair_stone", "home_page", "trade_show", "comment", "type",
      "cert_comments", "origin", "grouping_type", "box", "stones", "raw_xml",
    ];

    // 🛟 Preserve enriched fields across the truncate so a SOAP sync never
    // wipes data that a CSV import (or a prior sync) carried but the live SOAP
    // feed lacks — e.g. colour/clarity on older stones, cost_per_carat, holder,
    // jewelry_model. SOAP still WINS for any field it actually provides; the
    // snapshot only fills the gaps. Shared with the CSV importer so both paths
    // behave identically.
    let preservedSalesFields = [];
    try {
      preservedSalesFields = await snapshotPreserved(dbPool);
      console.log(`🛟 Preserving enriched fields for ${preservedSalesFields.length} stones across sync`);
    } catch (e) {
      console.warn('⚠️  Could not snapshot preserved fields (continuing):', e.message);
    }

    onProgress({ phase: 'clearing', progress: 35, detail: 'Clearing old data...', totalStones: stoneArray.length, processedStones: 0 });
    console.log("🧹 [4/6] Clearing soap_stones table...");
    await dbPool.query("TRUNCATE TABLE soap_stones RESTART IDENTITY");

    console.log("🧱 [5/6] Inserting chunks of data...");
    const totalChunks = Math.ceil(values.length / CHUNK_SIZE);
    let processedStones = 0;

    // --------------------------
    //  INSERT IN CHUNKS
    // --------------------------
    for (let i = 0; i < values.length; i += CHUNK_SIZE) {
      const chunk = values.slice(i, i + CHUNK_SIZE);
      const chunkIndex = Math.floor(i / CHUNK_SIZE) + 1;

      const placeholders = chunk
        .map((row, rowIndex) => {
          const base = rowIndex * columns.length;
          return `(${columns
            .map((_, colIndex) => `$${base + colIndex + 1}`)
            .join(", ")})`;
        })
        .join(", ");

      const insertQuery = `
        INSERT INTO soap_stones (${columns.join(", ")})
        VALUES ${placeholders}
      `;

      const flatValues = chunk.flat();

      console.log(
        `➡️  Inserting chunk ${chunkIndex}/${totalChunks} (${chunk.length} stones)...`
      );

      await dbPool.query(insertQuery, flatValues);
      
      processedStones += chunk.length;
      // Progress: 35% (start of insert) to 95% (end of insert)
      const insertProgress = 35 + Math.round((chunkIndex / totalChunks) * 60);
      onProgress({ 
        phase: 'inserting', 
        progress: Math.min(insertProgress, 95), 
        detail: `Saving stones... (${processedStones}/${stoneArray.length})`, 
        totalStones: stoneArray.length, 
        processedStones 
      });
    }

    // 🛟 Re-apply the preserved fields. The freshly-synced SOAP value wins
    // whenever it is non-empty; otherwise the snapshot value is restored.
    if (preservedSalesFields.length) {
      console.log(`🛟 Restoring enriched fields for ${preservedSalesFields.length} stones...`);
      const restored = await restorePreserved(dbPool, preservedSalesFields, CHUNK_SIZE);
      console.log(`🛟 Restored enriched fields on ${restored} stones.`);
    }

    onProgress({ phase: 'complete', progress: 100, detail: `Successfully synced ${stoneArray.length} stones!`, totalStones: stoneArray.length, processedStones: stoneArray.length });
    console.log(`🎉 DONE! Inserted ${stoneArray.length} stones successfully.`);
    return { success: true, count: stoneArray.length, message: `Successfully synced ${stoneArray.length} stones` };
  } catch (err) {
    console.error("❌ Error:", err);
    return { success: false, count: 0, message: err.message || "Unknown error during sync" };
  }
};

// Append one row to sync_log for every run — the Dashboard's "Sync history"
// window reads this. Logging must never fail the sync itself.
const logSyncRun = async (dbPool, entry) => {
  try {
    await dbPool.query(`
      CREATE TABLE IF NOT EXISTS sync_log (
        id           SERIAL PRIMARY KEY,
        source       TEXT NOT NULL DEFAULT 'manual',
        success      BOOLEAN NOT NULL,
        stones_count INTEGER NOT NULL DEFAULT 0,
        message      TEXT,
        duration_ms  INTEGER,
        started_at   TIMESTAMPTZ NOT NULL,
        finished_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
      )
    `);
    await dbPool.query(
      `INSERT INTO sync_log (source, success, stones_count, message, duration_ms, started_at)
       VALUES ($1, $2, $3, $4, $5, $6)`,
      [entry.source, entry.success, entry.count, entry.message, entry.durationMs, entry.startedAt]
    );
    console.log(`📝 sync_log recorded (${entry.source}, success=${entry.success}, count=${entry.count})`);
  } catch (e) {
    console.warn("⚠️  Could not write sync_log entry:", e.message);
  }
};

/**
 * Run the SOAP import and record the outcome in sync_log.
 * @param {object} options
 * @param {object} options.dbPool - Optional database pool to use (if called from server)
 * @param {boolean} options.closePool - Whether to close the pool when done (default: true)
 * @param {string} options.source - 'manual' (Sync button) or 'cron' (scheduled job)
 * @param {function} options.onProgress - Optional progress callback
 * @returns {Promise<{success: boolean, count: number, message: string}>}
 */
const run = async (options = {}) => {
  const dbPool = options.dbPool || pool;
  const closePool = options.closePool !== undefined ? options.closePool : true;
  const source = options.source || "manual";
  const startedAt = new Date();

  let result;
  try {
    result = await runImport(options);
  } catch (err) {
    result = { success: false, count: 0, message: err.message || "Unknown error during sync" };
  }

  await logSyncRun(dbPool, {
    source,
    success: result.success,
    count: result.count,
    message: result.message,
    durationMs: Date.now() - startedAt.getTime(),
    startedAt,
  });

  if (closePool) {
    await pool.end().catch(() => {});
  }
  return result;
};

// Export run function for use by server.js
module.exports = { run };

// Only auto-run when executed directly (the Render Cron Job runs
// `node api/stones/importFromSoap.js`)
if (require.main === module) {
  run({ source: "cron" }).then((result) => {
    console.log("📋 Result:", result);
    process.exit(result.success ? 0 : 1);
  });
}
