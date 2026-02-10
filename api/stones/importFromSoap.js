// Upload DATA from BARAK SOAP API to Neon DB into soap_stones table

const { fetchSoapData } = require("../../utils/soapClient");
const { parseXml } = require("../../utils/xmlParser");
const { pool } = require("../../db/client");

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
 * Run the SOAP import
 * @param {object} options
 * @param {object} options.dbPool - Optional database pool to use (if called from server)
 * @param {boolean} options.closePool - Whether to close the pool when done (default: true)
 * @param {function} options.onProgress - Optional progress callback: ({ phase, progress, detail, totalStones, processedStones })
 * @returns {Promise<{success: boolean, count: number, message: string}>}
 */
const run = async (options = {}) => {
  const dbPool = options.dbPool || pool;
  const closePool = options.closePool !== undefined ? options.closePool : true;
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
    const values = stoneArray.map((stone) => {
      const safeNumber = (value) => {
        const n = parseFloat(value);
        return Number.isFinite(n) ? n : null;
      };

      // 💰 הכפלת מחירים x2
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
        pricePerCarat !== null ? pricePerCarat * 2 : null,  // 💰 x2
        safeNumber(stone.RapPrice),
        safeNumber(stone["Rap.Price"]),
        totalPrice !== null ? totalPrice * 2 : null,        // 💰 x2
        stone.Location || null,
        mapBranch(stone.Branch),  // 🗺️ Map branch to consistent names
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
        null, // raw_xml
      ];
    });

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
      "cert_comments", "origin", "raw_xml",
    ];

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

    onProgress({ phase: 'complete', progress: 100, detail: `Successfully synced ${stoneArray.length} stones!`, totalStones: stoneArray.length, processedStones: stoneArray.length });
    console.log(`🎉 DONE! Inserted ${stoneArray.length} stones successfully.`);
    return { success: true, count: stoneArray.length, message: `Successfully synced ${stoneArray.length} stones` };
  } catch (err) {
    console.error("❌ Error:", err);
    return { success: false, count: 0, message: err.message || "Unknown error during sync" };
  } finally {
    if (closePool) {
      await pool.end().catch(() => {});
    }
  }
};

// Export run function for use by server.js
module.exports = { run };

// Only auto-run when executed directly (node importFromSoap.js)
if (require.main === module) {
  run().then((result) => {
    console.log("📋 Result:", result);
    process.exit(result.success ? 0 : 1);
  });
}
