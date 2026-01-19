// Upload DATA from BARAK SOAP API to Neon DB into soap_stones table

const { fetchSoapData } = require("../../utils/soapClient");
const { parseXml } = require("../../utils/xmlParser");
const { pool } = require("../../db/client");

const CHUNK_SIZE = 300; // ⭐ הכי יציב

// Branch mapping for consistent location names
const BRANCH_MAP = {
  HK: "Hong Kong",
  HKG: "Hong Kong",
  IL: "Israel",
  ISR: "Israel",
  NY: "New York",
  NYC: "New York",
  LA: "Los Angeles",
};

// Helper function to map branch
const mapBranch = (branch) => {
  if (!branch) return null;
  const upperBranch = branch.toUpperCase().trim();
  return BRANCH_MAP[upperBranch] || branch; // Return mapped value or original if not found
};

const run = async () => {
  try {
    console.log("🚀 [1/6] Fetching SOAP data...");
    const rawXml = await fetchSoapData();

    if (!rawXml) {
      console.log("❌ No XML received");
      return;
    }

    console.log("📦 [2/6] Parsing XML...");
    const parsed = await parseXml(rawXml);
    const stones = parsed?.Stock?.Stone;

    if (!stones) {
      console.log("❌ No <Stone> elements found");
      return;
    }

    const stoneArray = Array.isArray(stones) ? stones : [stones];
    console.log(`📊 [3/6] Total stones found: ${stoneArray.length}`);

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

    console.log("🧹 [4/6] Clearing soap_stones table...");
    await pool.query("TRUNCATE TABLE soap_stones RESTART IDENTITY");

    console.log("🧱 [5/6] Inserting chunks of data...");

    // --------------------------
    //  INSERT IN CHUNKS
    // --------------------------
    for (let i = 0; i < values.length; i += CHUNK_SIZE) {
      const chunk = values.slice(i, i + CHUNK_SIZE);

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
        `➡️  Inserting chunk ${i / CHUNK_SIZE + 1} (${chunk.length} stones)...`
      );

      await pool.query(insertQuery, flatValues);
    }

    console.log(`🎉 DONE! Inserted ${stoneArray.length} stones successfully.`);
  } catch (err) {
    console.error("❌ Error:", err);
  } finally {
    await pool.end().catch(() => {});
  }
};

run();
