const express = require("express");
const cors = require("cors");
const { Pool } = require("pg");
const dotenv = require('dotenv');
const path = require('path');
const CryptoJS = require('crypto-js'); 

dotenv.config({ path: path.resolve(__dirname, '../../.env') });

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
function encrypt(text) {
  return CryptoJS.AES.encrypt(text, process.env.ENCRYPT_SECRET).toString();
}


// 🔹 API להחזרת כל האבנים
app.get("/api/stones", async (req, res) => {
  try {
    const result = await pool.query("SELECT * FROM stones ORDER BY carat DESC");

    const formattedRows = result.rows.map(row => ({
      ...row,
      carat: row.carat ? parseFloat(row.carat) : null,
      ratio: row.ratio ? parseFloat(row.ratio) : null,
      price_per_carat: row.price_per_carat ? parseFloat(row.price_per_carat) : null,
      total_price: row.total_price ? parseFloat(row.total_price) : null,
      measurements1: row.measurements1 || null,
    }));

    res.json(formattedRows);
  } catch (error) {
    console.error("❌ Error fetching stones:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

// 🔹 API לשליפת אבן ספציפית
app.get("/api/stones/:stone_id", async (req, res) => {
  console.log("🚨 /api/stones/:stone_id CALLED");
  try {
    const { stone_id } = req.params;
    const result = await pool.query("SELECT * FROM stones WHERE stone_id = $1", [stone_id]);

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
      console.log(`💸 Encrypted price_per_carat: ${raw} → ${stone.price_per_carat}`);
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

// 🔹 API לתכשיט לפי modelNumber כולל הצפנה
app.get('/api/jewelry/:modelNumber', async (req, res) => {
  const { modelNumber } = req.params;

  try {
    const result = await pool.query(
      'SELECT * FROM jewelry_products WHERE model_number = $1',
      [modelNumber]
    );

    if (result.rows.length === 0) {
      return res.status(404).json({ error: 'Jewelry item not found' });
    }

    const item = result.rows[0];

    const numericFields = ['jewelry_weight', 'total_carat', 'center_stone_carat'];
    numericFields.forEach(field => {
      if (item[field] !== null && item[field] !== undefined) {
        item[field] = parseFloat(item[field]);
      }
    });

    if (item.price !== null && item.price !== undefined) {
      const originalPrice = item.price;
      item.price = encrypt(item.price.toString());
      console.log("🔐 Encrypted price:", originalPrice, '→', item.price);
    }

    res.json(item);
  } catch (error) {
    console.error('❌ Error fetching jewelry item:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});
app.listen(port, () => {
  console.log(`🚀 Server is running on http://localhost:${port}`);
});
