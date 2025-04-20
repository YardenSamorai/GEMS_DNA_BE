const express = require("express");
const cors = require("cors");
const { Pool } = require("pg");
const dotenv = require('dotenv');
const path = require('path');

dotenv.config({ path: path.resolve(__dirname, '../../.env') });



const app = express();
const port = process.env.PORT;
const dbUrl = process.env.DATABASE_URL;
const pool = new Pool({
  connectionString: dbUrl,
  ssl: { rejectUnauthorized: false },
});

app.use(cors());

// 🔹 נתיב API להחזרת כל האבנים
app.get("/api/stones", async (req, res) => {
  try {
    const result = await pool.query("SELECT * FROM stones ORDER BY carat DESC");

    // המרת שדות מספריים
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

// 🔹 נתיב API לשליפת אבן ספציפית לפי `stone_id`
app.get("/api/stones/:stone_id", async (req, res) => {
  try {
    const { stone_id } = req.params;
    const result = await pool.query("SELECT * FROM stones WHERE stone_id = $1", [stone_id]);

    if (result.rows.length === 0) {
      return res.status(404).json({ error: "Stone not found" });
    }

    // המרת שדות מספריים
    const stone = result.rows[0];
    const numericFields = ["carat", "ratio", "price_per_carat", "total_price"];
    numericFields.forEach((field) => {
      if (stone[field] !== null && stone[field] !== undefined) {
        stone[field] = parseFloat(stone[field]);
      }
    });

    res.json(stone);
  } catch (error) {
    console.error("❌ Error fetching stone:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

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

    // המרת שדות מספריים
    const numericFields = ['price', 'jewelry_weight', 'total_carat', 'center_stone_carat'];
    numericFields.forEach(field => {
      if (item[field] !== null && item[field] !== undefined) {
        item[field] = parseFloat(item[field]);
      }
    });

    res.json(item);
  } catch (error) {
    console.error('❌ Error fetching jewelry item:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

app.listen(port, () => {
  console.log(`🚀 Server is running on http://localhost:${port}`);
});