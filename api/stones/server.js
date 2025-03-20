const express = require("express");
const cors = require("cors"); // ✅ ייבוא CORS
const { Pool } = require("pg");
require("dotenv").config();

const app = express();
const port = process.env.PORT || 3000;

const pool = new Pool({
  connectionString: process.env.DATABASE_URL || "postgresql://gems_owner:npg_mr64DAeRqOfd@ep-bitter-paper-a5t16ihi-pooler.us-east-2.aws.neon.tech/gems?sslmode=require",
  ssl: { rejectUnauthorized: false },
});

app.use(cors()); // ✅ הפעלת CORS לכל הדומיינים

// 🔹 נתיב API להחזרת כל האבנים
app.get("/api/stones", async (req, res) => {
  try {
    const result = await pool.query("SELECT * FROM stones ORDER BY carat DESC");

    // המרת שדות מספריים למספרים ושמירת `measurements1` כמחרוזת
    const formattedRows = result.rows.map(row => ({
      ...row,
      carat: row.carat ? parseFloat(row.carat) : null,
      ratio: row.ratio ? parseFloat(row.ratio) : null,
      price_per_carat: row.price_per_carat ? parseFloat(row.price_per_carat) : null,
      total_price: row.total_price ? parseFloat(row.total_price) : null,
      measurements1: row.measurements1 || null, // ✅ השארת measurements1 כמחרוזת מלאה
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

    // המרת השדות המספריים, אבל השארת `measurements1` כמחרוזת
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

app.listen(port, () => {
  console.log(`🚀 Server is running on http://localhost:${port}`);
});
