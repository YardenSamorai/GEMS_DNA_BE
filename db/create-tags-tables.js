const { Pool } = require("pg");
const dotenv = require("dotenv");
const path = require("path");

dotenv.config({ path: path.resolve(__dirname, "../.env") });

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false },
});

async function createTagsTables() {
  console.log("🚀 Creating tags tables...\n");

  try {
    // Create tags table
    await pool.query(`
      CREATE TABLE IF NOT EXISTS tags (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL UNIQUE,
        color TEXT NOT NULL DEFAULT '#10b981',
        created_at TIMESTAMP DEFAULT NOW() NOT NULL
      );
    `);
    console.log("✅ Created 'tags' table");

    // Create stone_tags table
    await pool.query(`
      CREATE TABLE IF NOT EXISTS stone_tags (
        id SERIAL PRIMARY KEY,
        stone_sku TEXT NOT NULL,
        tag_id INTEGER NOT NULL REFERENCES tags(id) ON DELETE CASCADE,
        created_at TIMESTAMP DEFAULT NOW() NOT NULL,
        UNIQUE(stone_sku, tag_id)
      );
    `);
    console.log("✅ Created 'stone_tags' table");

    // Create index for faster lookups
    await pool.query(`
      CREATE INDEX IF NOT EXISTS idx_stone_tags_sku ON stone_tags(stone_sku);
    `);
    console.log("✅ Created index on stone_sku");

    await pool.query(`
      CREATE INDEX IF NOT EXISTS idx_stone_tags_tag_id ON stone_tags(tag_id);
    `);
    console.log("✅ Created index on tag_id");

    console.log("\n🎉 All tables created successfully!");

  } catch (error) {
    console.error("❌ Error creating tables:", error);
  } finally {
    await pool.end();
  }
}

createTagsTables();

