const { Pool } = require("pg");
const fs = require("fs");
const path = require("path");
const csvParser = require("csv-parser");
require("dotenv").config();

const pool = new Pool({
  connectionString: "postgresql://gems_owner:npg_mr64DAeRqOfd@ep-bitter-paper-a5t16ihi-pooler.us-east-2.aws.neon.tech/gems?sslmode=require",
  ssl: {
    rejectUnauthorized: false, // ✅ Required for Neon
  },
});

async function importCSV() {
  const csvFilePath = path.join(__dirname, "stones2finel.csv");
  const client = await pool.connect();

  try {
    const rows = [];

    fs.createReadStream(csvFilePath)
      .pipe(csvParser())
      .on("data", (row) => {
        if (rows.length === 0) {
          console.log("🔍 Column Names Detected:", Object.keys(row)); // ✅ הצגת שמות העמודות
        }

        const cleanedRow = {};
        for (let key in row) {
          let cleanKey = key.trim().toLowerCase().replace(/[\n\r\s]+/g, "_").replace(/[^a-z0-9_]/g, ""); // ניקוי שם העמודה
          cleanedRow[cleanKey] = row[key].trim() === "" ? null : row[key].trim();
        }

        rows.push(cleanedRow);
      })
      .on("end", async () => {
        await client.query("BEGIN"); // 🔒 פתיחת טרנזקציה
        try {
          for (let row of rows) {
            const stoneIdKey = Object.keys(row).find(key => key.includes("stone_id"));
            const caratKey = Object.keys(row).find(key => key.toLowerCase().includes("carat") && !key.includes("owned"));
            const measurements1Key = Object.keys(row).find(key => key.toLowerCase().includes("measurements1"));
            const certificateNumberKey = Object.keys(row).find(key => key.toLowerCase().includes("certificate_number"));
            const totalPriceKey = Object.keys(row).find(key => key.toLowerCase().includes("total_price"));
            const shapeKey = Object.keys(row).find(key => key.toLowerCase().includes("shape"));
            const ratioKey = Object.keys(row).find(key => key.toLowerCase().includes("ratio"));
            const labKey = Object.keys(row).find(key => key.toLowerCase().includes("lab"));
            const pictureKey = Object.keys(row).find(key => key.toLowerCase().includes("picture"));
            const certPdfKey = Object.keys(row).find(key => key.toLowerCase().includes("cert_pdf"));
            const videoKey = Object.keys(row).find(key => key.toLowerCase().includes("video"));
            const clarityKey = Object.keys(row).find(key => key.toLowerCase().includes("clarity"));
            const originKey = Object.keys(row).find(key => key.toLowerCase().includes("origin"));
            const pricePerCaratKey = Object.keys(row).find(key => key.toLowerCase().includes("price_per_carat"));

            if (!stoneIdKey || !row[stoneIdKey]) {
              console.warn("⚠️ דילוג על שורה כי אין לה `stone_id`:", row);
              continue;
            }

            if (!caratKey || !row[caratKey]) {
              console.warn("⚠️ דילוג על שורה כי אין לה `carat`:", row);
              continue;
            }

            console.log(`🔹 Inserting: ${row[stoneIdKey]}, m1: ${row[measurements1Key]}, No.${counter++}`);

            await client.query(
              `INSERT INTO stones (
                stone_id, carat, certificate_number, total_price, shape, measurements1, ratio, lab, picture, cert_pdf, 
                video, clarity, origin, price_per_carat
              ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14
              )`,
              [
                row[stoneIdKey],
                row[caratKey] ? parseFloat(row[caratKey].replace(/,/g, '')) : null,
                row[certificateNumberKey] || null,
                row[totalPriceKey] ? parseFloat(row[totalPriceKey].replace(/,/g, '')) : null,
                row[shapeKey] || null,
                row[measurements1Key] || null,
                row[ratioKey] ? parseFloat(row[ratioKey]) : null,
                row[labKey] || null,
                row[pictureKey] || null,
                row[certPdfKey] || null,
                row[videoKey] || null,
                row[clarityKey] || null,
                row[originKey] || null,
                row[pricePerCaratKey] ? parseFloat(row[pricePerCaratKey].replace(/,/g, '')) : null
              ]
            );
          }

          await client.query("COMMIT"); // ✅ שמירת הנתונים
          console.log("✅ CSV data successfully committed to Neon!");

        } catch (queryError) {
          await client.query("ROLLBACK"); // ❌ אם יש שגיאה, מוחקים את כל הטרנזקציה
          console.error("❌ שגיאה בהכנסת הנתונים, כל הנתונים בוטלו:", queryError);
        } finally {
          client.release();
        }
      });

  } catch (error) {
    console.error("❌ Error importing CSV:", error);
    client.release();
  }
}

importCSV();
