const fs = require('fs');
const path = require('path');
const csv = require('csv-parser');
const dotenv = require('dotenv');
const { Client } = require('pg');

dotenv.config();

const client = new Client({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false },
});

const filePath = path.resolve('./csv/EBAY_Jewelry_TEST.csv');

let total = 0;
let inserted = 0;
let failed = 0;

const loadRowCount = async () => {
  return new Promise((resolve) => {
    let count = 0;
    fs.createReadStream(filePath)
      .pipe(csv())
      .on('data', () => count++)
      .on('end', () => resolve(count));
  });
};

const importCSV = async () => {
  await client.connect();
  console.log('📡 Connected to database');

  // מחיקת כל המידע בטבלה
  await client.query('DELETE FROM jewelry_products');
  console.log('🧹 Cleared existing data from jewelry_products');

  total = await loadRowCount();
  console.log(`📦 Total rows in CSV: ${total}\n`);

  let current = 0;

  const stream = fs.createReadStream(filePath).pipe(csv());

  for await (const row of stream) {
    current++;

    try {
      await client.query(
        `INSERT INTO jewelry_products (
          model_number, stock_number, jewelry_type, style, collection, price,
          video_link, all_pictures_link, certificate_link, certificate_number,
          title, description, jewelry_weight, total_carat, stone_type,
          center_stone_carat, center_stone_shape, center_stone_color, center_stone_clarity,
          metal_type, currency, availability, shipping_from, category,
          full_description, jewelry_size, instructions_main
        ) VALUES (
          $1, $2, $3, $4, $5, $6,
          $7, $8, $9, $10,
          $11, $12, $13, $14, $15,
          $16, $17, $18, $19,
          $20, $21, $22, $23, $24,
          $25, $26, $27
        )`,
        [
          row['Model Number'],
          row['Stock Number'],
          row['Jewelry Type'],
          row['Style'],
          row['Collection'],
          parseFloat(row['Price']) || null,
          row['Video_Link'],
          row['All_Pictures_Link'],
          row['Certificate_Link'],
          row['Certificate_Number'],
          row['Title'],
          row['Description'],
          parseFloat(row['Jewelry_Weight']) || null,
          parseFloat(row['Total_Carat']) || null,
          row['Stone_Type'],
          parseFloat(row['Center_Stone_Carat']) || null,
          row['Center_Stone_Shape'],
          row['Center_Stone_Color'],
          row['Center_Stone_Clarity'],
          row['Metal_Type'],
          row['Currency'],
          row['Availability'],
          row['Shipping_From'],
          row['Category'],
          row['full_description'],
          row['jewelry_size'],
          row['Instructions_main']
        ]
      );

      inserted++;
      if (current % 10 === 0 || current === total) {
        console.log(`✅ Inserted ${inserted}/${total} (${Math.round((inserted / total) * 100)}%)`);
      }

    } catch (err) {
      failed++;
      console.error(`❌ Row ${current}/${total} [${row['Model Number'] || 'N/A'}] failed: ${err.message}`);
    }
  }

  await client.end();

  // סיכום
  console.log('\n📊 Import Summary:');
  console.log(`✔️  Success: ${inserted}`);
  console.log(`❌ Failed: ${failed}`);
  console.log(`📦 Total Processed: ${inserted + failed}`);
};

importCSV();
