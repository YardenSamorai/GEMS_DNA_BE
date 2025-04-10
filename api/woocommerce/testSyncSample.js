// 📂 api/woocommerce/syncProducts.js - Sample Mode (5 SKUs Only)
const WooCommerceRestApi = require('@woocommerce/woocommerce-rest-api').default;
const { pool } = require('../../db/client');
require('dotenv').config();

const api = new WooCommerceRestApi({
  url: process.env.WOOCOMMERCE_URL,
  consumerKey: process.env.WOOCOMMERCE_CONSUMER_KEY,
  consumerSecret: process.env.WOOCOMMERCE_CONSUMER_SECRET,
  version: 'wc/v3'
});

const categoryMap = {
  'Fancy': 18,
  'Amethyst O': 36,
  'Aquamarine': 36,
  'Garnet O': 36,
  'Kunzite O': 36,
  'Morganite O': 36,
  'Ruby': 36,
  'Sapphire': 36,
  'Spinel': 36,
  'Tanzanite O': 36,
  'Tourmaline O': 36,
  'Tsavorite O': 36,
  'chrome diopside O': 36,
  'Emeralds': 17,
  'Emerald': 17, // ✅ חדש
  'Diamonds': 16,
  'Diamond': 16, // ✅ חדש
  'Unrecognize': 15
};

const cleanCategory = (rawCategory) => {
  return (rawCategory || '').trim();
};

const TEST_SKUS = ['MTPS-0444', 'MG-112A0047', 'MG-112A0044', 'MG-7100A211', 'MT94-.0241'];

const run = async () => {
  try {
    console.log('🔍 Fetching sample stones from DB...');
    const { rows: stones } = await pool.query(
      `SELECT * FROM soap_stones WHERE sku = ANY($1::text[])`,
      [TEST_SKUS]
    );

    const total = stones.length;
    let index = 1;

    for (const stone of stones) {
      const cleanedCategory = cleanCategory(stone.category);
      const categoryId = categoryMap[cleanedCategory] || categoryMap['Unrecognize'];

      if (!categoryMap[cleanedCategory]) {
        console.warn(`⚠️ Unknown category: "${stone.category}" (cleaned: "${cleanedCategory}") for SKU: ${stone.sku}`);
      }

      const productPayload = {
        name: `${stone.shape} ${stone.weight} ${stone.color} ${stone.clarity} ${stone.lab}`.trim(),
        sku: stone.sku,
        type: 'simple',
        status: 'publish',
        regular_price: stone.total_price?.toFixed(2) || '0.00',
        description: `Stone SKU: ${stone.sku}`,
        categories: [{ id: categoryId }],
        images: stone.picture ? [{ src: stone.picture }] : [],
        meta_data: [
          { key: 'Type', value: stone.category },
          { key: 'type', value: stone.category }, // for custom ACF-style field
          { key: 'Shape', value: stone.shape },
          { key: 'Weight', value: stone.weight },
          { key: 'Color', value: stone.color },
          { key: 'Clarity', value: stone.clarity },
          { key: 'Polish', value: stone.polish },
          { key: 'Symmetry', value: stone.symmetry },
          { key: 'Fluorescence', value: stone.fluorescence },
          { key: 'Table %', value: stone.table_percent },
          { key: 'Depth %', value: stone.depth },
          { key: 'Lab (Certificate)', value: stone.lab },
          { key: 'Certificate PDF', value: stone.certificate_pdf },
          { key: 'Video', value: stone.video },
          { key: 'Origin', value: stone.origin },
          { key: 'Ratio', value: stone.ratio },
          { key: 'Measurements', value: stone.measurements },
          { key: 'Comment', value: stone.comment }
        ]
      };

      try {
        const { data: existing } = await api.get('products', { sku: stone.sku });
        if (existing.length > 0) {
          const existingProduct = existing[0];
          console.log(`(${index}/${total}) ✏️ Updating: ${stone.sku}`);
          await api.put(`products/${existingProduct.id}`, productPayload);
        } else {
          console.log(`(${index}/${total}) 🆕 Adding: ${stone.sku}`);
          await api.post('products', productPayload);
        }
      } catch (err) {
        console.error(`(${index}/${total}) ❌ Error syncing ${stone.sku}:`, err.response?.data || err.message);
      }

      index++;
    }

    console.log('✅ Sample sync complete.');
  } catch (err) {
    console.error('❌ General error:', err.message);
  }
};

run();
