require("dotenv").config();
const WooCommerceRestApi = require('@woocommerce/woocommerce-rest-api').default;
const { pool } = require('../../db/client');

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
  'Emerald': 17,
  'Diamonds': 16,
  'Diamond': 16,
  'Unrecognize': 15
};

const normalizeType = (category) => {
  switch ((category || "").toLowerCase()) {
    case "diamond":
    case "diamonds":
      return "diamonds";
    case "emerald":
    case "emeralds":
      return "emeralds";
    case "fancy":
    case "fancy color":
    case "fancy-color":
      return "fancy-color";
    case "gemstone":
    case "gemstones":
      return "gemstones";
    case "jewelry":
      return "jewelry";
    default:
      return "0";
  }
};

const run = async () => {
  try {
    console.log('🔄 Fetching WooCommerce products...');
    const { data: products } = await api.get('products', { per_page: 100 });
    const wooMap = new Map(products.map(p => [p.sku, p]));

    console.log('📦 Fetching stones from DB...');
    const { rows: stones } = await pool.query('SELECT * FROM soap_stones');
    const dbMap = new Map(stones.map(s => [s.sku, s]));

    for (const [sku, wooProduct] of wooMap) {
      if (!dbMap.has(sku)) {
        console.log(`🗑️ Deleting product not in DB: ${sku}`);
        await api.delete(`products/${wooProduct.id}`, { force: true });
      }
    }

    const total = dbMap.size;
    let index = 1;

    for (const [sku, stone] of dbMap) {
      const categoryId = categoryMap[stone.category] || categoryMap['Unrecognize'];

      const additionalPictures = typeof stone.additional_pictures === 'string'
        ? stone.additional_pictures.split(';').map(url => url.trim()).filter(Boolean)
        : [];

      const images = [
        ...(stone.picture ? [{ src: stone.picture }] : []),
        ...additionalPictures.map(url => ({ src: url }))
      ];

      const attributes = [
        { name: "Shape", options: [String(stone.shape || "")] },
        { name: "Carat", options: [String(stone.weight || "")] },
        { name: "Color", options: [String(stone.color || "")] },
        { name: "Clarity", options: [String(stone.clarity || "")] },
        { name: "Lab", options: [String(stone.lab || "")] },
        { name: "Fluorescence", options: [String(stone.fluorescence || "")] }
      ];

      const meta_data = Object.entries(stone).map(([key, value]) => ({
        key,
        value: value ?? ""
      })).concat([
        { key: "_product_filter_type", value: normalizeType(stone.category) || "" }
      ]);

      const productPayload = {
        name: `${stone.weight || ''}ct ${stone.shape || ''} ${stone.color || ''} ${stone.clarity || ''} (${stone.lab || ''})`.trim().replace(/\s+/g, ' '),
        sku: stone.sku,
        type: 'simple',
        status: 'publish',
        regular_price: stone.total_price?.toFixed(2) || '0.00',
        description: `Stone SKU: ${stone.sku}`,
        manage_stock: true,
        stock_quantity: 1,
        in_stock: true,
        categories: [ { id: categoryId } ],
        images,
        attributes,
        meta_data
      };

      try {
        const { data: existing } = await api.get('products', { sku: stone.sku });
        if (existing.length > 0) {
          const existingProduct = existing[0];
          console.log(`(${index}/${total}) ✏️ Updating: ${sku}`);
          await api.put(`products/${existingProduct.id}`, productPayload);
        } else {
          console.log(`(${index}/${total}) 🆕 Adding: ${sku}`);
          await api.post('products', productPayload);
        }
      } catch (err) {
        console.error(`(${index}/${total}) ❌ Error syncing ${sku}:`, err.response?.data || err.message);
      }

      index++;
    }

    console.log('✅ Sync complete.');
  } catch (err) {
    console.error('❌ General sync error:', err.message);
  }
};

run();
