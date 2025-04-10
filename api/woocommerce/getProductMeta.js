const WooCommerceRestApi = require('@woocommerce/woocommerce-rest-api').default;
require('dotenv').config();

// הגדרת החיבור ל-WooCommerce
const api = new WooCommerceRestApi({
  url: process.env.WOOCOMMERCE_URL,
  consumerKey: process.env.WOOCOMMERCE_CONSUMER_KEY,
  consumerSecret: process.env.WOOCOMMERCE_CONSUMER_SECRET,
  version: 'wc/v3'
});

// רשימת ה-SKU של המוצרים לבדיקה
const skus = ['T8258', 'NY-SA-03', 'HK0003', 'T7594', ]; // החלף ב-SKU האמיתיים

const fetchMetaData = async () => {
  try {
    for (const sku of skus) {
      // חיפוש המוצר לפי SKU
      const { data: products } = await api.get('products', { sku });
      if (products.length === 0) {
        console.log(`❌ מוצר עם SKU: ${sku} לא נמצא.`);
        continue;
      }
      const product = products[0];
      console.log(`🔍 מוצר: ${product.name} (ID: ${product.id})`);
      console.log('🧠 Meta Data:', product.meta_data);
    }
  } catch (err) {
    console.error('❌ שגיאה בשליפת הנתונים:', err.response?.data || err.message);
  }
};

fetchMetaData();