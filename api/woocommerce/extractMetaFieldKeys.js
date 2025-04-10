// 📂 api/woocommerce/extractMetaFieldKeys.js
const WooCommerceRestApi = require('@woocommerce/woocommerce-rest-api').default;
require('dotenv').config();

const api = new WooCommerceRestApi({
  url: process.env.WOOCOMMERCE_URL,
  consumerKey: process.env.WOOCOMMERCE_CONSUMER_KEY,
  consumerSecret: process.env.WOOCOMMERCE_CONSUMER_SECRET,
  version: 'wc/v3'
});

const run = async () => {
  try {
    const { data: products } = await api.get('products', { per_page: 20 }); // מריץ רק על 20 מוצרים לבדיקה
    const fieldMap = {};

    for (const product of products) {
      const { id, name } = product;
      const { data: fullProduct } = await api.get(`products/${id}`);
      const meta = fullProduct.meta_data || [];

      meta.forEach(m => {
        // אם זה שדה של ACF (כלומר מתחיל ב __)
        if (m.key.startsWith('__')) {
          const realKey = m.key.replace(/^__/, '');
          const fieldKey = m.value;
          fieldMap[realKey] = fieldKey;
        }
      });
    }

    console.log('🧠 Extracted field keys:');
    console.log(JSON.stringify(fieldMap, null, 2));
  } catch (err) {
    console.error('❌ Error:', err.response?.data || err.message);
  }
};

run();
