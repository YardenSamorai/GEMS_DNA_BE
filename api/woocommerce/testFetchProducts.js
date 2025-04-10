require('dotenv').config();
const WooCommerceRestApi = require('@woocommerce/woocommerce-rest-api').default;

const api = new WooCommerceRestApi({
  url: `${process.env.WOOCOMMERCE_URL}`,
  consumerKey: process.env.WOOCOMMERCE_CONSUMER_KEY,
  consumerSecret: process.env.WOOCOMMERCE_CONSUMER_SECRET,
  version: 'wc/v3'
});

const run = async () => {
  try {
    console.log('🛒 Fetching products from WooCommerce...');
    const response = await api.get('products', {
      per_page: 10 // נבדוק רק 10 מוצרים לדוגמה
    });

    const products = response.data.map(product => ({
      id: product.id,
      name: product.name,
      sku: product.sku,
      price: product.price,
      images: product.images.map(img => img.src) // כאן אנחנו לוקחים את התמונות
    }));

    console.log(`✅ Found ${products.length} products on WooCommerce`);
    console.dir(products, { depth: null });
  } catch (error) {
    console.error('❌ Failed to fetch WooCommerce products:', error.message);
  }
};

run();