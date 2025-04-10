//Import all Woocommerce products -> 100Items per page (using Axios)
const axios = require('axios');
require('dotenv').config();

const wooClient = axios.create({
  baseURL: `${process.env.WOOCOMMERCE_URL}/wp-json/wc/v3`,
  auth: {
    username: process.env.WOOCOMMERCE_CONSUMER_KEY,
    password: process.env.WOOCOMMERCE_CONSUMER_SECRET,
  },
});

// Get all products (with pagination)
const getAllProducts = async () => {
  try {
    let allProducts = [];
    let page = 1;
    let hasMore = true;

    console.log('🛒 Fetching products from WooCommerce...');

    while (hasMore) {
      const { data } = await wooClient.get('/products', {
        params: {
          per_page: 100,
          page,
        },
      });

      allProducts = [...allProducts, ...data];

      if (data.length < 100) {
        hasMore = false;
      } else {
        page++;
      }
    }

    console.log(`✅ Found ${allProducts.length} products on WooCommerce`);
    return allProducts;
  } catch (err) {
    console.error('❌ Failed to fetch WooCommerce products:', err.message);
    return [];
  }
};

module.exports = {
  getAllProducts,
  wooClient,
};