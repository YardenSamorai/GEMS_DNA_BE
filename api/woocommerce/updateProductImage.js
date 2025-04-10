//Update Pictures for Woocommerce Products.

require('dotenv').config();
const WooCommerceRestApi = require('@woocommerce/woocommerce-rest-api').default;

const api = new WooCommerceRestApi({
  url: `${process.env.WOOCOMMERCE_URL}`,
  consumerKey: process.env.WOOCOMMERCE_CONSUMER_KEY,
  consumerSecret: process.env.WOOCOMMERCE_CONSUMER_SECRET,
  version: 'wc/v3'
});

const updateProductImages = async (sku, imageUrls) => {
  try {
    console.log(`🔍 Search By SKU: ${sku}`);
    const response = await api.get('products', { sku });

    if (!response.data.length) {
      console.error('❌Not Founed');
      return;
    }

    const productId = response.data[0].id;
    console.log(`✅ Find ID: ${productId}`);
    
    const images = imageUrls.map(url => ({ src: url }));

    console.log(`🖼️ Uploading ${images.length} תמונות למוצר...`);

    const updateRes = await api.put(`products/${productId}`, {
      images
    });

    console.log('🎉 Updated succssufly', updateRes.data.images);
  } catch (err) {
    console.error('❌ Error:', err.message);
  }
};

// קריאה לפונקציה בפועל
updateProductImages('T8284', [
  'https://app.barakdiamonds.com/Gemstones/Output/StoneImages/T8284_Main_Blk.jpg',
  'https://app.barakdiamonds.com/Gemstones/Output/StoneImages/T8284.jpg',
  'https://app.barakdiamonds.com/Gemstones/Output/StoneImages/T8284_Picture.jpg',
]);