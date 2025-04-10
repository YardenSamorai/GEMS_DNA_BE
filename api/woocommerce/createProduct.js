require("dotenv").config();
const { createFullStoneProduct } = require("../woocommerce/createFullStoneProduct");

// דוגמת אבן לבדיקה
const testStone = {
  sku: "TEST-123451",
  shape: "Oval",
  carat: "2.05",
  color: "G",
  clarity: "VS1",
  lab: "GIA",
  fluorescence: "None",
  price_per_carat: "7000",
  total_price: "14350",
  certificate_url: "https://certs.com/test-certificate.pdf",
  image: "https://app.barakdiamonds.com/Gemstones/Output/StoneImages/SP-0003-MAIN.jpg",
  additional_pictures: [
    "https://app.barakdiamonds.com/Gemstones/Output/StoneImages/SP-0003-MAIN.jpg",
    "https://app.barakdiamonds.com/Gemstones/Output/StoneImages/SP-0003-MAIN.jpg"
  ],
  video: "",
  table_percent: "57",
  depth_percent: "62.3",
  ratio: "1.30",
  dimensions: "8.2x6.1x4.3",
  category: "Diamond"
};

createFullStoneProduct(testStone)
  .then(() => console.log("✅ Finished!"))
  .catch(err => console.error("❌ Failed!", err));