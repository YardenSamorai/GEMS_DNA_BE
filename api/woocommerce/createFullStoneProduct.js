require("dotenv").config();
const WooCommerceRestApi = require("@woocommerce/woocommerce-rest-api").default;

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

const getCategoryIdByName = (name) => {
  return categoryMap[name] || categoryMap['Unrecognize'];
};

const api = new WooCommerceRestApi({
  url: process.env.WOOCOMMERCE_URL,
  consumerKey: process.env.WOOCOMMERCE_CONSUMER_KEY,
  consumerSecret: process.env.WOOCOMMERCE_CONSUMER_SECRET,
  version: "wc/v3"
});

const createFullStoneProduct = async (stone) => {
  try {
    const {
      sku,
      shape,
      carat,
      color,
      clarity,
      lab,
      fluorescence,
      price_per_carat,
      total_price,
      certificate_url,
      image,
      additional_pictures = [],
      video,
      table_percent,
      depth_percent,
      ratio,
      dimensions,
      category
    } = stone;

    const categoryId = await getCategoryIdByName(category);

    const images = [
      ...(image ? [{ src: image }] : []),
      ...additional_pictures.map(url => ({ src: url }))
    ];

    const response = await api.post("products", {
      name: `${carat}ct ${shape} ${color} ${clarity} (${lab})`,
      type: "simple",
      status: "publish",
      sku,
      regular_price: total_price,
      manage_stock: true,
      stock_quantity: 1,
      in_stock: true,
      categories: categoryId ? [{ id: categoryId }] : [],
      images,
      attributes: [
        { name: "Shape", options: [shape] },
        { name: "Carat", options: [carat] },
        { name: "Color", options: [color] },
        { name: "Clarity", options: [clarity] },
        { name: "Lab", options: [lab] },
        { name: "Fluorescence", options: [fluorescence] }
      ],
      meta_data: [
        { key: "_product_filter_type", value: "emeralds" },
        { key: "_product_filter_gem_type", value: "" },
        { key: "_product_filter_ppc", value: "" },
        { key: "_product_filter_rap", value: "0%" },
        { key: "_product_filter_price_per_carat", value: "2250" },
        { key: "_produc_private_price_per_carat", value: "2475.0" },
        { key: "_product_filter_weight", value: "1.39" },
        { key: "_product_filter_weight_e", value: "1.39" },
        { key: "_prudect_private_price_total", value: "3440.250" },
        { key: "_product_filter_shape_e", value: "Emerald" },
        { key: "_product_filter_shape", value: "Emerald" },
        { key: "_product_filter_color_d", value: "" },
        { key: "_product_filter_clarity", value: "" },
        { key: "_product_filter_lab", value: "ICA" },
        { key: "_product_filter_lab_e", value: "ICA" },
        { key: "_product_filter_fluorescence", value: "" },
        { key: "_product_filter_location", value: "Los Angeles" },
        { key: "_product_filter_lab_num", value: "" },
        { key: "_product_filter_cut", value: "" },
        { key: "_product_filter_polish", value: "" },
        { key: "_product_filter_symmetry", value: "" },
        { key: "_product_filter_table", value: "%" },
        { key: "_product_filter_depth", value: "%" },
        { key: "_product_filter_lw_ratio", value: "1.38" },
        { key: "_product_filter_lwd", value: "7.93-5.74-3.86" },
        { key: "_pair_product_sku", value: "MT-114A2" },
        { key: "_product_filter_intensity", value: "" },
        { key: "_product_filter_color_f", value: "" },
        { key: "_product_filter_fancy_overtone", value: "" },
        { key: "_product_filter_fancy_color_2", value: "" },
        { key: "_product_filter_fancy_overtone_2", value: "" },
        { key: "_product_filter_comments", value: "Minor" },
        { key: "_product_filter_stone_type_e", value: "" },
        { key: "_item_comments", value: "" },
        { key: "_product_filter_origin", value: "" },
        { key: "_prudect_cert_link", value: "https://app.barakdiamonds.com/Gemstones/Output/Certificates/153941.pdf" },
        { key: "_product_special", value: "" },
        { key: "_product_attachments_list", value: "" },
        { key: "Type", value: category },
        { key: "price_per_carat", value: price_per_carat },
        { key: "certificate_url", value: certificate_url },
        { key: "video", value: video },
        { key: "table_percent", value: table_percent },
        { key: "depth_percent", value: depth_percent },
        { key: "ratio", value: ratio },
        { key: "dimensions", value: dimensions }
      ]
    });

    console.log("✅ New product created:", response.data.id);
    return response.data;

  } catch (err) {
    console.error("❌ Error creating product:", err.response?.data || err.message);
    throw err;
  }
};

module.exports = { createFullStoneProduct };
