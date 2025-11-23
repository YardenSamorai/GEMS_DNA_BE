import { pgTable, serial, text, numeric, varchar, timestamp } from "drizzle-orm/pg-core";

/* ========== stones ========== */

export const stones = pgTable("stones", {
  id: serial("id").primaryKey(),
  stoneId: text("stone_id").notNull().unique(),
  clarity: text("clarity"),
  measurements1: text("measurements1"),
  certificateNumber: text("certificate_number"),
  shape: text("shape"),
  lab: text("lab"),
  carat: numeric("carat", { precision: 5, scale: 2 }).notNull(),
  origin: text("origin"),
  ratio: numeric("ratio", { precision: 4, scale: 2 }),
  pricePerCarat: numeric("price_per_carat", { precision: 10, scale: 2 }),
  totalPrice: numeric("total_price", { precision: 10, scale: 2 }),
  certImage: text("cert_image"),
  video: text("video"),
  certPdf: text("cert_pdf"),
});

/* ========== jewelry_products ========== */

export const jewelryProducts = pgTable("jewelry_products", {
  modelNumber: varchar("model_number", { length: 50 }).primaryKey(),
  stockNumber: varchar("stock_number", { length: 50 }),
  jewelryType: varchar("jewelry_type", { length: 50 }),
  style: varchar("style", { length: 50 }),
  collection: varchar("collection", { length: 100 }),
  price: numeric("price", { precision: 10, scale: 2 }),
  videoLink: text("video_link"),
  allPicturesLink: text("all_pictures_link"),
  certificateLink: text("certificate_link"),
  certificateNumber: varchar("certificate_number", { length: 100 }),
  title: varchar("title", { length: 150 }),
  description: text("description"),
  jewelryWeight: numeric("jewelry_weight", { precision: 10, scale: 2 }),
  totalCarat: numeric("total_carat", { precision: 10, scale: 2 }),
  stoneType: varchar("stone_type", { length: 50 }),
  centerStoneCarat: numeric("center_stone_carat", { precision: 10, scale: 2 }),
  centerStoneShape: varchar("center_stone_shape", { length: 50 }),
  centerStoneColor: varchar("center_stone_color", { length: 50 }),
  centerStoneClarity: varchar("center_stone_clarity", { length: 50 }),
  metalType: varchar("metal_type", { length: 50 }),
  currency: varchar("currency", { length: 10 }),
  availability: varchar("availability", { length: 50 }),
  shippingFrom: varchar("shipping_from", { length: 100 }),
  category: varchar("category", { length: 100 }),
  fullDescription: text("full_description"),
  jewelrySize: varchar("jewelry_size", { length: 50 }),
  instructionsMain: text("instructions_main"),
});

/* ========== soap_stones ========== */

export const soapStones = pgTable("soap_stones", {
  id: serial("id").primaryKey(),

  category: text("category"),
  sku: text("sku").notNull().unique(),
  shape: text("shape"),
  weight: numeric("weight"),

  color: text("color"),
  clarity: text("clarity"),
  lab: text("lab"),
  fluorescence: text("fluorescence"),

  pricePerCarat: numeric("price_per_carat"),
  rapPrice: numeric("rap_price"),
  rapListPrice: numeric("rap_list_price"),
  totalPrice: numeric("total_price"),

  location: text("location"),
  branch: text("branch"),

  image: text("image"),
  additionalPictures: text("additional_pictures"),
  video: text("video"),
  additionalVideos: text("additional_videos"),
  certificateImage: text("certificate_image"),
  certificateNumber: text("certificate_number"),
  certificateImageJpg: text("certificate_image_jpg"),

  cut: text("cut"),
  polish: text("polish"),
  symmetry: text("symmetry"),
  tablePercent: numeric("table_percent"),
  depthPercent: numeric("depth_percent"),
  ratio: numeric("ratio"),
  measurements: text("measurements"),

  fancyIntensity: text("fancy_intensity"),
  fancyColor: text("fancy_color"),
  fancyOvertone: text("fancy_overtone"),
  fancyColor2: text("fancy_color_2"),
  fancyOvertone2: text("fancy_overtone_2"),

  pairStone: text("pair_stone"),
  homePage: text("home_page"),
  tradeShow: text("trade_show"),

  comment: text("comment"),
  type: text("type"),
  certComments: text("cert_comments"),
  origin: text("origin"),

  rawXml: text("raw_xml"),
  updatedAt: timestamp("updated_at").defaultNow().notNull(),
});
