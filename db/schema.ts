import { pgTable, serial, text, numeric } from "drizzle-orm/pg-core";

export const stones = pgTable("stones", {
  id: serial("id").primaryKey(),
  stoneId: text("stone_id").notNull().unique(),
  clarity: text("clarity"),
  measurements1: text("measurements"),
  measurements2: text("measurements"),
  measurements3:text("measurements"),
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