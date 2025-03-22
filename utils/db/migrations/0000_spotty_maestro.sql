CREATE TABLE "stones" (
	"id" serial PRIMARY KEY NOT NULL,
	"stone_id" text NOT NULL,
	"clarity" text,
	"measurements1" text,
	"certificate_number" text,
	"shape" text,
	"lab" text,
	"carat" numeric(5, 2) NOT NULL,
	"origin" text,
	"ratio" numeric(4, 2),
	"price_per_carat" numeric(10, 2),
	"total_price" numeric(10, 2),
	"cert_image" text,
	"video" text,
	"cert_pdf" text,
	CONSTRAINT "stones_stone_id_unique" UNIQUE("stone_id")
);
