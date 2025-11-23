import { defineConfig } from "drizzle-kit";

export default defineConfig({
  schema: "./db/**/*.ts",
  out: "./utils/db/migrations",
  dialect: "postgresql",
  dbCredentials: {
    url: process.env.DATABASE_URL as string,
  },
  strict: false, // ⬅️ משנה מ-true ל-false
});
