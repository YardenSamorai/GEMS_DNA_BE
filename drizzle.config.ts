import {defineConfig} from "drizzle-kit";

export default defineConfig({
  schema: "./db/schema.ts",
    out: "./utils/db/migrations",
    dialect: "postgresql",
    dbCredentials:{
        url: process.env.DATABASE_URL,
    }
});