import {defineConfig} from "drizzle-kit";

export default defineConfig({
  schema: "./db/schema.ts",
    out: "./utils/db/migrations",
    dialect: "postgresql",
    dbCredentials:{
        url: "postgresql://gems_owner:npg_mr64DAeRqOfd@ep-bitter-paper-a5t16ihi-pooler.us-east-2.aws.neon.tech/gems?sslmode=require",
    }
});