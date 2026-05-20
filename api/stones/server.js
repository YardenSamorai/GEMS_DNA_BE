const express = require("express");
const cors = require("cors");
const { Pool } = require("pg");
const dotenv = require("dotenv");
const path = require("path");
const crypto = require("crypto");
const CryptoJS = require("crypto-js");

dotenv.config({ path: path.resolve(__dirname, "../../.env") });

const ENCRYPT_SECRET = process.env.ENCRYPT_SECRET;
const app = express();
const port = process.env.PORT;
const dbUrl = process.env.DATABASE_URL;

const pool = new Pool({
  connectionString: dbUrl,
  ssl: { rejectUnauthorized: false },
});

console.log("🟢 Backend is running — This is the correct file.");
app.use(cors());
app.use(express.json({ limit: '50mb' }));

/* =========================================================
   Encryption helper
   ========================================================= */
const encrypt = (text) => {
  return CryptoJS.AES.encrypt(text, ENCRYPT_SECRET).toString();
};

const decrypt = (cipher) => {
  if (!cipher) return null;
  try {
    const bytes = CryptoJS.AES.decrypt(cipher, ENCRYPT_SECRET);
    return bytes.toString(CryptoJS.enc.Utf8) || null;
  } catch (e) {
    console.error("Decrypt failed:", e.message);
    return null;
  }
};

/* =========================================================
   Activity log (Sprint 2 / Phase 1)
   ---------------------------------------------------------
   Single source-of-truth feed for "what just happened in the
   workspace". Every meaningful mutation (create/update/delete,
   status change, link, etc.) calls logActivity() so we can
   build per-entity timelines, audit trails and a real activity
   feed in the Overview tab.

   Design notes:
   - Fire-and-forget: failures NEVER bubble up to the parent
     route. Activity logging is best-effort by definition.
   - Actor info comes from x-actor-id / x-actor-name headers
     (FE will start sending these in Phase 2). Until then we
     fall back to actorId = userId, which is correct for the
     current single-user-per-workspace assumption.
   - `related` is a JSONB array of {type,id,name} pointers used
     by the FE to render backlinks ("→ contact: Liora Kirsch").
   ========================================================= */
function getActor(req) {
  return {
    actorId:
      req?.headers?.['x-actor-id'] ||
      req?.body?.actorId ||
      req?.body?.userId ||
      null,
    actorName:
      req?.headers?.['x-actor-name'] ||
      req?.body?.actorName ||
      null,
  };
}

async function logActivity({
  userId,
  actorId,
  actorName,
  entityType,
  entityId,
  action,
  summary,
  changes,
  related,
}) {
  if (!userId || !entityType || !entityId || !action) return;
  try {
    await pool.query(
      `INSERT INTO activity_log
         (user_id, actor_id, actor_name, entity_type, entity_id,
          action, summary, changes, related)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8::jsonb,$9::jsonb)`,
      [
        userId,
        actorId || userId,
        actorName || null,
        entityType,
        String(entityId),
        action,
        summary || null,
        JSON.stringify(changes || null),
        JSON.stringify(related || null),
      ]
    );
  } catch (e) {
    // Don't fail the parent operation if logging hiccups
    console.warn('activity_log insert failed (non-fatal):', e.message);
  }
}

// Diff two row snapshots and return only the fields that actually changed.
// Used by UPDATE endpoints to populate `changes` with a compact { field: {from,to} } map.
function diffRows(before, after, keys) {
  if (!before || !after) return null;
  const out = {};
  for (const k of keys) {
    const a = before[k];
    const b = after[k];
    const aJson = a && typeof a === 'object' ? JSON.stringify(a) : a;
    const bJson = b && typeof b === 'object' ? JSON.stringify(b) : b;
    if (aJson !== bJson) out[k] = { from: a ?? null, to: b ?? null };
  }
  return Object.keys(out).length ? out : null;
}

/* =========================================================
   Team / Sales-Rep system (Sprint 3)
   ---------------------------------------------------------
   Design goal: a workshop with one owner ("admin") and up to ~10
   reps. Every record (contact, deal, task, jewelry_item) gets an
   `assigned_to` (Clerk user id of the rep) so reps see their own
   pipeline by default while the admin sees the whole workspace.

   - `team_members` is a thin lookup that maps Clerk users to a
     workspace (`team_owner_id`) + role.
   - All existing queries are scoped by `user_id = team_owner_id`,
     so reps automatically read/write into the admin's workspace.
   - resolveTeamContext(req) returns the current actor + the
     workspace + the role; if the actor is not in any team row we
     fall back to "you are your own one-person team" (backward
     compatible with the pre-team era).
   - Email-based linking lets the admin add a rep BEFORE the rep
     signs up: when the rep first signs in, we backfill clerk_user_id
     by email and they instantly join the team.
   ========================================================= */
async function resolveTeamContext(req) {
  const actorId =
    (req?.headers?.['x-actor-id'] && String(req.headers['x-actor-id']).trim()) ||
    (req?.query?.userId && String(req.query.userId).trim()) ||
    (req?.body?.userId && String(req.body.userId).trim()) ||
    null;
  const actorEmail =
    (req?.headers?.['x-actor-email'] && String(req.headers['x-actor-email']).trim()) ||
    (req?.query?.userEmail && String(req.query.userEmail).trim()) ||
    (req?.body?.userEmail && String(req.body.userEmail).trim()) ||
    null;
  const headerActorName =
    (req?.headers?.['x-actor-name'] && String(req.headers['x-actor-name']).trim()) ||
    (req?.body?.actorName && String(req.body.actorName).trim()) ||
    null;

  if (!actorId) {
    return {
      tenantUserId: null, actorUserId: null,
      role: null, memberId: null, memberName: null,
      isOwner: true, actorName: headerActorName,
    };
  }

  let rows = [];
  try {
    const r1 = await pool.query(
      `SELECT * FROM team_members
        WHERE clerk_user_id = $1 AND active = TRUE
        LIMIT 1`,
      [actorId]
    );
    rows = r1.rows;

    // Email-based first-time linkage: rep was added before they signed up,
    // now we recognise them by email and stamp their clerk_user_id in.
    if (!rows.length && actorEmail) {
      const r2 = await pool.query(
        `SELECT * FROM team_members
          WHERE clerk_user_id IS NULL
            AND LOWER(email) = LOWER($1)
            AND active = TRUE
          LIMIT 1`,
        [actorEmail]
      );
      if (r2.rows.length) {
        await pool.query(
          `UPDATE team_members
              SET clerk_user_id = $1, updated_at = NOW()
            WHERE id = $2`,
          [actorId, r2.rows[0].id]
        );
        r2.rows[0].clerk_user_id = actorId;
        rows = r2.rows;
      }
    }
  } catch (e) {
    // team_members table missing or transient DB hiccup — fall back to
    // legacy single-user mode so the workspace keeps working.
    console.warn('resolveTeamContext lookup warn:', e.message);
  }

  if (rows.length) {
    const m = rows[0];
    return {
      tenantUserId: m.team_owner_id,
      actorUserId:  actorId,
      role:         m.role,
      memberId:     m.id,
      memberName:   m.name,
      memberEmail:  m.email,
      // Sprint 4 — store_users are scoped to a single retail store.
      // The portal endpoints use this to filter memos.
      companyId:    m.company_id || null,
      isOwner:      m.role === 'owner',
      isStoreUser:  m.role === 'store_user',
      actorName:    headerActorName || m.name,
    };
  }

  // Backward compat: unrecognised user = their own one-person team.
  return {
    tenantUserId: actorId,
    actorUserId:  actorId,
    role:         'owner',
    memberId:     null,
    memberName:   null,
    isOwner:      true,
    actorName:    headerActorName,
  };
}

// Whether a team context can read a row that was assigned to `assignedTo`.
// Owners see everything; reps see their own + unassigned ("up for grabs").
// Customer-relationship gate (CRM contacts / deals / tasks).
// Reps can only read rows explicitly assigned to them. Unassigned rows
// stay private to the workspace owner — otherwise pre-existing data from
// before the team system was introduced would leak to every new rep on
// day one, and customer ownership wouldn't be enforceable.
function canReadAssignment(ctx, assignedTo) {
  if (!ctx || ctx.isOwner) return true;
  if (!assignedTo) return false;
  return String(assignedTo) === String(ctx.actorUserId);
}

// Inventory gate (loose stones / jewelry items in the catalog).
// Reps need to see what's in stock so they can pitch it to customers, so
// unassigned rows ARE visible to every rep. Once another rep claims a
// piece (sa.assigned_to = their id), only they (and the owner) see it.
function canReadInventoryItem(ctx, assignedTo) {
  if (!ctx || ctx.isOwner) return true;
  if (!assignedTo) return true;
  return String(assignedTo) === String(ctx.actorUserId);
}

/* =========================================================
   Store-portal isolation middleware.
   ---------------------------------------------------------
   When the actor's role is `store_user` we MUST refuse every
   internal API surface (stones, jewelry, CRM contacts, deals,
   memos lookup, dashboards, …). They live exclusively under
   /api/portal/* + a tiny allow-list of self-info endpoints.

   Without this, a curious store user could re-use the bearer
   userId in their browser DevTools and read the supplier's
   entire workspace — they share the same `tenantUserId`.
   ========================================================= */
const STORE_USER_ALLOWED_PREFIXES = [
  '/api/portal/',
  '/api/team/me',
];
app.use(async (req, res, next) => {
  try {
    // Cheap pre-check: only API requests, only when an actor is sent.
    if (!req.path.startsWith('/api/')) return next();
    const looksLikeActor =
      req.headers?.['x-actor-id'] ||
      req.query?.userId ||
      (req.body && req.body.userId);
    if (!looksLikeActor) return next();

    const ctx = await resolveTeamContext(req);
    if (ctx?.role !== 'store_user') return next();

    const allowed = STORE_USER_ALLOWED_PREFIXES.some((p) =>
      req.path === p || req.path.startsWith(p)
    );
    if (allowed) return next();

    return res.status(403).json({
      error: 'Store-portal users can only access /api/portal/* endpoints',
    });
  } catch (_) {
    // If anything in the gate explodes we err on the safe side and let
    // the request continue — this matches the rest of the codebase's
    // backwards-compatible "fail open to legacy single-user" stance.
    return next();
  }
});

/* =========================================================
   /api/stones – כל האבנים מטבלת stones (לא selector)
   ========================================================= */
app.get("/api/stones", async (req, res) => {
  try {
    const result = await pool.query("SELECT * FROM stones ORDER BY carat DESC");

    const formattedRows = result.rows.map((row) => ({
      ...row,
      carat: row.carat ? parseFloat(row.carat) : null,
      ratio: row.ratio ? parseFloat(row.ratio) : null,
      price_per_carat: row.price_per_carat
        ? parseFloat(row.price_per_carat)
        : null,
      total_price: row.total_price ? parseFloat(row.total_price) : null,
      measurements1: row.measurements1 || null,

      // ⭐ גם כאן נחזיר category אם קיים
      category: row.category || "",
    }));

    res.json(formattedRows);
  } catch (error) {
    console.error("❌ Error fetching stones:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

/* =========================================================
   /api/soap-stones – ל-Stone selector החדש
   Reps see their own assigned stones + unassigned ones (so they can
   claim them); admins see everything. Optional ?assignedTo= filter:
     "me"          → only mine
     "unassigned"  → only un-claimed
     <clerk-id>    → that rep's pile (admin-only respect, otherwise
                     silently coerced to "me" so reps can't snoop)
   ========================================================= */
app.get("/api/soap-stones", async (req, res) => {
  try {
    const ctx = await resolveTeamContext(req);
    const ownerId = ctx.tenantUserId || ctx.actorUserId || null;

    // What does the caller *want*?
    let assignedFilter = req.query?.assignedTo ? String(req.query.assignedTo) : null;
    if (assignedFilter === 'me') assignedFilter = ctx.actorUserId;
    // Reps can only ever see their own + unassigned, regardless of filter.
    if (!ctx.isOwner && assignedFilter && assignedFilter !== 'unassigned' && assignedFilter !== ctx.actorUserId) {
      assignedFilter = ctx.actorUserId;
    }

    const whereClauses = [`s.sku IS NOT NULL`];
    const params = [];

    // Always LEFT JOIN assignments so we can return assigned_to.
    if (ownerId) params.push(ownerId);
    const ownerParamIdx = ownerId ? params.length : null;

    // Visibility scope: admin sees all; rep sees their own claimed stones
    // PLUS every unclaimed stone in the workshop. Stones are inventory the
    // rep needs to *sell* — keeping them invisible until manually assigned
    // would mean the rep has nothing to offer customers. CRM data follows
    // a stricter policy (only explicit assignments) because it's customer
    // relationships, not stock.
    if (ownerId && !ctx.isOwner) {
      params.push(ctx.actorUserId);
      whereClauses.push(`(sa.assigned_to IS NULL OR sa.assigned_to = $${params.length})`);
    }

    // Optional explicit filter from the UI.
    if (assignedFilter === 'unassigned') {
      whereClauses.push(`sa.assigned_to IS NULL`);
    } else if (assignedFilter && assignedFilter !== 'all') {
      params.push(assignedFilter);
      whereClauses.push(`sa.assigned_to = $${params.length}`);
    }

    const sql = `
      SELECT s.*,
             sa.assigned_to AS assigned_to_clerk_id,
             sa.assigned_by AS assigned_by_clerk_id,
             sa.notes       AS assigned_notes,
             sa.updated_at  AS assigned_updated_at
        FROM soap_stones s
        ${ownerParamIdx
            ? `LEFT JOIN stone_assignments sa
                  ON sa.stone_sku = s.sku AND sa.team_owner_id = $${ownerParamIdx}`
            : `LEFT JOIN stone_assignments sa ON FALSE`}
       WHERE ${whereClauses.join(' AND ')}
       ORDER BY s.updated_at DESC
    `;
    const result = await pool.query(sql, params);

    const stones = result.rows.map((row) => {
      // בחירת תמונה ראשית
      let imageUrl = row.image;
      if (!imageUrl && row.additional_pictures) {
        const first = row.additional_pictures.split(";")[0];
        imageUrl = first ? first.trim() : null;
      }

      return {
        id: row.id,
        sku: row.sku,
        shape: row.shape,

        // ⭐ קטגוריה (Emerald / Diamond / Fancy / Gemstone וכו')
        category: row.category || "",

        // ⭐ Type — סיווג גרעיני יותר (e.g. "Diamonds MEMO" sub-type)
        type: row.type || "",

        // משקל
        weightCt: row.weight ? Number(row.weight) : null,

        // מחירים
        priceTotal:
          row.total_price !== null && row.total_price !== undefined
            ? Number(row.total_price)
            : null,
        pricePerCt:
          row.price_per_carat !== null && row.price_per_carat !== undefined
            ? Number(row.price_per_carat)
            : null,
        rapListPrice:
          row.rap_list_price !== null && row.rap_list_price !== undefined
            ? Number(row.rap_list_price)
            : null,

        // טיפול / Oil / Enhancement (מגיע מה־comment)
        treatment: row.comment || "",
        certComments: row.cert_comments || "",

        // מידות ויחס
        measurements: row.measurements || "",
        ratio:
          row.ratio !== undefined &&
          row.ratio !== null &&
          row.ratio !== ""
            ? Number(row.ratio)
            : null,

        // תמונות / וידאו / תעודה
        imageUrl,
        additionalPictures: row.additional_pictures || "",
        videoUrl: row.video || null,
        additionalVideos: row.additional_videos || "",
        certificateUrl: row.certificate_image || row.certificate_url || null,
        certificateImageJpg: row.certificate_image_jpg || null,
        certificateNumber: row.certificate_number || "",

        // מאפיינים נוספים
        lab: row.lab || "N/A",
        origin: row.origin || "N/A",
        color: row.color || "",
        clarity: row.clarity || "",
        luster: row.luster || "",
        fluorescence: row.fluorescence || "",

        // 📍 Location — חשיפה דו-שכבתית:
        //  - `location` נשמר תואם-אחורה (= branch). שאר ה-UI מסתמך על זה.
        //  - `branch` ו-`exactLocation` חדשים: סניף ומיקום פיזי מדויק בנפרד.
        location: row.branch || null,
        branch: row.branch || null,
        exactLocation: row.location || null,

        // Diamond specific fields (camelCase for frontend)
        cut: row.cut || "",
        polish: row.polish || "",
        symmetry: row.symmetry || "",
        tablePercent: row.table_percent !== null && row.table_percent !== undefined ? Number(row.table_percent) : null,
        depthPercent: row.depth_percent !== null && row.depth_percent !== undefined ? Number(row.depth_percent) : null,
        rapPrice: row.rap_price !== null && row.rap_price !== undefined ? Number(row.rap_price) : null,
        
        // Fancy diamond specific fields (camelCase for frontend)
        fancyIntensity: row.fancy_intensity || "",
        fancyColor: row.fancy_color || "",
        fancyOvertone: row.fancy_overtone || "",
        fancyColor2: row.fancy_color_2 || "",
        fancyOvertone2: row.fancy_overtone_2 || "",
        
        // Pair stone
        pairSku: row.pair_stone || null,
        
        // Grouping & inventory layout
        groupingType: row.grouping_type || "",
        box: row.box || "",
        stones: row.stones != null ? Number(row.stones) : null,

        // Marketing flags
        homePage: row.home_page || "",
        tradeShow: row.trade_show || "",

        // Sync timestamp
        updatedAt: row.updated_at ? new Date(row.updated_at).toISOString() : null,

        // Sales-rep assignment (loose-stones edition).
        // Stones don't live in jewelry_items so they have their own table.
        assignedTo:        row.assigned_to_clerk_id || null,
        assignedBy:        row.assigned_by_clerk_id || null,
        assignmentNotes:   row.assigned_notes       || null,
        assignmentUpdated: row.assigned_updated_at
          ? new Date(row.assigned_updated_at).toISOString()
          : null,
      };
    });

    res.json({ stones });
  } catch (error) {
    console.error("❌ Error fetching soap stones:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

/* =========================================================
   /api/stones/inventory-status – bulk status map
   Returns one row per SKU that currently has a non-final
   jewelry usage. The inventory list calls this once and
   merges client-side; SKUs not in the map are 'available'.

   IMPORTANT: this static path MUST be registered before the
   /api/stones/:stone_id wildcard below, otherwise Express
   matches "inventory-status" as a stone id and returns 404.
   ========================================================= */
app.get("/api/stones/inventory-status", async (req, res) => {
  try {
    const r = await pool.query(
      `SELECT jis.stone_sku,
              jis.inventory_status,
              ji.id   AS jewelry_item_id,
              ji.sku  AS jewelry_sku,
              ji.name AS jewelry_name,
              ji.status AS jewelry_status
         FROM jewelry_item_stones jis
         JOIN jewelry_items ji ON ji.id = jis.item_id
        WHERE jis.stone_sku IS NOT NULL
          AND jis.consume_from_inventory = TRUE
          AND jis.inventory_status IS NOT NULL`
    );
    const map = {};
    for (const row of r.rows) {
      const sku = row.stone_sku;
      const cur = map[sku];
      // Active (reserved/set) wins over sold; among active, set wins over reserved.
      const rank = (s) => (s === "set" ? 3 : s === "reserved" ? 2 : s === "sold" ? 1 : 0);
      if (!cur || rank(row.inventory_status) > rank(cur.status)) {
        map[sku] = {
          status: row.inventory_status,
          jewelry_item_id: row.jewelry_item_id,
          jewelry_sku: row.jewelry_sku,
          jewelry_name: row.jewelry_name,
          jewelry_status: row.jewelry_status,
        };
      }
    }
    res.json({ statuses: map });
  } catch (e) {
    console.error("Inventory-status error:", e);
    res.status(500).json({ error: e.message });
  }
});

/* =========================================================
   /api/stones/:stone_id – אבן ספציפית (מ-soap_stones)
   ========================================================= */
app.get("/api/stones/:stone_id", async (req, res) => {
  console.log("🚨 /api/stones/:stone_id CALLED");
  try {
    const { stone_id } = req.params;
    const result = await pool.query(
      "SELECT * FROM soap_stones WHERE sku = $1",
      [stone_id]
    );

    if (result.rows.length === 0) {
      return res.status(404).json({ error: "Stone not found" });
    }

    const row = result.rows[0];
    
    // Select main image
    let imageUrl = row.image;
    if (!imageUrl && row.additional_pictures) {
      const first = row.additional_pictures.split(";")[0];
      imageUrl = first ? first.trim() : null;
    }

    // Extract certificate number from URL if not provided
    let certNumber = row.certificate_number || null;
    if (!certNumber && row.certificate_image) {
      // Extract from URL like: https://app.barakdiamonds.com/Gemstones/output/Certificates/2023-107020.pdf
      const match = row.certificate_image.match(/\/([^\/]+)\.pdf$/i);
      if (match) {
        certNumber = match[1];
      }
    }

    // Map to frontend format (compatible with old format)
    const stone = {
      id: row.id,
      stone_id: row.sku,
      sku: row.sku,
      category: row.category || null, // For determining stone type
      shape: row.shape || null,
      carat: row.weight ? parseFloat(row.weight) : null,
      clarity: row.clarity || null,
      color: row.color || null,
      lab: row.lab || null,
      origin: row.origin || null,
      ratio: row.ratio ? parseFloat(row.ratio) : null,
      measurements1: row.measurements || null,
      picture: imageUrl,
      video: row.video || null,
      certificate_number: certNumber,
      certificate_url: row.certificate_image || null,
      treatment: row.comment || null, // For emeralds
      
      // Diamond-specific fields
      cut: row.cut || null,
      polish: row.polish || null,
      symmetry: row.symmetry || null,
      table_percent: row.table_percent ? parseFloat(row.table_percent) : null,
      depth_percent: row.depth_percent ? parseFloat(row.depth_percent) : null,
      fluorescence: row.fluorescence || null,
      rap_price: row.rap_price ? parseFloat(row.rap_price) : null,
      
      // Fancy-specific fields
      fancy_intensity: row.fancy_intensity || null,
      fancy_color: row.fancy_color || null,
      fancy_overtone: row.fancy_overtone || null,
      fancy_color_2: row.fancy_color_2 || null,
      fancy_overtone_2: row.fancy_overtone_2 || null,
      
      // Pair stone
      pair_stone: row.pair_stone || null,
      
      // Prices (will be encrypted below)
      price_per_carat: row.price_per_carat ? parseFloat(row.price_per_carat) : null,
      total_price: row.total_price ? parseFloat(row.total_price) : null,
    };

    // Encrypt prices
    if (stone.price_per_carat !== null && stone.price_per_carat !== undefined) {
      const raw = stone.price_per_carat;
      stone.price_per_carat = encrypt(raw.toString());
    }

    if (stone.total_price !== null && stone.total_price !== undefined) {
      const raw = stone.total_price;
      stone.total_price = encrypt(raw.toString());
    }

    res.json(stone);
  } catch (error) {
    console.error("❌ Error fetching stone:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

/* =========================================================
   /api/stones/:sku/usage – cross-system view of one stone:
     - jewelry pieces it lives in (workshop)
     - DNA inquiries that referenced it (CRM interactions)
     - sales/quote deals that included it (CRM deal items)
     - current_status:  available | reserved | set | sold
   This is what powers the "Used in / Inquired by" panel on the
   stone detail and inventory drawer so the same SKU is no longer
   blind to what's happening with it across the rest of the app.
   ========================================================= */
app.get("/api/stones/:sku/usage", async (req, res) => {
  try {
    const { sku } = req.params;
    if (!sku) return res.status(400).json({ error: "sku required" });

    // Detect whether the created_at column is present (older databases predate
    // it). Falling back to ORDER BY jis.id keeps the endpoint working on the
    // brief window before the migration above runs.
    const stoneTsCheck = await pool.query(
      `SELECT 1
         FROM information_schema.columns
        WHERE table_name = 'jewelry_item_stones'
          AND column_name = 'created_at'
        LIMIT 1`
    ).catch(() => ({ rowCount: 0 }));
    const hasStoneCreatedAt = stoneTsCheck.rowCount > 0;
    const createdAtSelect = hasStoneCreatedAt ? 'jis.created_at' : 'NULL::timestamp AS created_at';
    const createdAtOrder  = hasStoneCreatedAt ? 'jis.created_at DESC NULLS LAST, jis.id DESC' : 'jis.id DESC';

    const jewelry = await pool.query(
      `SELECT jis.id              AS link_id,
              jis.role,
              jis.quantity,
              jis.consume_from_inventory,
              jis.inventory_status,
              jis.snapshot,
              ${createdAtSelect},
              ji.id              AS jewelry_item_id,
              ji.sku             AS jewelry_sku,
              ji.name            AS jewelry_name,
              ji.status          AS jewelry_status,
              ji.cover_image_url,
              ji.contact_id,
              ji.deal_id,
              ji.sold_deal_id,
              ji.sold_at,
              c.name AS contact_name, c.company AS contact_company, c.type AS contact_type
         FROM jewelry_item_stones jis
         JOIN jewelry_items ji ON ji.id = jis.item_id
         LEFT JOIN crm_contacts c ON c.id = ji.contact_id
        WHERE jis.stone_sku = $1
        ORDER BY ${createdAtOrder}`,
      [sku]
    );

    const dnaInquiries = await pool.query(
      `SELECT i.id, i.type, i.subject, i.content, i.metadata, i.occurred_at,
              i.contact_id, i.deal_id,
              c.name AS contact_name, c.company AS contact_company, c.type AS contact_type, c.shared
         FROM crm_interactions i
         LEFT JOIN crm_contacts c ON c.id = i.contact_id
        WHERE (i.metadata->>'sku' = $1 OR i.metadata->>'dna_sku' = $1)
        ORDER BY i.occurred_at DESC NULLS LAST, i.id DESC
        LIMIT 50`,
      [sku]
    ).catch(() => ({ rows: [] }));

    const deals = await pool.query(
      `SELECT DISTINCT d.id, d.title, d.stage, d.value, d.currency, d.contact_id,
              d.dna_sku, d.created_at, d.shared,
              c.name AS contact_name, c.company AS contact_company, c.type AS contact_type
         FROM crm_deal_items di
         JOIN crm_deals d ON d.id = di.deal_id
         LEFT JOIN crm_contacts c ON c.id = d.contact_id
        WHERE di.sku = $1 OR di.snapshot->>'sku' = $1
        ORDER BY d.created_at DESC
        LIMIT 50`,
      [sku]
    ).catch(() => ({ rows: [] }));

    // Compute current_status from jewelry usage rows.
    let currentStatus = "available";
    const active = jewelry.rows.find(
      (r) =>
        r.consume_from_inventory &&
        r.inventory_status &&
        !["sold", "returned"].includes(r.inventory_status)
    );
    if (active) {
      currentStatus = active.inventory_status; // 'reserved' or 'set'
    } else {
      const lastSold = jewelry.rows.find((r) => r.consume_from_inventory && r.inventory_status === "sold");
      if (lastSold) currentStatus = "sold";
    }

    // Schema reminder: crm_contacts only has a single `name` column (no
    // first/last split) plus `company` and `type` ('business' | 'lead' | ...).
    // Show company name first for business contacts, fall back to person name.
    const fmtName = (r) => {
      const name = (r.contact_name || '').trim();
      const company = (r.contact_company || '').trim();
      if (r.contact_type === 'business') return company || name || null;
      return name || company || null;
    };

    res.json({
      sku,
      current_status: currentStatus,
      jewelry_items: jewelry.rows.map((r) => ({
        link_id: r.link_id,
        jewelry_item_id: r.jewelry_item_id,
        jewelry_sku: r.jewelry_sku,
        jewelry_name: r.jewelry_name,
        jewelry_status: r.jewelry_status,
        cover_image_url: r.cover_image_url,
        role: r.role,
        quantity: r.quantity,
        consume_from_inventory: r.consume_from_inventory,
        inventory_status: r.inventory_status,
        snapshot: r.snapshot,
        contact_id: r.contact_id,
        contact_name: fmtName(r),
        deal_id: r.deal_id,
        sold_deal_id: r.sold_deal_id,
        sold_at: r.sold_at,
        created_at: r.created_at,
      })),
      dna_inquiries: dnaInquiries.rows.map((r) => ({
        id: r.id,
        type: r.type,
        subject: r.subject,
        content: r.content,
        metadata: r.metadata,
        occurred_at: r.occurred_at,
        contact_id: r.contact_id,
        contact_name: fmtName(r),
        contact_shared: r.shared,
        deal_id: r.deal_id,
      })),
      deals: deals.rows.map((r) => ({
        id: r.id,
        title: r.title,
        stage: r.stage,
        value: r.value != null ? Number(r.value) : null,
        currency: r.currency,
        contact_id: r.contact_id,
        contact_name: fmtName(r),
        dna_sku: r.dna_sku,
        contact_shared: r.shared,
        created_at: r.created_at,
      })),
    });
  } catch (e) {
    console.error("Stone usage error:", e);
    res.status(500).json({ error: e.message });
  }
});

/* =========================================================
   /api/jewelry – all jewelry items (inventory list)
   ========================================================= */
app.get("/api/jewelry", async (req, res) => {
  try {
    const result = await pool.query("SELECT * FROM jewelry_products ORDER BY model_number ASC");
    const items = result.rows.map(row => ({
      model_number: row.model_number,
      stock_number: row.stock_number,
      jewelry_type: row.jewelry_type,
      style: row.style,
      collection: row.collection,
      price: row.price !== null ? parseFloat(row.price) : null,
      video_link: row.video_link,
      all_pictures_link: row.all_pictures_link,
      certificate_link: row.certificate_link,
      certificate_number: row.certificate_number,
      title: row.title,
      description: row.description,
      jewelry_weight: row.jewelry_weight !== null ? parseFloat(row.jewelry_weight) : null,
      total_carat: row.total_carat !== null ? parseFloat(row.total_carat) : null,
      stone_type: row.stone_type,
      center_stone_carat: row.center_stone_carat !== null ? parseFloat(row.center_stone_carat) : null,
      center_stone_shape: row.center_stone_shape,
      center_stone_color: row.center_stone_color,
      center_stone_clarity: row.center_stone_clarity,
      metal_type: row.metal_type,
      currency: row.currency,
      availability: row.availability,
      shipping_from: row.shipping_from,
      category: row.category,
      full_description: row.full_description,
      jewelry_size: row.jewelry_size,
      instructions_main: row.instructions_main,
    }));
    res.json({ jewelry: items });
  } catch (error) {
    console.error("Error fetching jewelry:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

/* =========================================================
   /api/jewelry/import-csv – Upload jewelry CSV
   ========================================================= */
let jewelryImportProgress = { active: false, phase: 'idle', progress: 0, detail: '', total: 0, processed: 0 };

app.get("/api/jewelry/import-csv/progress", (req, res) => {
  res.json(jewelryImportProgress);
});

app.post("/api/jewelry/import-csv", async (req, res) => {
  if (jewelryImportProgress.active) {
    return res.status(409).json({ success: false, error: "A jewelry import is already in progress" });
  }

  try {
    const { csvContent } = req.body;
    if (!csvContent) {
      return res.status(400).json({ success: false, error: "No CSV content provided" });
    }

    console.log("Jewelry CSV import requested");
    jewelryImportProgress = { active: true, phase: 'parsing', progress: 10, detail: 'Parsing CSV...', total: 0, processed: 0 };

    const rows = parseCsv(csvContent, { columns: true, skip_empty_lines: true, relax_quotes: true, relax_column_count: true });
    console.log(`Parsed ${rows.length} jewelry rows from CSV`);
    jewelryImportProgress = { ...jewelryImportProgress, phase: 'processing', progress: 20, detail: `Parsed ${rows.length} items`, total: rows.length };

    const columns = [
      'model_number','stock_number','jewelry_type','style','collection',
      'price','video_link','all_pictures_link','certificate_link','certificate_number',
      'title','description','jewelry_weight','total_carat','stone_type',
      'center_stone_carat','center_stone_shape','center_stone_color','center_stone_clarity',
      'metal_type','currency','availability','shipping_from','category',
      'full_description','jewelry_size','instructions_main'
    ];

    const rawValues = rows.map(r => [
      r['Model Number'] || null,
      r['Stock Number'] || null,
      r['Jewelry Type'] || null,
      r['Style'] || null,
      r['Collection'] || null,
      csvSafeNum(r['Price']),
      r['Video_Link'] || null,
      r['All_Pictures_Link'] || null,
      r['Certificate_Link'] || null,
      r['Certificate Number'] || null,
      r['Title'] || null,
      r['Description'] || null,
      csvSafeNum(r['Jewelry_Weight']),
      csvSafeNum(r['Total_Carat']),
      (r['Stone_Type'] || '').trim() || null,
      csvSafeNum(r['Center_Stone_Carat']),
      (r['Center_Stone_Shape'] || '').trim() || null,
      (r['Center_Stone_Color'] || '').trim() || null,
      (r['Center_Stone_Clarity'] || '').trim() || null,
      r['Metal_Type'] || null,
      r['Currency'] || null,
      r['Availability'] || null,
      r['Shipping_From'] || null,
      r['Category'] || null,
      r['full_description'] || null,
      r['jewelry_size'] || null,
      r['Instructions_main'] || null,
    ]).filter(v => v[0] !== null && String(v[0]).trim() !== '');

    // De-duplicate by model_number BEFORE issuing the upsert. If a single
    // CSV contains the same Model Number more than once, Postgres chokes
    // with: "ON CONFLICT DO UPDATE command cannot affect row a second time"
    // because the upsert can't touch the same target row twice in one
    // statement. Last occurrence wins (assumes the bottom of the file is
    // the freshest data).
    const dedupMap = new Map();
    let duplicateModelCount = 0;
    for (const row of rawValues) {
      const key = String(row[0]).trim();
      if (dedupMap.has(key)) duplicateModelCount += 1;
      dedupMap.set(key, row);
    }
    const values = Array.from(dedupMap.values());
    if (duplicateModelCount > 0) {
      console.warn(
        `[jewelry CSV import] Collapsed ${duplicateModelCount} duplicate Model Number row(s); kept the last occurrence of each.`
      );
    }

    jewelryImportProgress = {
      ...jewelryImportProgress,
      phase: 'clearing',
      progress: 40,
      detail: duplicateModelCount > 0
        ? `Preparing database (collapsed ${duplicateModelCount} duplicate rows)...`
        : 'Preparing database...',
    };

    await pool.query(`
      CREATE TABLE IF NOT EXISTS jewelry_products (
        model_number VARCHAR(50) PRIMARY KEY,
        stock_number VARCHAR(50),
        jewelry_type VARCHAR(50),
        style VARCHAR(50),
        collection VARCHAR(100),
        price NUMERIC(10,2),
        video_link TEXT,
        all_pictures_link TEXT,
        certificate_link TEXT,
        certificate_number VARCHAR(100),
        title VARCHAR(250),
        description TEXT,
        jewelry_weight NUMERIC(10,2),
        total_carat NUMERIC(10,3),
        stone_type VARCHAR(50),
        center_stone_carat NUMERIC(10,3),
        center_stone_shape VARCHAR(50),
        center_stone_color VARCHAR(50),
        center_stone_clarity VARCHAR(50),
        metal_type VARCHAR(50),
        currency VARCHAR(10),
        availability VARCHAR(50),
        shipping_from VARCHAR(100),
        category VARCHAR(100),
        full_description TEXT,
        jewelry_size VARCHAR(50),
        instructions_main TEXT
      );
    `);

    await pool.query('DELETE FROM jewelry_products');

    jewelryImportProgress = { ...jewelryImportProgress, phase: 'inserting', progress: 50, detail: 'Saving jewelry to database...' };
    const CHUNK = 100;
    const totalChunks = Math.ceil(values.length / CHUNK);
    for (let i = 0; i < values.length; i += CHUNK) {
      const chunk = values.slice(i, i + CHUNK);
      const chunkIdx = Math.floor(i / CHUNK) + 1;
      const ph = chunk.map((row, ri) =>
        '(' + columns.map((_, ci) => '$' + (ri * columns.length + ci + 1)).join(',') + ')'
      ).join(',');
      await pool.query('INSERT INTO jewelry_products (' + columns.join(',') + ') VALUES ' + ph + ' ON CONFLICT (model_number) DO UPDATE SET ' +
        columns.slice(1).map(c => `${c} = EXCLUDED.${c}`).join(', '),
        chunk.flat()
      );
      const pct = 50 + Math.round((chunkIdx / totalChunks) * 45);
      jewelryImportProgress = { ...jewelryImportProgress, progress: pct, processed: Math.min(i + CHUNK, values.length), detail: `Inserted ${Math.min(i + CHUNK, values.length)} / ${values.length} items` };
    }

    const completionDetail = duplicateModelCount > 0
      ? `Successfully imported ${values.length} jewelry items (collapsed ${duplicateModelCount} duplicate Model Number rows).`
      : `Successfully imported ${values.length} jewelry items!`;

    jewelryImportProgress = { active: false, phase: 'complete', progress: 100, detail: completionDetail, total: values.length, processed: values.length };
    console.log(`Jewelry CSV import completed: ${values.length} items` + (duplicateModelCount > 0 ? ` (${duplicateModelCount} duplicates collapsed)` : ''));

    res.json({
      success: true,
      count: values.length,
      duplicatesCollapsed: duplicateModelCount,
      status: "completed",
    });
  } catch (error) {
    console.error("Jewelry CSV import error:", error);
    jewelryImportProgress = { active: false, phase: 'error', progress: 0, detail: error.message, total: 0, processed: 0 };
    res.status(500).json({ success: false, error: error.message });
  }
});

/* =========================================================
   /api/jewelry/:modelNumber – תכשיט + הצפנה
   ========================================================= */
app.get("/api/jewelry/:modelNumber", async (req, res) => {
  const { modelNumber } = req.params;

  try {
    const result = await pool.query(
      "SELECT * FROM jewelry_products WHERE model_number = $1",
      [modelNumber]
    );

    if (result.rows.length === 0) {
      return res.status(404).json({ error: "Jewelry item not found" });
    }

    const item = result.rows[0];

    const numericFields = [
      "jewelry_weight",
      "total_carat",
      "center_stone_carat",
    ];
    numericFields.forEach((field) => {
      if (item[field] !== null && item[field] !== undefined) {
        item[field] = parseFloat(item[field]);
      }
    });

    if (item.price !== null && item.price !== undefined) {
      const originalPrice = item.price;
      item.price = encrypt(item.price.toString());
      console.log("🔐 Encrypted price:", originalPrice, "→", item.price);
    }

    res.json(item);
  } catch (error) {
    console.error("❌ Error fetching jewelry item:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

/* =========================================================
   /api/tags - Get all tags
   ========================================================= */
app.get("/api/tags", async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT t.*, COUNT(st.tag_id) as stone_count
      FROM tags t
      LEFT JOIN stone_tags st ON t.id = st.tag_id
      GROUP BY t.id
      ORDER BY t.created_at DESC
    `);
    res.json(result.rows);
  } catch (error) {
    console.error("❌ Error fetching tags:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

/* =========================================================
   /api/tags - Create a new tag
   ========================================================= */
app.post("/api/tags", async (req, res) => {
  try {
    const { name, color } = req.body;
    
    if (!name || !name.trim()) {
      return res.status(400).json({ error: "Tag name is required" });
    }

    const result = await pool.query(
      "INSERT INTO tags (name, color) VALUES ($1, $2) RETURNING *",
      [name.trim(), color || "#10b981"]
    );
    
    res.status(201).json(result.rows[0]);
  } catch (error) {
    if (error.code === '23505') { // Unique violation
      return res.status(400).json({ error: "Tag name already exists" });
    }
    console.error("❌ Error creating tag:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

/* =========================================================
   /api/tags/:id - Update a tag
   ========================================================= */
app.put("/api/tags/:id", async (req, res) => {
  try {
    const { id } = req.params;
    const { name, color } = req.body;
    
    if (!name || !name.trim()) {
      return res.status(400).json({ error: "Tag name is required" });
    }

    const result = await pool.query(
      "UPDATE tags SET name = $1, color = $2 WHERE id = $3 RETURNING *",
      [name.trim(), color || "#10b981", id]
    );
    
    if (result.rows.length === 0) {
      return res.status(404).json({ error: "Tag not found" });
    }
    
    res.json(result.rows[0]);
  } catch (error) {
    if (error.code === '23505') { // Unique violation
      return res.status(400).json({ error: "Tag name already exists" });
    }
    console.error("❌ Error updating tag:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

/* =========================================================
   /api/tags/:id - Delete a tag
   ========================================================= */
app.delete("/api/tags/:id", async (req, res) => {
  try {
    const { id } = req.params;
    
    // Delete all stone associations first
    await pool.query("DELETE FROM stone_tags WHERE tag_id = $1", [id]);
    
    // Then delete the tag
    const result = await pool.query("DELETE FROM tags WHERE id = $1 RETURNING *", [id]);
    
    if (result.rows.length === 0) {
      return res.status(404).json({ error: "Tag not found" });
    }
    
    res.json({ success: true, message: "Tag deleted successfully" });
  } catch (error) {
    console.error("❌ Error deleting tag:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

/* =========================================================
   /api/stone-tags - Get all stone tags (grouped by stone SKU)
   ========================================================= */
app.get("/api/stone-tags", async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT st.stone_sku, t.id, t.name, t.color
      FROM stone_tags st
      JOIN tags t ON st.tag_id = t.id
      ORDER BY st.stone_sku, t.name
    `);
    
    // Group by stone SKU
    const grouped = {};
    result.rows.forEach(row => {
      if (!grouped[row.stone_sku]) {
        grouped[row.stone_sku] = [];
      }
      grouped[row.stone_sku].push({
        id: row.id,
        name: row.name,
        color: row.color
      });
    });
    
    res.json(grouped);
  } catch (error) {
    console.error("❌ Error fetching stone tags:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

/* =========================================================
   /api/stones/:sku/tags - Add tag to a stone
   ========================================================= */
app.post("/api/stones/:sku/tags", async (req, res) => {
  try {
    const { sku } = req.params;
    const { tagId } = req.body;
    
    if (!tagId) {
      return res.status(400).json({ error: "Tag ID is required" });
    }

    // Check if association already exists
    const existing = await pool.query(
      "SELECT * FROM stone_tags WHERE stone_sku = $1 AND tag_id = $2",
      [sku, tagId]
    );
    
    if (existing.rows.length > 0) {
      return res.status(400).json({ error: "Tag already associated with this stone" });
    }

    const result = await pool.query(
      "INSERT INTO stone_tags (stone_sku, tag_id) VALUES ($1, $2) RETURNING *",
      [sku, tagId]
    );
    
    // Get the tag details
    const tagResult = await pool.query("SELECT * FROM tags WHERE id = $1", [tagId]);
    
    res.status(201).json(tagResult.rows[0]);
  } catch (error) {
    console.error("❌ Error adding tag to stone:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

/* =========================================================
   /api/stones/:sku/assign  – assign / unassign a loose stone to a rep
   Body: { assignedTo: <clerk_user_id> | null, notes?: string }
   - Owners can assign to anyone (or null = unassign).
   - Reps can only claim a stone for themselves OR clear their own claim;
     they cannot reassign someone else's stone.
   ========================================================= */
app.post("/api/stones/:sku/assign", async (req, res) => {
  try {
    const ctx = await resolveTeamContext(req);
    if (!ctx.actorUserId) return res.status(400).json({ error: 'userId is required' });
    const ownerId = ctx.tenantUserId || ctx.actorUserId;

    const { sku } = req.params;
    let { assignedTo = null, notes = null } = req.body || {};
    if (assignedTo === '' || assignedTo === undefined) assignedTo = null;
    if (assignedTo === 'me') assignedTo = ctx.actorUserId;

    // Make sure the stone actually exists in the synced inventory.
    const exists = await pool.query(
      `SELECT sku FROM soap_stones WHERE sku = $1 LIMIT 1`,
      [sku]
    );
    if (!exists.rows.length) return res.status(404).json({ error: 'Stone not found' });

    // Reps can only claim/unclaim *for themselves*.
    if (!ctx.isOwner) {
      const cur = await pool.query(
        `SELECT assigned_to FROM stone_assignments
          WHERE team_owner_id = $1 AND stone_sku = $2`,
        [ownerId, sku]
      );
      const currentAssignee = cur.rows[0]?.assigned_to || null;
      // If somebody else holds it, we refuse.
      if (currentAssignee && currentAssignee !== ctx.actorUserId) {
        return res.status(403).json({ error: 'This stone is already assigned to another rep' });
      }
      // The only legal targets for a rep: themselves or null (release).
      if (assignedTo && assignedTo !== ctx.actorUserId) {
        return res.status(403).json({ error: 'Reps can only claim a stone for themselves' });
      }
    }

    const r = await pool.query(
      `INSERT INTO stone_assignments
         (team_owner_id, stone_sku, assigned_to, assigned_by, notes)
       VALUES ($1, $2, $3, $4, $5)
       ON CONFLICT (team_owner_id, stone_sku)
       DO UPDATE SET assigned_to = EXCLUDED.assigned_to,
                     assigned_by = EXCLUDED.assigned_by,
                     notes       = COALESCE(EXCLUDED.notes, stone_assignments.notes),
                     updated_at  = NOW()
       RETURNING *`,
      [ownerId, sku, assignedTo, ctx.actorUserId, notes]
    );

    res.json({ success: true, assignment: r.rows[0] });

    logActivity({
      userId:     ownerId,
      actorId:    ctx.actorUserId,
      actorName:  ctx.actorName,
      entityType: 'loose_stone',
      entityId:   sku,
      action:     assignedTo ? 'assigned' : 'unassigned',
      summary:    assignedTo
        ? `Stone ${sku} assigned to ${assignedTo === ctx.actorUserId ? 'self' : 'rep'}`
        : `Stone ${sku} released back to the unassigned pool`,
    });
  } catch (error) {
    console.error("❌ Error assigning stone:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

/* =========================================================
   /api/stones/:sku/tags/:tagId - Remove tag from a stone
   ========================================================= */
app.delete("/api/stones/:sku/tags/:tagId", async (req, res) => {
  try {
    const { sku, tagId } = req.params;
    
    const result = await pool.query(
      "DELETE FROM stone_tags WHERE stone_sku = $1 AND tag_id = $2 RETURNING *",
      [sku, tagId]
    );
    
    if (result.rows.length === 0) {
      return res.status(404).json({ error: "Tag association not found" });
    }
    
    res.json({ success: true, message: "Tag removed from stone" });
  } catch (error) {
    console.error("❌ Error removing tag from stone:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

/* =========================================================
   /api/sync – Trigger SOAP data sync (with progress tracking)
   ========================================================= */
const { run: runSoapImport } = require('./importFromSoap');

// In-memory sync progress state
let syncProgress = {
  active: false,
  phase: 'idle',
  progress: 0,
  detail: '',
  totalStones: 0,
  processedStones: 0,
  startedAt: null,
};

app.get("/api/sync/progress", (req, res) => {
  res.json(syncProgress);
});

app.post("/api/sync", async (req, res) => {
  if (syncProgress.active) {
    return res.status(409).json({ 
      success: false, 
      error: "A sync is already in progress",
      progress: syncProgress 
    });
  }

  try {
    console.log("🔄 SOAP sync requested via API");
    
    syncProgress = {
      active: true,
      phase: 'starting',
      progress: 0,
      detail: 'Starting sync...',
      totalStones: 0,
      processedStones: 0,
      startedAt: Date.now(),
    };

    const onProgress = (update) => {
      syncProgress = { ...syncProgress, ...update };
    };

    // Run the import directly (not via exec) using the server's db pool
    // closePool: false so we don't kill the server's connection pool
    const result = await runSoapImport({ dbPool: pool, closePool: false, onProgress });
    
    if (result.success) {
      console.log(`✅ Sync completed: ${result.count} stones`);
      syncProgress = { 
        ...syncProgress, 
        active: false, 
        phase: 'complete', 
        progress: 100, 
        detail: `Successfully synced ${result.count} stones!`,
        processedStones: result.count,
      };
      res.json({ 
        success: true, 
        message: result.message,
        count: result.count,
        status: "completed"
      });
    } else {
      console.error("❌ Sync failed:", result.message);
      syncProgress = { 
        ...syncProgress, 
        active: false, 
        phase: 'error', 
        progress: 0, 
        detail: result.message 
      };
      res.status(500).json({ 
        success: false, 
        error: result.message,
        status: "failed"
      });
    }
  } catch (error) {
    console.error("❌ Error during sync:", error);
    syncProgress = { 
      ...syncProgress, 
      active: false, 
      phase: 'error', 
      progress: 0, 
      detail: error.message 
    };
    res.status(500).json({ 
      success: false, 
      error: error.message 
    });
  }
});

/* =========================================================
   /api/image-proxy – Proxy for loading images (bypass CORS)
   ========================================================= */
const fetch = require('node-fetch');

app.get("/api/image-proxy", async (req, res) => {
  try {
    const { url } = req.query;
    
    if (!url) {
      return res.status(400).json({ error: "URL parameter required" });
    }

    console.log("📷 Proxying image:", url);

    // Fetch the image from the external URL
    const response = await fetch(url, {
      timeout: 15000,
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'image/*,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
      }
    });

    if (!response.ok) {
      console.log("❌ Image fetch failed:", response.status);
      return res.status(response.status).json({ error: "Failed to fetch image" });
    }

    // Get content type
    const contentType = response.headers.get('content-type') || 'image/jpeg';
    
    // Get the image buffer
    const buffer = await response.buffer();
    
    // Convert to base64
    const base64 = buffer.toString('base64');
    const dataUri = `data:${contentType};base64,${base64}`;
    
    console.log("✅ Image proxied successfully, size:", buffer.length);
    res.json({ image: dataUri });
  } catch (error) {
    console.error("❌ Error proxying image:", error.message);
    res.status(500).json({ error: "Failed to proxy image: " + error.message });
  }
});

/* =========================================================
   /api/import-csv – Import stones from CSV file upload
   ========================================================= */
const { parse: parseCsv } = require('csv-parse/sync');

const CSV_BRANCH_MAP = {
  IL:'Israel',EM:'Israel',JI:'Israel',
  LA:'Los Angeles',EL:'Los Angeles',
  HK:'Hong Kong',ES:'Hong Kong',HS:'Hong Kong',JH:'Hong Kong',JS:'Hong Kong',EH:'Hong Kong',
  NY:'New York',EN:'New York',ET:'New York',DT:'New York',JT:'New York',EG:'New York',
  EV:'New York',GN:'New York',VG:'New York',JG:'New York',JV:'New York',EY:'New York',
  HKG:'Hong Kong',ISR:'Israel',NYC:'New York'
};

const csvMapBranch = (b) => {
  if (!b) return null;
  const clean = b.trim();
  if (clean.includes('http://') || clean.includes('https://') || clean.length > 20) return null;
  return CSV_BRANCH_MAP[clean.toUpperCase()] || clean;
};

const csvSafeNum = (v) => {
  const n = parseFloat(v);
  return Number.isFinite(n) ? n : null;
};

let csvImportProgress = {
  active: false,
  phase: 'idle',
  progress: 0,
  detail: '',
  totalStones: 0,
  processedStones: 0,
};

app.get("/api/import-csv/progress", (req, res) => {
  res.json(csvImportProgress);
});

app.post("/api/import-csv", async (req, res) => {
  if (csvImportProgress.active) {
    return res.status(409).json({ success: false, error: "A CSV import is already in progress" });
  }

  try {
    const { csvContent } = req.body;
    if (!csvContent) {
      return res.status(400).json({ success: false, error: "No CSV content provided" });
    }

    console.log("📄 CSV import requested via API");
    csvImportProgress = { active: true, phase: 'parsing', progress: 10, detail: 'Parsing CSV...', totalStones: 0, processedStones: 0 };

    const rows = parseCsv(csvContent, { columns: true, skip_empty_lines: true, relax_quotes: true, relax_column_count: true });
    console.log(`📄 Parsed ${rows.length} rows from CSV`);
    csvImportProgress = { ...csvImportProgress, phase: 'processing', progress: 20, detail: `Parsed ${rows.length} stones`, totalStones: rows.length };

    const columns = [
      'category','sku','shape','weight','color','clarity','lab',
      'fluorescence','price_per_carat','rap_price','rap_list_price',
      'total_price','location','branch','image','additional_pictures',
      'video','additional_videos','certificate_image','certificate_number',
      'certificate_image_jpg','cut','polish','symmetry','table_percent',
      'depth_percent','ratio','measurements','fancy_intensity',
      'fancy_color','fancy_overtone','fancy_color_2','fancy_overtone_2',
      'pair_stone','home_page','trade_show','comment','type',
      'cert_comments','origin','grouping_type','box','stones','raw_xml'
    ];

    const values = rows.map(r => {
      const ppc = csvSafeNum(r['Price Per Carat']);
      const tp = csvSafeNum(r['Total Price']);
      return [
        r['Category'] || null,
        r['SKU'] || null,
        r['Shape'] || null,
        csvSafeNum(r['Weight']),
        r['Color'] || null,
        r['Clarity'] || null,
        r['Lab'] || null,
        r['Fluorescence'] || null,
        ppc !== null ? ppc * 2 : null,
        csvSafeNum(r['Rap Price % ']),
        csvSafeNum(r['Rap. Price']),
        tp !== null ? tp * 2 : null,
        r['Location'] || null,
        csvMapBranch(r['Branch']),
        r['Image'] || null,
        r['additional_pictures'] || null,
        r['Video'] || null,
        r['additional_videos'] || null,
        r['Certificate image'] || null,
        r['Certificate Number'] || null,
        r['certificateImageJPG'] || null,
        r['Cut'] || null,
        r['Polish'] || null,
        r['Symmetry'] || null,
        csvSafeNum(r['Table']),
        csvSafeNum(r['Depth']),
        csvSafeNum(r['ratio']),
        r['Measurements (- delimiter)'] || null,
        r['fancy_intensity'] || null,
        r['fancy_color'] || null,
        r['fancy_overtone'] || null,
        r['fancy_color_2'] || null,
        r['fancy_overtone_2'] || null,
        r['Pair Stone'] || null,
        r['home_page'] || null,
        r['TradeShow'] || null,
        r['Comment'] || null,
        r['Type'] || null,
        r['Cert. Comments'] || null,
        r['Origin'] || null,
        r['Grouping Type'] || null,
        r['Box'] || null,
        csvSafeNum(r['Stones']),
        'csv_import'
      ];
    });

    csvImportProgress = { ...csvImportProgress, phase: 'clearing', progress: 40, detail: 'Preparing database...' };
    await pool.query('TRUNCATE TABLE soap_stones RESTART IDENTITY');

    csvImportProgress = { ...csvImportProgress, phase: 'inserting', progress: 50, detail: 'Saving stones to database...' };
    const CHUNK = 300;
    const totalChunks = Math.ceil(values.length / CHUNK);
    for (let i = 0; i < values.length; i += CHUNK) {
      const chunk = values.slice(i, i + CHUNK);
      const chunkIdx = Math.floor(i / CHUNK) + 1;
      const ph = chunk.map((row, ri) =>
        '(' + columns.map((_, ci) => '$' + (ri * columns.length + ci + 1)).join(',') + ')'
      ).join(',');
      await pool.query('INSERT INTO soap_stones (' + columns.join(',') + ') VALUES ' + ph, chunk.flat());
      const pct = 50 + Math.round((chunkIdx / totalChunks) * 45);
      csvImportProgress = { ...csvImportProgress, progress: pct, processedStones: Math.min(i + CHUNK, values.length), detail: `Inserted ${Math.min(i + CHUNK, values.length)} / ${values.length} stones` };
    }

    csvImportProgress = { active: false, phase: 'complete', progress: 100, detail: `Successfully imported ${values.length} stones!`, totalStones: values.length, processedStones: values.length };
    console.log(`✅ CSV import completed: ${values.length} stones`);

    res.json({ success: true, count: values.length, status: "completed" });
  } catch (error) {
    console.error("❌ CSV import error:", error);
    csvImportProgress = { active: false, phase: 'error', progress: 0, detail: error.message, totalStones: 0, processedStones: 0 };
    res.status(500).json({ success: false, error: error.message });
  }
});

/* =========================================================
   Saved Filters
   ========================================================= */

// Auto-create table on startup
pool.query(`
  CREATE TABLE IF NOT EXISTS saved_filters (
    id SERIAL PRIMARY KEY,
    user_id TEXT NOT NULL,
    name TEXT NOT NULL,
    inventory_mode TEXT NOT NULL DEFAULT 'diamonds',
    filters JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMP DEFAULT NOW()
  )
`).catch(err => console.error("saved_filters table creation error:", err));

app.get("/api/saved-filters", async (req, res) => {
  try {
    const { userId } = req.query;
    if (!userId) return res.status(400).json({ error: "userId is required" });
    const result = await pool.query(
      "SELECT * FROM saved_filters WHERE user_id = $1 ORDER BY created_at DESC",
      [userId]
    );
    res.json(result.rows);
  } catch (error) {
    console.error("Error fetching saved filters:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

app.post("/api/saved-filters", async (req, res) => {
  try {
    const { userId, name, inventoryMode, filters } = req.body;
    if (!userId || !name?.trim()) {
      return res.status(400).json({ error: "userId and name are required" });
    }
    const result = await pool.query(
      "INSERT INTO saved_filters (user_id, name, inventory_mode, filters) VALUES ($1, $2, $3, $4) RETURNING *",
      [userId, name.trim(), inventoryMode || 'diamonds', JSON.stringify(filters || {})]
    );
    res.status(201).json(result.rows[0]);
  } catch (error) {
    console.error("Error creating saved filter:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

app.delete("/api/saved-filters/:id", async (req, res) => {
  try {
    const { id } = req.params;
    await pool.query("DELETE FROM saved_filters WHERE id = $1", [id]);
    res.json({ success: true });
  } catch (error) {
    console.error("Error deleting saved filter:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

/* =========================================================
   Label Templates (per user)
   ========================================================= */

pool.query(`
  CREATE TABLE IF NOT EXISTS label_templates (
    id SERIAL PRIMARY KEY,
    user_id TEXT NOT NULL,
    name TEXT NOT NULL DEFAULT 'Default',
    elements JSONB NOT NULL DEFAULT '[]',
    is_active BOOLEAN NOT NULL DEFAULT false,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
  )
`).catch(err => console.error("label_templates table creation error:", err));

app.get("/api/label-templates", async (req, res) => {
  try {
    const { userId } = req.query;
    if (!userId) return res.status(400).json({ error: "userId is required" });
    const result = await pool.query(
      "SELECT * FROM label_templates WHERE user_id = $1 ORDER BY created_at ASC",
      [userId]
    );
    res.json(result.rows);
  } catch (error) {
    console.error("Error fetching label templates:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

app.post("/api/label-templates", async (req, res) => {
  try {
    const { userId, name, elements, isActive } = req.body;
    if (!userId) return res.status(400).json({ error: "userId is required" });
    const result = await pool.query(
      "INSERT INTO label_templates (user_id, name, elements, is_active) VALUES ($1, $2, $3, $4) RETURNING *",
      [userId, (name || "New Template").trim(), JSON.stringify(elements || []), isActive || false]
    );
    res.status(201).json(result.rows[0]);
  } catch (error) {
    console.error("Error creating label template:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

app.put("/api/label-templates/:id", async (req, res) => {
  try {
    const { id } = req.params;
    const { name, elements, isActive } = req.body;
    const fields = [];
    const values = [];
    let idx = 1;

    if (name !== undefined) { fields.push(`name = $${idx++}`); values.push(name.trim()); }
    if (elements !== undefined) { fields.push(`elements = $${idx++}`); values.push(JSON.stringify(elements)); }
    if (isActive !== undefined) { fields.push(`is_active = $${idx++}`); values.push(isActive); }
    fields.push(`updated_at = NOW()`);

    if (fields.length === 1) return res.status(400).json({ error: "No fields to update" });

    values.push(id);
    const result = await pool.query(
      `UPDATE label_templates SET ${fields.join(", ")} WHERE id = $${idx} RETURNING *`,
      values
    );
    if (result.rows.length === 0) return res.status(404).json({ error: "Template not found" });
    res.json(result.rows[0]);
  } catch (error) {
    console.error("Error updating label template:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

app.put("/api/label-templates/set-active/:id", async (req, res) => {
  try {
    const { id } = req.params;
    const { userId } = req.body;
    if (!userId) return res.status(400).json({ error: "userId is required" });
    await pool.query("UPDATE label_templates SET is_active = false WHERE user_id = $1", [userId]);
    const result = await pool.query(
      "UPDATE label_templates SET is_active = true, updated_at = NOW() WHERE id = $1 AND user_id = $2 RETURNING *",
      [id, userId]
    );
    if (result.rows.length === 0) return res.status(404).json({ error: "Template not found" });
    res.json(result.rows[0]);
  } catch (error) {
    console.error("Error setting active template:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

app.delete("/api/label-templates/:id", async (req, res) => {
  try {
    const { id } = req.params;
    await pool.query("DELETE FROM label_templates WHERE id = $1", [id]);
    res.json({ success: true });
  } catch (error) {
    console.error("Error deleting label template:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

/* =========================================================
   CRM – Contacts, Interactions, Deals, Tasks, WhatsApp Log
   ========================================================= */

let crmReady = false;
const crmReadyPromise = (async () => {
  try {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS crm_contacts (
        id SERIAL PRIMARY KEY,
        user_id TEXT NOT NULL,
        name TEXT NOT NULL,
        type TEXT NOT NULL DEFAULT 'lead',
        company TEXT,
        phone TEXT,
        email TEXT,
        country TEXT,
        city TEXT,
        address TEXT,
        source TEXT,
        status TEXT DEFAULT 'active',
        tags JSONB DEFAULT '[]',
        preferences JSONB DEFAULT '{}',
        notes TEXT,
        avatar_url TEXT,
        last_contact_at TIMESTAMP,
        created_at TIMESTAMP DEFAULT NOW(),
        updated_at TIMESTAMP DEFAULT NOW()
      )
    `);
    await pool.query(`
      CREATE TABLE IF NOT EXISTS crm_deals (
        id SERIAL PRIMARY KEY,
        user_id TEXT NOT NULL,
        contact_id INTEGER NOT NULL REFERENCES crm_contacts(id) ON DELETE CASCADE,
        title TEXT NOT NULL,
        stage TEXT NOT NULL DEFAULT 'lead',
        value NUMERIC(14,2) DEFAULT 0,
        currency TEXT DEFAULT 'USD',
        probability INTEGER DEFAULT 0,
        expected_close DATE,
        actual_close DATE,
        notes TEXT,
        created_at TIMESTAMP DEFAULT NOW(),
        updated_at TIMESTAMP DEFAULT NOW()
      )
    `);
    await pool.query(`
      CREATE TABLE IF NOT EXISTS crm_interactions (
        id SERIAL PRIMARY KEY,
        user_id TEXT NOT NULL,
        contact_id INTEGER NOT NULL REFERENCES crm_contacts(id) ON DELETE CASCADE,
        deal_id INTEGER,
        type TEXT NOT NULL,
        direction TEXT DEFAULT 'outgoing',
        subject TEXT,
        content TEXT,
        metadata JSONB DEFAULT '{}',
        occurred_at TIMESTAMP DEFAULT NOW(),
        created_at TIMESTAMP DEFAULT NOW()
      )
    `);
    await pool.query(`
      CREATE TABLE IF NOT EXISTS crm_deal_items (
        id SERIAL PRIMARY KEY,
        deal_id INTEGER NOT NULL REFERENCES crm_deals(id) ON DELETE CASCADE,
        stone_id TEXT,
        sku TEXT,
        category TEXT,
        snapshot JSONB DEFAULT '{}',
        custom_price NUMERIC(14,2),
        quantity INTEGER DEFAULT 1,
        notes TEXT,
        created_at TIMESTAMP DEFAULT NOW()
      )
    `);
    await pool.query(`
      CREATE TABLE IF NOT EXISTS crm_tasks (
        id SERIAL PRIMARY KEY,
        user_id TEXT NOT NULL,
        contact_id INTEGER REFERENCES crm_contacts(id) ON DELETE CASCADE,
        deal_id INTEGER REFERENCES crm_deals(id) ON DELETE CASCADE,
        title TEXT NOT NULL,
        description TEXT,
        due_date TIMESTAMP,
        priority TEXT DEFAULT 'normal',
        status TEXT DEFAULT 'pending',
        completed_at TIMESTAMP,
        created_at TIMESTAMP DEFAULT NOW(),
        updated_at TIMESTAMP DEFAULT NOW()
      )
    `);
    await pool.query(`
      CREATE TABLE IF NOT EXISTS crm_whatsapp_log (
        id SERIAL PRIMARY KEY,
        user_id TEXT NOT NULL,
        contact_id INTEGER REFERENCES crm_contacts(id) ON DELETE SET NULL,
        phone TEXT,
        message TEXT NOT NULL,
        related_items JSONB DEFAULT '[]',
        sent_at TIMESTAMP DEFAULT NOW()
      )
    `);

    // Folders (hierarchical)
    await pool.query(`
      CREATE TABLE IF NOT EXISTS crm_folders (
        id SERIAL PRIMARY KEY,
        user_id TEXT NOT NULL,
        name TEXT NOT NULL,
        parent_id INTEGER REFERENCES crm_folders(id) ON DELETE CASCADE,
        color TEXT,
        created_at TIMESTAMP DEFAULT NOW(),
        updated_at TIMESTAMP DEFAULT NOW()
      )
    `);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_crm_folders_user ON crm_folders(user_id)`);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_crm_folders_parent ON crm_folders(parent_id)`);

    // Per-user OAuth/integration tokens (Outlook, etc.)
    await pool.query(`
      CREATE TABLE IF NOT EXISTS crm_integrations (
        id SERIAL PRIMARY KEY,
        user_id TEXT NOT NULL,
        provider TEXT NOT NULL,
        account_email TEXT,
        account_name TEXT,
        access_token_enc TEXT,
        refresh_token_enc TEXT,
        expires_at TIMESTAMP,
        scope TEXT,
        last_sync_at TIMESTAMP,
        metadata JSONB DEFAULT '{}',
        created_at TIMESTAMP DEFAULT NOW(),
        updated_at TIMESTAMP DEFAULT NOW(),
        UNIQUE(user_id, provider)
      )
    `);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_crm_integrations_user ON crm_integrations(user_id)`);

    // Email broadcast log
    await pool.query(`
      CREATE TABLE IF NOT EXISTS crm_email_broadcasts (
        id SERIAL PRIMARY KEY,
        user_id TEXT NOT NULL,
        subject TEXT,
        body TEXT,
        recipients_count INTEGER DEFAULT 0,
        sent_count INTEGER DEFAULT 0,
        failed_count INTEGER DEFAULT 0,
        details JSONB DEFAULT '[]',
        sent_at TIMESTAMP DEFAULT NOW()
      )
    `);

    // Saved email templates (per-user, reusable HTML templates)
    await pool.query(`
      CREATE TABLE IF NOT EXISTS crm_email_templates (
        id SERIAL PRIMARY KEY,
        user_id TEXT NOT NULL,
        name TEXT NOT NULL,
        subject TEXT,
        html TEXT,
        thumbnail TEXT,
        created_at TIMESTAMP DEFAULT NOW(),
        updated_at TIMESTAMP DEFAULT NOW()
      )
    `);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_crm_email_templates_user ON crm_email_templates(user_id)`);

    // Invoices (per-customer billing)
    await pool.query(`
      CREATE TABLE IF NOT EXISTS crm_invoices (
        id SERIAL PRIMARY KEY,
        user_id TEXT NOT NULL,
        contact_id INTEGER REFERENCES crm_contacts(id) ON DELETE CASCADE,
        deal_id INTEGER REFERENCES crm_deals(id) ON DELETE SET NULL,
        invoice_number TEXT NOT NULL,
        status TEXT DEFAULT 'draft',
        subtotal NUMERIC(14,2) DEFAULT 0,
        tax NUMERIC(14,2) DEFAULT 0,
        total NUMERIC(14,2) DEFAULT 0,
        currency TEXT DEFAULT 'USD',
        issued_at DATE,
        due_at DATE,
        paid_at TIMESTAMP,
        notes TEXT,
        metadata JSONB DEFAULT '{}',
        created_at TIMESTAMP DEFAULT NOW(),
        updated_at TIMESTAMP DEFAULT NOW()
      )
    `);

    // Occasions (birthdays, anniversaries, weddings — recurring or one-off)
    await pool.query(`
      CREATE TABLE IF NOT EXISTS crm_occasions (
        id SERIAL PRIMARY KEY,
        user_id TEXT NOT NULL,
        contact_id INTEGER REFERENCES crm_contacts(id) ON DELETE CASCADE,
        kind TEXT NOT NULL,
        label TEXT,
        occurs_on DATE NOT NULL,
        recurring_yearly BOOLEAN DEFAULT TRUE,
        notes TEXT,
        created_at TIMESTAMP DEFAULT NOW()
      )
    `);

    // Sprint 2 / Phase 1 — unified activity_log.
    // entity_id is TEXT (not INTEGER) so the same table can hold rows for
    // both serial-id entities (deals, tasks) and string-keyed ones (stone SKUs).
    await pool.query(`
      CREATE TABLE IF NOT EXISTS activity_log (
        id           BIGSERIAL PRIMARY KEY,
        user_id      TEXT NOT NULL,
        actor_id     TEXT,
        actor_name   TEXT,
        entity_type  TEXT NOT NULL,
        entity_id    TEXT NOT NULL,
        action       TEXT NOT NULL,
        summary      TEXT,
        changes      JSONB,
        related      JSONB,
        occurred_at  TIMESTAMP DEFAULT NOW()
      )
    `);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_activity_user_time   ON activity_log(user_id, occurred_at DESC)`);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_activity_entity      ON activity_log(entity_type, entity_id, occurred_at DESC)`);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_activity_actor_time  ON activity_log(actor_id, occurred_at DESC)`);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_activity_action_time ON activity_log(action, occurred_at DESC)`);

    // One-time backfill: when activity_log is empty, seed it with a
    // 'created' row per existing entity using the row's own created_at /
    // occurred_at. Without this, the Overview feed would look empty for
    // existing workspaces on day-1 of the rollout.
    try {
      const { rows: [{ count }] } = await pool.query(
        `SELECT COUNT(*)::int AS count FROM activity_log`
      );
      if (count === 0) {
        await pool.query(`
          INSERT INTO activity_log
            (user_id, actor_id, actor_name, entity_type, entity_id, action, summary, occurred_at)
          SELECT user_id, user_id, NULL, 'contact', id::text, 'created',
                 'Created contact ' || COALESCE(NULLIF(name,''), '#' || id), created_at
            FROM crm_contacts WHERE created_at IS NOT NULL
          UNION ALL
          SELECT user_id, user_id, NULL, 'deal', id::text, 'created',
                 'Created deal: ' || COALESCE(NULLIF(title,''), '#' || id), created_at
            FROM crm_deals WHERE created_at IS NOT NULL
          UNION ALL
          SELECT user_id, user_id, NULL, 'jewelry_item', id::text, 'created',
                 COALESCE('Added jewelry: ' || NULLIF(name,''), 'Added jewelry ' || sku), created_at
            FROM jewelry_items WHERE created_at IS NOT NULL
          UNION ALL
          SELECT user_id, user_id, NULL, 'task', id::text, 'created',
                 'Created task: ' || COALESCE(NULLIF(title,''), '#' || id), created_at
            FROM crm_tasks WHERE created_at IS NOT NULL
          UNION ALL
          SELECT user_id, user_id, NULL, 'interaction', id::text, COALESCE(type,'logged'),
                 COALESCE(NULLIF(subject,''), 'Logged interaction'), occurred_at
            FROM crm_interactions WHERE occurred_at IS NOT NULL
        `);
        console.log('🧬 activity_log: backfilled from existing entities');
      }
    } catch (e) {
      console.warn('activity_log backfill skipped:', e.message);
    }

    // Add new columns to crm_contacts (idempotent)
    const newCols = [
      "ALTER TABLE crm_contacts ADD COLUMN IF NOT EXISTS title TEXT",
      "ALTER TABLE crm_contacts ADD COLUMN IF NOT EXISTS website TEXT",
      "ALTER TABLE crm_contacts ADD COLUMN IF NOT EXISTS phone_alt TEXT",
      "ALTER TABLE crm_contacts ADD COLUMN IF NOT EXISTS folder_id INTEGER",
      "ALTER TABLE crm_contacts ADD COLUMN IF NOT EXISTS linked_contact_ids JSONB DEFAULT '[]'",
      "ALTER TABLE crm_contacts ADD COLUMN IF NOT EXISTS card_back_notes TEXT",
      "ALTER TABLE crm_contacts ADD COLUMN IF NOT EXISTS outlook_contact_id TEXT",
      "ALTER TABLE crm_contacts ADD COLUMN IF NOT EXISTS outlook_synced_at TIMESTAMP",
      "ALTER TABLE crm_contacts ADD COLUMN IF NOT EXISTS card_image_front TEXT",
      "ALTER TABLE crm_contacts ADD COLUMN IF NOT EXISTS card_image_back TEXT",
      "ALTER TABLE crm_contacts ADD COLUMN IF NOT EXISTS card_image_thumb TEXT",
      // DNA-lead support: contacts visible to all users + which stone the lead is for
      "ALTER TABLE crm_contacts ADD COLUMN IF NOT EXISTS shared BOOLEAN DEFAULT FALSE",
      "ALTER TABLE crm_contacts ADD COLUMN IF NOT EXISTS dna_sku TEXT",
      "ALTER TABLE crm_deals ADD COLUMN IF NOT EXISTS shared BOOLEAN DEFAULT FALSE",
      "ALTER TABLE crm_deals ADD COLUMN IF NOT EXISTS dna_sku TEXT",
      "CREATE INDEX IF NOT EXISTS idx_crm_contacts_shared ON crm_contacts(shared) WHERE shared = TRUE",
      "CREATE INDEX IF NOT EXISTS idx_crm_deals_shared ON crm_deals(shared) WHERE shared = TRUE",
      "CREATE INDEX IF NOT EXISTS idx_crm_contacts_email_lower ON crm_contacts(LOWER(email))",
      "CREATE INDEX IF NOT EXISTS idx_crm_contacts_phone_norm ON crm_contacts(regexp_replace(COALESCE(phone,''),'[^0-9]','','g'))",
      // Performance indexes (huge speedup for the contacts list + drawer queries)
      "CREATE INDEX IF NOT EXISTS idx_crm_contacts_user_id ON crm_contacts(user_id)",
      "CREATE INDEX IF NOT EXISTS idx_crm_contacts_updated_at ON crm_contacts(updated_at DESC)",
      "CREATE INDEX IF NOT EXISTS idx_crm_contacts_folder_id ON crm_contacts(folder_id) WHERE folder_id IS NOT NULL",
      "CREATE INDEX IF NOT EXISTS idx_crm_contacts_type ON crm_contacts(type)",
      "CREATE INDEX IF NOT EXISTS idx_crm_deals_contact_id ON crm_deals(contact_id)",
      "CREATE INDEX IF NOT EXISTS idx_crm_deals_contact_stage ON crm_deals(contact_id, stage)",
      "CREATE INDEX IF NOT EXISTS idx_crm_deals_user_id ON crm_deals(user_id)",
      "CREATE INDEX IF NOT EXISTS idx_crm_interactions_contact ON crm_interactions(contact_id, occurred_at DESC)",
      "CREATE INDEX IF NOT EXISTS idx_crm_interactions_deal ON crm_interactions(deal_id) WHERE deal_id IS NOT NULL",
      "CREATE INDEX IF NOT EXISTS idx_crm_tasks_contact ON crm_tasks(contact_id)",
      "CREATE INDEX IF NOT EXISTS idx_crm_tasks_deal ON crm_tasks(deal_id) WHERE deal_id IS NOT NULL",
      "CREATE INDEX IF NOT EXISTS idx_crm_deal_items_deal ON crm_deal_items(deal_id)",
      "CREATE INDEX IF NOT EXISTS idx_crm_folders_parent ON crm_folders(parent_id)",
      "CREATE INDEX IF NOT EXISTS idx_crm_folders_user ON crm_folders(user_id)",
      // Invoices + occasions
      "CREATE INDEX IF NOT EXISTS idx_crm_invoices_contact ON crm_invoices(contact_id)",
      "CREATE INDEX IF NOT EXISTS idx_crm_invoices_user ON crm_invoices(user_id)",
      "CREATE INDEX IF NOT EXISTS idx_crm_invoices_status ON crm_invoices(status)",
      "CREATE INDEX IF NOT EXISTS idx_crm_invoices_deal ON crm_invoices(deal_id) WHERE deal_id IS NOT NULL",
      "CREATE INDEX IF NOT EXISTS idx_crm_occasions_contact ON crm_occasions(contact_id)",
      "CREATE INDEX IF NOT EXISTS idx_crm_occasions_user_date ON crm_occasions(user_id, occurs_on)",
      // Unread DNA leads badge query (polled every 30s by every signed-in user)
      "CREATE INDEX IF NOT EXISTS idx_crm_contacts_dna_recent ON crm_contacts(created_at DESC) WHERE shared = TRUE AND source = 'dna_lead'",
      // Sprint 3 / Team & Sales-Rep system: every CRM record can be assigned
      // to a rep so they see their own pipeline by default. NULL = unassigned
      // ("up for grabs"). Owner always sees everything regardless.
      "ALTER TABLE crm_contacts ADD COLUMN IF NOT EXISTS assigned_to TEXT",
      "ALTER TABLE crm_deals    ADD COLUMN IF NOT EXISTS assigned_to TEXT",
      "ALTER TABLE crm_tasks    ADD COLUMN IF NOT EXISTS assigned_to TEXT",
      "CREATE INDEX IF NOT EXISTS idx_crm_contacts_assigned_to ON crm_contacts(assigned_to) WHERE assigned_to IS NOT NULL",
      "CREATE INDEX IF NOT EXISTS idx_crm_deals_assigned_to    ON crm_deals(assigned_to)    WHERE assigned_to IS NOT NULL",
      "CREATE INDEX IF NOT EXISTS idx_crm_tasks_assigned_to    ON crm_tasks(assigned_to)    WHERE assigned_to IS NOT NULL",
    ];
    for (const sql of newCols) {
      try { await pool.query(sql); } catch (e) { console.warn("Migration warn:", e.message); }
    }

    // Sprint 3 — Team / Sales-Rep registry. Each row is either the workspace
    // OWNER (one per team_owner_id, role='owner') or a REP they invited.
    // `clerk_user_id` is filled in lazily on first sign-in by email match
    // (see resolveTeamContext).
    await pool.query(`
      CREATE TABLE IF NOT EXISTS team_members (
        id SERIAL PRIMARY KEY,
        team_owner_id   TEXT NOT NULL,
        clerk_user_id   TEXT,
        email           TEXT NOT NULL,
        name            TEXT NOT NULL,
        role            TEXT NOT NULL DEFAULT 'rep',
        avatar_color    TEXT,
        commission_pct  NUMERIC(6,2) DEFAULT 0,
        quota_monthly   NUMERIC(14,2) DEFAULT 0,
        active          BOOLEAN DEFAULT TRUE,
        created_at      TIMESTAMP DEFAULT NOW(),
        updated_at      TIMESTAMP DEFAULT NOW()
      )
    `);
    await pool.query(`CREATE UNIQUE INDEX IF NOT EXISTS idx_team_members_clerk      ON team_members(clerk_user_id) WHERE clerk_user_id IS NOT NULL`);
    // Old (non-partial) email uniqueness index used to block re-inviting a
    // soft-deleted rep with the same email. Drop it and recreate as a
    // partial index so the email slot is freed the moment a row goes
    // inactive (= "Removed" from the UI).
    await pool.query(`DROP INDEX IF EXISTS idx_team_members_team_email`);
    await pool.query(`CREATE UNIQUE INDEX IF NOT EXISTS idx_team_members_team_email ON team_members(team_owner_id, LOWER(email)) WHERE active = TRUE`);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_team_members_owner             ON team_members(team_owner_id) WHERE active = TRUE`);

    // Invitation lifecycle metadata. Older rows (pre-email integration) get
    // back-filled to created_at so the UI can show a sane "Invited X ago".
    await pool.query(`ALTER TABLE team_members ADD COLUMN IF NOT EXISTS invited_at      TIMESTAMP`);
    await pool.query(`ALTER TABLE team_members ADD COLUMN IF NOT EXISTS last_invited_at TIMESTAMP`);
    await pool.query(`ALTER TABLE team_members ADD COLUMN IF NOT EXISTS invite_count    INTEGER DEFAULT 0`);
    // Sprint 4 — Store-portal users. role = 'store_user' + company_id
    // pins them to a single retail store; resolveTeamContext exposes
    // companyId so the portal endpoints can scope queries automatically.
    await pool.query(`ALTER TABLE team_members ADD COLUMN IF NOT EXISTS company_id      INTEGER`);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_team_members_company_id ON team_members(company_id) WHERE company_id IS NOT NULL`);
    await pool.query(`UPDATE team_members SET invited_at = created_at WHERE invited_at IS NULL`);

    // Loose-stone assignments. We keep this in a separate table because
    // soap_stones is synced from an external source — adding columns there
    // risks getting wiped on the next sync. Stones with no row here are
    // "unassigned" and visible to every rep so they can claim them.
    await pool.query(`
      CREATE TABLE IF NOT EXISTS stone_assignments (
        id              SERIAL PRIMARY KEY,
        team_owner_id   TEXT NOT NULL,
        stone_sku       TEXT NOT NULL,
        assigned_to     TEXT,
        assigned_by     TEXT,
        notes           TEXT,
        created_at      TIMESTAMP DEFAULT NOW(),
        updated_at      TIMESTAMP DEFAULT NOW(),
        UNIQUE (team_owner_id, stone_sku)
      )
    `);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_stone_assignments_assigned  ON stone_assignments(assigned_to) WHERE assigned_to IS NOT NULL`);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_stone_assignments_owner_sku ON stone_assignments(team_owner_id, stone_sku)`);
    // FK for folder_id (separate so it doesn't fail if column already added)
    try {
      await pool.query(`
        DO $$ BEGIN
          IF NOT EXISTS (
            SELECT 1 FROM information_schema.table_constraints
            WHERE constraint_name = 'crm_contacts_folder_fk'
          ) THEN
            ALTER TABLE crm_contacts
            ADD CONSTRAINT crm_contacts_folder_fk
            FOREIGN KEY (folder_id) REFERENCES crm_folders(id) ON DELETE SET NULL;
          END IF;
        END $$;
      `);
    } catch (e) { console.warn("FK migration warn:", e.message); }

    // One-time backfill: enrich existing DNA-lead deal_items with real inventory
    // (image, price, specs). Safe to re-run — only touches rows whose snapshot
    // is missing imageUrl. Caps at 200 rows per boot to avoid long startup blocks.
    try {
      const todo = await pool.query(`
        SELECT i.id, i.deal_id, i.sku
          FROM crm_deal_items i
          JOIN crm_deals d ON d.id = i.deal_id
         WHERE d.shared = TRUE
           AND i.sku IS NOT NULL
           AND COALESCE(i.snapshot->>'imageUrl', '') = ''
         LIMIT 200
      `);
      for (const row of todo.rows) {
        try {
          const stoneRes = await pool.query(
            `SELECT sku, category, shape, weight, color, clarity, lab, origin,
                    measurements, image, additional_pictures,
                    certificate_number, certificate_image, comment,
                    price_per_carat, total_price
               FROM soap_stones WHERE sku = $1 LIMIT 1`,
            [row.sku]
          );
          let snap = null;
          let bruto = 0;
          let category = null;
          if (stoneRes.rows.length) {
            const s = stoneRes.rows[0];
            let img = s.image;
            if (!img && s.additional_pictures) {
              img = String(s.additional_pictures).split(';')[0]?.trim() || null;
            }
            bruto = Number(s.total_price) || 0;
            category = s.category;
            snap = {
              sku: s.sku, category: s.category, shape: s.shape,
              weightCt: s.weight ? Number(s.weight) : null,
              color: s.color, clarity: s.clarity, lab: s.lab, origin: s.origin,
              measurements: s.measurements,
              certificateNumber: s.certificate_number,
              certificateUrl: s.certificate_image,
              treatment: s.comment, imageUrl: img,
              pricePerCarat: Number(s.price_per_carat) || null,
              priceTotal: bruto || null,
            };
          } else {
            const jewRes = await pool.query(
              `SELECT model_number, jewelry_type, style, collection, metal_type,
                      total_carat, jewelry_weight, stone_type,
                      all_pictures_link, video_link, price
                 FROM jewelry_products WHERE model_number = $1 LIMIT 1`,
              [row.sku]
            );
            if (jewRes.rows.length) {
              const j = jewRes.rows[0];
              const img = j.all_pictures_link
                ? String(j.all_pictures_link).split(';').map((x) => x.trim()).filter(Boolean)[0] || null
                : null;
              bruto = Number(j.price) || 0;
              category = 'Jewelry';
              snap = {
                sku: j.model_number, category: 'Jewelry',
                jewelryType: j.jewelry_type, style: j.style,
                collection: j.collection, metalType: j.metal_type,
                totalCarat: j.total_carat ? Number(j.total_carat) : null,
                weightG: j.jewelry_weight ? Number(j.jewelry_weight) : null,
                stoneType: j.stone_type, imageUrl: img,
                video: j.video_link,
                priceTotal: bruto || null,
              };
            }
          }
          if (snap) {
            const neto = bruto ? Math.round(bruto / 2) : null;
            await pool.query(
              `UPDATE crm_deal_items
                  SET snapshot = $2::jsonb,
                      category = COALESCE(category, $3),
                      custom_price = COALESCE(custom_price, $4)
                WHERE id = $1`,
              [row.id, JSON.stringify(snap), category, neto]
            );
            if (neto != null) {
              await pool.query(
                `UPDATE crm_deals SET value = $2, updated_at = NOW()
                  WHERE id = $1 AND COALESCE(value, 0) = 0`,
                [row.deal_id, neto]
              );
            }
          }
        } catch (rowErr) {
          console.warn(`DNA backfill row ${row.id} (${row.sku}) failed:`, rowErr.message);
        }
      }
      if (todo.rows.length) console.log(`DNA backfill: processed ${todo.rows.length} item(s)`);
    } catch (e) {
      console.warn("DNA backfill warn:", e.message);
    }

    /* ────────────────────────────────────────────────────────────────
       Sprint 4 — Companies (retail stores) + Memo (consignment) system.

       Goal: track jewelry/stones we send out on consignment to retail
       stores. A "company" is a store entity that can have many CRM
       contacts inside it (owner, manager, salespeople). A "memo" is a
       dated bundle of items sent to a company; each item has its own
       lifecycle (out → returned / sold) so a memo can be partially
       closed without affecting the rest.

       Design notes:
         - Pricing: only memo_price is recorded per item. The store
           never sees Bruto/Neto/cost — they only see what we'd charge
           them if the item is sold from the memo.
         - Inventory awareness: items currently on an active memo
           (status='out') should NOT appear as "available" in the rep's
           inventory. We expose this via a small index used by the FE.
         - Polymorphic items: an item is either a loose stone (matched
           by sku from soap_stones) or a jewelry piece (matched by
           model_number/sku from jewelry_items). We store snapshot JSON
           so the memo doesn't break if inventory data changes later.
       ─────────────────────────────────────────────────────────────── */
    await pool.query(`
      CREATE TABLE IF NOT EXISTS crm_companies (
        id                SERIAL PRIMARY KEY,
        user_id           TEXT NOT NULL,
        name              TEXT NOT NULL,
        type              TEXT NOT NULL DEFAULT 'retail_store',
        primary_contact   TEXT,
        email             TEXT,
        phone             TEXT,
        website           TEXT,
        country           TEXT,
        city              TEXT,
        address           TEXT,
        tax_id            TEXT,
        notes             TEXT,
        tags              JSONB DEFAULT '[]',
        default_memo_days INTEGER DEFAULT 30,
        payment_terms     TEXT,
        credit_limit      NUMERIC(14,2),
        status            TEXT DEFAULT 'active',
        assigned_to       TEXT,
        shared            BOOLEAN DEFAULT FALSE,
        logo_url          TEXT,
        created_at        TIMESTAMP DEFAULT NOW(),
        updated_at        TIMESTAMP DEFAULT NOW()
      )
    `);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_crm_companies_user        ON crm_companies(user_id)`);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_crm_companies_type        ON crm_companies(type)`);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_crm_companies_assigned_to ON crm_companies(assigned_to) WHERE assigned_to IS NOT NULL`);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_crm_companies_email_lower ON crm_companies(LOWER(email))`);

    // Optional richer fields for the dedicated Store Profile page. Each
    // is added defensively so older databases roll forward without
    // touching existing rows. The page treats every one as optional.
    const storeCols = [
      "ALTER TABLE crm_companies ADD COLUMN IF NOT EXISTS description    TEXT",
      "ALTER TABLE crm_companies ADD COLUMN IF NOT EXISTS instagram      TEXT",
      "ALTER TABLE crm_companies ADD COLUMN IF NOT EXISTS facebook       TEXT",
      "ALTER TABLE crm_companies ADD COLUMN IF NOT EXISTS linkedin       TEXT",
      "ALTER TABLE crm_companies ADD COLUMN IF NOT EXISTS whatsapp       TEXT",
      "ALTER TABLE crm_companies ADD COLUMN IF NOT EXISTS business_hours JSONB DEFAULT '{}'",
      "ALTER TABLE crm_companies ADD COLUMN IF NOT EXISTS cover_image_url TEXT",
      "ALTER TABLE crm_companies ADD COLUMN IF NOT EXISTS currency       TEXT DEFAULT 'USD'",
      "ALTER TABLE crm_companies ADD COLUMN IF NOT EXISTS established_year INTEGER",
    ];
    for (const sql of storeCols) {
      try { await pool.query(sql); } catch (e) { console.warn("Store-col migration warn:", e.message); }
    }

    // Link contacts and deals to a company (idempotent — column may
    // already exist from a previous boot). FK is added separately so
    // adding the column never depends on the FK succeeding.
    try { await pool.query(`ALTER TABLE crm_contacts ADD COLUMN IF NOT EXISTS company_id INTEGER`); } catch (_) {}
    try { await pool.query(`ALTER TABLE crm_deals    ADD COLUMN IF NOT EXISTS company_id INTEGER`); } catch (_) {}
    try {
      await pool.query(`
        DO $$ BEGIN
          IF NOT EXISTS (
            SELECT 1 FROM information_schema.table_constraints
            WHERE constraint_name = 'crm_contacts_company_fk'
          ) THEN
            ALTER TABLE crm_contacts
            ADD CONSTRAINT crm_contacts_company_fk
            FOREIGN KEY (company_id) REFERENCES crm_companies(id) ON DELETE SET NULL;
          END IF;
          IF NOT EXISTS (
            SELECT 1 FROM information_schema.table_constraints
            WHERE constraint_name = 'crm_deals_company_fk'
          ) THEN
            ALTER TABLE crm_deals
            ADD CONSTRAINT crm_deals_company_fk
            FOREIGN KEY (company_id) REFERENCES crm_companies(id) ON DELETE SET NULL;
          END IF;
        END $$;
      `);
    } catch (e) { console.warn("Company FK migration warn:", e.message); }
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_crm_contacts_company_id ON crm_contacts(company_id) WHERE company_id IS NOT NULL`);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_crm_deals_company_id    ON crm_deals(company_id)    WHERE company_id IS NOT NULL`);

    // Memo header.
    await pool.query(`
      CREATE TABLE IF NOT EXISTS memos (
        id              SERIAL PRIMARY KEY,
        user_id         TEXT NOT NULL,
        memo_number     TEXT NOT NULL,
        company_id      INTEGER NOT NULL REFERENCES crm_companies(id) ON DELETE CASCADE,
        contact_id      INTEGER REFERENCES crm_contacts(id) ON DELETE SET NULL,
        status          TEXT NOT NULL DEFAULT 'draft',
        issued_at       TIMESTAMP,
        due_at          DATE,
        closed_at       TIMESTAMP,
        total_value     NUMERIC(14,2) DEFAULT 0,
        currency        TEXT DEFAULT 'USD',
        notes           TEXT,
        internal_notes  TEXT,
        created_by      TEXT,
        assigned_to     TEXT,
        created_at      TIMESTAMP DEFAULT NOW(),
        updated_at      TIMESTAMP DEFAULT NOW()
      )
    `);
    await pool.query(`CREATE UNIQUE INDEX IF NOT EXISTS idx_memos_user_number    ON memos(user_id, memo_number)`);
    await pool.query(`CREATE INDEX IF NOT EXISTS        idx_memos_company        ON memos(company_id)`);
    await pool.query(`CREATE INDEX IF NOT EXISTS        idx_memos_status         ON memos(status)`);
    await pool.query(`CREATE INDEX IF NOT EXISTS        idx_memos_user_status    ON memos(user_id, status)`);
    await pool.query(`CREATE INDEX IF NOT EXISTS        idx_memos_assigned_to    ON memos(assigned_to) WHERE assigned_to IS NOT NULL`);
    await pool.query(`CREATE INDEX IF NOT EXISTS        idx_memos_due_at_open    ON memos(due_at) WHERE status IN ('out','partially_returned')`);

    // Memo line items. Polymorphic: item_type is 'stone' or 'jewelry'.
    await pool.query(`
      CREATE TABLE IF NOT EXISTS memo_items (
        id            SERIAL PRIMARY KEY,
        memo_id       INTEGER NOT NULL REFERENCES memos(id) ON DELETE CASCADE,
        item_type     TEXT NOT NULL,
        item_sku      TEXT NOT NULL,
        item_id       TEXT,
        snapshot      JSONB DEFAULT '{}',
        memo_price    NUMERIC(14,2),
        quantity      INTEGER DEFAULT 1,
        status        TEXT NOT NULL DEFAULT 'out',
        returned_at   TIMESTAMP,
        sold_at       TIMESTAMP,
        notes         TEXT,
        created_at    TIMESTAMP DEFAULT NOW(),
        updated_at    TIMESTAMP DEFAULT NOW()
      )
    `);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_memo_items_memo        ON memo_items(memo_id)`);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_memo_items_sku         ON memo_items(item_sku)`);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_memo_items_active      ON memo_items(item_sku, status) WHERE status = 'out'`);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_memo_items_status      ON memo_items(status)`);

    // Approval workflow — when a store user (role='store_user') flags
    // an item as sold or returned, we don't apply it immediately.
    // Instead pending_status holds 'sold' or 'returned' and shows up
    // on the owner's MemoDetail with Approve / Decline buttons.
    await pool.query(`ALTER TABLE memo_items ADD COLUMN IF NOT EXISTS pending_status TEXT`);
    await pool.query(`ALTER TABLE memo_items ADD COLUMN IF NOT EXISTS pending_at     TIMESTAMP`);
    await pool.query(`ALTER TABLE memo_items ADD COLUMN IF NOT EXISTS pending_by     TEXT`);
    await pool.query(`ALTER TABLE memo_items ADD COLUMN IF NOT EXISTS pending_note   TEXT`);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_memo_items_pending ON memo_items(pending_status) WHERE pending_status IS NOT NULL`);

    // Memo requests — store users browse the available catalog and
    // submit a wishlist of items + an optional free-text message.
    // The supplier reviews these from their inbox and either converts
    // the request into a real memo (one click) or declines it.
    await pool.query(`
      CREATE TABLE IF NOT EXISTS memo_requests (
        id                SERIAL PRIMARY KEY,
        user_id           TEXT NOT NULL,
        company_id        INTEGER NOT NULL,
        requested_by      TEXT NOT NULL,
        status            TEXT NOT NULL DEFAULT 'pending',
        message           TEXT,
        preferred_due_at  TIMESTAMP,
        converted_memo_id INTEGER,
        decline_reason    TEXT,
        responded_at      TIMESTAMP,
        responded_by      TEXT,
        created_at        TIMESTAMP DEFAULT NOW(),
        updated_at        TIMESTAMP DEFAULT NOW()
      )
    `);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_memo_requests_tenant   ON memo_requests(user_id, status)`);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_memo_requests_company  ON memo_requests(company_id)`);

    await pool.query(`
      CREATE TABLE IF NOT EXISTS memo_request_items (
        id          SERIAL PRIMARY KEY,
        request_id  INTEGER NOT NULL REFERENCES memo_requests(id) ON DELETE CASCADE,
        item_type   TEXT NOT NULL,
        item_sku    TEXT,
        item_id     TEXT,
        snapshot    JSONB DEFAULT '{}',
        notes       TEXT,
        created_at  TIMESTAMP DEFAULT NOW()
      )
    `);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_memo_request_items_req ON memo_request_items(request_id)`);

    /* =========================================================
       Memo signatures (Sprint: digital signature workflow)
       ---------------------------------------------------------
       Two-way electronic signature on the memo lifecycle.
       Supplier signs on `event='issue'` (hard-gated — required
       before status flips draft→out). Store signs on `event='issue'`
       to acknowledge receipt and on `event='close'` to acknowledge
       return. A UNIQUE(memo_id, event, signer_role) ensures one
       signature per (event, role) combo — i.e. exactly one supplier
       issue-signature and one store issue-signature per memo.

       `memo_snapshot` freezes the memo + its items at signing time
       so we can prove exactly what was signed even if the memo is
       later amended. `integrity_hash` is SHA-256 over the snapshot
       + signature URL + signer + timestamp — any tamper attempt is
       detectable by recomputing and comparing.
       ========================================================= */
    await pool.query(`
      CREATE TABLE IF NOT EXISTS memo_signatures (
        id              SERIAL PRIMARY KEY,
        memo_id         INTEGER NOT NULL REFERENCES memos(id) ON DELETE CASCADE,
        user_id         TEXT NOT NULL,
        event           TEXT NOT NULL,
        signer_role     TEXT NOT NULL,
        signer_clerk_id TEXT,
        signer_name     TEXT NOT NULL,
        signer_email    TEXT,
        signature_url   TEXT NOT NULL,
        consent_text    TEXT NOT NULL,
        memo_snapshot   JSONB NOT NULL,
        pdf_url         TEXT,
        integrity_hash  TEXT NOT NULL,
        ip_address      TEXT,
        user_agent      TEXT,
        token_id        INTEGER,
        signed_at       TIMESTAMP DEFAULT NOW()
      )
    `);
    await pool.query(`CREATE UNIQUE INDEX IF NOT EXISTS idx_memo_sig_unique  ON memo_signatures(memo_id, event, signer_role)`);
    await pool.query(`CREATE INDEX        IF NOT EXISTS idx_memo_sig_memo    ON memo_signatures(memo_id)`);
    await pool.query(`CREATE INDEX        IF NOT EXISTS idx_memo_sig_tenant  ON memo_signatures(user_id)`);

    /* =========================================================
       Memo signature tokens (Sprint: digital signature workflow, ph.2)
       ---------------------------------------------------------
       Opaque, single-use URLs that the supplier can hand to a
       counterparty over WhatsApp / email when they don't have a
       portal account. The token authorises one signature on one
       memo for one (event, signer_role) pair. After it is redeemed,
       used_at is stamped and the link becomes inert. Expired or
       used tokens return a friendly "no longer available" message
       to keep the flow safe even when links are forwarded around.
       ========================================================= */
    await pool.query(`
      CREATE TABLE IF NOT EXISTS memo_signature_tokens (
        id            SERIAL PRIMARY KEY,
        memo_id       INTEGER NOT NULL REFERENCES memos(id) ON DELETE CASCADE,
        user_id       TEXT NOT NULL,
        token         TEXT NOT NULL UNIQUE,
        event         TEXT NOT NULL,
        signer_role   TEXT NOT NULL,
        signer_email  TEXT,
        expires_at    TIMESTAMP NOT NULL,
        used_at       TIMESTAMP,
        created_at    TIMESTAMP DEFAULT NOW(),
        created_by    TEXT
      )
    `);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_memo_sig_tok_memo ON memo_signature_tokens(memo_id)`);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_memo_sig_tok_open ON memo_signature_tokens(token) WHERE used_at IS NULL`);

    /* =========================================================
       Catalog Tiers — per-supplier curated buckets of inventory
       that decide which items each store sees in the portal.

       Model:
       - catalog_tiers           : a named collection ("Public",
                                   "VIP Bridal", "Israel Exclusive"…)
                                   owned by one supplier (user_id).
       - catalog_tier_items      : SKUs (stones or jewelry) inside
                                   the tier. An item can live in many
                                   tiers, or in zero tiers (which means
                                   no store will ever see it — that is
                                   the deliberate default for new items).
       - catalog_tier_companies  : which stores see the tier. A store
                                   sees the union of items across the
                                   tiers it's subscribed to.
       ========================================================= */
    await pool.query(`
      CREATE TABLE IF NOT EXISTS catalog_tiers (
        id            SERIAL PRIMARY KEY,
        user_id       TEXT NOT NULL,
        name          TEXT NOT NULL,
        description   TEXT,
        color         TEXT,
        sort_order    INTEGER DEFAULT 0,
        is_default    BOOLEAN DEFAULT FALSE,
        created_at    TIMESTAMP DEFAULT NOW(),
        updated_at    TIMESTAMP DEFAULT NOW(),
        UNIQUE (user_id, name)
      )
    `);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_catalog_tiers_user ON catalog_tiers(user_id)`);

    await pool.query(`
      CREATE TABLE IF NOT EXISTS catalog_tier_items (
        tier_id     INTEGER NOT NULL REFERENCES catalog_tiers(id) ON DELETE CASCADE,
        item_type   TEXT NOT NULL,
        item_sku    TEXT NOT NULL,
        added_at    TIMESTAMP DEFAULT NOW(),
        added_by    TEXT,
        PRIMARY KEY (tier_id, item_type, item_sku)
      )
    `);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_cti_item ON catalog_tier_items(item_type, item_sku)`);

    await pool.query(`
      CREATE TABLE IF NOT EXISTS catalog_tier_companies (
        tier_id     INTEGER NOT NULL REFERENCES catalog_tiers(id) ON DELETE CASCADE,
        company_id  INTEGER NOT NULL,
        added_at    TIMESTAMP DEFAULT NOW(),
        PRIMARY KEY (tier_id, company_id)
      )
    `);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_ctc_company ON catalog_tier_companies(company_id)`);

    /* One-time seed.
       Until today every store-portal user could see every active
       stone and every active jewelry item belonging to the supplier.
       To preserve that behaviour the FIRST time the new tables exist,
       for each supplier that has at least one store user we:
         1. Create a "Public" tier (is_default = TRUE).
         2. Add every active SKU we currently expose in the catalog.
         3. Subscribe every CRM company that owns a store_user.
       Subsequent server boots are a no-op because we check whether
       the supplier already has any tier. */
    try {
      const suppliersWithStoreUsers = await pool.query(`
        SELECT DISTINCT tm.team_owner_id AS user_id
          FROM team_members tm
         WHERE tm.role = 'store_user'
           AND tm.active = TRUE
      `);
      for (const sup of suppliersWithStoreUsers.rows) {
        const uid = sup.user_id;
        const existing = await pool.query(`SELECT 1 FROM catalog_tiers WHERE user_id = $1 LIMIT 1`, [uid]);
        if (existing.rows.length) continue;

        const inserted = await pool.query(
          `INSERT INTO catalog_tiers (user_id, name, description, color, is_default, sort_order)
           VALUES ($1, 'Public', 'Default catalog visible to all linked stores', '#0ea5e9', TRUE, 0)
           RETURNING id`,
          [uid]
        );
        const tierId = inserted.rows[0].id;

        // Seed stones — every soap_stone with a SKU.
        await pool.query(
          `INSERT INTO catalog_tier_items (tier_id, item_type, item_sku, added_by)
             SELECT $1, 'stone', sku, 'system-seed'
               FROM soap_stones
              WHERE sku IS NOT NULL
           ON CONFLICT DO NOTHING`,
          [tierId]
        );

        // Seed jewelry — active, not archived/draft, not sold.
        await pool.query(
          `INSERT INTO catalog_tier_items (tier_id, item_type, item_sku, added_by)
             SELECT $1, 'jewelry', sku, 'system-seed'
               FROM jewelry_items
              WHERE user_id = $2
                AND sku IS NOT NULL
                AND COALESCE(status,'') NOT IN ('archived','draft')
                AND sold_at IS NULL
           ON CONFLICT DO NOTHING`,
          [tierId, uid]
        );

        // Subscribe every company that already has a store_user.
        await pool.query(
          `INSERT INTO catalog_tier_companies (tier_id, company_id)
             SELECT DISTINCT $1, tm.company_id
               FROM team_members tm
              WHERE tm.team_owner_id = $2
                AND tm.role = 'store_user'
                AND tm.active = TRUE
                AND tm.company_id IS NOT NULL
           ON CONFLICT DO NOTHING`,
          [tierId, uid]
        );

        console.log(`Catalog tiers seeded for supplier ${uid} → "Public"`);
      }
    } catch (seedErr) {
      console.error("⚠️ Catalog tier seed skipped:", seedErr.message);
    }

    crmReady = true;
    console.log("CRM tables ready (incl. companies + memos + memo_requests + catalog_tiers)");
  } catch (err) {
    console.error("❌ CRM table creation error:", err);
  }
})();

const ensureCrm = async (req, res, next) => {
  if (!crmReady) {
    try { await crmReadyPromise; } catch (_) {}
  }
  if (!crmReady) return res.status(503).json({ error: "CRM tables not ready" });
  next();
};
app.use("/api/crm", ensureCrm);

/* ---------- Contacts CRUD ---------- */

app.get("/api/crm/contacts", async (req, res) => {
  try {
    const {
      search, type, status, folderId, country, city, company,
      hasEmail, hasPhone, hasWebsite, lastContactDays, createdSince, createdUntil, tag,
      assignedTo, // 'me' | 'unassigned' | <clerk_user_id>
    } = req.query;
    const ctx = await resolveTeamContext(req);
    if (!ctx.actorUserId) return res.status(400).json({ error: "userId is required" });
    const tenantUserId = ctx.tenantUserId;

    // Broadcast: each user sees their own contacts AND any contact flagged as shared
    // (currently used for DNA-lead inquiries that arrive from the public DNA page).
    const conditions = ["(user_id = $1 OR shared = TRUE)"];
    const values = [tenantUserId];
    let idx = 2;

    // Sales-rep visibility: reps see ONLY records explicitly assigned to
    // them. Unassigned rows stay private to the workspace owner — owner
    // hands them over by setting `assigned_to`. Owner always sees the
    // whole workspace.
    if (!ctx.isOwner) {
      conditions.push(`assigned_to = $${idx}`);
      values.push(ctx.actorUserId);
      idx++;
    }
    // Optional explicit "Mine / Unassigned / <rep>" filter from the UI chip.
    if (assignedTo === 'me') {
      conditions.push(`assigned_to = $${idx}`);
      values.push(ctx.actorUserId);
      idx++;
    } else if (assignedTo === 'unassigned') {
      conditions.push(`assigned_to IS NULL`);
    } else if (assignedTo && assignedTo !== 'all') {
      conditions.push(`assigned_to = $${idx}`);
      values.push(assignedTo);
      idx++;
    }

    if (search) {
      conditions.push(`(name ILIKE $${idx} OR company ILIKE $${idx} OR phone ILIKE $${idx} OR email ILIKE $${idx} OR title ILIKE $${idx})`);
      values.push(`%${search}%`);
      idx++;
    }
    if (type && type !== 'all') {
      conditions.push(`type = $${idx++}`);
      values.push(type);
    }
    if (status && status !== 'all') {
      conditions.push(`status = $${idx++}`);
      values.push(status);
    }
    if (folderId) {
      if (folderId === 'unfiled') {
        conditions.push(`folder_id IS NULL`);
      } else {
        // Include sub-folders recursively
        conditions.push(`folder_id IN (
          WITH RECURSIVE descendants AS (
            SELECT id FROM crm_folders WHERE id = $${idx} AND user_id = $1
            UNION ALL
            SELECT f.id FROM crm_folders f INNER JOIN descendants d ON f.parent_id = d.id
          )
          SELECT id FROM descendants
        )`);
        values.push(parseInt(folderId, 10));
        idx++;
      }
    }
    if (country) { conditions.push(`country ILIKE $${idx++}`); values.push(country); }
    if (city) { conditions.push(`city ILIKE $${idx++}`); values.push(city); }
    if (company) { conditions.push(`company ILIKE $${idx++}`); values.push(`%${company}%`); }
    if (hasEmail === 'true') conditions.push(`email IS NOT NULL AND email <> ''`);
    if (hasEmail === 'false') conditions.push(`(email IS NULL OR email = '')`);
    if (hasPhone === 'true') conditions.push(`phone IS NOT NULL AND phone <> ''`);
    if (hasPhone === 'false') conditions.push(`(phone IS NULL OR phone = '')`);
    if (hasWebsite === 'true') conditions.push(`website IS NOT NULL AND website <> ''`);
    if (hasWebsite === 'false') conditions.push(`(website IS NULL OR website = '')`);
    if (lastContactDays) {
      const d = parseInt(lastContactDays, 10);
      if (!Number.isNaN(d)) {
        conditions.push(`(last_contact_at IS NULL OR last_contact_at < NOW() - INTERVAL '${d} days')`);
      }
    }
    if (createdSince) { conditions.push(`created_at >= $${idx++}`); values.push(createdSince); }
    if (createdUntil) { conditions.push(`created_at <= $${idx++}`); values.push(createdUntil); }
    if (tag) {
      conditions.push(`tags @> $${idx++}::jsonb`);
      values.push(JSON.stringify([tag]));
    }

    let result;
    try {
      // Lean payload: only fields the list/cards/filters need.
      // Heavy fields (notes, address, preferences, linked_contact_ids, card images, dates other than updated_at)
      // are fetched on demand from /api/crm/contacts/:id.
      // Single LEFT JOIN with one aggregate query replaces 2 per-row correlated subqueries
      // (was O(N×M); now ~O(N+M)).
      // The list endpoint NEVER returns card_image_thumb (heavy base64 ~5-50KB each).
      // The FE fetches thumbnails in a separate, batched, background request via /api/crm/contacts/thumbs.
      result = await pool.query(
        `SELECT
            c.id, c.user_id, c.name, c.type, c.title, c.company,
            c.phone, c.email, c.website,
            c.country, c.city,
            c.source, c.status, c.tags,
            c.folder_id,
            c.dna_sku, c.shared,
            c.assigned_to,
            (c.card_image_front IS NOT NULL) AS has_card_front,
            (c.card_image_back IS NOT NULL) AS has_card_back,
            (c.card_image_thumb IS NOT NULL) AS has_card_thumb,
            c.last_contact_at, c.updated_at,
            f.name AS folder_name,
            COALESCE(da.deals_count, 0) AS deals_count,
            COALESCE(da.total_won, 0) AS total_won
         FROM crm_contacts c
         LEFT JOIN crm_folders f ON f.id = c.folder_id
         LEFT JOIN (
           SELECT contact_id,
                  COUNT(*)::int AS deals_count,
                  COALESCE(SUM(CASE WHEN stage = 'won' THEN value ELSE 0 END), 0) AS total_won
             FROM crm_deals
            GROUP BY contact_id
         ) da ON da.contact_id = c.id
         WHERE ${conditions.join(" AND ")}
         ORDER BY c.updated_at DESC
         LIMIT 2000`,
        values
      );
    } catch (joinErr) {
      // Self-heal: maybe the new columns/tables aren't present yet. Re-run migrations and fall back.
      console.warn("Contacts JOIN failed, attempting self-heal:", joinErr.message);
      try {
        await pool.query(`CREATE TABLE IF NOT EXISTS crm_folders (
          id SERIAL PRIMARY KEY,
          user_id TEXT NOT NULL,
          name TEXT NOT NULL,
          parent_id INTEGER REFERENCES crm_folders(id) ON DELETE CASCADE,
          color TEXT,
          created_at TIMESTAMP DEFAULT NOW(),
          updated_at TIMESTAMP DEFAULT NOW()
        )`);
        await pool.query(`ALTER TABLE crm_contacts ADD COLUMN IF NOT EXISTS title TEXT`);
        await pool.query(`ALTER TABLE crm_contacts ADD COLUMN IF NOT EXISTS website TEXT`);
        await pool.query(`ALTER TABLE crm_contacts ADD COLUMN IF NOT EXISTS phone_alt TEXT`);
        await pool.query(`ALTER TABLE crm_contacts ADD COLUMN IF NOT EXISTS folder_id INTEGER`);
        await pool.query(`ALTER TABLE crm_contacts ADD COLUMN IF NOT EXISTS linked_contact_ids JSONB DEFAULT '[]'`);
        await pool.query(`ALTER TABLE crm_contacts ADD COLUMN IF NOT EXISTS card_back_notes TEXT`);
        await pool.query(`ALTER TABLE crm_contacts ADD COLUMN IF NOT EXISTS card_image_front TEXT`);
        await pool.query(`ALTER TABLE crm_contacts ADD COLUMN IF NOT EXISTS card_image_back TEXT`);
        await pool.query(`ALTER TABLE crm_contacts ADD COLUMN IF NOT EXISTS card_image_thumb TEXT`);
      } catch (healErr) {
        console.error("Self-heal failed:", healErr.message);
      }
      // Fallback query without the folder join (still safe even if folder_id is missing).
      // Uses c.* so it works regardless of which optional columns exist.
      result = await pool.query(
        `SELECT c.*,
          NULL::text AS folder_name,
          COALESCE(da.deals_count, 0) AS deals_count,
          COALESCE(da.total_won, 0) AS total_won
         FROM crm_contacts c
         LEFT JOIN (
           SELECT contact_id,
                  COUNT(*)::int AS deals_count,
                  COALESCE(SUM(CASE WHEN stage = 'won' THEN value ELSE 0 END), 0) AS total_won
             FROM crm_deals
            GROUP BY contact_id
         ) da ON da.contact_id = c.id
         WHERE ${conditions.join(" AND ")}
         ORDER BY c.updated_at DESC
         LIMIT 2000`,
        values
      );
    }
    res.json(result.rows);
  } catch (error) {
    console.error("Error fetching contacts:", error.stack || error.message || error);
    res.status(500).json({ error: error.message || "Internal server error" });
  }
});

app.get("/api/crm/contacts/:id", async (req, res) => {
  try {
    const { id } = req.params;
    const ctx = await resolveTeamContext(req);
    if (!ctx.actorUserId) return res.status(400).json({ error: "userId is required" });
    // Filter by the WORKSPACE owner's id (tenantUserId), not the caller —
    // a rep's own clerk_user_id never appears on crm_contacts.user_id, so
    // the previous query always returned 404 for reps even when the row
    // they clicked from the list belonged to their workspace.
    const tenantUserId = ctx.tenantUserId;

    const contact = await pool.query(
      "SELECT * FROM crm_contacts WHERE id = $1 AND (user_id = $2 OR shared = TRUE)",
      [id, tenantUserId]
    );
    if (contact.rows.length === 0) return res.status(404).json({ error: "Contact not found" });
    if (!canReadAssignment(ctx, contact.rows[0].assigned_to)) {
      // Same 404 we'd return for a non-existent contact — never reveal
      // that the row exists but belongs to a different rep.
      return res.status(404).json({ error: "Contact not found" });
    }

    const interactions = await pool.query(
      "SELECT * FROM crm_interactions WHERE contact_id = $1 ORDER BY occurred_at DESC LIMIT 100",
      [id]
    );
    const deals = await pool.query(
      "SELECT * FROM crm_deals WHERE contact_id = $1 ORDER BY updated_at DESC",
      [id]
    );
    const tasks = await pool.query(
      "SELECT * FROM crm_tasks WHERE contact_id = $1 ORDER BY due_date ASC NULLS LAST",
      [id]
    );

    res.json({
      ...contact.rows[0],
      interactions: interactions.rows,
      deals: deals.rows,
      tasks: tasks.rows,
    });
  } catch (error) {
    console.error("Error fetching contact:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

app.post("/api/crm/contacts", async (req, res) => {
  try {
    const {
      name, type, title, company, phone, phoneAlt, email, website,
      country, city, address, source, status, tags, preferences, notes, avatarUrl,
      folderId, linkedContactIds, cardBackNotes,
      cardImageFront, cardImageBack, cardImageThumb,
      assignedTo, // optional; defaults to the actor (auto-assign on create)
      companyId,  // optional: link this contact to a CRM company / store
    } = req.body;
    const ctx = await resolveTeamContext(req);
    if (!ctx.actorUserId) return res.status(400).json({ error: "userId is required" });
    if (!name) return res.status(400).json({ error: "name is required" });
    const tenantUserId = ctx.tenantUserId;
    // Reps can only assign to themselves; admin can assign to anyone.
    const finalAssignee = ctx.isOwner
      ? (assignedTo === undefined ? ctx.actorUserId : assignedTo || null)
      : ctx.actorUserId;

    const result = await pool.query(
      `INSERT INTO crm_contacts (
         user_id, name, type, title, company, phone, phone_alt, email, website,
         country, city, address, source, status, tags, preferences, notes, avatar_url,
         folder_id, linked_contact_ids, card_back_notes,
         card_image_front, card_image_back, card_image_thumb, assigned_to, company_id
       )
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26)
       RETURNING id, user_id, name, type, title, company, company_id, phone, phone_alt, email, website,
                 country, city, address, source, status, tags, preferences, notes, avatar_url,
                 folder_id, linked_contact_ids, card_back_notes, card_image_thumb,
                 assigned_to,
                 (card_image_front IS NOT NULL) AS has_card_front,
                 (card_image_back IS NOT NULL) AS has_card_back,
                 last_contact_at, created_at, updated_at`,
      [
        tenantUserId, name.trim(), type || 'lead', title || null, company || null,
        phone || null, phoneAlt || null, email || null, website || null,
        country || null, city || null, address || null, source || null, status || 'active',
        JSON.stringify(tags || []), JSON.stringify(preferences || {}),
        notes || null, avatarUrl || null,
        folderId || null, JSON.stringify(linkedContactIds || []), cardBackNotes || null,
        cardImageFront || null, cardImageBack || null, cardImageThumb || null,
        finalAssignee,
        companyId != null && companyId !== '' ? Number(companyId) : null,
      ]
    );
    res.status(201).json(result.rows[0]);

    logActivity({
      userId:    tenantUserId,
      actorId:   ctx.actorUserId,
      actorName: ctx.actorName,
      entityType: 'contact',
      entityId:   result.rows[0].id,
      action:     'created',
      summary:    `Created contact ${result.rows[0].name}`,
      related: result.rows[0].folder_id
        ? [{ type: 'folder', id: result.rows[0].folder_id }]
        : null,
    });
  } catch (error) {
    console.error("Error creating contact:", error);
    res.status(500).json({ error: error.message || "Internal server error" });
  }
});

app.put("/api/crm/contacts/:id", async (req, res) => {
  try {
    const { id } = req.params;
    const ctx = await resolveTeamContext(req);
    const allowed = [
      'name','type','title','company','phone','phone_alt','email','website',
      'country','city','address','source','status','tags','preferences','notes','avatar_url','last_contact_at',
      'folder_id','linked_contact_ids','card_back_notes',
      'card_image_front','card_image_back','card_image_thumb',
      'assigned_to','company_id',
    ];
    const fields = [];
    const values = [];
    let idx = 1;

    for (const key of allowed) {
      const camel = key.replace(/_([a-z])/g, (_,c) => c.toUpperCase());
      if (req.body[camel] !== undefined) {
        // Reps can only assign records to themselves; admin can re-assign freely.
        if (key === 'assigned_to' && ctx.actorUserId && !ctx.isOwner) {
          if (req.body[camel] && String(req.body[camel]) !== String(ctx.actorUserId)) {
            return res.status(403).json({ error: 'Reps can only assign records to themselves' });
          }
        }
        if (key === 'tags' || key === 'preferences' || key === 'linked_contact_ids') {
          fields.push(`${key} = $${idx++}`);
          values.push(JSON.stringify(req.body[camel]));
        } else {
          fields.push(`${key} = $${idx++}`);
          values.push(req.body[camel]);
        }
      }
    }
    fields.push(`updated_at = NOW()`);
    if (fields.length === 1) return res.status(400).json({ error: "No fields to update" });

    // Snapshot the row before mutating so we can compute a `changes` diff.
    const beforeRes = await pool.query("SELECT * FROM crm_contacts WHERE id = $1", [id]);
    const before = beforeRes.rows[0] || null;

    values.push(id);
    const result = await pool.query(
      `UPDATE crm_contacts SET ${fields.join(", ")} WHERE id = $${idx} RETURNING *`,
      values
    );
    if (result.rows.length === 0) return res.status(404).json({ error: "Contact not found" });
    res.json(result.rows[0]);

    const after = result.rows[0];
    const changes = diffRows(before, after, allowed);
    if (changes && before) {
      const changedKeys = Object.keys(changes);
      // Suppress activity noise for the silent thumbnail backfill flow
      // (FE regenerates a small thumb from a legacy card_image_front and
      // writes it back). Real user-driven edits still log normally.
      const isSilentThumbBackfill =
        changedKeys.length === 1 && changedKeys[0] === 'card_image_thumb';
      if (!isSilentThumbBackfill) {
        const { actorId, actorName } = getActor(req);
        const summary =
          changedKeys.length === 1
            ? `Updated ${changedKeys[0]} on ${after.name}`
            : `Updated ${after.name} (${changedKeys.length} fields)`;
        logActivity({
          userId:     after.user_id,
          actorId, actorName,
          entityType: 'contact',
          entityId:   after.id,
          action:     'updated',
          summary,
          changes,
        });
      }
    }
  } catch (error) {
    console.error("Error updating contact:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

app.delete("/api/crm/contacts/:id", async (req, res) => {
  try {
    const { id } = req.params;
    // Capture name + owner before delete so the activity row is human-readable.
    const beforeRes = await pool.query(
      "SELECT user_id, name FROM crm_contacts WHERE id = $1",
      [id]
    );
    await pool.query("DELETE FROM crm_contacts WHERE id = $1", [id]);
    res.json({ success: true });

    const before = beforeRes.rows[0];
    if (before) {
      const { actorId, actorName } = getActor(req);
      logActivity({
        userId:     before.user_id,
        actorId, actorName,
        entityType: 'contact',
        entityId:   id,
        action:     'deleted',
        summary:    `Deleted contact ${before.name}`,
      });
    }
  } catch (error) {
    console.error("Error deleting contact:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

/* ---------- Bulk operations on contacts ---------- */
app.post("/api/crm/contacts/bulk-delete", async (req, res) => {
  try {
    const { userId, ids } = req.body;
    if (!userId || !Array.isArray(ids) || ids.length === 0) {
      return res.status(400).json({ error: "userId and non-empty ids array are required" });
    }
    const result = await pool.query(
      "DELETE FROM crm_contacts WHERE (user_id = $1 OR shared = TRUE) AND id = ANY($2::int[]) RETURNING id",
      [userId, ids.map(Number)]
    );
    res.json({ success: true, deleted: result.rowCount });

    if (result.rowCount > 0) {
      const { actorId, actorName } = getActor(req);
      logActivity({
        userId,
        actorId, actorName,
        entityType: 'contact',
        entityId:   'bulk',
        action:     'bulk_deleted',
        summary:    `Bulk deleted ${result.rowCount} contact${result.rowCount === 1 ? '' : 's'}`,
        related:    result.rows.map(r => ({ type: 'contact', id: r.id })),
      });
    }
  } catch (error) {
    console.error("Bulk delete error:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

app.post("/api/crm/contacts/bulk-tag", async (req, res) => {
  // Action: 'add' | 'remove'
  try {
    const { userId, ids, tag, action } = req.body;
    if (!userId || !Array.isArray(ids) || !tag || !["add", "remove"].includes(action)) {
      return res.status(400).json({ error: "userId, ids, tag, and action(add|remove) are required" });
    }
    const safeTag = String(tag).trim();
    if (!safeTag) return res.status(400).json({ error: "Tag cannot be empty" });

    const sql =
      action === "add"
        ? `UPDATE crm_contacts
             SET tags = (
               CASE WHEN tags @> $1::jsonb THEN tags
                    ELSE tags || $1::jsonb END
             ),
             updated_at = NOW()
           WHERE (user_id = $2 OR shared = TRUE) AND id = ANY($3::int[])
           RETURNING id`
        : `UPDATE crm_contacts
             SET tags = COALESCE((SELECT jsonb_agg(elem) FROM jsonb_array_elements(tags) elem WHERE elem <> $1::jsonb), '[]'::jsonb),
             updated_at = NOW()
           WHERE (user_id = $2 OR shared = TRUE) AND id = ANY($3::int[])
           RETURNING id`;

    const result = await pool.query(sql, [JSON.stringify(safeTag), userId, ids.map(Number)]);
    res.json({ success: true, updated: result.rowCount });

    if (result.rowCount > 0) {
      const { actorId, actorName } = getActor(req);
      logActivity({
        userId,
        actorId, actorName,
        entityType: 'contact',
        entityId:   'bulk',
        action:     action === 'add' ? 'tagged' : 'untagged',
        summary:    `${action === 'add' ? 'Tagged' : 'Removed tag'} "${safeTag}" ${action === 'add' ? 'on' : 'from'} ${result.rowCount} contact${result.rowCount === 1 ? '' : 's'}`,
        changes:    { tag: safeTag, action },
        related:    result.rows.map(r => ({ type: 'contact', id: r.id })),
      });
    }
  } catch (error) {
    console.error("Bulk tag error:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

// GET /api/health — tiny wake-up endpoint used by FE to defrost cold Render instances
app.get("/api/health", (req, res) => {
  res.set("Cache-Control", "no-store");
  res.json({ ok: true, ts: Date.now() });
});

/* ---------- Lazy thumbnail loader (batched) ---------- */
// GET /api/crm/contacts/thumbs?ids=1,2,3
// Returns [{ id, thumb }] only for IDs that actually have a thumb.
// Used by the contacts list UI to render thumbnails in the background
// after the (lean) list payload has already painted.
app.get("/api/crm/contacts/thumbs", async (req, res) => {
  try {
    const ids = String(req.query.ids || "")
      .split(",")
      .map((x) => parseInt(x, 10))
      .filter((x) => Number.isFinite(x));
    if (ids.length === 0) return res.json([]);
    // Cap to keep payloads sane
    const capped = ids.slice(0, 200);
    // Falls back to card_image_front when a row never had a thumbnail
    // generated (legacy contacts created before the thumb pipeline existed).
    // The FE is expected to downscale `thumb` locally when `needs_backfill`
    // is true and POST a real thumb back, so subsequent loads are small.
    const r = await pool.query(
      `SELECT id,
              COALESCE(card_image_thumb, card_image_front) AS thumb,
              (card_image_thumb IS NULL AND card_image_front IS NOT NULL) AS needs_backfill
         FROM crm_contacts
        WHERE id = ANY($1::int[])
          AND (card_image_thumb IS NOT NULL OR card_image_front IS NOT NULL)`,
      [capped]
    );
    // Cache hint: thumbs change rarely
    res.set("Cache-Control", "private, max-age=60");
    res.json(r.rows);
  } catch (error) {
    console.error("Thumbs batch error:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

/* =========================================================
   Geo detection — country/city inference for CRM contacts
   ========================================================= */
const { detectGeo } = require("./geo");

// POST /api/crm/geo/detect
// Body: { phone?, city?, address?, country?, email? }
// Returns: { country, countryCode, flag, city, lat, lng, confidence, source, alternates, signals }
//
// Used by:
//   - ContactFormModal      (live suggestion as the user types)
//   - ScanCardModal         (auto-fill suggestion after card OCR)
//   - InterestedModal (DNA) (background detection on submit)
//   - ImportContactsModal   (per-row preview during CSV/Excel import)
app.post("/api/crm/geo/detect", async (req, res) => {
  try {
    const result = await detectGeo(req.body || {});
    // Geo lookups are stable for hours; CDN-friendly cache
    res.set("Cache-Control", "private, max-age=300");
    res.json(result);
  } catch (err) {
    console.error("geo/detect failed:", err);
    res.status(500).json({ error: "Geo detection failed" });
  }
});

/* =========================================================
   DNA → CRM bridge — public endpoints (no Clerk auth required)
   ========================================================= */

// Simple in-memory rate limiter: max 3 submissions per IP per minute
const dnaLeadHits = new Map(); // ip -> [timestamps]
const DNA_RATE_LIMIT = 3;
const DNA_RATE_WINDOW_MS = 60_000;

const checkDnaRateLimit = (ip) => {
  const now = Date.now();
  const arr = (dnaLeadHits.get(ip) || []).filter((t) => now - t < DNA_RATE_WINDOW_MS);
  if (arr.length >= DNA_RATE_LIMIT) return false;
  arr.push(now);
  dnaLeadHits.set(ip, arr);
  return true;
};

const normalisePhone = (s) => String(s || "").replace(/[^0-9]/g, "");
const normaliseEmail = (s) => String(s || "").trim().toLowerCase();
const cleanString = (s, max = 200) => String(s || "").trim().slice(0, max);
const titleCase = (s) => String(s || "")
  .split(/\s+/)
  .filter(Boolean)
  .map((w) => w[0]?.toUpperCase() + w.slice(1).toLowerCase())
  .join(" ");

// POST /api/crm/dna-lead — Public DNA "I'm interested" form
app.post("/api/crm/dna-lead", async (req, res) => {
  try {
    const ip = (req.headers["x-forwarded-for"] || req.socket?.remoteAddress || "unknown")
      .toString().split(",")[0].trim();

    if (!checkDnaRateLimit(ip)) {
      return res.status(429).json({ error: "Too many submissions. Please try again in a minute." });
    }

    const {
      firstName, lastName, email, phone, company, title, message, sku, snapshot, hp,
    } = req.body || {};

    // Honeypot — bots fill this hidden field; humans don't see it
    if (hp) return res.status(200).json({ success: true }); // pretend success, drop silently

    const cleanFirst = cleanString(firstName, 80);
    const cleanLast = cleanString(lastName, 80);
    const cleanEmail = normaliseEmail(email).slice(0, 200);
    const cleanPhone = cleanString(phone, 60);
    const cleanCompany = cleanString(company, 200);
    const cleanTitle = cleanString(title, 120);
    const cleanMessage = cleanString(message, 1000);
    const cleanSku = cleanString(sku, 60);

    if (!cleanFirst && !cleanLast) {
      return res.status(400).json({ error: "Name is required" });
    }
    if (!cleanEmail && !cleanPhone) {
      return res.status(400).json({ error: "Email or phone is required" });
    }
    if (cleanEmail && !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(cleanEmail)) {
      return res.status(400).json({ error: "Email is not valid" });
    }

    const fullName = titleCase([cleanFirst, cleanLast].filter(Boolean).join(" "));
    const phoneNorm = normalisePhone(cleanPhone);

    // ---- Find existing shared contact by email or normalised phone ----
    let contact = null;
    if (cleanEmail) {
      const r = await pool.query(
        `SELECT * FROM crm_contacts WHERE shared = TRUE AND LOWER(email) = $1 LIMIT 1`,
        [cleanEmail]
      );
      if (r.rows.length) contact = r.rows[0];
    }
    if (!contact && phoneNorm) {
      const r = await pool.query(
        `SELECT * FROM crm_contacts
           WHERE shared = TRUE
             AND regexp_replace(COALESCE(phone,''),'[^0-9]','','g') = $1
           LIMIT 1`,
        [phoneNorm]
      );
      if (r.rows.length) contact = r.rows[0];
    }

    // Silent geo detection from phone / email — for DNA leads we don't ask
    // the visitor for location, so this gives the CRM staff a populated
    // country field for filtering/segmenting without any extra UX noise.
    // We also reformat the phone with its country code if the visitor
    // typed a bare local number and we managed to infer the country.
    let detectedCountry = null;
    let normalisedPhone = cleanPhone;
    try {
      const geo = await detectGeo({ phone: cleanPhone, email: cleanEmail });
      if (geo?.country && (geo.confidence === "high" || geo.confidence === "medium")) {
        detectedCountry = geo.country;
      }
      if (geo?.formattedPhone?.international) {
        normalisedPhone = geo.formattedPhone.international;
      }
    } catch (_) { /* non-blocking */ }

    let isNew = false;
    if (contact) {
      // Update missing fields only — never overwrite human-edited data
      contact = (await pool.query(
        `UPDATE crm_contacts SET
            name = CASE WHEN COALESCE(NULLIF(name,''),'') = '' THEN $2 ELSE name END,
            email = COALESCE(NULLIF(email,''), $3),
            phone = COALESCE(NULLIF(phone,''), $4),
            company = COALESCE(NULLIF(company,''), $5),
            title = COALESCE(NULLIF(title,''), $6),
            dna_sku = COALESCE(dna_sku, $7),
            country = COALESCE(NULLIF(country,''), $8),
            last_contact_at = NOW(),
            updated_at = NOW()
          WHERE id = $1 RETURNING *`,
        [contact.id, fullName, cleanEmail || null, normalisedPhone || null, cleanCompany || null, cleanTitle || null, cleanSku || null, detectedCountry]
      )).rows[0];
    } else {
      isNew = true;
      contact = (await pool.query(
        `INSERT INTO crm_contacts
           (user_id, shared, name, type, email, phone, company, title, source, dna_sku, country, tags, last_contact_at)
         VALUES ($1, TRUE, $2, 'lead', $3, $4, $5, $6, 'dna_lead', $7, $8, $9::jsonb, NOW())
         RETURNING *`,
        [
          'dna_public',                      // sentinel user_id; the row is shared anyway
          fullName,
          cleanEmail || null,
          normalisedPhone || null,
          cleanCompany || null,
          cleanTitle || null,
          cleanSku || null,
          detectedCountry,
          JSON.stringify(['DNA Lead']),
        ]
      )).rows[0];
    }

    // ---- Look the SKU up in real inventory so the deal item carries
    //      a real image, real bruto price, and trustworthy specs.
    //      We try stones first, then jewelry.
    let realCategory = snapshot?.category || null;
    let realSnapshot = { ...(snapshot || {}) };
    let brutoPrice = 0;

    if (cleanSku) {
      try {
        const stoneRes = await pool.query(
          `SELECT sku, category, shape, weight, color, clarity, lab, origin,
                  measurements, image, additional_pictures, video,
                  certificate_number, certificate_image, comment,
                  price_per_carat, total_price
             FROM soap_stones WHERE sku = $1 LIMIT 1`,
          [cleanSku]
        );

        if (stoneRes.rows.length) {
          const s = stoneRes.rows[0];
          let img = s.image;
          if (!img && s.additional_pictures) {
            const first = String(s.additional_pictures).split(';')[0];
            img = first ? first.trim() : null;
          }
          brutoPrice = Number(s.total_price) || 0;
          realCategory = s.category || realCategory;
          realSnapshot = {
            sku: s.sku,
            category: s.category,
            shape: s.shape,
            weightCt: s.weight ? Number(s.weight) : null,
            color: s.color,
            clarity: s.clarity,
            lab: s.lab,
            origin: s.origin,
            measurements: s.measurements,
            certificateNumber: s.certificate_number,
            certificateUrl: s.certificate_image,
            treatment: s.comment,
            imageUrl: img,
            video: s.video,
            pricePerCarat: Number(s.price_per_carat) || null,
            priceTotal: brutoPrice || null,
          };
        } else {
          // Try jewelry
          const jewRes = await pool.query(
            `SELECT model_number, jewelry_type, style, collection, metal_type,
                    total_carat, jewelry_weight, stone_type,
                    all_pictures_link, video_link, price
               FROM jewelry_products WHERE model_number = $1 LIMIT 1`,
            [cleanSku]
          );
          if (jewRes.rows.length) {
            const j = jewRes.rows[0];
            const firstImg = j.all_pictures_link
              ? String(j.all_pictures_link).split(';').map((x) => x.trim()).filter(Boolean)[0] || null
              : null;
            brutoPrice = Number(j.price) || 0;
            realCategory = realCategory || 'Jewelry';
            realSnapshot = {
              sku: j.model_number,
              category: 'Jewelry',
              jewelryType: j.jewelry_type,
              style: j.style,
              collection: j.collection,
              metalType: j.metal_type,
              totalCarat: j.total_carat ? Number(j.total_carat) : null,
              weightG: j.jewelry_weight ? Number(j.jewelry_weight) : null,
              stoneType: j.stone_type,
              imageUrl: firstImg,
              video: j.video_link,
              priceTotal: brutoPrice || null,
            };
          }
        }
      } catch (lookupErr) {
        console.warn('DNA lead inventory lookup failed:', lookupErr.message);
      }
    }

    // Net price (display default — same convention used everywhere else in the app)
    const netoPrice = brutoPrice ? Math.round(brutoPrice / 2) : 0;

    // ---- Create the deal in 'lead' stage ----
    const dealTitle = cleanSku
      ? `DNA inquiry · ${cleanSku}`
      : `DNA inquiry · ${fullName}`;

    const deal = (await pool.query(
      `INSERT INTO crm_deals
         (user_id, contact_id, title, stage, value, currency, notes, shared, dna_sku)
       VALUES ($1, $2, $3, 'lead', $4, 'USD', $5, TRUE, $6)
       RETURNING *`,
      [
        'dna_public',
        contact.id,
        dealTitle,
        netoPrice,
        cleanMessage || null,
        cleanSku || null,
      ]
    )).rows[0];

    // Attach the stone to the deal as a deal_item with real snapshot + a default custom price (neto)
    if (cleanSku) {
      await pool.query(
        `INSERT INTO crm_deal_items (deal_id, sku, category, snapshot, custom_price)
         VALUES ($1, $2, $3, $4::jsonb, $5)`,
        [deal.id, cleanSku, realCategory, JSON.stringify(realSnapshot), netoPrice || null]
      );
    }

    // Log an interaction so it appears in the timeline
    const subject = `Inquiry from DNA${cleanSku ? ` (${cleanSku})` : ''}`;
    const content = [
      cleanMessage,
      cleanSku ? `Stone: ${cleanSku}` : null,
      `IP: ${ip}`,
    ].filter(Boolean).join('\n');

    await pool.query(
      `INSERT INTO crm_interactions
         (user_id, contact_id, deal_id, type, direction, subject, content, metadata)
       VALUES ($1, $2, $3, 'dna_inquiry', 'incoming', $4, $5, $6::jsonb)`,
      ['dna_public', contact.id, deal.id, subject, content, JSON.stringify({ source: 'dna', sku: cleanSku, snapshot })]
    );

    res.status(201).json({
      success: true,
      isNew,
      contactId: contact.id,
      dealId: deal.id,
    });
  } catch (error) {
    console.error("DNA lead error:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

// GET /api/crm/dna-leads/unread-count?since=ISO  → { count, latest }
// Lets the CRM sidebar show a badge with new DNA leads
app.get("/api/crm/dna-leads/unread-count", async (req, res) => {
  try {
    const { since } = req.query;
    const sinceDate = since ? new Date(since) : new Date(Date.now() - 7 * 24 * 60 * 60 * 1000);
    if (isNaN(sinceDate.getTime())) {
      return res.status(400).json({ error: "Invalid 'since' timestamp" });
    }
    const r = await pool.query(
      `SELECT COUNT(*)::int AS count, MAX(created_at) AS latest
         FROM crm_contacts
         WHERE shared = TRUE AND source = 'dna_lead' AND created_at > $1`,
      [sinceDate.toISOString()]
    );
    res.json({ count: r.rows[0].count || 0, latest: r.rows[0].latest });
  } catch (error) {
    console.error("DNA unread count error:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

app.get("/api/crm/tags", async (req, res) => {
  try {
    const { userId } = req.query;
    if (!userId) return res.status(400).json({ error: "userId is required" });
    const result = await pool.query(
      `SELECT tag, COUNT(*)::int AS count
         FROM crm_contacts c, jsonb_array_elements_text(c.tags) AS tag
         WHERE c.user_id = $1
         GROUP BY tag
         ORDER BY count DESC, tag ASC`,
      [userId]
    );
    res.json(result.rows);
  } catch (error) {
    console.error("Tags fetch error:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

/* ---------- Verify business online (OpenAI Web Search) ---------- */
app.post("/api/crm/verify-business", async (req, res) => {
  try {
    const { contact } = req.body;
    if (!contact || (!contact.name && !contact.company)) {
      return res.status(400).json({ error: "Contact name or company is required" });
    }
    if (!process.env.OPENAI_API_KEY) {
      return res.status(500).json({ error: "OPENAI_API_KEY is not configured" });
    }

    const detailsLines = [
      contact.name ? `Person: ${contact.name}` : null,
      contact.company ? `Company: ${contact.company}` : null,
      contact.email ? `Email: ${contact.email}` : null,
      contact.phone ? `Phone: ${contact.phone}` : null,
      contact.website ? `Website: ${contact.website}` : null,
      contact.country || contact.city ? `Location: ${[contact.city, contact.country].filter(Boolean).join(", ")}` : null,
    ].filter(Boolean).join("\n");

    const prompt = `You are a business verification assistant for a diamond/jewelry trading CRM.
Search the public web (Google, LinkedIn, official websites, business directories such as Rapaport, Polygon, IDEX, JCK, GIA, etc.) and verify the following contact.

Return a STRICT JSON object with this shape:
{
  "verified": true | false,
  "confidence": "high" | "medium" | "low",
  "summary": "1-2 sentence plain-language summary of what you found",
  "discoveredFields": {
    "company": "...optional, if found and improved...",
    "website": "...",
    "phone": "...",
    "email": "...",
    "country": "...",
    "city": "...",
    "address": "...",
    "linkedin": "...",
    "instagram": "...",
    "industry": "...",
    "yearsActive": "...",
    "notes": "interesting context (e.g. 'Listed on Rapaport member directory since 2015')"
  },
  "warnings": ["any red flags, e.g. inactive site, mismatched country, etc."],
  "sources": [{"label": "site name", "url": "https://..."}]
}

Only include fields you are reasonably confident about. Omit fields that are unknown rather than guessing.
If you cannot find anything credible, set verified=false, confidence="low" and explain in summary.

Contact data to verify:
${detailsLines}`;

    const aiRes = await fetch("https://api.openai.com/v1/chat/completions", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${process.env.OPENAI_API_KEY}`,
      },
      body: JSON.stringify({
        model: "gpt-4o-mini-search-preview",
        messages: [
          { role: "system", content: "You are a meticulous research assistant. You always reply with valid JSON only — no markdown, no commentary." },
          { role: "user", content: prompt },
        ],
        web_search_options: {},
        response_format: { type: "json_object" },
      }),
    });

    if (!aiRes.ok) {
      // Fallback to non-search model if search variant unavailable
      const errText = await aiRes.text();
      console.warn("Search model failed, trying fallback:", aiRes.status, errText);

      const fallback = await fetch("https://api.openai.com/v1/chat/completions", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${process.env.OPENAI_API_KEY}`,
        },
        body: JSON.stringify({
          model: "gpt-4o-mini",
          messages: [
            { role: "system", content: "You are a meticulous research assistant. Respond with valid JSON only." },
            { role: "user", content: prompt + "\n\nNote: Web search is unavailable — base your response only on widely-known public information you already have." },
          ],
          response_format: { type: "json_object" },
        }),
      });

      if (!fallback.ok) {
        const fbErr = await fallback.text();
        console.error("Fallback OpenAI error:", fallback.status, fbErr);
        let friendly = `Verification failed (${fallback.status})`;
        if (fallback.status === 429) friendly = "OpenAI quota exceeded. Add credit at platform.openai.com.";
        if (fallback.status === 401) friendly = "OpenAI API key invalid.";
        return res.status(502).json({ error: friendly });
      }
      const fbData = await fallback.json();
      try {
        const parsed = JSON.parse(fbData.choices?.[0]?.message?.content || "{}");
        return res.json({ ...parsed, _searchUsed: false });
      } catch (e) {
        return res.status(502).json({ error: "Could not parse verification response" });
      }
    }

    const data = await aiRes.json();
    let parsed;
    try {
      parsed = JSON.parse(data.choices?.[0]?.message?.content || "{}");
    } catch (e) {
      return res.status(502).json({ error: "Could not parse verification response" });
    }
    res.json({ ...parsed, _searchUsed: true });
  } catch (error) {
    console.error("Verify business error:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

/* ---------- Interactions ---------- */

app.post("/api/crm/interactions", async (req, res) => {
  try {
    const { userId, contactId, dealId, type, direction, subject, content, metadata, occurredAt } = req.body;
    if (!userId || !contactId || !type) return res.status(400).json({ error: "userId, contactId and type are required" });

    const result = await pool.query(
      `INSERT INTO crm_interactions (user_id, contact_id, deal_id, type, direction, subject, content, metadata, occurred_at)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,COALESCE($9, NOW())) RETURNING *`,
      [userId, contactId, dealId || null, type, direction || 'outgoing', subject || null, content || null, JSON.stringify(metadata || {}), occurredAt || null]
    );
    await pool.query("UPDATE crm_contacts SET last_contact_at = NOW(), updated_at = NOW() WHERE id = $1", [contactId]);
    res.status(201).json(result.rows[0]);

    // Mirror into the unified activity log so the contact's timeline + the
    // workspace feed see the same interaction without a UNION at read time.
    const inter = result.rows[0];
    const { actorId, actorName } = getActor(req);
    const meta = inter.metadata || {};
    const sku = meta.sku || meta.dna_sku;
    logActivity({
      userId,
      actorId, actorName,
      entityType: 'interaction',
      entityId:   inter.id,
      action:     inter.type || 'logged',
      summary:    inter.subject || `Logged ${inter.type}`,
      related: [
        { type: 'contact', id: contactId },
        dealId ? { type: 'deal', id: dealId } : null,
        sku    ? { type: 'stone', id: sku }  : null,
      ].filter(Boolean),
    });
  } catch (error) {
    console.error("Error creating interaction:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

app.delete("/api/crm/interactions/:id", async (req, res) => {
  try {
    const { id } = req.params;
    const beforeRes = await pool.query(
      "SELECT user_id, contact_id, deal_id, subject, type FROM crm_interactions WHERE id = $1",
      [id]
    );
    await pool.query("DELETE FROM crm_interactions WHERE id = $1", [id]);
    res.json({ success: true });

    const before = beforeRes.rows[0];
    if (before) {
      const { actorId, actorName } = getActor(req);
      logActivity({
        userId:     before.user_id,
        actorId, actorName,
        entityType: 'interaction',
        entityId:   id,
        action:     'deleted',
        summary:    `Deleted interaction: ${before.subject || before.type || '#' + id}`,
        related: [
          before.contact_id ? { type: 'contact', id: before.contact_id } : null,
          before.deal_id    ? { type: 'deal',    id: before.deal_id    } : null,
        ].filter(Boolean),
      });
    }
  } catch (error) {
    console.error("Error deleting interaction:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

/* ---------- Deals ---------- */

app.get("/api/crm/deals", async (req, res) => {
  try {
    const { stage, contactId, assignedTo } = req.query;
    const ctx = await resolveTeamContext(req);
    if (!ctx.actorUserId) return res.status(400).json({ error: "userId is required" });
    const tenantUserId = ctx.tenantUserId;

    // Broadcast: each user sees their own deals AND any deal flagged as shared (DNA leads)
    const conditions = ["(d.user_id = $1 OR d.shared = TRUE)"];
    const values = [tenantUserId];
    let idx = 2;

    if (!ctx.isOwner) {
      // Reps see ONLY deals explicitly assigned to them. Unassigned deals
      // stay owner-only.
      conditions.push(`d.assigned_to = $${idx}`);
      values.push(ctx.actorUserId);
      idx++;
    }
    if (assignedTo === 'me') {
      conditions.push(`d.assigned_to = $${idx}`);
      values.push(ctx.actorUserId);
      idx++;
    } else if (assignedTo === 'unassigned') {
      conditions.push(`d.assigned_to IS NULL`);
    } else if (assignedTo && assignedTo !== 'all') {
      conditions.push(`d.assigned_to = $${idx}`);
      values.push(assignedTo);
      idx++;
    }

    if (stage && stage !== 'all') { conditions.push(`d.stage = $${idx++}`); values.push(stage); }
    if (contactId) { conditions.push(`d.contact_id = $${idx++}`); values.push(contactId); }

    const result = await pool.query(
      `SELECT d.*, c.name AS contact_name, c.company AS contact_company, c.type AS contact_type,
        (SELECT COUNT(*)::int FROM crm_deal_items i WHERE i.deal_id = d.id) AS items_count
       FROM crm_deals d
       LEFT JOIN crm_contacts c ON c.id = d.contact_id
       WHERE ${conditions.join(" AND ")}
       ORDER BY d.updated_at DESC`,
      values
    );
    res.json(result.rows);
  } catch (error) {
    console.error("Error fetching deals:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

app.get("/api/crm/deals/:id", async (req, res) => {
  try {
    const { id } = req.params;
    const ctx = await resolveTeamContext(req);
    if (!ctx.actorUserId) return res.status(400).json({ error: "userId is required" });
    const tenantUserId = ctx.tenantUserId;

    const deal = await pool.query(
      `SELECT d.*, c.name AS contact_name, c.company AS contact_company, c.phone AS contact_phone, c.email AS contact_email
       FROM crm_deals d
       LEFT JOIN crm_contacts c ON c.id = d.contact_id
       WHERE d.id = $1 AND (d.user_id = $2 OR d.shared = TRUE)`,
      [id, tenantUserId]
    );
    if (deal.rows.length === 0) return res.status(404).json({ error: "Deal not found" });
    if (!canReadAssignment(ctx, deal.rows[0].assigned_to)) {
      return res.status(404).json({ error: "Deal not found" });
    }
    const items = await pool.query("SELECT * FROM crm_deal_items WHERE deal_id = $1 ORDER BY created_at ASC", [id]);
    const interactions = await pool.query("SELECT * FROM crm_interactions WHERE deal_id = $1 ORDER BY occurred_at DESC", [id]);
    // Linked jewelry items: anything pointing at this deal via deal_id or sold_deal_id
    const jewelry = await pool.query(
      `SELECT id, sku, name, type, status, category, sale_price, total_cost, cover_image_url, created_at
         FROM jewelry_items
        WHERE deal_id = $1 OR sold_deal_id = $1
        ORDER BY created_at DESC`,
      [id]
    ).catch(() => ({ rows: [] }));
    res.json({
      ...deal.rows[0],
      items: items.rows,
      interactions: interactions.rows,
      jewelry_items: jewelry.rows,
    });
  } catch (error) {
    console.error("Error fetching deal:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

app.post("/api/crm/deals", async (req, res) => {
  try {
    const { contactId, title, stage, value, currency, probability, expectedClose, notes, items, assignedTo } = req.body;
    const ctx = await resolveTeamContext(req);
    if (!ctx.actorUserId) return res.status(400).json({ error: "userId is required" });
    if (!contactId || !title) return res.status(400).json({ error: "contactId and title are required" });
    const tenantUserId = ctx.tenantUserId;
    const finalAssignee = ctx.isOwner
      ? (assignedTo === undefined ? ctx.actorUserId : assignedTo || null)
      : ctx.actorUserId;

    const result = await pool.query(
      `INSERT INTO crm_deals (user_id, contact_id, title, stage, value, currency, probability, expected_close, notes, assigned_to)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) RETURNING *`,
      [tenantUserId, contactId, title.trim(), stage || 'lead', value || 0, currency || 'USD', probability || 0, expectedClose || null, notes || null, finalAssignee]
    );
    const deal = result.rows[0];

    if (Array.isArray(items) && items.length > 0) {
      for (const item of items) {
        await pool.query(
          `INSERT INTO crm_deal_items (deal_id, stone_id, sku, category, snapshot, custom_price, quantity, notes)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
          [deal.id, item.stoneId || null, item.sku || null, item.category || null, JSON.stringify(item.snapshot || {}), item.customPrice || null, item.quantity || 1, item.notes || null]
        );
      }
    }
    res.status(201).json(deal);

    logActivity({
      userId:    tenantUserId,
      actorId:   ctx.actorUserId,
      actorName: ctx.actorName,
      entityType: 'deal',
      entityId:   deal.id,
      action:     'created',
      summary:    `Created deal "${deal.title}"${deal.value ? ` (${deal.currency || 'USD'} ${deal.value})` : ''}`,
      related: [
        contactId ? { type: 'contact', id: contactId } : null,
        ...(Array.isArray(items) ? items.filter(i => i.sku).map(i => ({ type: 'stone', id: i.sku })) : []),
      ].filter(Boolean),
    });
  } catch (error) {
    console.error("Error creating deal:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

app.put("/api/crm/deals/:id", async (req, res) => {
  try {
    const { id } = req.params;
    const ctx = await resolveTeamContext(req);
    const allowed = ['title','stage','value','currency','probability','expected_close','actual_close','notes','assigned_to'];
    const fields = [];
    const values = [];
    let idx = 1;

    for (const key of allowed) {
      const camel = key.replace(/_([a-z])/g, (_,c) => c.toUpperCase());
      if (req.body[camel] !== undefined) {
        if (key === 'assigned_to' && ctx.actorUserId && !ctx.isOwner) {
          if (req.body[camel] && String(req.body[camel]) !== String(ctx.actorUserId)) {
            return res.status(403).json({ error: 'Reps can only assign records to themselves' });
          }
        }
        fields.push(`${key} = $${idx++}`);
        values.push(req.body[camel]);
      }
    }
    if (req.body.stage === 'won' && !req.body.actualClose) {
      fields.push(`actual_close = $${idx++}`);
      values.push(new Date().toISOString().slice(0,10));
    }
    fields.push(`updated_at = NOW()`);
    if (fields.length === 1) return res.status(400).json({ error: "No fields to update" });

    // Snapshot before so we can detect stage transitions and field diffs.
    const beforeRes = await pool.query("SELECT * FROM crm_deals WHERE id = $1", [id]);
    const before = beforeRes.rows[0] || null;

    values.push(id);
    const result = await pool.query(
      `UPDATE crm_deals SET ${fields.join(", ")} WHERE id = $${idx} RETURNING *`,
      values
    );
    if (result.rows.length === 0) return res.status(404).json({ error: "Deal not found" });
    res.json(result.rows[0]);

    const after = result.rows[0];
    const changes = diffRows(before, after, allowed);
    if (changes && before) {
      const { actorId, actorName } = getActor(req);
      const stageChanged = changes.stage;
      // Stage transitions are first-class business events — give them their
      // own action so the FE can render them with a different icon/colour.
      const isStageChange = !!stageChanged && Object.keys(changes).length === 1;
      const action = isStageChange ? 'stage_changed' : 'updated';
      const summary = stageChanged
        ? `Moved deal "${after.title}" → ${stageChanged.to}`
        : (Object.keys(changes).length === 1
            ? `Updated ${Object.keys(changes)[0]} on "${after.title}"`
            : `Updated "${after.title}" (${Object.keys(changes).length} fields)`);
      logActivity({
        userId:     after.user_id,
        actorId, actorName,
        entityType: 'deal',
        entityId:   after.id,
        action,
        summary,
        changes,
        related:    after.contact_id ? [{ type: 'contact', id: after.contact_id }] : null,
      });
    }
  } catch (error) {
    console.error("Error updating deal:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

app.delete("/api/crm/deals/:id", async (req, res) => {
  try {
    const { id } = req.params;
    const beforeRes = await pool.query(
      "SELECT user_id, title, contact_id FROM crm_deals WHERE id = $1",
      [id]
    );
    await pool.query("DELETE FROM crm_deals WHERE id = $1", [id]);
    res.json({ success: true });

    const before = beforeRes.rows[0];
    if (before) {
      const { actorId, actorName } = getActor(req);
      logActivity({
        userId:     before.user_id,
        actorId, actorName,
        entityType: 'deal',
        entityId:   id,
        action:     'deleted',
        summary:    `Deleted deal "${before.title}"`,
        related:    before.contact_id ? [{ type: 'contact', id: before.contact_id }] : null,
      });
    }
  } catch (error) {
    console.error("Error deleting deal:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

/* ---------- Deal items ---------- */

app.post("/api/crm/deals/:id/items", async (req, res) => {
  try {
    const { id } = req.params;
    const { items } = req.body;
    if (!Array.isArray(items)) return res.status(400).json({ error: "items array is required" });

    const inserted = [];
    for (const item of items) {
      const r = await pool.query(
        `INSERT INTO crm_deal_items (deal_id, stone_id, sku, category, snapshot, custom_price, quantity, notes)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8) RETURNING *`,
        [id, item.stoneId || null, item.sku || null, item.category || null, JSON.stringify(item.snapshot || {}), item.customPrice || null, item.quantity || 1, item.notes || null]
      );
      inserted.push(r.rows[0]);
    }
    await pool.query("UPDATE crm_deals SET updated_at = NOW() WHERE id = $1", [id]);
    res.status(201).json(inserted);

    if (inserted.length > 0) {
      const dealRes = await pool.query("SELECT user_id, title FROM crm_deals WHERE id = $1", [id]);
      const deal = dealRes.rows[0];
      if (deal) {
        const { actorId, actorName } = getActor(req);
        logActivity({
          userId:     deal.user_id,
          actorId, actorName,
          entityType: 'deal',
          entityId:   id,
          action:     'items_added',
          summary:    `Added ${inserted.length} item${inserted.length === 1 ? '' : 's'} to "${deal.title}"`,
          related:    inserted.filter(it => it.sku).map(it => ({ type: 'stone', id: it.sku })),
        });
      }
    }
  } catch (error) {
    console.error("Error adding deal items:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

app.put("/api/crm/deal-items/:itemId", async (req, res) => {
  try {
    const { itemId } = req.params;
    const allowed = ['custom_price','quantity','notes'];
    const fields = [];
    const values = [];
    let idx = 1;
    for (const key of allowed) {
      const camel = key.replace(/_([a-z])/g, (_,c) => c.toUpperCase());
      if (req.body[camel] !== undefined) {
        fields.push(`${key} = $${idx++}`);
        values.push(req.body[camel]);
      }
    }
    if (fields.length === 0) return res.status(400).json({ error: "No fields to update" });
    values.push(itemId);
    const result = await pool.query(
      `UPDATE crm_deal_items SET ${fields.join(", ")} WHERE id = $${idx} RETURNING *`,
      values
    );
    res.json(result.rows[0]);
  } catch (error) {
    console.error("Error updating deal item:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

app.delete("/api/crm/deal-items/:itemId", async (req, res) => {
  try {
    await pool.query("DELETE FROM crm_deal_items WHERE id = $1", [req.params.itemId]);
    res.json({ success: true });
  } catch (error) {
    console.error("Error deleting deal item:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

/* ---------- Tasks ---------- */

app.get("/api/crm/tasks", async (req, res) => {
  try {
    const { status, contactId, dealId, assignedTo } = req.query;
    const ctx = await resolveTeamContext(req);
    if (!ctx.actorUserId) return res.status(400).json({ error: "userId is required" });
    const tenantUserId = ctx.tenantUserId;

    const conditions = ["t.user_id = $1"];
    const values = [tenantUserId];
    let idx = 2;
    if (!ctx.isOwner) {
      // Reps see ONLY tasks explicitly assigned to them.
      conditions.push(`t.assigned_to = $${idx}`);
      values.push(ctx.actorUserId);
      idx++;
    }
    if (assignedTo === 'me') {
      conditions.push(`t.assigned_to = $${idx}`);
      values.push(ctx.actorUserId);
      idx++;
    } else if (assignedTo === 'unassigned') {
      conditions.push(`t.assigned_to IS NULL`);
    } else if (assignedTo && assignedTo !== 'all') {
      conditions.push(`t.assigned_to = $${idx}`);
      values.push(assignedTo);
      idx++;
    }
    if (status && status !== 'all') { conditions.push(`t.status = $${idx++}`); values.push(status); }
    if (contactId) { conditions.push(`t.contact_id = $${idx++}`); values.push(contactId); }
    if (dealId) { conditions.push(`t.deal_id = $${idx++}`); values.push(dealId); }

    const result = await pool.query(
      `SELECT t.*, c.name AS contact_name, d.title AS deal_title
       FROM crm_tasks t
       LEFT JOIN crm_contacts c ON c.id = t.contact_id
       LEFT JOIN crm_deals d ON d.id = t.deal_id
       WHERE ${conditions.join(" AND ")}
       ORDER BY t.status ASC, t.due_date ASC NULLS LAST`,
      values
    );
    res.json(result.rows);
  } catch (error) {
    console.error("Error fetching tasks:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

app.post("/api/crm/tasks", async (req, res) => {
  try {
    const { contactId, dealId, title, description, dueDate, priority, status, assignedTo } = req.body;
    const ctx = await resolveTeamContext(req);
    if (!ctx.actorUserId) return res.status(400).json({ error: "userId is required" });
    if (!title) return res.status(400).json({ error: "title is required" });
    const tenantUserId = ctx.tenantUserId;
    const finalAssignee = ctx.isOwner
      ? (assignedTo === undefined ? ctx.actorUserId : assignedTo || null)
      : ctx.actorUserId;

    const result = await pool.query(
      `INSERT INTO crm_tasks (user_id, contact_id, deal_id, title, description, due_date, priority, status, assigned_to)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9) RETURNING *`,
      [tenantUserId, contactId || null, dealId || null, title.trim(), description || null, dueDate || null, priority || 'normal', status || 'pending', finalAssignee]
    );
    res.status(201).json(result.rows[0]);

    const task = result.rows[0];
    logActivity({
      userId:    tenantUserId,
      actorId:   ctx.actorUserId,
      actorName: ctx.actorName,
      entityType: 'task',
      entityId:   task.id,
      action:     'created',
      summary:    `Created task: ${task.title}`,
      related: [
        contactId ? { type: 'contact', id: contactId } : null,
        dealId    ? { type: 'deal',    id: dealId    } : null,
      ].filter(Boolean),
    });
  } catch (error) {
    console.error("Error creating task:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

app.put("/api/crm/tasks/:id", async (req, res) => {
  try {
    const { id } = req.params;
    const ctx = await resolveTeamContext(req);
    const allowed = ['title','description','due_date','priority','status','assigned_to'];
    const fields = [];
    const values = [];
    let idx = 1;
    for (const key of allowed) {
      const camel = key.replace(/_([a-z])/g, (_,c) => c.toUpperCase());
      if (req.body[camel] !== undefined) {
        if (key === 'assigned_to' && ctx.actorUserId && !ctx.isOwner) {
          if (req.body[camel] && String(req.body[camel]) !== String(ctx.actorUserId)) {
            return res.status(403).json({ error: 'Reps can only assign records to themselves' });
          }
        }
        fields.push(`${key} = $${idx++}`);
        values.push(req.body[camel]);
      }
    }
    if (req.body.status === 'done') {
      fields.push(`completed_at = NOW()`);
    }
    fields.push(`updated_at = NOW()`);
    if (fields.length === 1) return res.status(400).json({ error: "No fields to update" });

    const beforeRes = await pool.query("SELECT * FROM crm_tasks WHERE id = $1", [id]);
    const before = beforeRes.rows[0] || null;

    values.push(id);
    const result = await pool.query(
      `UPDATE crm_tasks SET ${fields.join(", ")} WHERE id = $${idx} RETURNING *`,
      values
    );
    res.json(result.rows[0]);

    const after = result.rows[0];
    const changes = diffRows(before, after, allowed);
    if (changes && before) {
      const { actorId, actorName } = getActor(req);
      const justCompleted = changes.status && changes.status.to === 'done';
      const action = justCompleted ? 'completed' : 'updated';
      const summary = justCompleted
        ? `Completed task: ${after.title}`
        : (Object.keys(changes).length === 1
            ? `Updated ${Object.keys(changes)[0]} on task "${after.title}"`
            : `Updated task "${after.title}" (${Object.keys(changes).length} fields)`);
      logActivity({
        userId:     after.user_id,
        actorId, actorName,
        entityType: 'task',
        entityId:   after.id,
        action,
        summary,
        changes,
        related: [
          after.contact_id ? { type: 'contact', id: after.contact_id } : null,
          after.deal_id    ? { type: 'deal',    id: after.deal_id    } : null,
        ].filter(Boolean),
      });
    }
  } catch (error) {
    console.error("Error updating task:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

app.delete("/api/crm/tasks/:id", async (req, res) => {
  try {
    const { id } = req.params;
    const beforeRes = await pool.query(
      "SELECT user_id, title, contact_id, deal_id FROM crm_tasks WHERE id = $1",
      [id]
    );
    await pool.query("DELETE FROM crm_tasks WHERE id = $1", [id]);
    res.json({ success: true });

    const before = beforeRes.rows[0];
    if (before) {
      const { actorId, actorName } = getActor(req);
      logActivity({
        userId:     before.user_id,
        actorId, actorName,
        entityType: 'task',
        entityId:   id,
        action:     'deleted',
        summary:    `Deleted task: ${before.title}`,
        related: [
          before.contact_id ? { type: 'contact', id: before.contact_id } : null,
          before.deal_id    ? { type: 'deal',    id: before.deal_id    } : null,
        ].filter(Boolean),
      });
    }
  } catch (error) {
    console.error("Error deleting task:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

/* ---------- Interactions (GET + PUT) ----------
 * POST + DELETE already exist above. The Sales/Customer profile page
 * needs a way to fetch interactions (calls, notes, emails) for a contact
 * and to update them (e.g. mark a call as completed).
 */
app.get("/api/crm/interactions", async (req, res) => {
  try {
    const { userId, contactId, type, dealId, limit } = req.query;
    if (!userId) return res.status(400).json({ error: "userId is required" });

    const lim = Math.min(Number(limit) || 200, 500);

    // When the caller scopes by contactId we can't blindly filter the
    // interactions table by user_id. DNA-lead inquiries are persisted
    // under the synthetic tenant 'dna_public' (so all workspace members
    // share them), so a user_id=$workspaceUser filter silently hides
    // every "I'm interested" event from the Activity timeline. Instead,
    // verify the caller can SEE the contact (own or shared), then return
    // *all* interactions linked to it regardless of which tenant wrote
    // the row.
    if (contactId) {
      const access = await pool.query(
        "SELECT 1 FROM crm_contacts WHERE id = $1 AND (user_id = $2 OR shared = TRUE) LIMIT 1",
        [contactId, userId]
      );
      if (access.rowCount === 0) return res.json([]);

      const conditions = ["contact_id = $1"];
      const values = [contactId];
      let idx = 2;
      if (dealId) { conditions.push(`deal_id = $${idx++}`); values.push(dealId); }
      if (type)   { conditions.push(`type = $${idx++}`);    values.push(type); }
      const result = await pool.query(
        `SELECT * FROM crm_interactions
         WHERE ${conditions.join(" AND ")}
         ORDER BY occurred_at DESC NULLS LAST, created_at DESC
         LIMIT ${lim}`,
        values
      );
      return res.json(result.rows);
    }

    // No contactId → workspace-wide list. Keep the strict user_id scope so
    // tenants never see each other's private interactions.
    const conditions = ["user_id = $1"];
    const values = [userId];
    let idx = 2;
    if (dealId) { conditions.push(`deal_id = $${idx++}`); values.push(dealId); }
    if (type)   { conditions.push(`type = $${idx++}`);    values.push(type); }
    const result = await pool.query(
      `SELECT * FROM crm_interactions
       WHERE ${conditions.join(" AND ")}
       ORDER BY occurred_at DESC NULLS LAST, created_at DESC
       LIMIT ${lim}`,
      values
    );
    res.json(result.rows);
  } catch (error) {
    console.error("Error fetching interactions:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

app.put("/api/crm/interactions/:id", async (req, res) => {
  try {
    const { id } = req.params;
    const allowed = ["type", "direction", "subject", "content", "metadata", "occurred_at"];
    const fields = [];
    const values = [];
    let idx = 1;
    for (const key of allowed) {
      const camel = key.replace(/_([a-z])/g, (_, c) => c.toUpperCase());
      if (req.body[camel] !== undefined) {
        fields.push(`${key} = $${idx++}`);
        values.push(key === "metadata" ? JSON.stringify(req.body[camel] || {}) : req.body[camel]);
      }
    }
    if (fields.length === 0) return res.status(400).json({ error: "No fields to update" });
    values.push(id);
    const result = await pool.query(
      `UPDATE crm_interactions SET ${fields.join(", ")} WHERE id = $${idx} RETURNING *`,
      values
    );
    res.json(result.rows[0]);
  } catch (error) {
    console.error("Error updating interaction:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

/* ---------- Invoices ---------- */

// Auto-generate INV-YYYY-NNNN per user
async function generateInvoiceNumber(userId) {
  const year = new Date().getFullYear();
  const prefix = `INV-${year}-`;
  const result = await pool.query(
    `SELECT invoice_number FROM crm_invoices
     WHERE user_id = $1 AND invoice_number LIKE $2
     ORDER BY id DESC LIMIT 1`,
    [userId, `${prefix}%`]
  );
  let next = 1;
  if (result.rows[0]) {
    const last = result.rows[0].invoice_number || "";
    const m = last.match(/-(\d+)$/);
    if (m) next = Number(m[1]) + 1;
  }
  return `${prefix}${String(next).padStart(4, "0")}`;
}

app.get("/api/crm/invoices", async (req, res) => {
  try {
    const { userId, contactId, status, dealId } = req.query;
    if (!userId) return res.status(400).json({ error: "userId is required" });
    const conditions = ["i.user_id = $1"];
    const values = [userId];
    let idx = 2;
    if (contactId) { conditions.push(`i.contact_id = $${idx++}`); values.push(contactId); }
    if (status)    { conditions.push(`i.status = $${idx++}`);     values.push(status); }
    if (dealId)    { conditions.push(`i.deal_id = $${idx++}`);    values.push(dealId); }
    const result = await pool.query(
      `SELECT i.*, c.name AS contact_name, d.title AS deal_title
       FROM crm_invoices i
       LEFT JOIN crm_contacts c ON c.id = i.contact_id
       LEFT JOIN crm_deals d ON d.id = i.deal_id
       WHERE ${conditions.join(" AND ")}
       ORDER BY i.created_at DESC
       LIMIT 500`,
      values
    );
    res.json(result.rows);
  } catch (error) {
    console.error("Error fetching invoices:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

app.get("/api/crm/invoices/:id", async (req, res) => {
  try {
    const result = await pool.query(
      `SELECT i.*, c.name AS contact_name, d.title AS deal_title
       FROM crm_invoices i
       LEFT JOIN crm_contacts c ON c.id = i.contact_id
       LEFT JOIN crm_deals d ON d.id = i.deal_id
       WHERE i.id = $1`,
      [req.params.id]
    );
    if (!result.rows[0]) return res.status(404).json({ error: "Invoice not found" });
    res.json(result.rows[0]);
  } catch (error) {
    console.error("Error fetching invoice:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

app.post("/api/crm/invoices", async (req, res) => {
  try {
    const {
      userId, contactId, dealId, invoiceNumber, status,
      subtotal, tax, total, currency, issuedAt, dueAt, paidAt, notes, metadata
    } = req.body;
    if (!userId) return res.status(400).json({ error: "userId is required" });

    const number = invoiceNumber || (await generateInvoiceNumber(userId));
    const computedTotal = total != null ? total : (Number(subtotal || 0) + Number(tax || 0));

    const result = await pool.query(
      `INSERT INTO crm_invoices
        (user_id, contact_id, deal_id, invoice_number, status,
         subtotal, tax, total, currency, issued_at, due_at, paid_at, notes, metadata)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
       RETURNING *`,
      [
        userId, contactId || null, dealId || null, number, status || "draft",
        subtotal || 0, tax || 0, computedTotal, currency || "USD",
        issuedAt || null, dueAt || null, paidAt || null, notes || null,
        JSON.stringify(metadata || {}),
      ]
    );
    res.status(201).json(result.rows[0]);
  } catch (error) {
    console.error("Error creating invoice:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

app.put("/api/crm/invoices/:id", async (req, res) => {
  try {
    const { id } = req.params;
    const allowed = ["status", "subtotal", "tax", "total", "currency", "issued_at", "due_at", "paid_at", "notes", "metadata", "deal_id"];
    const fields = [];
    const values = [];
    let idx = 1;
    for (const key of allowed) {
      const camel = key.replace(/_([a-z])/g, (_, c) => c.toUpperCase());
      if (req.body[camel] !== undefined) {
        fields.push(`${key} = $${idx++}`);
        values.push(key === "metadata" ? JSON.stringify(req.body[camel] || {}) : req.body[camel]);
      }
    }
    // Auto-stamp paid_at when status flips to "paid" and caller didn't set one
    if (req.body.status === "paid" && req.body.paidAt === undefined) {
      fields.push(`paid_at = NOW()`);
    }
    fields.push(`updated_at = NOW()`);
    if (fields.length === 1) return res.status(400).json({ error: "No fields to update" });
    values.push(id);
    const result = await pool.query(
      `UPDATE crm_invoices SET ${fields.join(", ")} WHERE id = $${idx} RETURNING *`,
      values
    );
    res.json(result.rows[0]);
  } catch (error) {
    console.error("Error updating invoice:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

app.delete("/api/crm/invoices/:id", async (req, res) => {
  try {
    await pool.query("DELETE FROM crm_invoices WHERE id = $1", [req.params.id]);
    res.json({ success: true });
  } catch (error) {
    console.error("Error deleting invoice:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

/* ---------- Occasions (birthdays, anniversaries, etc.) ---------- */

app.get("/api/crm/occasions", async (req, res) => {
  try {
    const { userId, contactId, upcomingDays } = req.query;
    if (!userId) return res.status(400).json({ error: "userId is required" });
    const conditions = ["o.user_id = $1"];
    const values = [userId];
    let idx = 2;
    if (contactId) { conditions.push(`o.contact_id = $${idx++}`); values.push(contactId); }
    // "upcomingDays=30" returns only occasions whose next instance is in the next N days
    let extra = "";
    if (upcomingDays) {
      const days = Math.max(1, Math.min(365, Number(upcomingDays)));
      extra = `
        AND (
          (o.recurring_yearly = TRUE AND
            (DATE(make_date(EXTRACT(YEAR FROM CURRENT_DATE)::int, EXTRACT(MONTH FROM o.occurs_on)::int, EXTRACT(DAY FROM o.occurs_on)::int))
              BETWEEN CURRENT_DATE AND CURRENT_DATE + INTERVAL '${days} days'
             OR
             DATE(make_date(EXTRACT(YEAR FROM CURRENT_DATE)::int + 1, EXTRACT(MONTH FROM o.occurs_on)::int, EXTRACT(DAY FROM o.occurs_on)::int))
              BETWEEN CURRENT_DATE AND CURRENT_DATE + INTERVAL '${days} days'))
          OR (o.recurring_yearly = FALSE AND o.occurs_on BETWEEN CURRENT_DATE AND CURRENT_DATE + INTERVAL '${days} days')
        )
      `;
    }
    const result = await pool.query(
      `SELECT o.*, c.name AS contact_name
       FROM crm_occasions o
       LEFT JOIN crm_contacts c ON c.id = o.contact_id
       WHERE ${conditions.join(" AND ")} ${extra}
       ORDER BY o.occurs_on ASC
       LIMIT 500`,
      values
    );
    res.json(result.rows);
  } catch (error) {
    console.error("Error fetching occasions:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

app.post("/api/crm/occasions", async (req, res) => {
  try {
    const { userId, contactId, kind, label, occursOn, recurringYearly, notes } = req.body;
    if (!userId || !kind || !occursOn) {
      return res.status(400).json({ error: "userId, kind, and occursOn are required" });
    }
    const result = await pool.query(
      `INSERT INTO crm_occasions (user_id, contact_id, kind, label, occurs_on, recurring_yearly, notes)
       VALUES ($1,$2,$3,$4,$5,$6,$7) RETURNING *`,
      [userId, contactId || null, kind, label || null, occursOn, recurringYearly !== false, notes || null]
    );
    res.status(201).json(result.rows[0]);
  } catch (error) {
    console.error("Error creating occasion:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

app.put("/api/crm/occasions/:id", async (req, res) => {
  try {
    const { id } = req.params;
    const allowed = ["kind", "label", "occurs_on", "recurring_yearly", "notes"];
    const fields = [];
    const values = [];
    let idx = 1;
    for (const key of allowed) {
      const camel = key.replace(/_([a-z])/g, (_, c) => c.toUpperCase());
      if (req.body[camel] !== undefined) {
        fields.push(`${key} = $${idx++}`);
        values.push(req.body[camel]);
      }
    }
    if (fields.length === 0) return res.status(400).json({ error: "No fields to update" });
    values.push(id);
    const result = await pool.query(
      `UPDATE crm_occasions SET ${fields.join(", ")} WHERE id = $${idx} RETURNING *`,
      values
    );
    res.json(result.rows[0]);
  } catch (error) {
    console.error("Error updating occasion:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

app.delete("/api/crm/occasions/:id", async (req, res) => {
  try {
    await pool.query("DELETE FROM crm_occasions WHERE id = $1", [req.params.id]);
    res.json({ success: true });
  } catch (error) {
    console.error("Error deleting occasion:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

/* ---------- Phase C: Auto-generate tasks from upcoming occasions ----------
 * Idempotent. For every occasion happening within `days` (default 30) we
 * INSERT a task with metadata.{source:'occasion', occasion_id, occurs_on}
 * unless a task with that same (occasion_id, occurs_on) already exists.
 *
 * The FE calls this on dashboard / tasks page load so the workflow stays
 * "occasions are real to-dos", not just a passive list. Recurring occasions
 * are projected to the next year automatically.
 */
app.post("/api/crm/occasions/ensure-tasks", async (req, res) => {
  try {
    const userId = req.body?.userId || req.query?.userId;
    if (!userId) return res.status(400).json({ error: "userId is required" });
    const days = Math.max(1, Math.min(365, Number(req.body?.days || req.query?.days || 30)));
    const leadDays = Math.max(0, Math.min(60, Number(req.body?.leadDays || req.query?.leadDays || 7)));

    // Project occasions (respect recurring_yearly), filter to the requested
    // window, then insert tasks where one doesn't already exist.
    const inserted = await pool.query(
      `WITH proj AS (
         SELECT
           o.id, o.user_id, o.contact_id, o.kind, o.label, o.notes,
           CASE
             WHEN o.recurring_yearly THEN
               CASE
                 WHEN make_date(EXTRACT(YEAR FROM CURRENT_DATE)::int,
                                EXTRACT(MONTH FROM o.occurs_on)::int,
                                EXTRACT(DAY FROM o.occurs_on)::int) >= CURRENT_DATE
                   THEN make_date(EXTRACT(YEAR FROM CURRENT_DATE)::int,
                                  EXTRACT(MONTH FROM o.occurs_on)::int,
                                  EXTRACT(DAY FROM o.occurs_on)::int)
                 ELSE make_date(EXTRACT(YEAR FROM CURRENT_DATE)::int + 1,
                                EXTRACT(MONTH FROM o.occurs_on)::int,
                                EXTRACT(DAY FROM o.occurs_on)::int)
               END
             ELSE o.occurs_on
           END AS next_occurrence
         FROM crm_occasions o
         WHERE o.user_id = $1
       )
       INSERT INTO crm_tasks (user_id, contact_id, title, description, due_date, priority, status, metadata)
       SELECT
         p.user_id,
         p.contact_id,
         CONCAT('Reach out: ', COALESCE(p.label, INITCAP(p.kind))),
         CONCAT('Upcoming ', p.kind, ' on ', to_char(p.next_occurrence, 'YYYY-MM-DD'),
                CASE WHEN p.notes IS NOT NULL AND p.notes <> '' THEN E'\n\n' || p.notes ELSE '' END),
         (p.next_occurrence - ($3 || ' days')::interval)::timestamp,
         'high',
         'pending',
         jsonb_build_object(
           'source', 'occasion',
           'occasion_id', p.id,
           'occurs_on', to_char(p.next_occurrence, 'YYYY-MM-DD'),
           'kind', p.kind
         )
       FROM proj p
       WHERE p.next_occurrence BETWEEN CURRENT_DATE AND CURRENT_DATE + ($2 || ' days')::interval
         AND NOT EXISTS (
           SELECT 1 FROM crm_tasks t
           WHERE t.user_id = p.user_id
             AND COALESCE(t.contact_id, -1) = COALESCE(p.contact_id, -1)
             AND t.metadata->>'source' = 'occasion'
             AND t.metadata->>'occasion_id' = p.id::text
             AND t.metadata->>'occurs_on' = to_char(p.next_occurrence, 'YYYY-MM-DD')
         )
       RETURNING id`,
      [userId, String(days), String(leadDays)]
    );

    res.json({ created: inserted.rowCount, taskIds: inserted.rows.map((r) => r.id) });
  } catch (e) {
    console.error("ensure-tasks error:", e);
    res.status(500).json({ error: e.message });
  }
});

/* ---------- WhatsApp Log ---------- */

app.post("/api/crm/whatsapp-log", async (req, res) => {
  try {
    const { userId, contactId, phone, message, relatedItems } = req.body;
    if (!userId || !message) return res.status(400).json({ error: "userId and message are required" });

    const result = await pool.query(
      `INSERT INTO crm_whatsapp_log (user_id, contact_id, phone, message, related_items)
       VALUES ($1,$2,$3,$4,$5) RETURNING *`,
      [userId, contactId || null, phone || null, message, JSON.stringify(relatedItems || [])]
    );

    if (contactId) {
      await pool.query(
        `INSERT INTO crm_interactions (user_id, contact_id, type, direction, subject, content, metadata)
         VALUES ($1,$2,'whatsapp','outgoing','WhatsApp message',$3,$4)`,
        [userId, contactId, message, JSON.stringify({ phone, relatedItems: relatedItems || [] })]
      );
      await pool.query("UPDATE crm_contacts SET last_contact_at = NOW(), updated_at = NOW() WHERE id = $1", [contactId]);
    }
    res.status(201).json(result.rows[0]);
  } catch (error) {
    console.error("Error logging WhatsApp message:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

app.get("/api/crm/whatsapp-log", async (req, res) => {
  try {
    const { userId, contactId } = req.query;
    if (!userId) return res.status(400).json({ error: "userId is required" });

    const conditions = ["user_id = $1"];
    const values = [userId];
    let idx = 2;
    if (contactId) { conditions.push(`contact_id = $${idx++}`); values.push(contactId); }

    const result = await pool.query(
      `SELECT * FROM crm_whatsapp_log WHERE ${conditions.join(" AND ")} ORDER BY sent_at DESC LIMIT 200`,
      values
    );
    res.json(result.rows);
  } catch (error) {
    console.error("Error fetching whatsapp log:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

/* ---------- CRM Dashboard Stats ---------- */

app.get("/api/crm/stats", async (req, res) => {
  try {
    const { userId } = req.query;
    if (!userId) return res.status(400).json({ error: "userId is required" });

    const [contacts, deals, tasks, recentInteractions, topContacts, monthlyWon] = await Promise.all([
      pool.query(`
        SELECT
          COUNT(*)::int AS total,
          COUNT(*) FILTER (WHERE type = 'lead')::int AS leads,
          COUNT(*) FILTER (WHERE type = 'buyer')::int AS buyers,
          COUNT(*) FILTER (WHERE type = 'dealer')::int AS dealers,
          COUNT(*) FILTER (WHERE type = 'designer')::int AS designers,
          COUNT(*) FILTER (WHERE type = 'supplier')::int AS suppliers
        FROM crm_contacts WHERE user_id = $1
      `, [userId]),
      pool.query(`
        SELECT
          COUNT(*)::int AS total,
          COUNT(*) FILTER (WHERE stage NOT IN ('won','lost'))::int AS active,
          COUNT(*) FILTER (WHERE stage = 'won')::int AS won,
          COUNT(*) FILTER (WHERE stage = 'lost')::int AS lost,
          COALESCE(SUM(value) FILTER (WHERE stage NOT IN ('won','lost')),0) AS pipeline_value,
          COALESCE(SUM(value) FILTER (WHERE stage = 'won'),0) AS won_value
        FROM crm_deals WHERE user_id = $1
      `, [userId]),
      pool.query(`
        SELECT
          COUNT(*) FILTER (WHERE status = 'pending')::int AS pending,
          COUNT(*) FILTER (WHERE status = 'pending' AND due_date < NOW())::int AS overdue,
          COUNT(*) FILTER (WHERE status = 'pending' AND due_date::date = CURRENT_DATE)::int AS today
        FROM crm_tasks WHERE user_id = $1
      `, [userId]),
      pool.query(`
        SELECT i.*, c.name AS contact_name FROM crm_interactions i
        LEFT JOIN crm_contacts c ON c.id = i.contact_id
        WHERE i.user_id = $1
        ORDER BY i.occurred_at DESC LIMIT 10
      `, [userId]),
      pool.query(`
        SELECT c.id, c.name, c.company, c.type,
          COALESCE(SUM(d.value) FILTER (WHERE d.stage = 'won'),0) AS total_won,
          COUNT(d.id) FILTER (WHERE d.stage = 'won')::int AS deals_won
        FROM crm_contacts c
        LEFT JOIN crm_deals d ON d.contact_id = c.id
        WHERE c.user_id = $1
        GROUP BY c.id
        HAVING COUNT(d.id) FILTER (WHERE d.stage = 'won') > 0
        ORDER BY total_won DESC LIMIT 5
      `, [userId]),
      pool.query(`
        SELECT
          to_char(date_trunc('month', actual_close), 'YYYY-MM') AS month,
          COALESCE(SUM(value),0) AS value,
          COUNT(*)::int AS count
        FROM crm_deals
        WHERE user_id = $1 AND stage = 'won' AND actual_close >= NOW() - INTERVAL '12 months'
        GROUP BY 1 ORDER BY 1
      `, [userId]),
    ]);

    res.json({
      contacts: contacts.rows[0],
      deals: deals.rows[0],
      tasks: tasks.rows[0],
      recentInteractions: recentInteractions.rows,
      topContacts: topContacts.rows,
      monthlyWon: monthlyWon.rows,
    });
  } catch (error) {
    console.error("Error fetching CRM stats:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

/* =========================================================
   /api/dashboard/exec-summary – Phase B
   One round-trip that aggregates KPIs across CRM + Workshop +
   Stone Inventory + DNA so the home dashboard can show "Today
   across the company" without firing 6 different requests.
   Every block is wrapped in its own try so a single failing
   query (e.g. a missing optional table) never breaks the rest.
   ========================================================= */
app.get("/api/dashboard/exec-summary", async (req, res) => {
  try {
    const { userId } = req.query;
    if (!userId) return res.status(400).json({ error: "userId is required" });

    const safe = async (q, fallback) => {
      try {
        const r = await q();
        return r;
      } catch (e) {
        console.warn("exec-summary block warn:", e.message);
        return fallback;
      }
    };

    const [
      dealsRow,
      jewelryRow,
      contactsRow,
      stonesRow,
      stonesActive,
      dnaRow,
      occasionsRow,
      tasksRow,
    ] = await Promise.all([
      safe(
        () =>
          pool.query(
            `SELECT
               COUNT(*) FILTER (WHERE stage NOT IN ('won','lost'))::int             AS open_count,
               COALESCE(SUM(value) FILTER (WHERE stage NOT IN ('won','lost')),0)    AS open_value,
               COUNT(*) FILTER (WHERE stage = 'won' AND date_trunc('month', COALESCE(actual_close, updated_at)) = date_trunc('month', NOW()))::int AS won_month_count,
               COALESCE(SUM(value) FILTER (WHERE stage = 'won' AND date_trunc('month', COALESCE(actual_close, updated_at)) = date_trunc('month', NOW())),0) AS won_month_value
             FROM crm_deals WHERE user_id = $1`,
            [userId]
          ),
        { rows: [{ open_count: 0, open_value: 0, won_month_count: 0, won_month_value: 0 }] }
      ),
      safe(
        () =>
          pool.query(
            `SELECT
               COUNT(*) FILTER (WHERE status NOT IN ('sold','archived'))::int                   AS wip_count,
               COUNT(*) FILTER (WHERE status = 'ready')::int                                    AS ready_count,
               COUNT(*) FILTER (WHERE status = 'qc')::int                                       AS qc_count,
               COUNT(*) FILTER (WHERE status = 'sold' AND date_trunc('month', sold_at) = date_trunc('month', NOW()))::int AS sold_month_count,
               COALESCE(SUM(sale_price) FILTER (WHERE status = 'sold' AND date_trunc('month', sold_at) = date_trunc('month', NOW())),0) AS sold_month_value,
               COALESCE(SUM(total_cost) FILTER (WHERE status NOT IN ('sold','archived')),0)    AS wip_cost
             FROM jewelry_items WHERE user_id = $1`,
            [userId]
          ),
        { rows: [{ wip_count: 0, ready_count: 0, qc_count: 0, sold_month_count: 0, sold_month_value: 0, wip_cost: 0 }] }
      ),
      safe(
        () =>
          pool.query(
            `SELECT
               COUNT(*)::int                                                       AS total,
               COUNT(*) FILTER (WHERE created_at >= NOW() - INTERVAL '7 days')::int AS new_week
             FROM crm_contacts WHERE user_id = $1`,
            [userId]
          ),
        { rows: [{ total: 0, new_week: 0 }] }
      ),
      safe(
        () =>
          pool.query(
            `SELECT
               COUNT(*)::int                                                  AS total,
               COALESCE(SUM(NULLIF(total_price,'')::numeric),0)               AS total_value
             FROM soap_stones WHERE sku IS NOT NULL`
          ),
        { rows: [{ total: 0, total_value: 0 }] }
      ),
      // How much of the live stone inventory is currently held by jewelry jobs
      safe(
        () =>
          pool.query(
            `SELECT
               COUNT(DISTINCT stone_sku)::int AS active_skus
             FROM jewelry_item_stones
             WHERE consume_from_inventory = TRUE
               AND inventory_status IN ('reserved','set')`
          ),
        { rows: [{ active_skus: 0 }] }
      ),
      safe(
        () =>
          pool.query(
            `SELECT
               COUNT(*) FILTER (WHERE created_at >= NOW() - INTERVAL '7 days')::int  AS new_7d,
               COUNT(*) FILTER (WHERE created_at >= NOW() - INTERVAL '30 days')::int AS new_30d,
               COUNT(*)::int                                                          AS total_shared
             FROM crm_contacts
             WHERE shared = TRUE AND source = 'dna_lead'`
          ),
        { rows: [{ new_7d: 0, new_30d: 0, total_shared: 0 }] }
      ),
      // Upcoming occasions: respect recurring_yearly by projecting this year's date.
      safe(
        () =>
          pool.query(
            `WITH proj AS (
               SELECT id, contact_id, kind, label, occurs_on, recurring_yearly,
                 CASE
                   WHEN recurring_yearly THEN
                     CASE
                       WHEN make_date(EXTRACT(YEAR FROM CURRENT_DATE)::int,
                                      EXTRACT(MONTH FROM occurs_on)::int,
                                      EXTRACT(DAY FROM occurs_on)::int) >= CURRENT_DATE
                         THEN make_date(EXTRACT(YEAR FROM CURRENT_DATE)::int,
                                        EXTRACT(MONTH FROM occurs_on)::int,
                                        EXTRACT(DAY FROM occurs_on)::int)
                       ELSE make_date(EXTRACT(YEAR FROM CURRENT_DATE)::int + 1,
                                      EXTRACT(MONTH FROM occurs_on)::int,
                                      EXTRACT(DAY FROM occurs_on)::int)
                     END
                   ELSE occurs_on
                 END AS next_occurrence
               FROM crm_occasions
               WHERE user_id = $1
             )
             SELECT
               COUNT(*) FILTER (WHERE next_occurrence BETWEEN CURRENT_DATE AND CURRENT_DATE + INTERVAL '30 days')::int AS upcoming_30d,
               COUNT(*) FILTER (WHERE next_occurrence BETWEEN CURRENT_DATE AND CURRENT_DATE + INTERVAL '7 days')::int  AS upcoming_7d
             FROM proj`,
            [userId]
          ),
        { rows: [{ upcoming_30d: 0, upcoming_7d: 0 }] }
      ),
      safe(
        () =>
          pool.query(
            `SELECT
               COUNT(*) FILTER (WHERE status = 'pending')::int                                   AS pending,
               COUNT(*) FILTER (WHERE status = 'pending' AND due_date < NOW())::int              AS overdue,
               COUNT(*) FILTER (WHERE status = 'pending' AND due_date::date = CURRENT_DATE)::int AS today
             FROM crm_tasks WHERE user_id = $1`,
            [userId]
          ),
        { rows: [{ pending: 0, overdue: 0, today: 0 }] }
      ),
    ]);

    res.json({
      deals: dealsRow.rows[0],
      jewelry: jewelryRow.rows[0],
      contacts: contactsRow.rows[0],
      stones: { ...stonesRow.rows[0], active_skus: stonesActive.rows[0]?.active_skus || 0 },
      dna: dnaRow.rows[0],
      occasions: occasionsRow.rows[0],
      tasks: tasksRow.rows[0],
      generated_at: new Date().toISOString(),
    });
  } catch (e) {
    console.error("exec-summary fatal:", e);
    res.status(500).json({ error: e.message });
  }
});

/* =========================================================
   /api/dashboard/overview – Sprint 1.F (Overview tab)

   Powers the redesigned Overview tab on the unified Dashboard.
   Returns three blocks in one round-trip:

     1. kpis     – 8 KPI cards across CRM/Jewelry/Stones/Tasks/Occasions
     2. queue    – up to ~25 items the user should act on TODAY
                   (tasks due/overdue, occasions today, ready jewelry,
                    stale deals, recently received stones)
     3. activity – up to ~15 most recent significant events.
                   This is a *proxy* feed unioned from updated_at on
                   crm_deals / jewelry_items / crm_contacts (DNA leads).
                   Sprint 2 (Phase 1) will replace this with real
                   activity_log entries — the FE shape stays identical.

   Every block is wrapped in its own try so a single failing query never
   blanks the whole dashboard.
   ========================================================= */
app.get("/api/dashboard/overview", async (req, res) => {
  try {
    const { userId } = req.query;
    if (!userId) return res.status(400).json({ error: "userId is required" });

    const safe = async (q, fallback) => {
      try { return await q(); } catch (e) {
        console.warn("overview block warn:", e.message);
        return fallback;
      }
    };

    /* ── KPIs ────────────────────────────────────────────── */
    const [
      dealsRow,
      jewelryRow,
      stonesRow,
      dnaRow,
      occasionsRow,
      tasksRow,
    ] = await Promise.all([
      // Pipeline + sold-MTD via CRM deals
      safe(
        () => pool.query(
          `SELECT
             COUNT(*) FILTER (WHERE stage NOT IN ('won','lost'))::int            AS open_count,
             COALESCE(SUM(value) FILTER (WHERE stage NOT IN ('won','lost')),0)   AS open_value,
             COUNT(*) FILTER (WHERE stage = 'won' AND date_trunc('month', COALESCE(actual_close, updated_at)) = date_trunc('month', NOW()))::int AS won_month_count,
             COALESCE(SUM(value) FILTER (WHERE stage = 'won' AND date_trunc('month', COALESCE(actual_close, updated_at)) = date_trunc('month', NOW())),0) AS won_month_value
           FROM crm_deals WHERE user_id = $1`,
          [userId]
        ),
        { rows: [{ open_count: 0, open_value: 0, won_month_count: 0, won_month_value: 0 }] }
      ),
      // WIP cost + ready count + sold-MTD via jewelry
      safe(
        () => pool.query(
          `SELECT
             COUNT(*) FILTER (WHERE status NOT IN ('sold','archived'))::int                AS wip_count,
             COALESCE(SUM(total_cost) FILTER (WHERE status NOT IN ('sold','archived')),0)  AS wip_value,
             COUNT(*) FILTER (WHERE status = 'ready')::int                                 AS ready_count,
             COUNT(*) FILTER (WHERE status = 'sold' AND date_trunc('month', sold_at) = date_trunc('month', NOW()))::int AS sold_mtd_count,
             COALESCE(SUM(sale_price) FILTER (WHERE status = 'sold' AND date_trunc('month', sold_at) = date_trunc('month', NOW())),0) AS sold_mtd_value
           FROM jewelry_items WHERE user_id = $1`,
          [userId]
        ),
        { rows: [{ wip_count: 0, wip_value: 0, ready_count: 0, sold_mtd_count: 0, sold_mtd_value: 0 }] }
      ),
      // Stones inventory $ — total available value (soap_stones is the live mirror)
      safe(
        () => pool.query(
          `SELECT
             COUNT(*)::int                                       AS total,
             COALESCE(SUM(NULLIF(total_price,'')::numeric),0)    AS total_value
           FROM soap_stones WHERE sku IS NOT NULL`
        ),
        { rows: [{ total: 0, total_value: 0 }] }
      ),
      // New DNA leads (last 7 days, last 30 days)
      safe(
        () => pool.query(
          `SELECT
             COUNT(*) FILTER (WHERE created_at >= NOW() - INTERVAL '7 days')::int   AS new_7d,
             COUNT(*) FILTER (WHERE created_at >= NOW() - INTERVAL '30 days')::int  AS new_30d
           FROM crm_contacts
           WHERE shared = TRUE AND source = 'dna_lead'`
        ),
        { rows: [{ new_7d: 0, new_30d: 0 }] }
      ),
      // Occasions today + this-week — recurring_yearly aware
      safe(
        () => pool.query(
          `WITH proj AS (
             SELECT id,
               CASE
                 WHEN recurring_yearly THEN
                   CASE
                     WHEN make_date(EXTRACT(YEAR FROM CURRENT_DATE)::int,
                                    EXTRACT(MONTH FROM occurs_on)::int,
                                    EXTRACT(DAY FROM occurs_on)::int) >= CURRENT_DATE
                       THEN make_date(EXTRACT(YEAR FROM CURRENT_DATE)::int,
                                      EXTRACT(MONTH FROM occurs_on)::int,
                                      EXTRACT(DAY FROM occurs_on)::int)
                     ELSE make_date(EXTRACT(YEAR FROM CURRENT_DATE)::int + 1,
                                    EXTRACT(MONTH FROM occurs_on)::int,
                                    EXTRACT(DAY FROM occurs_on)::int)
                   END
                 ELSE occurs_on
               END AS next_occurrence
             FROM crm_occasions WHERE user_id = $1
           )
           SELECT
             COUNT(*) FILTER (WHERE next_occurrence = CURRENT_DATE)::int                                    AS today,
             COUNT(*) FILTER (WHERE next_occurrence BETWEEN CURRENT_DATE AND CURRENT_DATE + INTERVAL '7 days')::int  AS this_week
           FROM proj`,
          [userId]
        ),
        { rows: [{ today: 0, this_week: 0 }] }
      ),
      // Tasks due today + overdue
      safe(
        () => pool.query(
          `SELECT
             COUNT(*) FILTER (WHERE status = 'pending' AND due_date::date = CURRENT_DATE)::int  AS today,
             COUNT(*) FILTER (WHERE status = 'pending' AND due_date < NOW())::int               AS overdue
           FROM crm_tasks WHERE user_id = $1`,
          [userId]
        ),
        { rows: [{ today: 0, overdue: 0 }] }
      ),
    ]);

    /* ── Queue: things demanding attention TODAY ──────────── */
    const [
      tasksToday,
      occasionsToday,
      readyJewelry,
      staleDeals,
      newLeadsToday,
    ] = await Promise.all([
      // Pending tasks: overdue first, then today's
      safe(
        () => pool.query(
          `SELECT t.id, t.title, t.due_date, t.priority, t.contact_id, c.name AS contact_name
           FROM crm_tasks t
           LEFT JOIN crm_contacts c ON c.id = t.contact_id
           WHERE t.user_id = $1
             AND t.status = 'pending'
             AND (t.due_date < NOW() OR t.due_date::date = CURRENT_DATE)
           ORDER BY t.due_date ASC NULLS LAST
           LIMIT 8`,
          [userId]
        ),
        { rows: [] }
      ),
      // Occasions occurring today (recurring-aware)
      safe(
        () => pool.query(
          `WITH proj AS (
             SELECT o.id, o.contact_id, o.kind, o.label, o.occurs_on,
               CASE
                 WHEN o.recurring_yearly THEN
                   make_date(EXTRACT(YEAR FROM CURRENT_DATE)::int,
                             EXTRACT(MONTH FROM o.occurs_on)::int,
                             EXTRACT(DAY FROM o.occurs_on)::int)
                 ELSE o.occurs_on
               END AS next_occurrence
             FROM crm_occasions o WHERE o.user_id = $1
           )
           SELECT p.id, p.contact_id, p.kind, p.label, p.next_occurrence,
                  c.name AS contact_name
           FROM proj p
           LEFT JOIN crm_contacts c ON c.id = p.contact_id
           WHERE p.next_occurrence = CURRENT_DATE
           ORDER BY c.name
           LIMIT 8`,
          [userId]
        ),
        { rows: [] }
      ),
      // Jewelry items in 'ready' status — need handoff
      safe(
        () => pool.query(
          `SELECT j.id, j.sku, j.name, j.cover_image_url, j.updated_at,
                  c.id AS contact_id, c.name AS contact_name
           FROM jewelry_items j
           LEFT JOIN crm_contacts c ON c.id = j.contact_id
           WHERE j.user_id = $1 AND j.status = 'ready'
           ORDER BY j.updated_at DESC
           LIMIT 6`,
          [userId]
        ),
        { rows: [] }
      ),
      // Open deals untouched for 7+ days
      safe(
        () => pool.query(
          `SELECT d.id, d.title, d.stage, d.value, d.updated_at,
                  c.id AS contact_id, c.name AS contact_name
           FROM crm_deals d
           LEFT JOIN crm_contacts c ON c.id = d.contact_id
           WHERE d.user_id = $1
             AND d.stage NOT IN ('won','lost')
             AND d.updated_at < NOW() - INTERVAL '7 days'
           ORDER BY d.updated_at ASC
           LIMIT 5`,
          [userId]
        ),
        { rows: [] }
      ),
      // New DNA leads from the last 24 hours that I haven't contacted
      safe(
        () => pool.query(
          `SELECT id, name, email, phone, dna_sku, created_at
           FROM crm_contacts
           WHERE shared = TRUE AND source = 'dna_lead'
             AND created_at >= NOW() - INTERVAL '24 hours'
             AND last_contact_at IS NULL
           ORDER BY created_at DESC
           LIMIT 5`
        ),
        { rows: [] }
      ),
    ]);

    const queue = [];
    for (const t of tasksToday.rows) {
      const overdue = t.due_date && new Date(t.due_date) < new Date();
      queue.push({
        type: "task",
        id: `task-${t.id}`,
        title: t.title,
        sub: t.contact_name ? `for ${t.contact_name}` : "",
        severity: overdue ? "overdue" : "today",
        priority: t.priority,
        link: t.contact_id ? `/crm/customers/${t.contact_id}` : "/crm/tasks",
      });
    }
    for (const o of occasionsToday.rows) {
      queue.push({
        type: "occasion",
        id: `occ-${o.id}`,
        title: `${o.contact_name || "Contact"} — ${o.label || o.kind}`,
        sub: "today",
        severity: "today",
        link: o.contact_id ? `/crm/customers/${o.contact_id}` : "/crm/contacts",
      });
    }
    for (const j of readyJewelry.rows) {
      queue.push({
        type: "ready_item",
        id: `ready-${j.id}`,
        title: `${j.sku || j.name} is ready`,
        sub: j.contact_name ? `for ${j.contact_name}` : "no customer linked",
        severity: "info",
        link: `/jewelry/items/${j.id}`,
        image: j.cover_image_url || null,
      });
    }
    for (const d of staleDeals.rows) {
      const days = Math.floor((Date.now() - new Date(d.updated_at).getTime()) / 86400000);
      queue.push({
        type: "stale_deal",
        id: `deal-${d.id}`,
        title: d.title,
        sub: `${d.contact_name || "no contact"} · stage ${d.stage} · ${days}d idle`,
        severity: days > 21 ? "warn" : "info",
        link: d.contact_id ? `/crm/customers/${d.contact_id}` : "/crm/deals",
      });
    }
    for (const l of newLeadsToday.rows) {
      queue.push({
        type: "new_lead",
        id: `lead-${l.id}`,
        title: `New DNA lead: ${l.name}`,
        sub: l.dna_sku ? `interested in ${l.dna_sku}` : (l.email || l.phone || ""),
        severity: "info",
        link: `/crm/customers/${l.id}`,
      });
    }

    /* ── Activity feed (Sprint 2 / Phase 1: real activity_log) ── */
    // Per-row link is computed FE-side from entity_type/entity_id/related so
    // we keep the BE payload schema-flat. Filtered to last 14 days to match
    // the prior proxy feed's window.
    const activityRes = await safe(
      () => pool.query(
        `SELECT id, actor_id, actor_name, entity_type, entity_id,
                action, summary, changes, related, occurred_at
           FROM activity_log
          WHERE user_id = $1
            AND occurred_at >= NOW() - INTERVAL '14 days'
          ORDER BY occurred_at DESC NULLS LAST, id DESC
          LIMIT 20`,
        [userId]
      ),
      { rows: [] }
    );
    const activityCapped = activityRes.rows.map(r => ({
      id:          `act-${r.id}`,
      type:        `${r.entity_type}_${r.action}`,
      entity_type: r.entity_type,
      entity_id:   r.entity_id,
      action:      r.action,
      label:       r.summary || `${r.action} ${r.entity_type}`,
      sub:         r.actor_name || '',
      ts:          r.occurred_at,
      changes:     r.changes,
      related:     r.related,
    }));

    /* ── Response ────────────────────────────────────────── */
    res.json({
      kpis: {
        pipeline: {
          value: Number(dealsRow.rows[0].open_value || 0),
          count: Number(dealsRow.rows[0].open_count || 0),
        },
        wip: {
          value: Number(jewelryRow.rows[0].wip_value || 0),
          count: Number(jewelryRow.rows[0].wip_count || 0),
        },
        inventory: {
          value: Number(stonesRow.rows[0].total_value || 0),
          count: Number(stonesRow.rows[0].total || 0),
        },
        sold_mtd: {
          value: Number(jewelryRow.rows[0].sold_mtd_value || 0),
          count: Number(jewelryRow.rows[0].sold_mtd_count || 0),
        },
        tasks_today: {
          count: Number(tasksRow.rows[0].today || 0),
          overdue: Number(tasksRow.rows[0].overdue || 0),
        },
        items_ready: {
          count: Number(jewelryRow.rows[0].ready_count || 0),
        },
        new_leads: {
          new_7d: Number(dnaRow.rows[0].new_7d || 0),
          new_30d: Number(dnaRow.rows[0].new_30d || 0),
        },
        occasions: {
          today: Number(occasionsRow.rows[0].today || 0),
          this_week: Number(occasionsRow.rows[0].this_week || 0),
        },
      },
      queue,
      activity: activityCapped,
      generated_at: new Date().toISOString(),
    });
  } catch (e) {
    console.error("overview fatal:", e);
    res.status(500).json({ error: e.message });
  }
});

/* =========================================================
   /api/activity – Sprint 2 / Phase 1 read endpoint
   Returns rows from activity_log, optionally filtered by entity,
   actor, action type and time range. Used by:
     - OverviewTab "What just happened" feed (no filters)
     - Per-entity timelines (entityType + entityId)
     - Per-user audit views (actorId)
   ========================================================= */
app.get("/api/activity", async (req, res) => {
  try {
    const {
      userId,
      entityType,
      entityId,
      actorId,
      action,
      since,
      until,
      limit = 50,
      offset = 0,
    } = req.query;
    if (!userId) return res.status(400).json({ error: "userId is required" });

    const conds = ['user_id = $1'];
    const vals  = [userId];
    let i = 2;
    if (entityType) { conds.push(`entity_type = $${i++}`); vals.push(entityType); }
    if (entityId)   { conds.push(`entity_id = $${i++}`);   vals.push(String(entityId)); }
    if (actorId)    { conds.push(`actor_id = $${i++}`);    vals.push(actorId); }
    if (action)     { conds.push(`action = $${i++}`);      vals.push(action); }
    if (since)      { conds.push(`occurred_at >= $${i++}`); vals.push(new Date(since)); }
    if (until)      { conds.push(`occurred_at <  $${i++}`); vals.push(new Date(until)); }

    const lim = Math.min(Math.max(parseInt(limit, 10) || 50, 1), 200);
    const off = Math.max(parseInt(offset, 10) || 0, 0);
    vals.push(lim, off);

    const r = await pool.query(
      `SELECT id, actor_id, actor_name, entity_type, entity_id,
              action, summary, changes, related, occurred_at
         FROM activity_log
        WHERE ${conds.join(' AND ')}
        ORDER BY occurred_at DESC NULLS LAST, id DESC
        LIMIT $${i++} OFFSET $${i}`,
      vals
    );

    res.json({
      items: r.rows,
      limit: lim,
      offset: off,
      has_more: r.rows.length === lim,
    });
  } catch (e) {
    console.error('/api/activity error:', e);
    res.status(500).json({ error: e.message });
  }
});

/* =========================================================
   /api/dashboard/reports – Phase D
   Powers the Reports page. Returns revenue trend, production
   throughput, top customers, profit margins, and pipeline
   distribution in one shot. Each block is independently safe.
   ========================================================= */
app.get("/api/dashboard/reports", async (req, res) => {
  try {
    const { userId } = req.query;
    if (!userId) return res.status(400).json({ error: "userId is required" });
    const months = Math.max(3, Math.min(24, Number(req.query.months || 12)));

    const safe = async (q, fallback) => {
      try { return await q(); } catch (e) {
        console.warn("reports block warn:", e.message);
        return fallback;
      }
    };

    const [
      revenueByMonth,
      pipelineByStage,
      jewelryByStatus,
      topCustomers,
      recentSales,
      throughput,
      stoneActivity,
    ] = await Promise.all([
      // Revenue by month (paid invoices)
      safe(
        () => pool.query(
          `SELECT to_char(date_trunc('month', issued_at), 'YYYY-MM') AS month,
                  COALESCE(SUM(total),0)::numeric AS revenue,
                  COUNT(*)::int AS invoices
             FROM crm_invoices
            WHERE user_id = $1
              AND status = 'paid'
              AND issued_at >= date_trunc('month', NOW() - ($2 || ' months')::interval)
            GROUP BY 1
            ORDER BY 1`,
          [userId, String(months)]
        ),
        { rows: [] }
      ),
      // Pipeline value distributed across stages
      safe(
        () => pool.query(
          `SELECT stage,
                  COUNT(*)::int AS count,
                  COALESCE(SUM(value),0)::numeric AS value
             FROM crm_deals
            WHERE user_id = $1
            GROUP BY stage
            ORDER BY value DESC`,
          [userId]
        ),
        { rows: [] }
      ),
      // Workshop pipeline: how many pieces sit at each production stage
      safe(
        () => pool.query(
          `SELECT status,
                  COUNT(*)::int AS count,
                  COALESCE(SUM(total_cost),0)::numeric AS cost,
                  COALESCE(SUM(sale_price),0)::numeric AS sale_value
             FROM jewelry_items
            WHERE user_id = $1
              AND status NOT IN ('archived')
            GROUP BY status
            ORDER BY count DESC`,
          [userId]
        ),
        { rows: [] }
      ),
      // Top customers — combine "sold" jewelry value + "won" deals
      safe(
        () => pool.query(
          `WITH won_deals AS (
             SELECT contact_id, SUM(value) AS deal_value, COUNT(*)::int AS deal_count
               FROM crm_deals
              WHERE user_id = $1 AND stage = 'won'
              GROUP BY contact_id
           ),
           sold_pieces AS (
             SELECT contact_id AS cid, SUM(sale_price) AS jew_value, COUNT(*)::int AS jew_count
               FROM jewelry_items
              WHERE user_id = $1 AND status = 'sold' AND contact_id IS NOT NULL
              GROUP BY contact_id
             UNION ALL
             SELECT sold_to AS cid, SUM(sale_price) AS jew_value, COUNT(*)::int AS jew_count
               FROM jewelry_items
              WHERE user_id = $1 AND status = 'sold' AND sold_to IS NOT NULL AND sold_to <> contact_id
              GROUP BY sold_to
           )
           SELECT c.id, c.name, c.company, c.type,
                  COALESCE(MAX(wd.deal_value),0)::numeric  AS total_deal_value,
                  COALESCE(MAX(wd.deal_count),0)::int      AS deals_won,
                  COALESCE(SUM(sp.jew_value),0)::numeric   AS total_jewelry_value,
                  COALESCE(SUM(sp.jew_count),0)::int       AS jewelry_sold,
                  (COALESCE(MAX(wd.deal_value),0) + COALESCE(SUM(sp.jew_value),0))::numeric AS total_value
             FROM crm_contacts c
             LEFT JOIN won_deals  wd ON wd.contact_id = c.id
             LEFT JOIN sold_pieces sp ON sp.cid       = c.id
            WHERE c.user_id = $1
            GROUP BY c.id
           HAVING (COALESCE(MAX(wd.deal_value),0) + COALESCE(SUM(sp.jew_value),0)) > 0
            ORDER BY total_value DESC
            LIMIT 10`,
          [userId]
        ),
        { rows: [] }
      ),
      // Recent sold pieces with profit margin
      safe(
        () => pool.query(
          `SELECT id, sku, name, category, contact_id, sold_at,
                  COALESCE(sale_price,0)::numeric AS sale_price,
                  COALESCE(total_cost,0)::numeric AS total_cost,
                  (COALESCE(sale_price,0) - COALESCE(total_cost,0))::numeric AS profit,
                  CASE WHEN COALESCE(sale_price,0) > 0
                       THEN ROUND(((COALESCE(sale_price,0) - COALESCE(total_cost,0)) / sale_price * 100)::numeric, 1)
                       ELSE NULL END AS margin_pct
             FROM jewelry_items
            WHERE user_id = $1
              AND status = 'sold'
              AND sold_at IS NOT NULL
            ORDER BY sold_at DESC
            LIMIT 25`,
          [userId]
        ),
        { rows: [] }
      ),
      // Throughput: avg days from created -> ready, ready -> sold, created -> sold
      safe(
        () => pool.query(
          `SELECT
             AVG(EXTRACT(EPOCH FROM (sold_at - created_at))/86400)::numeric AS avg_days_to_sell,
             COUNT(*) FILTER (WHERE status = 'sold' AND sold_at >= NOW() - INTERVAL '90 days')::int AS sold_90d,
             COALESCE(SUM(sale_price) FILTER (WHERE status = 'sold' AND sold_at >= NOW() - INTERVAL '90 days'),0)::numeric AS sold_90d_value,
             COALESCE(SUM(total_cost) FILTER (WHERE status = 'sold' AND sold_at >= NOW() - INTERVAL '90 days'),0)::numeric AS sold_90d_cost
           FROM jewelry_items
           WHERE user_id = $1`,
          [userId]
        ),
        { rows: [{ avg_days_to_sell: null, sold_90d: 0, sold_90d_value: 0, sold_90d_cost: 0 }] }
      ),
      // Stone consumption summary
      safe(
        () => pool.query(
          `SELECT
             COUNT(*) FILTER (WHERE consume_from_inventory = TRUE)::int                                AS consumed_total,
             COUNT(*) FILTER (WHERE consume_from_inventory = TRUE AND inventory_status = 'reserved')::int AS reserved,
             COUNT(*) FILTER (WHERE consume_from_inventory = TRUE AND inventory_status = 'set')::int      AS in_setting,
             COUNT(*) FILTER (WHERE consume_from_inventory = TRUE AND inventory_status = 'sold')::int     AS sold
           FROM jewelry_item_stones jis
           JOIN jewelry_items ji ON ji.id = jis.item_id
           WHERE ji.user_id = $1`,
          [userId]
        ),
        { rows: [{ consumed_total: 0, reserved: 0, in_setting: 0, sold: 0 }] }
      ),
    ]);

    res.json({
      months,
      revenueByMonth: revenueByMonth.rows,
      pipelineByStage: pipelineByStage.rows,
      jewelryByStatus: jewelryByStatus.rows,
      topCustomers: topCustomers.rows,
      recentSales: recentSales.rows,
      throughput: throughput.rows[0],
      stoneActivity: stoneActivity.rows[0],
      generated_at: new Date().toISOString(),
    });
  } catch (e) {
    console.error("reports fatal:", e);
    res.status(500).json({ error: e.message });
  }
});

/* =========================================================
   CRM – Business Card Scanner (OpenAI Vision)
   ========================================================= */

const normPhone = (p) => (p || "").replace(/[^\d]/g, "");

app.post("/api/crm/scan-card", async (req, res) => {
  try {
    const { userId, imageBase64, imageBase64Front, imageBase64Back } = req.body;
    if (!userId) return res.status(400).json({ error: "userId is required" });

    // Backward-compat: imageBase64 = single image; new: imageBase64Front + optional imageBase64Back
    const front = imageBase64Front || imageBase64;
    const back = imageBase64Back || null;

    if (!front) return res.status(400).json({ error: "Front image is required" });
    if (!process.env.OPENAI_API_KEY) {
      return res.status(500).json({ error: "OPENAI_API_KEY is not configured on the server" });
    }

    const toDataUrl = (b64) => b64.startsWith("data:") ? b64 : `data:image/jpeg;base64,${b64}`;

    const sideCount = back ? 2 : 1;
    const prompt = `You are an OCR assistant for business cards used at gem and jewelry trade shows.
You will be shown ${sideCount} image(s) of a business card${back ? " (front and back of the SAME physical card)" : ""}.

CRITICAL: Sometimes the two sides of a single card show TWO DIFFERENT PEOPLE (a partner / colleague, with their own name, title, phone and email). In that case you MUST return TWO contacts.
Otherwise (same person, or back contains only company info / extra phone / address / logo / language translation) return ONE contact and merge the data.

Return ONLY valid JSON in this exact shape:
{
  "contacts": [
    {
      "name": string|null,
      "title": string|null,
      "company": string|null,
      "phone": string|null,
      "phoneAlt": string|null,
      "email": string|null,
      "website": string|null,
      "country": string|null,
      "city": string|null,
      "address": string|null,
      "type": "buyer"|"dealer"|"designer"|"supplier"|"lead",
      "notes": string|null,
      "language": string|null,
      "side": "front"|"back"|"both"
    }
  ],
  "isTwoPeople": boolean,
  "reason": "1 short sentence explaining why one or two contacts"
}

Rules:
- "title" = job title (CEO, Sales Director, Designer, etc.). NEVER put the title inside "notes".
- Choose "type" by guessing from title/company (jeweler/designer = designer; wholesale/diamond dealer = dealer; supplier/manufacturer = supplier; retailer = buyer; otherwise lead).
- If multiple phones for the same person, put the main mobile in "phone" and the office in "phoneAlt".
- Keep phone numbers exactly as printed (with + and country code if present).
- "website" = the URL on the card (without http:// prefix is OK; we will normalise).
- "notes" = ONLY extra text that does not fit the other fields (e.g. "Specialises in Burmese rubies"). Never duplicate name/title/phone/email here.
- For ONE contact spanning both sides: set side="both" and merge fields (do not duplicate).
- For TWO different people: return two entries with side="front" and side="back".
- Output ONLY the JSON object, no markdown.`;

    const userContent = [{ type: "text", text: prompt }];
    userContent.push({ type: "image_url", image_url: { url: toDataUrl(front), detail: "high" } });
    if (back) {
      userContent.push({ type: "text", text: "Above is the FRONT side. Below is the BACK side of the same card:" });
      userContent.push({ type: "image_url", image_url: { url: toDataUrl(back), detail: "high" } });
    }

    const aiRes = await fetch("https://api.openai.com/v1/chat/completions", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Authorization": `Bearer ${process.env.OPENAI_API_KEY}`,
      },
      body: JSON.stringify({
        model: "gpt-4o-mini",
        temperature: 0,
        response_format: { type: "json_object" },
        messages: [{ role: "user", content: userContent }],
      }),
    });

    if (!aiRes.ok) {
      const errText = await aiRes.text();
      console.error("OpenAI error:", aiRes.status, errText);
      let friendly = `OCR provider error (${aiRes.status})`;
      if (aiRes.status === 429) {
        friendly = "OpenAI quota exceeded. Add credit at platform.openai.com/settings/organization/billing/overview.";
      } else if (aiRes.status === 401) {
        friendly = "OpenAI API key invalid or revoked.";
      } else if (aiRes.status === 400) {
        friendly = "OpenAI rejected the image. Try a smaller, clearer photo.";
      }
      return res.status(502).json({ error: friendly, providerStatus: aiRes.status });
    }

    const aiData = await aiRes.json();
    let parsed;
    try {
      parsed = JSON.parse(aiData.choices?.[0]?.message?.content || "{}");
    } catch (e) {
      return res.status(500).json({ error: "Failed to parse OCR response" });
    }

    // Normalise -> always return contacts array
    let contacts = Array.isArray(parsed.contacts) ? parsed.contacts : [];
    if (contacts.length === 0) {
      // Backward-compat: maybe AI returned a flat object
      if (parsed.name || parsed.email || parsed.phone) {
        contacts = [{
          name: parsed.name || null,
          title: parsed.title || parsed.jobTitle || null,
          company: parsed.company || null,
          phone: parsed.phone || null,
          phoneAlt: parsed.phoneAlt || null,
          email: parsed.email || null,
          website: parsed.website || null,
          country: parsed.country || null,
          city: parsed.city || null,
          address: parsed.address || null,
          type: parsed.type || "lead",
          notes: parsed.notes || null,
          language: parsed.language || null,
          side: back ? "both" : "front",
        }];
      }
    }

    // Find matches per contact, with a confidence rating so the FE can
    // distinguish a *certain* dup (same email, or same full phone) from
    // a fuzzier "looks similar" hit (matching last 9 digits / partial).
    // The FE shows a hard red banner for exact matches and the existing
    // soft amber for partial ones.
    const enriched = [];
    for (const c of contacts) {
      let matches = [];
      let matchConfidence = "none"; // "exact" | "partial" | "none"
      const phoneDigits = normPhone(c.phone);
      const email = (c.email || "").toLowerCase().trim();
      if (phoneDigits || email) {
        const conditions = [];
        const values = [userId];
        let idx = 2;
        if (phoneDigits && phoneDigits.length >= 7) {
          conditions.push(`regexp_replace(coalesce(phone,''), '[^0-9]', '', 'g') LIKE $${idx++}`);
          values.push(`%${phoneDigits.slice(-9)}%`);
        }
        if (email) {
          conditions.push(`LOWER(email) = $${idx++}`);
          values.push(email);
        }
        if (conditions.length > 0) {
          const r = await pool.query(
            `SELECT id, name, type, title, company, phone, email, country, city, last_contact_at
             FROM crm_contacts
             WHERE user_id = $1 AND (${conditions.join(" OR ")})
             ORDER BY updated_at DESC LIMIT 5`,
            values
          );
          matches = r.rows;
          // Decide confidence by re-checking *exact* equality on the
          // candidate rows. Phone equality compares the full normalised
          // digits string; email equality is case-insensitive.
          for (const row of matches) {
            const rowPhoneDigits = normPhone(row.phone || "");
            const rowEmail = (row.email || "").toLowerCase().trim();
            const exactPhone =
              !!phoneDigits && phoneDigits.length >= 7 && rowPhoneDigits === phoneDigits;
            const exactEmail = !!email && rowEmail === email;
            if (exactPhone || exactEmail) {
              matchConfidence = "exact";
              // Annotate why we matched so the FE can show "Same email"
              // / "Same phone" badges in the alert.
              row._matchReason = exactEmail ? "email" : "phone";
              break;
            }
          }
          if (matchConfidence === "none" && matches.length > 0) {
            matchConfidence = "partial";
          }
        }
      }
      enriched.push({ extracted: c, matches, matchConfidence });
    }

    res.json({
      contacts: enriched,
      isTwoPeople: !!parsed.isTwoPeople && enriched.length > 1,
      reason: parsed.reason || null,
      // Backward-compat for old client
      extracted: enriched[0]?.extracted || null,
      matches: enriched[0]?.matches || [],
      matchConfidence: enriched[0]?.matchConfidence || "none",
    });
  } catch (error) {
    console.error("Error scanning card:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

/* =========================================================
   CRM – Companies (retail stores / wholesale partners)

   A "company" is a tenant of the CRM that groups several contacts
   together (e.g. a store has an owner + a manager + 2 salespeople).
   Memos live under a company, not a single person, so any contact at
   the store can interact with the same memo.
   ========================================================= */

const CO_FIELDS = [
  'name','type','primary_contact','email','phone','website',
  'country','city','address','tax_id','notes','tags',
  'default_memo_days','payment_terms','credit_limit','status',
  'assigned_to','logo_url',
  // Sprint 4 — extended profile fields
  'description','instagram','facebook','linkedin','whatsapp',
  'business_hours','cover_image_url','currency','established_year',
];

// JSON columns must be stringified before going to the DB.
const CO_JSON_FIELDS = new Set(['tags','business_hours']);

app.get("/api/crm/companies", async (req, res) => {
  try {
    const ctx = await resolveTeamContext(req);
    if (!ctx.actorUserId) return res.status(400).json({ error: "userId is required" });
    const tenantUserId = ctx.tenantUserId;
    const { search, type, status, assignedTo } = req.query;

    const where = ['user_id = $1'];
    const args = [tenantUserId];
    let idx = 2;
    if (search) {
      where.push(`(name ILIKE $${idx} OR email ILIKE $${idx} OR city ILIKE $${idx} OR primary_contact ILIKE $${idx})`);
      args.push(`%${search}%`); idx++;
    }
    if (type)    { where.push(`type = $${idx}`);    args.push(type);    idx++; }
    if (status)  { where.push(`status = $${idx}`);  args.push(status);  idx++; }
    if (assignedTo === 'unassigned') {
      where.push('assigned_to IS NULL');
    } else if (assignedTo === 'me') {
      where.push(`assigned_to = $${idx}`); args.push(ctx.actorUserId); idx++;
    } else if (assignedTo) {
      where.push(`assigned_to = $${idx}`); args.push(assignedTo); idx++;
    }

    // Reps see: explicitly assigned to them OR unassigned (so they can claim).
    if (!ctx.isOwner && ctx.actorUserId) {
      where.push(`(assigned_to IS NULL OR assigned_to = $${idx})`);
      args.push(ctx.actorUserId); idx++;
    }

    const sql = `
      SELECT c.*,
        (SELECT COUNT(*)::int FROM memos m WHERE m.company_id = c.id AND m.status IN ('out','partially_returned')) AS active_memos,
        (SELECT COUNT(*)::int FROM memos m WHERE m.company_id = c.id) AS total_memos,
        (SELECT COUNT(*)::int FROM crm_contacts ct WHERE ct.company_id = c.id) AS contact_count,
        -- Portal access summary used by the FE Hero / store card to show
        -- "No portal access" / "Portal active" pills without an extra request.
        (SELECT COUNT(*)::int FROM team_members tm
            WHERE tm.company_id = c.id AND tm.role = 'store_user' AND tm.active = TRUE) AS portal_user_count,
        (SELECT COUNT(*)::int FROM team_members tm
            WHERE tm.company_id = c.id AND tm.role = 'store_user' AND tm.active = TRUE AND tm.clerk_user_id IS NOT NULL) AS portal_user_active,
        -- Pending memo requests waiting for the supplier's review.
        -- Surfaced on the store card so the supplier can spot them at a glance.
        (SELECT COUNT(*)::int FROM memo_requests mr
            WHERE mr.company_id = c.id AND mr.user_id = c.user_id AND mr.status = 'pending') AS pending_requests_count
      FROM crm_companies c
      WHERE ${where.join(' AND ')}
      ORDER BY c.updated_at DESC
    `;
    const r = await pool.query(sql, args);
    res.json(r.rows);
  } catch (error) {
    console.error("Error fetching companies:", error);
    res.status(500).json({ error: error.message });
  }
});

app.get("/api/crm/companies/:id", async (req, res) => {
  try {
    const { id } = req.params;
    const ctx = await resolveTeamContext(req);
    if (!ctx.actorUserId) return res.status(400).json({ error: "userId is required" });
    const tenantUserId = ctx.tenantUserId;

    const compRes = await pool.query(
      `SELECT * FROM crm_companies WHERE id = $1 AND user_id = $2`,
      [id, tenantUserId]
    );
    if (compRes.rows.length === 0) return res.status(404).json({ error: "Company not found" });
    const company = compRes.rows[0];
    if (!ctx.isOwner && company.assigned_to && company.assigned_to !== ctx.actorUserId) {
      return res.status(404).json({ error: "Company not found" });
    }

    const [contacts, memos, portalUsers, memoRequests] = await Promise.all([
      pool.query(`SELECT id, name, title, email, phone, type FROM crm_contacts WHERE company_id = $1 ORDER BY name`, [id]),
      pool.query(`SELECT id, memo_number, status, issued_at, due_at, total_value, currency,
                         (SELECT COUNT(*)::int FROM memo_items mi WHERE mi.memo_id = m.id) AS item_count
                  FROM memos m WHERE company_id = $1 ORDER BY created_at DESC LIMIT 50`, [id]),
      // Portal users for THIS store. The FE Hero shows the primary one
      // (the most recent active row) directly on the banner.
      pool.query(`SELECT id, name, email, avatar_color,
                         (clerk_user_id IS NULL) AS pending,
                         created_at, last_invited_at
                    FROM team_members
                   WHERE company_id = $1 AND role = 'store_user' AND active = TRUE
                   ORDER BY (clerk_user_id IS NOT NULL) DESC, created_at DESC`, [id]),
      // Memo requests originating from this store's portal users.
      // The most recent first, with a count of items each contains.
      pool.query(`SELECT r.id, r.status, r.message, r.preferred_due_at,
                         r.converted_memo_id, r.decline_reason,
                         r.created_at, r.responded_at,
                         tm.name AS requester_name, tm.email AS requester_email,
                         (SELECT COUNT(*)::int FROM memo_request_items mi WHERE mi.request_id = r.id) AS item_count
                    FROM memo_requests r
               LEFT JOIN team_members tm ON tm.team_owner_id = r.user_id
                                         AND tm.clerk_user_id = r.requested_by
                                         AND tm.active = TRUE
                   WHERE r.company_id = $1 AND r.user_id = $2
                ORDER BY CASE WHEN r.status = 'pending' THEN 0 ELSE 1 END,
                         r.created_at DESC
                   LIMIT 50`, [id, tenantUserId]),
    ]);
    const pendingCount = memoRequests.rows.filter((r) => r.status === 'pending').length;
    res.json({
      ...company,
      contacts: contacts.rows,
      memos: memos.rows,
      portal_users: portalUsers.rows,
      memo_requests: memoRequests.rows,
      pending_requests_count: pendingCount,
    });
  } catch (error) {
    console.error("Error fetching company:", error);
    res.status(500).json({ error: error.message });
  }
});

app.post("/api/crm/companies", async (req, res) => {
  try {
    const ctx = await resolveTeamContext(req);
    if (!ctx.actorUserId) return res.status(400).json({ error: "userId is required" });
    if (!req.body.name) return res.status(400).json({ error: "name is required" });
    const tenantUserId = ctx.tenantUserId;

    // Reps default-assign the new company to themselves so it doesn't
    // disappear from their list the moment they create it.
    const cols = ['user_id'];
    const vals = [tenantUserId];
    let idx = 2;
    const placeholders = ['$1'];
    for (const f of CO_FIELDS) {
      const camel = f.replace(/_([a-z])/g, (_,c) => c.toUpperCase());
      if (req.body[camel] !== undefined) {
        cols.push(f);
        const v = req.body[camel];
        vals.push(CO_JSON_FIELDS.has(f) ? JSON.stringify(v ?? (f === 'tags' ? [] : {})) : v);
        placeholders.push(`$${idx++}`);
      }
    }
    if (!ctx.isOwner && !cols.includes('assigned_to')) {
      cols.push('assigned_to');
      vals.push(ctx.actorUserId);
      placeholders.push(`$${idx++}`);
    }

    const r = await pool.query(
      `INSERT INTO crm_companies (${cols.join(', ')}) VALUES (${placeholders.join(', ')}) RETURNING *`,
      vals
    );
    const company = r.rows[0];

    const { actorId, actorName } = getActor(req);
    logActivity({
      userId: tenantUserId, actorId, actorName,
      entityType: 'company', entityId: String(company.id), action: 'created',
      summary: `Added company "${company.name}"`,
    });

    res.status(201).json(company);
  } catch (error) {
    console.error("Error creating company:", error);
    res.status(500).json({ error: error.message });
  }
});

app.put("/api/crm/companies/:id", async (req, res) => {
  try {
    const { id } = req.params;
    const ctx = await resolveTeamContext(req);
    const fields = [];
    const values = [];
    let idx = 1;

    for (const f of CO_FIELDS) {
      const camel = f.replace(/_([a-z])/g, (_,c) => c.toUpperCase());
      if (req.body[camel] !== undefined) {
        if (f === 'assigned_to' && ctx.actorUserId && !ctx.isOwner) {
          if (req.body[camel] && String(req.body[camel]) !== String(ctx.actorUserId)) {
            return res.status(403).json({ error: 'Reps can only assign records to themselves' });
          }
        }
        fields.push(`${f} = $${idx++}`);
        const v = req.body[camel];
        values.push(CO_JSON_FIELDS.has(f) ? JSON.stringify(v ?? (f === 'tags' ? [] : {})) : v);
      }
    }
    if (!fields.length) return res.status(400).json({ error: 'No updatable fields supplied' });
    fields.push(`updated_at = NOW()`);
    values.push(id);

    const r = await pool.query(
      `UPDATE crm_companies SET ${fields.join(', ')} WHERE id = $${idx} RETURNING *`,
      values
    );
    if (r.rows.length === 0) return res.status(404).json({ error: 'Company not found' });

    const { actorId, actorName } = getActor(req);
    logActivity({
      userId: r.rows[0].user_id, actorId, actorName,
      entityType: 'company', entityId: String(id), action: 'updated',
      summary: `Updated company "${r.rows[0].name}"`,
    });

    res.json(r.rows[0]);
  } catch (error) {
    console.error("Error updating company:", error);
    res.status(500).json({ error: error.message });
  }
});

app.delete("/api/crm/companies/:id", async (req, res) => {
  try {
    const { id } = req.params;
    const before = await pool.query(`SELECT user_id, name FROM crm_companies WHERE id = $1`, [id]);
    // Refuse to delete a company that has open memos — those would
    // silently disappear, hiding inventory we sent out. The user has
    // to close/return those memos first.
    const open = await pool.query(
      `SELECT COUNT(*)::int AS n FROM memos WHERE company_id = $1 AND status IN ('out','partially_returned')`,
      [id]
    );
    if (open.rows[0].n > 0) {
      return res.status(409).json({ error: 'Company has open memos — close them before deleting' });
    }
    await pool.query(`DELETE FROM crm_companies WHERE id = $1`, [id]);
    res.json({ success: true });

    if (before.rows[0]) {
      const { actorId, actorName } = getActor(req);
      logActivity({
        userId: before.rows[0].user_id, actorId, actorName,
        entityType: 'company', entityId: String(id), action: 'deleted',
        summary: `Deleted company "${before.rows[0].name}"`,
      });
    }
  } catch (error) {
    console.error("Error deleting company:", error);
    res.status(500).json({ error: error.message });
  }
});

/* =========================================================
   Memos (consignment) — list, detail, create, update,
   line-items, lifecycle transitions.

   Lifecycle:
     draft → out (issue)
     out   → partially_returned (when at least one item is returned/sold but others still out)
     out / partially_returned → closed (manually OR when all items are returned/sold)
     out / partially_returned → expired (cron / lazy on read when due_at is past)

   Item statuses: out → returned | sold (terminal).
   ========================================================= */

const MEMO_FIELDS = [
  'company_id','contact_id','status','issued_at','due_at','closed_at',
  'total_value','currency','notes','internal_notes','assigned_to',
];

const generateMemoNumber = async (tenantUserId) => {
  // MEMO-YYYY-#### — sequential per workspace per year.
  const year = new Date().getFullYear();
  const r = await pool.query(
    `SELECT memo_number FROM memos
      WHERE user_id = $1 AND memo_number LIKE $2
   ORDER BY id DESC LIMIT 1`,
    [tenantUserId, `MEMO-${year}-%`]
  );
  let next = 1;
  if (r.rows.length) {
    const m = /MEMO-\d{4}-(\d+)$/.exec(r.rows[0].memo_number);
    if (m) next = parseInt(m[1], 10) + 1;
  }
  return `MEMO-${year}-${String(next).padStart(4, '0')}`;
};

const recomputeMemoTotals = async (memoId) => {
  // Sum live (non-returned) memo prices into total_value, and flip the
  // header status when items move out/return/sell. Called after every
  // item-level mutation.
  const itemRes = await pool.query(
    `SELECT status, COALESCE(memo_price,0) AS memo_price
       FROM memo_items WHERE memo_id = $1`,
    [memoId]
  );
  const items = itemRes.rows;
  const total = items.reduce((a, i) => a + Number(i.memo_price || 0), 0);
  const allDone   = items.length > 0 && items.every((i) => i.status !== 'out');
  const someOut   = items.some((i) => i.status === 'out');
  const someClosed = items.some((i) => i.status !== 'out');

  const headerRes = await pool.query(`SELECT status FROM memos WHERE id = $1`, [memoId]);
  if (!headerRes.rows.length) return;
  const cur = headerRes.rows[0].status;

  let nextStatus = cur;
  if (cur === 'out' || cur === 'partially_returned') {
    if (allDone)              nextStatus = 'closed';
    else if (someOut && someClosed) nextStatus = 'partially_returned';
    else if (someOut)         nextStatus = 'out';
  }

  await pool.query(
    `UPDATE memos
        SET total_value = $1,
            status      = $2,
            closed_at   = CASE WHEN $2 = 'closed' AND closed_at IS NULL THEN NOW() ELSE closed_at END,
            updated_at  = NOW()
      WHERE id = $3`,
    [total, nextStatus, memoId]
  );
};

app.get("/api/memos", async (req, res) => {
  try {
    const ctx = await resolveTeamContext(req);
    if (!ctx.actorUserId) return res.status(400).json({ error: "userId is required" });
    const tenantUserId = ctx.tenantUserId;
    const { companyId, status, assignedTo, search } = req.query;

    const where = ['m.user_id = $1'];
    const args = [tenantUserId];
    let idx = 2;
    if (companyId)  { where.push(`m.company_id = $${idx}`); args.push(companyId); idx++; }
    if (status)     { where.push(`m.status = $${idx}`);     args.push(status);    idx++; }
    if (search) {
      where.push(`(m.memo_number ILIKE $${idx} OR c.name ILIKE $${idx})`);
      args.push(`%${search}%`); idx++;
    }
    if (assignedTo === 'unassigned') where.push('m.assigned_to IS NULL');
    else if (assignedTo === 'me')   { where.push(`m.assigned_to = $${idx}`); args.push(ctx.actorUserId); idx++; }
    else if (assignedTo)            { where.push(`m.assigned_to = $${idx}`); args.push(assignedTo);     idx++; }

    if (!ctx.isOwner && ctx.actorUserId) {
      where.push(`(m.assigned_to IS NULL OR m.assigned_to = $${idx})`);
      args.push(ctx.actorUserId); idx++;
    }

    const r = await pool.query(`
      SELECT m.*, c.name AS company_name, c.logo_url AS company_logo,
             ct.name AS contact_name,
             (SELECT COUNT(*)::int FROM memo_items mi WHERE mi.memo_id = m.id) AS item_count,
             (SELECT COUNT(*)::int FROM memo_items mi WHERE mi.memo_id = m.id AND mi.status = 'out') AS items_out,
             -- Signature presence flags powering the Documents Hub
             -- without paying for a full signatures join per row. Each
             -- flag is a cheap EXISTS lookup against the unique slot
             -- index on (memo_id, event, signer_role).
             EXISTS(SELECT 1 FROM memo_signatures s WHERE s.memo_id = m.id AND s.event='issue' AND s.signer_role='supplier') AS has_sig_issue_supplier,
             EXISTS(SELECT 1 FROM memo_signatures s WHERE s.memo_id = m.id AND s.event='issue' AND s.signer_role='store')    AS has_sig_issue_store,
             EXISTS(SELECT 1 FROM memo_signatures s WHERE s.memo_id = m.id AND s.event='close' AND s.signer_role='supplier') AS has_sig_close_supplier,
             EXISTS(SELECT 1 FROM memo_signatures s WHERE s.memo_id = m.id AND s.event='close' AND s.signer_role='store')    AS has_sig_close_store,
             (SELECT COUNT(*)::int FROM memo_signatures s WHERE s.memo_id = m.id) AS signature_count
        FROM memos m
        JOIN crm_companies c ON c.id = m.company_id
   LEFT JOIN crm_contacts ct ON ct.id = m.contact_id
       WHERE ${where.join(' AND ')}
    ORDER BY m.created_at DESC
    `, args);

    // Lazy-mark expired: any 'out' memo whose due_at is in the past.
    // We don't update the DB here (cron does that); the FE can show the
    // chip as "expired" if status is out/partially_returned and due_at < today.
    res.json(r.rows);
  } catch (error) {
    console.error("Error fetching memos:", error);
    res.status(500).json({ error: error.message });
  }
});

/* Returns SKUs that are currently on an active memo so the FE
 * inventory page can flag them as "On Memo" without per-row queries.
 * Shape: { byStoneSku: ['SKU1','SKU2'], byJewelrySku: ['MN1'] }.
 *
 * IMPORTANT: this static path MUST be registered BEFORE the
 * /api/memos/:id wildcard below, otherwise Express matches
 * "active-skus" as a memo id and the SQL fails with
 * "invalid input syntax for type integer".
 */
app.get("/api/memos/active-skus", async (req, res) => {
  try {
    const ctx = await resolveTeamContext(req);
    if (!ctx.actorUserId) return res.status(400).json({ error: "userId is required" });
    const tenantUserId = ctx.tenantUserId;
    const r = await pool.query(`
      SELECT mi.item_type, mi.item_sku, mi.memo_id, m.memo_number, m.company_id, c.name AS company_name
        FROM memo_items mi
        JOIN memos m ON m.id = mi.memo_id
        JOIN crm_companies c ON c.id = m.company_id
       WHERE mi.status = 'out'
         AND m.status IN ('out','partially_returned')
         AND m.user_id = $1
    `, [tenantUserId]);
    const byStoneSku = {};
    const byJewelrySku = {};
    for (const row of r.rows) {
      const target = row.item_type === 'jewelry' ? byJewelrySku : byStoneSku;
      target[row.item_sku] = {
        memoId: row.memo_id,
        memoNumber: row.memo_number,
        companyId: row.company_id,
        companyName: row.company_name,
      };
    }
    res.json({ byStoneSku, byJewelrySku });
  } catch (error) {
    console.error("Error fetching active memo SKUs:", error);
    res.status(500).json({ error: error.message });
  }
});

app.get("/api/memos/:id", async (req, res) => {
  try {
    const { id } = req.params;
    const ctx = await resolveTeamContext(req);
    if (!ctx.actorUserId) return res.status(400).json({ error: "userId is required" });
    const tenantUserId = ctx.tenantUserId;

    // Joining team_members twice — once for the issuer (created_by) and
    // once for the assignee (assigned_to) — so the FE can show real
    // names instead of opaque clerk_user_id strings.
    const memoRes = await pool.query(`
      SELECT m.*, c.name AS company_name, c.email AS company_email,
             c.phone AS company_phone, c.address AS company_address,
             c.city AS company_city, c.country AS company_country,
             c.logo_url AS company_logo, c.website AS company_website,
             c.tax_id AS company_tax_id, c.payment_terms AS company_payment_terms,
             ct.name  AS contact_name, ct.email AS contact_email, ct.phone AS contact_phone,
             tm_creator.name  AS created_by_name,  tm_creator.email  AS created_by_email,
             tm_assignee.name AS assigned_to_name, tm_assignee.email AS assigned_to_email,
             tm_owner.name    AS owner_name,       tm_owner.email    AS owner_email
        FROM memos m
        JOIN crm_companies c ON c.id = m.company_id
   LEFT JOIN crm_contacts ct ON ct.id = m.contact_id
   LEFT JOIN team_members tm_creator
          ON tm_creator.team_owner_id = m.user_id
         AND tm_creator.clerk_user_id = m.created_by
         AND tm_creator.active = TRUE
   LEFT JOIN team_members tm_assignee
          ON tm_assignee.team_owner_id = m.user_id
         AND tm_assignee.clerk_user_id = m.assigned_to
         AND tm_assignee.active = TRUE
   LEFT JOIN team_members tm_owner
          ON tm_owner.team_owner_id = m.user_id
         AND tm_owner.role = 'owner'
         AND tm_owner.active = TRUE
       WHERE m.id = $1 AND m.user_id = $2
    `, [id, tenantUserId]);
    if (memoRes.rows.length === 0) return res.status(404).json({ error: "Memo not found" });
    const memo = memoRes.rows[0];
    if (!ctx.isOwner && memo.assigned_to && memo.assigned_to !== ctx.actorUserId) {
      return res.status(404).json({ error: "Memo not found" });
    }

    const items = await pool.query(
      `SELECT * FROM memo_items WHERE memo_id = $1 ORDER BY created_at ASC`,
      [id]
    );
    const sigs = await pool.query(
      `SELECT id, event, signer_role, signer_clerk_id, signer_name, signer_email,
              signature_url, consent_text, integrity_hash, ip_address, user_agent,
              pdf_url, token_id, signed_at
         FROM memo_signatures
        WHERE memo_id = $1
     ORDER BY signed_at ASC, id ASC`,
      [id]
    );
    res.json({ ...memo, items: items.rows, signatures: sigs.rows });
  } catch (error) {
    console.error("Error fetching memo:", error);
    res.status(500).json({ error: error.message });
  }
});

/* Activity feed for a single memo. Powers the timeline card on the
 * detail page. Limited to a sensible window so the feed renders fast
 * even on memos with hundreds of item-level transitions. */
app.get("/api/memos/:id/activity", async (req, res) => {
  try {
    const { id } = req.params;
    const ctx = await resolveTeamContext(req);
    if (!ctx.actorUserId) return res.status(400).json({ error: "userId is required" });
    const tenantUserId = ctx.tenantUserId;

    // Confirm the caller may even see this memo before exposing its
    // activity feed (otherwise reps could probe other people's memos).
    const memoRes = await pool.query(
      `SELECT id, assigned_to FROM memos WHERE id = $1 AND user_id = $2`,
      [id, tenantUserId]
    );
    if (memoRes.rows.length === 0) return res.status(404).json({ error: "Memo not found" });
    if (!ctx.isOwner && memoRes.rows[0].assigned_to && memoRes.rows[0].assigned_to !== ctx.actorUserId) {
      return res.status(404).json({ error: "Memo not found" });
    }

    const r = await pool.query(`
      SELECT a.*, COALESCE(NULLIF(tm.name, ''), a.actor_name) AS resolved_actor_name
        FROM activity_log a
   LEFT JOIN team_members tm
          ON tm.team_owner_id = a.user_id
         AND tm.clerk_user_id = a.actor_id
         AND tm.active = TRUE
       WHERE a.user_id = $1
         AND a.entity_type = 'memo'
         AND a.entity_id = $2
    ORDER BY a.occurred_at DESC
       LIMIT 200
    `, [tenantUserId, String(id)]);
    res.json(r.rows);
  } catch (error) {
    console.error("Error fetching memo activity:", error);
    res.status(500).json({ error: error.message });
  }
});

app.post("/api/memos", async (req, res) => {
  try {
    const ctx = await resolveTeamContext(req);
    if (!ctx.actorUserId) return res.status(400).json({ error: "userId is required" });
    if (!req.body.companyId) return res.status(400).json({ error: "companyId is required" });
    const tenantUserId = ctx.tenantUserId;

    // Pull defaults off the company so reps don't have to retype them.
    const company = await pool.query(
      `SELECT default_memo_days FROM crm_companies WHERE id = $1 AND user_id = $2`,
      [req.body.companyId, tenantUserId]
    );
    if (company.rows.length === 0) return res.status(404).json({ error: "Company not found" });
    const defaultDays = company.rows[0].default_memo_days || 30;

    const memoNumber = await generateMemoNumber(tenantUserId);
    const dueAt = req.body.dueAt
      ? new Date(req.body.dueAt)
      : new Date(Date.now() + defaultDays * 86400 * 1000);

    const r = await pool.query(
      `INSERT INTO memos (
         user_id, memo_number, company_id, contact_id, status, due_at,
         currency, notes, internal_notes, created_by,
         assigned_to
       ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) RETURNING *`,
      [
        tenantUserId, memoNumber, req.body.companyId,
        req.body.contactId || null, 'draft', dueAt,
        req.body.currency || 'USD', req.body.notes || null,
        req.body.internalNotes || null, ctx.actorUserId,
        ctx.isOwner ? (req.body.assignedTo || null) : ctx.actorUserId,
      ]
    );
    const memo = r.rows[0];

    // If the caller supplied items in the same request, insert them too
    // — saves a round-trip from the FE wizard.
    if (Array.isArray(req.body.items) && req.body.items.length) {
      for (const it of req.body.items) {
        await pool.query(
          `INSERT INTO memo_items
             (memo_id, item_type, item_sku, item_id, snapshot, memo_price, quantity, notes)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
          [
            memo.id, it.itemType || 'stone', it.itemSku, it.itemId || null,
            JSON.stringify(it.snapshot || {}),
            it.memoPrice != null ? Number(it.memoPrice) : null,
            it.quantity || 1, it.notes || null,
          ]
        );
      }
      await recomputeMemoTotals(memo.id);
    }

    const { actorId, actorName } = getActor(req);
    logActivity({
      userId: tenantUserId, actorId, actorName,
      entityType: 'memo', entityId: String(memo.id), action: 'created',
      summary: `Created memo ${memoNumber}`,
      related: [{ type: 'company', id: memo.company_id }],
    });

    const full = await pool.query(`SELECT * FROM memos WHERE id = $1`, [memo.id]);
    const items = await pool.query(`SELECT * FROM memo_items WHERE memo_id = $1`, [memo.id]);
    res.status(201).json({ ...full.rows[0], items: items.rows });
  } catch (error) {
    console.error("Error creating memo:", error);
    res.status(500).json({ error: error.message });
  }
});

app.put("/api/memos/:id", async (req, res) => {
  try {
    const { id } = req.params;
    const ctx = await resolveTeamContext(req);
    const fields = [];
    const values = [];
    let idx = 1;

    for (const f of MEMO_FIELDS) {
      const camel = f.replace(/_([a-z])/g, (_,c) => c.toUpperCase());
      if (req.body[camel] !== undefined) {
        if (f === 'assigned_to' && ctx.actorUserId && !ctx.isOwner) {
          if (req.body[camel] && String(req.body[camel]) !== String(ctx.actorUserId)) {
            return res.status(403).json({ error: 'Reps can only assign records to themselves' });
          }
        }
        fields.push(`${f} = $${idx++}`);
        values.push(req.body[camel]);
      }
    }
    if (!fields.length) return res.status(400).json({ error: 'No updatable fields supplied' });
    fields.push(`updated_at = NOW()`);
    values.push(id);

    const r = await pool.query(
      `UPDATE memos SET ${fields.join(', ')} WHERE id = $${idx} RETURNING *`,
      values
    );
    if (r.rows.length === 0) return res.status(404).json({ error: 'Memo not found' });
    res.json(r.rows[0]);
  } catch (error) {
    console.error("Error updating memo:", error);
    res.status(500).json({ error: error.message });
  }
});

app.delete("/api/memos/:id", async (req, res) => {
  try {
    const { id } = req.params;
    // Refuse to delete a memo that's already been issued — those have
    // physical items at the customer. Only drafts can be removed.
    const r = await pool.query(`SELECT user_id, status, memo_number FROM memos WHERE id = $1`, [id]);
    if (r.rows.length === 0) return res.status(404).json({ error: 'Memo not found' });
    if (r.rows[0].status !== 'draft') {
      return res.status(409).json({ error: 'Only draft memos can be deleted — close it instead' });
    }
    await pool.query(`DELETE FROM memos WHERE id = $1`, [id]);
    res.json({ success: true });

    const { actorId, actorName } = getActor(req);
    logActivity({
      userId: r.rows[0].user_id, actorId, actorName,
      entityType: 'memo', entityId: String(id), action: 'deleted',
      summary: `Deleted draft memo ${r.rows[0].memo_number}`,
    });
  } catch (error) {
    console.error("Error deleting memo:", error);
    res.status(500).json({ error: error.message });
  }
});

/* ----- memo line items ------------------------------------------ */

app.post("/api/memos/:id/items", async (req, res) => {
  try {
    const { id } = req.params;
    const { items } = req.body;
    if (!Array.isArray(items) || !items.length) {
      return res.status(400).json({ error: 'items array is required' });
    }

    // Block adding items that are already on another OPEN memo — same
    // physical stone/jewelry can't sit at two stores at once.
    for (const it of items) {
      if (!it.itemSku) continue;
      const conflict = await pool.query(
        `SELECT m.id, m.memo_number
           FROM memo_items mi
           JOIN memos m ON m.id = mi.memo_id
          WHERE mi.item_sku = $1
            AND mi.status = 'out'
            AND m.id <> $2
            AND m.status IN ('out','partially_returned','draft')
          LIMIT 1`,
        [it.itemSku, id]
      );
      if (conflict.rows.length) {
        return res.status(409).json({
          error: `${it.itemSku} is already on memo ${conflict.rows[0].memo_number}`,
        });
      }
    }

    const inserted = [];
    for (const it of items) {
      const r = await pool.query(
        `INSERT INTO memo_items
           (memo_id, item_type, item_sku, item_id, snapshot, memo_price, quantity, notes)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8) RETURNING *`,
        [
          id, it.itemType || 'stone', it.itemSku, it.itemId || null,
          JSON.stringify(it.snapshot || {}),
          it.memoPrice != null ? Number(it.memoPrice) : null,
          it.quantity || 1, it.notes || null,
        ]
      );
      inserted.push(r.rows[0]);
    }
    await recomputeMemoTotals(id);
    res.status(201).json(inserted);
  } catch (error) {
    console.error("Error adding memo items:", error);
    res.status(500).json({ error: error.message });
  }
});

app.put("/api/memos/:id/items/:itemId", async (req, res) => {
  try {
    const { id, itemId } = req.params;

    // Operational gate for status flips (sold / returned). Price /
    // quantity / notes edits remain allowed without a signature so the
    // supplier can still tweak fields on a draft or correct typos on
    // an issued memo — only marking items as sold/returned is gated,
    // because that is what materially changes the memo's audit trail.
    if (req.body.status === 'sold' || req.body.status === 'returned') {
      try { await requireSupplierIssuanceSignature(id); }
      catch (e) { return sendSignatureError(res, e, 'item status gate:'); }
    }

    const allowed = ['memo_price','quantity','notes','status'];
    const fields = [];
    const values = [];
    let idx = 1;
    for (const f of allowed) {
      const camel = f.replace(/_([a-z])/g, (_,c) => c.toUpperCase());
      if (req.body[camel] !== undefined) {
        fields.push(`${f} = $${idx++}`);
        values.push(req.body[camel]);
      }
    }
    if (req.body.status === 'returned') {
      fields.push(`returned_at = NOW()`);
    } else if (req.body.status === 'sold') {
      fields.push(`sold_at = NOW()`);
    }
    if (!fields.length) return res.status(400).json({ error: 'No updatable fields supplied' });
    fields.push(`updated_at = NOW()`);
    values.push(itemId, id);

    const r = await pool.query(
      `UPDATE memo_items SET ${fields.join(', ')}
        WHERE id = $${idx++} AND memo_id = $${idx} RETURNING *`,
      values
    );
    if (r.rows.length === 0) return res.status(404).json({ error: 'Memo item not found' });
    await recomputeMemoTotals(id);

    if (req.body.status === 'sold' || req.body.status === 'returned') {
      const memoRes = await pool.query(`SELECT user_id, memo_number FROM memos WHERE id = $1`, [id]);
      if (memoRes.rows[0]) {
        const { actorId, actorName } = getActor(req);
        logActivity({
          userId: memoRes.rows[0].user_id, actorId, actorName,
          entityType: 'memo', entityId: String(id),
          action: req.body.status === 'sold' ? 'item_sold' : 'item_returned',
          summary: `${req.body.status === 'sold' ? 'Sold' : 'Returned'} ${r.rows[0].item_sku} from ${memoRes.rows[0].memo_number}`,
        });
      }
    }
    res.json(r.rows[0]);
  } catch (error) {
    console.error("Error updating memo item:", error);
    res.status(500).json({ error: error.message });
  }
});

app.delete("/api/memos/:id/items/:itemId", async (req, res) => {
  try {
    const { id, itemId } = req.params;
    // Only allow removing items from drafts. Once issued, you must
    // mark the item as returned/sold instead — that preserves history.
    const memoRes = await pool.query(`SELECT status FROM memos WHERE id = $1`, [id]);
    if (memoRes.rows.length === 0) return res.status(404).json({ error: 'Memo not found' });
    if (memoRes.rows[0].status !== 'draft') {
      return res.status(409).json({ error: 'Only draft memo items can be removed' });
    }
    await pool.query(`DELETE FROM memo_items WHERE id = $1 AND memo_id = $2`, [itemId, id]);
    await recomputeMemoTotals(id);
    res.json({ success: true });
  } catch (error) {
    console.error("Error deleting memo item:", error);
    res.status(500).json({ error: error.message });
  }
});

/* ----- memo lifecycle transitions ------------------------------- */

app.post("/api/memos/:id/issue", async (req, res) => {
  try {
    const { id } = req.params;
    const ctx = await resolveTeamContext(req);
    const memoRes = await pool.query(`SELECT * FROM memos WHERE id = $1`, [id]);
    if (memoRes.rows.length === 0) return res.status(404).json({ error: 'Memo not found' });
    const memo = memoRes.rows[0];
    if (memo.status !== 'draft') return res.status(409).json({ error: 'Only drafts can be issued' });

    const itemCount = await pool.query(`SELECT COUNT(*)::int AS n FROM memo_items WHERE memo_id = $1`, [id]);
    if (itemCount.rows[0].n === 0) return res.status(400).json({ error: 'Cannot issue an empty memo' });

    // Hard gate: a memo cannot transition draft→out until the supplier
    // side has signed event='issue'. The FE flow is "Sign & Issue"
    // (POST /signatures with role=supplier, then POST /issue); on retry,
    // the existing signature satisfies this gate without re-signing.
    const sigCheck = await pool.query(
      `SELECT id FROM memo_signatures
        WHERE memo_id = $1 AND event = 'issue' AND signer_role = 'supplier'
        LIMIT 1`,
      [id]
    );
    if (sigCheck.rows.length === 0) {
      return res.status(409).json({
        error: 'Supplier signature required to issue this memo',
        code: 'signature_required',
        missing: { event: 'issue', signerRole: 'supplier' },
      });
    }

    const r = await pool.query(
      `UPDATE memos SET status = 'out', issued_at = NOW(), updated_at = NOW()
        WHERE id = $1 RETURNING *`,
      [id]
    );

    const { actorId, actorName } = getActor(req);
    logActivity({
      userId: memo.user_id,
      actorId: actorId || ctx.actorUserId, actorName,
      entityType: 'memo', entityId: String(id), action: 'issued',
      summary: `Issued memo ${memo.memo_number}`,
      related: [{ type: 'company', id: memo.company_id }],
    });

    res.json(r.rows[0]);
  } catch (error) {
    console.error("Error issuing memo:", error);
    res.status(500).json({ error: error.message });
  }
});

app.post("/api/memos/:id/close", async (req, res) => {
  try {
    const { id } = req.params;
    const memoRes = await pool.query(`SELECT * FROM memos WHERE id = $1`, [id]);
    if (memoRes.rows.length === 0) return res.status(404).json({ error: 'Memo not found' });
    const memo = memoRes.rows[0];
    if (memo.status === 'closed') return res.json(memo);

    // Operational hard-gate #1. A memo without supplier issuance
    // signature can't be closed because legally it was never properly
    // issued — closing it would create an audit-trail gap.
    try { await requireSupplierIssuanceSignature(id); }
    catch (e) { return sendSignatureError(res, e, 'close gate (issuance):'); }

    // Operational hard-gate #2. Closing a memo is itself a legally
    // significant transition (it freezes the accounting), so we
    // require a supplier `event='close'` signature too. The FE flow
    // is "Sign & Close": POST /signatures with role=supplier event=close,
    // then POST /close. On retry, the existing signature satisfies
    // this gate without re-signing.
    const closeSig = await pool.query(
      `SELECT id FROM memo_signatures
        WHERE memo_id = $1 AND event = 'close' AND signer_role = 'supplier'
        LIMIT 1`,
      [id]
    );
    if (closeSig.rows.length === 0) {
      return res.status(409).json({
        error: 'Supplier signature required to close this memo',
        code: 'signature_required',
        missing: { event: 'close', signerRole: 'supplier' },
      });
    }

    // Force-close: any items still 'out' are flipped to 'returned'
    // (the user is saying "the customer brought everything back").
    await pool.query(
      `UPDATE memo_items SET status = 'returned', returned_at = NOW(), updated_at = NOW()
        WHERE memo_id = $1 AND status = 'out'`,
      [id]
    );
    const r = await pool.query(
      `UPDATE memos SET status = 'closed', closed_at = NOW(), updated_at = NOW()
        WHERE id = $1 RETURNING *`,
      [id]
    );

    const { actorId, actorName } = getActor(req);
    logActivity({
      userId: memo.user_id, actorId, actorName,
      entityType: 'memo', entityId: String(id), action: 'closed',
      summary: `Closed memo ${memo.memo_number}`,
    });

    res.json(r.rows[0]);
  } catch (error) {
    console.error("Error closing memo:", error);
    res.status(500).json({ error: error.message });
  }
});

/* =========================================================
   Memo signatures (Sprint: digital signature workflow)
   ---------------------------------------------------------
   POST /api/memos/:id/signatures        — authenticated (header ctx)
   POST /api/memos/:id/signature-tokens  — supplier creates opaque link
   GET  /api/sign/:token                 — public (preview before signing)
   POST /api/sign/:token                 — public (submit signature)

   The core insert logic — validate shape, freeze snapshot, upload PNG,
   compute SHA-256, write row — is shared across the authenticated and
   public paths via `createMemoSignatureRow`. The auth/permission checks
   live in each endpoint because they differ meaningfully (header ctx vs
   token). The UNIQUE(memo_id, event, signer_role) index plus an explicit
   dupe pre-check inside the helper guarantees at most one signature per
   slot regardless of which path was used.
   ========================================================= */

/* Internal helper. Throws errors carrying .status / .code so the
 * surrounding endpoint can map them to HTTP responses uniformly. */
async function createMemoSignatureRow({
  memo,
  event,
  signerRole,
  signerName,
  signerEmail = null,
  signerClerkId = null,
  signatureDataUrl,
  consentText,
  ipAddress = null,
  userAgent = null,
  tokenId = null,
}) {
  const fail = (status, code, message) => {
    const e = new Error(message);
    e.status = status;
    e.code = code;
    throw e;
  };

  if (event !== 'issue' && event !== 'close') {
    fail(400, 'bad_event', "event must be 'issue' or 'close'");
  }
  if (signerRole !== 'supplier' && signerRole !== 'store') {
    fail(400, 'bad_role', "signerRole must be 'supplier' or 'store'");
  }
  const trimmedName = String(signerName || '').trim();
  if (!trimmedName) fail(400, 'missing_name', 'signerName is required');

  if (!signatureDataUrl || !/^data:image\/png;base64,/.test(signatureDataUrl)) {
    fail(400, 'bad_image', 'signatureDataUrl must be a PNG data URL');
  }
  const trimmedConsent = String(consentText || '').trim();
  if (!trimmedConsent) fail(400, 'missing_consent', 'consentText is required');

  // Pre-check for duplicate so we can surface a clean 409 instead of a
  // raw constraint violation when both flows race.
  const dupe = await pool.query(
    `SELECT id FROM memo_signatures
      WHERE memo_id = $1 AND event = $2 AND signer_role = $3
      LIMIT 1`,
    [memo.id, event, signerRole]
  );
  if (dupe.rows.length) {
    fail(409, 'signature_exists', 'Signature already exists for this event and role');
  }

  if (!blobPut) fail(503, 'blob_missing', '@vercel/blob not installed on backend');
  if (!process.env.BLOB_READ_WRITE_TOKEN) {
    fail(503, 'blob_token_missing', 'BLOB_READ_WRITE_TOKEN is not set');
  }

  // Freeze the memo + items at signing time.
  const itemsRes = await pool.query(
    `SELECT * FROM memo_items WHERE memo_id = $1 ORDER BY created_at ASC`,
    [memo.id]
  );
  const snapshot = {
    memo,
    items: itemsRes.rows,
    capturedAt: new Date().toISOString(),
  };

  const base64 = signatureDataUrl.replace(/^data:image\/png;base64,/, '');
  const buffer = Buffer.from(base64, 'base64');
  const pathname = `memo-signatures/memo-${memo.id}-${event}-${signerRole}-${Date.now()}.png`;
  const blob = await blobPut(pathname, buffer, {
    access: 'public',
    contentType: 'image/png',
    addRandomSuffix: true,
  });

  const signedAt = new Date();
  const integrityHash = crypto
    .createHash('sha256')
    .update(JSON.stringify({
      snapshot,
      signatureUrl: blob.url,
      signerName: trimmedName,
      signerRole,
      event,
      signedAt: signedAt.toISOString(),
    }))
    .digest('hex');

  const ins = await pool.query(`
    INSERT INTO memo_signatures
      (memo_id, user_id, event, signer_role, signer_clerk_id,
       signer_name, signer_email, signature_url, consent_text,
       memo_snapshot, integrity_hash, ip_address, user_agent, token_id, signed_at)
    VALUES
      ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb, $11, $12, $13, $14, $15)
    RETURNING id, memo_id, event, signer_role, signer_clerk_id,
              signer_name, signer_email, signature_url, consent_text,
              integrity_hash, ip_address, user_agent, pdf_url, token_id, signed_at
  `, [
    memo.id, memo.user_id, event, signerRole,
    signerClerkId, trimmedName, signerEmail,
    blob.url, trimmedConsent,
    JSON.stringify(snapshot), integrityHash, ipAddress, userAgent, tokenId,
    signedAt,
  ]);

  const row = ins.rows[0];

  // Fire-and-forget audit notification to both parties. We do NOT
  // await this on purpose — the signature has already been persisted
  // above and we don't want a transient Resend / SMTP problem to
  // hold up the HTTP response or roll the row back.
  sendMemoSignatureEmail({ memoId: memo.id, signature: row })
    .catch((e) => console.warn('sendMemoSignatureEmail (fire-forget):', e.message));

  return row;
}

/* Fire-and-forget signature notification email. Sends a separate
 * message to the supplier and to the store so each side gets a
 * "View memo" link pointing into their own UI. Reads recipient
 * addresses by joining the memo row against team_members (supplier
 * side) and crm_companies (store side). All failures are logged but
 * NEVER thrown — the signature itself has already been persisted at
 * this point and we don't want a Resend outage to roll it back.
 *
 * Honors RESEND_API_KEY / RESEND_FROM_EMAIL like other mailers in
 * this file. Silently no-ops if Resend isn't configured. */
async function sendMemoSignatureEmail({ memoId, signature }) {
  try {
    if (!process.env.RESEND_API_KEY) return;

    // Resolve both sides' email addresses + display name.
    const r = await pool.query(
      `SELECT m.id, m.memo_number, m.user_id, m.company_id,
              m.issued_at, m.due_at, m.closed_at, m.status,
              c.name AS company_name, c.email AS company_email,
              tm_owner.name AS owner_name, tm_owner.email AS owner_email
         FROM memos m
         JOIN crm_companies c ON c.id = m.company_id
    LEFT JOIN team_members tm_owner
           ON tm_owner.team_owner_id = m.user_id
          AND tm_owner.clerk_user_id  = m.user_id
          AND tm_owner.active = TRUE
        WHERE m.id = $1
        LIMIT 1`,
      [memoId]
    );
    if (!r.rows.length) return;
    const memo = r.rows[0];

    const fromEmail  = process.env.RESEND_FROM_EMAIL || 'onboarding@resend.dev';
    const senderName = (memo.owner_name || 'GEMS DNA').replace(/[\r\n<>]/g, '');
    const fromHeader = `${senderName} <${fromEmail}>`;

    const eventLabel = signature.event === 'issue' ? 'issued' : 'closed';
    const roleLabel  = signature.signer_role === 'supplier' ? 'Supplier' : 'Store';
    const subject    = `Memo ${memo.memo_number} · ${eventLabel} · signed by ${roleLabel.toLowerCase()} (${signature.signer_name})`;
    const baseUrl    = FRONTEND_URL.replace(/\/$/, '');
    const supplierLink = `${baseUrl}/crm/memos/${memo.id}`;
    const portalLink   = `${baseUrl}/store-portal/memos/${memo.id}`;

    // Render once with a placeholder for the "View memo" CTA so we
    // can swap the link per-recipient without duplicating the whole
    // template.
    const renderHtml = (ctaUrl, recipientLabel) => `<!doctype html>
<html><body style="font-family:-apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif;background:#f5f5f4;margin:0;padding:24px;color:#1c1917">
  <div style="max-width:560px;margin:0 auto;background:#ffffff;border:1px solid #e7e5e4;border-radius:12px;overflow:hidden">
    <div style="padding:24px;border-bottom:1px solid #f5f5f4">
      <div style="font-size:10px;letter-spacing:2px;color:#a8a29e;text-transform:uppercase;font-weight:700">Electronic signature recorded</div>
      <div style="font-size:22px;font-weight:700;margin-top:6px">Memo ${escapeHtml(memo.memo_number)}</div>
      <div style="font-size:13px;color:#57534e;margin-top:2px">
        ${escapeHtml(memo.owner_name || 'Supplier')} &mdash; ${escapeHtml(memo.company_name || 'Store')}
      </div>
    </div>
    <div style="padding:24px">
      <p style="margin:0 0 12px;font-size:14px;color:#292524">
        <strong>${escapeHtml(signature.signer_name)}</strong>
        signed the <strong>${eventLabel}</strong> of this memo as the <strong>${roleLabel.toLowerCase()}</strong>.
      </p>
      ${signature.signature_url ? `
        <div style="margin:18px 0;border:1px solid #e7e5e4;border-radius:8px;padding:8px;background:#fafaf9;text-align:center">
          <img src="${escapeAttr(signature.signature_url)}" alt="signature" style="max-width:320px;max-height:120px"/>
        </div>` : ''}
      <table style="font-size:12px;color:#57534e;line-height:1.6;border-collapse:collapse">
        <tr><td style="padding-right:12px;color:#a8a29e">Signed&nbsp;at</td><td>${escapeHtml(new Date(signature.signed_at).toLocaleString('en-GB'))}</td></tr>
        ${signature.signer_email ? `<tr><td style="padding-right:12px;color:#a8a29e">Email</td><td>${escapeHtml(signature.signer_email)}</td></tr>` : ''}
        ${signature.ip_address ? `<tr><td style="padding-right:12px;color:#a8a29e">IP</td><td>${escapeHtml(signature.ip_address)}</td></tr>` : ''}
        ${signature.integrity_hash ? `<tr><td style="padding-right:12px;color:#a8a29e">Hash</td><td style="font-family:ui-monospace,Menlo,monospace;font-size:11px">${escapeHtml(String(signature.integrity_hash).slice(0,16))}&hellip;${escapeHtml(String(signature.integrity_hash).slice(-6))}</td></tr>` : ''}
      </table>
      <div style="margin:24px 0 8px">
        <a href="${escapeAttr(ctaUrl)}" style="display:inline-block;background:#1c1917;color:#ffffff;text-decoration:none;padding:10px 18px;border-radius:8px;font-size:13px;font-weight:600">
          View memo &amp; download PDF
        </a>
      </div>
      <p style="font-size:11px;color:#a8a29e;margin:18px 0 0;line-height:1.5">
        This is an automated audit notification. The signature above is bound to the memo's contents at signing time via a SHA-256 integrity hash. You are receiving this as the ${escapeHtml(recipientLabel)} of memo ${escapeHtml(memo.memo_number)}.
      </p>
    </div>
  </div>
</body></html>`;

    const textVersion = `${signature.signer_name} signed the ${eventLabel} of memo ${memo.memo_number} as the ${roleLabel.toLowerCase()}. Signed at ${new Date(signature.signed_at).toLocaleString('en-GB')}. View: `;

    const recipients = [];
    if (isDeliverableEmail(memo.owner_email))   recipients.push({ to: memo.owner_email,   label: 'supplier', link: supplierLink });
    if (isDeliverableEmail(memo.company_email)) recipients.push({ to: memo.company_email, label: 'recipient store', link: portalLink });

    await Promise.all(recipients.map(async (rcpt) => {
      try {
        const html = renderHtml(rcpt.link, rcpt.label);
        const text = textVersion + rcpt.link;
        const resp = await fetch('https://api.resend.com/emails', {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            Authorization: `Bearer ${process.env.RESEND_API_KEY}`,
          },
          body: JSON.stringify({
            from: fromHeader,
            to: [rcpt.to],
            subject,
            html,
            text,
          }),
        });
        if (!resp.ok) {
          const t = await resp.text().catch(() => '');
          console.warn(`[memo signature email -> ${rcpt.to}]`, t.slice(0, 200) || resp.status);
        }
      } catch (e) {
        console.warn(`[memo signature email -> ${rcpt.to}]`, e.message);
      }
    }));
  } catch (e) {
    console.warn('sendMemoSignatureEmail outer:', e.message);
  }
}

/* Tiny HTML escaper used by signature notification emails. We render
 * the template with literal user data (signer name, memo number, etc)
 * and need to keep it injection-safe without pulling in a full
 * sanitizer dep. */
function escapeHtml(s) {
  return String(s == null ? '' : s)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}
function escapeAttr(s) { return escapeHtml(s); }

/* Map a thrown status/code error back onto an HTTP response. */
function sendSignatureError(res, e, contextMsg) {
  if (e && e.status) {
    return res.status(e.status).json({ error: e.message, code: e.code || null });
  }
  console.error(contextMsg || 'Signature error:', e);
  return res.status(500).json({ error: e?.message || 'Internal error' });
}

/* Global hard-gate. Throws a 409 if the supplier hasn't signed the
 * issuance for this memo yet. We use this to block downstream
 * mutations (close, approve store request, decline store request,
 * portal store request) because all of them are operational actions
 * on a memo that legally hasn't been issued.
 *
 * The frontend mirrors this check to disable buttons and show a banner,
 * but the BE remains the source of truth — any client that bypasses
 * the UI still hits this gate. */
async function requireSupplierIssuanceSignature(memoId) {
  const r = await pool.query(
    `SELECT id FROM memo_signatures
      WHERE memo_id = $1 AND event = 'issue' AND signer_role = 'supplier'
      LIMIT 1`,
    [memoId]
  );
  if (r.rows.length === 0) {
    const e = new Error(
      'Supplier issuance signature is required before this action can be performed.'
    );
    e.status = 409;
    e.code = 'signature_required';
    e.missing = { event: 'issue', signerRole: 'supplier' };
    throw e;
  }
}

app.post("/api/memos/:id/signatures", async (req, res) => {
  try {
    const { id } = req.params;
    const ctx = await resolveTeamContext(req);
    if (!ctx.actorUserId) return res.status(400).json({ error: 'userId is required' });

    const { event, signerRole, signerName, signatureDataUrl, consentText } = req.body || {};

    const memoRes = await pool.query(`SELECT * FROM memos WHERE id = $1`, [id]);
    if (memoRes.rows.length === 0) return res.status(404).json({ error: 'Memo not found' });
    const memo = memoRes.rows[0];

    // Authorization differs by signerRole. The helper validates shape
    // but knows nothing about the caller.
    if (signerRole === 'store') {
      if (ctx.role !== 'store_user') {
        return res.status(403).json({ error: "Only store users can sign as 'store'" });
      }
      if (Number(ctx.companyId) !== Number(memo.company_id)) {
        return res.status(403).json({ error: 'Not authorized for this memo' });
      }
    } else if (signerRole === 'supplier') {
      if (memo.user_id !== ctx.tenantUserId) {
        return res.status(403).json({ error: 'Not authorized for this memo' });
      }
      if (!ctx.isOwner && memo.assigned_to && memo.assigned_to !== ctx.actorUserId) {
        return res.status(403).json({ error: 'Not authorized for this memo' });
      }
    }
    // bad signerRole falls through to the helper's own shape check.

    // Best-effort email from team_members so the row displays nicely.
    let resolvedEmail = null;
    try {
      const memberRes = await pool.query(
        `SELECT email FROM team_members WHERE clerk_user_id = $1 LIMIT 1`,
        [ctx.actorUserId]
      );
      resolvedEmail = memberRes.rows[0]?.email || null;
    } catch (_) { /* non-fatal */ }

    const ipAddress = (
      req.headers['x-forwarded-for'] ||
      req.socket?.remoteAddress ||
      ''
    ).toString().split(',')[0].trim() || null;
    const userAgent = req.headers['user-agent'] || null;

    const row = await createMemoSignatureRow({
      memo, event, signerRole,
      signerName, signerEmail: resolvedEmail, signerClerkId: ctx.actorUserId,
      signatureDataUrl, consentText,
      ipAddress, userAgent,
    });

    const { actorId, actorName } = getActor(req);
    logActivity({
      userId: memo.user_id,
      actorId: actorId || ctx.actorUserId,
      actorName: actorName || row.signer_name,
      entityType: 'memo',
      entityId: String(memo.id),
      action: `signed_${event}_${signerRole}`,
      summary: `${signerRole === 'supplier' ? 'Supplier' : 'Store'} signed ${event === 'issue' ? 'memo issuance' : 'memo close'} (${row.signer_name})`,
      related: [{ type: 'company', id: memo.company_id }],
    });

    res.json(row);
  } catch (e) {
    sendSignatureError(res, e, 'Memo signature error:');
  }
});

/* POST /api/memos/:id/signature-tokens
 * Supplier creates an opaque single-use link the counterparty can use
 * to sign without a portal account. The link encodes one (memo, event,
 * signer_role) tuple — typically (memoId, 'issue', 'store') sent to a
 * retail store over WhatsApp. Tokens expire after 7 days by default
 * (clamped 1–60) and become inert once `used_at` is stamped. */
app.post('/api/memos/:id/signature-tokens', async (req, res) => {
  try {
    const { id } = req.params;
    const ctx = await resolveTeamContext(req);
    if (!ctx.actorUserId) return res.status(400).json({ error: 'userId is required' });

    const { event, signerRole, signerEmail, expiresInDays } = req.body || {};
    if (event !== 'issue' && event !== 'close') {
      return res.status(400).json({ error: "event must be 'issue' or 'close'" });
    }
    if (signerRole !== 'supplier' && signerRole !== 'store') {
      return res.status(400).json({ error: "signerRole must be 'supplier' or 'store'" });
    }

    const memoRes = await pool.query(`SELECT * FROM memos WHERE id = $1`, [id]);
    if (memoRes.rows.length === 0) return res.status(404).json({ error: 'Memo not found' });
    const memo = memoRes.rows[0];

    // Only the memo's tenant (owner or its assigned rep) can mint tokens.
    if (memo.user_id !== ctx.tenantUserId) {
      return res.status(403).json({ error: 'Not authorized for this memo' });
    }
    if (!ctx.isOwner && memo.assigned_to && memo.assigned_to !== ctx.actorUserId) {
      return res.status(403).json({ error: 'Not authorized for this memo' });
    }

    // Don't mint a token for a slot that's already signed.
    const sigRes = await pool.query(
      `SELECT id FROM memo_signatures
        WHERE memo_id = $1 AND event = $2 AND signer_role = $3
        LIMIT 1`,
      [id, event, signerRole]
    );
    if (sigRes.rows.length) {
      return res.status(409).json({
        error: 'Signature already exists for this slot',
        code: 'signature_exists',
      });
    }

    const days = Math.max(1, Math.min(60, Number(expiresInDays || 7)));
    const expiresAt = new Date(Date.now() + days * 86400 * 1000);
    const token = crypto.randomBytes(32).toString('hex');

    const ins = await pool.query(`
      INSERT INTO memo_signature_tokens
        (memo_id, user_id, token, event, signer_role, signer_email, expires_at, created_by)
      VALUES
        ($1, $2, $3, $4, $5, $6, $7, $8)
      RETURNING id, memo_id, token, event, signer_role, signer_email,
                expires_at, used_at, created_at, created_by
    `, [id, memo.user_id, token, event, signerRole, signerEmail || null, expiresAt, ctx.actorUserId]);

    const { actorId, actorName } = getActor(req);
    logActivity({
      userId: memo.user_id,
      actorId: actorId || ctx.actorUserId,
      actorName,
      entityType: 'memo',
      entityId: String(memo.id),
      action: `signing_link_created_${event}_${signerRole}`,
      summary: `Signing link created for ${signerRole} ${event} (expires ${expiresAt.toISOString().slice(0, 10)})`,
      related: [{ type: 'company', id: memo.company_id }],
    });

    res.json(ins.rows[0]);
  } catch (e) {
    console.error('Signature token error:', e);
    res.status(500).json({ error: e.message });
  }
});

/* GET /api/sign/:token — public.
 * Returns enough memo context for the public signing page to render
 * a faithful preview. Sensitive fields (cost, internal_notes,
 * assigned_to, owner clerk_id) are deliberately omitted. */
app.get('/api/sign/:token', async (req, res) => {
  try {
    const { token } = req.params;
    const tokRes = await pool.query(
      `SELECT * FROM memo_signature_tokens WHERE token = $1`,
      [token]
    );
    if (tokRes.rows.length === 0) {
      return res.status(404).json({ error: 'Signing link not found.', code: 'not_found' });
    }
    const tok = tokRes.rows[0];
    if (tok.used_at) {
      return res.status(410).json({ error: 'This signing link has already been used.', code: 'already_used' });
    }
    if (tok.expires_at && new Date(tok.expires_at) < new Date()) {
      return res.status(410).json({ error: 'This signing link has expired.', code: 'expired' });
    }

    const memoRes = await pool.query(`
      SELECT m.id, m.memo_number, m.status, m.issued_at, m.due_at,
             m.total_value, m.currency, m.notes,
             m.company_id,
             c.name AS company_name, c.email AS company_email, c.phone AS company_phone,
             c.address AS company_address, c.city AS company_city, c.country AS company_country,
             c.logo_url AS company_logo,
             tm_owner.name AS supplier_name, tm_owner.email AS supplier_email
        FROM memos m
        JOIN crm_companies c ON c.id = m.company_id
   LEFT JOIN team_members tm_owner
          ON tm_owner.team_owner_id = m.user_id
         AND tm_owner.role = 'owner'
         AND tm_owner.active = TRUE
       WHERE m.id = $1
       LIMIT 1
    `, [tok.memo_id]);
    if (memoRes.rows.length === 0) {
      return res.status(404).json({ error: 'Memo not found.', code: 'not_found' });
    }
    const memo = memoRes.rows[0];

    const itemsRes = await pool.query(`
      SELECT id, item_type, item_sku, snapshot, memo_price, quantity, status
        FROM memo_items
       WHERE memo_id = $1
    ORDER BY created_at ASC
    `, [tok.memo_id]);

    // Race protection: if a portal user signed this slot first, surface
    // that fact so the public page can render a friendly "already
    // signed" state instead of letting the user sign a second time.
    const existingRes = await pool.query(
      `SELECT id, signer_name, signed_at FROM memo_signatures
        WHERE memo_id = $1 AND event = $2 AND signer_role = $3
        LIMIT 1`,
      [tok.memo_id, tok.event, tok.signer_role]
    );

    res.json({
      token: {
        event: tok.event,
        signerRole: tok.signer_role,
        signerEmail: tok.signer_email,
        expiresAt: tok.expires_at,
      },
      memo: { ...memo, items: itemsRes.rows },
      existingSignature: existingRes.rows[0] || null,
    });
  } catch (e) {
    console.error('Public sign GET error:', e);
    res.status(500).json({ error: e.message });
  }
});

/* POST /api/sign/:token — public.
 * Submits a signature against an opaque token. The token is consumed
 * (used_at stamped) on success. If the slot was already signed via the
 * portal before this redemption, createMemoSignatureRow surfaces 409
 * `signature_exists` and the token is left intact for inspection. */
app.post('/api/sign/:token', async (req, res) => {
  try {
    const { token } = req.params;
    const { signerName, signerEmail, signatureDataUrl, consentText } = req.body || {};

    const tokRes = await pool.query(
      `SELECT * FROM memo_signature_tokens WHERE token = $1`,
      [token]
    );
    if (tokRes.rows.length === 0) {
      return res.status(404).json({ error: 'Signing link not found.', code: 'not_found' });
    }
    const tok = tokRes.rows[0];
    if (tok.used_at) {
      return res.status(410).json({ error: 'This signing link has already been used.', code: 'already_used' });
    }
    if (tok.expires_at && new Date(tok.expires_at) < new Date()) {
      return res.status(410).json({ error: 'This signing link has expired.', code: 'expired' });
    }

    const memoRes = await pool.query(`SELECT * FROM memos WHERE id = $1`, [tok.memo_id]);
    if (memoRes.rows.length === 0) {
      return res.status(404).json({ error: 'Memo not found.', code: 'not_found' });
    }
    const memo = memoRes.rows[0];

    const ipAddress = (
      req.headers['x-forwarded-for'] ||
      req.socket?.remoteAddress ||
      ''
    ).toString().split(',')[0].trim() || null;
    const userAgent = req.headers['user-agent'] || null;

    const row = await createMemoSignatureRow({
      memo,
      event: tok.event,
      signerRole: tok.signer_role,
      signerName,
      signerEmail: signerEmail || tok.signer_email || null,
      signerClerkId: null,
      signatureDataUrl,
      consentText,
      ipAddress,
      userAgent,
      tokenId: tok.id,
    });

    // Burn the token. Best-effort — the signature is already persisted,
    // so a failed update here at worst lets the same person resubmit
    // and hit the unique-constraint check.
    await pool.query(
      `UPDATE memo_signature_tokens SET used_at = NOW() WHERE id = $1`,
      [tok.id]
    );

    logActivity({
      userId: memo.user_id,
      actorId: null,
      actorName: row.signer_name,
      entityType: 'memo',
      entityId: String(memo.id),
      action: `signed_${tok.event}_${tok.signer_role}_via_link`,
      summary: `${tok.signer_role === 'supplier' ? 'Supplier' : 'Store'} signed ${tok.event === 'issue' ? 'memo issuance' : 'memo close'} via signing link (${row.signer_name})`,
      related: [{ type: 'company', id: memo.company_id }],
    });

    res.json(row);
  } catch (e) {
    sendSignatureError(res, e, 'Public sign error:');
  }
});

/* =========================================================
   Approval workflow — owner side.
   When a store user marks an item as sold or asks for a return
   from the portal, we don't apply the change immediately.
   pending_status sits on the memo_items row and the owner can
   either approve (turning it into the real status) or decline
   (clearing the request).
   ========================================================= */

// POST /api/memos/:id/items/:itemId/approve
app.post("/api/memos/:id/items/:itemId/approve", async (req, res) => {
  try {
    const { id, itemId } = req.params;
    const ctx = await resolveTeamContext(req);
    if (!ctx.actorUserId) return res.status(400).json({ error: "userId is required" });
    if (!ctx.isOwner)     return res.status(403).json({ error: "Only the workspace owner can approve store requests" });

    try { await requireSupplierIssuanceSignature(id); }
    catch (e) { return sendSignatureError(res, e, 'approve gate:'); }

    const tenantUserId = ctx.tenantUserId;

    const cur = await pool.query(
      `SELECT mi.*, m.user_id AS memo_owner
         FROM memo_items mi
         JOIN memos m ON m.id = mi.memo_id
        WHERE mi.id = $1 AND mi.memo_id = $2 AND m.user_id = $3
        LIMIT 1`,
      [itemId, id, tenantUserId]
    );
    if (!cur.rows.length)         return res.status(404).json({ error: "Memo item not found" });
    if (!cur.rows[0].pending_status) return res.status(400).json({ error: "No pending request on this item" });

    const item = cur.rows[0];
    const newStatus = item.pending_status === 'sold' ? 'sold' : 'returned';
    const stampCol  = newStatus === 'sold' ? 'sold_at' : 'returned_at';

    await pool.query(
      `UPDATE memo_items
          SET status         = $1,
              ${stampCol}    = COALESCE(${stampCol}, NOW()),
              pending_status = NULL,
              pending_at     = NULL,
              pending_by     = NULL,
              pending_note   = NULL,
              updated_at     = NOW()
        WHERE id = $2`,
      [newStatus, itemId]
    );

    await recomputeMemoTotals(Number(id));

    logActivity({
      userId:     tenantUserId,
      actorId:    ctx.actorUserId,
      actorName:  ctx.actorName,
      entityType: 'memo',
      entityId:   String(id),
      action:     newStatus === 'sold' ? 'item_sold' : 'item_returned',
      summary:    `Approved store request — ${item.item_sku} marked as ${newStatus}`,
    });

    const refreshed = await pool.query(
      `SELECT * FROM memo_items WHERE id = $1`,
      [itemId]
    );
    res.json(refreshed.rows[0]);
  } catch (error) {
    console.error("Error approving memo item:", error);
    res.status(500).json({ error: error.message });
  }
});

// POST /api/memos/:id/items/:itemId/decline
app.post("/api/memos/:id/items/:itemId/decline", async (req, res) => {
  try {
    const { id, itemId } = req.params;
    const ctx = await resolveTeamContext(req);
    if (!ctx.actorUserId) return res.status(400).json({ error: "userId is required" });
    if (!ctx.isOwner)     return res.status(403).json({ error: "Only the workspace owner can decline store requests" });

    try { await requireSupplierIssuanceSignature(id); }
    catch (e) { return sendSignatureError(res, e, 'decline gate:'); }

    const tenantUserId = ctx.tenantUserId;
    const reason = (req.body?.reason || '').toString().slice(0, 500);

    const cur = await pool.query(
      `SELECT mi.*, m.user_id AS memo_owner
         FROM memo_items mi
         JOIN memos m ON m.id = mi.memo_id
        WHERE mi.id = $1 AND mi.memo_id = $2 AND m.user_id = $3
        LIMIT 1`,
      [itemId, id, tenantUserId]
    );
    if (!cur.rows.length)            return res.status(404).json({ error: "Memo item not found" });
    if (!cur.rows[0].pending_status) return res.status(400).json({ error: "No pending request on this item" });

    const item = cur.rows[0];

    await pool.query(
      `UPDATE memo_items
          SET pending_status = NULL,
              pending_at     = NULL,
              pending_by     = NULL,
              pending_note   = NULL,
              updated_at     = NOW()
        WHERE id = $1`,
      [itemId]
    );

    logActivity({
      userId:     tenantUserId,
      actorId:    ctx.actorUserId,
      actorName:  ctx.actorName,
      entityType: 'memo',
      entityId:   String(id),
      action:     'request_declined',
      summary:    `Declined store request to mark ${item.item_sku} as ${item.pending_status}` +
                  (reason ? ` · ${reason}` : ''),
    });

    const refreshed = await pool.query(`SELECT * FROM memo_items WHERE id = $1`, [itemId]);
    res.json(refreshed.rows[0]);
  } catch (error) {
    console.error("Error declining memo item:", error);
    res.status(500).json({ error: error.message });
  }
});

/* =========================================================
   Catalog Tiers — owner-side admin
   The supplier curates "tiers" (Public, VIP Bridal, etc.) and
   assigns inventory + stores to them. The store portal then only
   exposes the union of items across the tiers each store is in.
   Default-hidden: a brand-new SKU lives in zero tiers, so no store
   sees it until the supplier explicitly approves it.
   ========================================================= */

// Build a single tier row enriched with item / company counts.
async function loadTierWithCounts(tierId, tenantUserId) {
  const r = await pool.query(
    `SELECT t.*,
            (SELECT COUNT(*) FROM catalog_tier_items     WHERE tier_id = t.id) AS item_count,
            (SELECT COUNT(*) FROM catalog_tier_items     WHERE tier_id = t.id AND item_type = 'stone')   AS stone_count,
            (SELECT COUNT(*) FROM catalog_tier_items     WHERE tier_id = t.id AND item_type = 'jewelry') AS jewelry_count,
            (SELECT COUNT(*) FROM catalog_tier_companies WHERE tier_id = t.id) AS company_count
       FROM catalog_tiers t
      WHERE t.id = $1 AND t.user_id = $2
      LIMIT 1`,
    [tierId, tenantUserId]
  );
  return r.rows[0] || null;
}

// GET /api/catalog-tiers — list all tiers for the current supplier.
app.get("/api/catalog-tiers", async (req, res) => {
  try {
    const ctx = await resolveTeamContext(req);
    if (!ctx.actorUserId) return res.status(400).json({ error: "userId is required" });
    if (ctx.role === 'store_user') return res.status(403).json({ error: "Owners only" });

    const r = await pool.query(
      `SELECT t.*,
              (SELECT COUNT(*) FROM catalog_tier_items     WHERE tier_id = t.id) AS item_count,
              (SELECT COUNT(*) FROM catalog_tier_items     WHERE tier_id = t.id AND item_type = 'stone')   AS stone_count,
              (SELECT COUNT(*) FROM catalog_tier_items     WHERE tier_id = t.id AND item_type = 'jewelry') AS jewelry_count,
              (SELECT COUNT(*) FROM catalog_tier_companies WHERE tier_id = t.id) AS company_count
         FROM catalog_tiers t
        WHERE t.user_id = $1
        ORDER BY t.sort_order ASC, t.created_at ASC`,
      [ctx.tenantUserId]
    );
    res.json(r.rows);
  } catch (e) {
    console.error("Error listing catalog tiers:", e);
    res.status(500).json({ error: e.message });
  }
});

// POST /api/catalog-tiers
app.post("/api/catalog-tiers", async (req, res) => {
  try {
    const ctx = await resolveTeamContext(req);
    if (!ctx.actorUserId) return res.status(400).json({ error: "userId is required" });
    if (ctx.role === 'store_user') return res.status(403).json({ error: "Owners only" });

    const name = String(req.body?.name || '').trim();
    if (!name) return res.status(400).json({ error: "Name is required" });
    const description = req.body?.description || null;
    const color       = req.body?.color || null;
    const isDefault   = !!req.body?.is_default;
    const sortOrder   = Number.isFinite(Number(req.body?.sort_order)) ? Number(req.body.sort_order) : 0;

    const r = await pool.query(
      `INSERT INTO catalog_tiers (user_id, name, description, color, is_default, sort_order)
       VALUES ($1, $2, $3, $4, $5, $6)
       RETURNING *`,
      [ctx.tenantUserId, name, description, color, isDefault, sortOrder]
    );
    res.json(await loadTierWithCounts(r.rows[0].id, ctx.tenantUserId));
  } catch (e) {
    if (e.code === '23505') return res.status(409).json({ error: "A tier with that name already exists" });
    console.error("Error creating catalog tier:", e);
    res.status(500).json({ error: e.message });
  }
});

// PUT /api/catalog-tiers/:id
app.put("/api/catalog-tiers/:id", async (req, res) => {
  try {
    const ctx = await resolveTeamContext(req);
    if (!ctx.actorUserId) return res.status(400).json({ error: "userId is required" });
    if (ctx.role === 'store_user') return res.status(403).json({ error: "Owners only" });

    const id = Number(req.params.id);
    if (!Number.isFinite(id)) return res.status(400).json({ error: "Invalid tier id" });

    const fields = [];
    const values = [];
    let idx = 1;
    const allow = ['name','description','color','is_default','sort_order'];
    for (const k of allow) {
      if (Object.prototype.hasOwnProperty.call(req.body || {}, k)) {
        fields.push(`${k} = $${idx++}`);
        values.push(req.body[k]);
      }
    }
    if (!fields.length) return res.json(await loadTierWithCounts(id, ctx.tenantUserId));
    fields.push(`updated_at = NOW()`);
    values.push(id, ctx.tenantUserId);

    const r = await pool.query(
      `UPDATE catalog_tiers
          SET ${fields.join(', ')}
        WHERE id = $${idx++} AND user_id = $${idx}
        RETURNING id`,
      values
    );
    if (!r.rows.length) return res.status(404).json({ error: "Tier not found" });
    res.json(await loadTierWithCounts(id, ctx.tenantUserId));
  } catch (e) {
    if (e.code === '23505') return res.status(409).json({ error: "A tier with that name already exists" });
    console.error("Error updating catalog tier:", e);
    res.status(500).json({ error: e.message });
  }
});

// DELETE /api/catalog-tiers/:id
app.delete("/api/catalog-tiers/:id", async (req, res) => {
  try {
    const ctx = await resolveTeamContext(req);
    if (!ctx.actorUserId) return res.status(400).json({ error: "userId is required" });
    if (ctx.role === 'store_user') return res.status(403).json({ error: "Owners only" });

    const id = Number(req.params.id);
    if (!Number.isFinite(id)) return res.status(400).json({ error: "Invalid tier id" });

    const r = await pool.query(
      `DELETE FROM catalog_tiers WHERE id = $1 AND user_id = $2 RETURNING id`,
      [id, ctx.tenantUserId]
    );
    if (!r.rows.length) return res.status(404).json({ error: "Tier not found" });
    res.json({ ok: true });
  } catch (e) {
    console.error("Error deleting catalog tier:", e);
    res.status(500).json({ error: e.message });
  }
});

// GET /api/catalog-tiers/:id — detail with items + companies + counts.
app.get("/api/catalog-tiers/:id", async (req, res) => {
  try {
    const ctx = await resolveTeamContext(req);
    if (!ctx.actorUserId) return res.status(400).json({ error: "userId is required" });
    if (ctx.role === 'store_user') return res.status(403).json({ error: "Owners only" });

    const id = Number(req.params.id);
    if (!Number.isFinite(id)) return res.status(400).json({ error: "Invalid tier id" });

    const tier = await loadTierWithCounts(id, ctx.tenantUserId);
    if (!tier) return res.status(404).json({ error: "Tier not found" });

    // Items in tier — enrich with display fields for each kind.
    const itemsRes = await pool.query(
      `SELECT cti.item_type, cti.item_sku, cti.added_at,
              CASE WHEN cti.item_type = 'stone'   THEN s.shape       ELSE j.name        END AS title,
              CASE WHEN cti.item_type = 'stone'   THEN s.image       ELSE j.cover_image_url END AS image_url,
              CASE WHEN cti.item_type = 'stone'   THEN s.weight::TEXT      ELSE j.weight_grams::TEXT END AS weight,
              CASE WHEN cti.item_type = 'stone'   THEN s.color       ELSE j.metal_summary END AS subtitle,
              CASE WHEN cti.item_type = 'stone'   THEN s.category    ELSE j.category    END AS category
         FROM catalog_tier_items cti
    LEFT JOIN soap_stones s ON cti.item_type = 'stone' AND s.sku = cti.item_sku
    LEFT JOIN jewelry_items j ON cti.item_type = 'jewelry' AND j.sku = cti.item_sku AND j.user_id = $2
        WHERE cti.tier_id = $1
        ORDER BY cti.added_at DESC`,
      [id, ctx.tenantUserId]
    );

    // Companies subscribed.
    const companiesRes = await pool.query(
      `SELECT c.id, c.name, c.logo_url, c.city, c.country, ctc.added_at
         FROM catalog_tier_companies ctc
         JOIN crm_companies c ON c.id = ctc.company_id
        WHERE ctc.tier_id = $1 AND c.user_id = $2
        ORDER BY c.name ASC`,
      [id, ctx.tenantUserId]
    );

    res.json({
      ...tier,
      items: itemsRes.rows,
      companies: companiesRes.rows,
    });
  } catch (e) {
    console.error("Error loading catalog tier detail:", e);
    res.status(500).json({ error: e.message });
  }
});

// POST /api/catalog-tiers/:id/items   body: { items: [{type, sku}, ...] }
app.post("/api/catalog-tiers/:id/items", async (req, res) => {
  try {
    const ctx = await resolveTeamContext(req);
    if (!ctx.actorUserId) return res.status(400).json({ error: "userId is required" });
    if (ctx.role === 'store_user') return res.status(403).json({ error: "Owners only" });

    const id = Number(req.params.id);
    if (!Number.isFinite(id)) return res.status(400).json({ error: "Invalid tier id" });

    const own = await pool.query(`SELECT 1 FROM catalog_tiers WHERE id = $1 AND user_id = $2`, [id, ctx.tenantUserId]);
    if (!own.rows.length) return res.status(404).json({ error: "Tier not found" });

    const items = Array.isArray(req.body?.items) ? req.body.items : [];
    if (!items.length) return res.json({ ok: true, added: 0 });

    let added = 0;
    for (const it of items) {
      const type = it?.type || it?.item_type;
      const sku  = it?.sku  || it?.item_sku;
      if (!sku || !['stone','jewelry'].includes(type)) continue;
      const ins = await pool.query(
        `INSERT INTO catalog_tier_items (tier_id, item_type, item_sku, added_by)
         VALUES ($1, $2, $3, $4)
         ON CONFLICT DO NOTHING
         RETURNING tier_id`,
        [id, type, sku, ctx.actorUserId]
      );
      if (ins.rows.length) added++;
    }
    res.json({ ok: true, added });
  } catch (e) {
    console.error("Error adding items to catalog tier:", e);
    res.status(500).json({ error: e.message });
  }
});

// DELETE /api/catalog-tiers/:id/items   body: { items: [{type, sku}, ...] }
app.delete("/api/catalog-tiers/:id/items", async (req, res) => {
  try {
    const ctx = await resolveTeamContext(req);
    if (!ctx.actorUserId) return res.status(400).json({ error: "userId is required" });
    if (ctx.role === 'store_user') return res.status(403).json({ error: "Owners only" });

    const id = Number(req.params.id);
    if (!Number.isFinite(id)) return res.status(400).json({ error: "Invalid tier id" });

    const own = await pool.query(`SELECT 1 FROM catalog_tiers WHERE id = $1 AND user_id = $2`, [id, ctx.tenantUserId]);
    if (!own.rows.length) return res.status(404).json({ error: "Tier not found" });

    const items = Array.isArray(req.body?.items) ? req.body.items : [];
    if (!items.length) return res.json({ ok: true, removed: 0 });

    let removed = 0;
    for (const it of items) {
      const type = it?.type || it?.item_type;
      const sku  = it?.sku  || it?.item_sku;
      if (!sku || !['stone','jewelry'].includes(type)) continue;
      const del = await pool.query(
        `DELETE FROM catalog_tier_items WHERE tier_id = $1 AND item_type = $2 AND item_sku = $3 RETURNING tier_id`,
        [id, type, sku]
      );
      if (del.rows.length) removed++;
    }
    res.json({ ok: true, removed });
  } catch (e) {
    console.error("Error removing items from catalog tier:", e);
    res.status(500).json({ error: e.message });
  }
});

// POST /api/catalog-tiers/:id/companies   body: { company_ids: [...] }
app.post("/api/catalog-tiers/:id/companies", async (req, res) => {
  try {
    const ctx = await resolveTeamContext(req);
    if (!ctx.actorUserId) return res.status(400).json({ error: "userId is required" });
    if (ctx.role === 'store_user') return res.status(403).json({ error: "Owners only" });

    const id = Number(req.params.id);
    if (!Number.isFinite(id)) return res.status(400).json({ error: "Invalid tier id" });

    const own = await pool.query(`SELECT 1 FROM catalog_tiers WHERE id = $1 AND user_id = $2`, [id, ctx.tenantUserId]);
    if (!own.rows.length) return res.status(404).json({ error: "Tier not found" });

    const ids = Array.isArray(req.body?.company_ids) ? req.body.company_ids.map(Number).filter(Number.isFinite) : [];
    if (!ids.length) return res.json({ ok: true, added: 0 });

    // Defensive: only allow companies that belong to the current tenant.
    const valid = await pool.query(
      `SELECT id FROM crm_companies WHERE user_id = $1 AND id = ANY($2::int[])`,
      [ctx.tenantUserId, ids]
    );
    let added = 0;
    for (const row of valid.rows) {
      const ins = await pool.query(
        `INSERT INTO catalog_tier_companies (tier_id, company_id)
         VALUES ($1, $2)
         ON CONFLICT DO NOTHING
         RETURNING tier_id`,
        [id, row.id]
      );
      if (ins.rows.length) added++;
    }
    res.json({ ok: true, added });
  } catch (e) {
    console.error("Error adding companies to catalog tier:", e);
    res.status(500).json({ error: e.message });
  }
});

// DELETE /api/catalog-tiers/:id/companies   body: { company_ids: [...] }
app.delete("/api/catalog-tiers/:id/companies", async (req, res) => {
  try {
    const ctx = await resolveTeamContext(req);
    if (!ctx.actorUserId) return res.status(400).json({ error: "userId is required" });
    if (ctx.role === 'store_user') return res.status(403).json({ error: "Owners only" });

    const id = Number(req.params.id);
    if (!Number.isFinite(id)) return res.status(400).json({ error: "Invalid tier id" });

    const own = await pool.query(`SELECT 1 FROM catalog_tiers WHERE id = $1 AND user_id = $2`, [id, ctx.tenantUserId]);
    if (!own.rows.length) return res.status(404).json({ error: "Tier not found" });

    const ids = Array.isArray(req.body?.company_ids) ? req.body.company_ids.map(Number).filter(Number.isFinite) : [];
    if (!ids.length) return res.json({ ok: true, removed: 0 });

    const r = await pool.query(
      `DELETE FROM catalog_tier_companies WHERE tier_id = $1 AND company_id = ANY($2::int[]) RETURNING tier_id`,
      [id, ids]
    );
    res.json({ ok: true, removed: r.rows.length });
  } catch (e) {
    console.error("Error removing companies from catalog tier:", e);
    res.status(500).json({ error: e.message });
  }
});

// GET /api/items/:type/:sku/tiers — which tiers include this single item.
// Used by the inventory / jewelry edit screens to show membership.
app.get("/api/items/:type/:sku/tiers", async (req, res) => {
  try {
    const ctx = await resolveTeamContext(req);
    if (!ctx.actorUserId) return res.status(400).json({ error: "userId is required" });
    if (ctx.role === 'store_user') return res.status(403).json({ error: "Owners only" });

    const type = String(req.params.type || '').toLowerCase();
    const sku  = req.params.sku;
    if (!['stone','jewelry'].includes(type) || !sku) return res.status(400).json({ error: "Invalid item" });

    const r = await pool.query(
      `SELECT t.id, t.name, t.color
         FROM catalog_tier_items cti
         JOIN catalog_tiers t ON t.id = cti.tier_id
        WHERE t.user_id = $1
          AND cti.item_type = $2
          AND cti.item_sku  = $3
        ORDER BY t.sort_order ASC, t.name ASC`,
      [ctx.tenantUserId, type, sku]
    );
    res.json(r.rows);
  } catch (e) {
    console.error("Error fetching item tiers:", e);
    res.status(500).json({ error: e.message });
  }
});

// PUT /api/items/:type/:sku/tiers   body: { tier_ids: [...] }
// Replaces the full set of tiers for a single item.
app.put("/api/items/:type/:sku/tiers", async (req, res) => {
  const client = await pool.connect();
  try {
    const ctx = await resolveTeamContext(req);
    if (!ctx.actorUserId) {
      client.release();
      return res.status(400).json({ error: "userId is required" });
    }
    if (ctx.role === 'store_user') {
      client.release();
      return res.status(403).json({ error: "Owners only" });
    }

    const type = String(req.params.type || '').toLowerCase();
    const sku  = req.params.sku;
    if (!['stone','jewelry'].includes(type) || !sku) {
      client.release();
      return res.status(400).json({ error: "Invalid item" });
    }

    const requested = Array.isArray(req.body?.tier_ids) ? req.body.tier_ids.map(Number).filter(Number.isFinite) : [];

    await client.query('BEGIN');
    // Resolve which of the requested tier ids actually belong to this tenant.
    const validTiers = requested.length
      ? (await client.query(
          `SELECT id FROM catalog_tiers WHERE user_id = $1 AND id = ANY($2::int[])`,
          [ctx.tenantUserId, requested]
        )).rows.map(r => r.id)
      : [];

    // Wipe current memberships for this SKU across this tenant's tiers,
    // then re-insert. Cleaner than computing add/remove diffs.
    await client.query(
      `DELETE FROM catalog_tier_items
        WHERE item_type = $1 AND item_sku = $2
          AND tier_id IN (SELECT id FROM catalog_tiers WHERE user_id = $3)`,
      [type, sku, ctx.tenantUserId]
    );
    for (const tierId of validTiers) {
      await client.query(
        `INSERT INTO catalog_tier_items (tier_id, item_type, item_sku, added_by)
         VALUES ($1, $2, $3, $4)
         ON CONFLICT DO NOTHING`,
        [tierId, type, sku, ctx.actorUserId]
      );
    }
    await client.query('COMMIT');

    const r = await pool.query(
      `SELECT t.id, t.name, t.color
         FROM catalog_tier_items cti
         JOIN catalog_tiers t ON t.id = cti.tier_id
        WHERE t.user_id = $1
          AND cti.item_type = $2
          AND cti.item_sku  = $3
        ORDER BY t.sort_order ASC, t.name ASC`,
      [ctx.tenantUserId, type, sku]
    );
    res.json(r.rows);
  } catch (e) {
    try { await client.query('ROLLBACK'); } catch (_) {}
    console.error("Error setting item tiers:", e);
    res.status(500).json({ error: e.message });
  } finally {
    client.release();
  }
});

// GET /api/companies/:id/tiers — which tiers a single store is subscribed to.
app.get("/api/companies/:id/tiers", async (req, res) => {
  try {
    const ctx = await resolveTeamContext(req);
    if (!ctx.actorUserId) return res.status(400).json({ error: "userId is required" });
    if (ctx.role === 'store_user') return res.status(403).json({ error: "Owners only" });

    const id = Number(req.params.id);
    if (!Number.isFinite(id)) return res.status(400).json({ error: "Invalid company id" });

    const own = await pool.query(`SELECT 1 FROM crm_companies WHERE id = $1 AND user_id = $2`, [id, ctx.tenantUserId]);
    if (!own.rows.length) return res.status(404).json({ error: "Company not found" });

    const r = await pool.query(
      `SELECT t.id, t.name, t.color, t.is_default,
              (SELECT COUNT(*) FROM catalog_tier_items WHERE tier_id = t.id) AS item_count
         FROM catalog_tier_companies ctc
         JOIN catalog_tiers t ON t.id = ctc.tier_id
        WHERE ctc.company_id = $1 AND t.user_id = $2
        ORDER BY t.sort_order ASC, t.name ASC`,
      [id, ctx.tenantUserId]
    );
    res.json(r.rows);
  } catch (e) {
    console.error("Error fetching company tiers:", e);
    res.status(500).json({ error: e.message });
  }
});

// PUT /api/companies/:id/tiers   body: { tier_ids: [...] }
app.put("/api/companies/:id/tiers", async (req, res) => {
  const client = await pool.connect();
  try {
    const ctx = await resolveTeamContext(req);
    if (!ctx.actorUserId) {
      client.release();
      return res.status(400).json({ error: "userId is required" });
    }
    if (ctx.role === 'store_user') {
      client.release();
      return res.status(403).json({ error: "Owners only" });
    }

    const id = Number(req.params.id);
    if (!Number.isFinite(id)) {
      client.release();
      return res.status(400).json({ error: "Invalid company id" });
    }

    const own = await pool.query(`SELECT 1 FROM crm_companies WHERE id = $1 AND user_id = $2`, [id, ctx.tenantUserId]);
    if (!own.rows.length) {
      client.release();
      return res.status(404).json({ error: "Company not found" });
    }

    const requested = Array.isArray(req.body?.tier_ids) ? req.body.tier_ids.map(Number).filter(Number.isFinite) : [];

    await client.query('BEGIN');
    const validTiers = requested.length
      ? (await client.query(
          `SELECT id FROM catalog_tiers WHERE user_id = $1 AND id = ANY($2::int[])`,
          [ctx.tenantUserId, requested]
        )).rows.map(r => r.id)
      : [];

    await client.query(
      `DELETE FROM catalog_tier_companies
        WHERE company_id = $1
          AND tier_id IN (SELECT id FROM catalog_tiers WHERE user_id = $2)`,
      [id, ctx.tenantUserId]
    );
    for (const tierId of validTiers) {
      await client.query(
        `INSERT INTO catalog_tier_companies (tier_id, company_id) VALUES ($1, $2) ON CONFLICT DO NOTHING`,
        [tierId, id]
      );
    }
    await client.query('COMMIT');

    const r = await pool.query(
      `SELECT t.id, t.name, t.color, t.is_default,
              (SELECT COUNT(*) FROM catalog_tier_items WHERE tier_id = t.id) AS item_count
         FROM catalog_tier_companies ctc
         JOIN catalog_tiers t ON t.id = ctc.tier_id
        WHERE ctc.company_id = $1 AND t.user_id = $2
        ORDER BY t.sort_order ASC, t.name ASC`,
      [id, ctx.tenantUserId]
    );
    res.json(r.rows);
  } catch (e) {
    try { await client.query('ROLLBACK'); } catch (_) {}
    console.error("Error setting company tiers:", e);
    res.status(500).json({ error: e.message });
  } finally {
    client.release();
  }
});

/* =========================================================
   Store Portal endpoints
   ========================================================= */

// Helper: load the active store_user row, or 403 with a clear message.
async function requireStoreUser(req) {
  const ctx = await resolveTeamContext(req);
  if (!ctx.actorUserId)              throw Object.assign(new Error("userId is required"), { status: 400 });
  if (ctx.role !== 'store_user' || !ctx.companyId) {
    throw Object.assign(new Error("This account is not a store-portal user"), { status: 403 });
  }
  return ctx;
}

// GET /api/portal/me — returns the store user's profile + linked store + supplier (workspace owner).
app.get("/api/portal/me", async (req, res) => {
  try {
    const ctx = await requireStoreUser(req);
    const company = await pool.query(
      `SELECT id, name, type, logo_url, cover_image_url, description,
              email, phone, address, city, country, website,
              instagram, facebook, linkedin, whatsapp,
              business_hours, established_year, currency,
              tax_id, payment_terms, default_memo_days
         FROM crm_companies
        WHERE id = $1 AND user_id = $2
        LIMIT 1`,
      [ctx.companyId, ctx.tenantUserId]
    );
    if (!company.rows.length) return res.status(404).json({ error: "Linked store not found" });

    const supplier = await pool.query(
      `SELECT name, email, avatar_color
         FROM team_members
        WHERE team_owner_id = $1 AND role = 'owner' AND active = TRUE
        LIMIT 1`,
      [ctx.tenantUserId]
    );

    res.json({
      user: {
        id: ctx.memberId,
        name: ctx.memberName,
        email: ctx.memberEmail,
        role: ctx.role,
      },
      store: company.rows[0],
      supplier: supplier.rows[0] || null,
    });
  } catch (e) {
    res.status(e.status || 500).json({ error: e.message });
  }
});

// GET /api/portal/memos — list memos visible to the store user.
// Excludes drafts; orders open memos first, then closed/returned.
app.get("/api/portal/memos", async (req, res) => {
  try {
    const ctx = await requireStoreUser(req);
    const status = (req.query.status || '').toString();
    const params = [ctx.tenantUserId, ctx.companyId];
    let where = `m.user_id = $1 AND m.company_id = $2 AND m.status <> 'draft'`;
    if (status === 'active') {
      where += ` AND m.status IN ('out','partially_returned')`;
    } else if (status === 'closed') {
      where += ` AND m.status IN ('closed','fully_returned')`;
    }

    const r = await pool.query(`
      SELECT m.id, m.memo_number, m.status, m.issued_at, m.due_at,
             m.total_value, m.currency, m.notes,
             tm_owner.name AS supplier_name,
             (SELECT COUNT(*)::int FROM memo_items mi WHERE mi.memo_id = m.id)                                 AS item_count,
             (SELECT COUNT(*)::int FROM memo_items mi WHERE mi.memo_id = m.id AND mi.status = 'out')           AS items_out,
             (SELECT COUNT(*)::int FROM memo_items mi WHERE mi.memo_id = m.id AND mi.pending_status IS NOT NULL) AS items_pending,
             EXISTS(SELECT 1 FROM memo_signatures s WHERE s.memo_id = m.id AND s.event='issue' AND s.signer_role='supplier') AS has_sig_issue_supplier,
             EXISTS(SELECT 1 FROM memo_signatures s WHERE s.memo_id = m.id AND s.event='issue' AND s.signer_role='store')    AS has_sig_issue_store,
             EXISTS(SELECT 1 FROM memo_signatures s WHERE s.memo_id = m.id AND s.event='close' AND s.signer_role='supplier') AS has_sig_close_supplier,
             EXISTS(SELECT 1 FROM memo_signatures s WHERE s.memo_id = m.id AND s.event='close' AND s.signer_role='store')    AS has_sig_close_store,
             (SELECT COUNT(*)::int FROM memo_signatures s WHERE s.memo_id = m.id) AS signature_count
        FROM memos m
   LEFT JOIN team_members tm_owner
          ON tm_owner.team_owner_id = m.user_id
         AND tm_owner.role = 'owner'
         AND tm_owner.active = TRUE
       WHERE ${where}
    ORDER BY CASE WHEN m.status IN ('out','partially_returned') THEN 0 ELSE 1 END,
             m.issued_at DESC NULLS LAST,
             m.created_at DESC
    `, params);

    res.json(r.rows);
  } catch (e) {
    res.status(e.status || 500).json({ error: e.message });
  }
});

// GET /api/portal/memos/:id — single memo + items, scoped to the store.
app.get("/api/portal/memos/:id", async (req, res) => {
  try {
    const ctx = await requireStoreUser(req);
    const { id } = req.params;

    const memoRes = await pool.query(`
      SELECT m.id, m.memo_number, m.status, m.issued_at, m.due_at,
             m.total_value, m.currency, m.notes,
             m.user_id, m.company_id,
             c.name AS company_name, c.email AS company_email, c.phone AS company_phone,
             c.address AS company_address, c.city AS company_city, c.country AS company_country,
             c.logo_url AS company_logo,
             tm_owner.name AS supplier_name, tm_owner.email AS supplier_email
        FROM memos m
        JOIN crm_companies c ON c.id = m.company_id
   LEFT JOIN team_members tm_owner
          ON tm_owner.team_owner_id = m.user_id
         AND tm_owner.role = 'owner'
         AND tm_owner.active = TRUE
       WHERE m.id = $1 AND m.user_id = $2 AND m.company_id = $3 AND m.status <> 'draft'
       LIMIT 1
    `, [id, ctx.tenantUserId, ctx.companyId]);

    if (!memoRes.rows.length) return res.status(404).json({ error: "Memo not found" });
    const memo = memoRes.rows[0];

    // Items — strip cost / internal fields, only expose memo_price.
    const itemsRes = await pool.query(`
      SELECT id, memo_id, item_type, item_sku, item_id,
             snapshot, memo_price, quantity, status,
             pending_status, pending_at, pending_note,
             returned_at, sold_at, notes,
             created_at, updated_at
        FROM memo_items
       WHERE memo_id = $1
    ORDER BY created_at ASC
    `, [id]);

    // Signatures — same shape the supplier sees so both parties can
    // verify each other's signature blocks.
    const sigsRes = await pool.query(`
      SELECT id, event, signer_role, signer_clerk_id, signer_name, signer_email,
             signature_url, consent_text, integrity_hash, ip_address, user_agent,
             pdf_url, token_id, signed_at
        FROM memo_signatures
       WHERE memo_id = $1
    ORDER BY signed_at ASC, id ASC
    `, [id]);

    res.json({ ...memo, items: itemsRes.rows, signatures: sigsRes.rows });
  } catch (e) {
    res.status(e.status || 500).json({ error: e.message });
  }
});

// POST /api/portal/memos/:id/items/:itemId/request
// Body: { kind: 'sold' | 'returned', note? }
// Stages a pending request — owner approves/declines from the main app.
app.post("/api/portal/memos/:id/items/:itemId/request", async (req, res) => {
  try {
    const ctx = await requireStoreUser(req);
    const { id, itemId } = req.params;
    const { kind, note } = req.body || {};
    if (!['sold', 'returned'].includes(kind)) {
      return res.status(400).json({ error: "kind must be 'sold' or 'returned'" });
    }

    try { await requireSupplierIssuanceSignature(id); }
    catch (e) { return sendSignatureError(res, e, 'portal request gate:'); }

    // Confirm the item belongs to a memo this store user can see.
    const cur = await pool.query(`
      SELECT mi.*, m.status AS memo_status, m.company_id
        FROM memo_items mi
        JOIN memos m ON m.id = mi.memo_id
       WHERE mi.id = $1 AND mi.memo_id = $2
         AND m.user_id = $3 AND m.company_id = $4
       LIMIT 1
    `, [itemId, id, ctx.tenantUserId, ctx.companyId]);
    if (!cur.rows.length)             return res.status(404).json({ error: "Memo item not found" });
    if (cur.rows[0].status !== 'out') return res.status(400).json({ error: "Item is no longer out on memo" });

    const cleanNote = (note || '').toString().slice(0, 500) || null;

    await pool.query(`
      UPDATE memo_items
         SET pending_status = $1,
             pending_at     = NOW(),
             pending_by     = $2,
             pending_note   = $3,
             updated_at     = NOW()
       WHERE id = $4
    `, [kind, ctx.actorUserId, cleanNote, itemId]);

    logActivity({
      userId:     ctx.tenantUserId,
      actorId:    ctx.actorUserId,
      actorName:  ctx.actorName || ctx.memberName,
      entityType: 'memo',
      entityId:   String(id),
      action:     kind === 'sold' ? 'request_sold' : 'request_return',
      summary:    `Store requested ${kind === 'sold' ? 'mark as sold' : 'return'} for ${cur.rows[0].item_sku}` +
                  (cleanNote ? ` · ${cleanNote}` : ''),
    });

    const refreshed = await pool.query(`SELECT * FROM memo_items WHERE id = $1`, [itemId]);
    res.json(refreshed.rows[0]);
  } catch (e) {
    res.status(e.status || 500).json({ error: e.message });
  }
});

// POST /api/portal/memos/:id/items/:itemId/cancel-request
// Lets the store user retract a pending request before the owner acts.
app.post("/api/portal/memos/:id/items/:itemId/cancel-request", async (req, res) => {
  try {
    const ctx = await requireStoreUser(req);
    const { id, itemId } = req.params;

    const cur = await pool.query(`
      SELECT mi.*
        FROM memo_items mi
        JOIN memos m ON m.id = mi.memo_id
       WHERE mi.id = $1 AND mi.memo_id = $2
         AND m.user_id = $3 AND m.company_id = $4
       LIMIT 1
    `, [itemId, id, ctx.tenantUserId, ctx.companyId]);
    if (!cur.rows.length)                  return res.status(404).json({ error: "Memo item not found" });
    if (!cur.rows[0].pending_status)       return res.status(400).json({ error: "No pending request to cancel" });
    if (cur.rows[0].pending_by !== ctx.actorUserId) {
      return res.status(403).json({ error: "Only the requester can cancel this request" });
    }

    await pool.query(`
      UPDATE memo_items
         SET pending_status = NULL,
             pending_at     = NULL,
             pending_by     = NULL,
             pending_note   = NULL,
             updated_at     = NOW()
       WHERE id = $1
    `, [itemId]);

    logActivity({
      userId:     ctx.tenantUserId,
      actorId:    ctx.actorUserId,
      actorName:  ctx.actorName || ctx.memberName,
      entityType: 'memo',
      entityId:   String(id),
      action:     'request_cancelled',
      summary:    `Store cancelled request on ${cur.rows[0].item_sku}`,
    });

    const refreshed = await pool.query(`SELECT * FROM memo_items WHERE id = $1`, [itemId]);
    res.json(refreshed.rows[0]);
  } catch (e) {
    res.status(e.status || 500).json({ error: e.message });
  }
});

/* POST /api/portal/memos/:id/signatures
 * Store-portal counterpart of the supplier-facing
 * POST /api/memos/:id/signatures. The store-user gate up top blocks
 * `store_user` from touching anything outside /api/portal/*, so this
 * mirror endpoint is what the StorePortalMemoDetail page actually
 * calls. The body matches the supplier endpoint minus signerRole
 * (locked to 'store' here because store users can only sign as 'store').
 * Both paths feed the same `memo_signatures` table through the shared
 * createMemoSignatureRow helper. */
app.post("/api/portal/memos/:id/signatures", async (req, res) => {
  try {
    const ctx = await requireStoreUser(req);
    const { id } = req.params;

    // The memo must belong to this store and be past draft (you can't
    // acknowledge a memo that the supplier hasn't issued yet).
    const memoRes = await pool.query(
      `SELECT * FROM memos
        WHERE id = $1 AND user_id = $2 AND company_id = $3
          AND status <> 'draft'
        LIMIT 1`,
      [id, ctx.tenantUserId, ctx.companyId]
    );
    if (!memoRes.rows.length) return res.status(404).json({ error: 'Memo not found' });
    const memo = memoRes.rows[0];

    const { event, signerName, signatureDataUrl, consentText } = req.body || {};

    // Best-effort email so the row displays nicely on both sides.
    let resolvedEmail = null;
    try {
      const memberRes = await pool.query(
        `SELECT email FROM team_members WHERE clerk_user_id = $1 LIMIT 1`,
        [ctx.actorUserId]
      );
      resolvedEmail = memberRes.rows[0]?.email || null;
    } catch (_) { /* non-fatal */ }

    const ipAddress = (
      req.headers['x-forwarded-for'] ||
      req.socket?.remoteAddress ||
      ''
    ).toString().split(',')[0].trim() || null;
    const userAgent = req.headers['user-agent'] || null;

    const row = await createMemoSignatureRow({
      memo,
      event,
      signerRole: 'store', // locked — store users cannot sign as supplier
      signerName,
      signerEmail: resolvedEmail,
      signerClerkId: ctx.actorUserId,
      signatureDataUrl,
      consentText,
      ipAddress,
      userAgent,
    });

    logActivity({
      userId: memo.user_id,
      actorId: ctx.actorUserId,
      actorName: ctx.actorName || ctx.memberName || row.signer_name,
      entityType: 'memo',
      entityId: String(memo.id),
      action: `signed_${event}_store`,
      summary: `Store signed ${event === 'issue' ? 'memo issuance' : 'memo close'} (${row.signer_name})`,
      related: [{ type: 'company', id: memo.company_id }],
    });

    res.json(row);
  } catch (e) {
    sendSignatureError(res, e, 'Portal memo signature error:');
  }
});

/* =========================================================
   Portal — Catalog
   GET /api/portal/catalog?type=stones|jewelry|all&search=&shape=&category=
   Returns the supplier's available inventory (anything not currently
   out on a memo) without exposing cost / internal pricing fields.
   The store user uses this to pick items to request a memo for.
   ========================================================= */
app.get("/api/portal/catalog", async (req, res) => {
  try {
    const ctx = await requireStoreUser(req);
    const type   = String(req.query.type   || 'all').toLowerCase();
    const search = String(req.query.search || '').trim().toLowerCase();
    const shape  = String(req.query.shape  || '').trim().toLowerCase();
    const cat    = String(req.query.category || '').trim().toLowerCase();
    const limit  = Math.min(Number(req.query.limit) || 60, 200);

    // Build the set of SKUs already out on a memo (any company) so we
    // don't tease the store with items they can't actually have.
    const busy = await pool.query(`
      SELECT DISTINCT mi.item_type, mi.item_sku
        FROM memo_items mi
        JOIN memos m ON m.id = mi.memo_id
       WHERE mi.status = 'out'
         AND m.status IN ('out','partially_returned')
         AND m.user_id = $1
    `, [ctx.tenantUserId]);
    const busyStones  = new Set(busy.rows.filter(r => r.item_type !== 'jewelry').map(r => r.item_sku));
    const busyJewelry = new Set(busy.rows.filter(r => r.item_type === 'jewelry').map(r => r.item_sku));

    // Visibility gate: a store only sees SKUs that live in at least one
    // catalog tier the store is subscribed to. Anything not in any of
    // those tiers stays hidden — that's the curated default.
    const visibleStones  = new Set();
    const visibleJewelry = new Set();
    const visRes = await pool.query(`
      SELECT cti.item_type, cti.item_sku
        FROM catalog_tier_items cti
        JOIN catalog_tiers t           ON t.id  = cti.tier_id
        JOIN catalog_tier_companies ctc ON ctc.tier_id = t.id
       WHERE t.user_id = $1
         AND ctc.company_id = $2
    `, [ctx.tenantUserId, ctx.companyId]);
    for (const row of visRes.rows) {
      (row.item_type === 'jewelry' ? visibleJewelry : visibleStones).add(row.item_sku);
    }

    const out = { stones: [], jewelry: [] };

    // Stones — pull from soap_stones (the canonical "for sale" pile).
    if (type === 'all' || type === 'stones') {
      const r = await pool.query(`SELECT * FROM soap_stones WHERE sku IS NOT NULL ORDER BY updated_at DESC LIMIT 500`);
      out.stones = r.rows
        .filter(row => visibleStones.has(row.sku))
        .filter(row => !busyStones.has(row.sku))
        .filter(row => {
          if (shape && String(row.shape || '').toLowerCase() !== shape) return false;
          if (cat   && String(row.category || '').toLowerCase() !== cat) return false;
          if (!search) return true;
          const hay = [row.sku, row.shape, row.category, row.color, row.clarity, row.origin, row.lab]
            .filter(Boolean).join(' ').toLowerCase();
          return hay.includes(search);
        })
        .slice(0, limit)
        .map(row => {
          let imageUrl = row.image;
          if (!imageUrl && row.additional_pictures) {
            const first = row.additional_pictures.split(';')[0];
            imageUrl = first ? first.trim() : null;
          }
          return {
            kind:        'stone',
            sku:         row.sku,
            shape:       row.shape || '',
            category:    row.category || '',
            type:        row.type || '',
            weightCt:    row.weight ? Number(row.weight) : null,
            color:       row.color || '',
            clarity:     row.clarity || '',
            origin:      row.origin || '',
            lab:         row.lab || '',
            measurements: row.measurements || '',
            imageUrl,
            videoUrl:    row.video || null,
            certificateNumber: row.certificate_number || '',
            // Pricing intentionally omitted — store sees price only on
            // the issued memo. Discreet by design.
          };
        });
    }

    // Jewelry — pull from jewelry_items, only the ones marked "active"
    // (anything not archived / not draft / not sold).
    if (type === 'all' || type === 'jewelry') {
      const r = await pool.query(`
        SELECT id, sku, name, category, type, status, cover_image_url,
               metal_summary, weight_grams, size, sold_at, updated_at
          FROM jewelry_items
         WHERE user_id = $1
           AND COALESCE(status,'') NOT IN ('archived','draft')
           AND sold_at IS NULL
         ORDER BY updated_at DESC
         LIMIT 500
      `, [ctx.tenantUserId]);
      out.jewelry = r.rows
        .filter(row => visibleJewelry.has(row.sku))
        .filter(row => !busyJewelry.has(row.sku))
        .filter(row => {
          if (cat && String(row.category || '').toLowerCase() !== cat) return false;
          if (!search) return true;
          const hay = [row.sku, row.name, row.category, row.type, row.metal_summary]
            .filter(Boolean).join(' ').toLowerCase();
          return hay.includes(search);
        })
        .slice(0, limit)
        .map(row => ({
          kind:        'jewelry',
          id:          row.id,
          sku:         row.sku,
          name:        row.name || row.sku,
          category:    row.category || '',
          type:        row.type || '',
          metalType:   row.metal_summary || '',
          metalColor:  '',
          totalWeight: row.weight_grams != null ? Number(row.weight_grams) : null,
          size:        row.size || '',
          imageUrl:    row.cover_image_url || null,
        }));
    }

    // Diagnostic block — only shown when the resulting catalog is empty.
    // Helps the supplier (or store user) understand WHY: not subscribed
    // to any tier? subscribed but the tier has no items? everything in
    // the tier currently out on memo? Without this the empty catalog is
    // a black box and the supplier ends up wondering whether something
    // is broken.
    if (out.stones.length === 0 && out.jewelry.length === 0) {
      const tiersRes = await pool.query(`
        SELECT t.id, t.name, t.color,
               (SELECT COUNT(*) FROM catalog_tier_items WHERE tier_id = t.id) AS item_count
          FROM catalog_tier_companies ctc
          JOIN catalog_tiers t ON t.id = ctc.tier_id
         WHERE ctc.company_id = $1
           AND t.user_id = $2
         ORDER BY t.sort_order ASC, t.name ASC
      `, [ctx.companyId, ctx.tenantUserId]);
      const visibleSkuCount = visibleStones.size + visibleJewelry.size;
      out._diagnostic = {
        subscribedTiers: tiersRes.rows.map(r => ({
          id: r.id,
          name: r.name,
          color: r.color,
          itemCount: Number(r.item_count) || 0,
        })),
        visibleSkuCount,
        busySkuCount: busyStones.size + busyJewelry.size,
        reason:
          tiersRes.rows.length === 0
            ? 'no_tier_subscriptions'
            : visibleSkuCount === 0
              ? 'tiers_empty'
              : 'all_items_busy',
      };
    }

    res.json(out);
  } catch (e) {
    res.status(e.status || 500).json({ error: e.message });
  }
});

/* =========================================================
   Portal — Item detail (stone or jewelry)
   GET /api/portal/items/stone/:sku
   GET /api/portal/items/jewelry/:idOrSku
   Returns the full inventory record so the store user can review
   every spec the supplier has on file before adding it to a memo
   request — but with all cost / pricing fields stripped.
   ========================================================= */
// Helper: confirm the requesting store has at least one tier that
// contains this SKU before we hand back full inventory details.
async function ensureSkuVisibleToStore(ctx, type, sku) {
  const v = await pool.query(
    `SELECT 1
       FROM catalog_tier_items cti
       JOIN catalog_tiers t           ON t.id  = cti.tier_id
       JOIN catalog_tier_companies ctc ON ctc.tier_id = t.id
      WHERE t.user_id    = $1
        AND ctc.company_id = $2
        AND cti.item_type  = $3
        AND cti.item_sku   = $4
      LIMIT 1`,
    [ctx.tenantUserId, ctx.companyId, type, sku]
  );
  return v.rows.length > 0;
}

app.get("/api/portal/items/stone/:sku", async (req, res) => {
  try {
    const ctx = await requireStoreUser(req);
    const { sku } = req.params;
    if (!(await ensureSkuVisibleToStore(ctx, 'stone', sku))) {
      return res.status(404).json({ error: "Stone not found" });
    }
    const r = await pool.query(`SELECT * FROM soap_stones WHERE sku = $1 LIMIT 1`, [sku]);
    if (!r.rows.length) return res.status(404).json({ error: "Stone not found" });
    const row = r.rows[0];

    // Build the full image gallery (image + additional_pictures CSV).
    const gallery = [];
    if (row.image) gallery.push(row.image);
    if (row.additional_pictures) {
      for (const p of String(row.additional_pictures).split(';').map((s) => s.trim()).filter(Boolean)) {
        if (!gallery.includes(p)) gallery.push(p);
      }
    }
    const videos = [];
    if (row.video) videos.push(row.video);
    if (row.additional_videos) {
      for (const v of String(row.additional_videos).split(';').map((s) => s.trim()).filter(Boolean)) {
        if (!videos.includes(v)) videos.push(v);
      }
    }

    // Anti-leak: never echo any column whose name hints at money.
    // Do NOT include: total_price, price_per_carat, rap_list_price,
    // rap_price, comment (often has supplier-internal pricing notes).
    const detail = {
      kind: 'stone',
      sku: row.sku,
      shape: row.shape || '',
      category: row.category || '',
      type: row.type || '',
      weightCt: row.weight ? Number(row.weight) : null,
      color: row.color || '',
      clarity: row.clarity || '',
      cut: row.cut || '',
      polish: row.polish || '',
      symmetry: row.symmetry || '',
      tablePercent: row.table_percent != null ? Number(row.table_percent) : null,
      depthPercent: row.depth_percent != null ? Number(row.depth_percent) : null,
      ratio: row.ratio != null ? Number(row.ratio) : null,
      measurements: row.measurements || '',
      lab: row.lab || '',
      origin: row.origin || '',
      treatment: row.comment || '',
      certComments: row.cert_comments || '',
      certificateNumber: row.certificate_number || '',
      certificateUrl: row.certificate_image || row.certificate_url || null,
      certificateImageJpg: row.certificate_image_jpg || null,
      luster: row.luster || '',
      fluorescence: row.fluorescence || '',
      fancyIntensity: row.fancy_intensity || '',
      fancyColor: row.fancy_color || '',
      fancyOvertone: row.fancy_overtone || '',
      fancyColor2: row.fancy_color_2 || '',
      fancyOvertone2: row.fancy_overtone_2 || '',
      pairSku: row.pair_stone || null,
      groupingType: row.grouping_type || '',
      stones: row.stones != null ? Number(row.stones) : null,
      images: gallery,
      videos,
      updatedAt: row.updated_at ? new Date(row.updated_at).toISOString() : null,
    };
    res.json(detail);
  } catch (e) {
    res.status(e.status || 500).json({ error: e.message });
  }
});

app.get("/api/portal/items/jewelry/:idOrSku", async (req, res) => {
  try {
    const ctx = await requireStoreUser(req);
    const { idOrSku } = req.params;
    const isNumeric = /^\d+$/.test(idOrSku);

    const itemRes = await pool.query(
      isNumeric
        ? `SELECT * FROM jewelry_items WHERE id = $1 AND user_id = $2 LIMIT 1`
        : `SELECT * FROM jewelry_items WHERE sku = $1 AND user_id = $2 LIMIT 1`,
      [isNumeric ? Number(idOrSku) : idOrSku, ctx.tenantUserId]
    );
    if (!itemRes.rows.length) return res.status(404).json({ error: "Jewelry item not found" });
    const item = itemRes.rows[0];

    // Must live in at least one tier the store is subscribed to.
    if (!(await ensureSkuVisibleToStore(ctx, 'jewelry', item.sku))) {
      return res.status(404).json({ error: "Jewelry item not found" });
    }

    const [stones, metals, files] = await Promise.all([
      pool.query(`SELECT id, role, quantity, snapshot, notes, stone_sku FROM jewelry_item_stones WHERE item_id = $1 ORDER BY id`, [item.id]),
      pool.query(`SELECT id, metal_type, purity, color, weight_grams FROM jewelry_item_metals WHERE item_id = $1 ORDER BY id`, [item.id]),
      pool.query(`SELECT id, url, kind, stage, filename FROM jewelry_item_files WHERE item_id = $1 ORDER BY uploaded_at DESC, id DESC`, [item.id]),
    ]);

    // Strip cost / margin fields. Anything price-related is owner-only.
    res.json({
      kind: 'jewelry',
      id: item.id,
      sku: item.sku,
      name: item.name,
      type: item.type,
      category: item.category || '',
      metalSummary: item.metal_summary || '',
      weightGrams: item.weight_grams != null ? Number(item.weight_grams) : null,
      size: item.size || '',
      description: item.description || '',
      coverImageUrl: item.cover_image_url || null,
      stones: stones.rows.map((s) => ({
        id: s.id,
        role: s.role || '',
        quantity: s.quantity || 1,
        sku: s.stone_sku,
        snapshot: s.snapshot || {},
        notes: s.notes || '',
      })),
      metals: metals.rows.map((m) => ({
        id: m.id,
        metalType: m.metal_type || '',
        purity: m.purity || '',
        color: m.color || '',
        weightGrams: m.weight_grams != null ? Number(m.weight_grams) : null,
      })),
      files: files.rows.map((f) => ({
        id: f.id,
        url: f.url,
        kind: f.kind || 'image',
        stage: f.stage || '',
        label: f.filename || '',
      })),
      updatedAt: item.updated_at ? new Date(item.updated_at).toISOString() : null,
    });
  } catch (e) {
    res.status(e.status || 500).json({ error: e.message });
  }
});

/* =========================================================
   Portal — Memo Requests
   ========================================================= */

// POST /api/portal/memo-requests
// Body: { items: [{kind:'stone'|'jewelry', sku, snapshot, notes}], message, preferredDueAt }
app.post("/api/portal/memo-requests", async (req, res) => {
  try {
    const ctx = await requireStoreUser(req);
    const { items = [], message, preferredDueAt } = req.body || {};
    const cleanItems = Array.isArray(items) ? items.filter(i => i && (i.sku || i.kind)) : [];
    const cleanMsg   = (message || '').toString().slice(0, 2000) || null;
    if (!cleanItems.length && !cleanMsg) {
      return res.status(400).json({ error: "Request must include at least one item or a message" });
    }

    const dueAt = preferredDueAt ? new Date(preferredDueAt) : null;
    const ins = await pool.query(`
      INSERT INTO memo_requests (user_id, company_id, requested_by, status, message, preferred_due_at)
      VALUES ($1, $2, $3, 'pending', $4, $5)
      RETURNING *
    `, [ctx.tenantUserId, ctx.companyId, ctx.actorUserId, cleanMsg, dueAt && !isNaN(dueAt) ? dueAt : null]);
    const reqRow = ins.rows[0];

    for (const it of cleanItems) {
      const itemType = it.kind === 'jewelry' ? 'jewelry' : 'stone';
      await pool.query(`
        INSERT INTO memo_request_items (request_id, item_type, item_sku, item_id, snapshot, notes)
        VALUES ($1, $2, $3, $4, $5::jsonb, $6)
      `, [
        reqRow.id,
        itemType,
        it.sku || null,
        it.id != null ? String(it.id) : null,
        JSON.stringify(it.snapshot || it || {}),
        (it.notes || '').toString().slice(0, 500) || null,
      ]);
    }

    logActivity({
      userId:     ctx.tenantUserId,
      actorId:    ctx.actorUserId,
      actorName:  ctx.actorName || ctx.memberName,
      entityType: 'memo_request',
      entityId:   String(reqRow.id),
      action:     'created',
      summary:    `Memo request from store · ${cleanItems.length} item(s)` + (cleanMsg ? ` · "${cleanMsg.slice(0, 80)}"` : ''),
    });

    res.status(201).json(reqRow);
  } catch (e) {
    res.status(e.status || 500).json({ error: e.message });
  }
});

// GET /api/portal/memo-requests — store sees their own request history.
app.get("/api/portal/memo-requests", async (req, res) => {
  try {
    const ctx = await requireStoreUser(req);
    const r = await pool.query(`
      SELECT r.*,
             (SELECT COUNT(*)::int FROM memo_request_items i WHERE i.request_id = r.id) AS item_count
        FROM memo_requests r
       WHERE r.user_id = $1 AND r.company_id = $2
    ORDER BY r.created_at DESC
       LIMIT 200
    `, [ctx.tenantUserId, ctx.companyId]);
    res.json(r.rows);
  } catch (e) {
    res.status(e.status || 500).json({ error: e.message });
  }
});

// GET /api/portal/memo-requests/:id — single request with items.
app.get("/api/portal/memo-requests/:id", async (req, res) => {
  try {
    const ctx = await requireStoreUser(req);
    const { id } = req.params;
    const r = await pool.query(`
      SELECT * FROM memo_requests
       WHERE id = $1 AND user_id = $2 AND company_id = $3
       LIMIT 1
    `, [id, ctx.tenantUserId, ctx.companyId]);
    if (!r.rows.length) return res.status(404).json({ error: "Request not found" });
    const items = await pool.query(`SELECT * FROM memo_request_items WHERE request_id = $1 ORDER BY id`, [id]);
    res.json({ ...r.rows[0], items: items.rows });
  } catch (e) {
    res.status(e.status || 500).json({ error: e.message });
  }
});

// POST /api/portal/memo-requests/:id/cancel — store can cancel a pending request.
app.post("/api/portal/memo-requests/:id/cancel", async (req, res) => {
  try {
    const ctx = await requireStoreUser(req);
    const { id } = req.params;
    const cur = await pool.query(`
      SELECT * FROM memo_requests
       WHERE id = $1 AND user_id = $2 AND company_id = $3
       LIMIT 1
    `, [id, ctx.tenantUserId, ctx.companyId]);
    if (!cur.rows.length)              return res.status(404).json({ error: "Request not found" });
    if (cur.rows[0].status !== 'pending') return res.status(400).json({ error: "Only pending requests can be cancelled" });

    const upd = await pool.query(`
      UPDATE memo_requests
         SET status = 'cancelled', updated_at = NOW()
       WHERE id = $1
       RETURNING *
    `, [id]);
    res.json(upd.rows[0]);
  } catch (e) {
    res.status(e.status || 500).json({ error: e.message });
  }
});

/* =========================================================
   Owner-side Memo Requests (inbox + actions)
   ========================================================= */

// GET /api/memo-requests — owner inbox (optionally filter by status / company).
app.get("/api/memo-requests", async (req, res) => {
  try {
    const ctx = await resolveTeamContext(req);
    if (!ctx.actorUserId) return res.status(400).json({ error: "userId is required" });
    const tenantUserId = ctx.tenantUserId;
    const status    = String(req.query.status    || '').toLowerCase();
    const companyId = req.query.companyId ? Number(req.query.companyId) : null;

    const params = [tenantUserId];
    let where = `r.user_id = $1`;
    if (status === 'pending' || status === 'converted' || status === 'declined' || status === 'cancelled') {
      params.push(status);
      where += ` AND r.status = $${params.length}`;
    }
    if (companyId) {
      params.push(companyId);
      where += ` AND r.company_id = $${params.length}`;
    }

    const r = await pool.query(`
      SELECT r.*,
             c.name AS company_name, c.logo_url AS company_logo,
             tm.name AS requester_name, tm.email AS requester_email,
             (SELECT COUNT(*)::int FROM memo_request_items i WHERE i.request_id = r.id) AS item_count
        FROM memo_requests r
        JOIN crm_companies c ON c.id = r.company_id
   LEFT JOIN team_members tm ON tm.team_owner_id = r.user_id
                            AND tm.clerk_user_id = r.requested_by
                            AND tm.active = TRUE
       WHERE ${where}
    ORDER BY CASE WHEN r.status = 'pending' THEN 0 ELSE 1 END,
             r.created_at DESC
       LIMIT 200
    `, params);
    res.json(r.rows);
  } catch (e) {
    res.status(e.status || 500).json({ error: e.message });
  }
});

// GET /api/memo-requests/:id — owner: full detail incl. items.
app.get("/api/memo-requests/:id", async (req, res) => {
  try {
    const ctx = await resolveTeamContext(req);
    if (!ctx.actorUserId) return res.status(400).json({ error: "userId is required" });
    const { id } = req.params;
    const r = await pool.query(`
      SELECT r.*,
             c.name AS company_name, c.email AS company_email, c.logo_url AS company_logo,
             tm.name AS requester_name, tm.email AS requester_email
        FROM memo_requests r
        JOIN crm_companies c ON c.id = r.company_id
   LEFT JOIN team_members tm ON tm.team_owner_id = r.user_id
                            AND tm.clerk_user_id = r.requested_by
                            AND tm.active = TRUE
       WHERE r.id = $1 AND r.user_id = $2
       LIMIT 1
    `, [id, ctx.tenantUserId]);
    if (!r.rows.length) return res.status(404).json({ error: "Request not found" });
    const items = await pool.query(`SELECT * FROM memo_request_items WHERE request_id = $1 ORDER BY id`, [id]);
    res.json({ ...r.rows[0], items: items.rows });
  } catch (e) {
    res.status(e.status || 500).json({ error: e.message });
  }
});

// POST /api/memo-requests/:id/decline — owner declines the request.
app.post("/api/memo-requests/:id/decline", async (req, res) => {
  try {
    const ctx = await resolveTeamContext(req);
    if (!ctx.actorUserId) return res.status(400).json({ error: "userId is required" });
    if (!ctx.isOwner)     return res.status(403).json({ error: "Only owners can act on memo requests" });
    const { id } = req.params;
    const reason = (req.body?.reason || '').toString().slice(0, 500) || null;

    const cur = await pool.query(`SELECT * FROM memo_requests WHERE id = $1 AND user_id = $2`, [id, ctx.tenantUserId]);
    if (!cur.rows.length)                    return res.status(404).json({ error: "Request not found" });
    if (cur.rows[0].status !== 'pending')    return res.status(400).json({ error: "Only pending requests can be declined" });

    const upd = await pool.query(`
      UPDATE memo_requests
         SET status         = 'declined',
             decline_reason = $1,
             responded_at   = NOW(),
             responded_by   = $2,
             updated_at     = NOW()
       WHERE id = $3
       RETURNING *
    `, [reason, ctx.actorUserId, id]);

    logActivity({
      userId:     ctx.tenantUserId,
      actorId:    ctx.actorUserId,
      actorName:  ctx.actorName || ctx.memberName,
      entityType: 'memo_request',
      entityId:   String(id),
      action:     'declined',
      summary:    `Memo request declined` + (reason ? ` · ${reason}` : ''),
    });

    res.json(upd.rows[0]);
  } catch (e) {
    res.status(e.status || 500).json({ error: e.message });
  }
});

// POST /api/memo-requests/:id/convert — owner turns the request into
// a real draft memo. Items are pre-staged on the new memo so the
// owner just has to set prices and click "Issue".
app.post("/api/memo-requests/:id/convert", async (req, res) => {
  try {
    const ctx = await resolveTeamContext(req);
    if (!ctx.actorUserId) return res.status(400).json({ error: "userId is required" });
    if (!ctx.isOwner)     return res.status(403).json({ error: "Only owners can act on memo requests" });
    const { id } = req.params;

    const cur = await pool.query(`SELECT * FROM memo_requests WHERE id = $1 AND user_id = $2`, [id, ctx.tenantUserId]);
    if (!cur.rows.length)                    return res.status(404).json({ error: "Request not found" });
    if (cur.rows[0].status !== 'pending')    return res.status(400).json({ error: "Only pending requests can be converted" });
    const reqRow = cur.rows[0];

    // Generate a memo number using the same pattern POST /api/memos uses.
    const ymd = new Date();
    const yearStr = String(ymd.getFullYear());
    const last = await pool.query(
      `SELECT memo_number FROM memos WHERE user_id = $1 AND memo_number LIKE $2 ORDER BY id DESC LIMIT 1`,
      [ctx.tenantUserId, `MEMO-${yearStr}-%`]
    );
    let nextSeq = 1;
    if (last.rows[0]?.memo_number) {
      const m = String(last.rows[0].memo_number).match(/-(\d+)$/);
      if (m) nextSeq = Number(m[1]) + 1;
    }
    const memoNumber = `MEMO-${yearStr}-${String(nextSeq).padStart(4, '0')}`;

    const memoIns = await pool.query(`
      INSERT INTO memos (user_id, company_id, memo_number, status, currency, notes, due_at, created_by)
      VALUES ($1, $2, $3, 'draft', 'USD', $4, $5, $6)
      RETURNING *
    `, [
      ctx.tenantUserId,
      reqRow.company_id,
      memoNumber,
      reqRow.message ? `From request: ${reqRow.message}` : null,
      reqRow.preferred_due_at,
      ctx.actorUserId,
    ]);
    const memo = memoIns.rows[0];

    // Pre-load the request items as draft memo items. memo_items.item_sku
    // is NOT NULL, so we drop free-text items (those without a sku) — the
    // owner can read the original request message in memo.notes and add
    // them manually if needed.
    const reqItems = await pool.query(`SELECT * FROM memo_request_items WHERE request_id = $1`, [id]);
    for (const it of reqItems.rows) {
      if (!it.item_sku) continue;
      const snap = it.snapshot || {};
      await pool.query(`
        INSERT INTO memo_items (memo_id, item_type, item_sku, item_id, snapshot, memo_price, quantity)
        VALUES ($1, $2, $3, $4, $5::jsonb, NULL, 1)
      `, [
        memo.id,
        it.item_type === 'jewelry' ? 'jewelry' : 'stone',
        it.item_sku,
        it.item_id,
        JSON.stringify(snap),
      ]);
    }

    const upd = await pool.query(`
      UPDATE memo_requests
         SET status            = 'converted',
             converted_memo_id = $1,
             responded_at      = NOW(),
             responded_by      = $2,
             updated_at        = NOW()
       WHERE id = $3
       RETURNING *
    `, [memo.id, ctx.actorUserId, id]);

    logActivity({
      userId:     ctx.tenantUserId,
      actorId:    ctx.actorUserId,
      actorName:  ctx.actorName || ctx.memberName,
      entityType: 'memo_request',
      entityId:   String(id),
      action:     'converted',
      summary:    `Memo request converted to ${memoNumber}`,
    });

    res.json({ request: upd.rows[0], memo });
  } catch (e) {
    console.error('Convert memo request error:', e);
    res.status(e.status || 500).json({ error: e.message });
  }
});

/* =========================================================
   CRM – Folders (hierarchical)
   ========================================================= */

app.get("/api/crm/folders", async (req, res) => {
  try {
    const { userId } = req.query;
    if (!userId) return res.status(400).json({ error: "userId is required" });
    const result = await pool.query(
      `SELECT f.*,
        (SELECT COUNT(*)::int FROM crm_contacts c WHERE c.folder_id = f.id) AS direct_count
       FROM crm_folders f
       WHERE f.user_id = $1
       ORDER BY f.name ASC`,
      [userId]
    );
    res.json(result.rows);
  } catch (error) {
    console.error("Folders fetch error:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

app.post("/api/crm/folders", async (req, res) => {
  try {
    const { userId, name, parentId, color } = req.body;
    if (!userId || !name) return res.status(400).json({ error: "userId and name are required" });
    const result = await pool.query(
      `INSERT INTO crm_folders (user_id, name, parent_id, color)
       VALUES ($1, $2, $3, $4) RETURNING *`,
      [userId, String(name).trim(), parentId || null, color || null]
    );
    res.status(201).json(result.rows[0]);
  } catch (error) {
    console.error("Folder create error:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

app.put("/api/crm/folders/:id", async (req, res) => {
  try {
    const { id } = req.params;
    const { name, parentId, color } = req.body;
    const fields = [];
    const values = [];
    let idx = 1;
    if (name !== undefined) { fields.push(`name = $${idx++}`); values.push(String(name).trim()); }
    if (parentId !== undefined) { fields.push(`parent_id = $${idx++}`); values.push(parentId || null); }
    if (color !== undefined) { fields.push(`color = $${idx++}`); values.push(color || null); }
    fields.push(`updated_at = NOW()`);
    if (fields.length === 1) return res.status(400).json({ error: "No fields to update" });

    // Prevent making a folder its own ancestor
    if (parentId) {
      const cyc = await pool.query(
        `WITH RECURSIVE descendants AS (
           SELECT id FROM crm_folders WHERE id = $1
           UNION ALL
           SELECT f.id FROM crm_folders f INNER JOIN descendants d ON f.parent_id = d.id
         )
         SELECT 1 FROM descendants WHERE id = $2`,
        [id, parentId]
      );
      if (cyc.rows.length > 0) return res.status(400).json({ error: "Cannot move a folder into its own descendant" });
    }

    values.push(id);
    const result = await pool.query(
      `UPDATE crm_folders SET ${fields.join(", ")} WHERE id = $${idx} RETURNING *`,
      values
    );
    if (result.rows.length === 0) return res.status(404).json({ error: "Folder not found" });
    res.json(result.rows[0]);
  } catch (error) {
    console.error("Folder update error:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

app.delete("/api/crm/folders/:id", async (req, res) => {
  try {
    const { id } = req.params;
    // ON DELETE CASCADE deletes children folders; ON DELETE SET NULL on contacts.folder_id moves contacts to root
    await pool.query("DELETE FROM crm_folders WHERE id = $1", [id]);
    res.json({ success: true });
  } catch (error) {
    console.error("Folder delete error:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

app.post("/api/crm/contacts/move-to-folder", async (req, res) => {
  try {
    const { userId, contactIds, folderId } = req.body;
    if (!userId || !Array.isArray(contactIds) || contactIds.length === 0) {
      return res.status(400).json({ error: "userId and non-empty contactIds required" });
    }
    const result = await pool.query(
      `UPDATE crm_contacts SET folder_id = $1, updated_at = NOW()
       WHERE user_id = $2 AND id = ANY($3::int[]) RETURNING id`,
      [folderId || null, userId, contactIds.map(Number)]
    );
    res.json({ success: true, updated: result.rowCount });
  } catch (error) {
    console.error("Move to folder error:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

/* =========================================================
   CRM – Title migration (extract titles from notes, one-time)
   ========================================================= */

app.post("/api/crm/contacts/migrate-titles", async (req, res) => {
  try {
    const { userId, dryRun } = req.body;
    if (!userId) return res.status(400).json({ error: "userId is required" });

    // Common job-title keywords (case-insensitive)
    const TITLE_PATTERNS = [
      /\b(CEO|CFO|COO|CTO|CMO|CIO|CSO|VP|SVP|EVP)\b/i,
      /\b(President|Vice President|Founder|Co[- ]?Founder|Owner|Partner|Managing Partner|Director|Managing Director|General Manager)\b/i,
      /\b(Sales (Director|Manager|Executive|Representative|Rep)|Account (Manager|Executive)|Business Development( Manager)?)\b/i,
      /\b(Designer|Senior Designer|Lead Designer|Creative Director|Art Director|Goldsmith|Jeweler|Gemologist|Appraiser|Polisher|Cutter|Setter)\b/i,
      /\b(Marketing (Director|Manager|Coordinator)|Brand Manager|PR Manager)\b/i,
      /\b(Buyer|Senior Buyer|Head Buyer|Procurement (Manager|Director))\b/i,
      /\b(Manager|Senior Manager|Head of [A-Za-z ]+|Chief [A-Za-z ]+)\b/i,
    ];

    const rows = await pool.query(
      `SELECT id, name, notes FROM crm_contacts
       WHERE user_id = $1 AND (title IS NULL OR title = '') AND notes IS NOT NULL AND notes <> ''`,
      [userId]
    );

    const updates = [];
    for (const r of rows.rows) {
      const lines = r.notes.split(/\r?\n/);
      let foundTitle = null;
      let remainingLines = [];
      for (const line of lines) {
        const trimmed = line.trim();
        if (!trimmed || foundTitle) { remainingLines.push(line); continue; }
        let matched = false;
        for (const pat of TITLE_PATTERNS) {
          const m = trimmed.match(pat);
          if (m) {
            // Use the whole line if short, else just the matched phrase
            foundTitle = trimmed.length <= 60 ? trimmed : m[0];
            matched = true;
            break;
          }
        }
        if (!matched) remainingLines.push(line);
      }
      if (foundTitle) {
        const newNotes = remainingLines.join("\n").replace(/\n{3,}/g, "\n\n").trim() || null;
        updates.push({ id: r.id, name: r.name, title: foundTitle, newNotes });
      }
    }

    if (!dryRun) {
      for (const u of updates) {
        await pool.query(
          `UPDATE crm_contacts SET title = $1, notes = $2, updated_at = NOW() WHERE id = $3`,
          [u.title, u.newNotes, u.id]
        );
      }
    }

    res.json({
      total: rows.rows.length,
      migrated: updates.length,
      preview: updates.slice(0, 20).map(u => ({ id: u.id, name: u.name, title: u.title })),
      dryRun: !!dryRun,
    });
  } catch (error) {
    console.error("Title migration error:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

/* =========================================================
   CRM – Import contacts (preview + execute)
   ========================================================= */

const normEmail = (e) => (e || "").toLowerCase().trim();

app.post("/api/crm/contacts/import-preview", async (req, res) => {
  try {
    const { userId, rows } = req.body;
    if (!userId || !Array.isArray(rows)) return res.status(400).json({ error: "userId and rows array required" });

    // Fetch all existing contacts once for fast in-memory matching
    const existing = await pool.query(
      `SELECT id, name, company, phone, email FROM crm_contacts WHERE user_id = $1`,
      [userId]
    );
    const byPhone = new Map();
    const byEmail = new Map();
    for (const e of existing.rows) {
      const ph = normPhone(e.phone);
      const em = normEmail(e.email);
      if (ph && ph.length >= 7) byPhone.set(ph.slice(-9), e);
      if (em) byEmail.set(em, e);
    }

    const preview = rows.map((r, idx) => {
      const ph = normPhone(r.phone);
      const em = normEmail(r.email);
      let match = null;
      if (em && byEmail.has(em)) match = byEmail.get(em);
      else if (ph && ph.length >= 7 && byPhone.has(ph.slice(-9))) match = byPhone.get(ph.slice(-9));
      return {
        rowIdx: idx,
        data: r,
        match,
        action: match ? "merge" : "create", // default suggestion
      };
    });

    res.json({
      total: rows.length,
      duplicates: preview.filter(p => p.match).length,
      newCount: preview.filter(p => !p.match).length,
      preview,
    });
  } catch (error) {
    console.error("Import preview error:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

app.post("/api/crm/contacts/import-execute", async (req, res) => {
  try {
    const { userId, rows, defaultFolderId } = req.body;
    // rows: [{ data, action: 'create'|'merge'|'skip', matchId? }, ...]
    if (!userId || !Array.isArray(rows)) return res.status(400).json({ error: "userId and rows array required" });

    let created = 0, merged = 0, skipped = 0, failed = 0;
    const errors = [];

    for (const r of rows) {
      try {
        if (r.action === "skip") { skipped++; continue; }
        const d = r.data || {};
        if (!d.name) { skipped++; continue; }

        if (r.action === "merge" && r.matchId) {
          // Update only fields that are currently empty on the existing contact
          const existing = await pool.query(`SELECT * FROM crm_contacts WHERE id = $1 AND user_id = $2`, [r.matchId, userId]);
          if (existing.rows.length === 0) { skipped++; continue; }
          const cur = existing.rows[0];
          const setFields = [];
          const setVals = [];
          let idx = 1;
          const fillIfEmpty = (col, camel) => {
            if (d[camel] && (cur[col] === null || cur[col] === "")) {
              setFields.push(`${col} = $${idx++}`);
              setVals.push(d[camel]);
            }
          };
          fillIfEmpty("title", "title");
          fillIfEmpty("company", "company");
          fillIfEmpty("phone", "phone");
          fillIfEmpty("phone_alt", "phoneAlt");
          fillIfEmpty("email", "email");
          fillIfEmpty("website", "website");
          fillIfEmpty("country", "country");
          fillIfEmpty("city", "city");
          fillIfEmpty("address", "address");
          if (d.notes) {
            const newNotes = (cur.notes || "") + (cur.notes ? "\n---\n" : "") + d.notes;
            setFields.push(`notes = $${idx++}`); setVals.push(newNotes);
          }
          if (defaultFolderId && !cur.folder_id) {
            setFields.push(`folder_id = $${idx++}`); setVals.push(defaultFolderId);
          }
          if (setFields.length > 0) {
            setFields.push(`updated_at = NOW()`);
            setVals.push(r.matchId);
            await pool.query(`UPDATE crm_contacts SET ${setFields.join(", ")} WHERE id = $${idx}`, setVals);
          }
          merged++;
        } else {
          await pool.query(
            `INSERT INTO crm_contacts (
               user_id, name, type, title, company, phone, phone_alt, email, website,
               country, city, address, source, status, tags, notes, folder_id
             ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)`,
            [
              userId, d.name, d.type || "lead", d.title || null, d.company || null,
              d.phone || null, d.phoneAlt || null, d.email || null, d.website || null,
              d.country || null, d.city || null, d.address || null, d.source || "import", "active",
              JSON.stringify(d.tags || []), d.notes || null, defaultFolderId || null,
            ]
          );
          created++;
        }
      } catch (e) {
        failed++;
        errors.push({ rowIdx: r.rowIdx, error: e.message });
      }
    }

    res.json({ created, merged, skipped, failed, errors });
  } catch (error) {
    console.error("Import execute error:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

/* =========================================================
   CRM – Email broadcast (Resend)
   ========================================================= */

app.post("/api/crm/email/send-broadcast", async (req, res) => {
  try {
    const { userId, contactIds, subject, html, text, fromName, replyTo, dryRun, provider } = req.body;
    if (!userId || !Array.isArray(contactIds) || contactIds.length === 0) {
      return res.status(400).json({ error: "userId and contactIds required" });
    }
    if (!subject || (!html && !text)) {
      return res.status(400).json({ error: "subject and (html or text) required" });
    }

    const sendProvider = provider === "outlook" ? "outlook" : "resend";

    let outlookAccessToken = null;
    if (sendProvider === "outlook") {
      try {
        outlookAccessToken = await getValidOutlookToken(userId);
      } catch (e) {
        return res.status(400).json({ error: e.message });
      }
    } else {
      if (!process.env.RESEND_API_KEY) {
        return res.status(500).json({
          error: "RESEND_API_KEY is not configured. Sign up at resend.com (free 3,000/month), add the API key to your server environment, and try again."
        });
      }
    }

    const fromEmail = process.env.RESEND_FROM_EMAIL || "onboarding@resend.dev";
    const senderName = (fromName || "GEMS DNA").replace(/[\r\n]/g, "");
    const fromHeader = `${senderName} <${fromEmail}>`;

    // Fetch recipients
    const recipientsRes = await pool.query(
      `SELECT id, name, email, company, title FROM crm_contacts
       WHERE user_id = $1 AND id = ANY($2::int[]) AND email IS NOT NULL AND email <> ''`,
      [userId, contactIds.map(Number)]
    );
    const recipients = recipientsRes.rows;

    if (recipients.length === 0) {
      return res.status(400).json({ error: "None of the selected contacts have an email address." });
    }

    if (dryRun) {
      return res.json({
        dryRun: true,
        provider: sendProvider,
        wouldSend: recipients.length,
        recipients: recipients.map(r => ({ id: r.id, name: r.name, email: r.email })),
      });
    }

    const personalize = (template, c) => {
      if (!template) return template;
      const firstName = (c.name || "").split(/\s+/)[0] || "";
      return template
        .replace(/\{\{?\s*name\s*\}?\}/gi, c.name || "")
        .replace(/\{\{?\s*firstName\s*\}?\}/gi, firstName)
        .replace(/\{\{?\s*company\s*\}?\}/gi, c.company || "")
        .replace(/\{\{?\s*title\s*\}?\}/gi, c.title || "");
    };

    const details = [];
    let sent = 0, failed = 0;

    for (const r of recipients) {
      try {
        const subj = personalize(subject, r);
        const personalizedHtml = html ? personalize(html, r) : null;
        const personalizedText = text ? personalize(text, r) : null;

        let ok = false, errTxt = "";

        if (sendProvider === "outlook") {
          const sendRes = await fetch("https://graph.microsoft.com/v1.0/me/sendMail", {
            method: "POST",
            headers: { Authorization: `Bearer ${outlookAccessToken}`, "Content-Type": "application/json" },
            body: JSON.stringify({
              message: {
                subject: subj,
                body: { contentType: personalizedHtml ? "HTML" : "Text", content: personalizedHtml || personalizedText },
                toRecipients: [{ emailAddress: { address: r.email, name: r.name } }],
              },
              saveToSentItems: true,
            }),
          });
          ok = sendRes.ok;
          if (!ok) errTxt = (await sendRes.text()).slice(0, 300);
        } else {
          const body = {
            from: fromHeader,
            to: [r.email],
            subject: subj,
            ...(personalizedHtml ? { html: personalizedHtml } : {}),
            ...(personalizedText ? { text: personalizedText } : {}),
            ...(replyTo ? { reply_to: replyTo } : {}),
          };
          const sendRes = await fetch("https://api.resend.com/emails", {
            method: "POST",
            headers: {
              "Content-Type": "application/json",
              "Authorization": `Bearer ${process.env.RESEND_API_KEY}`,
            },
            body: JSON.stringify(body),
          });
          ok = sendRes.ok;
          if (!ok) errTxt = (await sendRes.text()).slice(0, 300);
        }

        if (!ok) {
          failed++;
          details.push({ contactId: r.id, email: r.email, status: "failed", error: errTxt });
        } else {
          sent++;
          details.push({ contactId: r.id, email: r.email, status: "sent" });
          await pool.query(
            `INSERT INTO crm_interactions (user_id, contact_id, type, direction, subject, content, metadata)
             VALUES ($1, $2, 'email', 'outgoing', $3, $4, $5)`,
            [userId, r.id, subj, personalizedText, JSON.stringify({ broadcast: true, provider: sendProvider })]
          );
          await pool.query(`UPDATE crm_contacts SET last_contact_at = NOW(), updated_at = NOW() WHERE id = $1`, [r.id]);
        }
      } catch (e) {
        failed++;
        details.push({ contactId: r.id, email: r.email, status: "failed", error: e.message });
      }
    }

    // Save broadcast log
    await pool.query(
      `INSERT INTO crm_email_broadcasts (user_id, subject, body, recipients_count, sent_count, failed_count, details)
       VALUES ($1, $2, $3, $4, $5, $6, $7)`,
      [userId, subject, html || text, recipients.length, sent, failed, JSON.stringify({ provider: sendProvider, details })]
    );

    res.json({ sent, failed, total: recipients.length, provider: sendProvider, details });
  } catch (error) {
    console.error("Email broadcast error:", error);
    res.status(500).json({ error: error.message || "Internal server error" });
  }
});

app.get("/api/crm/email/broadcasts", async (req, res) => {
  try {
    const { userId } = req.query;
    if (!userId) return res.status(400).json({ error: "userId is required" });
    const result = await pool.query(
      `SELECT id, subject, recipients_count, sent_count, failed_count, sent_at
       FROM crm_email_broadcasts WHERE user_id = $1 ORDER BY sent_at DESC LIMIT 50`,
      [userId]
    );
    res.json(result.rows);
  } catch (error) {
    console.error("Broadcast log fetch error:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

/* =========================================================
   CRM – Email templates (saved HTML templates per user)
   ========================================================= */

app.get("/api/crm/email/templates", async (req, res) => {
  try {
    const { userId } = req.query;
    if (!userId) return res.status(400).json({ error: "userId is required" });
    const result = await pool.query(
      `SELECT id, name, subject, html, thumbnail, created_at, updated_at
       FROM crm_email_templates WHERE user_id = $1 ORDER BY updated_at DESC`,
      [userId]
    );
    res.json(result.rows);
  } catch (error) {
    console.error("Email templates fetch error:", error);
    res.status(500).json({ error: error.message || "Internal server error" });
  }
});

app.post("/api/crm/email/templates", async (req, res) => {
  try {
    const { userId, name, subject, html, thumbnail } = req.body || {};
    if (!userId || !name) return res.status(400).json({ error: "userId and name are required" });
    const result = await pool.query(
      `INSERT INTO crm_email_templates (user_id, name, subject, html, thumbnail)
       VALUES ($1, $2, $3, $4, $5)
       RETURNING id, name, subject, html, thumbnail, created_at, updated_at`,
      [userId, name, subject || "", html || "", thumbnail || null]
    );
    res.json(result.rows[0]);
  } catch (error) {
    console.error("Email template create error:", error);
    res.status(500).json({ error: error.message || "Internal server error" });
  }
});

app.put("/api/crm/email/templates/:id", async (req, res) => {
  try {
    const { id } = req.params;
    const { name, subject, html, thumbnail } = req.body || {};
    const fields = [];
    const values = [];
    let i = 1;
    if (name !== undefined) { fields.push(`name = $${i++}`); values.push(name); }
    if (subject !== undefined) { fields.push(`subject = $${i++}`); values.push(subject); }
    if (html !== undefined) { fields.push(`html = $${i++}`); values.push(html); }
    if (thumbnail !== undefined) { fields.push(`thumbnail = $${i++}`); values.push(thumbnail); }
    if (fields.length === 0) return res.status(400).json({ error: "No fields to update" });
    fields.push(`updated_at = NOW()`);
    values.push(id);
    const result = await pool.query(
      `UPDATE crm_email_templates SET ${fields.join(", ")} WHERE id = $${i}
       RETURNING id, name, subject, html, thumbnail, created_at, updated_at`,
      values
    );
    if (result.rows.length === 0) return res.status(404).json({ error: "Template not found" });
    res.json(result.rows[0]);
  } catch (error) {
    console.error("Email template update error:", error);
    res.status(500).json({ error: error.message || "Internal server error" });
  }
});

app.delete("/api/crm/email/templates/:id", async (req, res) => {
  try {
    const { id } = req.params;
    await pool.query(`DELETE FROM crm_email_templates WHERE id = $1`, [id]);
    res.json({ success: true });
  } catch (error) {
    console.error("Email template delete error:", error);
    res.status(500).json({ error: error.message || "Internal server error" });
  }
});

/* =========================================================
   CRM – Outlook / Microsoft Graph integration
   ========================================================= */

const OUTLOOK_TENANT = process.env.OUTLOOK_TENANT || "common";
const OUTLOOK_CLIENT_ID = process.env.OUTLOOK_CLIENT_ID;
const OUTLOOK_CLIENT_SECRET = process.env.OUTLOOK_CLIENT_SECRET;
const OUTLOOK_REDIRECT_URI = process.env.OUTLOOK_REDIRECT_URI; // e.g. https://gems-dna-be.onrender.com/api/crm/outlook/callback
const FRONTEND_URL = process.env.FRONTEND_URL || "https://gems-dna-fe.vercel.app";
const OUTLOOK_SCOPES = [
  "offline_access",
  "User.Read",
  "Contacts.ReadWrite",
  "Mail.Send",
  "Mail.Read",
];

const outlookConfigured = () => !!(OUTLOOK_CLIENT_ID && OUTLOOK_CLIENT_SECRET && OUTLOOK_REDIRECT_URI);

// Save (encrypted) integration record. Upsert by (user_id, provider).
const saveIntegration = async ({ userId, provider, accessToken, refreshToken, expiresIn, scope, accountEmail, accountName, metadata }) => {
  const expiresAt = expiresIn ? new Date(Date.now() + (expiresIn - 60) * 1000) : null;
  const accessEnc = accessToken ? encrypt(accessToken) : null;
  const refreshEnc = refreshToken ? encrypt(refreshToken) : null;
  await pool.query(
    `INSERT INTO crm_integrations (user_id, provider, account_email, account_name, access_token_enc, refresh_token_enc, expires_at, scope, metadata)
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
     ON CONFLICT (user_id, provider) DO UPDATE SET
       account_email = EXCLUDED.account_email,
       account_name = EXCLUDED.account_name,
       access_token_enc = EXCLUDED.access_token_enc,
       refresh_token_enc = COALESCE(EXCLUDED.refresh_token_enc, crm_integrations.refresh_token_enc),
       expires_at = EXCLUDED.expires_at,
       scope = EXCLUDED.scope,
       metadata = EXCLUDED.metadata,
       updated_at = NOW()`,
    [userId, provider, accountEmail || null, accountName || null, accessEnc, refreshEnc, expiresAt, scope || null, JSON.stringify(metadata || {})]
  );
};

const getIntegration = async (userId, provider) => {
  const r = await pool.query(
    `SELECT * FROM crm_integrations WHERE user_id = $1 AND provider = $2 LIMIT 1`,
    [userId, provider]
  );
  if (r.rows.length === 0) return null;
  const row = r.rows[0];
  return {
    ...row,
    access_token: decrypt(row.access_token_enc),
    refresh_token: decrypt(row.refresh_token_enc),
  };
};

// Refresh access token if expired
const getValidOutlookToken = async (userId) => {
  const integ = await getIntegration(userId, "outlook");
  if (!integ) throw new Error("Outlook is not connected for this user");
  if (!integ.access_token) throw new Error("Outlook tokens are missing — please reconnect");

  const expiresSoon = !integ.expires_at || new Date(integ.expires_at).getTime() < Date.now() + 30000;
  if (!expiresSoon) return integ.access_token;

  if (!integ.refresh_token) throw new Error("Outlook session expired — please reconnect");

  const tokenRes = await fetch(`https://login.microsoftonline.com/${OUTLOOK_TENANT}/oauth2/v2.0/token`, {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: new URLSearchParams({
      client_id: OUTLOOK_CLIENT_ID,
      client_secret: OUTLOOK_CLIENT_SECRET,
      grant_type: "refresh_token",
      refresh_token: integ.refresh_token,
      scope: OUTLOOK_SCOPES.join(" "),
    }).toString(),
  });
  if (!tokenRes.ok) {
    const err = await tokenRes.text();
    throw new Error(`Failed to refresh Outlook token: ${err.slice(0, 300)}`);
  }
  const td = await tokenRes.json();
  await saveIntegration({
    userId,
    provider: "outlook",
    accessToken: td.access_token,
    refreshToken: td.refresh_token || integ.refresh_token,
    expiresIn: td.expires_in,
    scope: td.scope || integ.scope,
    accountEmail: integ.account_email,
    accountName: integ.account_name,
  });
  return td.access_token;
};

// 1. Auth URL
app.get("/api/crm/outlook/auth-url", (req, res) => {
  if (!outlookConfigured()) {
    return res.status(500).json({
      error: "Outlook is not configured on the server. The administrator needs to set OUTLOOK_CLIENT_ID, OUTLOOK_CLIENT_SECRET and OUTLOOK_REDIRECT_URI."
    });
  }
  const { userId } = req.query;
  if (!userId) return res.status(400).json({ error: "userId is required" });

  const state = encrypt(JSON.stringify({ userId, ts: Date.now() }));
  const params = new URLSearchParams({
    client_id: OUTLOOK_CLIENT_ID,
    response_type: "code",
    redirect_uri: OUTLOOK_REDIRECT_URI,
    response_mode: "query",
    scope: OUTLOOK_SCOPES.join(" "),
    state,
    prompt: "select_account",
  });
  const url = `https://login.microsoftonline.com/${OUTLOOK_TENANT}/oauth2/v2.0/authorize?${params.toString()}`;
  res.json({ url });
});

// 2. Callback (Microsoft redirects here)
app.get("/api/crm/outlook/callback", async (req, res) => {
  const { code, state, error: authErr, error_description } = req.query;
  const redirectBack = (status, msg) => {
    const u = new URL(`${FRONTEND_URL}/crm/settings`);
    u.searchParams.set("outlook", status);
    if (msg) u.searchParams.set("msg", String(msg).slice(0, 200));
    res.redirect(u.toString());
  };

  if (authErr) return redirectBack("error", error_description || authErr);
  if (!code || !state) return redirectBack("error", "Missing code or state");

  try {
    const decoded = JSON.parse(decrypt(state));
    const userId = decoded.userId;
    if (!userId) return redirectBack("error", "Invalid state");
    if (Date.now() - decoded.ts > 10 * 60 * 1000) return redirectBack("error", "Auth link expired");

    // Exchange code for tokens
    const tokenRes = await fetch(`https://login.microsoftonline.com/${OUTLOOK_TENANT}/oauth2/v2.0/token`, {
      method: "POST",
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      body: new URLSearchParams({
        client_id: OUTLOOK_CLIENT_ID,
        client_secret: OUTLOOK_CLIENT_SECRET,
        grant_type: "authorization_code",
        code,
        redirect_uri: OUTLOOK_REDIRECT_URI,
        scope: OUTLOOK_SCOPES.join(" "),
      }).toString(),
    });
    if (!tokenRes.ok) {
      const t = await tokenRes.text();
      console.error("Outlook token exchange failed:", t);
      return redirectBack("error", `Token exchange failed (${tokenRes.status})`);
    }
    const td = await tokenRes.json();

    // Get user profile (email + name)
    let accountEmail = null, accountName = null;
    try {
      const meRes = await fetch("https://graph.microsoft.com/v1.0/me", {
        headers: { Authorization: `Bearer ${td.access_token}` },
      });
      if (meRes.ok) {
        const me = await meRes.json();
        accountEmail = me.mail || me.userPrincipalName || null;
        accountName = me.displayName || null;
      }
    } catch (_) {}

    await saveIntegration({
      userId,
      provider: "outlook",
      accessToken: td.access_token,
      refreshToken: td.refresh_token,
      expiresIn: td.expires_in,
      scope: td.scope,
      accountEmail,
      accountName,
    });

    return redirectBack("connected", accountEmail || "Outlook account connected");
  } catch (e) {
    console.error("Outlook callback error:", e);
    return redirectBack("error", e.message);
  }
});

// 3. Status
app.get("/api/crm/outlook/status", async (req, res) => {
  try {
    const { userId } = req.query;
    if (!userId) return res.status(400).json({ error: "userId is required" });
    const integ = await getIntegration(userId, "outlook");
    if (!integ) return res.json({ connected: false, configured: outlookConfigured() });
    res.json({
      connected: true,
      configured: outlookConfigured(),
      accountEmail: integ.account_email,
      accountName: integ.account_name,
      lastSyncAt: integ.last_sync_at,
      expiresAt: integ.expires_at,
    });
  } catch (e) {
    console.error("Outlook status error:", e);
    res.status(500).json({ error: e.message });
  }
});

// 4. Disconnect
app.post("/api/crm/outlook/disconnect", async (req, res) => {
  try {
    const { userId } = req.body;
    if (!userId) return res.status(400).json({ error: "userId is required" });
    await pool.query(`DELETE FROM crm_integrations WHERE user_id = $1 AND provider = 'outlook'`, [userId]);
    res.json({ success: true });
  } catch (e) {
    console.error("Outlook disconnect error:", e);
    res.status(500).json({ error: e.message });
  }
});

/* ----- Two-way contact sync ----- */
const outlookContactToCrm = (oc) => {
  const phones = [...(oc.businessPhones || []), ...(oc.homePhones || []), oc.mobilePhone].filter(Boolean);
  const emails = (oc.emailAddresses || []).map((e) => e.address).filter(Boolean);
  const addr = (oc.businessAddress || oc.homeAddress || {});
  return {
    name: oc.displayName || [oc.givenName, oc.surname].filter(Boolean).join(" ") || (emails[0] || "Unnamed"),
    title: oc.jobTitle || null,
    company: oc.companyName || null,
    phone: phones[0] || null,
    phoneAlt: phones[1] || null,
    email: emails[0] || null,
    website: oc.businessHomePage || (oc.websites && oc.websites[0]?.address) || null,
    country: addr.countryOrRegion || null,
    city: addr.city || null,
    address: [addr.street, addr.postalCode].filter(Boolean).join(", ") || null,
    notes: oc.personalNotes || null,
  };
};

const crmContactToOutlook = (c) => {
  const out = { displayName: c.name };
  const [given, ...rest] = (c.name || "").split(/\s+/);
  if (given) out.givenName = given;
  if (rest.length) out.surname = rest.join(" ");
  if (c.title) out.jobTitle = c.title;
  if (c.company) out.companyName = c.company;
  const businessPhones = [c.phone, c.phone_alt].filter(Boolean);
  if (businessPhones.length) out.businessPhones = businessPhones;
  if (c.email) out.emailAddresses = [{ address: c.email, name: c.name }];
  if (c.website) out.businessHomePage = c.website;
  if (c.notes) out.personalNotes = c.notes;
  if (c.country || c.city || c.address) {
    out.businessAddress = {
      ...(c.address ? { street: c.address } : {}),
      ...(c.city ? { city: c.city } : {}),
      ...(c.country ? { countryOrRegion: c.country } : {}),
    };
  }
  return out;
};

app.post("/api/crm/outlook/sync-contacts", async (req, res) => {
  try {
    const { userId, direction } = req.body;
    if (!userId) return res.status(400).json({ error: "userId is required" });
    const dir = direction || "two-way"; // 'pull' | 'push' | 'two-way'

    const accessToken = await getValidOutlookToken(userId);

    let pulledNew = 0, pulledUpdated = 0, pushedNew = 0, pushedUpdated = 0;

    /* ---- PULL Outlook -> CRM ---- */
    if (dir === "pull" || dir === "two-way") {
      let url = "https://graph.microsoft.com/v1.0/me/contacts?$top=100&$select=id,displayName,givenName,surname,jobTitle,companyName,businessPhones,homePhones,mobilePhone,emailAddresses,businessAddress,homeAddress,businessHomePage,personalNotes,lastModifiedDateTime";
      let pages = 0;
      while (url && pages < 20) {
        const r = await fetch(url, { headers: { Authorization: `Bearer ${accessToken}` } });
        if (!r.ok) {
          const t = await r.text();
          throw new Error(`Outlook contacts fetch failed (${r.status}): ${t.slice(0, 200)}`);
        }
        const data = await r.json();
        for (const oc of (data.value || [])) {
          const mapped = outlookContactToCrm(oc);
          // Find existing by outlook_contact_id, then by email/phone
          const existRes = await pool.query(
            `SELECT id, name, title, company, phone, phone_alt, email, website, country, city, address, notes
               FROM crm_contacts
              WHERE user_id = $1 AND (
                outlook_contact_id = $2
                OR (LOWER(email) = LOWER($3) AND $3 <> '' AND $3 IS NOT NULL)
              ) LIMIT 1`,
            [userId, oc.id, mapped.email || ""]
          );
          if (existRes.rows.length === 0) {
            await pool.query(
              `INSERT INTO crm_contacts (
                 user_id, name, type, title, company, phone, phone_alt, email, website,
                 country, city, address, source, status, notes, outlook_contact_id, outlook_synced_at
               ) VALUES ($1,$2,'lead',$3,$4,$5,$6,$7,$8,$9,$10,$11,'outlook','active',$12,$13,NOW())`,
              [userId, mapped.name, mapped.title, mapped.company, mapped.phone, mapped.phoneAlt,
               mapped.email, mapped.website, mapped.country, mapped.city, mapped.address, mapped.notes, oc.id]
            );
            pulledNew++;
          } else {
            // Fill empty fields only (non-destructive merge)
            const cur = existRes.rows[0];
            const sets = [];
            const vals = [];
            let i = 1;
            const fillIfEmpty = (col, val) => {
              if (val && (cur[col] === null || cur[col] === "")) {
                sets.push(`${col} = $${i++}`); vals.push(val);
              }
            };
            fillIfEmpty("title", mapped.title);
            fillIfEmpty("company", mapped.company);
            fillIfEmpty("phone", mapped.phone);
            fillIfEmpty("phone_alt", mapped.phoneAlt);
            fillIfEmpty("email", mapped.email);
            fillIfEmpty("website", mapped.website);
            fillIfEmpty("country", mapped.country);
            fillIfEmpty("city", mapped.city);
            fillIfEmpty("address", mapped.address);
            // Always update the link
            sets.push(`outlook_contact_id = $${i++}`); vals.push(oc.id);
            sets.push(`outlook_synced_at = NOW()`);
            sets.push(`updated_at = NOW()`);
            vals.push(cur.id);
            await pool.query(`UPDATE crm_contacts SET ${sets.join(", ")} WHERE id = $${i}`, vals);
            pulledUpdated++;
          }
        }
        url = data["@odata.nextLink"] || null;
        pages++;
      }
    }

    /* ---- PUSH CRM -> Outlook ---- */
    if (dir === "push" || dir === "two-way") {
      // Push CRM contacts that have an email and either no outlook_contact_id, or were updated since last sync
      const pushRows = await pool.query(
        `SELECT * FROM crm_contacts
          WHERE user_id = $1
            AND (
              outlook_contact_id IS NULL
              OR (outlook_synced_at IS NULL OR updated_at > outlook_synced_at)
            )
            AND name IS NOT NULL
          ORDER BY updated_at DESC
          LIMIT 200`,
        [userId]
      );
      for (const c of pushRows.rows) {
        const body = JSON.stringify(crmContactToOutlook(c));
        try {
          if (c.outlook_contact_id) {
            const u = `https://graph.microsoft.com/v1.0/me/contacts/${encodeURIComponent(c.outlook_contact_id)}`;
            const r = await fetch(u, {
              method: "PATCH",
              headers: { Authorization: `Bearer ${accessToken}`, "Content-Type": "application/json" },
              body,
            });
            if (r.ok) {
              await pool.query(`UPDATE crm_contacts SET outlook_synced_at = NOW() WHERE id = $1`, [c.id]);
              pushedUpdated++;
            }
          } else {
            const r = await fetch("https://graph.microsoft.com/v1.0/me/contacts", {
              method: "POST",
              headers: { Authorization: `Bearer ${accessToken}`, "Content-Type": "application/json" },
              body,
            });
            if (r.ok) {
              const created = await r.json();
              await pool.query(
                `UPDATE crm_contacts SET outlook_contact_id = $1, outlook_synced_at = NOW() WHERE id = $2`,
                [created.id, c.id]
              );
              pushedNew++;
            }
          }
        } catch (e) {
          console.warn("Push contact failed:", c.id, e.message);
        }
      }
    }

    await pool.query(
      `UPDATE crm_integrations SET last_sync_at = NOW(), updated_at = NOW() WHERE user_id = $1 AND provider = 'outlook'`,
      [userId]
    );

    res.json({ direction: dir, pulledNew, pulledUpdated, pushedNew, pushedUpdated });
  } catch (e) {
    console.error("Outlook sync error:", e);
    res.status(500).json({ error: e.message });
  }
});

/* ----- Send a single email through Outlook ----- */
app.post("/api/crm/outlook/send-email", async (req, res) => {
  try {
    const { userId, to, subject, html, text } = req.body;
    if (!userId || !to || !subject || (!html && !text)) {
      return res.status(400).json({ error: "userId, to, subject and html|text are required" });
    }
    const accessToken = await getValidOutlookToken(userId);
    const recipients = (Array.isArray(to) ? to : [to]).map((addr) => ({ emailAddress: { address: addr } }));
    const r = await fetch("https://graph.microsoft.com/v1.0/me/sendMail", {
      method: "POST",
      headers: { Authorization: `Bearer ${accessToken}`, "Content-Type": "application/json" },
      body: JSON.stringify({
        message: {
          subject,
          body: { contentType: html ? "HTML" : "Text", content: html || text },
          toRecipients: recipients,
        },
        saveToSentItems: true,
      }),
    });
    if (!r.ok) {
      const t = await r.text();
      return res.status(502).json({ error: `Outlook send failed (${r.status}): ${t.slice(0, 200)}` });
    }
    res.json({ success: true });
  } catch (e) {
    console.error("Outlook send error:", e);
    res.status(500).json({ error: e.message });
  }
});

/* ----- Import recent inbox emails as interactions ----- */
app.post("/api/crm/outlook/import-emails", async (req, res) => {
  try {
    const { userId, days } = req.body;
    if (!userId) return res.status(400).json({ error: "userId is required" });
    const lookbackDays = Math.min(Math.max(parseInt(days, 10) || 7, 1), 90);

    const accessToken = await getValidOutlookToken(userId);

    // Build email -> contact lookup map
    const contactsRes = await pool.query(
      `SELECT id, email FROM crm_contacts WHERE user_id = $1 AND email IS NOT NULL AND email <> ''`,
      [userId]
    );
    const emailToId = new Map();
    for (const c of contactsRes.rows) emailToId.set(c.email.toLowerCase().trim(), c.id);
    if (emailToId.size === 0) return res.json({ imported: 0, scanned: 0, message: "No contacts with email" });

    const since = new Date(Date.now() - lookbackDays * 86400 * 1000).toISOString();
    let scanned = 0, imported = 0;

    for (const folder of ["inbox", "sentitems"]) {
      let url = `https://graph.microsoft.com/v1.0/me/mailFolders/${folder}/messages?$top=50&$select=id,subject,bodyPreview,from,toRecipients,receivedDateTime,sentDateTime&$filter=receivedDateTime ge ${since}`;
      let pages = 0;
      while (url && pages < 5) {
        const r = await fetch(url, { headers: { Authorization: `Bearer ${accessToken}` } });
        if (!r.ok) break;
        const data = await r.json();
        for (const msg of (data.value || [])) {
          scanned++;
          const fromAddr = (msg.from?.emailAddress?.address || "").toLowerCase();
          const toAddrs = (msg.toRecipients || []).map((t) => (t.emailAddress?.address || "").toLowerCase());
          const direction = folder === "sentitems" ? "outgoing" : "incoming";
          const counterpart = direction === "outgoing" ? toAddrs : [fromAddr];
          for (const addr of counterpart) {
            const cId = emailToId.get(addr);
            if (!cId) continue;
            // Avoid duplicates by metadata.outlookMessageId
            const dupe = await pool.query(
              `SELECT 1 FROM crm_interactions WHERE contact_id = $1 AND metadata->>'outlookMessageId' = $2 LIMIT 1`,
              [cId, msg.id]
            );
            if (dupe.rows.length > 0) continue;
            await pool.query(
              `INSERT INTO crm_interactions (user_id, contact_id, type, direction, subject, content, metadata, occurred_at)
               VALUES ($1, $2, 'email', $3, $4, $5, $6, $7)`,
              [
                userId, cId, direction,
                msg.subject || null,
                msg.bodyPreview || null,
                JSON.stringify({ outlookMessageId: msg.id, source: "outlook" }),
                msg.receivedDateTime || msg.sentDateTime || new Date().toISOString(),
              ]
            );
            await pool.query(`UPDATE crm_contacts SET last_contact_at = NOW(), updated_at = NOW() WHERE id = $1`, [cId]);
            imported++;
          }
        }
        url = data["@odata.nextLink"] || null;
        pages++;
      }
    }

    res.json({ imported, scanned, lookbackDays });
  } catch (e) {
    console.error("Outlook import-emails error:", e);
    res.status(500).json({ error: e.message });
  }
});

/* =========================================================
   JEWELRY PRODUCTION SYSTEM
   ========================================================= */
const multer = require('multer');
let blobPut = null;
try {
  blobPut = require('@vercel/blob').put;
} catch (e) {
  console.warn('@vercel/blob not installed yet - blob uploads disabled until deps installed');
}

const blobUpload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 50 * 1024 * 1024 },
});

const JEWELRY_STATUSES = [
  'draft', 'design', 'cad', 'wax', 'casting', 'setting',
  'polishing', 'qc', 'ready', 'sold', 'archived',
];

const JEWELRY_TYPES = ['custom', 'stock'];

const isValidStatus = (s) => JEWELRY_STATUSES.includes(s);
const isValidType = (t) => JEWELRY_TYPES.includes(t);

let jewelryReady = false;
const jewelryReadyPromise = (async () => {
  try {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS jewelry_items (
        id SERIAL PRIMARY KEY,
        user_id TEXT NOT NULL,
        location TEXT,
        sku TEXT UNIQUE,
        name TEXT NOT NULL,
        type TEXT NOT NULL DEFAULT 'custom',
        status TEXT NOT NULL DEFAULT 'draft',
        contact_id INTEGER REFERENCES crm_contacts(id) ON DELETE SET NULL,
        deal_id INTEGER REFERENCES crm_deals(id) ON DELETE SET NULL,
        category TEXT,
        metal_summary TEXT,
        weight_grams NUMERIC(10,3),
        size TEXT,
        description TEXT,
        internal_notes TEXT,
        total_cost NUMERIC(14,2) DEFAULT 0,
        markup_percent NUMERIC(6,2) DEFAULT 0,
        sale_price NUMERIC(14,2),
        sold_at TIMESTAMP,
        sold_to INTEGER REFERENCES crm_contacts(id) ON DELETE SET NULL,
        sold_deal_id INTEGER REFERENCES crm_deals(id) ON DELETE SET NULL,
        cover_image_url TEXT,
        created_at TIMESTAMP DEFAULT NOW(),
        updated_at TIMESTAMP DEFAULT NOW()
      )
    `);

    await pool.query(`
      CREATE TABLE IF NOT EXISTS jewelry_item_stones (
        id SERIAL PRIMARY KEY,
        item_id INTEGER NOT NULL REFERENCES jewelry_items(id) ON DELETE CASCADE,
        stone_sku TEXT,
        role TEXT,
        quantity INTEGER DEFAULT 1,
        snapshot JSONB,
        notes TEXT,
        created_at TIMESTAMP DEFAULT NOW(),
        updated_at TIMESTAMP DEFAULT NOW()
      )
    `);

    await pool.query(`
      CREATE TABLE IF NOT EXISTS jewelry_item_metals (
        id SERIAL PRIMARY KEY,
        item_id INTEGER NOT NULL REFERENCES jewelry_items(id) ON DELETE CASCADE,
        metal_type TEXT,
        purity TEXT,
        color TEXT,
        weight_grams NUMERIC(10,3) NOT NULL,
        price_per_gram NUMERIC(10,2),
        total_cost NUMERIC(14,2)
      )
    `);

    await pool.query(`
      CREATE TABLE IF NOT EXISTS jewelry_item_costs (
        id SERIAL PRIMARY KEY,
        item_id INTEGER NOT NULL REFERENCES jewelry_items(id) ON DELETE CASCADE,
        label TEXT NOT NULL,
        category TEXT,
        amount NUMERIC(14,2) NOT NULL,
        notes TEXT
      )
    `);

    await pool.query(`
      CREATE TABLE IF NOT EXISTS jewelry_item_files (
        id SERIAL PRIMARY KEY,
        item_id INTEGER NOT NULL REFERENCES jewelry_items(id) ON DELETE CASCADE,
        url TEXT NOT NULL,
        kind TEXT,
        stage TEXT,
        filename TEXT,
        mime_type TEXT,
        size_bytes INTEGER,
        uploaded_by TEXT,
        uploaded_at TIMESTAMP DEFAULT NOW()
      )
    `);

    await pool.query(`
      CREATE TABLE IF NOT EXISTS jewelry_item_history (
        id SERIAL PRIMARY KEY,
        item_id INTEGER NOT NULL REFERENCES jewelry_items(id) ON DELETE CASCADE,
        from_status TEXT,
        to_status TEXT NOT NULL,
        changed_by TEXT,
        changed_at TIMESTAMP DEFAULT NOW(),
        notes TEXT
      )
    `);

    await pool.query(`CREATE INDEX IF NOT EXISTS idx_jewelry_items_user_id ON jewelry_items(user_id)`);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_jewelry_items_status ON jewelry_items(status)`);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_jewelry_items_type ON jewelry_items(type)`);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_jewelry_items_contact_id ON jewelry_items(contact_id)`);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_jewelry_items_deal_id ON jewelry_items(deal_id) WHERE deal_id IS NOT NULL`);

    // Phase A: hybrid stone consumption.
    //   - consume_from_inventory = caller opted to actually take this stone out of stock
    //   - inventory_status = 'reserved' (consumed, item still in early stages)
    //                      | 'set'      (item is at 'setting' or later but not sold)
    //                      | 'sold'     (item sold; stone physically gone)
    //                      | NULL       (snapshot-only row, doesn't affect inventory)
    // No FK to soap_stones because that table is SOAP-synced and rows can come and go.
    const stoneConsumeMigrations = [
      "ALTER TABLE jewelry_item_stones ADD COLUMN IF NOT EXISTS consume_from_inventory BOOLEAN DEFAULT FALSE",
      "ALTER TABLE jewelry_item_stones ADD COLUMN IF NOT EXISTS inventory_status TEXT",
      // Timestamps – needed by the StoneUsagePanel (DNA page) which orders by created_at.
      // Backfill old rows so ORDER BY behaves consistently.
      "ALTER TABLE jewelry_item_stones ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT NOW()",
      "ALTER TABLE jewelry_item_stones ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT NOW()",
      "UPDATE jewelry_item_stones SET created_at = NOW() WHERE created_at IS NULL",
      "CREATE INDEX IF NOT EXISTS idx_jewelry_item_stones_sku ON jewelry_item_stones(stone_sku) WHERE stone_sku IS NOT NULL",
      "CREATE INDEX IF NOT EXISTS idx_jewelry_item_stones_active ON jewelry_item_stones(stone_sku) WHERE consume_from_inventory = TRUE AND (inventory_status IS NULL OR inventory_status <> 'sold')",
    ];
    for (const sql of stoneConsumeMigrations) {
      try { await pool.query(sql); } catch (e) { console.warn('Stone-consume migration warn:', e.message); }
    }

    // Phase C: tasks need a metadata column so we can dedupe auto-generated
    // rows (e.g. one occasion -> one task per year) and trace where a task
    // came from in the UI.
    const taskMetadataMigrations = [
      "ALTER TABLE crm_tasks ADD COLUMN IF NOT EXISTS metadata JSONB DEFAULT '{}'::jsonb",
      "CREATE INDEX IF NOT EXISTS idx_crm_tasks_metadata_source ON crm_tasks((metadata->>'source')) WHERE metadata IS NOT NULL",
    ];
    for (const sql of taskMetadataMigrations) {
      try { await pool.query(sql); } catch (e) { console.warn('Task-metadata migration warn:', e.message); }
    }

    // Phase E: when a workshop job is spun off from a catalog template
    // (jewelry_products row), remember which template it came from so we
    // can show "Made from MODEL-123" links and report on template usage.
    const templateMigrations = [
      "ALTER TABLE jewelry_items ADD COLUMN IF NOT EXISTS template_model_number TEXT",
      "CREATE INDEX IF NOT EXISTS idx_jewelry_items_template ON jewelry_items(template_model_number) WHERE template_model_number IS NOT NULL",
    ];
    for (const sql of templateMigrations) {
      try { await pool.query(sql); } catch (e) { console.warn('Template migration warn:', e.message); }
    }

    // Phase F (3D viewer): pair each jewelry_item with a model living in
    // iJewel3D Drive. ijewel_file_id is the per-item GLB/configurator file
    // id; ijewel_instance is the workspace name (e.g. "drive"). Most users
    // will have a single workspace and rely on the FE-level default, so the
    // per-item override is nullable. When both are present the 3D Preview
    // tab swaps the procedural placeholder for the real iJewel viewer.
    const ijewelMigrations = [
      "ALTER TABLE jewelry_items ADD COLUMN IF NOT EXISTS ijewel_file_id TEXT",
      "ALTER TABLE jewelry_items ADD COLUMN IF NOT EXISTS ijewel_instance TEXT",
      // Sprint 3 — sales-rep assignment for the workshop too: any
      // jewelry_item can be the responsibility of a specific rep.
      "ALTER TABLE jewelry_items ADD COLUMN IF NOT EXISTS assigned_to TEXT",
      "CREATE INDEX IF NOT EXISTS idx_jewelry_items_assigned_to ON jewelry_items(assigned_to) WHERE assigned_to IS NOT NULL",
    ];
    for (const sql of ijewelMigrations) {
      try { await pool.query(sql); } catch (e) { console.warn('iJewel migration warn:', e.message); }
    }

    // Phase G (Customer Preview): public share links so the workshop can
    // send a clean URL to the customer for approval, plus a log of every
    // approval / change-request / view that came back through it.
    //
    //   jewelry_shares          - one row per generated link (token, expiry,
    //                             revocation, who created it)
    //   jewelry_share_responses - append-only log of customer interactions
    //                             (action: viewed | approved | changes_requested
    //                              | comment); also feeds activity_log
    //
    // Tokens are URL-safe random base64 (24 bytes ≈ 32 chars) — opaque enough
    // that nobody can guess another item's preview without the link.
    await pool.query(`
      CREATE TABLE IF NOT EXISTS jewelry_shares (
        id SERIAL PRIMARY KEY,
        item_id INTEGER NOT NULL REFERENCES jewelry_items(id) ON DELETE CASCADE,
        token TEXT UNIQUE NOT NULL,
        created_by TEXT,
        created_at TIMESTAMP DEFAULT NOW(),
        expires_at TIMESTAMP,
        revoked_at TIMESTAMP,
        last_viewed_at TIMESTAMP,
        view_count INTEGER DEFAULT 0,
        notes TEXT
      )
    `);
    await pool.query(`
      CREATE TABLE IF NOT EXISTS jewelry_share_responses (
        id SERIAL PRIMARY KEY,
        share_id INTEGER NOT NULL REFERENCES jewelry_shares(id) ON DELETE CASCADE,
        action TEXT NOT NULL,
        customer_name TEXT,
        comment TEXT,
        ip TEXT,
        user_agent TEXT,
        created_at TIMESTAMP DEFAULT NOW()
      )
    `);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_jewelry_shares_item_id ON jewelry_shares(item_id)`);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_jewelry_shares_token ON jewelry_shares(token)`);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_jewelry_share_responses_share_id ON jewelry_share_responses(share_id)`);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_jewelry_item_stones_item_id ON jewelry_item_stones(item_id)`);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_jewelry_item_files_item_id ON jewelry_item_files(item_id)`);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_jewelry_item_history_item_id ON jewelry_item_history(item_id)`);

    jewelryReady = true;
    console.log('Jewelry tables ready');
  } catch (err) {
    console.error('Jewelry table creation error:', err);
  }
})();

const ensureJewelry = async (req, res, next) => {
  if (!jewelryReady) {
    try { await jewelryReadyPromise; } catch (_) {}
  }
  if (!jewelryReady) return res.status(503).json({ error: 'Jewelry tables not ready' });
  next();
};
app.use('/api/jewelry-items', ensureJewelry);

async function generateJewelrySku() {
  const year = new Date().getFullYear();
  const prefix = `JW-${year}-`;
  const r = await pool.query(
    `SELECT sku FROM jewelry_items WHERE sku LIKE $1 ORDER BY sku DESC LIMIT 1`,
    [`${prefix}%`]
  );
  let nextNum = 1;
  if (r.rows[0]?.sku) {
    const m = r.rows[0].sku.match(/-(\d+)$/);
    if (m) nextNum = parseInt(m[1], 10) + 1;
  }
  return `${prefix}${String(nextNum).padStart(4, '0')}`;
}

/* ---------- Jewelry Items: List ---------- */
app.get('/api/jewelry-items', async (req, res) => {
  try {
    const { status, type, contactId, search, includeArchived, assignedTo } = req.query;
    const ctx = await resolveTeamContext(req);
    if (!ctx.actorUserId) return res.status(400).json({ error: 'userId is required' });
    const tenantUserId = ctx.tenantUserId;

    // All filter columns must be prefixed with `ji.` because the JOIN with
    // crm_contacts brings in columns like user_id/name that would otherwise
    // be ambiguous to the planner.
    const where = ['ji.user_id = $1'];
    const params = [tenantUserId];
    let p = 2;

    if (!ctx.isOwner) {
      // Same inventory rule as soap-stones: rep sees their own assigned
      // pieces PLUS every unassigned piece (catalog/templates/ready
      // stock). Production board is admin-only, so reps only ever hit
      // this endpoint from the inventory grid where this is what they
      // need to sell from.
      where.push(`(ji.assigned_to IS NULL OR ji.assigned_to = $${p})`);
      params.push(ctx.actorUserId);
      p++;
    }
    if (assignedTo === 'me') {
      where.push(`ji.assigned_to = $${p}`);
      params.push(ctx.actorUserId);
      p++;
    } else if (assignedTo === 'unassigned') {
      where.push(`ji.assigned_to IS NULL`);
    } else if (assignedTo && assignedTo !== 'all') {
      where.push(`ji.assigned_to = $${p}`);
      params.push(assignedTo);
      p++;
    }

    if (status) { where.push(`ji.status = $${p++}`); params.push(status); }
    if (type) { where.push(`ji.type = $${p++}`); params.push(type); }
    if (contactId) { where.push(`ji.contact_id = $${p++}`); params.push(contactId); }
    if (search) {
      where.push(`(ji.name ILIKE $${p} OR ji.sku ILIKE $${p} OR ji.description ILIKE $${p})`);
      params.push(`%${search}%`);
      p++;
    }
    if (!includeArchived || includeArchived === 'false') {
      where.push(`ji.status <> 'archived'`);
    }

    const sql = `
      SELECT ji.*,
             c.name AS contact_name, c.company AS contact_company,
             (SELECT COUNT(*) FROM jewelry_item_files f WHERE f.item_id = ji.id) AS files_count,
             (SELECT COUNT(*) FROM jewelry_item_stones s WHERE s.item_id = ji.id) AS stones_count
        FROM jewelry_items ji
        LEFT JOIN crm_contacts c ON c.id = ji.contact_id
       WHERE ${where.join(' AND ')}
       ORDER BY ji.updated_at DESC, ji.id DESC
       LIMIT 500
    `;
    const r = await pool.query(sql, params);
    res.json({ items: r.rows });
  } catch (e) {
    console.error('GET jewelry-items error:', e);
    res.status(500).json({ error: e.message });
  }
});

/* ---------- Jewelry Items: Get one (with all relations) ---------- */
app.get('/api/jewelry-items/:id', async (req, res) => {
  try {
    const { id } = req.params;
    const ctx = await resolveTeamContext(req);
    if (!ctx.actorUserId) return res.status(400).json({ error: 'userId is required' });
    const tenantUserId = ctx.tenantUserId;

    const item = await pool.query(
      `SELECT ji.*,
              c.name AS contact_name, c.company AS contact_company, c.email AS contact_email,
              d.title AS deal_title, d.stage AS deal_stage, d.value AS deal_value
         FROM jewelry_items ji
         LEFT JOIN crm_contacts c ON c.id = ji.contact_id
         LEFT JOIN crm_deals    d ON d.id = ji.deal_id
        WHERE ji.id = $1 AND ji.user_id = $2`,
      [id, tenantUserId]
    );
    if (!item.rows[0]) return res.status(404).json({ error: 'Item not found' });
    // Inventory rules: unassigned items are visible to every rep, claimed
    // items only to the rep that claimed them.
    if (!canReadInventoryItem(ctx, item.rows[0].assigned_to)) {
      return res.status(404).json({ error: 'Item not found' });
    }

    const [stones, metals, costs, files, history] = await Promise.all([
      pool.query(`SELECT * FROM jewelry_item_stones WHERE item_id = $1 ORDER BY id`, [id]),
      pool.query(`SELECT * FROM jewelry_item_metals WHERE item_id = $1 ORDER BY id`, [id]),
      pool.query(`SELECT * FROM jewelry_item_costs WHERE item_id = $1 ORDER BY id`, [id]),
      pool.query(`SELECT * FROM jewelry_item_files WHERE item_id = $1 ORDER BY uploaded_at DESC, id DESC`, [id]),
      pool.query(`SELECT * FROM jewelry_item_history WHERE item_id = $1 ORDER BY changed_at DESC, id DESC LIMIT 100`, [id]),
    ]);

    // Lazy backfill: any inventory-consumed stone with an empty snapshot gets
    // re-snapshotted on the spot. Catches rows added during the brief window
    // before the new snapshot path was deployed (and any future regressions
    // that leave snapshot empty). Bounded — only runs for stones that lack
    // it, and the result is persisted so subsequent loads are pure reads.
    const stoneRows = stones.rows;
    const needsSnapshot = stoneRows.filter((r) => {
      if (!r.consume_from_inventory || !r.stone_sku) return false;
      const snap = r.snapshot;
      if (snap == null) return true;
      if (typeof snap === 'object' && Object.keys(snap).length === 0) return true;
      return false;
    });
    if (needsSnapshot.length) {
      await Promise.all(needsSnapshot.map(async (row) => {
        try {
          const built = await _buildStoneSnapshot(row.stone_sku, null);
          if (built && Object.keys(built).length) {
            const upd = await pool.query(
              `UPDATE jewelry_item_stones
                  SET snapshot = $1::jsonb, updated_at = NOW()
                WHERE id = $2
              RETURNING snapshot`,
              [JSON.stringify(built), row.id]
            );
            row.snapshot = upd.rows[0]?.snapshot || built;
          }
        } catch (e) {
          console.warn('Lazy snapshot backfill failed for stone', row.id, e.message);
        }
      }));
    }

    res.json({
      item: item.rows[0],
      stones: stoneRows,
      metals: metals.rows,
      costs: costs.rows,
      files: files.rows,
      history: history.rows,
    });
  } catch (e) {
    console.error('GET jewelry-item error:', e);
    res.status(500).json({ error: e.message });
  }
});

/* ---------- Jewelry Items: Create ---------- */
app.post('/api/jewelry-items', async (req, res) => {
  try {
    const {
      location, name, type = 'custom', category,
      contactId, dealId, description, internalNotes, size, weightGrams,
      metalSummary, status = 'draft', assignedTo,
    } = req.body || {};
    const ctx = await resolveTeamContext(req);
    if (!ctx.actorUserId) return res.status(400).json({ error: 'userId is required' });
    if (!name || !String(name).trim()) return res.status(400).json({ error: 'name is required' });
    if (!isValidType(type)) return res.status(400).json({ error: 'invalid type' });
    if (!isValidStatus(status)) return res.status(400).json({ error: 'invalid status' });
    const tenantUserId = ctx.tenantUserId;
    const finalAssignee = ctx.isOwner
      ? (assignedTo === undefined ? ctx.actorUserId : assignedTo || null)
      : ctx.actorUserId;

    const sku = await generateJewelrySku();

    const r = await pool.query(
      `INSERT INTO jewelry_items
         (user_id, location, sku, name, type, status, contact_id, deal_id,
          category, metal_summary, weight_grams, size, description, internal_notes, assigned_to)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
       RETURNING *`,
      [
        tenantUserId, location || null, sku, name.trim(), type, status,
        contactId || null, dealId || null,
        category || null, metalSummary || null,
        weightGrams || null, size || null, description || null, internalNotes || null,
        finalAssignee,
      ]
    );

    await pool.query(
      `INSERT INTO jewelry_item_history (item_id, from_status, to_status, changed_by, notes)
       VALUES ($1, NULL, $2, $3, $4)`,
      [r.rows[0].id, status, ctx.actorUserId, 'Item created']
    );

    res.json({ item: r.rows[0] });

    logActivity({
      userId:    tenantUserId,
      actorId:   ctx.actorUserId,
      actorName: ctx.actorName,
      entityType: 'jewelry_item',
      entityId:   r.rows[0].id,
      action:     'created',
      summary:    `Added jewelry: ${r.rows[0].name} (${r.rows[0].sku})`,
      related: [
        contactId ? { type: 'contact', id: contactId } : null,
        dealId    ? { type: 'deal',    id: dealId    } : null,
      ].filter(Boolean),
    });
  } catch (e) {
    console.error('POST jewelry-items error:', e);
    res.status(500).json({ error: e.message });
  }
});

/* ---------- Jewelry Items: Create from catalog template (Phase E) ----------
 * Spins off a workshop job from a `jewelry_products` row. Copies the
 * descriptive fields (title, category, metal, weight, size, description,
 * cover image) so the bench worker has everything they need, but starts
 * fresh on stones/costs/history. Tracked via `template_model_number` so we
 * can later report on which catalog pieces are most often re-made.
 */
const looksLikeImageUrl = (u) => {
  if (!u || typeof u !== 'string') return false;
  const s = u.trim().toLowerCase();
  if (!/^https?:\/\//.test(s)) return false;
  return /\.(png|jpe?g|webp|gif|avif)(\?|#|$)/.test(s);
};

// Templates store `all_pictures_link` as a `;`-separated list of URLs (CSV
// import preserves the column verbatim). Walk it and return the first entry
// that actually looks like an image so we can use it as the workshop job's
// cover. Returns null when nothing in the list qualifies.
const pickFirstImageUrl = (raw) => {
  if (!raw || typeof raw !== 'string') return null;
  const candidates = raw.split(/[;,]+/).map((s) => s.trim()).filter(Boolean);
  for (const u of candidates) {
    if (looksLikeImageUrl(u)) return u;
  }
  return null;
};

app.post('/api/jewelry-items/from-template', async (req, res) => {
  try {
    const {
      modelNumber, location,
      contactId, dealId, name: nameOverride, assignedTo,
    } = req.body || {};
    const ctx = await resolveTeamContext(req);
    if (!ctx.actorUserId) return res.status(400).json({ error: 'userId is required' });
    if (!modelNumber) return res.status(400).json({ error: 'modelNumber is required' });
    const tenantUserId = ctx.tenantUserId;
    const finalAssignee = ctx.isOwner
      ? (assignedTo === undefined ? ctx.actorUserId : assignedTo || null)
      : ctx.actorUserId;

    const tplRes = await pool.query(
      `SELECT * FROM jewelry_products WHERE model_number = $1 LIMIT 1`,
      [modelNumber]
    );
    const tpl = tplRes.rows[0];
    if (!tpl) return res.status(404).json({ error: 'Template not found' });

    const sku = await generateJewelrySku();
    const name = (nameOverride && String(nameOverride).trim()) ||
                 tpl.title ||
                 `${tpl.jewelry_type || 'Piece'} ${tpl.model_number}`;
    const description = tpl.full_description || tpl.description || null;
    const category = tpl.category || tpl.jewelry_type || null;
    const metalSummary = [tpl.metal_type, tpl.style].filter(Boolean).join(' / ') || null;
    const weightGrams = tpl.jewelry_weight || null;
    const size = tpl.jewelry_size || null;
    // CSV-imported templates store images as `url1; url2; url3` rather than a
    // single URL, so we have to walk the list. The earlier single-URL check
    // never matched and we lost the cover.
    const coverImage = pickFirstImageUrl(tpl.all_pictures_link);

    // "Make from template" is an explicit commit to start production — the
    // previous default of 'draft' hid the new job from the Production Kanban
    // (which only shows items in active stages). Land directly on 'design',
    // the first stage of the workflow.
    const r = await pool.query(
      `INSERT INTO jewelry_items
         (user_id, location, sku, name, type, status, contact_id, deal_id,
          category, metal_summary, weight_grams, size, description,
          cover_image_url, template_model_number, assigned_to)
       VALUES ($1,$2,$3,$4,'custom','design',$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
       RETURNING *`,
      [
        tenantUserId, location || null, sku, name,
        contactId || null, dealId || null,
        category, metalSummary, weightGrams, size, description,
        coverImage, tpl.model_number, finalAssignee,
      ]
    );

    await pool.query(
      `INSERT INTO jewelry_item_history (item_id, from_status, to_status, changed_by, notes)
       VALUES ($1, NULL, 'design', $2, $3)`,
      [r.rows[0].id, ctx.actorUserId, `Created from catalog template ${tpl.model_number}`]
    );

    res.json({ item: r.rows[0], template: { model_number: tpl.model_number, title: tpl.title } });

    logActivity({
      userId:    tenantUserId,
      actorId:   ctx.actorUserId,
      actorName: ctx.actorName,
      entityType: 'jewelry_item',
      entityId:   r.rows[0].id,
      action:     'created',
      summary:    `Made ${r.rows[0].name} from template ${tpl.model_number}`,
      changes:    { template_model_number: { from: null, to: tpl.model_number } },
      related: [
        contactId ? { type: 'contact', id: contactId } : null,
        dealId    ? { type: 'deal',    id: dealId    } : null,
      ].filter(Boolean),
    });
  } catch (e) {
    console.error('POST jewelry-items/from-template error:', e);
    res.status(500).json({ error: e.message });
  }
});

/* ---------- Jewelry Items: Update ---------- */
app.put('/api/jewelry-items/:id', async (req, res) => {
  try {
    const { id } = req.params;
    const ctx = await resolveTeamContext(req);
    const fields = [
      'name', 'type', 'category', 'contact_id', 'deal_id',
      'description', 'internal_notes', 'size', 'weight_grams', 'metal_summary',
      'cover_image_url', 'markup_percent', 'sale_price', 'location',
      'ijewel_file_id', 'ijewel_instance',
      'assigned_to',
    ];
    const map = {
      name: 'name', type: 'type', category: 'category',
      contactId: 'contact_id', dealId: 'deal_id',
      description: 'description', internalNotes: 'internal_notes',
      size: 'size', weightGrams: 'weight_grams', metalSummary: 'metal_summary',
      coverImageUrl: 'cover_image_url', markupPercent: 'markup_percent',
      salePrice: 'sale_price', location: 'location',
      ijewelFileId: 'ijewel_file_id', ijewelInstance: 'ijewel_instance',
      assignedTo: 'assigned_to',
    };
    const sets = [];
    const params = [];
    let p = 1;
    for (const [k, v] of Object.entries(req.body || {})) {
      const col = map[k];
      if (col && fields.includes(col)) {
        if (col === 'type' && v && !isValidType(v)) {
          return res.status(400).json({ error: 'invalid type' });
        }
        if (col === 'assigned_to' && ctx.actorUserId && !ctx.isOwner) {
          if (v && String(v) !== String(ctx.actorUserId)) {
            return res.status(403).json({ error: 'Reps can only assign records to themselves' });
          }
        }
        sets.push(`${col} = $${p++}`);
        params.push(v === '' ? null : v);
      }
    }
    if (!sets.length) return res.status(400).json({ error: 'no valid fields' });
    sets.push(`updated_at = NOW()`);

    // Snapshot before so we can record what changed.
    const beforeRes = await pool.query(`SELECT * FROM jewelry_items WHERE id = $1`, [id]);
    const before = beforeRes.rows[0] || null;

    params.push(id);
    const sql = `UPDATE jewelry_items SET ${sets.join(', ')} WHERE id = $${p} RETURNING *`;
    const r = await pool.query(sql, params);
    if (!r.rows[0]) return res.status(404).json({ error: 'Item not found' });
    res.json({ item: r.rows[0] });

    const after = r.rows[0];
    const changes = diffRows(before, after, fields);
    if (changes && before) {
      const { actorId, actorName } = getActor(req);
      const ck = Object.keys(changes);
      const summary = ck.length === 1
        ? `Updated ${ck[0]} on ${after.name || after.sku}`
        : `Updated ${after.name || after.sku} (${ck.length} fields)`;
      logActivity({
        userId:     after.user_id,
        actorId, actorName,
        entityType: 'jewelry_item',
        entityId:   after.id,
        action:     'updated',
        summary,
        changes,
        related: [
          after.contact_id ? { type: 'contact', id: after.contact_id } : null,
          after.deal_id    ? { type: 'deal',    id: after.deal_id    } : null,
        ].filter(Boolean),
      });
    }
  } catch (e) {
    console.error('PUT jewelry-item error:', e);
    res.status(500).json({ error: e.message });
  }
});

/* ---------- Jewelry Items: Delete ---------- */
app.delete('/api/jewelry-items/:id', async (req, res) => {
  try {
    const { id } = req.params;
    const beforeRes = await pool.query(
      `SELECT user_id, sku, name, contact_id FROM jewelry_items WHERE id = $1`,
      [id]
    );
    const r = await pool.query(`DELETE FROM jewelry_items WHERE id = $1 RETURNING id`, [id]);
    if (!r.rows[0]) return res.status(404).json({ error: 'Item not found' });
    res.json({ success: true, id: r.rows[0].id });

    const before = beforeRes.rows[0];
    if (before) {
      const { actorId, actorName } = getActor(req);
      logActivity({
        userId:     before.user_id,
        actorId, actorName,
        entityType: 'jewelry_item',
        entityId:   id,
        action:     'deleted',
        summary:    `Deleted jewelry ${before.name || before.sku}`,
        related:    before.contact_id ? [{ type: 'contact', id: before.contact_id }] : null,
      });
    }
  } catch (e) {
    console.error('DELETE jewelry-item error:', e);
    res.status(500).json({ error: e.message });
  }
});

/* ---------- Jewelry Items: Change status ----------
 * Side effect: when an item with a contact_id changes status, we also drop a
 * `production_update` row into crm_interactions so the customer's profile
 * Activity timeline stays in sync with what's happening in the workshop.
 */
app.post('/api/jewelry-items/:id/status', async (req, res) => {
  try {
    const { id } = req.params;
    const { newStatus, notes, userId } = req.body || {};
    if (!isValidStatus(newStatus)) return res.status(400).json({ error: 'invalid status' });

    const cur = await pool.query(
      `SELECT status, contact_id, sku, name, user_id FROM jewelry_items WHERE id = $1`,
      [id]
    );
    if (!cur.rows[0]) return res.status(404).json({ error: 'Item not found' });

    const fromStatus = cur.rows[0].status;
    const r = await pool.query(
      `UPDATE jewelry_items SET status = $1, updated_at = NOW() WHERE id = $2 RETURNING *`,
      [newStatus, id]
    );
    await pool.query(
      `INSERT INTO jewelry_item_history (item_id, from_status, to_status, changed_by, notes)
       VALUES ($1,$2,$3,$4,$5)`,
      [id, fromStatus, newStatus, userId || null, notes || null]
    );

    // Inventory bridge: any consumed stones on this piece track the production status.
    //   reserved -> set : when piece moves into setting/polishing/qc/ready
    //   set -> reserved : if piece moves backward (e.g. setting -> wax/casting)
    //   anything -> sold : not handled here (the /sell endpoint owns that transition)
    if (fromStatus !== newStatus && newStatus !== 'sold') {
      try {
        const setStages = ['setting','polishing','qc','ready'];
        const target = setStages.includes(newStatus) ? 'set' : 'reserved';
        await pool.query(
          `UPDATE jewelry_item_stones
              SET inventory_status = $1
            WHERE item_id = $2
              AND consume_from_inventory = TRUE
              AND (inventory_status IS NULL OR inventory_status NOT IN ('sold','returned'))`,
          [target, id]
        );
      } catch (stHookErr) {
        console.warn('Stone-status hook warn:', stHookErr.message);
      }
    }

    // CRM bridge: customer-facing status update in their activity timeline.
    // Skip when status didn't actually change, when there's no linked contact,
    // or when transitioning to 'sold' (the /sell endpoint already records that).
    if (cur.rows[0].contact_id && fromStatus !== newStatus && newStatus !== 'sold') {
      try {
        const skuOrName = cur.rows[0].sku || cur.rows[0].name || `#${id}`;
        await pool.query(
          `INSERT INTO crm_interactions (user_id, contact_id, type, direction, subject, content, metadata)
           VALUES ($1,$2,'production_update','outgoing',$3,$4,$5)`,
          [
            cur.rows[0].user_id,
            cur.rows[0].contact_id,
            `${skuOrName} moved to ${newStatus}`,
            notes || null,
            JSON.stringify({
              jewelry_item_id: Number(id),
              sku: cur.rows[0].sku,
              name: cur.rows[0].name,
              from_status: fromStatus,
              to_status: newStatus,
            }),
          ]
        );
      } catch (hookErr) {
        // Never let the CRM hook fail the status change itself.
        console.warn('CRM production_update hook warn:', hookErr.message);
      }
    }

    res.json({ item: r.rows[0] });

    if (fromStatus !== newStatus) {
      const { actorId, actorName } = getActor(req);
      const skuOrName = cur.rows[0].name || cur.rows[0].sku || `#${id}`;
      logActivity({
        userId:     cur.rows[0].user_id,
        actorId, actorName,
        entityType: 'jewelry_item',
        entityId:   id,
        action:     newStatus === 'sold' ? 'sold' : 'status_changed',
        summary:    `${skuOrName}: ${fromStatus || 'new'} → ${newStatus}`,
        changes:    { status: { from: fromStatus, to: newStatus } },
        related:    cur.rows[0].contact_id ? [{ type: 'contact', id: cur.rows[0].contact_id }] : null,
      });
    }
  } catch (e) {
    console.error('POST jewelry-item status error:', e);
    res.status(500).json({ error: e.message });
  }
});

/* ---------- Jewelry Items: Files (register a blob URL on the item) ---------- */
app.post('/api/jewelry-items/:id/files', async (req, res) => {
  try {
    const { id } = req.params;
    const { url, kind, stage, filename, mimeType, sizeBytes, uploadedBy, setAsCover } = req.body || {};
    if (!url) return res.status(400).json({ error: 'url is required' });

    const r = await pool.query(
      `INSERT INTO jewelry_item_files (item_id, url, kind, stage, filename, mime_type, size_bytes, uploaded_by)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
       RETURNING *`,
      [id, url, kind || null, stage || null, filename || null, mimeType || null, sizeBytes || null, uploadedBy || null]
    );

    if (setAsCover) {
      await pool.query(`UPDATE jewelry_items SET cover_image_url = $1, updated_at = NOW() WHERE id = $2`, [url, id]);
    }

    res.json({ file: r.rows[0] });
  } catch (e) {
    console.error('POST jewelry-item file error:', e);
    res.status(500).json({ error: e.message });
  }
});

app.delete('/api/jewelry-items/:id/files/:fileId', async (req, res) => {
  try {
    const { id, fileId } = req.params;
    const r = await pool.query(
      `DELETE FROM jewelry_item_files WHERE id = $1 AND item_id = $2 RETURNING id`,
      [fileId, id]
    );
    if (!r.rows[0]) return res.status(404).json({ error: 'File not found' });
    res.json({ success: true });
  } catch (e) {
    console.error('DELETE jewelry-item file error:', e);
    res.status(500).json({ error: e.message });
  }
});

/* ---------- Jewelry Items: Stones (composition) ----------
 *
 * Accepts EITHER:
 *   - { stoneSku, role, quantity, snapshot, notes, consumeFromInventory }
 *     (single legacy payload — kept for backwards compatibility)
 *   - { stones: [ { stoneSku, role, quantity, snapshot, notes, consumeFromInventory }, ... ] }
 *     (batch payload — used by the queue UI in StonesPanel so the user can
 *     add several stones in one click, including splitting a single SKU
 *     across multiple roles like 12 sides + 8 accents from the same parcel)
 *
 * For each row we:
 *   1) Conflict-check the SKU against rows on OTHER items (same-item rows
 *      are allowed so split-by-role works).
 *   2) Auto-snapshot a wide set of fields from soap_stones (measurements,
 *      cut/polish/symmetry, table/depth %, fluorescence, certificate#,
 *      video, fancy color, treatment, etc.) so the BOM keeps a faithful
 *      record even after the inventory row changes.
 *   3) Pick an initial inventory_status based on the parent item's stage.
 */

// Centralised auto-snapshot so single + batch paths stay in sync.
// IMPORTANT: soap_stones is externally synced (SOAP) and column shape can
// drift, so we use SELECT * + optional-chained column reads — a single
// missing column would otherwise throw and we'd silently end up with an
// empty snapshot (which is exactly what showed up in production).
async function _buildStoneSnapshot(stoneSku, baseSnapshot) {
  let finalSnapshot = baseSnapshot && typeof baseSnapshot === 'object'
    ? { ...baseSnapshot }
    : (baseSnapshot ? baseSnapshot : null);
  if (!stoneSku) return finalSnapshot;
  try {
    const sr = await pool.query(
      `SELECT * FROM soap_stones WHERE sku = $1 LIMIT 1`,
      [stoneSku]
    );
    const s = sr.rows[0];
    if (!s) {
      console.warn(`Stone snapshot: SKU ${stoneSku} not found in soap_stones`);
      return finalSnapshot;
    }
    // Helper to coerce numeric-ish columns that might come back as strings.
    const num = (v) => (v != null && v !== '' && !isNaN(Number(v)) ? Number(v) : null);
    const txt = (v) => (v != null && v !== '' ? v : null);
    const auto = {
      shape:        txt(s.shape),
      weight:       num(s.weight),
      color:        txt(s.color),
      clarity:      txt(s.clarity),
      lab:          txt(s.lab),
      origin:       txt(s.origin),
      category:     txt(s.category),
      // Pre-existing field — keeps 'certificate' in case the column carries
      // a friendlier identifier; the explicit number/image are also captured.
      certificate:        txt(s.certificate),
      certificateNumber:  txt(s.certificate_number),
      certificateUrl:     txt(s.certificate_image) || txt(s.certificate_url),
      videoUrl:           txt(s.video),
      // The spec block — what the user explicitly asked for.
      measurements:   txt(s.measurements),
      ratio:          num(s.ratio),
      cut:            txt(s.cut),
      polish:         txt(s.polish),
      symmetry:       txt(s.symmetry),
      tablePercent:   num(s.table_percent),
      depthPercent:   num(s.depth_percent),
      fluorescence:   txt(s.fluorescence),
      luster:         txt(s.luster),
      treatment:      txt(s.comment),
      fancyIntensity: txt(s.fancy_intensity),
      fancyColor:     txt(s.fancy_color),
      fancyOvertone:  txt(s.fancy_overtone),
      pairSku:        txt(s.pair_stone),
      sourcedFrom:    txt(s.branch),
      // Price priority: bruto > net > total_price > price_per_carat * weight.
      price: num(s.bruto_price) ?? num(s.net_price) ?? num(s.total_price)
              ?? (num(s.price_per_carat) != null && num(s.weight) != null
                    ? num(s.price_per_carat) * num(s.weight)
                    : null),
      imageUrl: txt(s.image)
              || (s.additional_pictures ? String(s.additional_pictures).split(';')[0].trim() || null : null),
      sourcedAt: new Date().toISOString(),
    };
    finalSnapshot = {
      ...(finalSnapshot || {}),
      ...Object.fromEntries(Object.entries(auto).filter(([, v]) => v != null && v !== '')),
    };
  } catch (snapErr) {
    // Bubble enough info to the logs so this kind of issue isn't invisible
    // again. Caller still proceeds with the original snapshot (or null) so
    // the row gets inserted — better an empty snapshot than a 500.
    console.error(`Stone snapshot lookup failed for ${stoneSku}:`, snapErr.message, snapErr.stack);
  }
  return finalSnapshot;
}

// Reject if any OTHER jewelry item already reserves this SKU. Same-item rows
// are explicitly allowed so the user can split one parcel across roles.
async function _conflictForStoneOnItem(stoneSku, itemId) {
  if (!stoneSku) return null;
  const conflict = await pool.query(
    `SELECT jis.id, jis.item_id, ji.sku AS jewelry_sku, ji.name AS jewelry_name, jis.inventory_status
       FROM jewelry_item_stones jis
       JOIN jewelry_items ji ON ji.id = jis.item_id
      WHERE jis.stone_sku = $1
        AND jis.consume_from_inventory = TRUE
        AND (jis.inventory_status IS NULL OR jis.inventory_status NOT IN ('sold','returned'))
        AND jis.item_id <> $2
      LIMIT 1`,
    [stoneSku, itemId]
  );
  return conflict.rows[0] || null;
}

function _initialInventoryStatusFromItemStatus(itemStatus) {
  if (itemStatus === 'sold') return 'sold';
  if (['setting','polishing','qc','ready'].includes(itemStatus)) return 'set';
  return 'reserved';
}

app.post('/api/jewelry-items/:id/stones', async (req, res) => {
  try {
    const { id } = req.params;
    const body = req.body || {};
    // Normalise to a list so single + batch payloads share one path.
    const rows = Array.isArray(body.stones) && body.stones.length
      ? body.stones
      : [body];

    // Pre-check every row's conflict so we either insert all or insert none —
    // a partial batch on a multi-row pick is confusing for the user.
    for (const row of rows) {
      const wantsConsume = !!row.consumeFromInventory && !!row.stoneSku;
      if (!wantsConsume) continue;
      const c = await _conflictForStoneOnItem(row.stoneSku, id);
      if (c) {
        return res.status(409).json({
          error: `Stone ${row.stoneSku} is already reserved by ${c.jewelry_sku || ('job #' + c.item_id)}${c.jewelry_name ? ' (' + c.jewelry_name + ')' : ''}`,
          conflict: c,
        });
      }
    }

    // Cache the parent item's stage once — every row gets the same initial
    // inventory_status so we don't re-query inside the loop.
    const itStatusRes = await pool.query(`SELECT status FROM jewelry_items WHERE id = $1 LIMIT 1`, [id]);
    const itStatus = itStatusRes.rows[0]?.status || null;
    const consumedInitialStatus = _initialInventoryStatusFromItemStatus(itStatus);

    const inserted = [];
    for (const row of rows) {
      const consume = !!row.consumeFromInventory && !!row.stoneSku;
      const finalSnapshot = consume
        ? await _buildStoneSnapshot(row.stoneSku, row.snapshot)
        : (row.snapshot && typeof row.snapshot === 'object' ? { ...row.snapshot } : (row.snapshot || null));
      const r = await pool.query(
        `INSERT INTO jewelry_item_stones (item_id, stone_sku, role, quantity, snapshot, notes, consume_from_inventory, inventory_status)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8) RETURNING *`,
        [
          id,
          row.stoneSku || null,
          row.role || null,
          row.quantity || 1,
          finalSnapshot ? JSON.stringify(finalSnapshot) : null,
          row.notes || null,
          consume,
          consume ? consumedInitialStatus : null,
        ]
      );
      inserted.push(r.rows[0]);
    }

    // Backwards-compatible response: return `stone` for single, plus `stones` for batch.
    res.json({ stone: inserted[0], stones: inserted });

    // ---- Activity log (non-blocking) ----
    const itemRes = await pool.query(
      `SELECT user_id, name, sku FROM jewelry_items WHERE id = $1`,
      [id]
    );
    const item = itemRes.rows[0];
    if (item) {
      const { actorId, actorName } = getActor(req);
      const itemLabel = item.name || item.sku || `#${id}`;
      if (inserted.length === 1) {
        const only = inserted[0];
        const consumed = !!only.consume_from_inventory;
        logActivity({
          userId:     item.user_id,
          actorId, actorName,
          entityType: 'jewelry_item',
          entityId:   id,
          action:     consumed ? 'stone_consumed' : 'stone_added',
          summary:    only.stone_sku
            ? `${consumed ? 'Consumed' : 'Added'} stone ${only.stone_sku} on ${itemLabel}`
            : `Added stone slot on ${itemLabel}`,
          related:    only.stone_sku ? [{ type: 'stone', id: only.stone_sku }] : null,
        });
      } else {
        const distinctSkus = Array.from(new Set(inserted.map((s) => s.stone_sku).filter(Boolean)));
        const anyConsumed = inserted.some((s) => s.consume_from_inventory);
        logActivity({
          userId:     item.user_id,
          actorId, actorName,
          entityType: 'jewelry_item',
          entityId:   id,
          action:     anyConsumed ? 'stone_consumed' : 'stone_added',
          summary:    `${anyConsumed ? 'Consumed' : 'Added'} ${inserted.length} stones on ${itemLabel}`,
          related:    distinctSkus.length ? distinctSkus.map((sku) => ({ type: 'stone', id: sku })) : null,
        });
      }
    }
  } catch (e) {
    console.error('POST jewelry stone error:', e);
    res.status(500).json({ error: e.message });
  }
});

app.delete('/api/jewelry-items/:id/stones/:stoneId', async (req, res) => {
  try {
    const { id, stoneId } = req.params;
    // Just removing the row releases the stone back to inventory (the active-row index
    // no longer matches it). No extra cleanup needed because we don't mutate soap_stones.
    const r = await pool.query(
      `DELETE FROM jewelry_item_stones WHERE id = $1 AND item_id = $2 RETURNING id, stone_sku, consume_from_inventory`,
      [stoneId, id]
    );
    if (!r.rows[0]) return res.status(404).json({ error: 'Not found' });
    res.json({ success: true, released: r.rows[0].consume_from_inventory ? r.rows[0].stone_sku : null });

    const itemRes = await pool.query(
      `SELECT user_id, name, sku FROM jewelry_items WHERE id = $1`,
      [id]
    );
    const item = itemRes.rows[0];
    if (item) {
      const { actorId, actorName } = getActor(req);
      const itemLabel = item.name || item.sku || `#${id}`;
      const sku = r.rows[0].stone_sku;
      logActivity({
        userId:     item.user_id,
        actorId, actorName,
        entityType: 'jewelry_item',
        entityId:   id,
        action:     'stone_removed',
        summary:    sku
          ? `Removed stone ${sku} from ${itemLabel}${r.rows[0].consume_from_inventory ? ' (released to inventory)' : ''}`
          : `Removed stone slot from ${itemLabel}`,
        related:    sku ? [{ type: 'stone', id: sku }] : null,
      });
    }
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

/* ---------- Jewelry Items: Metals ---------- */
app.post('/api/jewelry-items/:id/metals', async (req, res) => {
  try {
    const { id } = req.params;
    const { metalType, purity, color, weightGrams, pricePerGram } = req.body || {};
    if (!weightGrams) return res.status(400).json({ error: 'weightGrams is required' });
    const totalCost = pricePerGram ? Number(pricePerGram) * Number(weightGrams) : null;
    const r = await pool.query(
      `INSERT INTO jewelry_item_metals (item_id, metal_type, purity, color, weight_grams, price_per_gram, total_cost)
       VALUES ($1,$2,$3,$4,$5,$6,$7) RETURNING *`,
      [id, metalType || null, purity || null, color || null, weightGrams, pricePerGram || null, totalCost]
    );
    res.json({ metal: r.rows[0] });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.delete('/api/jewelry-items/:id/metals/:metalId', async (req, res) => {
  try {
    const { id, metalId } = req.params;
    const r = await pool.query(
      `DELETE FROM jewelry_item_metals WHERE id = $1 AND item_id = $2 RETURNING id`,
      [metalId, id]
    );
    if (!r.rows[0]) return res.status(404).json({ error: 'Not found' });
    res.json({ success: true });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

/* ---------- Jewelry Items: Costs ---------- */
app.post('/api/jewelry-items/:id/costs', async (req, res) => {
  try {
    const { id } = req.params;
    const { label, category, amount, notes } = req.body || {};
    if (!label || amount == null) {
      return res.status(400).json({ error: 'label and amount are required' });
    }
    const r = await pool.query(
      `INSERT INTO jewelry_item_costs (item_id, label, category, amount, notes)
       VALUES ($1,$2,$3,$4,$5) RETURNING *`,
      [id, label, category || null, amount, notes || null]
    );
    res.json({ cost: r.rows[0] });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.delete('/api/jewelry-items/:id/costs/:costId', async (req, res) => {
  try {
    const { id, costId } = req.params;
    const r = await pool.query(
      `DELETE FROM jewelry_item_costs WHERE id = $1 AND item_id = $2 RETURNING id`,
      [costId, id]
    );
    if (!r.rows[0]) return res.status(404).json({ error: 'Not found' });
    res.json({ success: true });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

/* ---------- Jewelry Items: Recompute total cost ---------- */
app.post('/api/jewelry-items/:id/recalc', async (req, res) => {
  try {
    const { id } = req.params;
    const stones = await pool.query(`SELECT snapshot FROM jewelry_item_stones WHERE item_id = $1`, [id]);
    const stonesTotal = stones.rows.reduce((sum, r) => {
      const p = Number(r.snapshot?.price ?? r.snapshot?.priceTotal ?? 0);
      return sum + (Number.isFinite(p) ? p : 0);
    }, 0);
    const metalsTotal = await pool.query(`SELECT COALESCE(SUM(total_cost), 0) AS s FROM jewelry_item_metals WHERE item_id = $1`, [id]);
    const costsTotal = await pool.query(`SELECT COALESCE(SUM(amount), 0) AS s FROM jewelry_item_costs WHERE item_id = $1`, [id]);
    const total = Number(stonesTotal) + Number(metalsTotal.rows[0].s) + Number(costsTotal.rows[0].s);
    const cur = await pool.query(`SELECT markup_percent FROM jewelry_items WHERE id = $1`, [id]);
    const markup = Number(cur.rows[0]?.markup_percent || 0);
    const salePrice = total * (1 + markup / 100);
    const r = await pool.query(
      `UPDATE jewelry_items SET total_cost = $1, sale_price = $2, updated_at = NOW() WHERE id = $3 RETURNING *`,
      [total, salePrice, id]
    );
    res.json({ item: r.rows[0], breakdown: { stones: stonesTotal, metals: Number(metalsTotal.rows[0].s), costs: Number(costsTotal.rows[0].s), total, salePrice } });
  } catch (e) {
    console.error('Recalc error:', e);
    res.status(500).json({ error: e.message });
  }
});

/* ---------- Jewelry Items: Sell ---------- */
app.post('/api/jewelry-items/:id/sell', async (req, res) => {
  try {
    const { id } = req.params;
    const { contactId, salePrice, currency = 'USD', notes, userId } = req.body || {};
    if (!contactId) return res.status(400).json({ error: 'contactId is required' });

    const itemRes = await pool.query(`SELECT * FROM jewelry_items WHERE id = $1`, [id]);
    const item = itemRes.rows[0];
    if (!item) return res.status(404).json({ error: 'Item not found' });

    const finalPrice = salePrice != null ? Number(salePrice) : Number(item.sale_price || 0);
    const ownerUserId = userId || item.user_id;

    // If the item is already attached to a CRM deal, reuse it: mark won + update value.
    // Otherwise spin up a brand new "won" deal so the sale always lives in the pipeline.
    let deal;
    if (item.deal_id) {
      const upd = await pool.query(
        `UPDATE crm_deals
            SET stage = 'won',
                value = $1,
                currency = $2,
                actual_close = CURRENT_DATE,
                probability = 100,
                notes = COALESCE(NULLIF($3, ''), notes),
                updated_at = NOW()
          WHERE id = $4
          RETURNING *`,
        [finalPrice, currency, notes || '', item.deal_id]
      );
      deal = upd.rows[0] || null;
    }
    if (!deal) {
      const dealRes = await pool.query(
        `INSERT INTO crm_deals (user_id, contact_id, title, stage, value, currency, probability, notes, dna_sku, actual_close)
         VALUES ($1,$2,$3,'won',$4,$5,100,$6,$7,CURRENT_DATE)
         RETURNING *`,
        [ownerUserId, contactId, `Jewelry: ${item.name}`, finalPrice, currency, notes || null, item.sku]
      );
      deal = dealRes.rows[0];
    }

    // Auto-generate a paid invoice so the customer profile's "Invoices" KPI reflects reality.
    let invoice = null;
    try {
      const invoiceNumber = await generateInvoiceNumber(ownerUserId);
      const invRes = await pool.query(
        `INSERT INTO crm_invoices
           (user_id, contact_id, deal_id, invoice_number, status,
            subtotal, tax, total, currency, issued_at, paid_at, notes, metadata)
         VALUES ($1,$2,$3,$4,'paid',$5,0,$5,$6,CURRENT_DATE,NOW(),$7,$8)
         RETURNING *`,
        [
          ownerUserId, contactId, deal.id, invoiceNumber,
          finalPrice, currency,
          notes || `Auto-generated for ${item.sku || item.name}`,
          JSON.stringify({ jewelry_item_id: Number(id), sku: item.sku, source: 'jewelry_sale' }),
        ]
      );
      invoice = invRes.rows[0];
    } catch (invErr) {
      console.warn('Auto-invoice on sale warn:', invErr.message);
    }

    const r = await pool.query(
      `UPDATE jewelry_items
          SET status = 'sold', sold_at = NOW(), sold_to = $1, sold_deal_id = $2,
              deal_id = COALESCE(deal_id, $2), sale_price = $3, updated_at = NOW()
        WHERE id = $4 RETURNING *`,
      [contactId, deal.id, finalPrice, id]
    );
    await pool.query(
      `INSERT INTO jewelry_item_history (item_id, from_status, to_status, changed_by, notes)
       VALUES ($1,$2,'sold',$3,$4)`,
      [id, item.status, userId || null, `Sold via deal #${deal.id}${invoice ? `, invoice ${invoice.invoice_number}` : ''}`]
    );

    // Inventory bridge: any consumed stones on this piece are now physically gone.
    let releasedSkus = [];
    try {
      const sold = await pool.query(
        `UPDATE jewelry_item_stones
            SET inventory_status = 'sold'
          WHERE item_id = $1
            AND consume_from_inventory = TRUE
            AND (inventory_status IS NULL OR inventory_status NOT IN ('sold','returned'))
          RETURNING stone_sku`,
        [id]
      );
      releasedSkus = sold.rows.map(row => row.stone_sku).filter(Boolean);
    } catch (stSellErr) {
      console.warn('Stone-sell hook warn:', stSellErr.message);
    }

    res.json({ item: r.rows[0], deal, invoice, soldStones: releasedSkus });

    const { actorId, actorName } = getActor(req);
    const itemLabel = item.name || item.sku || `#${id}`;
    const moneyLabel = `${currency} ${finalPrice.toLocaleString('en-US', { maximumFractionDigits: 0 })}`;
    logActivity({
      userId:     ownerUserId,
      actorId, actorName,
      entityType: 'jewelry_item',
      entityId:   id,
      action:     'sold',
      summary:    `Sold ${itemLabel} for ${moneyLabel}`,
      changes:    { status: { from: item.status, to: 'sold' }, sale_price: { from: item.sale_price ?? null, to: finalPrice } },
      related: [
        { type: 'contact', id: contactId },
        { type: 'deal',    id: deal.id   },
        invoice ? { type: 'invoice', id: invoice.id } : null,
        ...releasedSkus.map(sku => ({ type: 'stone', id: sku })),
      ].filter(Boolean),
    });
  } catch (e) {
    console.error('Sell error:', e);
    res.status(500).json({ error: e.message });
  }
});

/* =====================================================================
   Customer Preview: Share links + AI mockup generation

   Replaces the old "3D Preview" tab with something the customer actually
   touches. Two parts:

   1) Share links — each row in jewelry_shares is a long, unguessable
      token granting public read access to a curated subset of the item
      (cover + photos + AI mockups + pricing + designer notes). The link
      is revocable and optionally expires. Customer responses (view,
      approve, request-changes, comment) are append-only in
      jewelry_share_responses AND mirrored to activity_log so the
      workshop sees them in the main feed without polling.

   2) AI mockup — POST a prompt and we call OpenAI Images (gpt-image-1),
      upload the result to Vercel Blob, and register it as a
      jewelry_item_files row with kind='ai_mockup'. From then on it's
      indistinguishable from any uploaded image — the share page picks it
      up automatically, the workshop can promote it to cover, etc.
   ===================================================================== */

const VALID_SHARE_RESPONSE_ACTIONS = new Set([
  'viewed',
  'approved',
  'changes_requested',
  'comment',
]);

function makeShareToken() {
  // 24 random bytes → 32 url-safe base64 chars. Plenty of entropy.
  return crypto.randomBytes(24).toString('base64')
    .replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
}

function isShareUsable(share) {
  if (!share) return false;
  if (share.revoked_at) return false;
  if (share.expires_at && new Date(share.expires_at).getTime() < Date.now()) return false;
  return true;
}

function getRequestIp(req) {
  const fwd = req.headers['x-forwarded-for'];
  if (typeof fwd === 'string') return fwd.split(',')[0].trim();
  return req.socket?.remoteAddress || null;
}

/* ---------- Share links: list / create / revoke (private) ---------- */
app.get('/api/jewelry-items/:id/shares', async (req, res) => {
  try {
    const { id } = req.params;
    const r = await pool.query(
      `SELECT s.*,
              (SELECT COUNT(*) FROM jewelry_share_responses
                 WHERE share_id = s.id AND action = 'approved') AS approve_count,
              (SELECT COUNT(*) FROM jewelry_share_responses
                 WHERE share_id = s.id AND action = 'changes_requested') AS change_request_count,
              (SELECT COUNT(*) FROM jewelry_share_responses
                 WHERE share_id = s.id AND action = 'comment') AS comment_count
         FROM jewelry_shares s
        WHERE s.item_id = $1
        ORDER BY s.created_at DESC`,
      [id]
    );
    res.json({ shares: r.rows });
  } catch (e) {
    console.error('GET shares error:', e);
    res.status(500).json({ error: e.message });
  }
});

app.post('/api/jewelry-items/:id/shares', async (req, res) => {
  try {
    const { id } = req.params;
    const { userId, expiresAt, notes } = req.body || {};

    const itemRes = await pool.query(
      `SELECT user_id, name, sku, contact_id FROM jewelry_items WHERE id = $1`,
      [id]
    );
    const item = itemRes.rows[0];
    if (!item) return res.status(404).json({ error: 'Item not found' });

    const token = makeShareToken();
    const r = await pool.query(
      `INSERT INTO jewelry_shares (item_id, token, created_by, expires_at, notes)
       VALUES ($1,$2,$3,$4,$5)
       RETURNING *`,
      [id, token, userId || null, expiresAt || null, notes || null]
    );

    res.json({ share: r.rows[0] });

    const { actorId, actorName } = getActor(req);
    logActivity({
      userId:     item.user_id,
      actorId, actorName,
      entityType: 'jewelry_item',
      entityId:   id,
      action:     'share_created',
      summary:    `Customer preview link created for ${item.name || item.sku}`,
      related:    item.contact_id ? [{ type: 'contact', id: item.contact_id }] : null,
    });
  } catch (e) {
    console.error('POST share error:', e);
    res.status(500).json({ error: e.message });
  }
});

app.delete('/api/jewelry-items/:id/shares/:shareId', async (req, res) => {
  try {
    const { id, shareId } = req.params;
    const r = await pool.query(
      `UPDATE jewelry_shares
          SET revoked_at = NOW()
        WHERE id = $1 AND item_id = $2 AND revoked_at IS NULL
        RETURNING *`,
      [shareId, id]
    );
    if (!r.rows[0]) return res.status(404).json({ error: 'Share not found or already revoked' });
    res.json({ share: r.rows[0] });

    const itemRes = await pool.query(
      `SELECT user_id, name, sku, contact_id FROM jewelry_items WHERE id = $1`,
      [id]
    );
    const item = itemRes.rows[0];
    if (item) {
      const { actorId, actorName } = getActor(req);
      logActivity({
        userId:     item.user_id,
        actorId, actorName,
        entityType: 'jewelry_item',
        entityId:   id,
        action:     'share_revoked',
        summary:    `Customer preview link revoked for ${item.name || item.sku}`,
        related:    item.contact_id ? [{ type: 'contact', id: item.contact_id }] : null,
      });
    }
  } catch (e) {
    console.error('DELETE share error:', e);
    res.status(500).json({ error: e.message });
  }
});

/* ---------- Public share endpoints (no auth) ---------- */
//
// `GET /api/share/:token` is the only window the customer ever sees into
// our DB, so we hand-pick exactly which jewelry_items columns ship out:
// no internal_notes, no markup_percent, no total_cost, no contact PII.
// Stones are summarised (count + carat total), files are filtered to
// images only. Anything we don't explicitly include here stays private.
app.get('/api/share/:token', async (req, res) => {
  try {
    const { token } = req.params;
    const sr = await pool.query(`SELECT * FROM jewelry_shares WHERE token = $1`, [token]);
    const share = sr.rows[0];
    if (!share || !isShareUsable(share)) {
      return res.status(404).json({ error: 'This preview link is no longer available.' });
    }

    const itemRes = await pool.query(
      `SELECT id, sku, name, category, type, status,
              metal_summary, weight_grams, size, description,
              cover_image_url, sale_price, created_at, updated_at
         FROM jewelry_items
        WHERE id = $1`,
      [share.item_id]
    );
    const item = itemRes.rows[0];
    if (!item) {
      return res.status(404).json({ error: 'Item not found.' });
    }

    // Image-only files (sketches, AI mockups, progress photos, finals).
    const filesRes = await pool.query(
      `SELECT id, url, kind, stage, filename, mime_type, uploaded_at
         FROM jewelry_item_files
        WHERE item_id = $1
          AND (mime_type LIKE 'image/%' OR kind IN ('sketch','progress','final','ai_mockup'))
        ORDER BY uploaded_at ASC`,
      [share.item_id]
    );

    // Stones: count + total carats only — never expose SKU/cost/cert#.
    const stonesRes = await pool.query(
      `SELECT COUNT(*)::int AS count,
              COALESCE(SUM((snapshot->>'weight')::numeric), 0)::numeric AS total_weight
         FROM jewelry_item_stones
        WHERE item_id = $1`,
      [share.item_id]
    );

    // Bump view counter (best-effort; never block the response on it)
    pool.query(
      `UPDATE jewelry_shares
          SET view_count = view_count + 1, last_viewed_at = NOW()
        WHERE id = $1`,
      [share.id]
    ).catch(() => { /* non-fatal */ });

    // Light "viewed" log — capped to once per IP per hour so refresh spam
    // doesn't drown the activity feed.
    try {
      const ip = getRequestIp(req);
      const ua = req.headers['user-agent'] || null;
      const recent = await pool.query(
        `SELECT 1 FROM jewelry_share_responses
          WHERE share_id = $1 AND action = 'viewed' AND ip = $2
            AND created_at > NOW() - INTERVAL '1 hour' LIMIT 1`,
        [share.id, ip]
      );
      if (!recent.rows.length) {
        await pool.query(
          `INSERT INTO jewelry_share_responses (share_id, action, ip, user_agent)
           VALUES ($1,'viewed',$2,$3)`,
          [share.id, ip, ua]
        );
      }
    } catch (_) { /* non-fatal */ }

    res.json({
      share: {
        id: share.id,
        createdAt: share.created_at,
        expiresAt: share.expires_at,
        notes: share.notes,
      },
      item: {
        id: item.id,
        sku: item.sku,
        name: item.name,
        category: item.category,
        type: item.type,
        status: item.status,
        metalSummary: item.metal_summary,
        weightGrams: item.weight_grams,
        size: item.size,
        description: item.description,
        coverImageUrl: item.cover_image_url,
        salePrice: item.sale_price,
      },
      images: filesRes.rows.map(r => ({
        id: r.id,
        url: r.url,
        kind: r.kind,
        stage: r.stage,
        filename: r.filename,
        uploadedAt: r.uploaded_at,
      })),
      stoneSummary: {
        count: stonesRes.rows[0]?.count || 0,
        totalCarats: Number(stonesRes.rows[0]?.total_weight || 0),
      },
    });
  } catch (e) {
    console.error('GET share error:', e);
    res.status(500).json({ error: e.message });
  }
});

app.post('/api/share/:token/respond', async (req, res) => {
  try {
    const { token } = req.params;
    const { action, customerName, comment } = req.body || {};
    if (!VALID_SHARE_RESPONSE_ACTIONS.has(action)) {
      return res.status(400).json({ error: 'Invalid action' });
    }
    const sr = await pool.query(`SELECT * FROM jewelry_shares WHERE token = $1`, [token]);
    const share = sr.rows[0];
    if (!share || !isShareUsable(share)) {
      return res.status(404).json({ error: 'This preview link is no longer available.' });
    }

    const ip = getRequestIp(req);
    const ua = req.headers['user-agent'] || null;
    const insRes = await pool.query(
      `INSERT INTO jewelry_share_responses
         (share_id, action, customer_name, comment, ip, user_agent)
       VALUES ($1,$2,$3,$4,$5,$6)
       RETURNING *`,
      [share.id, action, customerName || null, comment || null, ip, ua]
    );
    res.json({ response: insRes.rows[0] });

    // Mirror customer activity into the workshop's activity_log so it
    // shows up alongside everything else without a separate inbox.
    try {
      const itemRes = await pool.query(
        `SELECT user_id, name, sku, contact_id FROM jewelry_items WHERE id = $1`,
        [share.item_id]
      );
      const item = itemRes.rows[0];
      if (item) {
        const who = customerName ? `Customer (${customerName})` : 'Customer';
        const verbMap = {
          approved: 'approved the design',
          changes_requested: 'requested changes',
          comment: 'left a comment',
          viewed: 'viewed the preview',
        };
        logActivity({
          userId:     item.user_id,
          actorId:    null,
          actorName:  who,
          entityType: 'jewelry_item',
          entityId:   share.item_id,
          action:     `share_${action}`,
          summary:    `${who} ${verbMap[action] || action} on ${item.name || item.sku}`,
          changes:    comment ? { comment: { from: null, to: comment } } : null,
          related:    item.contact_id ? [{ type: 'contact', id: item.contact_id }] : null,
        });
      }
    } catch (_) { /* non-fatal */ }
  } catch (e) {
    console.error('POST share respond error:', e);
    res.status(500).json({ error: e.message });
  }
});

/* ---------- Share responses: list (workshop side) ---------- */
app.get('/api/jewelry-items/:id/share-responses', async (req, res) => {
  try {
    const { id } = req.params;
    const r = await pool.query(
      `SELECT r.*, s.token
         FROM jewelry_share_responses r
         JOIN jewelry_shares s ON s.id = r.share_id
        WHERE s.item_id = $1
        ORDER BY r.created_at DESC
        LIMIT 200`,
      [id]
    );
    res.json({ responses: r.rows });
  } catch (e) {
    console.error('GET share-responses error:', e);
    res.status(500).json({ error: e.message });
  }
});

/* ---------- AI Mockup: prompt → photoreal render → blob → file row ---------- */
//
// One round trip: caller sends a free-text prompt + (optional) item context
// flag. We compose a strong prompt for jewelry photography, ask OpenAI's
// gpt-image-1 model for a 1024x1024 b64 PNG, push it to Vercel Blob, and
// register it on the item as kind='ai_mockup'. Optionally promote to cover.
app.post('/api/jewelry-items/:id/ai-mockup', async (req, res) => {
  try {
    const { id } = req.params;
    const {
      prompt,
      userId,
      useItemContext = true,
      setAsCover = false,
      size = '1024x1024',
    } = req.body || {};

    if (!prompt || !String(prompt).trim()) {
      return res.status(400).json({ error: 'prompt is required' });
    }
    if (!process.env.OPENAI_API_KEY) {
      return res.status(503).json({ error: 'OPENAI_API_KEY is not configured on the server.' });
    }
    if (!blobPut || !process.env.BLOB_READ_WRITE_TOKEN) {
      return res.status(503).json({ error: 'Vercel Blob is not configured (BLOB_READ_WRITE_TOKEN missing).' });
    }

    // Pull just enough item context to ground the model. We avoid sending
    // pricing / customer info — the prompt only needs material + stone hints.
    let composedPrompt = String(prompt).trim();
    if (useItemContext) {
      const itemRes = await pool.query(
        `SELECT name, category, metal_summary, size, description
           FROM jewelry_items WHERE id = $1`,
        [id]
      );
      const item = itemRes.rows[0];
      if (item) {
        const ctxBits = [
          item.category && `category: ${item.category}`,
          item.metal_summary && `metal: ${item.metal_summary}`,
          item.size && `size: ${item.size}`,
          item.name && `piece name: ${item.name}`,
        ].filter(Boolean);
        if (ctxBits.length) {
          composedPrompt = `${composedPrompt}\n\nItem context — ${ctxBits.join('; ')}.`;
        }
      }
    }

    // Boilerplate that turns "Pear emerald two side diamonds" into a
    // proper studio jewelry shot every time, regardless of how terse the
    // user is. Negative prompts go in the same string for image models.
    const photoStyle =
      'Professional fine-jewelry product photography. Soft studio lighting, ' +
      'gradient white-to-grey seamless background, macro lens, shallow depth ' +
      'of field, hero hero-angle, crisp metal reflections, sparkle on stones, ' +
      'no human hands, no text, no watermark.';

    const finalPrompt = `${photoStyle}\n\nDesign brief: ${composedPrompt}`;

    const aiRes = await fetch('https://api.openai.com/v1/images/generations', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${process.env.OPENAI_API_KEY}`,
      },
      body: JSON.stringify({
        model: 'gpt-image-1',
        prompt: finalPrompt,
        size,
        quality: 'high',
        n: 1,
      }),
    });

    if (!aiRes.ok) {
      const errText = await aiRes.text();
      console.error('OpenAI image generation failed:', aiRes.status, errText);
      return res.status(502).json({ error: `Image generation failed (${aiRes.status})`, detail: errText.slice(0, 500) });
    }

    const aiJson = await aiRes.json();
    const b64 = aiJson?.data?.[0]?.b64_json;
    const revisedPrompt = aiJson?.data?.[0]?.revised_prompt || null;
    if (!b64) {
      return res.status(502).json({ error: 'Image generation returned no data.' });
    }

    const buffer = Buffer.from(b64, 'base64');
    const pathname = `jewelry/ai-mockup/${id}/${Date.now()}.png`;
    const blob = await blobPut(pathname, buffer, {
      access: 'public',
      contentType: 'image/png',
      addRandomSuffix: true,
    });

    const fileRes = await pool.query(
      `INSERT INTO jewelry_item_files
         (item_id, url, kind, filename, mime_type, size_bytes, uploaded_by)
       VALUES ($1, $2, 'ai_mockup', $3, 'image/png', $4, $5)
       RETURNING *`,
      [id, blob.url, `ai-mockup-${Date.now()}.png`, buffer.length, userId || null]
    );

    if (setAsCover) {
      await pool.query(
        `UPDATE jewelry_items SET cover_image_url = $1, updated_at = NOW() WHERE id = $2`,
        [blob.url, id]
      );
    }

    res.json({
      file: fileRes.rows[0],
      revisedPrompt,
      promptSent: finalPrompt,
    });

    // Activity log so the workshop sees mockups being generated.
    try {
      const itemRes = await pool.query(
        `SELECT user_id, name, sku, contact_id FROM jewelry_items WHERE id = $1`,
        [id]
      );
      const item = itemRes.rows[0];
      if (item) {
        const { actorId, actorName } = getActor(req);
        logActivity({
          userId:     item.user_id,
          actorId, actorName,
          entityType: 'jewelry_item',
          entityId:   id,
          action:     'ai_mockup_generated',
          summary:    `Generated AI mockup for ${item.name || item.sku}`,
          related:    item.contact_id ? [{ type: 'contact', id: item.contact_id }] : null,
        });
      }
    } catch (_) { /* non-fatal */ }
  } catch (e) {
    console.error('AI mockup error:', e);
    res.status(500).json({ error: e.message });
  }
});

/* ---------- Vercel Blob: Upload ---------- */
app.post('/api/blob/upload', blobUpload.single('file'), async (req, res) => {
  try {
    if (!blobPut) {
      return res.status(503).json({
        error: '@vercel/blob not installed. Run npm install on the backend.',
      });
    }
    if (!process.env.BLOB_READ_WRITE_TOKEN) {
      return res.status(503).json({
        error: 'BLOB_READ_WRITE_TOKEN env var is not set on the backend.',
      });
    }
    if (!req.file) return res.status(400).json({ error: 'No file uploaded' });

    const { originalname, mimetype, buffer, size } = req.file;
    const folder = (req.query.folder || req.body?.folder || 'jewelry').replace(/[^a-zA-Z0-9_-]/g, '');

    const ext = path.extname(originalname || '');
    const baseName = path.basename(originalname || 'file', ext).replace(/[^a-zA-Z0-9_-]/g, '_').slice(0, 80);
    const pathname = `${folder}/${Date.now()}_${baseName}${ext}`;

    const blob = await blobPut(pathname, buffer, {
      access: 'public',
      contentType: mimetype,
      addRandomSuffix: true,
    });

    res.json({
      url: blob.url,
      pathname: blob.pathname,
      contentType: mimetype,
      size,
      filename: originalname,
    });
  } catch (e) {
    console.error('Blob upload error:', e);
    res.status(500).json({ error: e.message });
  }
});

/* =========================================================
   Team / Sales-Rep endpoints
   ---------------------------------------------------------
   /api/team/me              GET   — who am I + team roster
   /api/team/members         GET   — full roster (admin + reps)
   /api/team/members         POST  — invite a new rep (by email)
   /api/team/members/:id     PUT   — edit name / role / commission / quota
   /api/team/members/:id     DELETE — soft-deactivate a rep
   /api/team/leaderboard     GET   — month-to-date KPIs per rep
   ========================================================= */

// Tiny deterministic palette so each rep gets a stable visual identity even
// without a real avatar photo. The FE renders initials + this color.
const TEAM_AVATAR_COLORS = [
  '#0ea5e9', '#10b981', '#f59e0b', '#8b5cf6', '#ef4444',
  '#06b6d4', '#ec4899', '#84cc16', '#f97316', '#6366f1',
];
function pickAvatarColor(seed) {
  let h = 0;
  for (const ch of String(seed || '')) h = ((h << 5) - h + ch.charCodeAt(0)) | 0;
  return TEAM_AVATAR_COLORS[Math.abs(h) % TEAM_AVATAR_COLORS.length];
}

// Tiny HTML escaper for the invite email — we never want a workspace name to
// leak as raw HTML.
function escHtml(s) {
  return String(s == null ? '' : s)
    .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;').replace(/'/g, '&#39;');
}

/**
 * buildInviteEmail({ rep, inviter, workspaceName, signInUrl })
 *
 * Returns { subject, html, text } so the same template is reused by the
 * initial POST and the "Resend invitation" endpoint.
 */
function buildInviteEmail({ rep, inviter, workspaceName, signInUrl, variant = 'rep', companyName = null }) {
  const repName    = escHtml(rep?.name || 'there');
  const inviteName = escHtml(inviter?.name || inviter?.email || 'Your team admin');
  const wsName     = escHtml(workspaceName || 'GEMS DNA workspace');
  const ctaUrl     = signInUrl || 'https://www.gems-dna.com/sign-in';
  const repEmail   = escHtml(rep?.email || '');
  const storeName  = escHtml(companyName || 'your store');
  const isStore    = variant === 'store_user';

  const subject = isStore
    ? `${inviter?.name || 'Your supplier'} invited ${companyName || 'your store'} to the consignment portal`
    : `${inviter?.name || 'Your team admin'} invited you to join ${workspaceName || 'GEMS DNA'}`;

  if (isStore) {
    const html = `
<!DOCTYPE html>
<html lang="en">
<head><meta charset="utf-8" /><title>${escHtml(subject)}</title></head>
<body style="margin:0;padding:0;background:#f5f5f4;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;color:#1c1917;">
  <table width="100%" cellpadding="0" cellspacing="0" border="0" style="background:#f5f5f4;padding:32px 16px;">
    <tr><td align="center">
      <table width="560" cellpadding="0" cellspacing="0" border="0" style="max-width:560px;background:#ffffff;border-radius:16px;overflow:hidden;box-shadow:0 1px 2px rgba(0,0,0,0.04);">
        <tr><td style="padding:28px 32px 0 32px;">
          <div style="display:inline-flex;align-items:center;gap:8px;">
            <div style="width:36px;height:36px;border-radius:10px;background:linear-gradient(135deg,#0ea5e9 0%,#2563eb 100%);display:inline-block;line-height:36px;text-align:center;color:white;font-weight:700;font-size:16px;">GD</div>
            <div style="font-weight:700;color:#1c1917;font-size:18px;letter-spacing:-0.01em;">GEMS DNA</div>
          </div>
        </td></tr>
        <tr><td style="padding:24px 32px 8px 32px;">
          <h1 style="margin:0 0 8px 0;font-size:22px;font-weight:700;color:#1c1917;line-height:1.3;">
            <span style="color:#2563eb;">${storeName}</span> has access to the consignment portal
          </h1>
          <p style="margin:0 0 16px 0;font-size:15px;line-height:1.6;color:#44403c;">Hey ${repName},</p>
          <p style="margin:0 0 16px 0;font-size:15px;line-height:1.6;color:#44403c;">
            <strong>${inviteName}</strong> from <strong>${wsName}</strong> invited you to a private portal where you can
            see every <strong>memo</strong> you have on consignment, mark items as <strong>sold</strong>,
            and request <strong>returns</strong> — all in one place.
          </p>
          <p style="margin:0 0 24px 0;font-size:15px;line-height:1.6;color:#44403c;">
            Create your account using this exact email so we can link it to your store account:
            <strong style="color:#1c1917;">${repEmail}</strong>
          </p>
        </td></tr>
        <tr><td align="center" style="padding:0 32px 8px 32px;">
          <a href="${ctaUrl}"
             style="display:inline-block;background:#2563eb;color:#ffffff;text-decoration:none;padding:14px 28px;border-radius:10px;font-weight:600;font-size:15px;letter-spacing:0.01em;">
             Open the portal
          </a>
        </td></tr>
        <tr><td style="padding:16px 32px 8px 32px;">
          <p style="margin:0;font-size:12px;line-height:1.6;color:#78716c;text-align:center;">
            Or paste this link into your browser:<br/>
            <a href="${ctaUrl}" style="color:#2563eb;word-break:break-all;">${ctaUrl}</a>
          </p>
        </td></tr>
        <tr><td style="padding:24px 32px;border-top:1px solid #f5f5f4;margin-top:16px;">
          <p style="margin:0;font-size:12px;line-height:1.5;color:#a8a29e;">In the portal you'll be able to:</p>
          <ul style="margin:8px 0 0 0;padding:0 0 0 18px;font-size:13px;line-height:1.7;color:#57534e;">
            <li>See every active memo your store has on consignment</li>
            <li>View item photos, specs and the agreed memo price</li>
            <li>Mark items as sold (subject to ${inviteName}'s approval)</li>
            <li>Request returns directly from the item card</li>
            <li>Get a full history of past memos for your records</li>
          </ul>
        </td></tr>
        <tr><td style="padding:18px 32px 28px 32px;background:#fafaf9;">
          <p style="margin:0;font-size:11px;line-height:1.5;color:#a8a29e;text-align:center;">
            If you weren't expecting this invitation, you can safely ignore this email — your store will not be activated until you create your account.
          </p>
        </td></tr>
      </table>
      <p style="margin:16px 0 0 0;font-size:11px;color:#a8a29e;">© GEMS DNA · Consignment portal · Sent because ${inviteName} added your store as a portal user.</p>
    </td></tr>
  </table>
</body>
</html>`;

    const text = [
      `Hey ${rep?.name || 'there'},`,
      ``,
      `${inviter?.name || 'Your supplier'} from ${workspaceName || 'GEMS DNA'} invited ${companyName || 'your store'} to the GEMS DNA consignment portal.`,
      ``,
      `In the portal you'll see every memo on consignment, mark items as sold, and request returns.`,
      ``,
      `Create your account using this exact email so we can link it to your store: ${rep?.email || ''}`,
      `${ctaUrl}`,
      ``,
      `If you weren't expecting this invitation, you can safely ignore this email.`,
      ``,
      `— GEMS DNA Consignment Portal`,
    ].join('\n');

    return { subject, html, text };
  }

  const html = `
<!DOCTYPE html>
<html lang="en">
<head><meta charset="utf-8" /><title>${escHtml(subject)}</title></head>
<body style="margin:0;padding:0;background:#f5f5f4;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;color:#1c1917;">
  <table width="100%" cellpadding="0" cellspacing="0" border="0" style="background:#f5f5f4;padding:32px 16px;">
    <tr><td align="center">
      <table width="560" cellpadding="0" cellspacing="0" border="0" style="max-width:560px;background:#ffffff;border-radius:16px;overflow:hidden;box-shadow:0 1px 2px rgba(0,0,0,0.04);">
        <tr><td style="padding:28px 32px 0 32px;">
          <div style="display:inline-flex;align-items:center;gap:8px;">
            <div style="width:36px;height:36px;border-radius:10px;background:linear-gradient(135deg,#10b981 0%,#059669 100%);display:inline-block;line-height:36px;text-align:center;color:white;font-weight:700;font-size:16px;">GD</div>
            <div style="font-weight:700;color:#1c1917;font-size:18px;letter-spacing:-0.01em;">GEMS DNA</div>
          </div>
        </td></tr>
        <tr><td style="padding:24px 32px 8px 32px;">
          <h1 style="margin:0 0 8px 0;font-size:22px;font-weight:700;color:#1c1917;line-height:1.3;">
            You're invited to join <span style="color:#059669;">${wsName}</span>
          </h1>
          <p style="margin:0 0 16px 0;font-size:15px;line-height:1.6;color:#44403c;">
            Hey ${repName},
          </p>
          <p style="margin:0 0 16px 0;font-size:15px;line-height:1.6;color:#44403c;">
            <strong>${inviteName}</strong> just added you as a sales rep on <strong>GEMS DNA</strong> — the
            workshop platform for managing customers, deals, loose-stone inventory and jewelry production.
          </p>
          <p style="margin:0 0 24px 0;font-size:15px;line-height:1.6;color:#44403c;">
            You'll see your own contacts, deals, jewelry items and the diamonds &amp; gemstones assigned to you
            the moment you log in. Create your account using this exact email so we can link it to the team:
            <strong style="color:#1c1917;">${repEmail}</strong>
          </p>
        </td></tr>
        <tr><td align="center" style="padding:0 32px 8px 32px;">
          <a href="${ctaUrl}"
             style="display:inline-block;background:#059669;color:#ffffff;text-decoration:none;padding:14px 28px;border-radius:10px;font-weight:600;font-size:15px;letter-spacing:0.01em;">
             Accept invitation
          </a>
        </td></tr>
        <tr><td style="padding:16px 32px 8px 32px;">
          <p style="margin:0;font-size:12px;line-height:1.6;color:#78716c;text-align:center;">
            Or paste this link into your browser:<br/>
            <a href="${ctaUrl}" style="color:#059669;word-break:break-all;">${ctaUrl}</a>
          </p>
        </td></tr>
        <tr><td style="padding:24px 32px;border-top:1px solid #f5f5f4;margin-top:16px;">
          <p style="margin:0;font-size:12px;line-height:1.5;color:#a8a29e;">
            What you'll be able to do:
          </p>
          <ul style="margin:8px 0 0 0;padding:0 0 0 18px;font-size:13px;line-height:1.7;color:#57534e;">
            <li>Manage the contacts &amp; deals assigned to you</li>
            <li>Sell loose diamonds &amp; gemstones from the workshop inventory</li>
            <li>Track jewelry items in production</li>
            <li>See your own commission &amp; quota progress</li>
          </ul>
        </td></tr>
        <tr><td style="padding:18px 32px 28px 32px;background:#fafaf9;">
          <p style="margin:0;font-size:11px;line-height:1.5;color:#a8a29e;text-align:center;">
            If you weren't expecting this invitation you can safely ignore this email — you won't be added until you create your account.
          </p>
        </td></tr>
      </table>
      <p style="margin:16px 0 0 0;font-size:11px;color:#a8a29e;">© GEMS DNA · Sent because ${inviteName} added you to a workspace.</p>
    </td></tr>
  </table>
</body>
</html>`;

  const text = [
    `Hey ${rep?.name || 'there'},`,
    ``,
    `${inviter?.name || 'Your team admin'} just added you as a sales rep on GEMS DNA — the workshop platform for managing customers, deals, loose-stone inventory and jewelry production.`,
    ``,
    `Create your account using this exact email so we can link it to the team: ${rep?.email || ''}`,
    `${ctaUrl}`,
    ``,
    `What you'll be able to do:`,
    `  • Manage the contacts & deals assigned to you`,
    `  • Sell loose diamonds & gemstones from the workshop inventory`,
    `  • Track jewelry items in production`,
    `  • See your own commission & quota progress`,
    ``,
    `If you weren't expecting this invitation, you can safely ignore this email — you won't be added until you create your account.`,
    ``,
    `— GEMS DNA`,
  ].join('\n');

  return { subject, html, text };
}

/**
 * sendTeamInviteEmail({ rep, inviter, workspaceName })
 *
 * Returns { ok: true, id } on success, { ok: false, error } on failure.
 * Never throws — callers can decide whether to surface the error.
 *
 * Honors RESEND_API_KEY / RESEND_FROM_EMAIL. If RESEND is not configured we
 * return { ok: false, skipped: true } so the row stays as Pending sign-in
 * but the admin can still copy the sign-in URL manually.
 */
// Resend rejects anything that isn't a real-looking address (e.g. our
// synthetic `owner-user_xxx@local` placeholder used when the owner row
// has no Clerk email). Keep this strict-but-loose: must have an @, a TLD,
// and not be the `@local` placeholder we generate internally.
function isDeliverableEmail(value) {
  if (!value || typeof value !== 'string') return false;
  const v = value.trim();
  if (!v || v.length > 254) return false;
  if (/@local$/i.test(v)) return false;
  // RFC-ish: local@domain.tld (no spaces, at least one dot in the domain)
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(v);
}

/**
 * createClerkInvitation({ email, redirectUrl, metadata, revokeExisting })
 *
 * Talks to Clerk's Backend API to mint an invitation ticket. Required when
 * the workspace has Sign-up Mode set to "Restricted" — without a ticket,
 * Clerk blocks self-signup. The ticket URL contains a one-time
 * `__clerk_ticket=...` query param that the FE `<SignUp />` component picks
 * up automatically.
 *
 * Resolves to:
 *   { ok: true,  url, ticketId, alreadyUser: false }   -> use `url` in CTA
 *   { ok: true,  url, ticketId, alreadyUser: true  }   -> existing user, send sign-in URL
 *   { ok: false, skipped: true, error }                -> CLERK_SECRET_KEY missing
 *   { ok: false, error }                                -> Clerk API error
 *
 * Notes:
 *   - We always pass `notify: false` because we send our own branded email.
 *   - Set `revokeExisting=true` (used by /resend-invite) to discard a prior
 *     pending ticket and mint a fresh one.
 */
async function createClerkInvitation({ email, redirectUrl, metadata, revokeExisting = false }) {
  if (!process.env.CLERK_SECRET_KEY) {
    return { ok: false, skipped: true, error: 'CLERK_SECRET_KEY not configured' };
  }
  if (!isDeliverableEmail(email)) {
    return { ok: false, error: `invalid email for invitation: ${email}` };
  }

  const auth = `Bearer ${process.env.CLERK_SECRET_KEY}`;

  // 1. Optionally revoke any existing pending invitation so a fresh URL is minted.
  //    Clerk identifies the existing one via `meta.invitation_id` on the
  //    duplicate_record error; we also support the `?status=pending&email_address=`
  //    list endpoint as a fallback for older API revisions.
  if (revokeExisting) {
    try {
      const list = await fetch(
        `https://api.clerk.com/v1/invitations?status=pending&query=${encodeURIComponent(email)}`,
        { headers: { Authorization: auth } }
      );
      if (list.ok) {
        const items = await list.json().catch(() => []);
        const arr = Array.isArray(items) ? items : (items?.data || []);
        for (const inv of arr) {
          if (inv?.email_address?.toLowerCase() === email.toLowerCase() && inv?.id) {
            await fetch(`https://api.clerk.com/v1/invitations/${inv.id}/revoke`, {
              method: 'POST',
              headers: { Authorization: auth },
            }).catch(() => {});
          }
        }
      }
    } catch (_) { /* best effort */ }
  }

  // 2. Create the invitation. `notify: false` -> Clerk does not send its own
  //    email; we use the returned `url` in our Resend template instead.
  try {
    const r = await fetch('https://api.clerk.com/v1/invitations', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: auth,
      },
      body: JSON.stringify({
        email_address: email,
        redirect_url: redirectUrl,
        notify: false,
        ...(metadata ? { public_metadata: metadata } : {}),
      }),
    });

    if (r.ok) {
      const data = await r.json().catch(() => ({}));
      return {
        ok: true,
        url: data?.url || null,
        ticketId: data?.id || null,
        alreadyUser: false,
      };
    }

    // Common error paths:
    //  422 duplicate_record   -> a pending invitation already exists
    //  422 form_identifier_exists / "already exists" / 400 -> user already
    //                             has a Clerk account, no ticket needed.
    const body = await r.json().catch(() => ({}));
    const errors = Array.isArray(body?.errors) ? body.errors : [];
    const code = errors[0]?.code || '';
    const msg  = (errors[0]?.message || '').toLowerCase();

    // Existing pending invitation -> fetch its URL so the email still works.
    if (code === 'duplicate_record' && errors[0]?.meta?.invitation_id) {
      const invId = errors[0].meta.invitation_id;
      // Clerk doesn't return the ticket URL on GET, so we revoke + recreate.
      await fetch(`https://api.clerk.com/v1/invitations/${invId}/revoke`, {
        method: 'POST',
        headers: { Authorization: auth },
      }).catch(() => {});
      // One retry. If this fails too, surface the original error.
      const retry = await fetch('https://api.clerk.com/v1/invitations', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', Authorization: auth },
        body: JSON.stringify({
          email_address: email,
          redirect_url: redirectUrl,
          notify: false,
          ...(metadata ? { public_metadata: metadata } : {}),
        }),
      });
      if (retry.ok) {
        const data = await retry.json().catch(() => ({}));
        return { ok: true, url: data?.url || null, ticketId: data?.id || null, alreadyUser: false };
      }
    }

    // Email already belongs to a Clerk user — they should sign IN, not up.
    if (
      code === 'form_identifier_exists' ||
      code === 'identifier_already_signed_up' ||
      msg.includes('already exists') ||
      msg.includes('already signed up')
    ) {
      return { ok: true, url: null, ticketId: null, alreadyUser: true };
    }

    return { ok: false, error: errors[0]?.message || `Clerk HTTP ${r.status}` };
  } catch (e) {
    return { ok: false, error: e.message };
  }
}

/**
 * sendTeamInviteEmail({ rep, inviter, workspaceName, ctaUrl })
 *
 * `ctaUrl` overrides the default sign-up URL. When the caller has a Clerk
 * invitation ticket URL, it should pass it here; otherwise we fall back to
 * `/sign-up?email=` (which only works if Clerk Sign-up Mode is Public).
 */
async function sendTeamInviteEmail({ rep, inviter, workspaceName, ctaUrl, variant = 'rep', companyName = null }) {
  if (!process.env.RESEND_API_KEY) {
    return { ok: false, skipped: true, error: 'RESEND_API_KEY not configured' };
  }
  if (!rep?.email) return { ok: false, error: 'rep email missing' };
  if (!isDeliverableEmail(rep.email)) {
    return { ok: false, error: `rep email is not deliverable: ${rep.email}` };
  }

  const fromEmail  = process.env.RESEND_FROM_EMAIL || 'onboarding@resend.dev';
  const senderName = (workspaceName || 'GEMS DNA').replace(/[\r\n<>]/g, '');
  const fromHeader = `${senderName} <${fromEmail}>`;
  // Default: send invitees to /sign-up. Callers that have a Clerk ticket
  // URL should pass it via `ctaUrl` so we don't fall back to a URL that
  // would be blocked by Restricted sign-up mode.
  const signInUrl  = ctaUrl || `${FRONTEND_URL.replace(/\/$/, '')}/sign-up?email=${encodeURIComponent(rep.email)}`;

  const { subject, html, text } = buildInviteEmail({ rep, inviter, workspaceName, signInUrl, variant, companyName });

  // Only set reply_to if the inviter has a real email address. Otherwise
  // Resend rejects the whole request with a 422 validation_error.
  const replyTo = isDeliverableEmail(inviter?.email) ? inviter.email : null;

  try {
    const r = await fetch('https://api.resend.com/emails', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${process.env.RESEND_API_KEY}`,
      },
      body: JSON.stringify({
        from: fromHeader,
        to: [rep.email],
        subject,
        html,
        text,
        ...(replyTo ? { reply_to: replyTo } : {}),
      }),
    });
    if (!r.ok) {
      const txt = await r.text().catch(() => '');
      return { ok: false, error: txt.slice(0, 300) || `HTTP ${r.status}` };
    }
    const data = await r.json().catch(() => ({}));
    return { ok: true, id: data?.id || null };
  } catch (e) {
    return { ok: false, error: e.message };
  }
}

// Make sure the workspace owner exists in team_members. Idempotent — safe
// to call on every /api/team/me hit. We can't auto-create reps (we don't
// know their emails), but we can guarantee the owner row is always there.
async function ensureOwnerRow(ctx) {
  if (!ctx?.actorUserId) return null;
  // If we already resolved a real member row, nothing to do.
  if (ctx.memberId) return ctx;
  try {
    const existing = await pool.query(
      `SELECT * FROM team_members
        WHERE team_owner_id = $1 AND role = 'owner'
        LIMIT 1`,
      [ctx.actorUserId]
    );
    if (existing.rows.length) {
      // Backfill clerk_user_id if missing (legacy seed rows).
      if (!existing.rows[0].clerk_user_id) {
        await pool.query(
          `UPDATE team_members SET clerk_user_id = $1, updated_at = NOW() WHERE id = $2`,
          [ctx.actorUserId, existing.rows[0].id]
        );
      }
      return { ...ctx, memberId: existing.rows[0].id, role: 'owner', isOwner: true };
    }
    const email =
      (ctx.actorEmail && String(ctx.actorEmail).trim()) || `owner-${ctx.actorUserId}@local`;
    const name = ctx.actorName || 'Workshop Owner';
    const ins = await pool.query(
      `INSERT INTO team_members
         (team_owner_id, clerk_user_id, email, name, role, avatar_color, active)
       VALUES ($1, $1, $2, $3, 'owner', $4, TRUE)
       RETURNING *`,
      [ctx.actorUserId, email, name, pickAvatarColor(ctx.actorUserId)]
    );
    return { ...ctx, memberId: ins.rows[0].id, role: 'owner', isOwner: true };
  } catch (e) {
    console.warn('ensureOwnerRow warn:', e.message);
    return ctx;
  }
}

// GET /api/team/me  — bootstrap call. Returns: { me, members }
app.get('/api/team/me', async (req, res) => {
  try {
    const ctx = await resolveTeamContext(req);
    if (!ctx.actorUserId) return res.status(400).json({ error: 'userId is required' });

    // For brand-new admins this lazily seeds the owner row.
    const finalCtx = await ensureOwnerRow(ctx);

    const ownerId = finalCtx.tenantUserId || finalCtx.actorUserId;
    const all = await pool.query(
      `SELECT id, team_owner_id, clerk_user_id, email, name, role,
              avatar_color, commission_pct, quota_monthly, active,
              company_id,
              created_at, updated_at,
              invited_at, last_invited_at, invite_count,
              (clerk_user_id IS NULL) AS pending
         FROM team_members
        WHERE team_owner_id = $1 AND active = TRUE
        ORDER BY (role = 'owner') DESC, name ASC`,
      [ownerId]
    );
    const me = all.rows.find(
      (r) => r.clerk_user_id === finalCtx.actorUserId
    ) || null;

    const role = me?.role || finalCtx.role || 'owner';
    const isStoreUser = role === 'store_user';

    // Store users are scoped to the portal — they don't need (and shouldn't
    // see) the full team roster of the supplier. We send back only their
    // own row so the FE can render the portal chrome.
    const members = isStoreUser ? (me ? [me] : []) : all.rows;

    res.json({
      me,
      members,
      tenantUserId: ownerId,
      actorUserId:  finalCtx.actorUserId,
      role,
      isOwner:      role === 'owner',
      isStoreUser,
      companyId:    me?.company_id || null,
    });
  } catch (e) {
    console.error('GET /api/team/me error:', e);
    res.status(500).json({ error: e.message });
  }
});

// GET /api/team/members  — full active roster for the current tenant.
app.get('/api/team/members', async (req, res) => {
  try {
    const ctx = await resolveTeamContext(req);
    if (!ctx.actorUserId) return res.status(400).json({ error: 'userId is required' });
    const ownerId = ctx.tenantUserId || ctx.actorUserId;
    const r = await pool.query(
      `SELECT id, team_owner_id, clerk_user_id, email, name, role,
              avatar_color, commission_pct, quota_monthly, active,
              created_at, updated_at,
              invited_at, last_invited_at, invite_count,
              (clerk_user_id IS NULL) AS pending
         FROM team_members
        WHERE team_owner_id = $1 AND active = TRUE
        ORDER BY (role = 'owner') DESC, name ASC`,
      [ownerId]
    );
    res.json({ members: r.rows });
  } catch (e) {
    console.error('GET /api/team/members error:', e);
    res.status(500).json({ error: e.message });
  }
});

// POST /api/team/members  — admin invites a rep by email + name.
// The rep doesn't need to exist in Clerk yet; clerk_user_id gets backfilled
// when they first sign in (resolveTeamContext does the email match).
app.post('/api/team/members', async (req, res) => {
  try {
    const ctx = await resolveTeamContext(req);
    if (!ctx.actorUserId) return res.status(400).json({ error: 'userId is required' });
    await ensureOwnerRow(ctx);
    if (!ctx.isOwner) return res.status(403).json({ error: 'Only the workspace owner can invite members' });

    const { email, name, role = 'rep', commissionPct, quotaMonthly, avatarColor, companyId } = req.body || {};
    if (!email || !String(email).trim()) return res.status(400).json({ error: 'email is required' });
    if (!name || !String(name).trim())  return res.status(400).json({ error: 'name is required' });
    if (!['rep', 'owner', 'store_user'].includes(role)) return res.status(400).json({ error: 'invalid role' });

    const ownerId = ctx.tenantUserId || ctx.actorUserId;

    // store_user must be tied to a single retail store. We look it up to
    // confirm it exists in this workspace before the row is created.
    let companyRow = null;
    if (role === 'store_user') {
      if (!companyId) return res.status(400).json({ error: 'companyId is required for store users' });
      const cr = await pool.query(
        `SELECT id, name, type FROM crm_companies WHERE id = $1 AND user_id = $2 LIMIT 1`,
        [companyId, ownerId]
      );
      if (!cr.rows.length) return res.status(404).json({ error: 'Store not found in your workspace' });
      companyRow = cr.rows[0];
    }

    // Cap free-tier teams at 10 members so we don't accidentally invite the
    // whole world. Owner counts as 1.
    const cnt = await pool.query(
      `SELECT COUNT(*)::int AS c FROM team_members WHERE team_owner_id = $1 AND active = TRUE`,
      [ownerId]
    );
    if (cnt.rows[0].c >= 11) {
      return res.status(400).json({ error: 'Team is full (max 10 reps + owner). Deactivate someone first.' });
    }

    try {
      // If we previously had this email as a *removed* (active=FALSE) row,
      // bring it back to life instead of erroring out with a duplicate.
      // This is what the admin actually expects when they hit "Remove" and
      // then re-invite the same person.
      const normalizedEmail = String(email).trim().toLowerCase();
      const inactivePrior = await pool.query(
        `SELECT * FROM team_members
          WHERE team_owner_id = $1
            AND LOWER(email) = $2
            AND active = FALSE
          ORDER BY updated_at DESC
          LIMIT 1`,
        [ownerId, normalizedEmail]
      );

      let member;
      if (inactivePrior.rows.length) {
        const prior = inactivePrior.rows[0];
        const upd = await pool.query(
          `UPDATE team_members
              SET active          = TRUE,
                  name            = $1,
                  role            = $2,
                  avatar_color    = COALESCE($3, avatar_color),
                  commission_pct  = $4,
                  quota_monthly   = $5,
                  company_id      = $6,
                  invited_at      = NOW(),
                  last_invited_at = NOW(),
                  invite_count    = COALESCE(invite_count, 0) + 1,
                  -- Wipe any stale Clerk linkage from the previous life so
                  -- the new rep gets re-linked cleanly on their first sign-in.
                  clerk_user_id   = NULL,
                  updated_at      = NOW()
            WHERE id = $7
            RETURNING *`,
          [
            String(name).trim(),
            role,
            avatarColor || pickAvatarColor(email),
            Number(commissionPct) || 0,
            Number(quotaMonthly)  || 0,
            role === 'store_user' ? Number(companyId) : null,
            prior.id,
          ]
        );
        member = upd.rows[0];
      } else {
        const ins = await pool.query(
          `INSERT INTO team_members
             (team_owner_id, email, name, role, avatar_color, commission_pct, quota_monthly, company_id, active, invited_at, last_invited_at, invite_count)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8, TRUE, NOW(), NOW(), 1)
           RETURNING *`,
          [
            ownerId,
            normalizedEmail,
            String(name).trim(),
            role,
            avatarColor || pickAvatarColor(email),
            Number(commissionPct) || 0,
            Number(quotaMonthly)  || 0,
            role === 'store_user' ? Number(companyId) : null,
          ]
        );
        member = ins.rows[0];
      }

      // Look up the inviter (admin) row for the email "from" name + reply-to.
      let inviter = null;
      try {
        const ownerRow = await pool.query(
          `SELECT name, email FROM team_members
            WHERE team_owner_id = $1 AND role = 'owner' LIMIT 1`,
          [ownerId]
        );
        inviter = ownerRow.rows[0] || { name: ctx.actorName, email: ctx.actorEmail };
      } catch (_) { inviter = { name: ctx.actorName, email: ctx.actorEmail }; }

      // Reps only show up under one workspace, so we treat the inviter's name
      // (or "GEMS DNA Workshop") as the workspace label.
      const workspaceName = inviter?.name ? `${inviter.name}'s workshop` : 'GEMS DNA Workshop';

      // Skip email for owner role — that's the admin themselves.
      let emailResult = { ok: false, skipped: true };
      let inviteResult = { ok: false, skipped: true };
      if (role === 'rep' || role === 'store_user') {
        // Where do we send them after they finish signing up?
        // Reps land in the dashboard; store users land directly in the portal.
        const postSignUpRedirect =
          role === 'store_user'
            ? `${FRONTEND_URL.replace(/\/$/, '')}/store-portal`
            : `${FRONTEND_URL.replace(/\/$/, '')}/dashboard`;

        // Mint a Clerk invitation ticket so the invitee can sign up even
        // when Sign-up Mode is set to Restricted in the Clerk dashboard.
        inviteResult = await createClerkInvitation({
          email: member.email,
          redirectUrl: postSignUpRedirect,
          metadata: {
            team_owner_id:  ownerId,
            team_member_id: member.id,
            role,
            company_id:     role === 'store_user' ? Number(companyId) : null,
          },
          // First invite: don't bother revoking — there shouldn't be one.
          revokeExisting: false,
        });

        let ctaUrl = null;
        if (inviteResult.ok && inviteResult.url) {
          ctaUrl = inviteResult.url;
        } else if (inviteResult.ok && inviteResult.alreadyUser) {
          // The email already has a Clerk account → send them to /sign-in
          // with the email pre-filled. No ticket needed; signing in is what
          // links them to the team_members row.
          ctaUrl = `${FRONTEND_URL.replace(/\/$/, '')}/sign-in?email=${encodeURIComponent(member.email)}`;
        }
        if (!inviteResult.ok && !inviteResult.skipped) {
          console.warn(`[clerk invite] ${member.email}:`, inviteResult.error);
        }

        emailResult = await sendTeamInviteEmail({
          rep: member,
          inviter,
          workspaceName,
          ctaUrl,
          variant: role === 'store_user' ? 'store_user' : 'rep',
          companyName: companyRow?.name || null,
        });
        if (!emailResult.ok && !emailResult.skipped) {
          console.warn(`[team invite] failed to email ${member.email}:`, emailResult.error);
        }
      }

      res.status(201).json({
        member,
        email: {
          sent:    emailResult.ok === true,
          skipped: emailResult.skipped === true,
          error:   emailResult.ok ? null : (emailResult.error || null),
        },
        invite: {
          ticketed:    inviteResult.ok === true && !inviteResult.alreadyUser && !!inviteResult.url,
          alreadyUser: inviteResult.alreadyUser === true,
          skipped:     inviteResult.skipped === true,
          error:       inviteResult.ok ? null : (inviteResult.error || null),
        },
      });

      logActivity({
        userId:     ownerId,
        actorId:    ctx.actorUserId,
        actorName:  ctx.actorName,
        entityType: 'team_member',
        entityId:   member.id,
        action:     'invited',
        summary:    `Invited ${member.name} (${member.email}) as ${member.role}` +
                    (emailResult.ok ? ' · email sent' : ''),
      });
    } catch (dupErr) {
      if (String(dupErr.message || '').includes('duplicate')) {
        return res.status(409).json({ error: 'A member with this email already exists in your team' });
      }
      throw dupErr;
    }
  } catch (e) {
    console.error('POST /api/team/members error:', e);
    res.status(500).json({ error: e.message });
  }
});

// PUT /api/team/members/:id
app.put('/api/team/members/:id', async (req, res) => {
  try {
    const ctx = await resolveTeamContext(req);
    if (!ctx.actorUserId) return res.status(400).json({ error: 'userId is required' });
    if (!ctx.isOwner)     return res.status(403).json({ error: 'Only the workspace owner can edit members' });

    const { id } = req.params;
    const ownerId = ctx.tenantUserId || ctx.actorUserId;

    const allowed = {
      name: 'name', email: 'email', role: 'role',
      avatarColor: 'avatar_color',
      commissionPct: 'commission_pct',
      quotaMonthly:  'quota_monthly',
      active: 'active',
    };
    const sets = [];
    const params = [];
    let p = 1;
    for (const [k, col] of Object.entries(allowed)) {
      if (req.body[k] !== undefined) {
        if (col === 'role' && !['rep', 'owner'].includes(req.body[k])) {
          return res.status(400).json({ error: 'invalid role' });
        }
        sets.push(`${col} = $${p++}`);
        params.push(col === 'email' ? String(req.body[k]).trim().toLowerCase() : req.body[k]);
      }
    }
    if (!sets.length) return res.status(400).json({ error: 'no fields to update' });
    sets.push(`updated_at = NOW()`);

    params.push(id, ownerId);
    const r = await pool.query(
      `UPDATE team_members SET ${sets.join(', ')}
        WHERE id = $${p++} AND team_owner_id = $${p}
        RETURNING *`,
      params
    );
    if (!r.rows[0]) return res.status(404).json({ error: 'Member not found' });
    res.json({ member: r.rows[0] });
  } catch (e) {
    console.error('PUT /api/team/members error:', e);
    res.status(500).json({ error: e.message });
  }
});

// DELETE /api/team/members/:id  — soft-deactivate (data assignments persist).
app.delete('/api/team/members/:id', async (req, res) => {
  try {
    const ctx = await resolveTeamContext(req);
    if (!ctx.actorUserId) return res.status(400).json({ error: 'userId is required' });
    if (!ctx.isOwner)     return res.status(403).json({ error: 'Only the workspace owner can remove members' });

    const { id } = req.params;
    const ownerId = ctx.tenantUserId || ctx.actorUserId;

    const r = await pool.query(
      `UPDATE team_members
          SET active = FALSE, updated_at = NOW()
        WHERE id = $1 AND team_owner_id = $2 AND role <> 'owner'
        RETURNING *`,
      [id, ownerId]
    );
    if (!r.rows[0]) return res.status(404).json({ error: 'Member not found (cannot remove owner)' });
    res.json({ success: true, member: r.rows[0] });

    logActivity({
      userId:     ownerId,
      actorId:    ctx.actorUserId,
      actorName:  ctx.actorName,
      entityType: 'team_member',
      entityId:   id,
      action:     'removed',
      summary:    `Removed ${r.rows[0].name} from the team`,
    });
  } catch (e) {
    console.error('DELETE /api/team/members error:', e);
    res.status(500).json({ error: e.message });
  }
});

// POST /api/team/members/:id/resend-invite
//   Owner-only. Re-sends the invitation email and bumps invite_count /
//   last_invited_at. Returns the updated row + the email status.
app.post('/api/team/members/:id/resend-invite', async (req, res) => {
  try {
    const ctx = await resolveTeamContext(req);
    if (!ctx.actorUserId) return res.status(400).json({ error: 'userId is required' });
    if (!ctx.isOwner)     return res.status(403).json({ error: 'Only the workspace owner can resend invitations' });

    const { id } = req.params;
    const ownerId = ctx.tenantUserId || ctx.actorUserId;

    const memberRes = await pool.query(
      `SELECT * FROM team_members
        WHERE id = $1 AND team_owner_id = $2 AND active = TRUE`,
      [id, ownerId]
    );
    if (!memberRes.rows[0]) return res.status(404).json({ error: 'Member not found' });
    const member = memberRes.rows[0];

    if (member.role === 'owner') {
      return res.status(400).json({ error: 'Cannot send invitation to the workspace owner' });
    }
    if (member.clerk_user_id) {
      return res.status(400).json({ error: `${member.name} has already accepted and signed in` });
    }

    let inviter = null;
    try {
      const ownerRow = await pool.query(
        `SELECT name, email FROM team_members
          WHERE team_owner_id = $1 AND role = 'owner' LIMIT 1`,
        [ownerId]
      );
      inviter = ownerRow.rows[0] || { name: ctx.actorName, email: ctx.actorEmail };
    } catch (_) { inviter = { name: ctx.actorName, email: ctx.actorEmail }; }

    const workspaceName = inviter?.name ? `${inviter.name}'s workshop` : 'GEMS DNA Workshop';

    // Mint a fresh Clerk invitation ticket. Pass `revokeExisting: true` so
    // any prior pending ticket is discarded and the new email always points
    // at a working URL (Clerk doesn't expose ticket URLs on GET).
    const inviteResult = await createClerkInvitation({
      email: member.email,
      redirectUrl: `${FRONTEND_URL.replace(/\/$/, '')}/dashboard`,
      metadata: {
        team_owner_id: ownerId,
        team_member_id: member.id,
        role: 'rep',
      },
      revokeExisting: true,
    });

    let ctaUrl = null;
    if (inviteResult.ok && inviteResult.url) {
      ctaUrl = inviteResult.url;
    } else if (inviteResult.ok && inviteResult.alreadyUser) {
      ctaUrl = `${FRONTEND_URL.replace(/\/$/, '')}/sign-in?email=${encodeURIComponent(member.email)}`;
    }
    if (!inviteResult.ok && !inviteResult.skipped) {
      console.warn(`[clerk invite resend] ${member.email}:`, inviteResult.error);
    }

    const emailResult = await sendTeamInviteEmail({ rep: member, inviter, workspaceName, ctaUrl });

    // Bump counters even if RESEND is misconfigured — admin still tried.
    const upd = await pool.query(
      `UPDATE team_members
          SET last_invited_at = NOW(),
              invite_count    = COALESCE(invite_count, 0) + 1,
              updated_at      = NOW()
        WHERE id = $1
        RETURNING *`,
      [id]
    );

    if (!emailResult.ok) {
      const status = emailResult.skipped ? 503 : 502;
      return res.status(status).json({
        error: emailResult.skipped
          ? 'Email service is not configured (set RESEND_API_KEY).'
          : `Failed to send invitation: ${emailResult.error || 'unknown error'}`,
        member: upd.rows[0],
      });
    }

    res.json({
      success: true,
      member: upd.rows[0],
      email:  { sent: true, id: emailResult.id || null },
      invite: {
        ticketed:    inviteResult.ok === true && !inviteResult.alreadyUser && !!inviteResult.url,
        alreadyUser: inviteResult.alreadyUser === true,
        skipped:     inviteResult.skipped === true,
        error:       inviteResult.ok ? null : (inviteResult.error || null),
      },
    });

    logActivity({
      userId:     ownerId,
      actorId:    ctx.actorUserId,
      actorName:  ctx.actorName,
      entityType: 'team_member',
      entityId:   id,
      action:     'invite_resent',
      summary:    `Re-sent invitation email to ${member.name} (${member.email})`,
    });
  } catch (e) {
    console.error('POST /api/team/members/:id/resend-invite error:', e);
    res.status(500).json({ error: e.message });
  }
});

// GET /api/team/leaderboard
//   Month-to-date snapshot per rep:
//   - assigned_contacts: contacts owned right now
//   - won_deals_mtd / revenue_mtd      (covers BOTH jewelry + loose stones —
//                                       any deal whose stage=won counts here)
//   - jewelry_in_progress              jewelry items in active production
//   - stones_in_progress               loose stones the rep has claimed
//   - inventory_in_progress            jewelry_in_progress + stones_in_progress
app.get('/api/team/leaderboard', async (req, res) => {
  try {
    const ctx = await resolveTeamContext(req);
    if (!ctx.actorUserId) return res.status(400).json({ error: 'userId is required' });
    const ownerId = ctx.tenantUserId || ctx.actorUserId;

    const monthStart = new Date();
    monthStart.setUTCDate(1);
    monthStart.setUTCHours(0, 0, 0, 0);

    const members = await pool.query(
      `SELECT id, clerk_user_id, name, email, role, avatar_color,
              commission_pct, quota_monthly
         FROM team_members
        WHERE team_owner_id = $1 AND active = TRUE
        ORDER BY (role = 'owner') DESC, name ASC`,
      [ownerId]
    );

    const out = [];
    for (const m of members.rows) {
      const who = m.clerk_user_id;
      const [contacts, wonMtd, jewActive, stonesActive] = await Promise.all([
        pool.query(
          `SELECT COUNT(*)::int AS c FROM crm_contacts WHERE user_id = $1 AND assigned_to = $2`,
          [ownerId, who]
        ),
        pool.query(
          `SELECT COUNT(*)::int AS c, COALESCE(SUM(value),0)::numeric AS v
             FROM crm_deals
            WHERE user_id = $1 AND assigned_to = $2 AND stage = 'won'
              AND COALESCE(actual_close, updated_at) >= $3`,
          [ownerId, who, monthStart]
        ),
        pool.query(
          `SELECT COUNT(*)::int AS c
             FROM jewelry_items
            WHERE user_id = $1 AND assigned_to = $2
              AND status NOT IN ('sold','archived')`,
          [ownerId, who]
        ),
        pool.query(
          `SELECT COUNT(*)::int AS c
             FROM stone_assignments
            WHERE team_owner_id = $1 AND assigned_to = $2`,
          [ownerId, who]
        ),
      ]).catch(() => [
        { rows: [{ c: 0 }] }, { rows: [{ c: 0, v: 0 }] },
        { rows: [{ c: 0 }] }, { rows: [{ c: 0 }] },
      ]);

      const revenue        = Number(wonMtd.rows[0]?.v || 0);
      const quota          = Number(m.quota_monthly || 0);
      const jewelryActive  = jewActive.rows[0]?.c || 0;
      const stonesAssigned = stonesActive.rows[0]?.c || 0;

      out.push({
        memberId:        m.id,
        clerkUserId:     m.clerk_user_id,
        name:            m.name,
        email:           m.email,
        role:            m.role,
        avatarColor:     m.avatar_color,
        commissionPct:   Number(m.commission_pct || 0),
        quotaMonthly:    quota,
        assignedContacts:contacts.rows[0]?.c || 0,
        wonDealsMtd:     wonMtd.rows[0]?.c || 0,
        revenueMtd:      revenue,
        jewelryInProgress:   jewelryActive,
        stonesInProgress:    stonesAssigned,
        inventoryInProgress: jewelryActive + stonesAssigned,
        commissionEarned:    Math.round(revenue * Number(m.commission_pct || 0)) / 100,
        quotaPct:            quota > 0 ? Math.round((revenue / quota) * 100) : null,
      });
    }
    res.json({ leaderboard: out, monthStart: monthStart.toISOString() });
  } catch (e) {
    console.error('GET /api/team/leaderboard error:', e);
    res.status(500).json({ error: e.message });
  }
});

/* =========================================================
   Start server
   ========================================================= */
app.listen(port, () => {
  console.log(`🚀 Server is running on http://localhost:${port}`);
});
