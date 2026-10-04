/* =========================================================
   Public API (API keys) — read-only access to the catalog
   ---------------------------------------------------------
   A user mints a key in the app; anything holding that key
   (a script, a Custom GPT, an MCP client) reads stones and
   jewelry exactly as that user would see them in the app —
   same location tier, same cost visibility. The key carries
   no permissions of its own: every request re-resolves the
   owner's current team context, so demoting or removing the
   user takes effect on their keys immediately.

   Only the SHA-256 of a key is stored; the full key is shown
   once, at creation.
   ========================================================= */
const crypto = require('crypto');
const rateLimit = require('express-rate-limit');
const q = require('./catalogQuery');
const { buildWorkbook } = require('./exportXlsx');
const { buildOpenApi } = require('./openapi');
const { mountMcp } = require('./mcp');

const KEY_PREFIX = 'gdna_';
const MAX_KEYS_PER_USER = 10;
const EXPORT_MAX_ROWS = 20000;
const LINK_TTL_MS = 60 * 60 * 1000;
const CACHE_TTL_MS = 2 * 60 * 1000;

const LINK_SECRET =
  process.env.API_LINK_SECRET ||
  process.env.ENCRYPT_SECRET ||
  crypto.randomBytes(32).toString('hex');

const sha256 = (s) => crypto.createHash('sha256').update(s).digest('hex');
const b64url = (buf) => Buffer.from(buf).toString('base64url');

const looksLikeApiKey = (s) => typeof s === 'string' && s.startsWith(KEY_PREFIX);

function bearerOf(req) {
  const h = req.headers['authorization'];
  if (h && /^Bearer\s+/i.test(h)) return h.replace(/^Bearer\s+/i, '').trim();
  const x = req.headers['x-api-key'];
  return x ? String(x).trim() : null;
}

// Whether a resolved team context may read the catalog at all.
const canReadCatalog = (ctx) => {
  if (!ctx || !ctx.actorUserId || ctx.isStoreUser) return false;
  if (ctx.isOwner) return true;
  const sections = ctx.permissions?.sections || [];
  return sections.includes('sales') || sections.includes('inventory');
};

class ApiError extends Error {
  constructor(status, message) {
    super(message);
    this.status = status;
  }
}

module.exports = function registerPublicApi(app, deps) {
  const {
    pool,
    requireAuth,
    resolveTeamContext,
    viewerCanSeeCost,
    listSoapStonesFor,
    listJewelryFor,
    rewriteMedia,
  } = deps;

  const ready = pool
    .query(`
      CREATE TABLE IF NOT EXISTS api_keys (
        id            SERIAL PRIMARY KEY,
        user_id       TEXT NOT NULL,
        team_owner_id TEXT,
        member_id     INTEGER,
        name          TEXT NOT NULL,
        key_prefix    TEXT NOT NULL,
        key_hash      TEXT NOT NULL UNIQUE,
        created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        last_used_at  TIMESTAMPTZ,
        use_count     BIGINT NOT NULL DEFAULT 0,
        revoked_at    TIMESTAMPTZ,
        revoked_by    TEXT
      );
      CREATE INDEX IF NOT EXISTS api_keys_user_idx ON api_keys (user_id);
      CREATE INDEX IF NOT EXISTS api_keys_tenant_idx ON api_keys (team_owner_id);
    `)
    .catch((e) => console.error('api_keys table init failed:', e.message));

  /* ---------- key → caller ---------- */

  // The team context of the key's owner as of right now. A key minted by a
  // team member dies with that membership: an unknown user would otherwise
  // fall through to resolveTeamContext's legacy "own one-person workspace".
  async function contextForKey(row) {
    const ctx = await resolveTeamContext({
      auth: { userId: row.user_id },
      headers: {},
      query: {},
      body: {},
    });
    if (!ctx.memberId) throw new ApiError(403, 'The user who created this key is no longer an active team member.');
    if (row.team_owner_id && ctx.tenantUserId !== row.team_owner_id) {
      throw new ApiError(403, 'The user who created this key has moved to another workspace.');
    }
    if (!canReadCatalog(ctx)) {
      throw new ApiError(403, "The key owner's permissions no longer include the inventory.");
    }
    return ctx;
  }

  async function authenticate(rawKey) {
    await ready;
    if (!looksLikeApiKey(rawKey)) throw new ApiError(401, 'Missing or malformed API key.');
    const r = await pool.query(
      `SELECT * FROM api_keys WHERE key_hash = $1 AND revoked_at IS NULL LIMIT 1`,
      [sha256(rawKey)]
    );
    if (!r.rows.length) throw new ApiError(401, 'Invalid or revoked API key.');
    const row = r.rows[0];
    const ctx = await contextForKey(row);
    pool
      .query(
        `UPDATE api_keys
            SET use_count = use_count + 1,
                last_used_at = NOW()
          WHERE id = $1`,
        [row.id]
      )
      .catch(() => {});
    return { key: row, ctx };
  }

  const sendError = (res, e) => {
    const status = e.status || 500;
    if (status >= 500) console.error('public api error:', e);
    res.status(status).json({ error: status >= 500 ? 'Internal server error' : e.message });
  };

  async function requireApiKey(req, res, next) {
    try {
      const { key, ctx } = await authenticate(bearerOf(req));
      req.apiKey = key;
      req.apiCtx = ctx;
      next();
    } catch (e) {
      sendError(res, e);
    }
  }

  const keyLimiter = rateLimit({
    windowMs: 60 * 1000,
    max: 240,
    standardHeaders: true,
    legacyHeaders: false,
    keyGenerator: (req) => `key:${req.apiKey?.id}`,
    message: { error: 'Rate limit exceeded (240 requests per minute per key).' },
  });

  /* ---------- data service (shared by REST and MCP) ---------- */

  const cache = new Map();
  const viewerKey = (ctx) =>
    `${ctx.actorUserId}|${ctx.role}|${JSON.stringify(ctx.permissions || {})}`;

  // An AI client fires several calls in a row; they share one catalog load
  // (including one still in flight) instead of each re-reading the table.
  function load(ctx, dataset) {
    const id = `${dataset}|${viewerKey(ctx)}`;
    const hit = cache.get(id);
    if (hit && Date.now() - hit.at < CACHE_TTL_MS) return hit.rows;
    const canSeeCost = viewerCanSeeCost(ctx);
    const rows = (
      dataset === 'jewelry'
        ? listJewelryFor(ctx).then((list) => list.map((j) => q.toApiJewelry(j, { canSeeCost })))
        : listSoapStonesFor(ctx, {}).then((list) => list.map((s) => q.toApiStone(s, { canSeeCost })))
    ).catch((e) => {
      cache.delete(id);
      throw e;
    });
    cache.set(id, { at: Date.now(), rows });
    if (cache.size > 200) cache.delete(cache.keys().next().value);
    return rows;
  }

  const service = {
    async me(ctx, key) {
      return {
        key: { id: key.id, name: key.name, prefix: key.key_prefix },
        user: { name: ctx.memberName || ctx.actorName || null, role: ctx.role },
        access: {
          readOnly: true,
          canSeeCost: viewerCanSeeCost(ctx),
          locationView: ctx.permissions?.locationView || 'full',
        },
      };
    },
    async searchStones(ctx, p) {
      return q.page(q.filterStones(await load(ctx, 'stones'), p), p, q.STONE_SORTS, 'updated');
    },
    async getStone(ctx, sku) {
      const want = String(sku || '').trim().toLowerCase();
      const s = (await load(ctx, 'stones')).find((r) => String(r.sku).toLowerCase() === want);
      if (!s) throw new ApiError(404, `Stone ${sku} not found.`);
      return s;
    },
    async summarizeStones(ctx, p) {
      const rows = q.filterStones(await load(ctx, 'stones'), p);
      return q.summarize(rows, p.groupBy, q.STONE_GROUPS, q.stoneMeasure(viewerCanSeeCost(ctx)));
    },
    async searchJewelry(ctx, p) {
      return q.page(q.filterJewelry(await load(ctx, 'jewelry'), p), p, q.JEWELRY_SORTS, 'newest');
    },
    async getJewelry(ctx, model) {
      const want = String(model || '').trim().toLowerCase();
      const j = (await load(ctx, 'jewelry')).find(
        (r) =>
          String(r.modelNumber || '').toLowerCase() === want ||
          String(r.stockNumber || '').toLowerCase() === want
      );
      if (!j) throw new ApiError(404, `Jewelry item ${model} not found.`);
      return j;
    },
    async summarizeJewelry(ctx, p) {
      const rows = q.filterJewelry(await load(ctx, 'jewelry'), p);
      return q.summarize(rows, p.groupBy, q.JEWELRY_GROUPS, q.jewelryMeasure(viewerCanSeeCost(ctx)));
    },
    async meta(ctx) {
      const [stones, jewelry] = await Promise.all([load(ctx, 'stones'), load(ctx, 'jewelry')]);
      return {
        stones: { total: stones.length, values: q.facets(stones, q.STONE_GROUPS) },
        jewelry: { total: jewelry.length, values: q.facets(jewelry, q.JEWELRY_GROUPS) },
      };
    },
    async exportRows(ctx, dataset, p) {
      const isJewelry = dataset === 'jewelry';
      const all = await load(ctx, isJewelry ? 'jewelry' : 'stones');
      const filtered = isJewelry ? q.filterJewelry(all, p) : q.filterStones(all, p);
      const sorts = isJewelry ? q.JEWELRY_SORTS : q.STONE_SORTS;
      const rows = q.sortRows(filtered, sorts, p.sort || (isJewelry ? 'newest' : 'updated'), p.order);
      if (rows.length > EXPORT_MAX_ROWS) {
        throw new ApiError(413, `Export has ${rows.length} rows; narrow the filters to ${EXPORT_MAX_ROWS} or fewer.`);
      }
      return rows;
    },
  };

  /* ---------- export links (for AI chats that can't download files) ---------- */

  const datasetOf = (v) => (String(v || '').toLowerCase() === 'jewelry' ? 'jewelry' : 'stones');

  const baseUrlOf = (req) =>
    (process.env.PUBLIC_API_URL || `${req.protocol}://${req.get('host')}`).replace(/\/+$/, '');

  const exportParams = (p) => {
    const out = {};
    for (const [k, v] of Object.entries(p || {})) {
      if (['dataset', 'limit', 'offset', 'fields', 'key'].includes(k)) continue;
      if (v == null || v === '') continue;
      out[k] = Array.isArray(v) ? v.join(',') : String(v);
    }
    return out;
  };

  function signLink(keyId, dataset, params) {
    const payload = b64url(JSON.stringify({ k: keyId, d: dataset, p: params, e: Date.now() + LINK_TTL_MS }));
    const sig = b64url(crypto.createHmac('sha256', LINK_SECRET).update(payload).digest());
    return `${payload}.${sig}`;
  }

  function readLink(token) {
    const [payload, sig] = String(token || '').split('.');
    if (!payload || !sig) throw new ApiError(400, 'Malformed link.');
    const want = b64url(crypto.createHmac('sha256', LINK_SECRET).update(payload).digest());
    const a = Buffer.from(sig);
    const b = Buffer.from(want);
    if (a.length !== b.length || !crypto.timingSafeEqual(a, b)) throw new ApiError(403, 'Invalid link.');
    const data = JSON.parse(Buffer.from(payload, 'base64url').toString('utf8'));
    if (!data.e || Date.now() > data.e) throw new ApiError(410, 'This download link has expired. Ask for a new one.');
    return data;
  }

  service.exportLink = async (ctx, key, req, p) => {
    const dataset = datasetOf(p.dataset);
    const params = exportParams(p);
    const rows = await service.exportRows(ctx, dataset, params);
    return {
      url: `${baseUrlOf(req)}/api/v1/download/${signLink(key.id, dataset, params)}`,
      dataset,
      rows: rows.length,
      expiresAt: new Date(Date.now() + LINK_TTL_MS).toISOString(),
    };
  };

  async function sendWorkbook(req, res, ctx, dataset, params) {
    const rows = rewriteMedia(await service.exportRows(ctx, dataset, params), req);
    const buf = await buildWorkbook(dataset, rows, { canSeeCost: viewerCanSeeCost(ctx) });
    const stamp = new Date().toISOString().slice(0, 10);
    res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
    res.setHeader('Content-Disposition', `attachment; filename="gems-dna-${dataset}-${stamp}.xlsx"`);
    res.setHeader('Cache-Control', 'no-store');
    res.send(Buffer.from(buf));
  }

  /* ---------- key management (signed-in app users) ---------- */

  const publicKeyRow = (r, ctx) => ({
    id: r.id,
    name: r.name,
    prefix: r.key_prefix,
    createdAt: r.created_at,
    lastUsedAt: r.last_used_at,
    useCount: Number(r.use_count || 0),
    revokedAt: r.revoked_at,
    owner: r.owner_name || null,
    mine: r.user_id === ctx.actorUserId,
  });

  app.get('/api/api-keys', requireAuth, async (req, res) => {
    try {
      await ready;
      const ctx = await resolveTeamContext(req);
      // Admins see every key in the workspace so they can revoke them.
      const r = ctx.isOwner && ctx.tenantUserId
        ? await pool.query(
            `SELECT k.*, tm.name AS owner_name
               FROM api_keys k
               LEFT JOIN team_members tm ON tm.id = k.member_id
              WHERE k.team_owner_id = $1 OR k.user_id = $2
              ORDER BY k.revoked_at IS NOT NULL, k.created_at DESC`,
            [ctx.tenantUserId, ctx.actorUserId]
          )
        : await pool.query(
            `SELECT k.*, tm.name AS owner_name
               FROM api_keys k
               LEFT JOIN team_members tm ON tm.id = k.member_id
              WHERE k.user_id = $1
              ORDER BY k.revoked_at IS NOT NULL, k.created_at DESC`,
            [ctx.actorUserId]
          );
      res.json({
        keys: r.rows.map((row) => publicKeyRow(row, ctx)),
        canCreate: canReadCatalog(ctx) && !!ctx.memberId,
        baseUrl: baseUrlOf(req),
        access: {
          canSeeCost: viewerCanSeeCost(ctx),
          locationView: ctx.permissions?.locationView || 'full',
        },
      });
    } catch (e) {
      sendError(res, e);
    }
  });

  app.post('/api/api-keys', requireAuth, deps.limiter, async (req, res) => {
    try {
      await ready;
      const ctx = await resolveTeamContext(req);
      if (!ctx.memberId || !canReadCatalog(ctx)) {
        throw new ApiError(403, 'Your account has no inventory access to share through a key.');
      }
      const name = String(req.body?.name || '').trim().slice(0, 80);
      if (!name) throw new ApiError(400, 'Give the key a name (e.g. "ChatGPT").');
      const active = await pool.query(
        `SELECT COUNT(*)::int AS n FROM api_keys WHERE user_id = $1 AND revoked_at IS NULL`,
        [ctx.actorUserId]
      );
      if (active.rows[0].n >= MAX_KEYS_PER_USER) {
        throw new ApiError(400, `You already have ${MAX_KEYS_PER_USER} active keys. Revoke one first.`);
      }
      const secret = `${KEY_PREFIX}${crypto.randomBytes(30).toString('base64url')}`;
      const ins = await pool.query(
        `INSERT INTO api_keys (user_id, team_owner_id, member_id, name, key_prefix, key_hash)
         VALUES ($1, $2, $3, $4, $5, $6)
         RETURNING *`,
        [ctx.actorUserId, ctx.tenantUserId || null, ctx.memberId, name, secret.slice(0, 12), sha256(secret)]
      );
      res.status(201).json({
        key: secret,
        apiKey: publicKeyRow({ ...ins.rows[0], owner_name: ctx.memberName }, ctx),
      });
    } catch (e) {
      sendError(res, e);
    }
  });

  app.delete('/api/api-keys/:id', requireAuth, async (req, res) => {
    try {
      await ready;
      const ctx = await resolveTeamContext(req);
      const r = await pool.query(`SELECT * FROM api_keys WHERE id = $1`, [Number(req.params.id) || 0]);
      const row = r.rows[0];
      const allowed =
        row &&
        (row.user_id === ctx.actorUserId ||
          (ctx.isOwner && ctx.tenantUserId && row.team_owner_id === ctx.tenantUserId));
      if (!allowed) throw new ApiError(404, 'Key not found.');
      await pool.query(
        `UPDATE api_keys SET revoked_at = COALESCE(revoked_at, NOW()), revoked_by = $2 WHERE id = $1`,
        [row.id, ctx.actorUserId]
      );
      cache.clear();
      res.json({ ok: true });
    } catch (e) {
      sendError(res, e);
    }
  });

  /* ---------- /api/v1 (API key) ---------- */

  const v1 = (path, handler) =>
    app.get(path, requireApiKey, keyLimiter, async (req, res) => {
      try {
        await handler(req, res, req.apiCtx);
      } catch (e) {
        sendError(res, e);
      }
    });

  app.get('/api/v1/openapi.json', (req, res) => {
    res.json(buildOpenApi(baseUrlOf(req)));
  });

  v1('/api/v1/me', async (req, res, ctx) => res.json(await service.me(ctx, req.apiKey)));
  v1('/api/v1/meta', async (req, res, ctx) => res.json(await service.meta(ctx)));
  v1('/api/v1/stones', async (req, res, ctx) => res.json(await service.searchStones(ctx, req.query)));
  v1('/api/v1/stones/summary', async (req, res, ctx) => res.json(await service.summarizeStones(ctx, req.query)));
  v1('/api/v1/stones/:sku', async (req, res, ctx) => res.json(await service.getStone(ctx, req.params.sku)));
  v1('/api/v1/jewelry', async (req, res, ctx) => res.json(await service.searchJewelry(ctx, req.query)));
  v1('/api/v1/jewelry/summary', async (req, res, ctx) => res.json(await service.summarizeJewelry(ctx, req.query)));
  v1('/api/v1/jewelry/:model', async (req, res, ctx) => res.json(await service.getJewelry(ctx, req.params.model)));
  v1('/api/v1/export', async (req, res, ctx) =>
    sendWorkbook(req, res, ctx, datasetOf(req.query.dataset), exportParams(req.query))
  );
  v1('/api/v1/export-link', async (req, res, ctx) =>
    res.json(await service.exportLink(ctx, req.apiKey, req, req.query))
  );

  // The link itself is the credential, but the key behind it is re-checked
  // so revoking a key also kills the links it handed out.
  app.get('/api/v1/download/:token', deps.limiter, async (req, res) => {
    try {
      await ready;
      const link = readLink(req.params.token);
      const r = await pool.query(`SELECT * FROM api_keys WHERE id = $1 AND revoked_at IS NULL`, [link.k]);
      if (!r.rows.length) throw new ApiError(403, 'The key behind this link was revoked.');
      const ctx = await contextForKey(r.rows[0]);
      await sendWorkbook(req, res, ctx, datasetOf(link.d), link.p || {});
    } catch (e) {
      sendError(res, e);
    }
  });

  mountMcp(app, {
    authenticate,
    bearerOf,
    service,
    keyLimiter,
    baseUrlOf,
    rewriteMedia,
    sendError,
  });
};

module.exports.looksLikeApiKey = looksLikeApiKey;
