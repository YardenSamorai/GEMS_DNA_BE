// Saved presentations: the stones and jewelry for the iPad showcase
// (/present on the frontend), kept under a name so a show can be prepared at
// the office and opened on the iPad, or run again later.
//
//   GET    /api/presentations        the ones the caller can see
//   GET    /api/presentations/:id
//   POST   /api/presentations        { name, items, settings, shared }
//   PATCH  /api/presentations/:id    any of the same fields
//   DELETE /api/presentations/:id
//
// Items are kept as the showcase's own references (a stone SKU, or "j:" and a
// jewelry model number), never as copies: prices, photos and stock stay live,
// and a piece that left the inventory is simply reported missing on opening.
//
// A presentation belongs to the workspace. A shared one is open to everyone
// in it who may present, to view and edit; a private one only to whoever
// made it. Only the maker changes which it is, and deletes it (an admin may
// also delete a shared one).

const NAME_MAX = 120;
const ITEMS_MAX = 500;
const REF_MAX = 80;
const SETTINGS = {
  prices: ['off', 'total', 'both'],
  interval: ['8', '12', '20', '30'],
  brand: ['none', 'eshed', 'gemstar'],
};
const DEFAULT_SETTINGS = { prices: 'off', interval: '12', brand: 'none' };

const canPresent = (ctx) =>
  !!ctx && ctx.isAuthenticated && !ctx.isStoreUser && !!ctx.tenantUserId &&
  (ctx.isOwner || (ctx.permissions?.sections || []).includes('inventory'));

const cleanName = (v) => String(v ?? '').replace(/\s+/g, ' ').trim().slice(0, NAME_MAX);

// The same piece twice would show twice; the first place it appears wins.
const cleanItems = (v) => {
  if (!Array.isArray(v)) return null;
  const seen = new Set();
  const out = [];
  for (const raw of v) {
    const ref = String(raw ?? '').trim();
    if (!ref || ref.length > REF_MAX || ref.includes(',') || seen.has(ref)) continue;
    seen.add(ref);
    out.push(ref);
    if (out.length >= ITEMS_MAX) break;
  }
  return out;
};

const cleanSettings = (v, base = DEFAULT_SETTINGS) => {
  const out = { ...DEFAULT_SETTINGS, ...base };
  if (v && typeof v === 'object') {
    for (const [k, allowed] of Object.entries(SETTINGS)) {
      if (allowed.includes(String(v[k]))) out[k] = String(v[k]);
    }
  }
  return out;
};

const out = (row, ctx) => {
  const mine = row.created_by === ctx.actorUserId;
  return {
    id: row.id,
    name: row.name,
    items: row.items || [],
    settings: cleanSettings(row.settings),
    shared: row.shared,
    createdByName: row.created_by_name,
    updatedByName: row.updated_by_name,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
    mine,
    canDelete: mine || (row.shared && !!ctx.isOwner),
  };
};

module.exports = function registerPresentations(app, { pool, requireAuth, resolveTeamContext, limiter }) {
  (async () => {
    try {
      await pool.query(`
        CREATE TABLE IF NOT EXISTS presentations (
          id SERIAL PRIMARY KEY,
          tenant_id TEXT NOT NULL,
          name TEXT NOT NULL,
          items JSONB NOT NULL DEFAULT '[]',
          settings JSONB NOT NULL DEFAULT '{}',
          shared BOOLEAN NOT NULL DEFAULT TRUE,
          created_by TEXT,
          created_by_name TEXT,
          updated_by_name TEXT,
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
      `);
      await pool.query(`CREATE INDEX IF NOT EXISTS idx_presentations_tenant ON presentations(tenant_id)`);
      console.log('presentations ready');
    } catch (err) {
      console.error('presentations table creation error:', err);
    }
  })();

  const presenter = [
    requireAuth,
    async (req, res, next) => {
      try {
        const ctx = await resolveTeamContext(req);
        if (!canPresent(ctx)) return res.status(403).json({ error: 'Not allowed' });
        req.teamCtx = ctx;
        return next();
      } catch (e) {
        return res.status(500).json({ error: 'Authorization check failed' });
      }
    },
  ];
  const actorName = (ctx) => ctx.memberName || ctx.actorName || null;

  // Visible to the caller: in their workspace, and shared or their own.
  const VISIBLE = `tenant_id = $1 AND (shared OR created_by = $2)`;
  const findVisible = async (ctx, id) => {
    const { rows } = await pool.query(
      `SELECT * FROM presentations WHERE id = $3 AND ${VISIBLE}`,
      [ctx.tenantUserId, ctx.actorUserId, id]
    );
    return rows[0] || null;
  };
  const idParam = (req) => {
    const id = Number.parseInt(req.params.id, 10);
    return Number.isFinite(id) && id > 0 ? id : null;
  };

  app.get('/api/presentations', ...presenter, async (req, res) => {
    try {
      const ctx = req.teamCtx;
      const { rows } = await pool.query(
        `SELECT * FROM presentations WHERE ${VISIBLE} ORDER BY updated_at DESC LIMIT 500`,
        [ctx.tenantUserId, ctx.actorUserId]
      );
      res.json({ presentations: rows.map((r) => out(r, ctx)) });
    } catch (e) {
      console.error('presentations list error:', e);
      res.status(500).json({ error: e.message });
    }
  });

  app.get('/api/presentations/:id', ...presenter, async (req, res) => {
    try {
      const id = idParam(req);
      const row = id && (await findVisible(req.teamCtx, id));
      if (!row) return res.status(404).json({ error: 'Presentation not found' });
      res.json(out(row, req.teamCtx));
    } catch (e) {
      console.error('presentation read error:', e);
      res.status(500).json({ error: e.message });
    }
  });

  app.post('/api/presentations', limiter, ...presenter, async (req, res) => {
    try {
      const ctx = req.teamCtx;
      const name = cleanName(req.body?.name);
      if (!name) return res.status(400).json({ error: 'A name is required' });
      const items = cleanItems(req.body?.items) || [];
      const { rows } = await pool.query(
        `INSERT INTO presentations (tenant_id, name, items, settings, shared, created_by, created_by_name, updated_by_name)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $7)
         RETURNING *`,
        [ctx.tenantUserId, name, JSON.stringify(items), JSON.stringify(cleanSettings(req.body?.settings)),
          req.body?.shared !== false, ctx.actorUserId, actorName(ctx)]
      );
      res.status(201).json(out(rows[0], ctx));
    } catch (e) {
      console.error('presentation create error:', e);
      res.status(500).json({ error: e.message });
    }
  });

  app.patch('/api/presentations/:id', limiter, ...presenter, async (req, res) => {
    try {
      const ctx = req.teamCtx;
      const id = idParam(req);
      const row = id && (await findVisible(ctx, id));
      if (!row) return res.status(404).json({ error: 'Presentation not found' });
      const body = req.body || {};
      const next = { name: row.name, items: row.items || [], settings: cleanSettings(row.settings), shared: row.shared };
      if (body.name !== undefined) {
        next.name = cleanName(body.name);
        if (!next.name) return res.status(400).json({ error: 'A name is required' });
      }
      if (body.items !== undefined) {
        next.items = cleanItems(body.items);
        if (!next.items) return res.status(400).json({ error: 'Items must be a list' });
      }
      if (body.settings !== undefined) next.settings = cleanSettings(body.settings, next.settings);
      if (body.shared !== undefined && Boolean(body.shared) !== row.shared) {
        if (row.created_by !== ctx.actorUserId) {
          return res.status(403).json({ error: 'Only whoever made this presentation can change who sees it' });
        }
        next.shared = Boolean(body.shared);
      }
      const { rows } = await pool.query(
        `UPDATE presentations
            SET name = $2, items = $3, settings = $4, shared = $5, updated_by_name = $6, updated_at = NOW()
          WHERE id = $1
          RETURNING *`,
        [id, next.name, JSON.stringify(next.items), JSON.stringify(next.settings), next.shared, actorName(ctx)]
      );
      res.json(out(rows[0], ctx));
    } catch (e) {
      console.error('presentation update error:', e);
      res.status(500).json({ error: e.message });
    }
  });

  app.delete('/api/presentations/:id', limiter, ...presenter, async (req, res) => {
    try {
      const ctx = req.teamCtx;
      const id = idParam(req);
      const row = id && (await findVisible(ctx, id));
      if (!row) return res.status(404).json({ error: 'Presentation not found' });
      if (!out(row, ctx).canDelete) {
        return res.status(403).json({ error: 'Only whoever made this presentation can delete it' });
      }
      await pool.query(`DELETE FROM presentations WHERE id = $1`, [id]);
      res.json({ ok: true });
    } catch (e) {
      console.error('presentation delete error:', e);
      res.status(500).json({ error: e.message });
    }
  });
};

module.exports.cleanItems = cleanItems;
module.exports.cleanSettings = cleanSettings;
