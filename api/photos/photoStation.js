// Photo Station: office staff photograph stones and jewelry, a manager
// approves each shot, and the approved image becomes a DNA photo of the item:
// it is added to the item's gallery (see utils/dnaPhotos.js). Optionally a
// stone's photo is also uploaded to Barak's picture FTP (see
// utils/barakPictures.js for the naming contract).
//
//   GET  /api/photo-station/status              what is configured
//   GET  /api/photo-station/queue               stones still needing a photo, most valuable first
//   GET  /api/photo-station/search?q=           stone SKUs and jewelry model numbers matching what is typed
//   GET  /api/photo-station/mine                the caller's recent shots and their fate
//   GET  /api/photo-station/stone/:sku          one stone or jewelry item, for a scanned barcode
//   POST /api/photo-station/captures            upload + process a shot (multipart: photo, sku)
//   GET  /api/photo-station/captures            review list (manager)
//   POST /api/photo-station/captures/:id/approve        save the chosen version in DNA (manager)
//   POST /api/photo-station/captures/:id/send-to-barak  upload an approved stone photo to Barak (manager)
//   POST /api/photo-station/captures/:id/reject         send the item back to the queue (manager)
//
// Capture lifecycle: pending -> approved
//                    pending -> rejected
//                    pending -> replaced   (the same item was shot again)
//                    failed                (no stone found; kept, with the
//                                           original, to tune the checks)
//                    uploaded              (before DNA photos: sent to Barak only)
// Sending to Barak is tracked apart, in barak_status: sending -> sent | failed.
// A failed send leaves the DNA photo in place and can simply be retried.

const multer = require('multer');
const sharp = require('sharp');
const { processClean, processCutout } = require('../../utils/stonePhoto');
const ai = require('../../utils/aiStonePhoto');
const barak = require('../../utils/barakPictures');
const { applyDnaPhotos } = require('../../utils/dnaPhotos');

const DEFAULT_BRANCH = 'Israel';
const DEFAULT_CATEGORY = 'emerald';

const upload = multer({ storage: multer.memoryStorage(), limits: { fileSize: 25 * 1024 * 1024 } });

// Same rule as the frontend's usableMediaUrl: the feed sometimes carries a
// bare folder URL, which is not a photo.
const usableUrl = (u) => {
  const s = typeof u === 'string' ? u.trim() : '';
  return !!s && !!s.split('?')[0].split('/').pop();
};
const hasPhoto = (row) =>
  usableUrl(row.image) || String(row.additional_pictures || '').split(';').some(usableUrl);
const jewelryHasPhoto = (row) => String(row.all_pictures_link || '').split(';').some(usableUrl);

const canShoot = (ctx) =>
  !!ctx && !ctx.isStoreUser && (ctx.isOwner || ctx.role === 'manager' ||
    (ctx.permissions?.sections || []).includes('photos'));
const canReview = (ctx) => !!ctx && !ctx.isStoreUser && (ctx.isOwner || ctx.role === 'manager');

const stoneSummary = (row, onMemo) => ({
  sku: row.sku,
  itemType: 'stone',
  shape: row.shape || null,
  weightCt: row.weight != null ? Number(row.weight) : null,
  category: row.category || null,
  color: row.color || null,
  groupingType: row.grouping_type || null,
  stones: row.stones != null ? Number(row.stones) : null,
  branch: row.branch || null,
  location: row.location || null,
  box: row.box || null,
  holder: row.holder || null,
  onMemo,
  hasPhoto: hasPhoto(row),
});

// A jewelry item goes by its model number, which is what a capture's sku holds.
const jewelrySummary = (row, onMemo) => ({
  sku: row.model_number,
  itemType: 'jewelry',
  title: row.title || null,
  stockNumber: row.stock_number || null,
  category: row.jewelry_type || row.category || null,
  metal: row.metal_type || null,
  weightCt: row.total_carat != null ? Number(row.total_carat) : null,
  location: row.location || null,
  onMemo,
  hasPhoto: jewelryHasPhoto(row),
});

const STONE_COLS = `sku, shape, weight, category, color, grouping_type, stones, branch, location, box, holder,
                    image, additional_pictures`;
const JEWELRY_COLS = `model_number, stock_number, title, jewelry_type, category, metal_type, total_carat,
                      location, all_pictures_link`;

// The URL of the version a reviewer picked; $2 is the variant.
const VARIANT_URL = `(CASE $2::text WHEN 'cutout' THEN cutout_url WHEN 'ai' THEN ai_url ELSE clean_url END)`;

// Queue tabs. Lots without a grouping type are parcels in practice.
const KINDS = { single: 'Single', pair: 'Pair', set: 'Set', parcel: 'Parcel' };
const kindOf = (row) => {
  const g = String(row.grouping_type || '').toLowerCase();
  return KINDS[g] ? g : 'parcel';
};

// Generic bulk lots ("LOT PS", "LOT EC-2") hold hundreds of carats in the
// factory; they are not what a photographer at the office should start with.
const isBulkLot = (sku) => /^LOT\b/i.test(String(sku || ''));

const countBy = (rows, key) => {
  const out = {};
  for (const r of rows) {
    const k = key(r);
    out[k] = (out[k] || 0) + 1;
  }
  return out;
};

const captureOut = (c) => ({
  id: c.id,
  sku: c.sku,
  itemType: c.item_type || 'stone',
  status: c.status,
  originalUrl: c.original_url,
  cleanUrl: c.clean_url,
  cutoutUrl: c.cutout_url,
  cutoutError: c.cutout_error,
  aiUrl: c.ai_url,
  aiStatus: c.ai_status,
  aiError: c.ai_error,
  chosenVariant: c.chosen_variant,
  quality: c.quality,
  capturedBy: c.captured_by_name,
  capturedAt: c.captured_at,
  reviewedBy: c.reviewed_by_name,
  reviewedAt: c.reviewed_at,
  rejectReason: c.reject_reason,
  dnaImageUrl: c.dna_image_url,
  savedAt: c.saved_at,
  barakStatus: c.barak_status,
  ftpFilename: c.ftp_filename,
  uploadedAt: c.uploaded_at,
  uploadError: c.upload_error,
  filenameWarning: (c.item_type || 'stone') === 'stone' ? barak.skuFilenameWarning(c.sku) : null,
});

module.exports = function registerPhotoStation(app, { pool, requireAuth, resolveTeamContext, computeOnMemo, blobPut, limiter }) {
  (async () => {
    try {
      await pool.query(`
        CREATE TABLE IF NOT EXISTS stone_photo_captures (
          id SERIAL PRIMARY KEY,
          sku TEXT NOT NULL,
          status TEXT NOT NULL DEFAULT 'pending',
          original_url TEXT,
          clean_url TEXT,
          cutout_url TEXT,
          cutout_error TEXT,
          chosen_variant TEXT,
          quality JSONB,
          captured_by TEXT,
          captured_by_name TEXT,
          captured_at TIMESTAMP DEFAULT NOW(),
          reviewed_by TEXT,
          reviewed_by_name TEXT,
          reviewed_at TIMESTAMP,
          reject_reason TEXT,
          ftp_filename TEXT,
          uploaded_at TIMESTAMP,
          upload_error TEXT
        )
      `);
      await pool.query(`CREATE INDEX IF NOT EXISTS idx_stone_photo_captures_sku ON stone_photo_captures(sku)`);
      await pool.query(`CREATE INDEX IF NOT EXISTS idx_stone_photo_captures_status ON stone_photo_captures(status)`);
      await pool.query(`
        ALTER TABLE stone_photo_captures
          ADD COLUMN IF NOT EXISTS ai_url TEXT,
          ADD COLUMN IF NOT EXISTS ai_status TEXT,
          ADD COLUMN IF NOT EXISTS ai_error TEXT
      `);
      await pool.query(`
        ALTER TABLE stone_photo_captures
          ADD COLUMN IF NOT EXISTS item_type TEXT NOT NULL DEFAULT 'stone',
          ADD COLUMN IF NOT EXISTS dna_image_url TEXT,
          ADD COLUMN IF NOT EXISTS saved_at TIMESTAMP,
          ADD COLUMN IF NOT EXISTS barak_status TEXT
      `);
      await pool.query(
        `UPDATE stone_photo_captures SET barak_status = 'sent' WHERE status = 'uploaded' AND barak_status IS NULL`
      );
      // A restart kills edits and uploads in flight; they would otherwise spin forever.
      await pool.query(
        `UPDATE stone_photo_captures SET ai_status = 'failed', ai_error = 'Interrupted by a server restart'
          WHERE ai_status = 'working'`
      );
      await pool.query(
        `UPDATE stone_photo_captures SET barak_status = 'failed', upload_error = 'Interrupted by a server restart'
          WHERE barak_status = 'sending'`
      );
      await pool.query(
        `UPDATE stone_photo_captures SET status = 'pending', upload_error = 'Interrupted by a server restart'
          WHERE status = 'uploading'`
      );
      console.log('stone_photo_captures ready');
    } catch (err) {
      console.error('stone_photo_captures table creation error:', err);
    }
  })();

  const guard = (check) => async (req, res, next) => {
    try {
      const ctx = await resolveTeamContext(req);
      if (!check(ctx)) return res.status(403).json({ error: 'Not allowed' });
      req.teamCtx = ctx;
      return next();
    } catch (e) {
      return res.status(500).json({ error: 'Authorization check failed' });
    }
  };
  const shooter = [requireAuth, guard(canShoot)];
  const reviewer = [requireAuth, guard(canReview)];

  const putBlob = async (sku, kind, buffer) => {
    const safe = String(sku).replace(/[^a-zA-Z0-9_-]/g, '_').slice(0, 60);
    const blob = await blobPut(`stone-photos/${safe}/${Date.now()}_${kind}.jpg`, buffer, {
      access: 'public',
      contentType: 'image/jpeg',
      addRandomSuffix: true,
    });
    return blob.url;
  };

  // A scanned or typed code: a stone SKU first, else a jewelry model number
  // or stock number.
  const findItem = async (code) => {
    const stone = await pool.query(`SELECT ${STONE_COLS} FROM soap_stones WHERE UPPER(sku) = UPPER($1) LIMIT 1`, [code]);
    if (stone.rows.length) return { itemType: 'stone', sku: stone.rows[0].sku, row: stone.rows[0] };
    const piece = await pool.query(
      `SELECT ${JEWELRY_COLS} FROM jewelry_products
        WHERE UPPER(model_number) = UPPER($1) OR UPPER(stock_number) = UPPER($1)
        ORDER BY (UPPER(model_number) = UPPER($1)) DESC LIMIT 1`,
      [code]
    );
    if (piece.rows.length) return { itemType: 'jewelry', sku: piece.rows[0].model_number, row: piece.rows[0] };
    return null;
  };
  const summaryOf = (item) =>
    (item.itemType === 'jewelry' ? jewelrySummary : stoneSummary)(item.row, computeOnMemo(item.row.location));

  const attachToItem = async (capture) => {
    try {
      await applyDnaPhotos(pool, capture.item_type, capture.sku);
    } catch (e) {
      // The capture is approved either way; the next sync attaches it.
      console.error('photo-station attach error:', e.message);
    }
  };

  // Uploads an approved stone photo to Barak's FTP. Returns the updated row,
  // or null when it is not an approved stone photo waiting to be sent.
  const sendToBarak = async (id) => {
    const claimed = await pool.query(
      `UPDATE stone_photo_captures SET barak_status = 'sending', upload_error = NULL
        WHERE id = $1 AND status = 'approved' AND item_type = 'stone'
          AND (barak_status IS NULL OR barak_status = 'failed')
        RETURNING *`,
      [id]
    );
    if (!claimed.rows.length) return null;
    const capture = claimed.rows[0];
    try {
      if (!barak.isConfigured()) throw new Error('Barak picture FTP is not configured');
      const imgRes = await fetch(capture.dna_image_url);
      if (!imgRes.ok) throw new Error(`Could not read the stored photo (${imgRes.status})`);
      const filename = await barak.uploadStonePhoto(capture.sku, Buffer.from(await imgRes.arrayBuffer()));
      const done = await pool.query(
        `UPDATE stone_photo_captures SET barak_status = 'sent', ftp_filename = $2, uploaded_at = NOW()
          WHERE id = $1 RETURNING *`,
        [id, filename]
      );
      return done.rows[0];
    } catch (e) {
      const failed = await pool.query(
        `UPDATE stone_photo_captures SET barak_status = 'failed', upload_error = $2 WHERE id = $1 RETURNING *`,
        [id, String(e.message).slice(0, 300)]
      );
      return failed.rows[0];
    }
  };

  app.get('/api/photo-station/status', ...shooter, async (req, res) => {
    const out = {
      canReview: canReview(req.teamCtx),
      storage: !!(blobPut && process.env.BLOB_READ_WRITE_TOKEN),
      cutout: !!process.env.REMOVE_BG_API_KEY,
      ai: ai.isConfigured(),
      aiModel: ai.isConfigured() ? ai.MODEL : null,
      ftp: barak.isConfigured(),
      suffix: barak.SUFFIX,
    };
    if (req.query.check === '1' && out.canReview) out.ftpConnection = await barak.checkConnection();
    res.json(out);
  });

  app.get('/api/photo-station/queue', ...shooter, async (req, res) => {
    try {
      const branch = String(req.query.branch || DEFAULT_BRANCH).trim();
      const category = String(req.query.category || DEFAULT_CATEGORY).trim().toLowerCase();
      const limit = Math.min(200, Math.max(1, parseInt(req.query.limit, 10) || 30));
      const kind = String(req.query.kind || 'all').toLowerCase();
      const location = String(req.query.location || 'all').trim();
      const { rows } = await pool.query(
        `SELECT s.sku, s.shape, s.weight, s.category, s.color, s.grouping_type, s.stones,
                s.branch, s.location, s.box, s.holder, s.total_price, s.image, s.additional_pictures,
                EXISTS (
                  SELECT 1 FROM stone_photo_captures c
                   WHERE c.sku = s.sku AND c.item_type = 'stone'
                     AND c.status IN ('pending', 'approved', 'uploaded')) AS in_flow
           FROM soap_stones s
          WHERE s.sku IS NOT NULL
            AND ($1 = '' OR s.branch = $1)
            AND ($2 = '' OR LOWER(s.category) LIKE '%' || $2 || '%')`,
        [branch === 'all' ? '' : branch, category === 'all' ? '' : category]
      );
      const photographed = rows.filter(hasPhoto).length;
      const unphotographed = rows.filter((r) => !hasPhoto(r));
      const onMemo = unphotographed.filter((r) => !r.in_flow && computeOnMemo(r.location)).length;
      const waiting = unphotographed
        .filter((r) => !r.in_flow && !computeOnMemo(r.location))
        .sort((a, b) => isBulkLot(a.sku) - isBulkLot(b.sku) || (Number(b.total_price) || 0) - (Number(a.total_price) || 0));
      // Each filter's counts are taken with the other filter applied, so the
      // numbers on the chips are what tapping them would show.
      const atLocation = location === 'all' ? waiting : waiting.filter((r) => (r.location || '') === location);
      const ofKind = kind === 'all' ? waiting : waiting.filter((r) => kindOf(r) === kind);
      const shown = atLocation.filter((r) => kind === 'all' || kindOf(r) === kind);
      const rejected = shown.length
        ? await pool.query(
          `SELECT DISTINCT ON (sku) sku, reject_reason, reviewed_at FROM stone_photo_captures
            WHERE status = 'rejected' AND item_type = 'stone' AND sku = ANY($1)
            ORDER BY sku, reviewed_at DESC`,
          [shown.slice(0, limit).map((r) => r.sku)]
        )
        : { rows: [] };
      const rejectedBySku = new Map(rejected.rows.map((r) => [r.sku, r]));
      const counts = await pool.query(
        `SELECT status, COUNT(*)::int AS n FROM stone_photo_captures
          WHERE status IN ('pending', 'approved', 'uploaded') GROUP BY status`
      );
      const mine = await pool.query(
        `SELECT COUNT(*) FILTER (WHERE captured_at >= date_trunc('day', NOW()))::int AS today,
                COUNT(*) FILTER (WHERE captured_at >= date_trunc('week', NOW()))::int AS week
           FROM stone_photo_captures
          WHERE captured_by = $1 AND status NOT IN ('replaced', 'failed')`,
        [req.teamCtx.actorUserId]
      );
      const team = await pool.query(
        `SELECT COALESCE(captured_by_name, '') AS name,
                COUNT(*) FILTER (WHERE captured_at >= date_trunc('day', NOW()))::int AS today,
                COUNT(*)::int AS week,
                COUNT(*) FILTER (WHERE status IN ('approved', 'uploaded'))::int AS saved
           FROM stone_photo_captures
          WHERE status NOT IN ('replaced', 'failed') AND captured_at >= date_trunc('week', NOW())
          GROUP BY 1 ORDER BY week DESC, name LIMIT 8`
      );
      const recent = await pool.query(
        `SELECT * FROM stone_photo_captures
          WHERE status NOT IN ('replaced', 'failed')
          ORDER BY captured_at DESC LIMIT 8`
      );
      const byStatus = Object.fromEntries(counts.rows.map((r) => [r.status, r.n]));
      res.json({
        remaining: waiting.length,
        matching: shown.length,
        // Every stone in scope: has a photo, has a shot awaiting review (or
        // approved but not yet in the gallery), waits here, or is out on memo
        // and can't be shot right now.
        progress: {
          total: rows.length,
          photographed,
          inFlow: unphotographed.filter((r) => r.in_flow).length,
          waiting: waiting.length,
          onMemo,
        },
        team: team.rows,
        recent: recent.rows.map(captureOut),
        awaitingReview: byStatus.pending || 0,
        saved: (byStatus.approved || 0) + (byStatus.uploaded || 0),
        mineToday: mine.rows[0]?.today || 0,
        mineWeek: mine.rows[0]?.week || 0,
        kinds: { all: atLocation.length, ...countBy(atLocation, kindOf) },
        locations: Object.entries(countBy(ofKind, (r) => r.location || ''))
          .filter(([name]) => name)
          .sort((a, b) => b[1] - a[1])
          .map(([name, n]) => ({ name, n })),
        items: shown.slice(0, limit).map((r, i) => {
          const rej = rejectedBySku.get(r.sku);
          return {
            rank: i + 1,
            ...stoneSummary(r, false),
            lastRejected: rej ? { reason: rej.reject_reason, at: rej.reviewed_at } : null,
          };
        }),
      });
    } catch (e) {
      console.error('photo-station queue error:', e);
      res.status(500).json({ error: e.message });
    }
  });

  app.get('/api/photo-station/search', ...shooter, async (req, res) => {
    try {
      const q = String(req.query.q || '').trim();
      if (!q) return res.json({ items: [] });
      const compact = q.toUpperCase().replace(/[^A-Z0-9]/g, '');
      const params = [q, compact, q.replace(/[\\%_]/g, '\\$&')];
      const match = (col) => `(${col} ILIKE '%' || $3 || '%'
             OR ($2 <> '' AND regexp_replace(UPPER(${col}), '[^A-Z0-9]', '', 'g') LIKE '%' || $2 || '%'))`;
      const order = (col) => `(UPPER(${col}) = UPPER($1)) DESC,
                   ($2 <> '' AND regexp_replace(UPPER(${col}), '[^A-Z0-9]', '', 'g') LIKE $2 || '%') DESC,
                   LENGTH(${col}), ${col}`;
      const [stones, jewelry] = await Promise.all([
        pool.query(`SELECT ${STONE_COLS} FROM soap_stones WHERE ${match('sku')} ORDER BY ${order('sku')} LIMIT 8`, params),
        pool.query(
          `SELECT ${JEWELRY_COLS} FROM jewelry_products
            WHERE ${match('model_number')} OR UPPER(stock_number) = UPPER($1)
            ORDER BY ${order('model_number')} LIMIT 8`,
          params
        ),
      ]);
      const exact = (s) => s.sku.toUpperCase() === q.toUpperCase() || (s.stockNumber || '').toUpperCase() === q.toUpperCase();
      const items = [
        ...stones.rows.map((r) => stoneSummary(r, computeOnMemo(r.location))),
        ...jewelry.rows.map((r) => jewelrySummary(r, computeOnMemo(r.location))),
      ];
      res.json({ items: [...items.filter(exact), ...items.filter((s) => !exact(s))].slice(0, 8) });
    } catch (e) {
      console.error('photo-station search error:', e);
      res.status(500).json({ error: e.message });
    }
  });

  app.get('/api/photo-station/mine', ...shooter, async (req, res) => {
    try {
      const limit = Math.min(50, Math.max(1, parseInt(req.query.limit, 10) || 12));
      const { rows } = await pool.query(
        `SELECT * FROM stone_photo_captures
          WHERE captured_by = $1 AND status NOT IN ('replaced', 'failed')
          ORDER BY captured_at DESC LIMIT $2`,
        [req.teamCtx.actorUserId, limit]
      );
      res.json({ items: rows.map(captureOut) });
    } catch (e) {
      console.error('photo-station mine error:', e);
      res.status(500).json({ error: e.message });
    }
  });

  app.get('/api/photo-station/stone/:sku', ...shooter, async (req, res) => {
    try {
      const item = await findItem(String(req.params.sku || '').trim());
      if (!item) return res.status(404).json({ error: 'Item not found' });
      const last = await pool.query(
        `SELECT * FROM stone_photo_captures
          WHERE sku = $1 AND item_type = $2 AND status NOT IN ('replaced', 'failed')
          ORDER BY captured_at DESC LIMIT 1`,
        [item.sku, item.itemType]
      );
      res.json({
        ...summaryOf(item),
        lastCapture: last.rows[0] ? captureOut(last.rows[0]) : null,
      });
    } catch (e) {
      console.error('photo-station stone error:', e);
      res.status(500).json({ error: e.message });
    }
  });

  app.post('/api/photo-station/captures', limiter, ...shooter, upload.single('photo'), async (req, res) => {
    try {
      if (!blobPut || !process.env.BLOB_READ_WRITE_TOKEN) {
        return res.status(503).json({ error: 'Photo storage (BLOB_READ_WRITE_TOKEN) is not configured' });
      }
      if (!req.file) return res.status(400).json({ error: 'No photo uploaded' });
      const item = await findItem(String(req.body?.sku || '').trim());
      if (!item) return res.status(404).json({ error: 'Item not found' });
      const { sku, itemType } = item;

      const clean = await processClean(req.file.buffer);
      if (!clean.image && itemType === 'jewelry') {
        // The detection is tuned for loose stones on the lightbox. A piece it
        // can't frame is kept as shot (upright, resized) for the reviewer.
        clean.image = await sharp(req.file.buffer)
          .rotate()
          .resize({ width: 2400, height: 2400, fit: 'inside', withoutEnlargement: true })
          .jpeg({ quality: 90 })
          .toBuffer();
        clean.quality = { ...clean.quality, issues: (clean.quality?.issues || []).filter((c) => c !== 'no_stone') };
      }
      if (!clean.image) {
        const ctx = req.teamCtx;
        putBlob(sku, 'failed', req.file.buffer)
          .then((url) => pool.query(
            `INSERT INTO stone_photo_captures (sku, item_type, status, original_url, quality, captured_by, captured_by_name)
             VALUES ($1, $2, 'failed', $3, $4, $5, $6)`,
            [sku, itemType, url, JSON.stringify(clean.quality), ctx.actorUserId, ctx.memberName || ctx.actorName || null]
          ))
          .catch((e) => console.error('photo-station failed-shot log error:', e.message));
        return res.status(422).json({ error: 'No stone found in the photo', quality: clean.quality });
      }
      const cutout = await processCutout(req.file.buffer, process.env.REMOVE_BG_API_KEY, clean.geometry)
        .catch((e) => ({ image: null, error: e.message }));

      const [originalUrl, cleanUrl, cutoutUrl] = await Promise.all([
        putBlob(sku, 'original', req.file.buffer),
        putBlob(sku, 'clean', clean.image),
        cutout.image ? putBlob(sku, 'cutout', cutout.image) : Promise.resolve(null),
      ]);

      const ctx = req.teamCtx;
      await pool.query(
        `UPDATE stone_photo_captures SET status = 'replaced' WHERE sku = $1 AND item_type = $2 AND status = 'pending'`,
        [sku, itemType]
      );
      const { rows } = await pool.query(
        `INSERT INTO stone_photo_captures
           (sku, item_type, status, original_url, clean_url, cutout_url, cutout_error, quality,
            captured_by, captured_by_name)
         VALUES ($1, $2, 'pending', $3, $4, $5, $6, $7, $8, $9)
         RETURNING *`,
        [sku, itemType, originalUrl, cleanUrl, cutoutUrl, cutout.error || null, JSON.stringify(clean.quality),
          ctx.actorUserId, ctx.memberName || ctx.actorName || null]
      );
      res.json(captureOut(rows[0]));
    } catch (e) {
      console.error('photo-station capture error:', e);
      res.status(500).json({ error: e.message });
    }
  });

  // The AI version is paid per photo (about $0.28), so it is made only when a
  // reviewer asks for it, in the background, and at most once per capture
  // unless the previous attempt failed.
  app.post('/api/photo-station/captures/:id/ai', ...reviewer, async (req, res) => {
    try {
      if (!ai.isConfigured()) return res.status(503).json({ error: 'The AI version is not configured' });
      const id = parseInt(req.params.id, 10);
      const claimed = await pool.query(
        `UPDATE stone_photo_captures SET ai_status = 'working', ai_error = NULL
          WHERE id = $1 AND status = 'pending' AND clean_url IS NOT NULL
            AND (ai_status IS NULL OR ai_status = 'failed')
          RETURNING *`,
        [id]
      );
      if (!claimed.rows.length) {
        const current = await pool.query(`SELECT * FROM stone_photo_captures WHERE id = $1`, [id]);
        if (!current.rows.length) return res.status(404).json({ error: 'Capture not found' });
        return res.json(captureOut(current.rows[0]));
      }
      const capture = claimed.rows[0];
      res.json(captureOut(capture));

      const read = async (url) => {
        const r = await fetch(url);
        if (!r.ok) throw new Error(`Could not read the stored photo (${r.status})`);
        return Buffer.from(await r.arrayBuffer());
      };
      ai.enqueue(async () => ai.processAi(await read(capture.original_url), await read(capture.clean_url)))
        .then((image) => putBlob(capture.sku, 'ai', image))
        .then((url) => pool.query(
          `UPDATE stone_photo_captures SET ai_url = $2, ai_status = 'ready', ai_error = NULL WHERE id = $1`,
          [id, url]
        ))
        .catch((e) => {
          console.error('photo-station AI edit error:', e.message);
          return pool.query(
            `UPDATE stone_photo_captures SET ai_status = 'failed', ai_error = $2 WHERE id = $1`,
            [id, String(e.message).slice(0, 300)]
          );
        })
        .catch((e) => console.error('photo-station AI status error:', e.message));
    } catch (e) {
      console.error('photo-station AI request error:', e);
      res.status(500).json({ error: e.message });
    }
  });

  app.get('/api/photo-station/captures', ...reviewer, async (req, res) => {
    try {
      // One status, or several separated by commas (approved,uploaded).
      const statuses = String(req.query.status || 'pending').split(',').map((s) => s.trim()).filter(Boolean);
      const limit = Math.min(200, Math.max(1, parseInt(req.query.limit, 10) || 60));
      const { rows } = await pool.query(
        `SELECT c.*, s.shape, s.weight, s.category, s.color, s.grouping_type, s.stones, s.location,
                j.title AS j_title, j.jewelry_type AS j_type, j.category AS j_category,
                j.metal_type AS j_metal, j.total_carat AS j_carat, j.location AS j_location
           FROM stone_photo_captures c
           LEFT JOIN soap_stones s ON c.item_type = 'stone' AND s.sku = c.sku
           LEFT JOIN jewelry_products j ON c.item_type = 'jewelry' AND j.model_number = c.sku
          WHERE c.status = ANY($1)
          ORDER BY ${statuses.includes('pending') ? 'c.captured_at ASC' : 'COALESCE(c.reviewed_at, c.captured_at) DESC'}
          LIMIT $2`,
        [statuses, limit]
      );
      res.json({
        items: rows.map((r) => ({
          ...captureOut(r),
          stone: r.item_type === 'jewelry'
            ? {
              title: r.j_title || null,
              category: r.j_type || r.j_category || null,
              metal: r.j_metal || null,
              weightCt: r.j_carat != null ? Number(r.j_carat) : null,
              location: r.j_location || null,
            }
            : {
              shape: r.shape || null,
              weightCt: r.weight != null ? Number(r.weight) : null,
              category: r.category || null,
              color: r.color || null,
              groupingType: r.grouping_type || null,
              stones: r.stones != null ? Number(r.stones) : null,
              location: r.location || null,
            },
        })),
      });
    } catch (e) {
      console.error('photo-station list error:', e);
      res.status(500).json({ error: e.message });
    }
  });

  app.post('/api/photo-station/captures/:id/approve', ...reviewer, async (req, res) => {
    const id = parseInt(req.params.id, 10);
    const variant = ['cutout', 'ai'].includes(req.body?.variant) ? req.body.variant : 'clean';
    const ctx = req.teamCtx;
    try {
      // One statement, so two managers can't approve the same shot twice.
      const approved = await pool.query(
        `UPDATE stone_photo_captures
            SET status = 'approved', chosen_variant = $2, dna_image_url = ${VARIANT_URL}, saved_at = NOW(),
                reviewed_by = $3, reviewed_by_name = $4, reviewed_at = NOW(), upload_error = NULL
          WHERE id = $1 AND status = 'pending' AND ${VARIANT_URL} IS NOT NULL
          RETURNING *`,
        [id, variant, ctx.actorUserId, ctx.memberName || ctx.actorName || null]
      );
      if (!approved.rows.length) {
        const current = await pool.query(`SELECT status FROM stone_photo_captures WHERE id = $1`, [id]);
        if (!current.rows.length) return res.status(404).json({ error: 'Capture not found' });
        if (current.rows[0].status !== 'pending') {
          return res.status(409).json({ error: 'This photo is no longer waiting for review' });
        }
        return res.status(400).json({ error: `There is no ${variant} version of this photo` });
      }
      let capture = approved.rows[0];
      await attachToItem(capture);
      if (req.body?.sendToBarak === true && capture.item_type === 'stone') {
        capture = (await sendToBarak(id)) || capture;
      }
      res.json(captureOut(capture));
    } catch (e) {
      console.error('photo-station approve error:', e);
      res.status(500).json({ error: e.message });
    }
  });

  app.post('/api/photo-station/captures/:id/send-to-barak', ...reviewer, async (req, res) => {
    try {
      const id = parseInt(req.params.id, 10);
      const sent = await sendToBarak(id);
      if (!sent) {
        const current = await pool.query(`SELECT * FROM stone_photo_captures WHERE id = $1`, [id]);
        if (!current.rows.length) return res.status(404).json({ error: 'Capture not found' });
        return res.status(409).json({ error: 'This photo cannot be sent to Barak', capture: captureOut(current.rows[0]) });
      }
      if (sent.barak_status === 'failed') {
        return res.status(502).json({ error: sent.upload_error, capture: captureOut(sent) });
      }
      res.json(captureOut(sent));
    } catch (e) {
      console.error('photo-station send-to-barak error:', e);
      res.status(500).json({ error: e.message });
    }
  });

  app.post('/api/photo-station/captures/:id/reject', ...reviewer, async (req, res) => {
    try {
      const id = parseInt(req.params.id, 10);
      const ctx = req.teamCtx;
      const reason = String(req.body?.reason || '').trim().slice(0, 300) || null;
      const { rows } = await pool.query(
        `UPDATE stone_photo_captures
            SET status = 'rejected', reject_reason = $2, reviewed_by = $3,
                reviewed_by_name = $4, reviewed_at = NOW()
          WHERE id = $1 AND status = 'pending'
          RETURNING *`,
        [id, reason, ctx.actorUserId, ctx.memberName || ctx.actorName || null]
      );
      if (!rows.length) return res.status(409).json({ error: 'This photo is no longer waiting for review' });
      res.json(captureOut(rows[0]));
    } catch (e) {
      console.error('photo-station reject error:', e);
      res.status(500).json({ error: e.message });
    }
  });
};
