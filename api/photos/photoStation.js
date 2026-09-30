// Photo Station: office staff photograph stones that have no picture, a
// manager approves each shot, and the approved image is uploaded to Barak's
// picture FTP (see utils/barakPictures.js for the naming contract).
//
//   GET  /api/photo-station/status              what is configured
//   GET  /api/photo-station/queue               stones still needing a photo, most valuable first
//   GET  /api/photo-station/stone/:sku          one stone, for a scanned barcode
//   POST /api/photo-station/captures            upload + process a shot (multipart: photo, sku)
//   GET  /api/photo-station/captures            review list (manager)
//   POST /api/photo-station/captures/:id/approve   upload the chosen version to Barak (manager)
//   POST /api/photo-station/captures/:id/reject    send the stone back to the queue (manager)
//
// Capture lifecycle: pending -> uploading -> uploaded
//                    pending -> rejected
//                    pending -> replaced   (the same stone was shot again)
// A failed FTP upload goes back to pending with upload_error set, so it can
// simply be approved again.

const multer = require('multer');
const { processClean, processCutout } = require('../../utils/stonePhoto');
const barak = require('../../utils/barakPictures');

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

const canShoot = (ctx) =>
  !!ctx && !ctx.isStoreUser && (ctx.isOwner || ctx.role === 'manager' ||
    (ctx.permissions?.sections || []).includes('photos'));
const canReview = (ctx) => !!ctx && !ctx.isStoreUser && (ctx.isOwner || ctx.role === 'manager');

const stoneSummary = (row, onMemo) => ({
  sku: row.sku,
  shape: row.shape || null,
  weightCt: row.weight != null ? Number(row.weight) : null,
  category: row.category || null,
  color: row.color || null,
  groupingType: row.grouping_type || null,
  stones: row.stones != null ? Number(row.stones) : null,
  branch: row.branch || null,
  onMemo,
  hasPhoto: hasPhoto(row),
});

const captureOut = (c) => ({
  id: c.id,
  sku: c.sku,
  status: c.status,
  originalUrl: c.original_url,
  cleanUrl: c.clean_url,
  cutoutUrl: c.cutout_url,
  cutoutError: c.cutout_error,
  chosenVariant: c.chosen_variant,
  quality: c.quality,
  capturedBy: c.captured_by_name,
  capturedAt: c.captured_at,
  reviewedBy: c.reviewed_by_name,
  reviewedAt: c.reviewed_at,
  rejectReason: c.reject_reason,
  ftpFilename: c.ftp_filename,
  uploadedAt: c.uploaded_at,
  uploadError: c.upload_error,
  filenameWarning: barak.skuFilenameWarning(c.sku),
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

  app.get('/api/photo-station/status', ...shooter, async (req, res) => {
    const out = {
      canReview: canReview(req.teamCtx),
      storage: !!(blobPut && process.env.BLOB_READ_WRITE_TOKEN),
      cutout: !!process.env.REMOVE_BG_API_KEY,
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
      const { rows } = await pool.query(
        `SELECT s.sku, s.shape, s.weight, s.category, s.color, s.grouping_type, s.stones,
                s.branch, s.location, s.total_price, s.image, s.additional_pictures
           FROM soap_stones s
          WHERE s.sku IS NOT NULL
            AND ($1 = '' OR s.branch = $1)
            AND ($2 = '' OR LOWER(s.category) LIKE '%' || $2 || '%')
            AND NOT EXISTS (
                  SELECT 1 FROM stone_photo_captures c
                   WHERE c.sku = s.sku AND c.status IN ('pending', 'uploading', 'uploaded'))`,
        [branch === 'all' ? '' : branch, category === 'all' ? '' : category]
      );
      const waiting = rows
        .filter((r) => !hasPhoto(r) && !computeOnMemo(r.location))
        .sort((a, b) => (Number(b.total_price) || 0) - (Number(a.total_price) || 0));
      const counts = await pool.query(
        `SELECT status, COUNT(*)::int AS n FROM stone_photo_captures
          WHERE status IN ('pending', 'uploaded') GROUP BY status`
      );
      const mine = await pool.query(
        `SELECT COUNT(*)::int AS n FROM stone_photo_captures
          WHERE captured_by = $1 AND captured_at >= date_trunc('day', NOW())
            AND status <> 'replaced'`,
        [req.teamCtx.actorUserId]
      );
      const byStatus = Object.fromEntries(counts.rows.map((r) => [r.status, r.n]));
      res.json({
        remaining: waiting.length,
        awaitingReview: byStatus.pending || 0,
        uploaded: byStatus.uploaded || 0,
        mineToday: mine.rows[0]?.n || 0,
        items: waiting.slice(0, limit).map((r, i) => ({ rank: i + 1, ...stoneSummary(r, false) })),
      });
    } catch (e) {
      console.error('photo-station queue error:', e);
      res.status(500).json({ error: e.message });
    }
  });

  app.get('/api/photo-station/stone/:sku', ...shooter, async (req, res) => {
    try {
      const code = String(req.params.sku || '').trim();
      const { rows } = await pool.query(
        `SELECT sku, shape, weight, category, color, grouping_type, stones, branch, location,
                image, additional_pictures
           FROM soap_stones WHERE UPPER(sku) = UPPER($1) LIMIT 1`,
        [code]
      );
      if (!rows.length) return res.status(404).json({ error: 'Stone not found' });
      const row = rows[0];
      const last = await pool.query(
        `SELECT * FROM stone_photo_captures
          WHERE sku = $1 AND status <> 'replaced'
          ORDER BY captured_at DESC LIMIT 1`,
        [row.sku]
      );
      res.json({
        ...stoneSummary(row, computeOnMemo(row.location)),
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
      const code = String(req.body?.sku || '').trim();
      const found = await pool.query(`SELECT sku FROM soap_stones WHERE UPPER(sku) = UPPER($1) LIMIT 1`, [code]);
      if (!found.rows.length) return res.status(404).json({ error: 'Stone not found' });
      const sku = found.rows[0].sku;

      const clean = await processClean(req.file.buffer);
      if (!clean.image) {
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
        `UPDATE stone_photo_captures SET status = 'replaced' WHERE sku = $1 AND status = 'pending'`,
        [sku]
      );
      const { rows } = await pool.query(
        `INSERT INTO stone_photo_captures
           (sku, status, original_url, clean_url, cutout_url, cutout_error, quality,
            captured_by, captured_by_name)
         VALUES ($1, 'pending', $2, $3, $4, $5, $6, $7, $8)
         RETURNING *`,
        [sku, originalUrl, cleanUrl, cutoutUrl, cutout.error || null, JSON.stringify(clean.quality),
          ctx.actorUserId, ctx.memberName || ctx.actorName || null]
      );
      res.json(captureOut(rows[0]));
    } catch (e) {
      console.error('photo-station capture error:', e);
      res.status(500).json({ error: e.message });
    }
  });

  app.get('/api/photo-station/captures', ...reviewer, async (req, res) => {
    try {
      const status = String(req.query.status || 'pending');
      const limit = Math.min(200, Math.max(1, parseInt(req.query.limit, 10) || 60));
      const { rows } = await pool.query(
        `SELECT c.*, s.shape, s.weight, s.category, s.color, s.grouping_type, s.stones
           FROM stone_photo_captures c
           LEFT JOIN soap_stones s ON s.sku = c.sku
          WHERE c.status = $1
          ORDER BY ${status === 'pending' ? 'c.captured_at ASC' : 'COALESCE(c.reviewed_at, c.captured_at) DESC'}
          LIMIT $2`,
        [status, limit]
      );
      res.json({
        items: rows.map((r) => ({
          ...captureOut(r),
          stone: {
            shape: r.shape || null,
            weightCt: r.weight != null ? Number(r.weight) : null,
            category: r.category || null,
            color: r.color || null,
            groupingType: r.grouping_type || null,
            stones: r.stones != null ? Number(r.stones) : null,
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
    const variant = req.body?.variant === 'cutout' ? 'cutout' : 'clean';
    const ctx = req.teamCtx;
    try {
      if (!barak.isConfigured()) {
        return res.status(503).json({ error: 'Barak picture FTP is not configured' });
      }
      // Claim the row first so two managers can't upload the same shot twice.
      const claimed = await pool.query(
        `UPDATE stone_photo_captures
            SET status = 'uploading', chosen_variant = $2, reviewed_by = $3,
                reviewed_by_name = $4, reviewed_at = NOW(), upload_error = NULL
          WHERE id = $1 AND status = 'pending'
          RETURNING *`,
        [id, variant, ctx.actorUserId, ctx.memberName || ctx.actorName || null]
      );
      if (!claimed.rows.length) return res.status(409).json({ error: 'This photo is no longer waiting for review' });
      const capture = claimed.rows[0];
      const url = variant === 'cutout' ? capture.cutout_url : capture.clean_url;
      try {
        if (!url) throw new Error(`There is no ${variant} version of this photo`);
        const imgRes = await fetch(url);
        if (!imgRes.ok) throw new Error(`Could not read the stored photo (${imgRes.status})`);
        const buffer = Buffer.from(await imgRes.arrayBuffer());
        const filename = await barak.uploadStonePhoto(capture.sku, buffer);
        const done = await pool.query(
          `UPDATE stone_photo_captures
              SET status = 'uploaded', ftp_filename = $2, uploaded_at = NOW()
            WHERE id = $1 RETURNING *`,
          [id, filename]
        );
        res.json(captureOut(done.rows[0]));
      } catch (e) {
        const back = await pool.query(
          `UPDATE stone_photo_captures SET status = 'pending', upload_error = $2 WHERE id = $1 RETURNING *`,
          [id, e.message]
        );
        res.status(502).json({ error: e.message, capture: captureOut(back.rows[0]) });
      }
    } catch (e) {
      console.error('photo-station approve error:', e);
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
