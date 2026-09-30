// Turns a raw station photo of a stone into a catalog image, and says whether
// the photo is good enough to keep.
//
// The one rule every step here obeys: a pixel that belongs to the stone is
// never recoloured. For an emerald the colour is the product, so "cleaning"
// may only touch the background around it. The stone itself is moved,
// straightened and resized, and nothing else.
//
// Two ways to clean the background, run side by side while we find out which
// one holds up on real stones:
//
//   clean  - deterministic. Finds the lightbox background by flood-filling in
//            from the frame edge, then protects everything inside each
//            stone's outline, and fades only what is left to white.
//
//   cutout - remove.bg segmentation. Handles messy backgrounds better, but
//            translucent stones are exactly what segmentation models get
//            wrong, so it is offered next to `clean`, never instead of it.

const sharp = require('sharp');

// Working resolution. Phones upload at most this; anything larger is reduced.
const WORK_SIDE = 3000;
const OUT_SIDE = 1600;
// Share of the square frame's side the stone (or pair / parcel) fills.
const FILL = 0.72;
// Foreground pieces smaller than this share of the largest one are dust.
const MIN_COMPONENT_SHARE = 0.03;
// A stone whose outline spans fewer pixels than this ends up enlarged in the
// 1600px catalog image and looks soft, however well it was focused.
const MIN_STONE_PX = 600;
// Laplacian variance, measured on the stone at SHARPNESS_SIDE (never
// enlarged). Barak's own catalog shots score 78 and up, the same shots blurred
// by 3px score 53 and down. Revisit once real station photos exist.
const SHARPNESS_SIDE = 600;
const BLUR_THRESHOLD = 60;
// A bigger tilt is a deliberate pose (a pear shot on the diagonal), not a
// crooked stone. The station's holder keeps accidental tilt well under this.
const MAX_STRAIGHTEN = 10;

const WHITE = { r: 255, g: 255, b: 255 };

async function toWorking(buffer, rotateBy = 0, fill = WHITE) {
  let pipeline = sharp(buffer, { failOn: 'none' })
    .rotate()
    .resize({ width: WORK_SIDE, height: WORK_SIDE, fit: 'inside', withoutEnlargement: true });
  if (rotateBy) {
    const oriented = await pipeline.toBuffer();
    pipeline = sharp(oriented).rotate(-rotateBy, { background: fill });
  }
  const { data, info } = await pipeline
    .toColourspace('srgb')
    .removeAlpha()
    .raw()
    .toBuffer({ resolveWithObject: true });
  return { data, width: info.width, height: info.height };
}

const dist = (data, i, c) => {
  const dr = data[i] - c.r;
  const dg = data[i + 1] - c.g;
  const db = data[i + 2] - c.b;
  return Math.sqrt(dr * dr + dg * dg + db * db);
};

const median = (arr) => {
  const s = Float64Array.from(arr).sort();
  return s.length ? s[s.length >> 1] : 0;
};

function estimateBackground({ data, width, height }) {
  const band = Math.max(4, Math.round(Math.min(width, height) * 0.03));
  const rs = [], gs = [], bs = [];
  const step = Math.max(1, Math.round((width + height) / 1500));
  const take = (x, y) => {
    const i = (y * width + x) * 3;
    rs.push(data[i]); gs.push(data[i + 1]); bs.push(data[i + 2]);
  };
  for (let y = 0; y < height; y += step) {
    for (let x = 0; x < band; x += step) { take(x, y); take(width - 1 - x, y); }
  }
  for (let x = band; x < width - band; x += step) {
    for (let y = 0; y < band; y += step) { take(x, y); take(x, height - 1 - y); }
  }
  const color = { r: median(rs), g: median(gs), b: median(bs) };
  const ds = [];
  for (let k = 0; k < rs.length; k++) {
    const dr = rs[k] - color.r, dg = gs[k] - color.g, db = bs[k] - color.b;
    ds.push(Math.sqrt(dr * dr + dg * dg + db * db));
  }
  ds.sort((a, b) => a - b);
  const spread = ds.length ? ds[Math.floor(ds.length * 0.9)] : 0;
  const low = Math.max(20, spread * 1.5);
  return { color, spread, low, high: low * 1.8 };
}

// Everything reachable from the frame edge through pixels close to the
// background colour.
function floodBackground({ data, width, height }, bg) {
  const n = width * height;
  const isBg = new Uint8Array(n);
  const stack = new Int32Array(n);
  let sp = 0;
  const seed = (p) => {
    if (!isBg[p] && dist(data, p * 3, bg.color) < bg.high) { isBg[p] = 1; stack[sp++] = p; }
  };
  for (let x = 0; x < width; x++) { seed(x); seed((height - 1) * width + x); }
  for (let y = 0; y < height; y++) { seed(y * width); seed(y * width + width - 1); }
  while (sp > 0) {
    const p = stack[--sp];
    const x = p % width;
    if (x > 0) seed(p - 1);
    if (x < width - 1) seed(p + 1);
    if (p >= width) seed(p - width);
    if (p < n - width) seed(p + width);
  }
  return isBg;
}

// Labels 4-connected regions where mask[p] is set. Returns the kept regions
// (area >= MIN_COMPONENT_SHARE of the largest) and a per-pixel label map.
function components(mask, width, height) {
  const n = width * height;
  const label = new Int32Array(n);
  const stack = new Int32Array(n);
  const all = [];
  for (let start = 0; start < n; start++) {
    if (label[start] || !mask[start]) continue;
    const id = all.length + 1;
    let sp = 0;
    stack[sp++] = start;
    label[start] = id;
    let area = 0, x0 = width, y0 = height, x1 = -1, y1 = -1;
    while (sp > 0) {
      const p = stack[--sp];
      const x = p % width, y = (p - x) / width;
      area++;
      if (x < x0) x0 = x; if (x > x1) x1 = x;
      if (y < y0) y0 = y; if (y > y1) y1 = y;
      if (x > 0 && !label[p - 1] && mask[p - 1]) { label[p - 1] = id; stack[sp++] = p - 1; }
      if (x < width - 1 && !label[p + 1] && mask[p + 1]) { label[p + 1] = id; stack[sp++] = p + 1; }
      if (y > 0 && !label[p - width] && mask[p - width]) { label[p - width] = id; stack[sp++] = p - width; }
      if (y < height - 1 && !label[p + width] && mask[p + width]) { label[p + width] = id; stack[sp++] = p + width; }
    }
    all.push({ id, area, x0, y0, x1, y1 });
  }
  if (!all.length) return { kept: [], label };
  const largest = Math.max(...all.map((c) => c.area));
  return { kept: all.filter((c) => c.area >= largest * MIN_COMPONENT_SHARE), label };
}

// Binary dilation / erosion with a (2r+1) square, separable, via prefix sums.
function morph(mask, width, height, r, erode) {
  const tmp = new Uint8Array(width * height);
  const out = new Uint8Array(width * height);
  const full = 2 * r + 1;
  const row = new Int32Array(width + 1);
  for (let y = 0; y < height; y++) {
    const o = y * width;
    for (let x = 0; x < width; x++) row[x + 1] = row[x] + mask[o + x];
    for (let x = 0; x < width; x++) {
      const a = Math.max(0, x - r), b = Math.min(width, x + r + 1);
      const s = row[b] - row[a];
      tmp[o + x] = erode ? (s === full ? 1 : 0) : (s > 0 ? 1 : 0);
    }
  }
  const col = new Int32Array(height + 1);
  for (let x = 0; x < width; x++) {
    for (let y = 0; y < height; y++) col[y + 1] = col[y] + tmp[y * width + x];
    for (let y = 0; y < height; y++) {
      const a = Math.max(0, y - r), b = Math.min(height, y + r + 1);
      const s = col[b] - col[a];
      out[y * width + x] = erode ? (s === full ? 1 : 0) : (s > 0 ? 1 : 0);
    }
  }
  return out;
}

const cross = (o, a, b) => (a[0] - o[0]) * (b[1] - o[1]) - (a[1] - o[1]) * (b[0] - o[0]);

// Convex hull (monotone chain) of the region's row extremes, rasterised into
// `into`. Returns the hull's pixel moments for the straightening step.
function fillHull(label, id, box, width, into) {
  const pts = [];
  for (let y = box.y0; y <= box.y1; y++) {
    let lo = -1, hi = -1;
    const o = y * width;
    for (let x = box.x0; x <= box.x1; x++) {
      if (label[o + x] === id) { if (lo < 0) lo = x; hi = x; }
    }
    if (lo >= 0) { pts.push([lo, y]); if (hi !== lo) pts.push([hi, y]); }
  }
  pts.sort((a, b) => a[0] - b[0] || a[1] - b[1]);
  const lower = [], upper = [];
  for (const p of pts) {
    while (lower.length >= 2 && cross(lower[lower.length - 2], lower[lower.length - 1], p) <= 0) lower.pop();
    lower.push(p);
  }
  for (let i = pts.length - 1; i >= 0; i--) {
    const p = pts[i];
    while (upper.length >= 2 && cross(upper[upper.length - 2], upper[upper.length - 1], p) <= 0) upper.pop();
    upper.push(p);
  }
  const hull = lower.slice(0, -1).concat(upper.slice(0, -1));

  const m = { area: 0, sx: 0, sy: 0, sxx: 0, syy: 0, sxy: 0 };
  if (hull.length < 3) return m;
  for (let y = box.y0; y <= box.y1; y++) {
    let xmin = Infinity, xmax = -Infinity;
    for (let i = 0; i < hull.length; i++) {
      const [ax, ay] = hull[i];
      const [bx, by] = hull[(i + 1) % hull.length];
      if ((ay <= y && by >= y) || (by <= y && ay >= y)) {
        if (ay === by) { xmin = Math.min(xmin, ax, bx); xmax = Math.max(xmax, ax, bx); continue; }
        const x = ax + ((y - ay) * (bx - ax)) / (by - ay);
        xmin = Math.min(xmin, x); xmax = Math.max(xmax, x);
      }
    }
    if (xmin > xmax) continue;
    const o = y * width;
    for (let x = Math.ceil(xmin); x <= Math.floor(xmax); x++) {
      into[o + x] = 1;
      m.area++; m.sx += x; m.sy += y; m.sxx += x * x; m.syy += y * y; m.sxy += x * y;
    }
  }
  return m;
}

// Finds the stones. Background detection alone isn't enough: on a pale stone
// the bright facets are the same colour as the lightbox, the flood leaks in
// through them, and the stone comes apart into fragments. Closing the
// fragments back together and taking each group's convex outline restores
// the whole stone, and everything inside that outline is left untouched.
function analyse(img, bg) {
  const { width, height } = img;
  const isBg = floodBackground(img, bg);
  const fg = new Uint8Array(width * height);
  for (let p = 0; p < fg.length; p++) fg[p] = isBg[p] ? 0 : 1;

  const pieces = components(fg, width, height);
  if (!pieces.kept.length) return { isBg, protect: new Uint8Array(width * height), groups: [] };
  const keptIds = new Set(pieces.kept.map((c) => c.id));
  const solid = new Uint8Array(width * height);
  for (let p = 0; p < solid.length; p++) if (keptIds.has(pieces.label[p])) solid[p] = 1;

  const r = Math.max(3, Math.round(Math.min(width, height) * 0.015));
  const closed = morph(morph(solid, width, height, r, false), width, height, r, true);
  const grouped = components(closed, width, height);

  const protect = new Uint8Array(width * height);
  const groups = grouped.kept.map((g) => ({ ...g, moments: fillHull(grouped.label, g.id, g, width, protect) }));
  return { isBg, protect, groups };
}

const unionBox = (groups) => groups.reduce(
  (b, c) => ({
    x0: Math.min(b.x0, c.x0), y0: Math.min(b.y0, c.y0),
    x1: Math.max(b.x1, c.x1), y1: Math.max(b.y1, c.y1),
  }),
  { x0: Infinity, y0: Infinity, x1: -Infinity, y1: -Infinity }
);

// How far a single elongated stone sits off the nearest axis, in degrees.
// Round stones have no meaningful axis and pairs/parcels are laid out by
// hand, so both are left alone.
function skewDegrees(groups) {
  if (groups.length !== 1) return 0;
  const m = groups[0].moments;
  if (!m.area) return 0;
  const mx = m.sx / m.area, my = m.sy / m.area;
  const mu20 = m.sxx / m.area - mx * mx;
  const mu02 = m.syy / m.area - my * my;
  const mu11 = m.sxy / m.area - mx * my;
  const common = Math.sqrt(((mu20 - mu02) / 2) ** 2 + mu11 ** 2);
  const l1 = (mu20 + mu02) / 2 + common;
  const l2 = (mu20 + mu02) / 2 - common;
  if (l2 <= 0 || Math.sqrt(l1 / l2) < 1.12) return 0;
  const theta = (0.5 * Math.atan2(2 * mu11, mu20 - mu02) * 180) / Math.PI;
  let d = ((theta % 90) + 90) % 90;
  if (d > 45) d -= 90;
  return Math.abs(d) >= 1.5 && Math.abs(d) <= MAX_STRAIGHTEN ? d : 0;
}

async function sharpnessOf(img, box) {
  const width = box.x1 - box.x0 + 1;
  const height = box.y1 - box.y0 + 1;
  if (width < 8 || height < 8) return 0;
  const { data, info } = await sharp(img.data, { raw: { width: img.width, height: img.height, channels: 3 } })
    .extract({ left: box.x0, top: box.y0, width, height })
    .resize({ width: SHARPNESS_SIDE, height: SHARPNESS_SIDE, fit: 'inside', withoutEnlargement: true })
    .greyscale()
    .raw()
    .toBuffer({ resolveWithObject: true });
  const w = info.width, h = info.height;
  let sum = 0, sumSq = 0, count = 0;
  for (let y = 1; y < h - 1; y++) {
    for (let x = 1; x < w - 1; x++) {
      const p = y * w + x;
      const v = data[p - 1] + data[p + 1] + data[p - w] + data[p + w] - 4 * data[p];
      sum += v; sumSq += v * v; count++;
    }
  }
  if (!count) return 0;
  const mean = sum / count;
  return sumSq / count - mean * mean;
}

function exposureOf({ data }, protect) {
  let total = 0, clipped = 0, lumSum = 0;
  for (let p = 0; p < protect.length; p++) {
    if (!protect[p]) continue;
    const i = p * 3;
    const l = 0.299 * data[i] + 0.587 * data[i + 1] + 0.114 * data[i + 2];
    lumSum += l;
    if (l >= 250) clipped++;
    total++;
  }
  return {
    meanLum: total ? lumSum / total : 0,
    clippedShare: total ? clipped / total : 0,
  };
}

async function assessQuality(img, bg, { groups, protect }) {
  const issues = [];
  const imgArea = img.width * img.height;
  const stoneArea = groups.reduce((s, g) => s + g.moments.area, 0);
  if (!groups.length || stoneArea < imgArea * 0.002) {
    return { ok: false, issues: ['no_stone'], metrics: { backgroundSpread: Math.round(bg.spread) } };
  }
  const box = unionBox(groups);
  const touches = box.x0 <= 1 || box.y0 <= 1 || box.x1 >= img.width - 2 || box.y1 >= img.height - 2;
  if (touches) issues.push('stone_cut_off');
  const stonePx = Math.max(box.x1 - box.x0, box.y1 - box.y0) + 1;
  if (stonePx < MIN_STONE_PX) issues.push('stone_too_small');

  const sharpness = await sharpnessOf(img, box);
  if (sharpness < BLUR_THRESHOLD && stonePx >= MIN_STONE_PX) issues.push('blurry');

  const { meanLum, clippedShare } = exposureOf(img, protect);
  if (meanLum < 40) issues.push('too_dark');
  if (clippedShare > 0.25) issues.push('too_bright');

  const bgLum = 0.299 * bg.color.r + 0.587 * bg.color.g + 0.114 * bg.color.b;
  if (bgLum < 150) issues.push('dark_background');
  if (bg.spread > 45) issues.push('uneven_background');

  return {
    ok: issues.length === 0,
    issues,
    metrics: {
      sharpness: Math.round(sharpness),
      stonePx,
      stoneLuminance: Math.round(meanLum),
      clippedShare: Math.round(clippedShare * 1000) / 1000,
      backgroundLuminance: Math.round(bgLum),
      backgroundSpread: Math.round(bg.spread),
      objects: groups.length,
    },
  };
}

// Only pixels that are background AND outside every stone outline are faded.
function whitenBackground(img, isBg, protect, bg) {
  const out = Buffer.from(img.data);
  for (let p = 0; p < isBg.length; p++) {
    if (!isBg[p] || protect[p]) continue;
    const i = p * 3;
    const d = dist(img.data, i, bg.color);
    const w = d <= bg.low ? 1 : Math.max(0, (bg.high - d) / (bg.high - bg.low));
    out[i] = Math.round(img.data[i] * (1 - w) + 255 * w);
    out[i + 1] = Math.round(img.data[i + 1] * (1 - w) + 255 * w);
    out[i + 2] = Math.round(img.data[i + 2] * (1 - w) + 255 * w);
  }
  return { data: out, width: img.width, height: img.height };
}

// Paints `img` onto a white square centred on `box`, sized so the box fills
// FILL of it, then scales to the output size.
async function frame(img, box) {
  const bw = box.x1 - box.x0 + 1;
  const bh = box.y1 - box.y0 + 1;
  const side = Math.max(16, Math.round(Math.max(bw, bh) / FILL));
  const cx = (box.x0 + box.x1) / 2;
  const cy = (box.y0 + box.y1) / 2;
  const left = Math.round(cx - side / 2);
  const top = Math.round(cy - side / 2);
  const out = Buffer.alloc(side * side * 3, 255);
  for (let y = 0; y < side; y++) {
    const sy = top + y;
    if (sy < 0 || sy >= img.height) continue;
    const sx0 = Math.max(0, left);
    const sx1 = Math.min(img.width, left + side);
    if (sx1 <= sx0) continue;
    img.data.copy(out, (y * side + (sx0 - left)) * 3, (sy * img.width + sx0) * 3, (sy * img.width + sx1) * 3);
  }
  return sharp(out, { raw: { width: side, height: side, channels: 3 } })
    .resize(OUT_SIDE, OUT_SIDE, { fit: 'fill', kernel: 'lanczos3' })
    .jpeg({ quality: 92, chromaSubsampling: '4:4:4', mozjpeg: true })
    .toBuffer();
}

// Deterministic path. Returns the framed JPEG plus the quality verdict.
async function processClean(buffer) {
  let img = await toWorking(buffer);
  // Estimated once, on the photo as shot. After straightening, the rotated-in
  // corners are a flat fill and would make the backdrop look perfectly even.
  const bg = estimateBackground(img);
  let a = analyse(img, bg);
  const skew = skewDegrees(a.groups);
  if (skew) {
    img = await toWorking(buffer, skew, bg.color);
    a = analyse(img, bg);
  }
  const quality = await assessQuality(img, bg, a);
  quality.metrics = { ...quality.metrics, straightenedBy: Math.round(skew * 10) / 10 };
  if (!a.groups.length) return { image: null, quality };
  const cleaned = whitenBackground(img, a.isBg, a.protect, bg);
  const image = await frame(cleaned, unionBox(a.groups));
  return { image, quality };
}

// remove.bg path. The stone's pixels come back from the service untouched;
// only its alpha mask is used, and we composite onto white ourselves.
async function processCutout(buffer, apiKey) {
  if (!apiKey) return { image: null, error: 'REMOVE_BG_API_KEY is not configured' };
  const input = await sharp(buffer, { failOn: 'none' })
    .rotate()
    .resize({ width: WORK_SIDE, height: WORK_SIDE, fit: 'inside', withoutEnlargement: true })
    .jpeg({ quality: 95 })
    .toBuffer();
  const form = new FormData();
  form.append('image_file', new Blob([input], { type: 'image/jpeg' }), 'stone.jpg');
  form.append('size', 'auto');
  form.append('type', 'product');
  form.append('format', 'png');
  const res = await fetch('https://api.remove.bg/v1.0/removebg', {
    method: 'POST',
    headers: { 'X-Api-Key': apiKey },
    body: form,
  });
  if (!res.ok) {
    const text = await res.text().catch(() => '');
    return { image: null, error: `remove.bg ${res.status}: ${text.slice(0, 200)}` };
  }
  const png = Buffer.from(await res.arrayBuffer());
  const { data: alpha, info } = await sharp(png).ensureAlpha().extractChannel(3).raw()
    .toBuffer({ resolveWithObject: true });
  const mask = new Uint8Array(alpha.length);
  for (let p = 0; p < alpha.length; p++) mask[p] = alpha[p] > 16 ? 1 : 0;
  const { kept } = components(mask, info.width, info.height);
  if (!kept.length) return { image: null, error: 'remove.bg found no object' };
  const { data, info: flatInfo } = await sharp(png).flatten({ background: WHITE }).removeAlpha().raw()
    .toBuffer({ resolveWithObject: true });
  const image = await frame({ data, width: flatInfo.width, height: flatInfo.height }, unionBox(kept));
  return { image, error: null };
}

module.exports = { processClean, processCutout };
