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
//   clean  - deterministic. Models the backdrop as lit in this very photo,
//            finds each stone's outline against it (shadow and dust are
//            backdrop), copies the stone's pixels untouched onto pure white
//            and softens only the outermost edge pixel or two. A pale stone
//            that can't be outlined reliably keeps a generous outline instead.
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

// Everything not reachable from the frame edge through unset pixels is
// inside an object, so a pale facet surrounded by stone stays stone.
function fillHoles(mask, width, height) {
  const n = width * height;
  const outside = new Uint8Array(n);
  const stack = new Int32Array(n);
  let sp = 0;
  const seed = (p) => { if (!mask[p] && !outside[p]) { outside[p] = 1; stack[sp++] = p; } };
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
  const out = new Uint8Array(n);
  for (let p = 0; p < n; p++) out[p] = outside[p] ? 0 : 1;
  return out;
}

// Fills unknown cells from their known neighbourhood, coarse to fine, so a
// hole the size of the stone gets a smooth estimate of what the backdrop
// would look like there.
function pushPull(v, known, w, h) {
  if (w <= 2 || h <= 2) {
    const mean = [0, 0, 0];
    let k = 0;
    for (let c = 0; c < w * h; c++) if (known[c]) { mean[0] += v[c * 3]; mean[1] += v[c * 3 + 1]; mean[2] += v[c * 3 + 2]; k++; }
    for (let c = 0; c < w * h; c++) {
      if (known[c]) continue;
      for (let ch = 0; ch < 3; ch++) v[c * 3 + ch] = k ? mean[ch] / k : 200;
    }
    return;
  }
  const cw = Math.ceil(w / 2), chh = Math.ceil(h / 2);
  const cv = new Float64Array(cw * chh * 3);
  const cn = new Float64Array(cw * chh);
  for (let y = 0; y < h; y++) {
    for (let x = 0; x < w; x++) {
      const c = y * w + x;
      if (!known[c]) continue;
      const cc = (y >> 1) * cw + (x >> 1);
      cv[cc * 3] += v[c * 3]; cv[cc * 3 + 1] += v[c * 3 + 1]; cv[cc * 3 + 2] += v[c * 3 + 2];
      cn[cc]++;
    }
  }
  const ck = new Uint8Array(cw * chh);
  for (let cc = 0; cc < cw * chh; cc++) {
    if (!cn[cc]) continue;
    ck[cc] = 1;
    cv[cc * 3] /= cn[cc]; cv[cc * 3 + 1] /= cn[cc]; cv[cc * 3 + 2] /= cn[cc];
  }
  pushPull(cv, ck, cw, chh);
  for (let y = 0; y < h; y++) {
    for (let x = 0; x < w; x++) {
      const c = y * w + x;
      if (known[c]) continue;
      const cc = (y >> 1) * cw + (x >> 1);
      v[c * 3] = cv[cc * 3]; v[c * 3 + 1] = cv[cc * 3 + 1]; v[c * 3 + 2] = cv[cc * 3 + 2];
    }
  }
  for (let it = 0; it < 6; it++) {
    for (let y = 0; y < h; y++) {
      for (let x = 0; x < w; x++) {
        const c = y * w + x;
        if (known[c]) continue;
        let n = 0;
        const s = [0, 0, 0];
        const add = (q) => { s[0] += v[q * 3]; s[1] += v[q * 3 + 1]; s[2] += v[q * 3 + 2]; n++; };
        if (x > 0) add(c - 1);
        if (x < w - 1) add(c + 1);
        if (y > 0) add(c - w);
        if (y < h - 1) add(c + w);
        v[c * 3] = s[0] / n; v[c * 3 + 1] = s[1] / n; v[c * 3 + 2] = s[2] / n;
      }
    }
  }
}

// What the backdrop looks like at every pixel, lit exactly as it was. Phone
// photos of a lightbox are never evenly lit: corners fall off, the stone
// casts a shadow, a lamp leaves a gradient. Judging each pixel against its
// own local backdrop, instead of one colour for the whole frame, is what lets
// a faint shadow be told apart from a dark facet.
const SURFACE_SIDE = 160;
async function backgroundSurface(img, exclude) {
  const { data, width, height } = img;
  const scale = SURFACE_SIDE / Math.max(width, height);
  const lw = Math.max(4, Math.round(width * scale));
  const lh = Math.max(4, Math.round(height * scale));
  const sum = new Float64Array(lw * lh * 3);
  const cnt = new Float64Array(lw * lh);
  const all = new Float64Array(lw * lh);
  for (let y = 0; y < height; y++) {
    const cy = Math.min(lh - 1, Math.floor((y * lh) / height));
    for (let x = 0; x < width; x++) {
      const c = cy * lw + Math.min(lw - 1, Math.floor((x * lw) / width));
      all[c]++;
      const p = y * width + x;
      if (exclude[p]) continue;
      const i = p * 3;
      sum[c * 3] += data[i]; sum[c * 3 + 1] += data[i + 1]; sum[c * 3 + 2] += data[i + 2];
      cnt[c]++;
    }
  }
  const known = new Uint8Array(lw * lh);
  for (let c = 0; c < lw * lh; c++) {
    if (cnt[c] < all[c] * 0.5) continue;
    known[c] = 1;
    sum[c * 3] /= cnt[c]; sum[c * 3 + 1] /= cnt[c]; sum[c * 3 + 2] /= cnt[c];
  }
  pushPull(sum, known, lw, lh);
  const low = Buffer.alloc(lw * lh * 3);
  for (let k = 0; k < low.length; k++) low[k] = Math.max(0, Math.min(255, Math.round(sum[k])));
  return sharp(low, { raw: { width: lw, height: lh, channels: 3 } })
    .blur(1)
    .resize(width, height, { fit: 'fill', kernel: 'cubic' })
    .raw()
    .toBuffer();
}

// Mean of `v` over a (2r+1) square, separable running sums.
function boxMean(v, width, height, r) {
  const tmp = new Float32Array(v.length);
  const out = new Float32Array(v.length);
  for (let y = 0; y < height; y++) {
    const o = y * width;
    let s = 0;
    for (let x = -r; x <= r; x++) s += v[o + Math.min(width - 1, Math.max(0, x))];
    for (let x = 0; x < width; x++) {
      tmp[o + x] = s / (2 * r + 1);
      s += v[o + Math.min(width - 1, x + r + 1)] - v[o + Math.max(0, x - r)];
    }
  }
  for (let x = 0; x < width; x++) {
    let s = 0;
    for (let y = -r; y <= r; y++) s += tmp[Math.min(height - 1, Math.max(0, y)) * width + x];
    for (let y = 0; y < height; y++) {
      out[y * width + x] = s / (2 * r + 1);
      s += tmp[Math.min(height - 1, y + r + 1) * width + x] - tmp[Math.max(0, y - r) * width + x];
    }
  }
  return out;
}

// Relative to its local backdrop, a pixel is backdrop when it has the
// backdrop's colour at any brightness: lit (glare, caustics that picked up
// no colour) or dimmed (the stone's shadow, a vignette). A shadow is also
// smooth, which keeps the grey facets of a colourless stone, crisp and
// high-contrast, from being taken for one. Light through a coloured stone
// tints its own shadow, measured at up to 0.15 under an emerald, while the
// emerald itself sits at 0.5 and up.
const NEUTRAL_TINT = 0.12;
const SHADOW_TINT = 0.25;
const SHADOW_DEPTH = 0.35;
const SHADOW_TEXTURE = 0.08;
function classify(img, surface) {
  const { data, width, height } = img;
  const n = width * height;
  const ratio = new Float32Array(n);
  const tint = new Float32Array(n);
  for (let p = 0; p < n; p++) {
    const i = p * 3;
    const rr = (data[i] + 2) / (surface[i] + 2);
    const rg = (data[i + 1] + 2) / (surface[i + 1] + 2);
    const rb = (data[i + 2] + 2) / (surface[i + 2] + 2);
    const m = (rr + rg + rb) / 3;
    ratio[p] = m;
    tint[p] = Math.max(Math.abs(rr - m), Math.abs(rg - m), Math.abs(rb - m)) / Math.max(m, 0.05);
  }
  const mean = boxMean(ratio, width, height, 3);
  const sq = new Float32Array(n);
  for (let p = 0; p < n; p++) sq[p] = ratio[p] * ratio[p];
  const meanSq = boxMean(sq, width, height, 3);
  const fg = new Uint8Array(n);
  for (let p = 0; p < n; p++) {
    const m = ratio[p];
    if (m > 0.88 && tint[p] < NEUTRAL_TINT) continue;
    if (m > SHADOW_DEPTH && tint[p] < SHADOW_TINT) {
      const texture = Math.sqrt(Math.max(0, meanSq[p] - mean[p] * mean[p]));
      if (texture < SHADOW_TEXTURE) continue;
    }
    fg[p] = 1;
  }
  return fg;
}

function momentsOf(label, id, box, width) {
  const m = { area: 0, sx: 0, sy: 0, sxx: 0, syy: 0, sxy: 0 };
  for (let y = box.y0; y <= box.y1; y++) {
    const o = y * width;
    for (let x = box.x0; x <= box.x1; x++) {
      if (label[o + x] !== id) continue;
      m.area++; m.sx += x; m.sy += y; m.sxx += x * x; m.syy += y * y; m.sxy += x * y;
    }
  }
  return m;
}

const touchesFrame = (c, width, height) => c.x0 <= 1 || c.y0 <= 1 || c.x1 >= width - 2 || c.y1 >= height - 2;

// Turns a raw foreground map into whole stones: drops dust, bridges hairline
// gaps, fills each outline. When some objects sit clear of the frame edge,
// ones touching it are the table edge or a wall, not the stone.
function stonesFrom(fg, width, height) {
  const pieces = components(fg, width, height);
  let kept = pieces.kept;
  const inner = kept.filter((c) => !touchesFrame(c, width, height));
  if (inner.length && inner.length < kept.length) {
    const largest = Math.max(...inner.map((c) => c.area));
    kept = inner.filter((c) => c.area >= largest * MIN_COMPONENT_SHARE);
  }
  const solid = new Uint8Array(width * height);
  if (!kept.length) return { mask: solid, groups: [] };
  const keptIds = new Set(kept.map((c) => c.id));
  for (let p = 0; p < solid.length; p++) if (keptIds.has(pieces.label[p])) solid[p] = 1;

  const r = Math.max(2, Math.round(Math.min(width, height) * 0.004));
  const closed = morph(morph(solid, width, height, r, false), width, height, r, true);
  const mask = fillHoles(closed, width, height);
  const grouped = components(mask, width, height);
  const ids = new Set(grouped.kept.map((g) => g.id));
  for (let p = 0; p < mask.length; p++) if (mask[p] && !ids.has(grouped.label[p])) mask[p] = 0;
  const groups = grouped.kept.map((g) => ({ ...g, moments: momentsOf(grouped.label, g.id, g, width) }));
  return { mask, groups, label: grouped.label };
}

const cross = (o, a, b) => (a[0] - o[0]) * (b[1] - o[1]) - (a[1] - o[1]) * (b[0] - o[0]);

function convexHull(pts) {
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
  return lower.slice(0, -1).concat(upper.slice(0, -1));
}

// Pixels covered by a hull through pixel centres: its area plus the half
// pixel it cuts off all round.
function hullPixelArea(hull) {
  if (hull.length < 3) return hull.length;
  let area = 0, perimeter = 0;
  for (let i = 0; i < hull.length; i++) {
    const [ax, ay] = hull[i];
    const [bx, by] = hull[(i + 1) % hull.length];
    area += ax * by - bx * ay;
    perimeter += Math.hypot(bx - ax, by - ay);
  }
  return Math.abs(area) / 2 + perimeter / 2 + 1;
}

function rasterHull(hull, into, width, y0, y1) {
  if (hull.length < 3) return;
  for (let y = y0; y <= y1; y++) {
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
    for (let x = Math.ceil(xmin); x <= Math.floor(xmax); x++) into[o + x] = 1;
  }
}

// Hull of the pixels of `mask` that fall in each labelled region, from the
// row extremes (enough for a convex hull).
function hullsByRegion(mask, label, regions, width) {
  return regions.map((g) => {
    const pts = [];
    let count = 0;
    for (let y = g.y0; y <= g.y1; y++) {
      let lo = -1, hi = -1;
      const o = y * width;
      for (let x = g.x0; x <= g.x1; x++) {
        if (label[o + x] !== g.id || !mask[o + x]) continue;
        count++;
        if (lo < 0) lo = x;
        hi = x;
      }
      if (lo >= 0) { pts.push([lo, y]); if (hi !== lo) pts.push([hi, y]); }
    }
    return { region: g, hull: convexHull(pts), count };
  });
}

// Below this share of its own convex outline, a precise cut-out isn't a
// whole stone: it is a pale stone whose facets read as backdrop and broke it
// apart. Whole cut-outs measured 0.987 and up, broken ones 0.90 and down.
// A heart's notch also falls below it and takes the safe path, which is fine.
const MIN_SOLIDITY = 0.96;
// And it must cover most of the object the first round found, or it may be
// just the saturated core of a stone whose pale rim was lost.
const MIN_COVERAGE = 0.75;

// Finds the stones in two rounds. The first, from a flood of the frame
// against a single backdrop colour, is only good enough to say roughly where
// the stones are, so the backdrop can be modelled around them. The second
// judges every pixel against that model, then the model is rebuilt around
// the tighter outline and the pixels judged once more.
//
// The precise outline is what makes a coloured stone look studio-shot, but
// colour cannot separate a colourless stone from a white box. So each object
// found in the first round is checked: if the precise cut-out of it is solid,
// it is used; if it came apart, the object keeps the first round's generous
// outline (closed up and convex), which may leave some shadow around a pale
// stone but never whitens part of it.
async function analyse(img, bg) {
  const { width, height } = img;
  const isBg = floodBackground(img, bg);
  const coarse = new Uint8Array(width * height);
  for (let p = 0; p < coarse.length; p++) coarse[p] = isBg[p] ? 0 : 1;
  let found = stonesFrom(coarse, width, height);
  if (!found.groups.length) return found;

  const r = Math.max(3, Math.round(Math.min(width, height) * 0.015));
  const objects = components(morph(morph(found.mask, width, height, r, false), width, height, r, true), width, height);

  const margin = (share) => Math.max(4, Math.round(Math.min(width, height) * share));
  let precise = null;
  for (const share of [0.03, 0.015]) {
    const exclude = morph((precise || found).mask, width, height, margin(share), false);
    const surface = await backgroundSurface(img, exclude);
    const next = stonesFrom(classify(img, surface), width, height);
    if (!next.groups.length) break;
    precise = next;
  }
  if (!precise) precise = { mask: new Uint8Array(width * height), groups: [], label: new Int32Array(width * height) };

  const pieces = new Map(precise.groups.map((g) => [g.id, g]));
  for (const { region, hull, count } of hullsByRegion(precise.mask, precise.label, precise.groups, width)) {
    region.solidity = count / hullPixelArea(hull);
  }

  const mask = new Uint8Array(width * height);
  const claimed = new Set();
  let protectedObjects = 0;
  for (const object of objects.kept) {
    const overlapping = new Set();
    for (let y = object.y0; y <= object.y1; y++) {
      const o = y * width;
      for (let x = object.x0; x <= object.x1; x++) {
        const id = precise.label[o + x];
        if (id && objects.label[o + x] === object.id && pieces.has(id)) overlapping.add(id);
      }
    }
    const parts = [...overlapping].map((id) => pieces.get(id));
    const covered = parts.reduce((s, g) => s + g.area, 0);
    const solid = parts.length > 0
      && parts.every((g) => g.solidity >= MIN_SOLIDITY)
      && covered >= object.area * MIN_COVERAGE;
    parts.forEach((g) => claimed.add(g.id));
    if (solid) {
      for (let p = 0; p < mask.length; p++) if (overlapping.has(precise.label[p])) mask[p] = 1;
      continue;
    }
    protectedObjects++;
    const box = parts.reduce((b, g) => ({
      x0: Math.min(b.x0, g.x0), y0: Math.min(b.y0, g.y0), x1: Math.max(b.x1, g.x1), y1: Math.max(b.y1, g.y1),
    }), { x0: object.x0, y0: object.y0, x1: object.x1, y1: object.y1 });
    const pts = [];
    for (let y = box.y0; y <= box.y1; y++) {
      let lo = -1, hi = -1;
      const o = y * width;
      for (let x = box.x0; x <= box.x1; x++) {
        if (objects.label[o + x] !== object.id && !overlapping.has(precise.label[o + x])) continue;
        if (lo < 0) lo = x;
        hi = x;
      }
      if (lo >= 0) { pts.push([lo, y]); if (hi !== lo) pts.push([hi, y]); }
    }
    rasterHull(convexHull(pts), mask, width, box.y0, box.y1);
  }
  for (const g of precise.groups) {
    if (claimed.has(g.id) || g.solidity < MIN_SOLIDITY) continue;
    for (let p = 0; p < mask.length; p++) if (precise.label[p] === g.id) mask[p] = 1;
  }

  const grouped = components(mask, width, height);
  const ids = new Set(grouped.kept.map((g) => g.id));
  for (let p = 0; p < mask.length; p++) if (mask[p] && !ids.has(grouped.label[p])) mask[p] = 0;
  const groups = grouped.kept.map((g) => ({ ...g, moments: momentsOf(grouped.label, g.id, g, width) }));
  return { mask, groups, protectedObjects };
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

function exposureOf({ data }, mask) {
  let total = 0, clipped = 0, lumSum = 0;
  for (let p = 0; p < mask.length; p++) {
    if (!mask[p]) continue;
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

async function assessQuality(img, bg, { groups, mask }) {
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

  const { meanLum, clippedShare } = exposureOf(img, mask);
  if (meanLum < 40) issues.push('too_dark');
  if (clippedShare > 0.25) issues.push('too_bright');

  // The backdrop is modelled locally, so ordinary fall-off and a stone's own
  // shadow clean up fine; only a backdrop this patchy still leaves marks.
  const bgLum = 0.299 * bg.color.r + 0.587 * bg.color.g + 0.114 * bg.color.b;
  if (bgLum < 150) issues.push('dark_background');
  if (bg.spread > 90) issues.push('uneven_background');

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

// Puts the stone on pure white. Inside the outline every pixel is copied
// as shot; outside it everything, shadow and dust included, is white. The
// outermost pixel or two of the outline are a mix of stone and grey
// backdrop, so the outline is pulled in by that much and then softened,
// which gives the anti-aliased edge of a studio cut-out instead of a grey
// rim or a jagged one.
function composeOnWhite(img, mask) {
  const { data, width, height } = img;
  const pull = Math.max(1, Math.round(Math.min(width, height) / 1500));
  const core = morph(mask, width, height, pull, true);
  const soft = new Float32Array(core.length);
  for (let p = 0; p < core.length; p++) soft[p] = core[p];
  const alpha = boxMean(boxMean(soft, width, height, pull), width, height, pull);
  const out = Buffer.alloc(data.length, 255);
  for (let p = 0; p < alpha.length; p++) {
    const a = alpha[p];
    if (a <= 0) continue;
    const i = p * 3;
    if (a >= 1) {
      out[i] = data[i]; out[i + 1] = data[i + 1]; out[i + 2] = data[i + 2];
      continue;
    }
    out[i] = Math.round(data[i] * a + 255 * (1 - a));
    out[i + 1] = Math.round(data[i + 1] * a + 255 * (1 - a));
    out[i + 2] = Math.round(data[i + 2] * a + 255 * (1 - a));
  }
  return { data: out, width, height };
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
  let a = await analyse(img, bg);
  // A protective outline is loose by design, so its axis says little about
  // how the stone actually lies.
  const skew = a.protectedObjects ? 0 : skewDegrees(a.groups);
  if (skew) {
    img = await toWorking(buffer, skew, bg.color);
    a = await analyse(img, bg);
  }
  const quality = await assessQuality(img, bg, a);
  quality.metrics = {
    ...quality.metrics,
    straightenedBy: Math.round(skew * 10) / 10,
    protectedOutlines: a.protectedObjects || 0,
  };
  if (!a.groups.length) return { image: null, quality };
  const image = await frame(composeOnWhite(img, a.mask), unionBox(a.groups));
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
