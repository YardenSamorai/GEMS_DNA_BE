// AI version of a station photo, made in the background when a reviewer asks
// for it and offered next to the clean one.
//
// An OpenAI model edits the photo as shot: background to white, tweezers and
// dust gone, stone centred and straight. Measured on real captures it does
// that better than the deterministic path, but it shifts the stone's colour
// (an emerald came back up to ΔE 7-11 greener and brighter). stonePhoto's
// `restyle` therefore puts the colour back to the one measured on the real
// photo and frames the result in the house style.

const sharp = require('sharp');
const { restyle } = require('./stonePhoto');

const MODEL = process.env.PHOTO_AI_MODEL || 'gpt-6-astra';
// The model takes 20-100s per photo; past this something is stuck.
const TIMEOUT_MS = 4 * 60 * 1000;
// Edits run one or two at a time so a burst of captures doesn't hit the
// API's rate limit all at once.
const MAX_RUNNING = 2;
const INPUT_SIDE = 1536;

const PROMPT = [
  'Edit this photo into a professional e-commerce catalog image of the loose gemstone(s).',
  'Pure white (#FFFFFF) background. Remove tweezers, holders, stickers, labels, dust, fingers and table texture.',
  'Center the stone(s), straight, filling about half of a square frame, with a very soft natural shadow underneath.',
  'If there are several stones (a pair, a set or a parcel), keep every one of them and their arrangement.',
  'CRITICAL: the gemstone must stay faithful to the photo - same color, saturation, brightness, inclusions, facets, shape and size.',
  'Do not beautify, sharpen, recolor or redraw the stone, and do not add or remove stones.',
].join(' ');

const isConfigured = () => Boolean(process.env.OPENAI_API_KEY) && process.env.PHOTO_AI !== 'off';

async function editPhoto(original) {
  const input = await sharp(original, { failOn: 'none' })
    .rotate()
    .resize(INPUT_SIDE, INPUT_SIDE, { fit: 'inside', withoutEnlargement: true })
    .jpeg({ quality: 92 })
    .toBuffer();
  const ctl = new AbortController();
  const timer = setTimeout(() => ctl.abort(), TIMEOUT_MS);
  try {
    const res = await fetch('https://api.openai.com/v1/responses', {
      method: 'POST',
      signal: ctl.signal,
      headers: { Authorization: `Bearer ${process.env.OPENAI_API_KEY}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({
        model: MODEL,
        input: [{
          role: 'user',
          content: [
            { type: 'input_text', text: PROMPT },
            { type: 'input_image', image_url: `data:image/jpeg;base64,${input.toString('base64')}` },
          ],
        }],
        tools: [{ type: 'image_generation', action: 'edit', size: '1024x1024', quality: 'high' }],
        tool_choice: { type: 'image_generation' },
      }),
    });
    const body = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error(`OpenAI ${res.status}: ${(body.error && body.error.message) || 'request failed'}`);
    const call = (body.output || []).find((o) => o.type === 'image_generation_call' && o.result);
    if (!call) throw new Error('The model returned no image');
    return Buffer.from(call.result, 'base64');
  } catch (e) {
    if (e.name === 'AbortError') throw new Error('OpenAI did not answer in time');
    throw e;
  } finally {
    clearTimeout(timer);
  }
}

// `clean` is the capture's clean image: the real stone pixels the colour is
// measured on.
async function processAi(original, clean) {
  const generated = await editPhoto(original);
  const image = await restyle(generated, clean);
  if (!image) throw new Error('No stone found in the AI image');
  return image;
}

let running = 0;
const waiting = [];
const next = () => {
  if (running >= MAX_RUNNING || !waiting.length) return;
  running++;
  const { job, resolve, reject } = waiting.shift();
  job().then(resolve, reject).finally(() => {
    running--;
    next();
  });
};
const enqueue = (job) => new Promise((resolve, reject) => {
  waiting.push({ job, resolve, reject });
  next();
});

module.exports = { isConfigured, processAi, enqueue, MODEL };
