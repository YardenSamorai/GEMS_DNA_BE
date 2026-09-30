// Uploads approved stone photos to Barak's picture FTP (the folder Barak serves
// as Gemstones/Output/StoneImages).
//
// A file on that server is not yet a photo in Barak. It sits there as
// "Non-Linked" until someone runs Barak's Picture Mapping screen, which links
// every unlinked file named <parcel name><suffix>.<extension> to that parcel.
// So the filename is the whole contract:
//
//   <SKU>_DNA.jpg
//
// The SKU must be exact, since Picture Mapping matches on the parcel name.
// The _DNA suffix is ours alone. Nothing already on the server uses it, so a
// mapping run with suffix "_DNA" links only photos from the station and never
// the years of old files sitting there unlinked.
//
// Nothing is ever overwritten. If <SKU>_DNA.jpg exists, the next free name is
// used (_DNA2, _DNA3, ...). A mapping run with suffix "_DNA" won't pick those
// up, so the review screen says so when it happens.

const ftp = require('basic-ftp');
const { Readable } = require('stream');

const SUFFIX = '_DNA';
const MAX_VERSIONS = 20;

const config = () => ({
  host: process.env.BARAK_PICS_FTP_HOST,
  user: process.env.BARAK_PICS_FTP_USER,
  password: process.env.BARAK_PICS_FTP_PASSWORD,
});

const isConfigured = () => {
  const c = config();
  return !!(c.host && c.user && c.password);
};

// Characters Windows (and Barak's file server) cannot hold in a filename, plus
// control characters. A SKU carrying any of them can't be matched by name.
const UNSAFE = /[\\/:*?"<>|\x00-\x1f]/;
const skuFilenameProblem = (sku) => {
  if (!sku || sku !== sku.trim()) return 'SKU has leading or trailing spaces';
  if (UNSAFE.test(sku)) return 'SKU contains a character that cannot be used in a filename';
  return null;
};

// A dot in the SKU is legal in a filename, but the server already holds files
// like "MT94-0008.0008.jpg" for SKU "MT94-.0008", which suggests Barak's
// tooling splits names at the first dot. Worth a warning, not a block.
const skuFilenameWarning = (sku) =>
  String(sku).includes('.') ? 'SKU contains a dot. Check this one links after the Picture Mapping run.' : null;

const nameFor = (sku, version) => `${sku}${SUFFIX}${version > 1 ? version : ''}.jpg`;

async function withClient(fn) {
  const c = config();
  const client = new ftp.Client(30000);
  try {
    await client.access({ host: c.host, user: c.user, password: c.password, secure: false });
    return await fn(client);
  } finally {
    client.close();
  }
}

async function exists(client, name) {
  try {
    await client.size(name);
    return true;
  } catch (e) {
    if (e && e.code === 550) return false;
    throw e;
  }
}

// Uploads `buffer` under the first free name for `sku`. Returns the filename.
async function uploadStonePhoto(sku, buffer) {
  if (!isConfigured()) throw new Error('Barak picture FTP is not configured (BARAK_PICS_FTP_HOST/USER/PASSWORD)');
  const problem = skuFilenameProblem(sku);
  if (problem) throw new Error(problem);
  return withClient(async (client) => {
    let name = null;
    for (let v = 1; v <= MAX_VERSIONS; v++) {
      const candidate = nameFor(sku, v);
      if (!(await exists(client, candidate))) { name = candidate; break; }
    }
    if (!name) throw new Error(`${MAX_VERSIONS} photos already uploaded for ${sku}; refusing to add more`);
    await client.uploadFrom(Readable.from(buffer), name);
    const size = await client.size(name);
    if (size !== buffer.length) {
      throw new Error(`Upload of ${name} is incomplete on the server (${size} of ${buffer.length} bytes)`);
    }
    return name;
  });
}

// Read-only connectivity check for the status endpoint.
async function checkConnection() {
  if (!isConfigured()) return { ok: false, error: 'not configured' };
  try {
    const cwd = await withClient((client) => client.pwd());
    return { ok: true, cwd };
  } catch (e) {
    return { ok: false, error: e.message };
  }
}

module.exports = {
  SUFFIX,
  isConfigured,
  skuFilenameProblem,
  skuFilenameWarning,
  uploadStonePhoto,
  checkConnection,
};
