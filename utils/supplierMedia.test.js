/* Run with:  node utils/supplierMedia.test.js
 *
 * Dependency-free, matching the other util tests here. Exits non-zero on the
 * first failure. */

const assert = require("assert");
const {
  SUPPLIER_ORIGIN,
  PROXY_PREFIX,
  proxyBaseFor,
  resolveSupplierUrl,
  rewriteToProxy,
  rewriteToSupplier,
  isAllowedContentType,
} = require("./supplierMedia");

let passed = 0;
const test = (name, fn) => {
  try {
    fn();
    passed++;
    console.log(`  ok  ${name}`);
  } catch (e) {
    console.error(`  FAIL  ${name}`);
    console.error(`        ${e.message}`);
    process.exitCode = 1;
  }
};

const BASE = `https://api.example.com${PROXY_PREFIX}`;
const IMG = `${SUPPLIER_ORIGIN}/Gemstones/Output/StoneImages/MTCU-0108.jpg`;

console.log("\nresolveSupplierUrl");

test("resolves a stone photo", () => {
  assert.strictEqual(
    resolveSupplierUrl("Gemstones/Output/StoneImages/MTCU-0108.jpg"),
    IMG
  );
});

test("resolves a certificate", () => {
  assert.strictEqual(
    resolveSupplierUrl("Gemstones/Output/Certificates/2023-107020.pdf"),
    `${SUPPLIER_ORIGIN}/Gemstones/Output/Certificates/2023-107020.pdf`
  );
});

test("encodes the spaces that 79 supplier filenames contain", () => {
  assert.strictEqual(
    resolveSupplierUrl("Gemstones/Output/StoneImages/RD SET-0019.jpg"),
    `${SUPPLIER_ORIGIN}/Gemstones/Output/StoneImages/RD%20SET-0019.jpg`
  );
});

test("leaves an already-encoded space alone", () => {
  assert.strictEqual(
    resolveSupplierUrl("Gemstones/Output/StoneImages/RD%20SET-0019.jpg"),
    `${SUPPLIER_ORIGIN}/Gemstones/Output/StoneImages/RD%20SET-0019.jpg`
  );
});

test("refuses the supplier's admin UI outside the media folder", () => {
  assert.strictEqual(resolveSupplierUrl("gemstones/Main.aspx"), null);
  assert.strictEqual(resolveSupplierUrl("web.config"), null);
});

test("refuses traversal back out of the media folder", () => {
  assert.strictEqual(resolveSupplierUrl("Gemstones/Output/../../Main.aspx"), null);
});

test("refuses to be pointed at another host", () => {
  assert.strictEqual(resolveSupplierUrl("https://evil.example/x.jpg"), null);
  assert.strictEqual(resolveSupplierUrl("//evil.example/x.jpg"), null);
  assert.strictEqual(resolveSupplierUrl("http://169.254.169.254/latest/meta-data"), null);
});

test("refuses junk", () => {
  assert.strictEqual(resolveSupplierUrl(""), null);
  assert.strictEqual(resolveSupplierUrl(null), null);
  assert.strictEqual(resolveSupplierUrl(undefined), null);
});

console.log("\nrewriteToProxy");

test("rewrites a supplier URL onto our origin", () => {
  assert.strictEqual(
    rewriteToProxy(IMG, BASE),
    `${BASE}Gemstones/Output/StoneImages/MTCU-0108.jpg`
  );
});

test("keeps the path intact so URL-shape checks still read the same", () => {
  /* `usableImg` calls a URL "no photo" when nothing follows the last slash.
   * A query-string proxy would end every one of those in `…?url=`, which
   * reads as a filename and would bring the broken thumbnails back. */
  const folderOnly = `${SUPPLIER_ORIGIN}/Gemstones/Output/StoneImages/`;
  const lastSegment = (u) => u.split("?")[0].split("/").pop();

  assert.strictEqual(lastSegment(rewriteToProxy(folderOnly, BASE)), "");
  assert.strictEqual(lastSegment(rewriteToProxy(IMG, BASE)), "MTCU-0108.jpg");
});

test("rewrites every occurrence in a serialised payload", () => {
  const body = JSON.stringify({
    sku: "MTCU-0108",
    imageUrl: IMG,
    additionalPictures: `${IMG};${IMG}`,
    nested: { snapshot: { image: IMG } },
  });
  const out = rewriteToProxy(body, BASE);

  assert.strictEqual(out.includes("app.barakdiamonds.com"), false);
  assert.strictEqual(out.split(BASE).length - 1, 4);
  assert.strictEqual(JSON.parse(out).nested.snapshot.image.startsWith(BASE), true);
});

test("matches http as well as https, and is case-insensitive about the host", () => {
  assert.strictEqual(
    rewriteToProxy("http://APP.BarakDiamonds.com/Gemstones/Output/a.jpg", BASE),
    `${BASE}Gemstones/Output/a.jpg`
  );
});

test("leaves other hosts untouched", () => {
  const others = "https://player.vimeo.com/video/123 https://eshed.com/x.png";
  assert.strictEqual(rewriteToProxy(others, BASE), others);
});

console.log("\nrewriteToSupplier");

test("normalises a proxy URL back to the supplier before we store it", () => {
  /* Keeps our own hostname out of the CRM snapshots and memo rows, so the
   * proxy stays a display-time concern we can delete the day the supplier
   * relaxes the header. */
  assert.strictEqual(rewriteToProxy(IMG, BASE) === IMG, false);
  assert.strictEqual(rewriteToSupplier(rewriteToProxy(IMG, BASE)), IMG);
});

test("normalises whichever host the client happened to be on", () => {
  for (const host of [
    "https://gems-dna-be.onrender.com",
    "https://gems-dna-be-preview.vercel.app",
    "http://localhost:5000",
  ]) {
    assert.strictEqual(
      rewriteToSupplier(`${host}${PROXY_PREFIX}Gemstones/Output/StoneImages/MTCU-0108.jpg`),
      IMG
    );
  }
});

test("leaves a plain supplier URL alone", () => {
  assert.strictEqual(rewriteToSupplier(IMG), IMG);
});

console.log("\nproxyBaseFor");

test("builds an absolute base from the request", () => {
  const req = { protocol: "https", get: (h) => (h === "host" ? "api.example.com" : null) };
  assert.strictEqual(proxyBaseFor(req), BASE);
});

console.log("\nisAllowedContentType");

test("allows images and PDFs", () => {
  for (const t of ["image/jpeg", "image/png", "application/pdf", "IMAGE/JPEG"]) {
    assert.strictEqual(isAllowedContentType(t), true, t);
  }
});

test("refuses to relay anything else from our own origin", () => {
  for (const t of ["text/html", "application/javascript", "", null, undefined]) {
    assert.strictEqual(isAllowedContentType(t), false, String(t));
  }
});

console.log(`\n${passed} passed\n`);
