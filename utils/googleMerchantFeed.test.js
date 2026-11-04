/* Run with:  node utils/googleMerchantFeed.test.js */

const assert = require("assert");
const { toFeedItem, includeInFeed, uniqueIds, buildFeedXml, plainText } = require("./googleMerchantFeed");

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

const product = (over = {}) => ({
  id: 193332,
  sku: "RI-HE-007",
  name: "10.05ct Emerald-cut Diamond Ring &#8211; 10.05ct TW",
  permalink: "https://eshed.com/eshed/RI-HE-007/",
  short_description: "<p>A GIA-certified <b>L SI1</b> diamond.</p>",
  description: "",
  is_in_stock: true,
  prices: { price: "13500000", regular_price: "13500000", sale_price: "13500000", currency_code: "USD", currency_minor_unit: 2 },
  images: [{ src: "https://eshed.com/a.jpg" }, { src: "https://eshed.com/b.jpg" }],
  categories: [{ slug: "jewelry", name: "Jewelry" }],
  ...over,
});

const stone = (total) =>
  product({
    sku: "M36-127Y",
    name: "1.12 Emerald ICA Minor",
    categories: [{ slug: "emerald", name: "Emerald" }],
    prices: { price: String(total * 100), regular_price: String(total * 100), currency_code: "USD", currency_minor_unit: 2 },
  });

test("uses the full SKU as the id", () => {
  assert.strictEqual(toFeedItem(product()).id, "RI-HE-007");
});

test("falls back to the WooCommerce id when there is no SKU", () => {
  assert.strictEqual(toFeedItem(product({ sku: "" })).id, "wc-193332");
});

test("reads prices from minor units", () => {
  const item = toFeedItem(product());
  assert.strictEqual(item.price, 135000);
  assert.strictEqual(item.salePrice, null);
});

test("keeps a real WooCommerce sale as sale_price", () => {
  const item = toFeedItem(product({ prices: { price: "9000000", regular_price: "10000000", currency_code: "USD", currency_minor_unit: 2 } }));
  assert.strictEqual(item.price, 100000);
  assert.strictEqual(item.salePrice, 90000);
});

test("decodes the title and strips HTML from the description", () => {
  const item = toFeedItem(product());
  assert.strictEqual(item.title, "10.05ct Emerald-cut Diamond Ring – 10.05ct TW");
  assert.strictEqual(item.description, "A GIA-certified L SI1 diamond.");
  assert.strictEqual(plainText("a&nbsp;&amp;&#039;b"), "a &'b");
});

test("includes jewelry at any price", () => {
  assert.ok(includeInFeed(toFeedItem(product())));
});

test("includes stones up to $50,000 and excludes above", () => {
  assert.ok(includeInFeed(toFeedItem(stone(50000))));
  assert.ok(!includeInFeed(toFeedItem(stone(50001))));
});

test("excludes products without a price", () => {
  assert.ok(!includeInFeed(toFeedItem(product({ prices: { price: "0", regular_price: "0", currency_minor_unit: 2 } }))));
});

test("keeps duplicate SKUs apart", () => {
  const items = uniqueIds([toFeedItem(product()), toFeedItem(product({ id: 5 }))]);
  assert.deepStrictEqual(items.map((i) => i.id), ["RI-HE-007", "RI-HE-007-5"]);
});

test("writes a Merchant Center RSS item", () => {
  const xml = buildFeedXml([toFeedItem(product())], { siteUrl: "https://eshed.com" });
  assert.ok(xml.includes('xmlns:g="http://base.google.com/ns/1.0"'));
  assert.ok(xml.includes("<g:id>RI-HE-007</g:id>"));
  assert.ok(xml.includes("<g:price>135000.00 USD</g:price>"));
  assert.ok(xml.includes("<g:additional_image_link>https://eshed.com/b.jpg</g:additional_image_link>"));
  assert.ok(!xml.includes("<g:sale_price>"));
  assert.ok(xml.includes("<g:availability>in_stock</g:availability>"));
});

test("escapes XML special characters", () => {
  const xml = buildFeedXml([toFeedItem(product({ name: "Emerald &amp; Diamond &lt;Ring&gt;" }))], { siteUrl: "https://eshed.com" });
  assert.ok(xml.includes("<g:title>Emerald &amp; Diamond &lt;Ring&gt;</g:title>"));
});

console.log(`\n${passed} passed`);
