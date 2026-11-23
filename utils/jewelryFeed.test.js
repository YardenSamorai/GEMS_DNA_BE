/* Run with:  node utils/jewelryFeed.test.js */

const assert = require("assert");
const { feedDecision } = require("./jewelryFeed");

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

const now = new Date("2026-11-23T10:00:00Z");
const hoursAgo = (h) => new Date(now.getTime() - h * 3600 * 1000);

test("mirrors a fresh file, deletions included", () => {
  assert.deepStrictEqual(feedDecision({ fileModifiedAt: hoursAgo(5), lastUploadAt: null, now }), {
    action: "sync",
    prune: true,
    warning: null,
  });
});

test("keeps everything when the file has gone stale, and says since when", () => {
  const d = feedDecision({ fileModifiedAt: new Date("2026-08-27T12:42:03Z"), lastUploadAt: null, now });
  assert.strictEqual(d.action, "sync");
  assert.strictEqual(d.prune, false);
  assert.match(d.warning, /hasn't changed since Aug 27, 2026 \(87 days\)/);
});

test("a file two days old is still current", () => {
  assert.strictEqual(feedDecision({ fileModifiedAt: hoursAgo(60), lastUploadAt: null, now }).prune, true);
});

test("never overwrites a hand upload with an older file", () => {
  const d = feedDecision({ fileModifiedAt: new Date("2026-08-27T12:42:03Z"), lastUploadAt: new Date("2026-10-22T11:00:00Z"), now });
  assert.strictEqual(d.action, "skip");
  assert.match(d.warning, /uploaded on Oct 22, 2026 is newer than Barak's \(Aug 27, 2026\)/);
});

test("a file Barak writes after the upload takes over again", () => {
  const d = feedDecision({ fileModifiedAt: hoursAgo(2), lastUploadAt: hoursAgo(30), now });
  assert.deepStrictEqual(d, { action: "sync", prune: true, warning: null });
});

test("unknown file time keeps the old behaviour", () => {
  assert.deepStrictEqual(feedDecision({ fileModifiedAt: null, lastUploadAt: hoursAgo(1), now }), {
    action: "sync",
    prune: true,
    warning: null,
  });
});

console.log(`\n${passed} passed`);
