/* What the scheduled jewelry sync may do with Barak's Jewelry_Web.csv.
 *
 * The sync mirrors the file: whatever is missing from it is deleted as
 * sold. That is only right while the file is current. Barak stopped
 * rewriting it on 27 Aug 2026 and nothing noticed for three months: every
 * run reported success, and each time someone uploaded a newer export by
 * hand, the next run put the August file back and deleted the new pieces as
 * "sold/discontinued" (eleven times between September and October).
 *
 * So the file's own modification time decides:
 *  - older than the last hand upload → skip; the upload is the newer truth.
 *  - older than STALE_DAYS → refresh what is in it, delete nothing, and say
 *    so on the Dashboard.
 *  - otherwise (or when the server can't tell) → mirror it as before.
 */

const STALE_DAYS = 3;
const DAY_MS = 24 * 60 * 60 * 1000;

const fmtDay = (d) =>
  d.toLocaleDateString("en-US", { month: "short", day: "numeric", year: "numeric", timeZone: "Asia/Jerusalem" });

const validDate = (d) => (d instanceof Date && Number.isFinite(d.getTime()) ? d : null);

const feedDecision = ({ fileModifiedAt, lastUploadAt, now = new Date(), staleDays = STALE_DAYS }) => {
  const file = validDate(fileModifiedAt);
  const upload = validDate(lastUploadAt);
  if (!file) return { action: "sync", prune: true, warning: null };

  if (upload && upload > file) {
    return {
      action: "skip",
      prune: false,
      warning: `Skipped: the file uploaded on ${fmtDay(upload)} is newer than Barak's (${fmtDay(file)}).`,
    };
  }

  const ageDays = Math.floor((now - file) / DAY_MS);
  if (ageDays >= staleDays) {
    return {
      action: "sync",
      prune: false,
      warning: `Barak's jewelry file hasn't changed since ${fmtDay(file)} (${ageDays} days), so nothing was removed.`,
    };
  }

  return { action: "sync", prune: true, warning: null };
};

module.exports = { feedDecision, STALE_DAYS };
