const { pool } = require('../../db/client');

const getOverview = async (req, res) => {
  try {
    const { rows: countRows } = await pool.query('SELECT COUNT(*) FROM soap_stones');
    const count = countRows[0].count;

    // נניח שאתה שומר מתי בוצע סנכרון אחרון בטבלה כלשהי (אפשר גם בקובץ)
    const { rows: syncRows } = await pool.query(`
      SELECT MAX(created_at) AS last_sync FROM soap_stones
    `);
    const lastSync = syncRows[0].last_sync;

    res.json({
      productsCount: parseInt(count),
      syncStatus: "Active", // אפשר להחליף לדינמי אם יש לך לוגיקה
      lastSync: lastSync || null
    });
  } catch (err) {
    console.error("❌ Error fetching dashboard overview:", err);
    res.status(500).json({ error: "Failed to fetch overview" });
  }
};

module.exports = getOverview;