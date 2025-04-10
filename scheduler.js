const cron = require('node-cron');
const { exec } = require('child_process');

console.log('⏰ Scheduler started. Will run importFromSoap.js every 5 hours.');

// Runs every 5 hours at minute 0
cron.schedule('0 */5 * * *', () => {
  console.log('🔁 Running importFromSoap.js at', new Date().toLocaleString());

  exec('node api/stones/importFromSoap.js', (error, stdout, stderr) => {
    if (error) {
      console.error(`❌ Error: ${error.message}`);
      return;
    }
    if (stderr) {
      console.error(`⚠️ STDERR: ${stderr}`);
      return;
    }

    console.log(`✅ Output:\n${stdout}`);
  });
});