require('ts-node/register'); // או ts-node/register/transpile-only אם אתה רוצה יותר מהיר
const path = require('path');

module.exports = require(path.resolve(__dirname, './index.ts'));