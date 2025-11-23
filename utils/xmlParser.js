const xml2js = require('xml2js');

/**
 * Parse raw XML string into JS object
 */
const parseXml = async (xmlString) => {
  const parser = new xml2js.Parser({ explicitArray: false, ignoreAttrs: false });
  return await parser.parseStringPromise(xmlString);
};

/**
 * Extract <Stone> entries from SOAP XML
 */
const parseSoapStones = async (xmlString) => {
  const parsed = await parseXml(xmlString);

  // Expecting: <Stock><Stone> ... </Stone></Stock>
  if (!parsed.Stock || !parsed.Stock.Stone) {
    return [];
  }

  const stones = parsed.Stock.Stone;
  return Array.isArray(stones) ? stones : [stones];
};

module.exports = { parseXml, parseSoapStones };