const xml2js = require('xml2js');

const parseXml = async (xmlString) => {
  const parser = new xml2js.Parser({ explicitArray: false });
  return await parser.parseStringPromise(xmlString);
};

module.exports = { parseXml };