const soap = require('soap');

const WSDL_URL = 'https://app.barakdiamonds.com/Gemstones/InternetTrade/BarakInventoryWS.asmx?wsdl';

const fetchSoapData = async () => {
  try {
    const client = await soap.createClientAsync(WSDL_URL);

    const args = {
        userName: 'gemstones',
        passWord: '20255588',
        apiKey: '2025',
      };

    const [response] = await client.GetStoneDataAsync(args); // 👈 updated method name

    // You might want to inspect which field contains the actual XML
    console.log('--- SOAP Response ---');
    console.log(response);

    // assuming XML is inside response.GetStoneDataResult
    return response.GetStoneDataResult || null;

  } catch (error) {
    console.error('❌ SOAP Fetch Error:', error.message);
    return null;
  }
};

module.exports = { fetchSoapData };