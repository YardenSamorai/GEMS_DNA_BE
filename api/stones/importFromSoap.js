// Upload DATA from BARAK API to Neon DB calling SOAP_STONES
const { fetchSoapData } = require('../../utils/soapClient');
const { parseXml } = require('../../utils/xmlParser');
const { pool } = require('../../db/client');

const run = async () => {
  try {
    console.log('🚀 [1/6] Fetching SOAP data...');
    const rawXml = await fetchSoapData();

    if (!rawXml) {
      console.log('❌ No XML received');
      return;
    }

    console.log('📦 [2/6] Parsing XML...');
    const parsed = await parseXml(rawXml);
    const stones = parsed?.Stock?.Stone;

    if (!stones) {
      console.log('❌ No stones found inside parsed XML');
      return;
    }

    const stoneArray = Array.isArray(stones) ? stones : [stones];
    console.log(`📊 [3/6] Total stones found: ${stoneArray.length}`);

    const values = stoneArray.map(stone => [
      stone.Category || '',
      stone.SKU || '',
      stone.Shape || '',
      parseFloat(stone.Weight) || 0,
      stone.Color || '',
      stone.Clarity || '',
      stone.Lab || '',
      stone.Origin || '',
      parseFloat(stone.PricePerCarat) || 0,
      parseFloat(stone.TotalPrice) || 0,
      parseFloat(stone.ratio) || 0,
      stone['Measurements-delimiter'] || '',
      stone.Video || '',
      stone.Image || '',
      stone.Certificate || '',
      stone.Branch || '',
      stone.Fluorescence || '',
      stone.Polish || '',
      stone.Symmetry || '',
      parseFloat(stone.Table) || 0,
      parseFloat(stone.Depth) || 0,
      stone.Comment || '',
      stone.Certificateimage || '',
      stone.fancy_intensity || '',
      stone.fancy_color || '',
      stone.fancy_overtone || '',
      stone.fancy_color_2 || '',
      stone.fancy_overtone_2 || '',
      stone.home_page || '',
      stone.additional_pictures || '',
      stone.additional_videos || '',
      stone.Type || '',
      stone['Cert. Comments'] || '',
      stone.certificateImageJPG || '',
      parseFloat(stone['Rap. Price']) || 0,
      stone['Rap Price %'] || '',
      stone['Certificate Number'] || '',
      stone.Cut || '',
      stone['Pair Stone'] || '',
      new Date() // created_at
    ]);

    console.log('🧹 [4/6] Clearing soap_stones table...');
    await pool.query('TRUNCATE TABLE soap_stones RESTART IDENTITY');

    console.log('🧱 [5/6] Preparing bulk insert query...');
    const insertQuery = `
      INSERT INTO soap_stones (
        category, sku, shape, weight, color, clarity, lab, origin,
        price_per_carat, total_price, ratio, measurements, video, picture,
        certificate_pdf, branch, fluorescence, polish, symmetry, table_percent,
        depth, comment, certificate_image, fancy_intensity, fancy_color,
        fancy_overtone, fancy_color_2, fancy_overtone_2, home_page,
        additional_pictures, additional_videos, type, cert_comments,
        certificate_image_jpg, rap_price, rap_price_percent,
        certificate_number, cut, pair_stone, created_at
      ) VALUES ${values.map((_, i) => `(
        ${Array(40).fill(0).map((_, j) => `$${i * 40 + j + 1}`).join(', ')}
      )`).join(',')}
    `;

    const flatValues = values.flat();

    console.log('💾 [6/6] Inserting data into database...');
    await pool.query(insertQuery, flatValues);

    console.log(`✅ Done! Inserted ${stoneArray.length} stones into soap_stones 🎉`);
  } catch (err) {
    console.error('❌ Error during process:', err.message);
  }
};

run();
