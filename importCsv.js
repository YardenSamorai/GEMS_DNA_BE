const fs = require('fs');
const { parse } = require('csv-parse/sync');
const { pool } = require('./db/client');

const BRANCH_MAP = {
  IL:'Israel',EM:'Israel',JI:'Israel',
  LA:'Los Angeles',EL:'Los Angeles',
  HK:'Hong Kong',ES:'Hong Kong',HS:'Hong Kong',JH:'Hong Kong',JS:'Hong Kong',EH:'Hong Kong',
  NY:'New York',EN:'New York',ET:'New York',DT:'New York',JT:'New York',EG:'New York',
  EV:'New York',GN:'New York',VG:'New York',JG:'New York',JV:'New York',EY:'New York',
  HKG:'Hong Kong',ISR:'Israel',NYC:'New York'
};

const mapBranch = (b) => {
  if (!b) return null;
  const clean = b.trim();
  if (clean.includes('http://') || clean.includes('https://') || clean.length > 20) return null;
  return BRANCH_MAP[clean.toUpperCase()] || clean;
};

const safeNum = (v) => {
  const n = parseFloat(v);
  return Number.isFinite(n) ? n : null;
};

(async () => {
  const csvPath = process.argv[2] || 'c:/Users/yarden/Desktop/Diamonds20260329_125513.csv';
  console.log('Reading CSV:', csvPath);
  const csv = fs.readFileSync(csvPath, 'utf8');
  const rows = parse(csv, { columns: true, skip_empty_lines: true, relax_quotes: true, relax_column_count: true });
  console.log('Parsed rows:', rows.length);

  const columns = [
    'category','sku','shape','weight','color','clarity','lab',
    'fluorescence','price_per_carat','rap_price','rap_list_price',
    'total_price','location','branch','image','additional_pictures',
    'video','additional_videos','certificate_image','certificate_number',
    'certificate_image_jpg','cut','polish','symmetry','table_percent',
    'depth_percent','ratio','measurements','fancy_intensity',
    'fancy_color','fancy_overtone','fancy_color_2','fancy_overtone_2',
    'pair_stone','home_page','trade_show','comment','type',
    'cert_comments','origin','grouping_type','box','stones','raw_xml'
  ];

  const values = rows.map(r => {
    const ppc = safeNum(r['Price Per Carat']);
    const tp = safeNum(r['Total Price']);
    return [
      r['Category'] || null,
      r['SKU'] || null,
      r['Shape'] || null,
      safeNum(r['Weight']),
      r['Color'] || null,
      r['Clarity'] || null,
      r['Lab'] || null,
      r['Fluorescence'] || null,
      ppc !== null ? ppc * 2 : null,
      safeNum(r['Rap Price % ']),
      safeNum(r['Rap. Price']),
      tp !== null ? tp * 2 : null,
      r['Location'] || null,
      mapBranch(r['Branch']),
      r['Image'] || null,
      r['additional_pictures'] || null,
      r['Video'] || null,
      r['additional_videos'] || null,
      r['Certificate image'] || null,
      r['Certificate Number'] || null,
      r['certificateImageJPG'] || null,
      r['Cut'] || null,
      r['Polish'] || null,
      r['Symmetry'] || null,
      safeNum(r['Table']),
      safeNum(r['Depth']),
      safeNum(r['ratio']),
      r['Measurements (- delimiter)'] || null,
      r['fancy_intensity'] || null,
      r['fancy_color'] || null,
      r['fancy_overtone'] || null,
      r['fancy_color_2'] || null,
      r['fancy_overtone_2'] || null,
      r['Pair Stone'] || null,
      r['home_page'] || null,
      r['TradeShow'] || null,
      r['Comment'] || null,
      r['Type'] || null,
      r['Cert. Comments'] || null,
      r['Origin'] || null,
      r['Grouping Type'] || null,
      r['Box'] || null,
      safeNum(r['Stones']),
      'csv_import'
    ];
  });

  // Quick verification before insert
  const check = (sku) => {
    const row = rows.find(r => r['SKU'] === sku);
    if (row) console.log(`  ${sku}: Grouping Type="${row['Grouping Type']}", Origin="${row['Origin']}", TradeShow="${row['TradeShow']}"`);
  };
  console.log('CSV verification:');
  check('T-310H');
  check('T0773');
  check('T9446B');

  console.log('Truncating soap_stones...');
  await pool.query('TRUNCATE TABLE soap_stones RESTART IDENTITY');

  const CHUNK = 300;
  const totalChunks = Math.ceil(values.length / CHUNK);
  for (let i = 0; i < values.length; i += CHUNK) {
    const chunk = values.slice(i, i + CHUNK);
    const chunkIdx = Math.floor(i / CHUNK) + 1;
    const ph = chunk.map((row, ri) =>
      '(' + columns.map((_, ci) => '$' + (ri * columns.length + ci + 1)).join(',') + ')'
    ).join(',');
    await pool.query('INSERT INTO soap_stones (' + columns.join(',') + ') VALUES ' + ph, chunk.flat());
    console.log(`  Chunk ${chunkIdx}/${totalChunks} (${chunk.length} rows)`);
  }

  // Verify in DB
  const res = await pool.query("SELECT sku, grouping_type, origin, trade_show FROM soap_stones WHERE sku IN ('T-310H','T0773','T9446B') ORDER BY sku");
  console.log('DB verification:');
  res.rows.forEach(r => console.log(`  ${r.sku}: grouping_type=${r.grouping_type}, origin=${r.origin}, trade_show=${r.trade_show}`));

  // GroupingType distribution
  const dist = await pool.query("SELECT grouping_type, COUNT(*) as cnt FROM soap_stones GROUP BY grouping_type ORDER BY cnt DESC");
  console.log('GroupingType distribution:');
  dist.rows.forEach(r => console.log(`  ${r.grouping_type || 'NULL'}: ${r.cnt}`));

  console.log(`DONE! Inserted ${values.length} stones from CSV`);
  await pool.end();
})();
