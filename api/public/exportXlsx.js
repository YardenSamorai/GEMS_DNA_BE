const ExcelJS = require('exceljs');

const MONEY = '"$"#,##0';
const CARAT = '0.00';

const STONE_COLUMNS = [
  { key: 'sku', header: 'SKU', width: 14 },
  { key: 'type', header: 'Type', width: 14 },
  { key: 'category', header: 'Category', width: 18 },
  { key: 'shapeName', header: 'Shape', width: 12 },
  { key: 'shape', header: 'Shape Code', width: 10 },
  { key: 'weightCt', header: 'Carat', width: 8, numFmt: CARAT },
  { key: 'color', header: 'Color', width: 10 },
  { key: 'clarity', header: 'Clarity', width: 10 },
  { key: 'fancyIntensity', header: 'Fancy Intensity', width: 14 },
  { key: 'fancyColor', header: 'Fancy Color', width: 14 },
  { key: 'cut', header: 'Cut', width: 8 },
  { key: 'polish', header: 'Polish', width: 8 },
  { key: 'symmetry', header: 'Symmetry', width: 9 },
  { key: 'fluorescence', header: 'Fluorescence', width: 12 },
  { key: 'lab', header: 'Lab', width: 8 },
  { key: 'certificateNumber', header: 'Certificate #', width: 14 },
  { key: 'origin', header: 'Origin', width: 12 },
  { key: 'treatment', header: 'Treatment', width: 14 },
  { key: 'measurements', header: 'Measurements', width: 18 },
  { key: 'ratio', header: 'Ratio', width: 7, numFmt: '0.00' },
  { key: 'pricePerCt', header: 'Price / ct', width: 12, numFmt: MONEY },
  { key: 'priceTotal', header: 'Total Price', width: 13, numFmt: MONEY },
  { key: 'costPerCt', header: 'Cost / ct', width: 12, numFmt: MONEY, costOnly: true },
  { key: 'pairSku', header: 'Pair SKU', width: 12 },
  { key: 'groupingType', header: 'Grouping', width: 12 },
  { key: 'branch', header: 'Branch', width: 14 },
  { key: 'exactLocation', header: 'Location', width: 22 },
  { key: 'holder', header: 'Holder', width: 16 },
  { key: 'onMemo', header: 'On Memo', width: 9, bool: true },
  { key: 'onHold', header: 'On Hold', width: 9, bool: true },
  { key: 'imageUrl', header: 'Image', width: 10, link: true },
  { key: 'videoUrl', header: 'Video', width: 10, link: true },
  { key: 'certificateUrl', header: 'Certificate', width: 12, link: true },
];

const JEWELRY_COLUMNS = [
  { key: 'modelNumber', header: 'Model', width: 14 },
  { key: 'stockNumber', header: 'Stock #', width: 12 },
  { key: 'title', header: 'Title', width: 40 },
  { key: 'jewelryType', header: 'Type', width: 12 },
  { key: 'style', header: 'Style', width: 12 },
  { key: 'collection', header: 'Collection', width: 14 },
  { key: 'category', header: 'Category', width: 14 },
  { key: 'metalType', header: 'Metal', width: 12 },
  { key: 'stoneType', header: 'Stone Type', width: 14 },
  { key: 'totalCarat', header: 'Total Carat', width: 10, numFmt: CARAT },
  { key: 'centerStoneCarat', header: 'Center ct', width: 9, numFmt: CARAT },
  { key: 'centerStoneShape', header: 'Center Shape', width: 12 },
  { key: 'centerStoneColor', header: 'Center Color', width: 11 },
  { key: 'centerStoneClarity', header: 'Center Clarity', width: 12 },
  { key: 'jewelryWeight', header: 'Weight (g)', width: 10, numFmt: '0.00' },
  { key: 'jewelrySize', header: 'Size', width: 8 },
  { key: 'price', header: 'Price', width: 12, numFmt: MONEY },
  { key: 'cost', header: 'Cost', width: 12, numFmt: MONEY, costOnly: true },
  { key: 'availability', header: 'Availability', width: 12 },
  { key: 'branch', header: 'Branch', width: 14 },
  { key: 'exactLocation', header: 'Location', width: 22 },
  { key: 'holder', header: 'Holder', width: 16 },
  { key: 'onMemo', header: 'On Memo', width: 9, bool: true },
  { key: 'certificateNumber', header: 'Certificate #', width: 14 },
  { key: 'certificateUrl', header: 'Certificate', width: 12, link: true },
  { key: 'videoUrl', header: 'Video', width: 10, link: true },
  { key: 'picturesUrl', header: 'Pictures', width: 10, link: true },
];

// Location columns are dropped entirely when the viewer's tier blanks them,
// rather than shipping a column that is empty on every row.
const visibleColumns = (columns, rows, canSeeCost) =>
  columns.filter((c) => {
    if (c.costOnly) return canSeeCost;
    if (['branch', 'exactLocation', 'holder'].includes(c.key)) return rows.some((r) => r[c.key]);
    return true;
  });

async function buildWorkbook(dataset, rows, { canSeeCost, title }) {
  const columns = visibleColumns(
    dataset === 'jewelry' ? JEWELRY_COLUMNS : STONE_COLUMNS,
    rows,
    canSeeCost
  );
  const wb = new ExcelJS.Workbook();
  wb.creator = 'GEMS DNA';
  wb.created = new Date();
  const ws = wb.addWorksheet(title || (dataset === 'jewelry' ? 'Jewelry' : 'Stones'), {
    views: [{ state: 'frozen', ySplit: 1 }],
  });
  ws.columns = columns.map((c) => ({ key: c.key, header: c.header, width: c.width }));

  for (const r of rows) {
    const row = {};
    for (const c of columns) {
      const v = r[c.key];
      if (c.bool) row[c.key] = v ? 'Yes' : '';
      else if (c.link) row[c.key] = v ? { text: 'Open', hyperlink: v } : '';
      else row[c.key] = v == null ? '' : v;
    }
    ws.addRow(row);
  }

  const header = ws.getRow(1);
  header.font = { bold: true, color: { argb: 'FFFFFFFF' } };
  header.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FF1F2937' } };
  header.alignment = { vertical: 'middle' };
  header.height = 20;
  columns.forEach((c, i) => {
    const col = ws.getColumn(i + 1);
    if (c.numFmt) col.numFmt = c.numFmt;
    if (c.link) col.font = { color: { argb: 'FF047857' }, underline: true };
  });
  header.font = { bold: true, color: { argb: 'FFFFFFFF' } };
  ws.autoFilter = { from: { row: 1, column: 1 }, to: { row: 1, column: columns.length } };

  return wb.xlsx.writeBuffer();
}

module.exports = { buildWorkbook };
