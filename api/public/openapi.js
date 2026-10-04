const { STONE_GROUPS, JEWELRY_GROUPS } = require('./catalogQuery');

const str = (name, description) => ({ name, in: 'query', required: false, description, schema: { type: 'string' } });
const numP = (name, description) => ({ name, in: 'query', required: false, description, schema: { type: 'number' } });
const boolP = (name, description) => ({ name, in: 'query', required: false, description, schema: { type: 'boolean' } });

const STONE_FILTERS = [
  str('q', 'Free text over SKU, shape, colour, clarity, lab, origin, treatment, branch, certificate number.'),
  str('sku', 'Exact SKUs, comma-separated.'),
  str('type', 'Stone family, comma-separated: diamond, fancy, emerald, gemstone.'),
  str('gem', 'Gem names, comma-separated, e.g. Sapphire,Ruby.'),
  str('shape', 'Shapes, comma-separated.'),
  str('color', 'Colour grades, comma-separated.'),
  str('clarity', 'Clarity grades, comma-separated.'),
  str('lab', 'Labs, comma-separated (GIA, GRS, SSEF…).'),
  str('origin', 'Origin, partial match, comma-separated.'),
  str('treatment', 'Treatment, partial match, comma-separated.'),
  str('fancyColor', 'Fancy diamond colour, partial match.'),
  str('branch', 'Branches, comma-separated.'),
  str('groupingType', 'Grouping (Pair, Layout…).'),
  numP('minCt', 'Minimum carat.'),
  numP('maxCt', 'Maximum carat.'),
  numP('minPrice', 'Minimum total price USD.'),
  numP('maxPrice', 'Maximum total price USD.'),
  numP('minPpc', 'Minimum price per carat USD.'),
  numP('maxPpc', 'Maximum price per carat USD.'),
  boolP('onMemo', 'true = out on memo, false = in house.'),
  boolP('onHold', 'true = on hold.'),
  boolP('hasImage', 'Has a photo.'),
  boolP('hasCertificate', 'Has a certificate.'),
  boolP('isPair', 'Part of a pair.'),
];

const JEWELRY_FILTERS = [
  str('q', 'Free text over model, title, type, collection, metal, stone type, description.'),
  str('model', 'Exact model numbers, comma-separated.'),
  str('jewelryType', 'Type, partial match (Ring, Earrings, Necklace…).'),
  str('collection', 'Collection, partial match.'),
  str('metal', 'Metal, partial match.'),
  str('stoneType', 'Main stone type, partial match.'),
  str('category', 'Category, partial match.'),
  str('centerShape', 'Centre stone shape, partial match.'),
  str('branch', 'Branches, comma-separated.'),
  numP('minPrice', 'Minimum price USD.'),
  numP('maxPrice', 'Maximum price USD.'),
  numP('minCt', 'Minimum total carat.'),
  numP('maxCt', 'Maximum total carat.'),
  boolP('onMemo', 'true = out on memo, false = in house.'),
];

const paging = (sorts) => [
  { name: 'sort', in: 'query', required: false, description: 'Sort field.', schema: { type: 'string', enum: sorts } },
  { name: 'order', in: 'query', required: false, description: 'asc or desc (default desc).', schema: { type: 'string', enum: ['asc', 'desc'] } },
  { name: 'limit', in: 'query', required: false, description: 'Rows per page, default 50, max 500.', schema: { type: 'integer' } },
  { name: 'offset', in: 'query', required: false, description: 'Rows to skip.', schema: { type: 'integer' } },
  str('fields', 'Comma-separated fields to return, to keep responses small.'),
];

const groupBy = (groups) => ({
  name: 'groupBy',
  in: 'query',
  required: true,
  description: `One or two comma-separated of: ${Object.keys(groups).join(', ')}.`,
  schema: { type: 'string' },
});

const ok = (description) => ({
  200: { description, content: { 'application/json': { schema: { type: 'object', additionalProperties: true } } } },
});

function buildOpenApi(baseUrl) {
  return {
    openapi: '3.1.0',
    info: {
      title: 'GEMS DNA API',
      version: '1.0.0',
      description:
        'Read-only access to the GEMS DNA inventory: loose stones (diamonds, fancy diamonds, emeralds, gemstones) and finished jewelry. ' +
        'Results are limited to what the API key owner may see in the app. Prices are USD. ' +
        'Call getCatalogMeta first to learn the available filter values.',
    },
    servers: [{ url: baseUrl }],
    components: {
      securitySchemes: { apiKey: { type: 'http', scheme: 'bearer' } },
      schemas: {},
    },
    security: [{ apiKey: [] }],
    paths: {
      '/api/v1/meta': {
        get: {
          operationId: 'getCatalogMeta',
          summary: 'Totals and available values (with counts) for every filter of stones and jewelry.',
          responses: ok('Catalog vocabulary'),
        },
      },
      '/api/v1/stones': {
        get: {
          operationId: 'searchStones',
          summary: 'Search loose stones with filters, sorting and paging.',
          parameters: [...STONE_FILTERS, ...paging(['weight', 'price', 'ppc', 'ratio', 'sku', 'updated'])],
          responses: ok('{ total, offset, limit, hasMore, items }'),
        },
      },
      '/api/v1/stones/summary': {
        get: {
          operationId: 'summarizeStones',
          summary: 'Group filtered stones by one or two dimensions with count, carats, value, average price and price per carat.',
          parameters: [groupBy(STONE_GROUPS), ...STONE_FILTERS],
          responses: ok('{ groupBy, totals, groups }'),
        },
      },
      '/api/v1/stones/{sku}': {
        get: {
          operationId: 'getStone',
          summary: 'Full details of one stone.',
          parameters: [{ name: 'sku', in: 'path', required: true, schema: { type: 'string' } }],
          responses: ok('Stone'),
        },
      },
      '/api/v1/jewelry': {
        get: {
          operationId: 'searchJewelry',
          summary: 'Search finished jewelry with filters, sorting and paging.',
          parameters: [...JEWELRY_FILTERS, ...paging(['price', 'carat', 'model', 'newest'])],
          responses: ok('{ total, offset, limit, hasMore, items }'),
        },
      },
      '/api/v1/jewelry/summary': {
        get: {
          operationId: 'summarizeJewelry',
          summary: 'Group filtered jewelry by one or two dimensions with count, carats, value and average price.',
          parameters: [groupBy(JEWELRY_GROUPS), ...JEWELRY_FILTERS],
          responses: ok('{ groupBy, totals, groups }'),
        },
      },
      '/api/v1/jewelry/{model}': {
        get: {
          operationId: 'getJewelry',
          summary: 'Full details of one jewelry piece by model or stock number.',
          parameters: [{ name: 'model', in: 'path', required: true, schema: { type: 'string' } }],
          responses: ok('Jewelry piece'),
        },
      },
      '/api/v1/export-link': {
        get: {
          operationId: 'createExcelExport',
          summary: 'Build an Excel file of all rows matching the filters and return a one-hour download link to give the user.',
          parameters: [
            { name: 'dataset', in: 'query', required: true, schema: { type: 'string', enum: ['stones', 'jewelry'] } },
            { name: 'sort', in: 'query', required: false, schema: { type: 'string' } },
            { name: 'order', in: 'query', required: false, schema: { type: 'string', enum: ['asc', 'desc'] } },
            ...STONE_FILTERS,
            ...JEWELRY_FILTERS.filter((p) => !STONE_FILTERS.some((s) => s.name === p.name)),
          ],
          responses: ok('{ url, rows, expiresAt }'),
        },
      },
    },
  };
}

module.exports = { buildOpenApi };
