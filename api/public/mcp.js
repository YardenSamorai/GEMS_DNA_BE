/* =========================================================
   MCP server (Streamable HTTP, stateless)
   ---------------------------------------------------------
   POST /mcp          key in "Authorization: Bearer gdna_…"
                      or "X-API-Key"
   POST /mcp/:key     key in the path, for connectors that
                      can't send custom headers (ChatGPT)

   Every request builds a fresh server bound to the caller's
   team context, so tools can only ever see what the key's
   owner sees.
   ========================================================= */
const { z } = require('zod');
const { McpServer } = require('@modelcontextprotocol/sdk/server/mcp.js');
const { StreamableHTTPServerTransport } = require('@modelcontextprotocol/sdk/server/streamableHttp.js');
const { STONE_GROUPS, JEWELRY_GROUPS } = require('./catalogQuery');

const APP_URL = (process.env.FRONTEND_URL || 'https://gems-dna.com').replace(/\/+$/, '');

const list = (what) => z.string().optional().describe(`${what}. Comma-separated for several values.`);
const n = (what) => z.number().optional().describe(what);
const b = (what) => z.boolean().optional().describe(what);

const stoneFilters = {
  q: z.string().optional().describe('Free text over SKU, shape, colour, clarity, lab, origin, treatment, branch, certificate number.'),
  sku: list('Exact SKUs'),
  type: list('Stone family: diamond, fancy, emerald, gemstone'),
  gem: list('Gem name, e.g. Sapphire, Ruby, Emerald, Spinel, Diamond'),
  shape: list('Shape, e.g. Oval, Cushion, Round, Emerald'),
  color: list('Colour grade, e.g. D, E, F, or gemstone colour'),
  clarity: list('Clarity, e.g. VVS1, VS2, SI1'),
  lab: list('Lab, e.g. GIA, GRS, SSEF, Gubelin'),
  origin: list('Origin (partial match), e.g. Colombia, Burma, Sri Lanka'),
  treatment: list('Treatment (partial match), e.g. No Oil, Minor, Heated'),
  fancyColor: list('Fancy diamond colour (partial match), e.g. Yellow, Pink'),
  branch: list('Branch, e.g. New York, Israel, Hong Kong'),
  groupingType: list('Grouping, e.g. Pair, Layout'),
  minCt: n('Minimum carat weight'),
  maxCt: n('Maximum carat weight'),
  minPrice: n('Minimum total price (USD)'),
  maxPrice: n('Maximum total price (USD)'),
  minPpc: n('Minimum price per carat (USD)'),
  maxPpc: n('Maximum price per carat (USD)'),
  onMemo: b('Only stones out on memo (true) or in house (false)'),
  onHold: b('Only stones on hold (true) or not on hold (false)'),
  hasImage: b('Only stones with / without a photo'),
  hasCertificate: b('Only stones with / without a certificate'),
  isPair: b('Only stones that are / are not part of a pair'),
};

const jewelryFilters = {
  q: z.string().optional().describe('Free text over model, title, type, collection, metal, stone type, description.'),
  model: list('Exact model numbers'),
  jewelryType: list('Type (partial match), e.g. Ring, Earrings, Necklace, Bracelet'),
  collection: list('Collection (partial match)'),
  metal: list('Metal (partial match), e.g. Platinum, 18K'),
  stoneType: list('Main stone type (partial match), e.g. Emerald, Diamond'),
  category: list('Category (partial match)'),
  centerShape: list('Centre stone shape (partial match)'),
  branch: list('Branch'),
  minPrice: n('Minimum price (USD)'),
  maxPrice: n('Maximum price (USD)'),
  minCt: n('Minimum total carat'),
  maxCt: n('Maximum total carat'),
  onMemo: b('Only pieces out on memo (true) or in house (false)'),
};

const paging = (sorts) => ({
  sort: z.enum(sorts).optional().describe('Sort field'),
  order: z.enum(['asc', 'desc']).optional().describe('Sort direction (default desc)'),
  limit: z.number().int().min(1).max(200).optional().describe('Rows per page (default 25, max 200)'),
  offset: z.number().int().min(0).optional().describe('Rows to skip, for paging'),
  fields: z.string().optional().describe('Comma-separated fields to return, to keep answers small (e.g. sku,shape,weightCt,priceTotal)'),
});

function buildServer({ ctx, key }, req, { service, rewriteMedia }) {
  const server = new McpServer({ name: 'gems-dna', version: '1.0.0' });
  const reply = (data) => ({
    content: [{ type: 'text', text: JSON.stringify(rewriteMedia(data, req)) }],
  });
  const tool = (name, description, inputSchema, run) =>
    server.registerTool(name, { description, inputSchema, annotations: { readOnlyHint: true } }, async (args) => {
      try {
        return reply(await run(args || {}));
      } catch (e) {
        return { isError: true, content: [{ type: 'text', text: e.status && e.status < 500 ? e.message : 'Internal error' }] };
      }
    });

  tool(
    'catalog_overview',
    'Totals and the available values (with counts) for every filter of stones and jewelry. Call this first to learn the vocabulary (gems, shapes, labs, branches, jewelry types…).',
    {},
    () => service.meta(ctx)
  );

  tool(
    'search_stones',
    'Search loose stones (diamonds, fancy diamonds, emeralds, gemstones) with filters, sorting and paging. Prices are USD. Returns { total, hasMore, items }.',
    { ...stoneFilters, ...paging(['weight', 'price', 'ppc', 'ratio', 'sku', 'updated']) },
    (a) => service.searchStones(ctx, { limit: 25, ...a })
  );

  tool('get_stone', 'Full details of one stone by SKU.', { sku: z.string() }, (a) => service.getStone(ctx, a.sku));

  tool(
    'summarize_stones',
    'Group the (filtered) stones by one or two dimensions and total them: count, total carats, total value, average price, average price per carat (and total cost when the user may see cost). Use it for breakdowns, pivots and "cuts" of the inventory.',
    {
      groupBy: z.string().describe(`One or two (comma-separated) of: ${Object.keys(STONE_GROUPS).join(', ')}`),
      ...stoneFilters,
    },
    (a) => service.summarizeStones(ctx, a)
  );

  tool(
    'search_jewelry',
    'Search finished jewelry with filters, sorting and paging. Prices are USD. Returns { total, hasMore, items }.',
    { ...jewelryFilters, ...paging(['price', 'carat', 'model', 'newest']) },
    (a) => service.searchJewelry(ctx, { limit: 25, ...a })
  );

  tool(
    'get_jewelry',
    'Full details of one jewelry piece by model number or stock number.',
    { model: z.string() },
    (a) => service.getJewelry(ctx, a.model)
  );

  tool(
    'summarize_jewelry',
    'Group the (filtered) jewelry by one or two dimensions and total it: count, total carats, total value, average price.',
    {
      groupBy: z.string().describe(`One or two (comma-separated) of: ${Object.keys(JEWELRY_GROUPS).join(', ')}`),
      ...jewelryFilters,
    },
    (a) => service.summarizeJewelry(ctx, a)
  );

  tool(
    'export_excel',
    'Create an Excel (.xlsx) file of every stone or jewelry piece matching the filters and return a download link valid for one hour. Give the link to the user.',
    {
      dataset: z.enum(['stones', 'jewelry']),
      sort: z.string().optional().describe('Sort field (stones: weight, price, ppc, sku, updated; jewelry: price, carat, model, newest)'),
      order: z.enum(['asc', 'desc']).optional(),
      ...stoneFilters,
      ...jewelryFilters,
    },
    (a) => service.exportLink(ctx, key, req, a)
  );

  // Generic search/fetch pair that ChatGPT connectors and deep research look for.
  tool(
    'search',
    'Search the whole catalog (stones and jewelry) by free text. Returns ids usable with fetch.',
    { query: z.string() },
    async ({ query }) => {
      const [stones, jewelry] = await Promise.all([
        service.searchStones(ctx, { q: query, limit: 15 }),
        service.searchJewelry(ctx, { q: query, limit: 10 }),
      ]);
      return {
        results: [
          ...stones.items.map((s) => ({
            id: `stone:${s.sku}`,
            title: [s.sku, s.weightCt != null ? `${s.weightCt}ct` : null, s.shape, s.gems.join('/') || s.type]
              .filter(Boolean)
              .join(' '),
            url: `${APP_URL}/sales/stone/${encodeURIComponent(s.sku)}`,
          })),
          ...jewelry.items.map((j) => ({
            id: `jewelry:${j.modelNumber}`,
            title: j.title || `${j.modelNumber} ${j.jewelryType || ''}`.trim(),
            url: `${APP_URL}/sales/jewelry/${encodeURIComponent(j.modelNumber)}`,
          })),
        ],
      };
    }
  );

  tool('fetch', 'Fetch one catalog record by an id returned from search.', { id: z.string() }, async ({ id }) => {
    const [kind, ...rest] = String(id).split(':');
    const ref = rest.join(':');
    const isJewelry = kind === 'jewelry';
    const rec = isJewelry ? await service.getJewelry(ctx, ref) : await service.getStone(ctx, ref || kind);
    const sku = isJewelry ? rec.modelNumber : rec.sku;
    return {
      id: `${isJewelry ? 'jewelry' : 'stone'}:${sku}`,
      title: isJewelry ? rec.title || sku : `${sku} ${rec.weightCt ?? ''}ct ${rec.shape || ''}`.trim(),
      text: JSON.stringify(rec),
      url: `${APP_URL}/sales/${isJewelry ? 'jewelry' : 'stone'}/${encodeURIComponent(sku)}`,
      metadata: { kind: isJewelry ? 'jewelry' : 'stone' },
    };
  });

  return server;
}

function mountMcp(app, deps) {
  const { authenticate, bearerOf, keyLimiter, sendError } = deps;

  const handle = async (req, res, rawKey) => {
    let auth;
    try {
      auth = await authenticate(rawKey);
    } catch (e) {
      return sendError(res, e);
    }
    req.apiKey = auth.key;
    keyLimiter(req, res, async () => {
      try {
        const server = buildServer(auth, req, deps);
        const transport = new StreamableHTTPServerTransport({
          sessionIdGenerator: undefined,
          enableJsonResponse: true,
        });
        res.on('close', () => {
          transport.close();
          server.close();
        });
        await server.connect(transport);
        await transport.handleRequest(req, res, req.body);
      } catch (e) {
        if (!res.headersSent) sendError(res, e);
      }
    });
  };

  app.post('/mcp', (req, res) => handle(req, res, bearerOf(req)));
  app.post('/mcp/:key', (req, res) => handle(req, res, req.params.key));
  const notAllowed = (req, res) =>
    res.status(405).set('Allow', 'POST').json({
      jsonrpc: '2.0',
      error: { code: -32000, message: 'Method not allowed (stateless server: use POST).' },
      id: null,
    });
  app.get(['/mcp', '/mcp/:key'], notAllowed);
  app.delete(['/mcp', '/mcp/:key'], notAllowed);
}

module.exports = { mountMcp };
