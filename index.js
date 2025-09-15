/**
 * Combined Proxy Health Checker (Master + Support Worker)
 *
 * This single file contains the logic for both the Master and Support workers.
 * The behavior is determined by the `ROLE` environment variable set in wrangler.toml.
 *
 * Master Role (`ROLE=MASTER`):
 * - Cron `*/30 * * * *`: Triggers a full health check.
 * - Cron `*/15 * * * *`: Triggers a failover check for stale dispatches.
 * - POST /force-health -> Manually triggers a full health check.
 * - POST /submit-results -> Protected endpoint for support workers to post back results.
 *
 * Support Role (`ROLE=SUPPORT`):
 * - POST /check-batch -> Receives a batch of proxies to check from the master.
 */

const DISPATCH_CHUNK_SIZE = 40; // Batches to dispatch per invocation, well under 50 subrequest limit
const STALE_DISPATCH_TIMEOUT = 10 * 60 * 1000; // 10 minutes in milliseconds

const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET,POST,OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type,Authorization',
};

function jsonResponse(obj, status = 200) {
  return new Response(JSON.stringify(obj, null, 2), {
    status,
    headers: { 'Content-Type': 'application/json', ...CORS }
  });
}

function textResponse(text, status = 200) {
  return new Response(text, { status, headers: CORS });
}

export default {
  async fetch(request, env, ctx) {
    if (request.method === 'OPTIONS') return new Response(null, { headers: CORS });
    try {
      if (env.ROLE === 'MASTER') {
        return await handleMasterRequest(request, env, ctx);
      } else if (env.ROLE === 'SUPPORT') {
        return await handleSupportRequest(request, env, ctx);
      } else {
        return textResponse('Configuration error: ROLE not set.', 500);
      }
    } catch (err) {
      return textResponse(String(err), 500);
    }
  },

  async scheduled(controller, env, ctx) {
    console.log(`Scheduled event triggered: ${controller.cron}`);
    if (env.ROLE !== 'MASTER') return;

    // Route to different handlers based on which cron string triggered the event
    if (controller.cron === '*/30 * * * *') {
      ctx.waitUntil(triggerHealthCheck(env, ctx));
    } else if (controller.cron === '*/15 * * * *') {
      ctx.waitUntil(handleFailoverCheck(env, ctx));
    }
  }
};

// ===================================================================================
//  MASTER WORKER LOGIC
// ===================================================================================

async function handleMasterRequest(request, env, ctx) {
  const url = new URL(request.url);
  const path = url.pathname;

  if (request.method === 'POST' && path === '/force-health') {
    const auth = request.headers.get('authorization') || '';
    if (env.FORCE_TOKEN && auth !== `Bearer ${env.FORCE_TOKEN}`) return textResponse('Unauthorized', 401);
    ctx.waitUntil(triggerHealthCheck(env, ctx));
    return jsonResponse({ ok: true, message: 'Health check process triggered in the background.' });
  }

  if (request.method === 'POST' && path === '/dispatch-internal') {
    const auth = request.headers.get('authorization') || '';
    if (!env.SLAVE_TOKEN || auth !== `Bearer ${env.SLAVE_TOKEN}`) return textResponse('Unauthorized', 401);
    ctx.waitUntil(handleInternalDispatch(request, env, ctx));
    return jsonResponse({ ok: true, message: 'Internal dispatch accepted.' });
  }

  if (request.method === 'POST' && path === '/submit-results') {
    const auth = request.headers.get('authorization') || '';
    if (!env.MASTER_TOKEN || auth !== `Bearer ${env.MASTER_TOKEN}`) return textResponse('Unauthorized', 401);
    let payload;
    try { payload = await request.json(); } catch (e) { return textResponse('Invalid JSON', 400); }
    if (!payload || !Array.isArray(payload.results) || !payload.batchId) return textResponse('Bad payload', 400);

    if (env.DISPATCH_LOG) {
      ctx.waitUntil(env.DISPATCH_LOG.delete(payload.batchId));
    }

    const validResults = payload.results.filter(r => r && r.proxy);
    const keys = validResults.map(r => r.proxy);
    const prevValuesRaw = await Promise.all(keys.map(key => env.PROXY_CACHE.get(key)));
    const prevValues = prevValuesRaw.map(v => { try { return v ? JSON.parse(v) : null; } catch (e) { return null; } });
    const putPromises = validResults.map((rec, i) => env.PROXY_CACHE.put(keys[i], JSON.stringify(rec)));
    await Promise.all(putPromises);
    await updateSummaryBatch(env, prevValues, validResults);
    return jsonResponse({ ok: true, stored: validResults.length });
  }

  if (request.method === 'GET' && path === '/health') {
    const healthSummary = await env.PROXY_CACHE.get('_HEALTH_SUMMARY', 'json') || {};
    const supportStats = await env.SUPPORT_STATS.list().then(async ({ keys }) => {
      const values = await Promise.all(keys.map(k => env.SUPPORT_STATS.get(k.name, 'json')));
      return values.filter(Boolean);
    });
    return jsonResponse({ health_summary: healthSummary, support_workers: supportStats });
  }

  if (request.method === 'GET' && path === '/stats') {
     const supportStats = await env.SUPPORT_STATS.list().then(async ({ keys }) => {
      const values = await Promise.all(keys.map(k => env.SUPPORT_STATS.get(k.name, 'json')));
      return values.filter(Boolean);
    });
    return jsonResponse({ support_workers: supportStats });
  }

  return textResponse('Not found.', 404);
}

async function handleInternalDispatch(request, env, ctx) {
  let payload;
  try { payload = await request.json(); } catch (e) { return; }
  const { remainingBatches, endpoints, originalDispatchIndex } = payload;
  if (!remainingBatches || !Array.isArray(remainingBatches) || !endpoints || !Array.isArray(endpoints)) return;
  if (remainingBatches.length === 0) return;

  const currentDispatchIndex = originalDispatchIndex || 0;
  const batchesToDispatch = remainingBatches.slice(0, DISPATCH_CHUNK_SIZE);
  const nextRemainingBatches = remainingBatches.slice(DISPATCH_CHUNK_SIZE);

  const dispatchPromises = batchesToDispatch.map((batch, idx) => {
    const globalIndex = currentDispatchIndex + idx;
    const endpointIndex = globalIndex % endpoints.length;
    const endpoint = endpoints[endpointIndex];
    const batchId = `${Date.now()}-${globalIndex}`;
    return dispatchToSlave(endpoint, batch, batchId, env, ctx);
  });
  await Promise.all(dispatchPromises);

  if (nextRemainingBatches.length > 0) {
    const nextDispatchIndex = currentDispatchIndex + batchesToDispatch.length;
    const nextRequest = new Request(`${env.MASTER_ENDPOINT}/dispatch-internal`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${env.SLAVE_TOKEN || ''}` },
      body: JSON.stringify({ remainingBatches: nextRemainingBatches, endpoints, originalDispatchIndex: nextDispatchIndex }),
    });
    ctx.waitUntil(fetch(nextRequest));
  }
}

async function triggerHealthCheck(env, ctx) {
  console.log('Starting health check via triggerHealthCheck');
  if (!env.PROXY_LIST_URL) return;
  const r = await fetch(env.PROXY_LIST_URL);
  if (!r.ok) return;
  const text = await r.text();
  const lines = text.split('\n').map(l => l.trim()).filter(Boolean);
  if (!lines.length) return;

  const proxies = lines.map(line => {
    if (line.includes(',')) {
      const p = line.split(',').map(s => s.trim());
      return { ip: p[0], port: p[1], country: p[2] || null, isp: p[3] || null };
    } else if (line.includes(':')) {
      const [ip, port] = line.split(':').map(s => s.trim());
      return { ip, port };
    }
    return null;
  }).filter(Boolean);

  const batchSize = Math.max(1, parseInt(env.BATCH_SIZE || '50', 10));
  const endpoints = (env.SLAVE_ENDPOINTS || '').split(',').map(s => s.trim()).filter(Boolean);
  if (!endpoints.length) return;

  const allBatches = [];
  for (let i = 0; i < proxies.length; i += batchSize) {
    allBatches.push(proxies.slice(i, i + batchSize));
  }

  const initialBatches = allBatches.slice(0, DISPATCH_CHUNK_SIZE);
  const remainingBatches = allBatches.slice(DISPATCH_CHUNK_SIZE);

  const initialTasks = initialBatches.map((batch, idx) => {
    const endpoint = endpoints[idx % endpoints.length];
    const batchId = `${Date.now()}-${idx}`;
    return dispatchToSlave(endpoint, batch, batchId, env, ctx);
  });
  await Promise.all(initialTasks);

  if (remainingBatches.length > 0) {
    const nextRequest = new Request(`${env.MASTER_ENDPOINT}/dispatch-internal`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${env.SLAVE_TOKEN || ''}` },
      body: JSON.stringify({ remainingBatches, endpoints, originalDispatchIndex: initialBatches.length }),
    });
    ctx.waitUntil(fetch(nextRequest));
  }
}

async function dispatchToSlave(endpoint, batch, batchId, env, ctx) {
  if (env.DISPATCH_LOG) {
    const logEntry = { batch, dispatchedTo: endpoint, timestamp: Date.now() };
    ctx.waitUntil(env.DISPATCH_LOG.put(batchId, JSON.stringify(logEntry), { expirationTtl: 1800 }));
  }
  try {
    await fetch(`${endpoint.replace(/\/$/, '')}/check-batch`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${env.SLAVE_TOKEN || ''}` },
      body: JSON.stringify({ batch, batchId })
    });
  } catch (e) {
    console.error(`Failed to dispatch batch ${batchId} to ${endpoint}:`, e);
  }
}

async function handleFailoverCheck(env, ctx) {
  console.log('Running failover check for stale dispatches...');
  if (!env.DISPATCH_LOG || !env.SUPPORT_STATS) return;

  const { keys: staleKeys } = await env.DISPATCH_LOG.list();
  if (staleKeys.length === 0) {
    console.log('No pending dispatches found. Failover check complete.');
    return;
  }

  const { keys: supportWorkerKeys } = await env.SUPPORT_STATS.list();
  const supportWorkers = await Promise.all(
    supportWorkerKeys.map(k => env.SUPPORT_STATS.get(k.name, 'json'))
  );
  const activeWorkers = supportWorkers.filter(w => w && w.status === 'alive').map(w => w.support_url);

  if (activeWorkers.length === 0) {
    console.error('FAILOVER_FAIL: No active support workers available to re-dispatch tasks.');
    return;
  }

  let redispatchedCount = 0;
  for (const key of staleKeys) {
    const logEntry = await env.DISPATCH_LOG.get(key.name, 'json');
    if (!logEntry) continue;

    if (Date.now() - logEntry.timestamp > STALE_DISPATCH_TIMEOUT) {
      console.warn(`Found stale dispatch: ${key.name}, originally sent to ${logEntry.dispatchedTo}. Re-dispatching...`);

      const originalWorker = logEntry.dispatchedTo;
      const availableWorkers = activeWorkers.filter(w => w !== originalWorker);
      const targetWorker = availableWorkers.length > 0 ? availableWorkers[0] : activeWorkers[0]; // Pick a different worker if possible

      if (!targetWorker) {
          console.error(`FAILOVER_SKIP: Could not find a suitable active worker for stale batch ${key.name}.`);
          continue;
      }

      // Re-dispatch to the new target worker
      const newBatchId = `failover-${key.name}`;
      await dispatchToSlave(targetWorker, logEntry.batch, newBatchId, env, ctx);
      redispatchedCount++;

      // Delete the old stale log to prevent it from being re-dispatched again
      await env.DISPATCH_LOG.delete(key.name);
    }
  }
  console.log(`Failover check complete. Re-dispatched ${redispatchedCount} stale batches.`);
}

async function updateSummaryBatch(env, prevResults, currResults) {
  if (currResults.length === 0) return;
  let summary;
  try { summary = await env.PROXY_CACHE.get('_HEALTH_SUMMARY', 'json'); } catch(e) { summary = {}; }
  if (!summary) summary = { total: 0, alive: 0, dead: 0, countries: {} };
  // ... (rest of the function is the same, omitted for brevity)
}

// ===================================================================================
//  SUPPORT WORKER LOGIC
// ===================================================================================

async function handleSupportRequest(request, env, ctx) {
  const url = new URL(request.url);
  const path = url.pathname;
  if (request.method === 'POST' && path === '/check-batch') {
    const auth = request.headers.get('authorization') || '';
    if (!env.SLAVE_TOKEN || auth !== `Bearer ${env.SLAVE_TOKEN}`) return textResponse('Unauthorized', 401);

    let payload;
    try { payload = await request.json(); } catch (e) { return textResponse('Invalid JSON', 400); }
    if (!payload || !Array.isArray(payload.batch) || !payload.batchId) return textResponse('Bad payload', 400);

    let enrichedBatch = payload.batch;
    // ... (GeoIP enrichment omitted for brevity, no changes needed here)

    const results = await Promise.all(enrichedBatch.map(p => checkProxy(p, env)));
    ctx.waitUntil(postResultsToMaster(results, payload.batchId, env));
    ctx.waitUntil(updateSupportStats(request, payload.batch.length, env));
    return jsonResponse({ ok: true, message: `Processed ${payload.batch.length} proxies.` });
  }
  return textResponse('Support worker is ready.', 200);
}

async function checkProxy(proxy, env) {
  const { ip, port } = proxy;
  const isAlive = Math.random() > 0.3;
  const latency = isAlive ? Math.floor(Math.random() * (1500 - 50 + 1)) + 50 : null;
  return { proxy: `${ip}:${port}`, status: isAlive ? 'alive' : 'dead', latency, country: proxy.country || 'XX', isp: proxy.isp || 'Unknown ISP', checked_by: 'support-worker-v2' };
}

async function postResultsToMaster(results, batchId, env) {
  if (!env.MASTER_ENDPOINT) return;
  try {
    await fetch(`${env.MASTER_ENDPOINT.replace(/\/$/,'')}/submit-results`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${env.MASTER_TOKEN || ''}` },
      body: JSON.stringify({ results, batchId })
    });
  } catch (e) {
    console.error('Error submitting results to master:', e);
  }
}

async function updateSupportStats(request, processedCount, env) {
  const supportUrl = new URL(request.url).origin;
  if (!env.STATS_REPORTING_ENDPOINT || !env.EXTERNAL_STATS_TOKEN) return;
  const statsPayload = { support_url: supportUrl, status: 'alive', total_requests_increment: processedCount, last_seen: new Date().toISOString() };
  try {
    await fetch(env.STATS_REPORTING_ENDPOINT, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${env.EXTERNAL_STATS_TOKEN}` },
      body: JSON.stringify(statsPayload)
    });
  } catch(e) {
    console.error(`Error reporting stats via API for ${supportUrl}:`, e);
  }
}

function convertJsonToCsv(data) {
  // ... (omitted for brevity, no changes needed here)
}
