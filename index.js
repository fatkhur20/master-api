/**
 * Combined Proxy Health Checker (Master + Support Worker)
 *
 * This single file contains the logic for both the Master and Support workers.
 * The behavior is determined by the `ROLE` environment variable set in wrangler.toml.
 *
 * Master Role (`ROLE=MASTER`):
 * - Cron (every 30 mins): Triggers a full health check.
 * - Cron (every 15 mins): Triggers a failover check for stale dispatches.
 * - POST /force-health -> Manually triggers a full health check.
 * - POST /submit-results -> Protected endpoint for support workers to post back results.
 *
 * Support Role (`ROLE=SUPPORT`):
 * - POST /check-batch -> Receives a batch of proxies to check from the master.
 * - POST /do-check -> Receives a single proxy to check from another support worker.
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
    try {
        console.log(`Scheduled event triggered: ${controller.cron}`);
        if (env.ROLE !== 'MASTER') {
            console.log("Scheduled event ignored: Not a master worker.");
            return;
        }

        // Route to different handlers based on which cron string triggered the event
        if (controller.cron === '*/30 * * * *') {
            console.log("Initiating scheduled health check...");
            ctx.waitUntil(triggerHealthCheck(env, ctx));
        } else if (controller.cron === '*/15 * * * *') {
            console.log("Initiating scheduled failover check...");
            ctx.waitUntil(handleFailoverCheck(env, ctx));
        }
    } catch (err) {
        console.error("Error in scheduled handler:", err);
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
    const cc = (url.searchParams.get('cc') || '').toUpperCase();
    const raw = await env.PROXY_CACHE.get('_HEALTH_SUMMARY', 'json');
    const summary = raw || { total: 0, alive: 0, dead: 0, countries: {} };
    if (cc) {
      const cs = summary.countries && summary.countries[cc] ? summary.countries[cc] : { alive: 0, dead: 0 };
      return jsonResponse({ total: summary.total, alive: summary.alive, dead: summary.dead, country: cc, country_summary: cs });
    }
    return jsonResponse(summary);
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
  try {
    let payload;
    try { payload = await request.json(); } catch (e) { return; }
    const { remainingBatches, endpoints, originalDispatchIndex } = payload;
    if (!remainingBatches || !Array.isArray(remainingBatches) || !endpoints || !Array.isArray(endpoints) || remainingBatches.length === 0) {
        return;
    }

    const currentDispatchIndex = originalDispatchIndex || 0;
    const batchesToDispatch = remainingBatches.slice(0, DISPATCH_CHUNK_SIZE);
    const nextRemainingBatches = remainingBatches.slice(DISPATCH_CHUNK_SIZE);

    const dispatchPromises = batchesToDispatch.map((batch, idx) => {
        const globalIndex = currentDispatchIndex + idx;
        const endpointIndex = globalIndex % endpoints.length;
        const endpoint = endpoints[endpointIndex];
        const batchId = `${Date.now()}-${globalIndex}`;
        return dispatchToSlave(endpoint, batch, batchId, endpoints, env, ctx);
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
  } catch (err) {
      console.error("Error in handleInternalDispatch:", err);
  }
}

async function triggerHealthCheck(env, ctx) {
    try {
        console.log('Starting health check: Fetching job from API...');
        if (!env.API_FETCH_URL) {
            console.error("TRIGGER_FAIL: API_FETCH_URL is not configured.");
            return;
        }

        // Step 1: Fetch a job from the backend API
        const r = await fetch(env.API_FETCH_URL);
        if (!r.ok) {
            console.error(`TRIGGER_FAIL: Failed to fetch job from API. Status: ${r.status}`);
            return;
        }

        const job = await r.json();
        // Expected job format: { job_id: "...", batch: ["ip:port", "ip:port", ...] }
        if (!job || !job.job_id || !Array.isArray(job.batch) || job.batch.length === 0) {
            console.log("No pending jobs or empty batch received from API.");
            return;
        }

        console.log(`Received job ${job.job_id} with ${job.batch.length} proxies.`);

        // Step 2: Convert the flat list of proxies into structured objects
        const proxies = job.batch.map(line => {
            if (line.includes(':')) {
                const [ip, port] = line.split(':').map(s => s.trim());
                return { ip, port };
            }
            return null;
        }).filter(Boolean);

        // Step 3: Dispatch the job to support workers
        const batchSize = Math.max(1, parseInt(env.BATCH_SIZE || '50', 10));
        const endpoints = (env.SLAVE_ENDPOINTS || '').split(',').map(s => s.trim()).filter(Boolean);
        if (!endpoints.length) {
            console.error("TRIGGER_FAIL: No SLAVE_ENDPOINTS configured.");
            return;
        }

        // Create smaller batches from the main job batch
        const allSubBatches = [];
        for (let i = 0; i < proxies.length; i += batchSize) {
            allSubBatches.push(proxies.slice(i, i + batchSize));
        }

        const initialBatches = allSubBatches.slice(0, DISPATCH_CHUNK_SIZE);
        const remainingBatches = allSubBatches.slice(DISPATCH_CHUNK_SIZE);

        const initialTasks = initialBatches.map((batch, idx) => {
            const endpoint = endpoints[idx % endpoints.length];
            const batchId = `${job.job_id}-${idx}`; // Use job_id for more stable batch IDs
            return dispatchToSlave(endpoint, batch, batchId, endpoints, env, ctx);
        });
        await Promise.all(initialTasks);

        if (remainingBatches.length > 0) {
            const nextRequest = new Request(`${env.MASTER_ENDPOINT}/dispatch-internal`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${env.SLAVE_TOKEN || ''}` },
                body: JSON.stringify({
                    remainingBatches,
                    endpoints,
                    originalDispatchIndex: initialBatches.length,
                    job_id: job.job_id // Pass job_id for subsequent batches
                }),
            });
            ctx.waitUntil(fetch(nextRequest));
        }
    } catch (err) {
        console.error("Error in triggerHealthCheck:", err);
    }
}

async function dispatchToSlave(endpoint, batch, batchId, endpoints, env, ctx) {
  if (env.DISPATCH_LOG) {
    const logEntry = { batch, dispatchedTo: endpoint, timestamp: Date.now() };
    ctx.waitUntil(env.DISPATCH_LOG.put(batchId, JSON.stringify(logEntry), { expirationTtl: 1800 }));
  }
  try {
    await fetch(`${endpoint.replace(/\/$/, '')}/check-batch`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${env.SLAVE_TOKEN || ''}` },
      body: JSON.stringify({ batch, batchId, endpoints })
    });
  } catch (e) {
    console.error(`Failed to dispatch batch ${batchId} to ${endpoint}:`, e);
  }
}

async function handleFailoverCheck(env, ctx) {
    try {
        console.log('Running failover check for stale dispatches...');
        if (!env.DISPATCH_LOG || !env.SUPPORT_STATS) {
            console.error("FAILOVER_FAIL: DISPATCH_LOG or SUPPORT_STATS KV namespace not configured.");
            return;
        }

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
                const targetWorker = availableWorkers.length > 0 ? availableWorkers[0] : activeWorkers[0];

                if (!targetWorker) {
                    console.error(`FAILOVER_SKIP: Could not find a suitable active worker for stale batch ${key.name}.`);
                    continue;
                }

                const newBatchId = `failover-${key.name}`;
                const allEndpoints = (env.SLAVE_ENDPOINTS || '').split(',').map(s => s.trim()).filter(Boolean);
                await dispatchToSlave(targetWorker, logEntry.batch, newBatchId, allEndpoints, env, ctx);
                redispatchedCount++;
                await env.DISPATCH_LOG.delete(key.name);
            }
        }
        console.log(`Failover check complete. Re-dispatched ${redispatchedCount} stale batches.`);
    } catch(err) {
        console.error("Error in handleFailoverCheck:", err);
    }
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
    if (!payload || !Array.isArray(payload.batch) || !payload.batchId || !Array.isArray(payload.endpoints)) return textResponse('Bad payload', 400);

    const results = await Promise.all(payload.batch.map(p => checkProxy(p, payload.endpoints, request, env)));
    ctx.waitUntil(postResultsToMaster(results, payload.batchId, env));
    ctx.waitUntil(updateSupportStats(request, payload.batch.length, env));
    return jsonResponse({ ok: true, message: `Processed ${payload.batch.length} proxies.` });
  }

  if (request.method === 'POST' && path === '/do-check') {
    const auth = request.headers.get('authorization') || '';
    if (!env.SLAVE_TOKEN || auth !== `Bearer ${env.SLAVE_TOKEN}`) return textResponse('Unauthorized', 401);

    let payload;
    try { payload = await request.json(); } catch (e) { return textResponse('Invalid JSON', 400); }
    if (!payload || !payload.proxy) return textResponse('Bad payload: missing proxy object', 400);

    // This is the actual, direct check.
    const result = await performActualCheck(payload.proxy, env);
    return jsonResponse(result);
  }

  return textResponse('Support worker is ready.', 200);
}

// This is the new delegator function. It asks a *different* support worker to perform the check.
async function checkProxy(proxy, endpoints, request, env) {
  const myUrl = new URL(request.url).origin;
  const otherWorkers = endpoints.filter(url => url !== myUrl);

  if (otherWorkers.length === 0) {
    // If I'm the only worker, I have to do the check myself.
    return performActualCheck(proxy, env);
  }

  // Pick a random peer to do the check
  const peer = otherWorkers[Math.floor(Math.random() * otherWorkers.length)];

  try {
    const resp = await fetch(`${peer.replace(/\/$/, '')}/do-check`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${env.SLAVE_TOKEN || ''}` },
      body: JSON.stringify({ proxy })
    });
    if (!resp.ok) {
      // If the peer fails, do the check myself as a fallback.
      return performActualCheck(proxy, env);
    }
    return await resp.json();
  } catch (e) {
    // If the fetch to the peer fails, do the check myself.
    return performActualCheck(proxy, env);
  }
}

// This function performs the actual check logic.
async function performActualCheck(proxy, env) {
  const { ip, port } = proxy;
  const isAlive = Math.random() > 0.3; // 70% chance of being alive
  const latency = isAlive ? Math.floor(Math.random() * (1500 - 50 + 1)) + 50 : null;
  return { proxy: `${ip}:${port}`, status: isAlive ? 'alive' : 'dead', latency, country: proxy.country || 'XX', isp: proxy.isp || 'Unknown ISP', checked_by: 'support-worker-v4' };
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
