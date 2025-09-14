/**
 * Combined Proxy Health Checker (Master + Support Worker)
 *
 * This single file contains the logic for both the Master and Support workers.
 * The behavior is determined by the `ROLE` environment variable set in wrangler.toml.
 *
 * Master Role (`ROLE=MASTER`):
 * - POST /force-health -> fetch proxy list and dispatch jobs to support workers
 * - POST /submit-results -> protected endpoint for support workers to post back results
 * - GET /health -> summary snapshot from KV (_HEALTH_SUMMARY)
 * - GET /health/download/all -> download all results
 * - GET /stats -> view status of support workers
 *
 * Support Role (`ROLE=SUPPORT`):
 * - POST /check-batch -> receives a batch of proxies to check from the master
 * - (Internally, it checks proxies and posts results back to the master's /submit-results)
 */

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

    // Route traffic based on the ROLE environment variable
    try {
      if (env.ROLE === 'MASTER') {
        return await handleMasterRequest(request, env, ctx);
      } else if (env.ROLE === 'SUPPORT') {
        return await handleSupportRequest(request, env, ctx);
      } else {
        console.error('FATAL: ROLE environment variable is not set. Worker does not know how to behave.');
        return textResponse('Configuration error: ROLE not set.', 500);
      }
    } catch (err) {
      console.error(`Error in ${env.ROLE || 'UNKNOWN'} role:`, err);
      return textResponse(String(err), 500);
    }
  }
};


// ===================================================================================
//  MASTER WORKER LOGIC
// ===================================================================================

async function handleMasterRequest(request, env, ctx) {
  const url = new URL(request.url);
  const path = url.pathname;

  // POST /force-health -> fetch proxy list and dispatch jobs to support workers
  if (request.method === 'POST' && path === '/force-health') {
    const auth = request.headers.get('authorization') || '';
    if (env.FORCE_TOKEN && auth !== `Bearer ${env.FORCE_TOKEN}`) return textResponse('Unauthorized', 401);
    return await triggerHealthCheck(env);
  }

  // POST /submit-results -> slave posts results back
  if (request.method === 'POST' && path === '/submit-results') {
    const auth = request.headers.get('authorization') || '';
    if (!env.MASTER_TOKEN || auth !== `Bearer ${env.MASTER_TOKEN}`) return textResponse('Unauthorized', 401);
    let payload;
    try { payload = await request.json(); } catch (e) { return textResponse('Invalid JSON', 400); }
    if (!payload || !Array.isArray(payload.results)) return textResponse('Bad payload', 400);

    const validResults = payload.results.filter(r => r && r.proxy);
    const keys = validResults.map(r => r.proxy);

    // Fetch previous records in parallel
    const prevValuesRaw = await Promise.all(keys.map(key => env.PROXY_CACHE.get(key)));
    const prevValues = prevValuesRaw.map(v => {
      try { return v ? JSON.parse(v) : null; } catch (e) { console.warn('failed to parse prevValue', v); return null; }
    });

    // Store new records in parallel
    const putPromises = validResults.map((rec, i) => {
      return env.PROXY_CACHE.put(keys[i], JSON.stringify(rec)).catch(e => console.error('KV put failed:', e));
    });
    await Promise.all(putPromises);

    // Update summary in a single batch operation
    try {
      await updateSummaryBatch(env, prevValues, validResults);
    } catch (e) {
      console.error('Failed to update summary:', e);
    }

    return jsonResponse({ ok: true, stored: validResults.length });
  }

  // GET /health -> summary, with optional force trigger
  if (request.method === 'GET' && path === '/health') {
    // Check if a force health check is triggered via query param
    if (url.searchParams.get('FORCE_TOKEN')) {
      if (env.FORCE_TOKEN && url.searchParams.get('FORCE_TOKEN') === env.FORCE_TOKEN) {
        // Run the health check in the background and return a message
        ctx.waitUntil(triggerHealthCheck(env));
        return jsonResponse({ ok: true, message: 'Health check process triggered in the background.' });
      } else {
        return textResponse('Unauthorized', 401);
      }
    }

    const cc = (url.searchParams.get('cc') || '').toUpperCase();
    const raw = await env.PROXY_CACHE.get('_HEALTH_SUMMARY', 'json');
    const summary = raw || { total: 0, alive: 0, dead: 0, countries: {} };
    if (cc) {
      const cs = summary.countries && summary.countries[cc] ? summary.countries[cc] : { alive: 0, dead: 0 };
      return jsonResponse({ total: summary.total, alive: summary.alive, dead: summary.dead, country: cc, country_summary: cs });
    }
    return jsonResponse(summary);
  }

  // GET /health/download/all?format=json|csv
  if (request.method === 'GET' && path === '/health/download/all') {
    const format = (url.searchParams.get('format') || 'json').toLowerCase();
    if (format !== 'json' && format !== 'csv') {
      return textResponse('Invalid format. Use "json" or "csv".', 400);
    }
    const cacheKey = `_HEALTH_DUMP_ALL_${format.toUpperCase()}`;
    const cached = await env.PROXY_CACHE.get(cacheKey);
    if (cached) {
      const headers = format === 'csv'
        ? { 'Content-Type': 'text/csv', 'Content-Disposition': 'attachment; filename="proxy_health_all.csv"', ...CORS }
        : { 'Content-Type': 'application/json', 'Content-Disposition': 'attachment; filename="proxy_health_all.json"', ...CORS };
      return new Response(cached, { headers });
    }

    // Fetch all data from KV
    const out = [];
    let cursor = undefined;
    do {
      const page = await env.PROXY_CACHE.list({ cursor, limit: 1000 });
      cursor = page.cursor;
      for (const k of page.keys) {
        if (k.name.startsWith('_')) continue;
        const v = await env.PROXY_CACHE.get(k.name, 'json');
        if (v) out.push(v);
      }
    } while (cursor);

    let payload;
    let headers;

    if (format === 'csv') {
      payload = convertJsonToCsv(out);
      headers = { 'Content-Type': 'text/csv', 'Content-Disposition': 'attachment; filename="proxy_health_all.csv"', ...CORS };
    } else {
      payload = JSON.stringify(out, null, 2);
      headers = { 'Content-Type': 'application/json', 'Content-Disposition': 'attachment; filename="proxy_health_all.json"', ...CORS };
    }

    ctx.waitUntil(env.PROXY_CACHE.put(cacheKey, payload, { expirationTtl: 60 }));
    return new Response(payload, { headers });
  }

  // GET /health/download/country?cc=XX
  if (request.method === 'GET' && path === '/health/download/country') {
    const cc = (url.searchParams.get('cc') || '').toUpperCase();
    if (!cc) return textResponse('missing cc', 400);
    const cacheKey = `_HEALTH_DUMP_CC_${cc}`;
    const cached = await env.PROXY_CACHE.get(cacheKey);
    if (cached) return new Response(cached, { headers: { 'Content-Type': 'application/json', 'Content-Disposition': `attachment; filename="proxy_health_${cc}.json"`, ...CORS } });
    const out = [];
    let cursor = undefined;
    do {
      const page = await env.PROXY_CACHE.list({ cursor, limit: 1000 });
      cursor = page.cursor;
      for (const k of page.keys) {
        if (k.name.startsWith('_')) continue;
        const v = await env.PROXY_CACHE.get(k.name, 'json');
        if (v && v.country && v.country.toUpperCase() === cc) out.push(v);
      }
    } while (cursor);
    const payload = JSON.stringify(out, null, 2);
    await env.PROXY_CACHE.put(cacheKey, payload, { expirationTtl: 60 });
    return new Response(payload, { headers: { 'Content-Type': 'application/json', 'Content-Disposition': `attachment; filename="proxy_health_${cc}.json"`, ...CORS } });
  }

  // POST /report-stats -> external workers post their stats
  if (request.method === 'POST' && path === '/report-stats') {
    const auth = request.headers.get('authorization') || '';
    if (!env.EXTERNAL_STATS_TOKEN || auth !== `Bearer ${env.EXTERNAL_STATS_TOKEN}`) {
      return textResponse('Unauthorized', 401);
    }
    if (!env.SUPPORT_STATS) {
      return textResponse('SUPPORT_STATS KV namespace not configured on master', 501);
    }

    let stats;
    try { stats = await request.json(); } catch(e) { return textResponse('Invalid JSON', 400); }

    // The payload should contain its own identifier, e.g., support_url
    if (!stats || !stats.support_url) {
      return textResponse('Bad payload: missing support_url', 400);
    }

    // The key is the URL of the support worker
    const key = stats.support_url;

    // Perform a read-modify-write to handle increments safely
    const prevStats = await env.SUPPORT_STATS.get(key, 'json') || {
      support_url: key,
      total_requests: 0,
    };

    const newStats = {
      ...prevStats,
      status: stats.status || 'alive',
      total_requests: (prevStats.total_requests || 0) + (stats.total_requests_increment || 0),
      last_seen: stats.last_seen || new Date().toISOString(),
    };

    await env.SUPPORT_STATS.put(key, JSON.stringify(newStats), { expirationTtl: 180 });

    return jsonResponse({ ok: true, message: `Stats received and updated for ${key}`});
  }

  // POST /lookup-geoip -> support workers ask master for geoip data
  if (request.method === 'POST' && path === '/lookup-geoip') {
    const auth = request.headers.get('authorization') || '';
    // Re-use SLAVE_TOKEN for this internal M2M communication
    if (!env.SLAVE_TOKEN || auth !== `Bearer ${env.SLAVE_TOKEN}`) {
      return textResponse('Unauthorized', 401);
    }
    if (!env.GEO_CACHE) {
      return textResponse('GEO_CACHE KV namespace not configured on master', 501);
    }

    let payload;
    try { payload = await request.json(); } catch(e) { return textResponse('Invalid JSON', 400); }
    if (!payload || !Array.isArray(payload.proxies)) return textResponse('Bad payload', 400);

    const proxiesToLookup = payload.proxies;
    const results = {};

    // First, check cache for existing entries
    const cachedResults = await Promise.all(proxiesToLookup.map(p => env.GEO_CACHE.get(p, 'json')));

    const proxiesStillNeedingLookup = [];
    cachedResults.forEach((cached, i) => {
      if (cached) {
        results[proxiesToLookup[i]] = cached;
      } else {
        proxiesStillNeedingLookup.push(proxiesToLookup[i]);
      }
    });

    // For proxies not in cache, perform simulated lookup
    if (proxiesStillNeedingLookup.length > 0) {
      const newLookups = {};
      const putPromises = [];
      for (const proxy of proxiesStillNeedingLookup) {
        // --- SIMULATED GEOIP LOOKUP ---
        const geoData = {
          country: 'SIM', // Simulated Country
          isp: 'Simulated ISP',
        };
        // --- END SIMULATION ---

        newLookups[proxy] = geoData;
        results[proxy] = geoData;
        putPromises.push(env.GEO_CACHE.put(proxy, JSON.stringify(geoData)));
      }
      // Store new results in cache without waiting for it to complete
      ctx.waitUntil(Promise.all(putPromises));
    }

    return jsonResponse(results);
  }

  // GET /stats -> view status of support workers
  if (request.method === 'GET' && path === '/stats') {
    if (!env.SUPPORT_STATS) {
      return textResponse('SUPPORT_STATS KV namespace not configured', 501);
    }
    const { keys } = await env.SUPPORT_STATS.list();
    const values = await Promise.all(keys.map(k => env.SUPPORT_STATS.get(k.name, 'json')));
    return jsonResponse(values.filter(Boolean));
  }

  return jsonResponse({ ok: true, message: 'Master is ready. POST /force-health to dispatch.' });
}

async function triggerHealthCheck(env) {
  console.log('Starting forced health check...');
  if (!env.PROXY_LIST_URL) {
    console.error('TRIGGER_FAIL: PROXY_LIST_URL not configured');
    return textResponse('PROXY_LIST_URL not configured', 500);
  }
  const r = await fetch(env.PROXY_LIST_URL);
  if (!r.ok) {
    console.error('TRIGGER_FAIL: Failed to fetch proxy list');
    return textResponse('Failed to fetch proxy list', 502);
  }
  const text = await r.text();
  const lines = text.split('\n').map(l => l.trim()).filter(Boolean);
  if (!lines.length) {
    console.warn('TRIGGER_WARN: No proxies found in list.');
    return textResponse('No proxies found', 400);
  }

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
  if (!endpoints.length) {
    console.error('TRIGGER_FAIL: No SLAVE_ENDPOINTS configured');
    return textResponse('No SLAVE_ENDPOINTS configured', 500);
  }

  // create batches and dispatch round-robin
  const batches = [];
  for (let i = 0; i < proxies.length; i += batchSize) batches.push(proxies.slice(i, i + batchSize));

  console.log(`Dispatching ${proxies.length} proxies in ${batches.length} batches to ${endpoints.length} support workers.`);
  const tasks = batches.map((batch, idx) => {
    const endpoint = endpoints[idx % endpoints.length].replace(/\/$/, '');
    return dispatchToSlave(endpoint, batch, env);
  });

  const results = await Promise.all(tasks);
  const assigned = results.filter(r => r.ok).length;
  console.log(`Dispatch complete. ${assigned} of ${batches.length} batches assigned successfully.`);
  return jsonResponse({ message: 'dispatched', totalProxies: proxies.length, batches: batches.length, endpoints: endpoints.length, assigned, results });
}

async function dispatchToSlave(endpoint, batch, env) {
  try {
    const resp = await fetch(`${endpoint.replace(/\/$/, '')}/check-batch`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${env.SLAVE_TOKEN || ''}` },
      body: JSON.stringify({ batch })
    });
    if (!resp.ok) {
      return { ok: false, status: resp.status, text: await resp.text(), endpoint };
    }
    const j = await resp.json();
    return { ok: true, endpoint, result: j };
  } catch (e) {
    return { ok: false, error: String(e), endpoint };
  }
}

async function updateSummaryBatch(env, prevResults, currResults) {
  if (currResults.length === 0) return;

  let summary;
  try {
    summary = await env.PROXY_CACHE.get('_HEALTH_SUMMARY', 'json');
  } catch (e) {
    console.warn('Could not parse _HEALTH_SUMMARY, rebuilding from scratch.', e);
  }
  if (!summary) summary = { total: 0, alive: 0, dead: 0, countries: {} };

  const changes = {
    alive: 0,
    dead: 0,
    countries: {}
  };

  for (const prev of prevResults) {
    if (!prev || !prev.country) continue;
    const cc = prev.country.toUpperCase();
    changes.countries[cc] = changes.countries[cc] || { alive: 0, dead: 0 };
    if (prev.status === 'alive') {
      changes.alive--;
      changes.countries[cc].alive--;
    } else {
      changes.dead--;
      changes.countries[cc].dead--;
    }
  }

  for (const curr of currResults) {
    if (!curr || !curr.country) continue;
    const cc = curr.country.toUpperCase();
    changes.countries[cc] = changes.countries[cc] || { alive: 0, dead: 0 };
    if (curr.status === 'alive') {
      changes.alive++;
      changes.countries[cc].alive++;
    } else {
      changes.dead++;
      changes.countries[cc].dead++;
    }
  }

  summary.alive = (summary.alive || 0) + changes.alive;
  summary.dead = (summary.dead || 0) + changes.dead;
  summary.total = summary.alive + summary.dead;

  for (const [cc, c_changes] of Object.entries(changes.countries)) {
    summary.countries[cc] = summary.countries[cc] || { alive: 0, dead: 0 };
    summary.countries[cc].alive = (summary.countries[cc].alive || 0) + c_changes.alive;
    summary.countries[cc].dead = (summary.countries[cc].dead || 0) + c_changes.dead;
  }

  summary.alive = Math.max(0, summary.alive);
  summary.dead = Math.max(0, summary.dead);
  Object.values(summary.countries).forEach(c => {
    c.alive = Math.max(0, c.alive);
    c.dead = Math.max(0, c.dead);
  });

  try {
    await env.PROXY_CACHE.put('_HEALTH_SUMMARY', JSON.stringify(summary));
  } catch (e) {
    console.error('Failed to PUT updated summary', e);
  }
}


// ===================================================================================
//  SUPPORT WORKER LOGIC
// ===================================================================================

async function handleSupportRequest(request, env, ctx) {
  const url = new URL(request.url);
  const path = url.pathname;

  // POST /check-batch -> Master sends a batch of proxies to be checked
  if (request.method === 'POST' && path === '/check-batch') {
    // 1. Authenticate the request from the master
    const auth = request.headers.get('authorization') || '';
    if (!env.SLAVE_TOKEN || auth !== `Bearer ${env.SLAVE_TOKEN}`) {
      return textResponse('Unauthorized', 401);
    }

    // 2. Parse the JSON payload
    let payload;
    try {
      payload = await request.json();
    } catch (e) {
      return textResponse('Invalid JSON', 400);
    }
    if (!payload || !Array.isArray(payload.batch)) {
      return textResponse('Bad payload', 400);
    }

    // 3. Enrich proxies with GeoIP data from the master
    let enrichedBatch = payload.batch; // Start with the original batch
    const proxiesToLookup = payload.batch.filter(p => p && p.ip && !p.country);

    if (proxiesToLookup.length > 0) {
      try {
        const geoResponse = await fetch(`${env.MASTER_ENDPOINT.replace(/\/$/,'')}/lookup-geoip`, {
          method: 'POST',
          headers: { 'Content-Type':'application/json', 'Authorization': `Bearer ${env.SLAVE_TOKEN || ''}` },
          body: JSON.stringify({ proxies: proxiesToLookup.map(p => `${p.ip}:${p.port}`) })
        });

        if (geoResponse.ok) {
          const geoResults = await geoResponse.json();
          // Create a new, enriched batch instead of mutating in place
          enrichedBatch = payload.batch.map(p => {
            const key = `${p.ip}:${p.port}`;
            if (geoResults[key]) {
              // Return a new object with the original proxy data plus the new geo data
              return { ...p, ...geoResults[key] };
            }
            return p; // Return the original object if no new data
          });
        }
      } catch (e) {
        console.error('Failed to fetch GeoIP data from master:', e);
      }
    }

    // 4. Perform health checks on the (potentially) enriched batch
    const results = await Promise.all(enrichedBatch.map(p => checkProxy(p, env)));

    // 4. Post results back to master asynchronously
    ctx.waitUntil(postResultsToMaster(results, env));

    // 5. Update this worker's own stats in SUPPORT_STATS KV
    ctx.waitUntil(updateSupportStats(request, payload.batch.length, env));

    return jsonResponse({ ok: true, message: `Processed ${payload.batch.length} proxies. Results sent to master.` });
  }

  return textResponse('Support worker is ready. Awaiting batches.', 200);
}

/**
 * Simulates a health check for a single proxy.
 * In a real implementation, this would involve a TCP connection or similar.
 */
async function checkProxy(proxy, env) {
  const { ip, port } = proxy;
  const start = Date.now();

  // In a real worker, you would use sockets or other APIs to check the proxy.
  // Here, we just simulate a result.
  const isAlive = Math.random() > 0.3; // 70% chance of being alive
  const latency = Math.floor(Math.random() * (1500 - 50 + 1)) + 50; // Random latency 50-1500ms

  // GeoIP lookup is now handled by the master before this function is called.

  return {
    proxy: `${ip}:${port}`,
    status: isAlive ? 'alive' : 'dead',
    latency: isAlive ? latency : null,
    country: proxy.country || 'XX', // Use existing country or a default
    isp: proxy.isp || 'Unknown ISP',
    checked_by: 'support-worker-v1' // Example metadata
  };
}

/**
 * Posts the collected results back to the master worker.
 */
async function postResultsToMaster(results, env) {
  if (!env.MASTER_ENDPOINT) {
    console.error('MASTER_ENDPOINT is not configured on support worker. Cannot send results.');
    return;
  }
  try {
    const resp = await fetch(`${env.MASTER_ENDPOINT.replace(/\/$/,'')}/submit-results`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${env.MASTER_TOKEN || ''}`
      },
      body: JSON.stringify({ results })
    });
    if (!resp.ok) {
      console.error(`Failed to submit results to master. Status: ${resp.status}`, await resp.text());
    } else {
      console.log(`Successfully submitted ${results.length} results to master.`);
    }
  } catch (e) {
    console.error('Error submitting results to master:', e);
  }
}

/**
 * Updates the stats for this support worker by reporting to the master worker via API.
 */
async function updateSupportStats(request, processedCount, env) {
  const supportUrl = new URL(request.url).origin;

  if (!env.STATS_REPORTING_ENDPOINT || !env.EXTERNAL_STATS_TOKEN) {
    console.error('Support worker is not configured to report stats. Missing STATS_REPORTING_ENDPOINT or EXTERNAL_STATS_TOKEN.');
    return;
  }

  const statsPayload = {
    support_url: supportUrl,
    status: 'alive',
    total_requests_increment: processedCount,
    last_seen: new Date().toISOString(),
  };

  try {
    const resp = await fetch(env.STATS_REPORTING_ENDPOINT, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${env.EXTERNAL_STATS_TOKEN}`
      },
      body: JSON.stringify(statsPayload)
    });
    if (!resp.ok) {
      console.error(`Failed to report stats via API. Status: ${resp.status}`, await resp.text());
    }
  } catch(e) {
    console.error(`Error reporting stats via API for ${supportUrl}:`, e);
  }
}

/**
 * Converts an array of flat JSON objects into a CSV string.
 */
function convertJsonToCsv(data) {
  if (!data || data.length === 0) {
    return "";
  }

  const headers = Object.keys(data[0]);
  const replacer = (key, value) => value === null ? '' : value;

  const csv = [
    headers.join(','), // header row
    ...data.map(row =>
      headers.map(fieldName =>
        JSON.stringify(row[fieldName], replacer)
      ).join(',')
    )
  ].join('\r\n');

  return csv;
}
