/**
 * proxy-health-master (Master Worker)
 * - POST /force-health     -> split proxy list and dispatch to support workers
 * - POST /submit-results   -> protected endpoint for support workers to post back results
 * - GET  /health           -> summary snapshot from KV (_HEALTH_SUMMARY)
 * - GET  /health/download/all
 * - GET  /health/download/country?cc=XX
 *
 * ENV / Bindings:
 * - PROXY_CACHE (KV)
 * - GEO_CACHE (KV) optional
 * - SLAVE_ENDPOINTS (env): comma-separated support worker URLs
 * - SLAVE_TOKEN (env): shared secret for master->slave calls
 * - MASTER_TOKEN (env): secret support uses to post back to master (/submit-results)
 * - PROXY_LIST_URL (env)
 * - BATCH_SIZE, HEALTH_CHECK_TIMEOUT, FORCE_TOKEN
 */

const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET,POST,OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type,Authorization',
};

function jsonResponse(obj, status=200) {
  return new Response(JSON.stringify(obj, null, 2), { status, headers: { 'Content-Type': 'application/json', ...CORS } });
}
function textResponse(text, status=200) {
  return new Response(text, { status, headers: CORS });
}

export default {
  async fetch(request, env) {
    if (request.method === 'OPTIONS') return new Response(null, { headers: CORS });
    try {
      const url = new URL(request.url);
      const path = url.pathname;

      // POST /force-health -> fetch proxy list and dispatch jobs to support workers
      if (request.method === 'POST' && path === '/force-health') {
        const auth = request.headers.get('authorization') || '';
        if (env.FORCE_TOKEN && auth !== `Bearer ${env.FORCE_TOKEN}`) return textResponse('Unauthorized', 401);

        if (!env.PROXY_LIST_URL) return textResponse('PROXY_LIST_URL not configured', 500);
        const r = await fetch(env.PROXY_LIST_URL);
        if (!r.ok) return textResponse('Failed to fetch proxy list', 502);
        const text = await r.text();
        const lines = text.split('\n').map(l => l.trim()).filter(Boolean);
        if (!lines.length) return textResponse('No proxies found', 400);

        const proxies = lines.map(line => {
          if (line.includes(',')) {
            const p = line.split(',').map(s=>s.trim());
            return { ip: p[0], port: p[1], country: p[2]||null, isp: p[3]||null };
          } else if (line.includes(':')) {
            const [ip, port] = line.split(':').map(s=>s.trim());
            return { ip, port };
          }
          return null;
        }).filter(Boolean);

        const batchSize = Math.max(1, parseInt(env.BATCH_SIZE || '50', 10));
        const endpoints = (env.SLAVE_ENDPOINTS||'').split(',').map(s=>s.trim()).filter(Boolean);
        if (!endpoints.length) return textResponse('No SLAVE_ENDPOINTS configured', 500);

        // create batches and dispatch round-robin
        const batches = [];
        for (let i=0;i<proxies.length;i+=batchSize) batches.push(proxies.slice(i,i+batchSize));

        const tasks = batches.map((batch, idx) => {
          const endpoint = endpoints[idx % endpoints.length].replace(/\/$/,'');
          return dispatchToSlave(endpoint, batch, env);
        });

        const results = await Promise.all(tasks);
        const assigned = results.filter(r=>r.ok).length;
        return jsonResponse({ message:'dispatched', totalProxies: proxies.length, batches: batches.length, endpoints: endpoints.length, assigned, results });
      }

      // POST /submit-results -> slave posts results back
      if (request.method === 'POST' && path === '/submit-results') {
        const auth = request.headers.get('authorization') || '';
        if (!env.MASTER_TOKEN || auth !== `Bearer ${env.MASTER_TOKEN}`) return textResponse('Unauthorized', 401);
        let payload;
        try { payload = await request.json(); } catch(e){ return textResponse('Invalid JSON',400); }
        if (!payload || !Array.isArray(payload.results)) return textResponse('Bad payload', 400);

        const validResults = payload.results.filter(r => r && r.proxy);
        const keys = validResults.map(r => r.proxy);

        // Fetch previous records in parallel
        const prevValuesRaw = await Promise.all(keys.map(key => env.PROXY_CACHE.get(key)));
        const prevValues = prevValuesRaw.map(v => {
          try { return v ? JSON.parse(v) : null; } catch(e) { console.warn('failed to parse prevValue',v); return null; }
        });

        // Store new records in parallel
        const putPromises = validResults.map((rec, i) => {
          return env.PROXY_CACHE.put(keys[i], JSON.stringify(rec)).catch(e => console.error('KV put failed:', e));
        });
        await Promise.all(putPromises);

        // Update summary in a single batch operation
        try {
          await updateSummaryBatch(env, prevValues, validResults);
        } catch(e) {
          console.error('Failed to update summary:', e);
        }

        return jsonResponse({ ok:true, stored: validResults.length });
      }

      // GET /health -> summary
      if (request.method === 'GET' && path === '/health') {
        const cc = (url.searchParams.get('cc')||'').toUpperCase();
        const raw = await env.PROXY_CACHE.get('_HEALTH_SUMMARY', 'json');
        const summary = raw || { total:0, alive:0, dead:0, countries:{} };
        if (cc) {
          const cs = summary.countries && summary.countries[cc] ? summary.countries[cc] : { alive:0, dead:0 };
          return jsonResponse({ total: summary.total, alive: summary.alive, dead: summary.dead, country: cc, country_summary: cs });
        }
        return jsonResponse(summary);
      }

      // GET /health/download/all
      if (request.method === 'GET' && path === '/health/download/all') {
        const cached = await env.PROXY_CACHE.get('_HEALTH_DUMP_ALL');
        if (cached) return new Response(cached, { headers: { 'Content-Type':'application/json', 'Content-Disposition':'attachment; filename="proxy_health_all.json"', ...CORS }});
        const out = [];
        let cursor = undefined;
        do {
          const page = await env.PROXY_CACHE.list({ cursor, limit: 1000 });
          cursor = page.cursor;
          for (const k of page.keys) {
            if (k.name.startsWith('_')) continue;
            const v = await env.PROXY_CACHE.get(k.name);
            if (v) {
              try {
                out.push(JSON.parse(v));
              } catch(e) {
                console.warn(`Failed to parse KV value for key ${k.name}:`, e);
              }
            }
          }
        } while (cursor);
        const payload = JSON.stringify(out, null, 2);
        await env.PROXY_CACHE.put('_HEALTH_DUMP_ALL', payload, { expirationTtl: 60 });
        return new Response(payload, { headers: { 'Content-Type':'application/json', 'Content-Disposition':'attachment; filename="proxy_health_all.json"', ...CORS }});
      }

      // GET /health/download/country?cc=XX
      if (request.method === 'GET' && path === '/health/download/country') {
        const cc = (url.searchParams.get('cc')||'').toUpperCase();
        if (!cc) return textResponse('missing cc',400);
        const cacheKey = `_HEALTH_DUMP_CC_${cc}`;
        const cached = await env.PROXY_CACHE.get(cacheKey);
        if (cached) return new Response(cached, { headers: { 'Content-Type':'application/json', 'Content-Disposition': `attachment; filename="proxy_health_${cc}.json"`, ...CORS }});
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
        return new Response(payload, { headers: { 'Content-Type':'application/json', 'Content-Disposition': `attachment; filename="proxy_health_${cc}.json"`, ...CORS }});
      }

      return jsonResponse({ ok:true, message: 'master ready. POST /force-health to dispatch.' });
    } catch (err) {
      console.error('master error', err);
      return textResponse(String(err), 500);
    }
  }
};

async function dispatchToSlave(endpoint, batch, env) {
  try {
    const resp = await fetch(`${endpoint.replace(/\/$/,'')}/check-batch`, {
      method: 'POST',
      headers: { 'Content-Type':'application/json', 'Authorization': `Bearer ${env.SLAVE_TOKEN || ''}` },
      body: JSON.stringify({ batch })
    });
    if (!resp.ok) {
      return { ok:false, status:resp.status, text: await resp.text(), endpoint };
    }
    const j = await resp.json();
    return { ok:true, endpoint, result: j };
  } catch (e) {
    return { ok:false, error: String(e), endpoint };
  }
}

/* Batched summary update to reduce race conditions */
async function updateSummaryBatch(env, prevResults, currResults) {
  if (currResults.length === 0) return;

  // Note: This is still a read-modify-write operation which has a small chance of a race condition
  // if two /submit-results requests arrive at the exact same time. However, by batching the changes,
  // we reduce the number of RMW cycles from N to 1 per request, making a collision much less likely.
  // A proper transactional update would require Durable Objects.
  let summary;
  try {
    summary = await env.PROXY_CACHE.get('_HEALTH_SUMMARY', 'json');
  } catch(e) {
    console.warn('Could not parse _HEALTH_SUMMARY, rebuilding from scratch.', e);
  }
  if (!summary) summary = { total: 0, alive: 0, dead: 0, countries: {} };

  const changes = {
    alive: 0,
    dead: 0,
    countries: {} // { 'US': { alive: 5, dead: -2 } }
  };

  // Calculate net changes from previous results
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

  // Calculate net changes from new results
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

  // Apply net changes to the summary
  summary.alive = (summary.alive || 0) + changes.alive;
  summary.dead = (summary.dead || 0) + changes.dead;
  summary.total = summary.alive + summary.dead;

  for (const [cc, c_changes] of Object.entries(changes.countries)) {
    summary.countries[cc] = summary.countries[cc] || { alive: 0, dead: 0 };
    summary.countries[cc].alive = (summary.countries[cc].alive || 0) + c_changes.alive;
    summary.countries[cc].dead = (summary.countries[cc].dead || 0) + c_changes.dead;
  }

  // Ensure no negative counts
  summary.alive = Math.max(0, summary.alive);
  summary.dead = Math.max(0, summary.dead);
  Object.values(summary.countries).forEach(c => {
    c.alive = Math.max(0, c.alive);
    c.dead = Math.max(0, c.dead);
  });

  try {
    await env.PROXY_CACHE.put('_HEALTH_SUMMARY', JSON.stringify(summary));
  } catch(e) {
    console.error('Failed to PUT updated summary', e);
  }
}
