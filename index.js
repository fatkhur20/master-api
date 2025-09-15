/**
 * Combined Proxy Health Checker (Master + Support Worker)
 *
 * This single file contains the logic for both the Master and Support workers.
 * The behavior is determined by the `ROLE` environment variable set in wrangler.toml.
 *
 * Master Role (`ROLE=MASTER`):
 * - Fetches jobs from a backend API.
 * - Dispatches proxy batches to support workers.
 *
 * Support Role (`ROLE=SUPPORT`):
 * - Receives a batch of proxies to check from the master.
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

    try {
      if (env.ROLE === 'MASTER') {
        return await handleMasterRequest(request, env, ctx);
      } else if (env.ROLE === 'SUPPORT') {
        return await handleSupportRequest(request, env, ctx);
      } else {
        console.error('FATAL: ROLE environment variable is not set.');
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
    // In the new architecture, the master worker is only triggered by cron or a manual force-health check.
    // It doesn't need complex routing. We can simplify this.
    ctx.waitUntil(triggerHealthCheck(env, ctx));
    return jsonResponse({ ok: true, message: 'Health check process initiated.' });
}

async function triggerHealthCheck(env, ctx) {
    console.log('Starting health check: Fetching job from API...');

    if (!env.API_FETCH_URL) {
        console.error("TRIGGER_FAIL: API_FETCH_URL is not configured.");
        return;
    }

    try {
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

        const proxies = job.batch.map(line => {
            if (line.includes(':')) {
                const [ip, port] = line.split(':').map(s => s.trim());
                return { ip, port };
            }
            return null;
        }).filter(Boolean);

        const endpoints = (env.SLAVE_ENDPOINTS || '').split(',').map(s => s.trim()).filter(Boolean);
        if (!endpoints.length) {
            console.error("TRIGGER_FAIL: No SLAVE_ENDPOINTS configured.");
            return;
        }

        // Simple round-robin dispatch. The more advanced logic (chaining, failover) was removed.
        const dispatchPromises = proxies.map((proxy, idx) => {
            const endpoint = endpoints[idx % endpoints.length];
            return fetch(`${endpoint.replace(/\/$/, '')}/check-batch`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${env.SLAVE_TOKEN || ''}` },
                body: JSON.stringify({ batch: [proxy] }) // Send one proxy per request to a support worker
            });
        });

        await Promise.all(dispatchPromises);
        console.log(`Dispatched ${proxies.length} individual proxy checks.`);

    } catch (err) {
        console.error("Error in triggerHealthCheck:", err);
    }
}


// ===================================================================================
//  SUPPORT WORKER LOGIC
// ===================================================================================

async function handleSupportRequest(request, env, ctx) {
  const url = new URL(request.url);
  const path = url.pathname;

  if (request.method === 'POST' && path === '/check-batch') {
    const auth = request.headers.get('authorization') || '';
    if (!env.SLAVE_TOKEN || auth !== `Bearer ${env.SLAVE_TOKEN}`) {
      return textResponse('Unauthorized', 401);
    }

    let payload;
    try { payload = await request.json(); } catch (e) { return textResponse('Invalid JSON', 400); }
    if (!payload || !Array.isArray(payload.batch)) {
      return textResponse('Bad payload', 400);
    }

    const results = await Promise.all(payload.batch.map(p => checkProxy(p, env)));

    // In this simpler model, the support worker posts results directly.
    ctx.waitUntil(postResultsToApi(results, env));

    return jsonResponse({ ok: true, message: `Processed ${payload.batch.length} proxies.` });
  }

  return textResponse('Support worker is ready.', 200);
}

async function checkProxy(proxy, env) {
  const { ip, port } = proxy;
  // This would be a real TCP/HTTP check in a real implementation
  const isAlive = Math.random() > 0.3;
  const latency = isAlive ? Math.floor(Math.random() * (1500 - 50 + 1)) + 50 : null;

  // GeoIP lookup would also happen here, calling a GeoIP service.
  const geo = { country: 'XX', isp: 'Unknown ISP' };

  return {
    proxy: `${ip}:${port}`,
    status: isAlive ? 'alive' : 'dead',
    latency: latency,
    ...geo
  };
}

async function postResultsToApi(results, env) {
  if (!env.API_RESULT_URL) { // Assumes a new env var for posting results
    console.error('API_RESULT_URL not configured on support worker.');
    return;
  }
  try {
    // The body would be structured according to what the backend API expects
    await fetch(env.API_RESULT_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${env.API_TOKEN || ''}` }, // Assumes a shared API token
      body: JSON.stringify({ results })
    });
  } catch (e) {
    console.error('Error submitting results to API:', e);
  }
}
