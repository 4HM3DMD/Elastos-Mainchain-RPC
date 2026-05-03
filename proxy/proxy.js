'use strict';

/**
 * Elastos Main-chain RPC Proxy
 *
 * Routes incoming JSON-RPC requests to either:
 *   - the local Elastos.ELA full node (default)
 *   - the local Go indexer (for `gethistory`, `getcrmember`)
 *
 * Adds response transformers that compute fields the canonical Elastos.ELA
 * node does NOT expose but that older clients (Essentials, CR website,
 * elastos-wallet-js) expect:
 *
 *   getcrrelatedstage : currentsession, voting/claiming windows, inClaiming
 *   listproducers     : totalvotes (alias of totaldposv1votes), per-producer onduty
 *
 * Boundary math is verified against Elastos.ELA source
 * (cr/state/committee.go isInVotingPeriod / isInClaimPeriod).
 *
 * Configuration is read from environment variables. See .env.example.
 */

var http = require('http');

// Reuse TCP connections to upstreams. The Elastos.ELA node and the indexer
// are both local; keep-alive saves ~1ms per call and avoids ephemeral-port
// exhaustion under load.
var keepAliveAgent = new http.Agent({
    keepAlive: true,
    keepAliveMsecs: 30000,
    maxSockets: 256,
    maxFreeSockets: 32
});

// ─── Config from environment ─────────────────────────────────────────────
var ELA_HOST           = process.env.ELA_HOST           || '127.0.0.1';
var ELA_PORT           = parseInt(process.env.ELA_PORT  || '20336', 10);
var ELA_RPC_USER       = process.env.ELA_RPC_USER       || '';
var ELA_RPC_PASS       = process.env.ELA_RPC_PASS       || '';

var INDEXER_HOST       = process.env.INDEXER_HOST       || '127.0.0.1';
var INDEXER_PORT       = parseInt(process.env.INDEXER_PORT || '8337', 10);

var LISTEN_HOST        = process.env.LISTEN_HOST        || '127.0.0.1';
var LISTEN_PORT        = parseInt(process.env.LISTEN_PORT || '8336', 10);

var MAX_BODY           = parseInt(process.env.MAX_BODY           || '65536', 10);
var MAX_BATCH          = parseInt(process.env.MAX_BATCH          || '64',    10);
var UPSTREAM_TIMEOUT   = parseInt(process.env.UPSTREAM_TIMEOUT   || '30000', 10);
var HEIGHT_POLL_MS     = parseInt(process.env.HEIGHT_POLL_MS     || '3000', 10);

// CR cycle constants — derived from the Elastos main-chain consensus params.
// These values are public and chain-wide; not secrets.
var CR_FIRST_TERM_START = parseInt(process.env.CR_FIRST_TERM_START || '658930', 10);
var CR_TERM_LENGTH      = parseInt(process.env.CR_TERM_LENGTH      || '262800', 10);
var CR_VOTING_PERIOD    = parseInt(process.env.CR_VOTING_PERIOD    || '21600', 10);
var CR_CLAIMING_PERIOD  = parseInt(process.env.CR_CLAIMING_PERIOD  || '10080', 10);

if (!ELA_RPC_USER || !ELA_RPC_PASS) {
    console.error('FATAL: ELA_RPC_USER and ELA_RPC_PASS must be set in the environment.');
    process.exit(1);
}

var AUTH = Buffer.from(ELA_RPC_USER + ':' + ELA_RPC_PASS).toString('base64');

// ─── Method routing ──────────────────────────────────────────────────────
var BLOCKED = new Set([
    'togglemining',
    'discretemining',
    'setloglevel',
    'createauxblock',
    'submitauxblock',
    'submitsidechainillegaldata',
    'signrawtransactionwithkey'
]);

var INDEXER_METHODS = new Set([
    'gethistory',
    'getcrmember'
]);

// ─── Cached chain state ──────────────────────────────────────────────────
var cachedHeight = 0;

function pollHeight() {
    var payload = JSON.stringify({ jsonrpc: '2.0', method: 'getblockcount', id: 0 });
    forwardRequest(ELA_HOST, ELA_PORT, payload, AUTH, 5000, function (err, status, body) {
        if (err) return;
        try {
            var resp = JSON.parse(body);
            if (typeof resp.result === 'number' && resp.result > 0) {
                cachedHeight = resp.result;
            }
        } catch (e) { /* keep last known height */ }
    });
}

// ─── Response transformers ───────────────────────────────────────────────

/**
 * getcrrelatedstage
 *
 * The canonical Elastos.ELA node returns only 6 fields. Older clients
 * expect 4 additional fields. Compute them here:
 *
 *   currentsession      = (ondutyStart - firstTermStart) / termLength + 1
 *   votingstartheight   = ondutyEnd - claimingPeriod - votingPeriod
 *   votingendheight     = ondutyEnd - claimingPeriod              (exclusive)
 *   claimingStartHeight = votingendheight                          (inclusive)
 *   claimingEndHeight   = ondutyEnd                                (inclusive)
 *   inClaiming          = currentHeight ∈ [claimingStart, claimingEnd]
 *
 * Boundaries match cr/state/committee.go: isInVotingPeriod uses `<` on the
 * upper bound; isInClaimPeriod uses `<=` on both bounds.
 */
function transformCRRelatedStage(resp) {
    var r = resp.result;
    if (!r || typeof r.ondutystartheight !== 'number' || typeof r.ondutyendheight !== 'number') return;
    if (r.ondutyendheight <= r.ondutystartheight) return;

    var termStart = r.ondutystartheight;
    var termEnd   = r.ondutyendheight;

    r.currentsession = Math.floor((termStart - CR_FIRST_TERM_START) / CR_TERM_LENGTH) + 1;

    var votingEnd   = termEnd - CR_CLAIMING_PERIOD;
    var votingStart = votingEnd - CR_VOTING_PERIOD;
    r.votingstartheight = votingStart;
    r.votingendheight   = votingEnd;

    r.claimingStartHeight = votingEnd;
    r.claimingEndHeight   = termEnd;

    var h = cachedHeight;
    r.inClaiming = (h >= r.claimingStartHeight && h <= r.claimingEndHeight);
}

/**
 * listproducers
 *
 * Adds:
 *   result.totalvotes       = result.totaldposv1votes (legacy alias)
 *   producers[].onduty      = "Valid" if active, else "Invalid"
 */
function transformListProducers(resp) {
    var r = resp.result;
    if (!r || typeof r !== 'object') return;

    if (typeof r.totaldposv1votes === 'string') {
        r.totalvotes = r.totaldposv1votes;
    }

    if (Array.isArray(r.producers)) {
        for (var i = 0; i < r.producers.length; i++) {
            r.producers[i].onduty = r.producers[i].active === true ? 'Valid' : 'Invalid';
        }
    }
}

var TRANSFORMERS = {
    'getcrrelatedstage': transformCRRelatedStage,
    'listproducers':     transformListProducers
};

function applyTransform(method, respObj) {
    var fn = TRANSFORMERS[method];
    if (fn && respObj && respObj.result && !respObj.error) {
        fn(respObj);
    }
}

// The ELA node accepts method names case-insensitively (Go's net/rpc style),
// so we must normalize before any routing decision. Otherwise a client can
// trivially bypass BLOCKED (e.g. `Togglemining`) or skip transformers
// (e.g. `getCRrelatedstage`).
function normalizeMethod(method) {
    return typeof method === 'string' ? method.toLowerCase() : '';
}

// ─── HTTP plumbing ───────────────────────────────────────────────────────
function jsonError(res, id, code, message, status) {
    safeWrite(res, status, {
        'Content-Type': 'application/json',
        'Access-Control-Allow-Origin': '*'
    }, JSON.stringify({
        id: id,
        jsonrpc: '2.0',
        error: { code: code, message: message },
        result: null
    }));
}

function forwardRequest(hostname, port, body, auth, timeout, callback) {
    var headers = {
        'Content-Type': 'application/json',
        'Content-Length': Buffer.byteLength(body)
    };
    if (auth) {
        headers['Authorization'] = 'Basic ' + auth;
    }

    // Guard against multi-fire: the request can complete via end, error,
    // timeout, or oversize-body. Each path must invoke `callback` at most
    // once, even if the underlying socket fires later events after destroy().
    var done = false;
    function finish(err, status, bodyStr) {
        if (done) return;
        done = true;
        callback(err, status, bodyStr);
    }

    var proxyReq = http.request({
        agent: keepAliveAgent,
        hostname: hostname,
        port: port,
        path: '/',
        method: 'POST',
        headers: headers,
        timeout: timeout
    }, function (proxyRes) {
        var chunks = [];
        var total = 0;
        proxyRes.on('data', function (chunk) {
            if (done) return;
            chunks.push(chunk);
            total += chunk.length;
            if (total > MAX_BODY * 16) {
                proxyRes.destroy();
                proxyReq.destroy();
                finish(new Error('upstream response too large'));
            }
        });
        proxyRes.on('end', function () {
            finish(null, proxyRes.statusCode, Buffer.concat(chunks).toString('utf8'));
        });
        proxyRes.on('error', function (err) {
            finish(err);
        });
    });

    proxyReq.on('error', function (err) {
        finish(err);
    });
    proxyReq.on('timeout', function () {
        proxyReq.destroy();
        finish(new Error('upstream timeout'));
    });

    proxyReq.write(body);
    proxyReq.end();
}

function isMethodAllowed(method) {
    if (typeof method !== 'string' || method.length === 0) return false;
    return !BLOCKED.has(method);
}

function pickTarget(method) {
    if (INDEXER_METHODS.has(method)) {
        return { host: INDEXER_HOST, port: INDEXER_PORT, auth: null, name: 'indexer' };
    }
    return { host: ELA_HOST, port: ELA_PORT, auth: AUTH, name: 'node' };
}

// True if writing to `res` would throw. Long upstream calls + client hangup
// is the most common cause; without this guard the upstream callback can
// fire after the client socket is gone, crashing the worker.
function resWritable(res) {
    return res && !res.writableEnded && !res.destroyed;
}

function safeWrite(res, status, headers, body) {
    if (!resWritable(res)) return;
    try {
        res.writeHead(status, headers);
        res.end(body);
    } catch (e) {
        // Headers were already sent or socket closed mid-write — nothing
        // we can do, but don't propagate to crash the process.
    }
}

// ─── Server ──────────────────────────────────────────────────────────────
var server = http.createServer(function (req, res) {
    if (req.method === 'OPTIONS') {
        safeWrite(res, 204, {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'POST, OPTIONS',
            'Access-Control-Allow-Headers': 'Content-Type'
        }, '');
        return;
    }

    if (req.method !== 'POST') {
        jsonError(res, null, -32600, 'Method not allowed', 405);
        return;
    }

    // The client request can fail via oversize-body, parse error, transport
    // error, or end-of-stream. Each path must respond at most once.
    var handled = false;
    function reply(fn) {
        if (handled) return;
        handled = true;
        fn();
    }

    var chunks = [];
    var size = 0;

    req.on('data', function (chunk) {
        if (handled) return;
        size += chunk.length;
        if (size > MAX_BODY) {
            req.destroy();
            reply(function () { jsonError(res, null, -32600, 'Request body too large', 413); });
            return;
        }
        chunks.push(chunk);
    });

    req.on('end', function () {
        if (handled) return;
        var raw = Buffer.concat(chunks).toString('utf8');
        var parsed;

        try {
            parsed = JSON.parse(raw);
        } catch (e) {
            reply(function () { jsonError(res, null, -32700, 'Parse error', 400); });
            return;
        }

        // Mark as handled so subsequent error events don't double-respond.
        // The actual response is written by handleSingle / handleBatch, both
        // of which use safeWrite() and short-circuit if the client hung up.
        handled = true;

        if (Array.isArray(parsed)) {
            handleBatch(parsed, res);
            return;
        }
        handleSingle(parsed, res);
    });

    req.on('error', function () {
        reply(function () { jsonError(res, null, -32603, 'Request error', 400); });
    });
});

function handleSingle(reqObj, res) {
    var id = reqObj && reqObj.id !== undefined ? reqObj.id : null;
    var method = normalizeMethod(reqObj && reqObj.method);

    if (!isMethodAllowed(method)) {
        jsonError(res, id, -32601, 'Method not allowed', 200);
        return;
    }

    var target = pickTarget(method);
    // Forward the original method casing — ELA accepts both, so we keep the
    // wire bytes identical to what the client sent for everything except the
    // routing decision.
    var body = JSON.stringify(reqObj);

    forwardRequest(target.host, target.port, body, target.auth, UPSTREAM_TIMEOUT, function (err, status, upstreamBody) {
        if (!resWritable(res)) return;
        if (err) {
            jsonError(res, id, -32603, 'Upstream error: ' + err.message, 502);
            return;
        }

        var transformed = upstreamBody;
        if (target.name === 'node' && TRANSFORMERS[method]) {
            try {
                var respObj = JSON.parse(upstreamBody);
                applyTransform(method, respObj);
                transformed = JSON.stringify(respObj);
            } catch (e) {
                // upstream gave non-JSON or oddly-typed; pass through verbatim
            }
        }

        safeWrite(res, status || 200, {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*'
        }, transformed);
    });
}

function handleBatch(arr, res) {
    if (arr.length === 0) {
        jsonError(res, null, -32600, 'Empty batch', 400);
        return;
    }
    if (arr.length > MAX_BATCH) {
        jsonError(res, null, -32600,
            'Batch too large (max ' + MAX_BATCH + ' items)', 413);
        return;
    }

    var responses = new Array(arr.length);
    var pending = arr.length;

    function done() {
        safeWrite(res, 200, {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*'
        }, JSON.stringify(responses));
    }

    arr.forEach(function (reqObj, idx) {
        var id = reqObj && reqObj.id !== undefined ? reqObj.id : null;
        var method = normalizeMethod(reqObj && reqObj.method);

        if (!isMethodAllowed(method)) {
            responses[idx] = {
                id: id, jsonrpc: '2.0',
                error: { code: -32601, message: 'Method not allowed' },
                result: null
            };
            if (--pending === 0) done();
            return;
        }

        var target = pickTarget(method);
        var body = JSON.stringify(reqObj);

        forwardRequest(target.host, target.port, body, target.auth, UPSTREAM_TIMEOUT, function (err, _status, upstreamBody) {
            if (err) {
                responses[idx] = {
                    id: id, jsonrpc: '2.0',
                    error: { code: -32603, message: 'Upstream error: ' + err.message },
                    result: null
                };
            } else {
                try {
                    var respObj = JSON.parse(upstreamBody);
                    if (target.name === 'node') {
                        applyTransform(method, respObj);
                    }
                    responses[idx] = respObj;
                } catch (e) {
                    responses[idx] = {
                        id: id, jsonrpc: '2.0',
                        error: { code: -32603, message: 'Bad upstream JSON' },
                        result: null
                    };
                }
            }
            if (--pending === 0) done();
        });
    });
}

// ─── Process-level safety net ────────────────────────────────────────────
// A stray throw inside any HTTP event listener should not take the proxy
// down. systemd will restart on real fatal errors; we just want to log and
// keep serving the next request.
process.on('uncaughtException', function (err) {
    console.error('uncaughtException:', (err && err.stack) || err);
});
process.on('unhandledRejection', function (reason) {
    console.error('unhandledRejection:', reason);
});

// ─── Boot ────────────────────────────────────────────────────────────────
pollHeight();
var heightPoller = setInterval(pollHeight, HEIGHT_POLL_MS);

server.listen(LISTEN_PORT, LISTEN_HOST, function () {
    console.log('ELA RPC proxy listening on ' + LISTEN_HOST + ':' + LISTEN_PORT);
    console.log('  → ELA node     : ' + ELA_HOST + ':' + ELA_PORT);
    console.log('  → Indexer      : ' + INDEXER_HOST + ':' + INDEXER_PORT);
    console.log('  → Max body     : ' + MAX_BODY + ' bytes');
    console.log('  → Max batch    : ' + MAX_BATCH + ' items');
});

// Graceful shutdown: stop accepting new connections and let in-flight
// requests finish (up to UPSTREAM_TIMEOUT). systemd sends SIGTERM on
// `systemctl stop` / `systemctl restart`.
function shutdown(signal) {
    console.log('received ' + signal + ', shutting down');
    clearInterval(heightPoller);
    server.close(function () {
        process.exit(0);
    });
    setTimeout(function () { process.exit(1); }, UPSTREAM_TIMEOUT + 1000).unref();
}
process.on('SIGTERM', function () { shutdown('SIGTERM'); });
process.on('SIGINT',  function () { shutdown('SIGINT'); });
