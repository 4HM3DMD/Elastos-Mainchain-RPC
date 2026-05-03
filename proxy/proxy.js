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

// ─── HTTP plumbing ───────────────────────────────────────────────────────
function jsonError(res, id, code, message, status) {
    res.writeHead(status, {
        'Content-Type': 'application/json',
        'Access-Control-Allow-Origin': '*'
    });
    res.end(JSON.stringify({
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

    var proxyReq = http.request({
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
            chunks.push(chunk);
            total += chunk.length;
            if (total > MAX_BODY * 16) {
                proxyReq.destroy();
                callback(new Error('upstream response too large'));
            }
        });
        proxyRes.on('end', function () {
            callback(null, proxyRes.statusCode, Buffer.concat(chunks).toString('utf8'));
        });
    });

    proxyReq.on('error', function (err) {
        callback(err);
    });
    proxyReq.on('timeout', function () {
        proxyReq.destroy();
        callback(new Error('upstream timeout'));
    });

    proxyReq.write(body);
    proxyReq.end();
}

function isMethodAllowed(method) {
    if (typeof method !== 'string') return false;
    return !BLOCKED.has(method);
}

function pickTarget(method) {
    if (INDEXER_METHODS.has(method)) {
        return { host: INDEXER_HOST, port: INDEXER_PORT, auth: null, name: 'indexer' };
    }
    return { host: ELA_HOST, port: ELA_PORT, auth: AUTH, name: 'node' };
}

// ─── Server ──────────────────────────────────────────────────────────────
var server = http.createServer(function (req, res) {
    if (req.method === 'OPTIONS') {
        res.writeHead(204, {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'POST, OPTIONS',
            'Access-Control-Allow-Headers': 'Content-Type'
        });
        res.end();
        return;
    }

    if (req.method !== 'POST') {
        jsonError(res, null, -32600, 'Method not allowed', 405);
        return;
    }

    var chunks = [];
    var size = 0;

    req.on('data', function (chunk) {
        size += chunk.length;
        if (size > MAX_BODY) {
            req.destroy();
            jsonError(res, null, -32600, 'Request body too large', 413);
            return;
        }
        chunks.push(chunk);
    });

    req.on('end', function () {
        var raw = Buffer.concat(chunks).toString('utf8');
        var parsed;

        try {
            parsed = JSON.parse(raw);
        } catch (e) {
            jsonError(res, null, -32700, 'Parse error', 400);
            return;
        }

        // Batch
        if (Array.isArray(parsed)) {
            handleBatch(parsed, res);
            return;
        }

        handleSingle(parsed, res);
    });

    req.on('error', function () {
        jsonError(res, null, -32603, 'Request error', 400);
    });
});

function handleSingle(reqObj, res) {
    var id = reqObj && reqObj.id !== undefined ? reqObj.id : null;
    var method = reqObj && reqObj.method;

    if (!isMethodAllowed(method)) {
        jsonError(res, id, -32601, 'Method not allowed', 200);
        return;
    }

    var target = pickTarget(method);
    var body = JSON.stringify(reqObj);

    forwardRequest(target.host, target.port, body, target.auth, UPSTREAM_TIMEOUT, function (err, status, upstreamBody) {
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
                // keep upstream body as-is
            }
        }

        res.writeHead(status || 200, {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*'
        });
        res.end(transformed);
    });
}

function handleBatch(arr, res) {
    if (arr.length === 0) {
        jsonError(res, null, -32600, 'Empty batch', 400);
        return;
    }

    var responses = new Array(arr.length);
    var pending = arr.length;

    function done() {
        res.writeHead(200, {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*'
        });
        res.end(JSON.stringify(responses));
    }

    arr.forEach(function (reqObj, idx) {
        var id = reqObj && reqObj.id !== undefined ? reqObj.id : null;
        var method = reqObj && reqObj.method;

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

// ─── Boot ────────────────────────────────────────────────────────────────
pollHeight();
setInterval(pollHeight, HEIGHT_POLL_MS);

server.listen(LISTEN_PORT, LISTEN_HOST, function () {
    console.log('ELA RPC proxy listening on ' + LISTEN_HOST + ':' + LISTEN_PORT);
    console.log('  → ELA node     : ' + ELA_HOST + ':' + ELA_PORT);
    console.log('  → Indexer      : ' + INDEXER_HOST + ':' + INDEXER_PORT);
});
