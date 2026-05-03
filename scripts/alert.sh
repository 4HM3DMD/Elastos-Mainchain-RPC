#!/bin/bash
TG_TOKEN="PASTE_YOUR_BOT_TOKEN_HERE"
TG_CHAT="PASTE_YOUR_CHAT_ID_HERE"

RPC_USER="PASTE_YOUR_RPC_USER_HERE"
RPC_PASS="PASTE_YOUR_RPC_PASS_HERE"

STATE_DIR="/opt/ela-indexer"
STATE_FILE="$STATE_DIR/.alert_state"
NODE_RPC="http://${RPC_USER}:${RPC_PASS}@127.0.0.1:20336"

send_alert() {
    curl -s -X POST "https://api.telegram.org/bot${TG_TOKEN}/sendMessage" \
        -d chat_id="${TG_CHAT}" \
        -d text="$1" \
        -d parse_mode="HTML" > /dev/null 2>&1
}

rpc_call() {
    curl -s -m 5 -X POST "$NODE_RPC" \
        -H "Content-Type: application/json" \
        -d "{\"method\":\"$1\"}" 2>/dev/null
}

load_state() {
    if [ -f "$STATE_FILE" ]; then
        . "$STATE_FILE"
    fi
    PREV_HEIGHT="${PREV_HEIGHT:-0}"
    STALL_COUNT="${STALL_COUNT:-0}"
    PREV_STATUS="${PREV_STATUS:-ok}"
}

save_state() {
    cat > "$STATE_FILE" <<EOF
PREV_HEIGHT=$NODE_HEIGHT
STALL_COUNT=$STALL_COUNT
PREV_STATUS=$CURRENT_STATUS
EOF
}

load_state
CURRENT_STATUS="ok"
ALERTS=""
NOW=$(date '+%Y-%m-%d %H:%M:%S UTC')

# --- Indexer checks ---

RESPONSE=$(curl -s -m 5 http://127.0.0.1:8337/health 2>/dev/null)

if [ -z "$RESPONSE" ]; then
    CURRENT_STATUS="critical"
    send_alert "🔴 <b>ELA Indexer DOWN</b>%0AHealth endpoint unreachable%0ATime: $NOW"
    save_state
    exit 1
fi

SYNCED=$(echo "$RESPONSE" | grep -o '"synced":true')
IDX_HEIGHT=$(echo "$RESPONSE" | grep -o '"height":[0-9]*' | grep -o '[0-9]*')

if [ -z "$SYNCED" ]; then
    CURRENT_STATUS="warning"
    send_alert "🟡 <b>ELA Indexer SYNCING</b>%0AHeight: ${IDX_HEIGHT}%0ATime: $NOW"
    save_state
    exit 0
fi

# --- Node reachability ---

NODE_HEIGHT=$(rpc_call "getblockcount" | grep -o '"result":[0-9]*' | grep -o '[0-9]*')

if [ -z "$NODE_HEIGHT" ]; then
    CURRENT_STATUS="critical"
    send_alert "🔴 <b>ELA Node UNREACHABLE</b>%0AIndexer height: ${IDX_HEIGHT}%0ATime: $NOW"
    save_state
    exit 1
fi

# --- Indexer lag ---

NODE_TIP=$((NODE_HEIGHT - 1))
LAG=$((NODE_TIP - IDX_HEIGHT))

if [ "$LAG" -gt 10 ]; then
    CURRENT_STATUS="warning"
    ALERTS="${ALERTS}%0A🟠 Indexer lagging: ${IDX_HEIGHT} / ${NODE_TIP} (${LAG} blocks behind)"
fi

# --- Chain stall detection ---

if [ "$NODE_HEIGHT" -eq "$PREV_HEIGHT" ] && [ "$NODE_HEIGHT" -gt 0 ]; then
    STALL_COUNT=$((STALL_COUNT + 1))
else
    STALL_COUNT=0
fi

if [ "$STALL_COUNT" -ge 3 ]; then
    CURRENT_STATUS="warning"
    STALL_MINS=$((STALL_COUNT * 2))
    ALERTS="${ALERTS}%0A🟡 Chain stalled at height ${NODE_HEIGHT} for ~${STALL_MINS} min"
fi

# --- Peer count ---

PEERS=$(rpc_call "getconnectioncount" | grep -o '"result":[0-9]*' | grep -o '[0-9]*')

if [ -n "$PEERS" ] && [ "$PEERS" -lt 3 ]; then
    CURRENT_STATUS="warning"
    ALERTS="${ALERTS}%0A🟠 Low peer count: ${PEERS} connections"
fi

# --- Mempool size ---

MEMPOOL_RAW=$(rpc_call "getrawmempool")
MEMPOOL_COUNT=$(echo "$MEMPOOL_RAW" | grep -o '"[a-f0-9]\{64\}"' | wc -l)

if [ "$MEMPOOL_COUNT" -gt 500 ]; then
    ALERTS="${ALERTS}%0A🟡 Large mempool: ${MEMPOOL_COUNT} pending txs"
fi

# --- Disk usage ---

DISK_USAGE=$(df /opt/ela-indexer --output=pcent | tail -1 | tr -d ' %')
if [ "$DISK_USAGE" -gt 90 ]; then
    CURRENT_STATUS="warning"
    ALERTS="${ALERTS}%0A🟠 Disk usage: ${DISK_USAGE}%"
fi

# --- Recovery notification ---

if [ "$PREV_STATUS" != "ok" ] && [ "$CURRENT_STATUS" = "ok" ] && [ -z "$ALERTS" ]; then
    send_alert "✅ <b>ELA Node RECOVERED</b>%0AHeight: ${NODE_HEIGHT} | Peers: ${PEERS} | Indexer: ${IDX_HEIGHT}%0ATime: $NOW"
fi

# --- Send combined alert if any issues ---

if [ -n "$ALERTS" ]; then
    send_alert "⚠️ <b>ELA Node Health Report</b>${ALERTS}%0A%0AHeight: ${NODE_HEIGHT} | Peers: ${PEERS:-?} | Mempool: ${MEMPOOL_COUNT:-0}%0ATime: $NOW"
fi

save_state
