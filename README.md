# Elastos Main-chain RPC

A drop-in replacement for `api.elastos.io/ela`. It pairs an unmodified
`Elastos.ELA` full node with a Go indexer (Postgres-backed) and a small
Node.js proxy that:

- routes `gethistory` and `getcrmember` to the indexer,
- forwards every other JSON-RPC call straight to the ELA node,
- adds **response transformers** that fill in fields older clients (Elastos
  Essentials, the Cyber Republic website, `elastos-wallet-js`) expect but the
  canonical `Elastos.ELA` node does not return on its own,
- exposes pending-transaction visibility (real-time mempool index) so wallets
  can show unconfirmed sends/receives within ~1 second.

It is a strict superset of the public node: every working call on
`api.elastos.io/ela` returns identical data here, plus you get historical
address indexing on top.

## Why it exists

The current public node exposes 4 fields in `getcrrelatedstage`
(`currentsession`, `votingstartheight`, `votingendheight`, `inClaiming` …)
and a `totalvotes` field on `listproducers` that **do not exist** in the
canonical [Elastos.ELA](https://github.com/elastos/Elastos.ELA) source. They
come from a private fork. Wallets and dApps depend on them.

This project keeps the official `Elastos.ELA` binary intact and computes
those fields in a tiny proxy layer. Boundary math is verified against
`cr/state/committee.go` (`isInVotingPeriod`, `isInClaimPeriod`).

## Architecture

```
                          ┌─────────────────┐
                          │   nginx + TLS   │   (optional, recommended)
                          └────────┬────────┘
                                   │
                                   ▼
                          ┌─────────────────┐
   client ────POST /ela──▶│  proxy.js (Node)│
                          │  :8336          │
                          └──┬───────────┬──┘
                             │           │
              gethistory     │           │   everything else
              getcrmember    │           │
                             ▼           ▼
                       ┌─────────┐  ┌─────────────────┐
                       │ indexer │  │ Elastos.ELA node│
                       │ :8337   │  │ :20336 (RPC)    │
                       └─────────┘  └─────────────────┘
                             │
                             ▼
                       ┌─────────┐
                       │postgres │
                       └─────────┘
```

## What the proxy adds

| Method                | Field                                                                                                      |
|-----------------------|------------------------------------------------------------------------------------------------------------|
| `getcrrelatedstage`   | `currentsession`, `votingstartheight`, `votingendheight`, `claimingStartHeight`, `claimingEndHeight`, `inClaiming` |
| `listproducers`       | `result.totalvotes` (alias of `totaldposv1votes`); `producers[].onduty` (`Valid` / `Invalid`)              |
| `gethistory`          | served by the Go indexer; merges confirmed history with pending mempool entries in real time               |
| `getcrmember`         | served by the Go indexer                                                                                   |

Admin / mining methods (`togglemining`, `discretemining`, `setloglevel`,
`createauxblock`, `submitauxblock`, `submitsidechainillegaldata`,
`signrawtransactionwithkey`) are blocked at the proxy.

## Repository layout

```
.
├── proxy/                 # Node.js RPC proxy (env-driven)
│   ├── proxy.js
│   └── package.json
├── indexer/               # Go indexer (mempool + RPC + sync)
│   ├── cmd/indexer/
│   ├── internal/
│   └── go.mod
├── db/
│   └── schema.sql         # Postgres schema
├── systemd/
│   ├── ela-indexer.service
│   ├── ela-rpc-proxy.service
│   ├── nginx-rpc.conf.example
│   └── journal-retention.conf
├── scripts/
│   ├── alert.sh           # Telegram health alerts (placeholders)
│   └── backup.sh          # Daily pg_dump (placeholders)
├── .env.example
├── LICENSE                # MIT
└── README.md
```

## Requirements

| Component       | Version | Notes                                                        |
|-----------------|---------|--------------------------------------------------------------|
| Linux           | any     | tested on Debian 12 / Ubuntu 22.04                           |
| `Elastos.ELA`   | ≥ 0.9.9 | run as a normal full node — no patches needed                |
| PostgreSQL      | ≥ 14    | local instance, `pgxpool` connects via `DATABASE_URL`        |
| Go              | ≥ 1.22  | for building the indexer                                     |
| Node.js         | ≥ 18    | for the proxy                                                |
| nginx (opt.)    | any     | TLS termination + rate-limiting                              |

## Step-by-step installation

> All paths and users below are conventions, not hard-coded. Adjust to taste.

### 1 — Run an Elastos main-chain node

Follow [elastos/Elastos.ELA](https://github.com/elastos/Elastos.ELA). Leave
the binary unmodified. Make sure JSON-RPC is enabled on `127.0.0.1:20336`
with a username and password. Example `config.json` snippet:

```json
{
  "RpcConfiguration": {
    "User": "GENERATE_A_RANDOM_HEX_STRING",
    "Pass": "GENERATE_ANOTHER_RANDOM_HEX_STRING",
    "WhiteIPList": ["127.0.0.1"]
  }
}
```

Wait until the node is fully synced before continuing — the indexer reads
from it.

### 2 — Postgres

```bash
sudo apt install -y postgresql

sudo -u postgres psql <<SQL
CREATE USER ela_indexer WITH PASSWORD 'STRONG_RANDOM_PASSWORD' CONNECTION LIMIT 20;
CREATE DATABASE ela_index OWNER ela_indexer;
ALTER USER ela_indexer SET statement_timeout = '10s';
SQL

# Apply schema
psql -h 127.0.0.1 -U ela_indexer -d ela_index -f db/schema.sql
```

### 3 — Build the Go indexer

```bash
cd indexer
go mod tidy
CGO_ENABLED=0 go build -o bin/indexer ./cmd/indexer
```

### 4 — Install the proxy

```bash
cd proxy
# proxy.js has zero npm deps (uses Node's built-in http only)
node -v   # must be ≥ 18
```

### 5 — Configure environment

Copy `.env.example` to two locations and fill in real values:

```bash
sudo mkdir -p /opt/ela-indexer/bin   /opt/ela-rpc-proxy
sudo cp indexer/bin/indexer          /opt/ela-indexer/bin/
sudo cp proxy/proxy.js               /opt/ela-rpc-proxy/

# Indexer env
sudo cp .env.example /opt/ela-indexer/.env
sudo $EDITOR /opt/ela-indexer/.env
sudo chmod 600 /opt/ela-indexer/.env

# Proxy env
sudo cp .env.example /opt/ela-rpc-proxy/.env
sudo $EDITOR /opt/ela-rpc-proxy/.env
sudo chmod 600 /opt/ela-rpc-proxy/.env
```

Set the `ELA_RPC_USER` / `ELA_RPC_PASS` to the values from
`Elastos.ELA`'s `config.json`. Set `DATABASE_URL` with the Postgres password
you just chose.

### 6 — Create dedicated users (optional but recommended)

```bash
sudo useradd --system --no-create-home --shell /usr/sbin/nologin ela-indexer
sudo useradd --system --no-create-home --shell /usr/sbin/nologin ela-proxy
sudo chown -R ela-indexer:ela-indexer /opt/ela-indexer
sudo chown -R ela-proxy:ela-proxy     /opt/ela-rpc-proxy
```

### 7 — Install systemd units

```bash
sudo cp systemd/ela-indexer.service   /etc/systemd/system/
sudo cp systemd/ela-rpc-proxy.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now ela-indexer
sudo systemctl enable --now ela-rpc-proxy

# Tail the logs while initial sync runs
sudo journalctl -u ela-indexer -f
```

Initial sync replays every block; it takes hours on first run, then keeps
up in real time.

### 8 — Front with nginx + TLS (recommended)

```bash
sudo apt install -y nginx certbot python3-certbot-nginx
sudo certbot --nginx -d rpc.your-domain.example
sudo cp systemd/nginx-rpc.conf.example /etc/nginx/sites-available/ela-rpc
sudo $EDITOR /etc/nginx/sites-available/ela-rpc       # set server_name + cert paths
sudo ln -s /etc/nginx/sites-available/ela-rpc /etc/nginx/sites-enabled/
sudo nginx -t && sudo systemctl reload nginx
```

### 9 — Verify

```bash
# Block height (sanity)
curl -s -X POST https://rpc.your-domain.example/ela \
     -H 'Content-Type: application/json' \
     -d '{"jsonrpc":"2.0","method":"getblockcount","id":1,"params":{}}'

# CR stage with all 10 fields populated
curl -s -X POST https://rpc.your-domain.example/ela \
     -H 'Content-Type: application/json' \
     -d '{"jsonrpc":"2.0","method":"getcrrelatedstage","id":1,"params":{}}' | jq

# Address history (served by the indexer; includes pending mempool txs)
curl -s -X POST https://rpc.your-domain.example/ela \
     -H 'Content-Type: application/json' \
     -d '{"jsonrpc":"2.0","method":"gethistory","id":1,"params":{"address":"EYourAddressHere","limit":10}}'
```

## Operations

### Logs

```bash
sudo journalctl -u ela-indexer    -n 200 --no-pager
sudo journalctl -u ela-rpc-proxy  -n 200 --no-pager
```

Cap journal retention by copying `systemd/journal-retention.conf` to
`/etc/systemd/journald.conf.d/retention.conf` and running
`sudo systemctl restart systemd-journald`.

### Backups

`scripts/backup.sh` runs `pg_dump` and ships the gzipped result to a
backup directory. Edit the placeholders at the top of the file and add
to cron:

```cron
0 4 * * * /opt/ela-indexer/scripts/backup.sh >> /var/log/ela-backup.log 2>&1
```

### Health alerts

`scripts/alert.sh` polls `getblockcount` and warns via Telegram on stalls.
Edit the placeholders, run from cron every minute:

```cron
* * * * * /opt/ela-indexer/scripts/alert.sh
```

### Restart after a code change

```bash
# proxy only (zero downtime for indexer / sync)
sudo systemctl restart ela-rpc-proxy

# indexer (sync pauses for a couple of seconds, resumes from last height)
sudo systemctl restart ela-indexer
```

## Troubleshooting

| Symptom                                                       | Likely cause                                                                                            |
|---------------------------------------------------------------|---------------------------------------------------------------------------------------------------------|
| `FATAL: ELA_RPC_USER and ELA_RPC_PASS must be set`            | the proxy can't read its `.env` — check `EnvironmentFile=` path and file permissions                    |
| `connection refused` from proxy → node                        | `Elastos.ELA` is not running or `WhiteIPList` doesn't include `127.0.0.1`                               |
| `gethistory` returns empty for a known address                | initial indexer sync still running; check `journalctl -u ela-indexer -f`                                |
| `getcrrelatedstage` returns only 6 fields                     | proxy not in the request path; check nginx routing or that you're hitting `:8336` not `:20336` directly |
| `pending` transaction never appears                           | mempool poller stopped; restart the indexer (`systemctl restart ela-indexer`)                           |

## Security notes

- Never expose the ELA node RPC port (20336) to the public internet — only
  the proxy port (or the nginx fronting it) should be reachable from
  outside. The `WhiteIPList` in `config.json` should be `["127.0.0.1"]`.
- `ELA_RPC_USER` / `ELA_RPC_PASS` should be ≥ 32 hex characters and unique
  per node.
- The proxy strips and blocks admin / mining / sign-with-key methods. Do
  not unblock them on a public endpoint.
- Run the indexer and proxy as dedicated unprivileged users (step 6).
- Database statement timeout is set to 10 s so a runaway query can't pin
  a connection.

## License

MIT — see [LICENSE](LICENSE).
