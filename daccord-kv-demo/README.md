# daccord-kv-demo

Replicated, linearizable key-value store built on top of the
[`daccord`](../daccord) consensus library.

Each cluster node runs Multi-Paxos (default) or Raft and exposes a gRPC
`KvService` with two operations:

- `Put(key, value) → was_new` — `true` on first write, `false` on replace.
- `Get(key) → Option<value>`.

Both put and get are submitted as `KvOp` values into the consensus log, so
every node applies the same totally-ordered sequence of operations to a
local [redb](https://crates.io/crates/redb)-backed state machine. Reads are
linearizable at the cost of going through consensus.

## Persistence layout

Each node owns a `DATA_DIR` containing two redb files:

```
$DATA_DIR/
├── consensus.redb     # daccord log + algorithm metadata
└── kv-state.redb      # this crate's state machine (kv + meta tables)
```

A crash between "consensus persisted slot N" and "state machine applied
slot N" is safe: on restart the applier re-applies any slot above the
persisted watermark, and the state-write + watermark-bump live in one redb
transaction so the two can't diverge.

## Running

3-node TCP cluster, Multi-Paxos:

```bash
docker compose -f docker-compose.tcp.yml up --build -d

daccord-kv-cli --addr localhost:50051 put alice 'opaque-bytes'
daccord-kv-cli --addr localhost:50053 get alice    # reads via a different node
daccord-kv-cli --addr localhost:50051 status

docker compose -f docker-compose.tcp.yml down
```

Raft variant: `docker compose -f docker-compose.raft.yml up --build -d`.

## Environment

| Var | Required | Meaning |
|-----|----------|---------|
| `NODE_NAME` | no | Defaults to container hostname |
| `TRANSPORT` | yes | `tcp` \| `uds` |
| `BIND_ADDR` | for `tcp` | e.g. `0.0.0.0:9000` |
| `BIND_PATH` | for `uds` | Defaults to `/sockets/<name>.sock` |
| `PEERS` | yes for multi-node | `name=addr,...` (TCP) or `name=path,...` (UDS) |
| `GRPC_PORT` | yes | KV gRPC listen port |
| `ALGORITHM` | no | `paxos` (default) \| `raft` |
| `DATA_DIR` | yes | Directory for `consensus.redb` and `kv-state.redb` |

## Tests

```bash
cargo test -p daccord-kv-demo
```

Covers state-machine unit tests (including persistence across reopen), a
single-node end-to-end test, cross-node read consistency, concurrent puts
from different nodes, and a restart-recovery test.
