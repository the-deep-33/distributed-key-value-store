# Distributed Key-Value Store

A fault-tolerant distributed key-value store implemented in Python from scratch, using the **Raft consensus algorithm** for replication and leader election. Runs as a 3-node cluster orchestrated with Docker Compose, with each node persisting its state and log to disk via bind-mounted volumes.

## Features

- **Leader election** with randomized election timeouts
- **Log replication** with majority-based commit semantics
- **Persistent state**: `current_term`, `voted_for`, and the operation log survive node restarts
- **Automatic failover**: clients are redirected to the current leader; on connection loss, the client retries against random nodes
- **Length-prefixed TCP framing** for reliable message delivery between nodes
- **Containerized deployment**: each node runs in an isolated Docker container with its own data volume

## Architecture

Each node communicates with its peers over TCP using a custom message protocol (length-prefixed JSON). State is persisted as:

- `node_<id>.json`: current term and vote
- `node_<id>_logs.jsonl`: append-only operation log (one entry per line)

## Supported Commands

`SET <key> <value>` | Store a value (replicated through Raft)  
`GET <key>` | Read a value from the leader  
`DEL <key>` | Delete a key (replicated through Raft)  

## Requirements

- Docker Desktop (or Docker Engine + Docker Compose)
- Python 3.10+ (only needed for running the client locally)

## Running the Cluster

Start all three nodes:

```bash
docker compose up --build
```

The nodes will hold an election and elect a leader within a second or two. You'll see leader/follower state transitions logged to stdout.

To stop the cluster:

```bash
docker compose down
```

## Using the Client

In a separate terminal, run the client:

```bash
python src/client.py
```

The client will prompt you for a node ID (1, 2, or 3) to connect to. If you connect to a follower, it will redirect you to the current leader automatically.

Example session:

```
Enter the node id: 1
Enter the command that you wish to execute:
SET name Petar
{'status': 'ok'}
Enter the command that you wish to execute:
GET name
{'status': 'ok', 'value': 'Petar'}
Enter the command that you wish to execute:
DEL name
{'status': 'ok'}
```

## Testing Fault Tolerance

The cluster tolerates the failure of a minority of nodes (1 out of 3). To simulate failures:

**Stop the leader gracefully:**
```bash
docker compose stop node1
```
The remaining nodes will detect the missing heartbeats, hold a new election, and elect a new leader. The client will reconnect automatically.

**Bring a node back:**
```bash
docker compose start node1
```
The recovering node reads its persisted state from disk, rejoins the cluster as a follower, and catches up on any log entries it missed.

**Simulate a network partition (process freeze):**
```bash
docker compose pause node2
docker compose unpause node2
```

## Project Structure

```
.
├── docker-compose.yml      # 3-node cluster definition
├── Dockerfile              # Node container image
├── src/
│   ├── node.py             # Raft node implementation
│   ├── client.py           # Interactive CLI client
│   ├── config.py           # Cluster configuration parsing
│   └── models.py           # RPC message types (RequestVote, AppendEntries, ...)
└── data/                   # Per-node persistent state (created at runtime)
    ├── node1/
    ├── node2/
    └── node3/
```

## Configuration

The cluster topology is configured via the `CLUSTER` environment variable, in the format `id=host:port,id=host:port,...`. The default in `docker-compose.yml` defines three nodes communicating over the internal Docker network using service names as hostnames.

For local testing without Docker, the default in `config.py` falls back to `127.0.0.1` with ports `5001`, `5002`, and `5003`.

## Implementation Notes

- **Election timeout** is randomized between 0.5s and 1.0s per node to avoid split votes
- **Heartbeat interval** is 50ms
- **Commit advancement** follows the Raft safety rule: a leader only directly commits entries from its current term; older entries are committed indirectly when a newer entry from the current term reaches majority replication
- **TCP framing** uses a 128-byte ASCII length header followed by a JSON payload, and `recv_exact` ensures message boundaries are respected even when reads return partial data
