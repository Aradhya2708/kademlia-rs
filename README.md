# kademlia-rs

[![CI](https://github.com/Aradhya2708/kademlia-rs/actions/workflows/ci.yml/badge.svg)](https://github.com/Aradhya2708/kademlia-rs/actions/workflows/ci.yml)

A [Kademlia](https://pdos.csail.mit.edu/~petar/papers/maymounkov-kademlia-lncs.pdf)
distributed hash table in Rust, running over UDP on top of tokio.

It covers the whole protocol from the paper:

- 160-bit node ids and keys, with XOR as the distance metric
- k-buckets that prefer long-lived contacts, with a replacement cache for newcomers
- the four RPCs: `PING`, `STORE`, `FIND_NODE`, `FIND_VALUE`
- iterative lookups with `α` requests in flight
- values replicated on the `k` closest nodes, and cached along the lookup path
- bucket refresh, hourly republishing and record expiry

## Usage

```rust
use kademlia::{Config, Node};

#[tokio::main]
async fn main() -> kademlia::Result<()> {
    let node = Node::bind("0.0.0.0:4000", Config::default()).await?;
    node.bootstrap("203.0.113.7:4000".parse().unwrap()).await?;

    node.put(b"greeting", b"hello".to_vec()).await?;
    let value = node.get(b"greeting").await;
    println!("{value:?}");
    Ok(())
}
```

Keys can be any bytes. They're hashed into the keyspace with SHA-1.

### Command-line node

There's a small interactive node behind the `cli` feature. Start one, then
point others at it from more terminals (or machines):

```console
$ cargo run --features cli --bin kad -- --listen 127.0.0.1:4000
node 70575138f93d9ffe059e69835564d4a13c08cfba listening on 127.0.0.1:4000
> put greeting hello there
stored on 2 nodes

$ cargo run --features cli --bin kad -- --listen 127.0.0.1:4001 --bootstrap 127.0.0.1:4000
joined via 127.0.0.1:4000, 1 peers known
> get greeting
hello there
```

`help` lists the other commands. Set `RUST_LOG=kademlia=debug` to see what
the node is doing.

### Local swarm

```console
$ cargo run --example swarm -- 50
started 50 nodes in 1.91s, each knows 44.4 peers on average
key-0: stored on 21 nodes, read back Some("value 0") in 1.02ms
...
```

## How it works

**Ids and distance.** Nodes and keys share one 160-bit space. The distance
between two ids is their XOR, read as an unsigned integer. A node is
responsible for the keys closest to its own id.

**Routing table.** One k-bucket for each bit of the keyspace. Bucket `i` holds
contacts at a distance in `[2^i, 2^(i+1))`. Each bucket keeps up to `k`
contacts, ordered by when we last heard from them. When a full bucket hears
from someone new, the newcomer goes into a replacement cache, and we ping the
contact we've heard from least recently. If it answers, it stays. If not,
it's evicted and the newest replacement takes its place. Every incoming
message updates the table, so nodes learn about each other just by talking.

**Lookups.** To find the `k` nodes closest to an id, start from the closest
contacts we know and keep asking the closest unqueried ones for anyone closer,
with `α` requests in flight. The lookup ends when the `k` closest nodes it has
heard of have all answered. A `FIND_VALUE` lookup stops early as soon as
someone has the value.

**Storing and finding values.** `put` finds the `k` closest nodes to the key
and sends each of them a `STORE`. `get` checks locally, then runs a value
lookup. On a hit, it caches a copy at the closest node that answered without
the value, so popular keys get answered sooner.

**Keeping things alive.** Two background jobs keep the network healthy:

- buckets that haven't seen a lookup in an hour get one, for a random id in
  their range
- every hour, nodes push the records they hold back out to the `k` closest
  nodes, so values survive churn

Records expire 24 hours after they were published. Republishing passes the
*remaining* TTL along, so it keeps values available without making them live
forever.

## Configuration

| Field                | Default | What it does                                          |
| -------------------- | ------- | ----------------------------------------------------- |
| `k`                  | 20      | bucket size, and how many nodes each value goes to   |
| `alpha`              | 3       | requests a lookup keeps in flight at once             |
| `request_timeout`    | 2 s     | how long to wait for a reply                          |
| `record_ttl`         | 24 h    | how long a value lives                                |
| `max_records`        | 65 536  | most records a node will hold for others             |
| `republish_interval` | 1 h     | how often held records are pushed back out            |
| `refresh_interval`   | 1 h     | how long a bucket can go without a lookup             |

The tests shrink the timings a lot so they run quickly.

## Design notes

A few places where this differs from the paper, or where the paper leaves
the details open:

- **Flat bucket array instead of a split-on-demand tree.** The tree and a flat
  array of 160 buckets end up holding the same contacts in practice. The array
  is much simpler.
- **Continuous lookups instead of rounds.** The paper sends `α` requests,
  waits for the round to finish, and repeats. This keeps `α` requests going all
  the time instead, so one slow node doesn't hold up the others. The stopping
  condition is the same.
- **Lookups seed from the whole routing table.** Only the `k` closest live
  candidates are ever asked, but if those turn out to be dead, the next ones
  are already lined up and the lookup doesn't stall.
- **Timeouts don't evict on their own.** UDP drops packets now and then. A
  contact that misses one reply is only evicted if a replacement is waiting
  for its slot.
- **The publisher keeps a copy.** A single node works as a (tiny) DHT, and the
  original publisher keeps republishing the value for as long as it's up.

## Limitations

This is a faithful implementation of the protocol, not a hardened one:

- No NAT traversal. Nodes need to be reachable at the address they send from.
- No defence against Sybil or eclipse attacks. Node ids aren't tied to
  anything, and any node can overwrite any key (last writer wins).
- Records are kept in memory only.
- Values are capped at 60 KiB, so a `STORE` always fits in a single datagram.

## Layout

```
src/
├── id.rs           NodeId, Distance, bucket indexing
├── contact.rs      a peer's id and address
├── routing/        k-buckets and the routing table
├── protocol.rs     wire messages and their encoding
├── transport.rs    UDP socket, request/response matching, timeouts
├── lookup.rs       iterative node and value lookups
├── store.rs        records with expiry and republish tracking
├── node.rs         the public Node API and background tasks
└── bin/kad.rs      the command-line node
```

## Development

```console
cargo test
cargo clippy --all-targets --all-features
```

The integration tests in `tests/network.rs` start networks of up to 40 nodes
on localhost and check, among other things, that lookups return the true
closest nodes, values can be read from every node, and republishing keeps a
value alive after every node holding it has left.

## License

MIT
