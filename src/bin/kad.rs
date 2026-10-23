//! A small command-line node for poking at a network by hand.
//!
//! Start a first node, then point others at it:
//!
//! ```text
//! cargo run --features cli --bin kad -- --listen 127.0.0.1:4000
//! cargo run --features cli --bin kad -- --listen 127.0.0.1:4001 --bootstrap 127.0.0.1:4000
//! ```

use std::io::Write;
use std::net::SocketAddr;

use clap::Parser;
use kademlia::{Config, Node};
use tokio::io::{AsyncBufReadExt, BufReader};
use tracing_subscriber::EnvFilter;

/// Run a Kademlia node and talk to the network from a prompt.
#[derive(Parser)]
#[command(version)]
struct Args {
    /// Address to listen on.
    #[arg(short, long, default_value = "0.0.0.0:4000")]
    listen: SocketAddr,

    /// A node that's already part of the network. Can be given more than once.
    #[arg(short, long)]
    bootstrap: Vec<SocketAddr>,

    /// Bucket size, and how many nodes each value is stored on.
    #[arg(short, default_value_t = 20)]
    k: usize,
}

const HELP: &str = "\
commands:
  put <key> <value>   store a value in the network
  get <key>           look up a value
  peers               list the contacts in the routing table
  id                  show this node's id and address
  help                show this message
  quit                stop the node";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .init();

    let args = Args::parse();
    let config = Config {
        k: args.k,
        ..Config::default()
    };
    let node = Node::bind(args.listen, config).await?;
    println!("node {} listening on {}", node.id(), node.local_addr());

    for seed in &args.bootstrap {
        match node.bootstrap(*seed).await {
            Ok(()) => println!("joined via {seed}, {} peers known", node.peers().len()),
            Err(e) => eprintln!("couldn't bootstrap from {seed}: {e}"),
        }
    }
    println!("type `help` for a list of commands");

    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    loop {
        print!("> ");
        std::io::stdout().flush()?;
        let Some(line) = lines.next_line().await? else {
            break;
        };

        let line = line.trim();
        let (command, rest) = line.split_once(' ').unwrap_or((line, ""));
        let rest = rest.trim();
        match command {
            "" => {}
            "put" => match rest.split_once(' ') {
                Some((key, value)) => {
                    match node.put(key.as_bytes(), value.as_bytes().to_vec()).await {
                        Ok(copies) => println!("stored on {copies} nodes"),
                        Err(e) => println!("error: {e}"),
                    }
                }
                None => println!("usage: put <key> <value>"),
            },
            "get" if rest.is_empty() => println!("usage: get <key>"),
            "get" => match node.get(rest.as_bytes()).await {
                Some(value) => println!("{}", String::from_utf8_lossy(&value)),
                None => println!("not found"),
            },
            "peers" => {
                let peers = node.peers();
                for contact in &peers {
                    println!("{}  {}", contact.id, contact.addr);
                }
                println!("{} peers", peers.len());
            }
            "id" => println!("{}  {}", node.id(), node.local_addr()),
            "help" => println!("{HELP}"),
            "quit" | "exit" => break,
            other => println!("unknown command `{other}`, try `help`"),
        }
    }
    Ok(())
}
