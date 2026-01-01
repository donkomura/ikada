use std::net::SocketAddr;

use clap::Parser;
use ikada::client::KVStore;
use ikada::trace::init_tracing;

#[derive(Parser, Debug)]
#[command(name = "ikada-repl")]
#[command(about = "Interactive REPL for ikada KVS cluster", long_about = None)]
struct Cli {
    #[arg(
        short,
        long,
        value_name = "ADDR",
        help = "Cluster server addresses (comma-separated)",
        default_value = "127.0.0.1:1111,127.0.0.1:1112,127.0.0.1:1113"
    )]
    servers: String,
}

impl Cli {
    fn parse_addresses(&self) -> anyhow::Result<Vec<SocketAddr>> {
        self.servers
            .split(',')
            .map(|s| {
                s.trim().parse().map_err(|e| {
                    anyhow::anyhow!("Invalid address '{}': {}", s.trim(), e)
                })
            })
            .collect()
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let tracer_provider = init_tracing("ikada-repl")?;

    let cli = Cli::parse();
    let cluster_addrs = cli.parse_addresses()?;

    let mut store = KVStore::connect(cluster_addrs).await?;
    println!("Connected to cluster. Type 'help' for commands.");

    repl(&mut store).await?;

    tracer_provider.shutdown()?;
    Ok(())
}

async fn repl(store: &mut KVStore) -> anyhow::Result<()> {
    use std::io::{self, Write};

    loop {
        print!("> ");
        io::stdout().flush()?;

        let mut input = String::new();
        io::stdin().read_line(&mut input)?;
        let input = input.trim();

        if input.is_empty() {
            continue;
        }

        match process_command(store, input).await {
            Ok(Some(output)) => println!("{}", output),
            Ok(None) => break,
            Err(e) => eprintln!("Error: {}", e),
        }
    }

    Ok(())
}

async fn process_command(
    store: &mut KVStore,
    input: &str,
) -> anyhow::Result<Option<String>> {
    let parts: Vec<&str> = input.split_whitespace().collect();
    if parts.is_empty() {
        return Ok(Some(String::new()));
    }

    match parts[0] {
        "help" => Ok(Some(format_help())),
        "set" => {
            if parts.len() != 3 {
                return Err(anyhow::anyhow!("Usage: set <key> <value>"));
            }
            store
                .set(parts[1].to_string(), parts[2].to_string())
                .await?;
            Ok(Some("OK".to_string()))
        }
        "get" => {
            if parts.len() != 2 {
                return Err(anyhow::anyhow!("Usage: get <key>"));
            }
            let value = store.get(parts[1].to_string()).await?;
            match value {
                Some(v) => Ok(Some(v)),
                None => Ok(Some("(nil)".to_string())),
            }
        }
        "delete" | "del" => {
            if parts.len() != 2 {
                return Err(anyhow::anyhow!("Usage: delete <key>"));
            }
            let value = store.delete(parts[1].to_string()).await?;
            match value {
                Some(v) => Ok(Some(v)),
                None => Ok(Some("(nil)".to_string())),
            }
        }
        "cas" => {
            if parts.len() != 4 {
                return Err(anyhow::anyhow!("Usage: cas <key> <from> <to>"));
            }
            let success = store
                .compare_and_set(
                    parts[1].to_string(),
                    parts[2].to_string(),
                    parts[3].to_string(),
                )
                .await?;
            if success {
                Ok(Some("OK".to_string()))
            } else {
                Ok(Some("Failed: value mismatch".to_string()))
            }
        }
        "exit" | "quit" => Ok(None),
        _ => Err(anyhow::anyhow!(
            "Unknown command: {}. Type 'help' for available commands.",
            parts[0]
        )),
    }
}

fn format_help() -> String {
    r#"Available commands:
  set <key> <value>     - Set a key-value pair
  get <key>             - Get the value for a key
  delete <key>          - Delete a key-value pair
  cas <key> <from> <to> - Compare-and-set operation
  help                  - Show this help message
  exit                  - Exit the CLI"#
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    mod args_parsing {
        use super::*;

        #[test]
        fn test_parse_default_addresses() {
            let cli = Cli {
                servers: "127.0.0.1:1111,127.0.0.1:1112,127.0.0.1:1113"
                    .to_string(),
            };
            let addrs = cli.parse_addresses().unwrap();
            assert_eq!(addrs.len(), 3);
            assert_eq!(addrs[0].port(), 1111);
            assert_eq!(addrs[1].port(), 1112);
            assert_eq!(addrs[2].port(), 1113);
        }

        #[test]
        fn test_parse_single_address() {
            let cli = Cli {
                servers: "127.0.0.1:9999".to_string(),
            };
            let addrs = cli.parse_addresses().unwrap();
            assert_eq!(addrs.len(), 1);
            assert_eq!(addrs[0].port(), 9999);
        }

        #[test]
        fn test_parse_multiple_addresses() {
            let cli = Cli {
                servers: "127.0.0.1:1111,127.0.0.1:1112".to_string(),
            };
            let addrs = cli.parse_addresses().unwrap();
            assert_eq!(addrs.len(), 2);
        }

        #[test]
        fn test_parse_invalid_address() {
            let cli = Cli {
                servers: "invalid:address".to_string(),
            };
            let result = cli.parse_addresses();
            assert!(result.is_err());
        }

        #[test]
        fn test_parse_addresses_with_spaces() {
            let cli = Cli {
                servers: "127.0.0.1:1111, 127.0.0.1:1112, 127.0.0.1:1113"
                    .to_string(),
            };
            let addrs = cli.parse_addresses().unwrap();
            assert_eq!(addrs.len(), 3);
        }

        #[test]
        fn test_parse_empty_address() {
            let cli = Cli {
                servers: "".to_string(),
            };
            let result = cli.parse_addresses();
            assert!(result.is_err());
        }
    }

    mod command_parsing {
        use super::*;

        #[test]
        fn test_format_help() {
            let help = format_help();
            assert!(help.contains("Available commands"));
            assert!(help.contains("set <key> <value>"));
            assert!(help.contains("get <key>"));
            assert!(help.contains("delete <key>"));
            assert!(help.contains("cas <key> <from> <to>"));
            assert!(help.contains("help"));
            assert!(help.contains("exit"));
        }

        #[test]
        fn test_parse_empty_command() {
            let parts: Vec<&str> = "".split_whitespace().collect();
            assert!(parts.is_empty());
        }

        #[test]
        fn test_parse_set_command() {
            let parts: Vec<&str> =
                "set name Alice".split_whitespace().collect();
            assert_eq!(parts.len(), 3);
            assert_eq!(parts[0], "set");
            assert_eq!(parts[1], "name");
            assert_eq!(parts[2], "Alice");
        }

        #[test]
        fn test_parse_get_command() {
            let parts: Vec<&str> = "get name".split_whitespace().collect();
            assert_eq!(parts.len(), 2);
            assert_eq!(parts[0], "get");
            assert_eq!(parts[1], "name");
        }

        #[test]
        fn test_parse_delete_command() {
            let parts: Vec<&str> = "delete name".split_whitespace().collect();
            assert_eq!(parts.len(), 2);
            assert_eq!(parts[0], "delete");
            assert_eq!(parts[1], "name");
        }

        #[test]
        fn test_parse_cas_command() {
            let parts: Vec<&str> =
                "cas key old new".split_whitespace().collect();
            assert_eq!(parts.len(), 4);
            assert_eq!(parts[0], "cas");
            assert_eq!(parts[1], "key");
            assert_eq!(parts[2], "old");
            assert_eq!(parts[3], "new");
        }

        #[test]
        fn test_parse_exit_command() {
            let parts: Vec<&str> = "exit".split_whitespace().collect();
            assert_eq!(parts.len(), 1);
            assert_eq!(parts[0], "exit");
        }

        #[test]
        fn test_parse_quit_command() {
            let parts: Vec<&str> = "quit".split_whitespace().collect();
            assert_eq!(parts.len(), 1);
            assert_eq!(parts[0], "quit");
        }
    }
}
