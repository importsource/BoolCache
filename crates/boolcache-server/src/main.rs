mod connection;
mod replication;
mod resp;
mod server;

use std::net::SocketAddr;
use std::path::PathBuf;

use clap::Parser;

use boolcache::db::{Config, Db};
use boolcache::persistence::aof::FsyncPolicy;
use boolcache::replication::state::Role;

#[derive(Parser, Debug)]
#[command(name = "boolcache-server", about = "Redis-compatible in-memory cache server")]
struct Args {
    #[arg(long, default_value = "127.0.0.1")]
    host: String,

    #[arg(long, short, default_value_t = 6379)]
    port: u16,

    #[arg(long, default_value = ".")]
    dir: PathBuf,

    #[arg(long)]
    no_aof: bool,

    #[arg(long, default_value = "everysec")]
    aof_fsync: String,

    #[arg(long, default_value = "info")]
    log_level: String,

    /// Connect to a primary as a replica: --replicaof 127.0.0.1:6379
    #[arg(long, value_name = "HOST:PORT")]
    replicaof: Option<String>,
}

fn print_banner(host: &str, port: u16, dir: &std::path::Path, aof_enabled: bool, role: &str) {
    let version = env!("CARGO_PKG_VERSION");
    println!(r#"
  ____              _ ____           _
 | __ )  ___   ___ | / ___|__ _  ___| |__   ___
 |  _ \ / _ \ / _ \| \___ / _` |/ __| '_ \ / _ \
 | |_) | (_) | (_) | |___) (_| | (__| | | |  __/
 |____/ \___/ \___/|_|____\__,_|\___|_| |_|\___|
"#);
    println!("  Version   : {version}");
    println!("  PID       : {}", std::process::id());
    println!("  Address   : {host}:{port}");
    println!("  Data dir  : {}", dir.display());
    println!("  AOF       : {}", if aof_enabled { "enabled" } else { "disabled" });
    println!("  RDB       : enabled");
    println!("  Role      : {role}");
    println!();
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    // Parse --replicaof before setting up tracing so banner shows correct role.
    let replicaof: Option<(String, u16)> = args.replicaof.as_deref().and_then(|s| {
        let (h, p) = s.rsplit_once(':')?;
        let port: u16 = p.parse().ok()?;
        Some((h.to_string(), port))
    });

    let role_label = if replicaof.is_some() { "replica" } else { "standalone / primary" };
    print_banner(&args.host, args.port, &args.dir, !args.no_aof, role_label);

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| args.log_level.as_str().into()),
        )
        .init();

    let fsync = match args.aof_fsync.as_str() {
        "always" => FsyncPolicy::Always,
        "no"     => FsyncPolicy::No,
        _        => FsyncPolicy::EverySec,
    };

    let config = Config {
        dir: args.dir,
        aof_enabled: !args.no_aof,
        aof_fsync: fsync,
        save_rules: vec![(900, 1), (300, 10), (60, 10_000)],
    };

    let db = match Db::open(config) {
        Ok(db) => db,
        Err(e) => { eprintln!("Fatal: {e}"); std::process::exit(1); }
    };

    // If --replicaof was given, configure role and start sync task.
    if let Some((primary_host, primary_port)) = replicaof {
        *db.replication.role.write() = Role::Replica {
            primary_host: primary_host.clone(),
            primary_port,
        };
        replication::replica::start(primary_host, primary_port, db.clone());
    }

    let addr: SocketAddr = format!("{}:{}", args.host, args.port)
        .parse()
        .expect("invalid address");

    if let Err(e) = server::run(addr, db).await {
        eprintln!("Server error: {e}");
        std::process::exit(1);
    }
}
