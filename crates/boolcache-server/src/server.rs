use std::net::SocketAddr;
use std::sync::Arc;

use tokio::net::TcpListener;

use boolcache::db::Db;

use crate::connection::Connection;

pub async fn run(addr: SocketAddr, db: Db) -> std::io::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    tracing::info!("BoolCache listening on {addr}");

    let db = Arc::new(db);

    loop {
        tokio::select! {
            result = listener.accept() => {
                let (stream, _peer) = result?;
                let conn = Connection::new(stream, (*db).clone());
                tokio::spawn(async move { conn.run().await });
            }
            _ = shutdown_signal() => {
                tracing::info!("Signal received, starting graceful shutdown...");
                db.shutdown();
                break;
            }
        }
    }

    Ok(())
}

/// Resolves when SIGINT (Ctrl+C) or SIGTERM is received.
async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}
