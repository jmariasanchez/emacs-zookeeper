//! `zk-bridge`: JSON-RPC 2.0 over stdio for ZooKeeper. See docs/api/protocol.md.

mod dispatch;
mod ext;
mod rpc;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio_util::sync::CancellationToken;

use dispatch::{Bridge, Limits, PROTOCOL_VERSION};
use rpc::{id_key, Incoming, Outbox, RpcError};

struct Args {
    max_request_bytes: usize,
    max_concurrency: usize,
    watch_rate: u32,
}

fn parse_args() -> Args {
    let mut a = Args { max_request_bytes: 64 << 20, max_concurrency: 256, watch_rate: 500 };
    let mut it = std::env::args().skip(1);
    while let Some(arg) = it.next() {
        let mut num = |name: &str| -> usize {
            it.next().and_then(|v| v.parse().ok()).unwrap_or_else(|| {
                eprintln!("zk-bridge: {name} needs a numeric argument");
                std::process::exit(2)
            })
        };
        match arg.as_str() {
            "--max-request-bytes" => a.max_request_bytes = num("--max-request-bytes"),
            "--max-concurrency" => a.max_concurrency = num("--max-concurrency").max(1),
            "--watch-rate" => a.watch_rate = num("--watch-rate") as u32,
            "--version" | "-V" => {
                println!("zk-bridge {} (protocol {PROTOCOL_VERSION})", env!("CARGO_PKG_VERSION"));
                std::process::exit(0)
            },
            "--help" | "-h" => {
                println!(
                    "zk-bridge {}\nJSON-RPC 2.0 over stdio bridge for Apache ZooKeeper.\n\n\
                     Options:\n  --max-request-bytes N   (default 67108864)\n  --max-concurrency N     (default 256)\n  \
                     --watch-rate N          events/s per watch (default 500)\n  --version\n\n\
                     Environment:\n  ZK_BRIDGE_LOG        log level for stderr (default warn)\n  \
                     ZK_BRIDGE_AUDIT_LOG  append audit records as JSON lines to this file",
                    env!("CARGO_PKG_VERSION")
                );
                std::process::exit(0)
            },
            other => {
                eprintln!("zk-bridge: unknown argument {other}");
                std::process::exit(2)
            },
        }
    }
    a
}

fn main() {
    let args = parse_args();
    let filter = std::env::var("ZK_BRIDGE_LOG").unwrap_or_else(|_| "warn".into());
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::new(filter))
        .with_writer(std::io::stderr)
        .with_ansi(false)
        .init();
    let rt = tokio::runtime::Builder::new_multi_thread().enable_all().build().expect("tokio runtime");
    let code = rt.block_on(run(args));
    rt.shutdown_timeout(std::time::Duration::from_secs(2));
    std::process::exit(code);
}

async fn run(args: Args) -> i32 {
    let (outbox, mut rx) = Outbox::new();
    let writer = tokio::spawn(async move {
        let mut out = tokio::io::stdout();
        while let Some(line) = rx.recv().await {
            if out.write_all(line.as_bytes()).await.is_err() || out.write_all(b"\n").await.is_err() {
                break;
            }
            // Flush when there is nothing else queued, batching bursts.
            if rx.is_empty() && out.flush().await.is_err() {
                break;
            }
        }
        let _ = out.flush().await;
    });

    let bridge = Bridge::new(outbox.clone(), Limits { watch_rate: args.watch_rate });
    let inflight: Arc<Mutex<HashMap<String, CancellationToken>>> = Arc::default();
    let permits = Arc::new(tokio::sync::Semaphore::new(args.max_concurrency));
    let mut reader = BufReader::with_capacity(1 << 16, tokio::io::stdin());
    let mut line = Vec::new();
    let mut initialized = false;
    let mut exit_code = 0;
    loop {
        line.clear();
        let n = match read_line_limited(&mut reader, &mut line, args.max_request_bytes).await {
            Ok(n) => n,
            Err(LineError::TooLong) => {
                outbox.respond(Value::Null, Err(RpcError::new(rpc::INVALID_REQUEST, "request exceeds --max-request-bytes")));
                continue;
            },
            Err(LineError::Io) => break,
        };
        if n == 0 {
            break; // EOF
        }
        let text = String::from_utf8_lossy(&line);
        let text = text.trim();
        if text.is_empty() {
            continue;
        }
        let msg: Incoming = match serde_json::from_str(text) {
            Ok(m) => m,
            Err(e) => {
                // Distinguish malformed JSON from a JSON value that is not a request.
                let code = if serde_json::from_str::<Value>(text).is_ok() { rpc::INVALID_REQUEST } else { rpc::PARSE_ERROR };
                outbox.respond(Value::Null, Err(RpcError::new(code, e.to_string())));
                continue;
            },
        };
        if msg.jsonrpc.as_deref().is_some_and(|v| v != "2.0") {
            if let Some(id) = msg.id {
                outbox.respond(id, Err(RpcError::new(rpc::INVALID_REQUEST, "jsonrpc must be \"2.0\"")));
            }
            continue;
        }
        match msg.method.as_str() {
            "$/cancelRequest" => {
                if let Some(id) = msg.params.get("id") {
                    if let Some(tok) = inflight.lock().unwrap().get(&id_key(id)) {
                        tok.cancel();
                    }
                }
                continue;
            },
            "initialize" => {
                let Some(id) = msg.id else { continue };
                let client_ver = msg.params.get("protocol_version").and_then(Value::as_str).unwrap_or(PROTOCOL_VERSION);
                let major = |v: &str| v.split('.').next().unwrap_or("").to_string();
                if major(client_ver) != major(PROTOCOL_VERSION) {
                    outbox.respond(
                        id,
                        Err(RpcError::new(rpc::INVALID_REQUEST, format!("unsupported protocol version {client_ver}; bridge speaks {PROTOCOL_VERSION}"))),
                    );
                    exit_code = 3;
                    break;
                }
                initialized = true;
                outbox.respond(
                    id,
                    Ok(json!({
                        "server": "zk-bridge",
                        "server_version": env!("CARGO_PKG_VERSION"),
                        "protocol_version": PROTOCOL_VERSION,
                        "methods": Bridge::method_names(),
                        "features": {"tls": true, "sasl_digest_md5": true, "sasl_gssapi": cfg!(feature = "gssapi")},
                        "pid": std::process::id(),
                    })),
                );
                continue;
            },
            "shutdown" => {
                bridge.shutdown().await;
                if let Some(id) = msg.id {
                    outbox.respond(id, Ok(Value::Null));
                }
                break;
            },
            _ => {},
        }
        let Some(id) = msg.id else {
            // Unknown client notification: ignore per JSON-RPC.
            continue;
        };
        if !initialized {
            tracing::debug!("request before initialize; accepting");
        }
        let token = CancellationToken::new();
        let key = id_key(&id);
        inflight.lock().unwrap().insert(key.clone(), token.clone());
        let (b, out, infl, permits) = (bridge.clone(), outbox.clone(), inflight.clone(), permits.clone());
        let method = msg.method;
        let params = msg.params;
        tokio::spawn(async move {
            let _permit = permits.acquire_owned().await;
            // Run the handler in its own task so a panic becomes an internal error
            // response instead of a request that never completes.
            let mut handler = tokio::spawn(async move { b.dispatch(&method, params).await });
            let result = tokio::select! {
                _ = token.cancelled() => {
                    handler.abort();
                    Err(RpcError::new(rpc::REQUEST_CANCELLED, "request cancelled"))
                }
                r = &mut handler => match r {
                    Ok(r) => r,
                    Err(e) => Err(RpcError::internal(format!("handler failed: {e}"))),
                },
            };
            infl.lock().unwrap().remove(&key);
            out.respond(id, result);
        });
    }
    // EOF or shutdown: close everything (deletes our ephemerals).
    bridge.shutdown().await;
    drop(bridge);
    drop(outbox);
    let _ = tokio::time::timeout(std::time::Duration::from_secs(2), writer).await;
    exit_code
}

enum LineError {
    TooLong,
    Io,
}

/// Reads one `\n`-terminated line into `buf` without letting it grow past `max`.
/// On overflow the rest of the line is discarded.
async fn read_line_limited<R: tokio::io::AsyncBufRead + Unpin>(r: &mut R, buf: &mut Vec<u8>, max: usize) -> Result<usize, LineError> {
    let mut total = 0usize;
    let mut overflow = false;
    loop {
        let chunk = r.fill_buf().await.map_err(|_| LineError::Io)?;
        if chunk.is_empty() {
            return if overflow { Err(LineError::TooLong) } else { Ok(total) };
        }
        let (take, done) = match chunk.iter().position(|&b| b == b'\n') {
            Some(i) => (i + 1, true),
            None => (chunk.len(), false),
        };
        if !overflow {
            if buf.len() + take > max {
                overflow = true;
                buf.clear();
            } else {
                buf.extend_from_slice(&chunk[..take]);
            }
        }
        total += take;
        r.consume(take);
        if done {
            return if overflow { Err(LineError::TooLong) } else { Ok(total) };
        }
    }
}
