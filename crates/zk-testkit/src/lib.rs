//! Test harness for running against real Apache ZooKeeper servers.
//!
//! * [`ZkServer`] launches a standalone server from a ZooKeeper binary
//!   distribution (`ZOOKEEPER_HOME`, or the newest `apache-zookeeper-*-bin`
//!   under `~/.cache/zk`). The JVM is wrapped in a shell guard so it dies with
//!   the test process even if destructors do not run.
//! * [`shared_server`] returns one lazily started server per test binary.
//! * [`FaultProxy`] is a TCP proxy that can drop connections, black-hole
//!   traffic (partition) and refuse new connections, for fault injection.
//!
//! Tests should call [`require_server!`] which skips (passes with a note)
//! when no distribution is available, unless `ZK_TESTS_REQUIRED=1`.

use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

/// Locates a ZooKeeper binary distribution.
pub fn zookeeper_home() -> Option<PathBuf> {
    if let Ok(h) = std::env::var("ZOOKEEPER_HOME") {
        let p = PathBuf::from(h);
        if p.join("lib").is_dir() {
            return Some(p);
        }
    }
    let cache = dirs_home()?.join(".cache").join("zk");
    let mut candidates: Vec<PathBuf> = std::fs::read_dir(&cache)
        .ok()?
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| {
            p.file_name().and_then(|n| n.to_str()).map(|n| n.starts_with("apache-zookeeper-") && n.ends_with("-bin")).unwrap_or(false)
                && p.join("lib").is_dir()
        })
        .collect();
    candidates.sort();
    candidates.pop()
}

fn dirs_home() -> Option<PathBuf> {
    std::env::var_os("HOME").map(PathBuf::from)
}

/// Picks a free local TCP port.
pub fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

/// Options for launching a server.
#[derive(Debug, Clone, Default)]
pub struct ServerOptions {
    /// Extra `zoo.cfg` lines.
    pub extra_config: Vec<String>,
    /// Extra JVM arguments (e.g. `-Dzookeeper.authProvider.1=...`).
    pub jvm_args: Vec<String>,
    /// Files written into the data directory before start: (file name, content).
    pub files: Vec<(String, String)>,
    /// Minimum session timeout in ms (`minSessionTimeout`), lets tests use short sessions.
    pub min_session_timeout_ms: Option<u64>,
    /// Also listen for TLS clients on this port (`secureClientPort`). Requires
    /// `ssl.*` settings in `extra_config` and the Netty connection factory.
    pub secure_client_port: Option<u16>,
}

/// A running standalone ZooKeeper server.
pub struct ZkServer {
    child: Mutex<Option<Child>>,
    port: u16,
    dir: tempfile::TempDir,
    home: PathBuf,
    options: ServerOptions,
    version: String,
}

impl std::fmt::Debug for ZkServer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ZkServer").field("port", &self.port).field("version", &self.version).finish()
    }
}

impl ZkServer {
    /// Starts a server, or returns `None` if no distribution is available.
    pub fn start(options: ServerOptions) -> Option<ZkServer> {
        let home = zookeeper_home()?;
        let dir = tempfile::Builder::new().prefix("zk-testkit-").tempdir().expect("tempdir");
        let port = free_port();
        let version = home
            .file_name()
            .and_then(|n| n.to_str())
            .and_then(|n| n.strip_prefix("apache-zookeeper-"))
            .and_then(|n| n.strip_suffix("-bin"))
            .unwrap_or("unknown")
            .to_string();
        let server = ZkServer { child: Mutex::new(None), port, dir, home, options, version };
        server.write_config();
        server.launch();
        server.wait_ready(Duration::from_secs(60)).then_some(server)
    }

    fn data_dir(&self) -> PathBuf {
        self.dir.path().join("data")
    }

    fn write_config(&self) {
        std::fs::create_dir_all(self.data_dir()).unwrap();
        let mut cfg = format!(
            "tickTime=500\ninitLimit=10\nsyncLimit=5\ndataDir={}\nclientPort={}\nclientPortAddress=127.0.0.1\nadmin.enableServer=false\n\
             4lw.commands.whitelist=*\nextendedTypesEnabled=true\nemulate353TTLNodes=false\nmaxClientCnxns=0\n",
            self.data_dir().display(),
            self.port
        );
        if let Some(ms) = self.options.min_session_timeout_ms {
            cfg.push_str(&format!("minSessionTimeout={ms}\n"));
        }
        if let Some(p) = self.options.secure_client_port {
            cfg.push_str(&format!("secureClientPort={p}\nsecureClientPortAddress=127.0.0.1\n"));
        }
        for l in &self.options.extra_config {
            cfg.push_str(l);
            cfg.push('\n');
        }
        std::fs::write(self.dir.path().join("zoo.cfg"), cfg).unwrap();
        for (name, content) in &self.options.files {
            std::fs::write(self.dir.path().join(name), content).unwrap();
        }
        let log4j = "<configuration><appender name=\"F\" class=\"ch.qos.logback.core.FileAppender\"><file>${zookeeper.log.dir}/zk.log</file>\
                     <encoder><pattern>%d %-5level [%thread] %logger{36} - %msg%n</pattern></encoder></appender>\
                     <root level=\"INFO\"><appender-ref ref=\"F\"/></root></configuration>";
        std::fs::write(self.dir.path().join("logback.xml"), log4j).unwrap();
    }

    fn launch(&self) {
        let java = std::env::var("JAVA").unwrap_or_else(|_| {
            std::env::var("JAVA_HOME").map(|h| format!("{h}/bin/java")).unwrap_or_else(|_| "java".into())
        });
        let cp = format!("{}/lib/*:{}", self.home.display(), self.dir.path().display());
        let mut args: Vec<String> = vec![
            "-Xmx256m".into(),
            format!("-Dzookeeper.log.dir={}", self.dir.path().display()),
            format!("-Dlogback.configurationFile={}/logback.xml", self.dir.path().display()),
            "-Dzookeeper.admin.enableServer=false".into(),
            // Reap containers and TTL nodes quickly so tests can observe it.
            "-Dznode.container.checkIntervalMs=500".into(),
            "-Dznode.container.maxPerMinute=100000".into(),
        ];
        args.extend(self.options.jvm_args.iter().cloned());
        args.push("-cp".into());
        args.push(cp);
        args.push("org.apache.zookeeper.server.quorum.QuorumPeerMain".into());
        args.push(self.dir.path().join("zoo.cfg").display().to_string());
        let quoted: Vec<String> = std::iter::once(java).chain(args).map(|a| shell_quote(&a)).collect();
        // Guard: kill the JVM when our stdin pipe closes (i.e. when the test process exits).
        let script = format!("{} >/dev/null 2>&1 & pid=$!; read _ignored; kill -9 $pid 2>/dev/null; wait $pid 2>/dev/null", quoted.join(" "));
        let child = Command::new("sh")
            .arg("-c")
            .arg(script)
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn zookeeper");
        *self.child.lock().unwrap() = Some(child);
    }

    fn wait_ready(&self, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            if four_letter_sync(&self.address(), "ruok").map(|r| r.trim() == "imok").unwrap_or(false) {
                // `ruok` answers before the server is serving; check srvr.
                if four_letter_sync(&self.address(), "srvr").map(|r| r.contains("Mode:")).unwrap_or(false) {
                    return true;
                }
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        eprintln!("zk-testkit: server on port {} did not become ready; log:\n{}", self.port, self.log_tail(40));
        false
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    pub fn secure_port(&self) -> Option<u16> {
        self.options.secure_client_port
    }

    /// `127.0.0.1:port`
    pub fn address(&self) -> String {
        format!("127.0.0.1:{}", self.port)
    }

    pub fn version(&self) -> &str {
        &self.version
    }

    /// Whether the server version is at least `major.minor`.
    pub fn at_least(&self, major: u32, minor: u32) -> bool {
        let mut it = self.version.split(['.', '-']).map(|p| p.parse::<u32>().unwrap_or(0));
        let (a, b) = (it.next().unwrap_or(0), it.next().unwrap_or(0));
        (a, b) >= (major, minor)
    }

    pub fn dir(&self) -> &Path {
        self.dir.path()
    }

    pub fn log_tail(&self, lines: usize) -> String {
        let s = std::fs::read_to_string(self.dir.path().join("zk.log")).unwrap_or_default();
        let v: Vec<&str> = s.lines().collect();
        v[v.len().saturating_sub(lines)..].join("\n")
    }

    /// Stops the server (kills the JVM). Data is kept.
    pub fn stop(&self) {
        if let Some(mut child) = self.child.lock().unwrap().take() {
            drop(child.stdin.take());
            let _ = child.wait();
        }
        // Wait until the port is closed.
        let deadline = Instant::now() + Duration::from_secs(10);
        while Instant::now() < deadline && TcpStream::connect(self.address()).is_ok() {
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    /// Restarts on the same port with the same data.
    pub fn restart(&self) -> bool {
        self.stop();
        self.launch();
        self.wait_ready(Duration::from_secs(60))
    }
}

impl Drop for ZkServer {
    fn drop(&mut self) {
        self.stop();
    }
}

fn shell_quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', "'\\''"))
}

/// Synchronous four-letter word helper.
pub fn four_letter_sync(addr: &str, cmd: &str) -> std::io::Result<String> {
    let mut s = TcpStream::connect_timeout(&addr.parse().unwrap(), Duration::from_secs(2))?;
    s.set_read_timeout(Some(Duration::from_secs(2)))?;
    s.write_all(cmd.as_bytes())?;
    let mut out = String::new();
    let mut r = BufReader::new(s);
    let mut line = String::new();
    while r.read_line(&mut line)? > 0 {
        out.push_str(&line);
        line.clear();
    }
    Ok(out)
}

static SHARED: OnceLock<Option<ZkServer>> = OnceLock::new();

/// One server shared by all tests of a test binary (started on first use).
/// Tests must isolate themselves by using unique paths (see [`unique_path`]).
pub fn shared_server() -> Option<&'static ZkServer> {
    SHARED
        .get_or_init(|| ZkServer::start(ServerOptions { min_session_timeout_ms: Some(1000), ..Default::default() }))
        .as_ref()
}

static COUNTER: AtomicU64 = AtomicU64::new(0);

/// A unique absolute path for test isolation, e.g. `/t-<pid>-<n>-<name>`.
pub fn unique_path(name: &str) -> String {
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let clean: String = name.chars().map(|c| if c.is_ascii_alphanumeric() || c == '-' { c } else { '-' }).collect();
    format!("/t-{}-{}-{}", std::process::id(), n, clean)
}

/// Whether missing ZooKeeper must fail tests instead of skipping.
pub fn tests_required() -> bool {
    std::env::var("ZK_TESTS_REQUIRED").map(|v| v == "1" || v == "true").unwrap_or(false)
}

/// Skips the current test when the shared server is older than `major.minor`.
#[macro_export]
macro_rules! require_version {
    ($server:expr, $major:expr, $minor:expr) => {
        if !$server.at_least($major, $minor) {
            eprintln!("skipping: requires ZooKeeper {}.{}+, server is {}", $major, $minor, $server.version());
            return;
        }
    };
}

/// Returns the shared server or skips the current test.
#[macro_export]
macro_rules! require_server {
    () => {
        match $crate::shared_server() {
            Some(s) => s,
            None => {
                if $crate::tests_required() {
                    panic!("ZooKeeper distribution not found (set ZOOKEEPER_HOME) and ZK_TESTS_REQUIRED=1");
                }
                eprintln!("skipping: no ZooKeeper distribution (set ZOOKEEPER_HOME)");
                return;
            },
        }
    };
}

// ------------------------------------------------------------------ proxy

/// Proxy behavior.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProxyMode {
    /// Forward traffic.
    Pass,
    /// Keep connections open but forward nothing (simulates a partition: the
    /// client sees timeouts rather than resets).
    Blackhole,
    /// Refuse (immediately close) new connections.
    Refuse,
}

struct ProxyState {
    mode: Mutex<ProxyMode>,
    epoch: AtomicU64,
    stopped: AtomicBool,
    accepted: AtomicU64,
}

/// A fault-injecting TCP proxy in front of a server. Runs on the current tokio runtime.
pub struct FaultProxy {
    port: u16,
    state: Arc<ProxyState>,
}

impl FaultProxy {
    pub async fn start(upstream: String) -> FaultProxy {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let state = Arc::new(ProxyState {
            mode: Mutex::new(ProxyMode::Pass),
            epoch: AtomicU64::new(0),
            stopped: AtomicBool::new(false),
            accepted: AtomicU64::new(0),
        });
        let st = state.clone();
        tokio::spawn(async move {
            loop {
                let Ok((inbound, _)) = listener.accept().await else { return };
                if st.stopped.load(Ordering::SeqCst) {
                    return;
                }
                st.accepted.fetch_add(1, Ordering::Relaxed);
                if *st.mode.lock().unwrap() == ProxyMode::Refuse {
                    drop(inbound);
                    continue;
                }
                let st2 = st.clone();
                let up = upstream.clone();
                tokio::spawn(async move { proxy_conn(inbound, up, st2).await });
            }
        });
        FaultProxy { port, state }
    }

    pub fn address(&self) -> String {
        format!("127.0.0.1:{}", self.port)
    }

    pub fn set_mode(&self, mode: ProxyMode) {
        *self.state.mode.lock().unwrap() = mode;
    }

    pub fn mode(&self) -> ProxyMode {
        *self.state.mode.lock().unwrap()
    }

    /// Closes every open connection.
    pub fn drop_connections(&self) {
        self.state.epoch.fetch_add(1, Ordering::SeqCst);
    }

    /// Number of accepted connections so far.
    pub fn accepted(&self) -> u64 {
        self.state.accepted.load(Ordering::Relaxed)
    }
}

impl Drop for FaultProxy {
    fn drop(&mut self) {
        self.state.stopped.store(true, Ordering::SeqCst);
        self.state.epoch.fetch_add(1, Ordering::SeqCst);
    }
}

async fn proxy_conn(inbound: tokio::net::TcpStream, upstream: String, st: Arc<ProxyState>) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let Ok(outbound) = tokio::net::TcpStream::connect(&upstream).await else { return };
    let epoch = st.epoch.load(Ordering::SeqCst);
    let (mut ir, mut iw) = inbound.into_split();
    let (mut or, mut ow) = outbound.into_split();
    let st_a = st.clone();
    let st_b = st.clone();
    let alive = move |s: &ProxyState| s.epoch.load(Ordering::SeqCst) == epoch && !s.stopped.load(Ordering::SeqCst);
    let a = async move {
        let mut buf = vec![0u8; 16 * 1024];
        loop {
            let n = tokio::select! {
                r = ir.read(&mut buf) => match r { Ok(0) | Err(_) => return, Ok(n) => n },
                _ = tokio::time::sleep(Duration::from_millis(50)) => { if !alive(&st_a) { return } continue }
            };
            if !alive(&st_a) {
                return;
            }
            while *st_a.mode.lock().unwrap() == ProxyMode::Blackhole {
                tokio::time::sleep(Duration::from_millis(20)).await;
                if !alive(&st_a) {
                    return;
                }
            }
            if ow.write_all(&buf[..n]).await.is_err() {
                return;
            }
        }
    };
    let b = async move {
        let mut buf = vec![0u8; 16 * 1024];
        loop {
            let n = tokio::select! {
                r = or.read(&mut buf) => match r { Ok(0) | Err(_) => return, Ok(n) => n },
                _ = tokio::time::sleep(Duration::from_millis(50)) => { if !alive(&st_b) { return } continue }
            };
            if !alive(&st_b) {
                return;
            }
            while *st_b.mode.lock().unwrap() == ProxyMode::Blackhole {
                tokio::time::sleep(Duration::from_millis(20)).await;
                if !alive(&st_b) {
                    return;
                }
            }
            if iw.write_all(&buf[..n]).await.is_err() {
                return;
            }
        }
    };
    tokio::select! { _ = a => {}, _ = b => {} }
}

impl FaultProxy {
    /// Partitions the client from the server for longer than `session_timeout`
    /// so that the server expires the session, then heals the partition.
    /// This is the deterministic way to force expiry for a client connected
    /// through this proxy.
    pub async fn partition_until_expired(&self, session_timeout: Duration) {
        self.set_mode(ProxyMode::Refuse);
        self.drop_connections();
        tokio::time::sleep(session_timeout * 2 + Duration::from_millis(1500)).await;
        self.set_mode(ProxyMode::Pass);
    }
}

/// Polls `f` until it returns true or the timeout elapses.
pub async fn eventually<F: FnMut() -> bool>(timeout: Duration, mut f: F) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if f() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    f()
}
