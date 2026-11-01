//! Security integration tests: TLS (server auth, mutual TLS, hostname
//! verification), SASL DIGEST-MD5, digest auth failures and credential
//! redaction. Each test launches its own dedicated ZooKeeper server.

use std::path::{Path, PathBuf};
use std::process::Command;

use zk_core::*;
use zk_testkit::{tests_required, unique_path, zookeeper_home, ServerOptions, ZkServer};

fn have_tools() -> bool {
    let ok = zookeeper_home().is_some()
        && Command::new("openssl").arg("version").output().map(|o| o.status.success()).unwrap_or(false)
        && Command::new("keytool").arg("-help").output().is_ok();
    if !ok && tests_required() {
        panic!("security tests need ZooKeeper, openssl and keytool");
    }
    ok
}

fn run(cmd: &mut Command) {
    let out = cmd.output().expect("spawn");
    assert!(out.status.success(), "{:?} failed: {}", cmd, String::from_utf8_lossy(&out.stderr));
}

/// CA, server cert (SAN localhost/127.0.0.1), client cert, and a PKCS12
/// keystore/truststore for the server.
struct Pki {
    dir: tempfile::TempDir,
}

impl Pki {
    fn new() -> Pki {
        let dir = tempfile::tempdir().unwrap();
        let d = dir.path();
        let ossl = |args: &[&str]| run(Command::new("openssl").args(args).current_dir(d));
        ossl(&["req", "-x509", "-newkey", "rsa:2048", "-nodes", "-keyout", "ca.key", "-out", "ca.pem", "-days", "2", "-subj", "/CN=zk-test-ca"]);
        std::fs::write(d.join("server.ext"), "subjectAltName=DNS:localhost,IP:127.0.0.1\nbasicConstraints=CA:FALSE\nkeyUsage=digitalSignature,keyEncipherment\nextendedKeyUsage=serverAuth\n").unwrap();
        std::fs::write(d.join("wrong.ext"), "subjectAltName=DNS:not-this-host.example\nbasicConstraints=CA:FALSE\nextendedKeyUsage=serverAuth\n").unwrap();
        std::fs::write(d.join("client.ext"), "basicConstraints=CA:FALSE\nkeyUsage=digitalSignature,keyEncipherment\nextendedKeyUsage=clientAuth\n").unwrap();
        for (name, ext) in [("server", "server.ext"), ("wrong", "wrong.ext"), ("client", "client.ext")] {
            ossl(&["req", "-newkey", "rsa:2048", "-nodes", "-keyout", &format!("{name}.key"), "-out", &format!("{name}.csr"), "-subj", &format!("/CN={name}")]);
            ossl(&[
                "x509", "-req", "-in", &format!("{name}.csr"), "-CA", "ca.pem", "-CAkey", "ca.key", "-CAcreateserial", "-out",
                &format!("{name}.pem"), "-days", "2", "-extfile", ext,
            ]);
            ossl(&[
                "pkcs12", "-export", "-in", &format!("{name}.pem"), "-inkey", &format!("{name}.key"), "-out", &format!("{name}.p12"),
                "-passout", "pass:changeit", "-name", name,
            ]);
        }
        run(Command::new("keytool").current_dir(d).args([
            "-importcert", "-noprompt", "-alias", "ca", "-file", "ca.pem", "-keystore", "trust.p12", "-storetype", "PKCS12", "-storepass", "changeit",
        ]));
        // A second, unrelated CA for negative tests.
        ossl(&["req", "-x509", "-newkey", "rsa:2048", "-nodes", "-keyout", "other.key", "-out", "other-ca.pem", "-days", "2", "-subj", "/CN=other-ca"]);
        Pki { dir }
    }

    fn p(&self, f: &str) -> PathBuf {
        self.dir.path().join(f)
    }
}

fn tls_server(pki: &Pki, keystore: &str, client_auth: &str) -> ZkServer {
    let secure_port = zk_testkit::free_port();
    let ks = pki.p(keystore);
    let ts = pki.p("trust.p12");
    let opts = ServerOptions {
        secure_client_port: Some(secure_port),
        extra_config: vec![
            "serverCnxnFactory=org.apache.zookeeper.server.NettyServerCnxnFactory".into(),
            format!("ssl.keyStore.location={}", ks.display()),
            "ssl.keyStore.password=changeit".into(),
            "ssl.keyStore.type=PKCS12".into(),
            format!("ssl.trustStore.location={}", ts.display()),
            "ssl.trustStore.password=changeit".into(),
            "ssl.trustStore.type=PKCS12".into(),
            format!("ssl.clientAuth={client_auth}"),
        ],
        ..Default::default()
    };
    ZkServer::start(opts).expect("TLS server")
}

fn tls_cfg(server: &ZkServer, ca: &Path, identity: Option<(&Path, &Path)>, host: &str) -> ClusterConfig {
    let mut c = ClusterConfig::new(format!("{host}:{}", server.secure_port().unwrap()));
    c.connect_deadline_ms = 8_000;
    c.session_timeout_ms = 6_000;
    c.tls = Some(TlsConfig {
        ca_file: Some(ca.to_path_buf()),
        cert_file: identity.map(|i| i.0.to_path_buf()),
        key_file: identity.map(|i| i.1.to_path_buf()),
        verify_hostname: true,
    });
    c
}

#[tokio::test]
async fn tls_server_authentication() {
    if !have_tools() {
        return;
    }
    let pki = Pki::new();
    let server = tls_server(&pki, "server.p12", "none");
    let zk = ZkClient::connect(tls_cfg(&server, &pki.p("ca.pem"), None, "localhost")).await.expect("TLS connect");
    let p = unique_path("tls");
    zk.create(&p, b"secure", &CreateOptions::persistent()).await.unwrap();
    assert_eq!(zk.get_data(&p).await.unwrap().data, b"secure");
    let d = zk.diagnostics();
    assert!(d.tls);
    assert_eq!(d.tls_hostname_verification, Some(true));
    zk.close().await;

    // Also by IP SAN.
    let zk = ZkClient::connect(tls_cfg(&server, &pki.p("ca.pem"), None, "127.0.0.1")).await.expect("TLS via IP SAN");
    zk.close().await;

    // Untrusted CA must fail.
    let e = ZkClient::connect(tls_cfg(&server, &pki.p("other-ca.pem"), None, "localhost")).await.unwrap_err();
    assert!(!matches!(e.kind, ErrorKind::NoNode), "{e}");

    // Plain TCP to the TLS port must fail (not silently downgrade).
    let mut plain = ClusterConfig::new(format!("localhost:{}", server.secure_port().unwrap()));
    plain.connect_deadline_ms = 3_000;
    assert!(ZkClient::connect(plain).await.is_err());
}

#[tokio::test]
async fn tls_hostname_verification() {
    if !have_tools() {
        return;
    }
    let pki = Pki::new();
    let server = tls_server(&pki, "wrong.p12", "none");
    let e = ZkClient::connect(tls_cfg(&server, &pki.p("ca.pem"), None, "localhost")).await.unwrap_err();
    assert!(!matches!(e.kind, ErrorKind::NoNode), "{e}");
    // Explicitly disabled verification connects, and diagnostics say so.
    let mut c = tls_cfg(&server, &pki.p("ca.pem"), None, "localhost");
    c.tls.as_mut().unwrap().verify_hostname = false;
    let zk = ZkClient::connect(c).await.expect("connect without hostname verification");
    assert_eq!(zk.diagnostics().tls_hostname_verification, Some(false));
    zk.close().await;
}

#[tokio::test]
async fn mutual_tls() {
    if !have_tools() {
        return;
    }
    let pki = Pki::new();
    let server = tls_server(&pki, "server.p12", "need");
    let ident = (pki.p("client.pem"), pki.p("client.key"));
    let zk = ZkClient::connect(tls_cfg(&server, &pki.p("ca.pem"), Some((&ident.0, &ident.1)), "localhost"))
        .await
        .expect("mTLS connect");
    // The x509 provider authenticates the client certificate subject.
    let who = zk.whoami().await.unwrap();
    assert!(who.iter().any(|w| w.scheme == "x509"), "{who:?}");
    zk.close().await;
    // Without a client certificate the handshake is rejected.
    let r = ZkClient::connect(tls_cfg(&server, &pki.p("ca.pem"), None, "localhost")).await;
    assert!(r.is_err(), "connect without client cert must fail");
    // Mismatched cert/key configuration is rejected before connecting.
    let mut c = tls_cfg(&server, &pki.p("ca.pem"), None, "localhost");
    c.tls.as_mut().unwrap().cert_file = Some(ident.0.clone());
    assert_eq!(ZkClient::connect(c).await.unwrap_err().kind, ErrorKind::Config);
}

fn sasl_server(enforce: bool) -> ZkServer {
    let jaas = "Server {\n  org.apache.zookeeper.server.auth.DigestLoginModule required\n  user_alice=\"wonderland\";\n};\n";
    let dir = tempfile::tempdir().unwrap().keep();
    let jaas_path = dir.join("jaas.conf");
    std::fs::write(&jaas_path, jaas).unwrap();
    let mut jvm = vec![
        format!("-Djava.security.auth.login.config={}", jaas_path.display()),
        "-Dzookeeper.authProvider.1=org.apache.zookeeper.server.auth.SASLAuthenticationProvider".into(),
        // ZooKeeper 3.9 defaults to FIPS mode, which refuses DIGEST-MD5 on the server side.
        "-Dzookeeper.fips-mode=false".into(),
    ];
    if enforce {
        jvm.push("-Dzookeeper.enforce.auth.enabled=true".into());
        jvm.push("-Dzookeeper.enforce.auth.schemes=sasl".into());
        jvm.push("-Dzookeeper.sessionRequireClientSASLAuth=true".into());
    }
    ZkServer::start(ServerOptions { jvm_args: jvm, ..Default::default() }).expect("SASL server")
}

fn sasl_cfg(server: &ZkServer, user: &str, pw: &str) -> ClusterConfig {
    let mut c = ClusterConfig::new(format!("localhost:{}", server.port()));
    c.connect_deadline_ms = 8_000;
    c.sasl = Some(SaslConfig::DigestMd5 { username: user.into(), password: Secret::Value(pw.into()) });
    c
}

#[tokio::test]
async fn sasl_digest_md5() {
    if !have_tools() {
        return;
    }
    let server = sasl_server(false);
    let zk = ZkClient::connect(sasl_cfg(&server, "alice", "wonderland")).await.expect("SASL connect");
    let who = zk.whoami().await.unwrap();
    assert!(who.iter().any(|w| w.scheme == "sasl" && w.user == "alice"), "{who:?}");
    let p = unique_path("sasl");
    zk.create(&p, b"", &CreateOptions::persistent().with_acl(vec![Acl::new(Perms::ALL, "sasl", "alice")])).await.unwrap();
    let anon = ZkClient::connect(ClusterConfig::new(server.address())).await.unwrap();
    assert_eq!(anon.get_data(&p).await.unwrap_err().kind, ErrorKind::NoAuth);
    anon.close().await;
    assert_eq!(zk.diagnostics().sasl.as_deref(), Some("DIGEST-MD5"));
    zk.close().await;

    // Wrong password is an auth failure, not a hang.
    let mut c = sasl_cfg(&server, "alice", "nope");
    c.connect_deadline_ms = 5_000;
    let e = ZkClient::connect(c).await.unwrap_err();
    assert!(matches!(e.kind, ErrorKind::AuthFailed | ErrorKind::Closed | ErrorKind::Timeout), "{e}");
    assert!(!e.to_string().contains("nope"));
}

#[tokio::test]
async fn enforced_sasl_rejects_anonymous() {
    if !have_tools() {
        return;
    }
    let server = sasl_server(true);
    let zk = ZkClient::connect(sasl_cfg(&server, "alice", "wonderland")).await.expect("SASL connect");
    zk.exists("/").await.unwrap();
    zk.close().await;
    let mut anon = ClusterConfig::new(server.address());
    anon.connect_deadline_ms = 5_000;
    match ZkClient::connect(anon).await {
        Err(e) => assert!(matches!(e.kind, ErrorKind::AuthFailed | ErrorKind::Closed | ErrorKind::Timeout), "{e}"),
        Ok(c) => {
            // Some versions accept the session then fail the first request.
            let e = c.exists("/").await.unwrap_err();
            assert!(matches!(e.kind, ErrorKind::AuthFailed | ErrorKind::Closed | ErrorKind::NoAuth | ErrorKind::SessionExpired), "{e}");
            c.close().await;
        },
    }
}

#[tokio::test]
async fn digest_auth_failure_is_terminal_and_redacted() {
    if !have_tools() {
        return;
    }
    let server = zk_testkit::shared_server().unwrap();
    // Unknown scheme makes the server fail authentication.
    let mut c = ClusterConfig::new(server.address());
    c.connect_deadline_ms = 5_000;
    c.auth.push(AuthCredential { scheme: "no-such-scheme".into(), credential: Secret::Value("s3cr3t-value".into()) });
    let r = ZkClient::connect(c).await;
    let e = match r {
        Err(e) => e,
        Ok(zk) => {
            let e = zk.exists("/").await.unwrap_err();
            zk.close().await;
            e
        },
    };
    assert!(matches!(e.kind, ErrorKind::AuthFailed | ErrorKind::Closed), "{e}");
    assert!(!e.to_string().contains("s3cr3t-value"));
}
