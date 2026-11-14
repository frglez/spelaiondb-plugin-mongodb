//! The conformance set of SpelaionDB's plugin protocol, run against this plugin's own program
//! (ADR-042), through requests (protocol 1.1, ADR-056): it starts, speaks the protocol's version,
//! opens sessions, returns results in the order the contract says, fails without hanging, obeys
//! a cancel while a request runs and ends when its input closes. What documents look like is
//! `tests/mongodb.rs`.
//!
//! It needs the test servers (`ci/services.sh`, `SPELAION_TEST_MONGO=127.0.0.1:27018` and
//! `SPELAION_TEST_FERRETDB=127.0.0.1:27019`). Without the variables it is skipped, which does
//! not count as tested.

use mongodb::bson::doc;
use serde_json::json;
use spelaion_driver_api::{ConnectParams, Secret};
use spelaion_plugin_host::conformance::{RequestTarget, assert_conforms_requests};

const PROGRAM: &str = env!("CARGO_BIN_EXE_spelaiondb-plugin-mongodb");

fn params(addr: &str) -> ConnectParams {
    let (host, port) = addr.rsplit_once(':').expect("host:port");
    ConnectParams {
        host: host.into(),
        port: port.parse().expect("a port"),
        user: "spelaion".into(),
        password: Some(Secret::new("spelaion")),
        database: "spelaion".into(),
        ..Default::default()
    }
}

#[tokio::test]
async fn the_plugin_conforms_on_mongodb() {
    let Ok(addr) = std::env::var("SPELAION_TEST_MONGO") else {
        eprintln!("SPELAION_TEST_MONGO not set: test skipped");
        return;
    };
    // One document for the slow request to wait on.
    let client = mongodb::Client::with_uri_str(format!(
        "mongodb://spelaion:spelaion@{addr}/?directConnection=true"
    ))
    .await
    .unwrap();
    let slow = client
        .database("spelaion_conformance")
        .collection::<mongodb::bson::Document>("slow");
    slow.drop().await.unwrap();
    slow.insert_one(doc! { "_id": 1 }).await.unwrap();

    let target = RequestTarget {
        program: PROGRAM.into(),
        engine: "mongodb".into(),
        params: params(&addr),
        one_row: json!({"database": "admin", "command": {"ping": 1}}),
        refused: json!({"database": "spelaion", "command": {"noSuchCommand": 1}}),
        // A minute, unless it is cancelled.
        slow: Some(json!({"database": "spelaion_conformance", "command": {
            "find": "slow", "filter": {"$where": "sleep(60000); return true;"}
        }})),
    };
    assert_conforms_requests(&target).await;
}

#[tokio::test]
async fn the_plugin_conforms_on_ferretdb() {
    let Ok(addr) = std::env::var("SPELAION_TEST_FERRETDB") else {
        eprintln!("SPELAION_TEST_FERRETDB not set: test skipped");
        return;
    };
    let target = RequestTarget {
        program: PROGRAM.into(),
        engine: "mongodb".into(),
        params: params(&addr),
        one_row: json!({"database": "admin", "command": {"ping": 1}}),
        refused: json!({"database": "spelaion", "command": {"noSuchCommand": 1}}),
        // FerretDB runs no JavaScript: nothing here waits long enough to be cancelled.
        slow: None,
    };
    assert_conforms_requests(&target).await;
}

/// `plugin.toml` is what SpelaionDB's pin says of this plugin: it must be what the program
/// says of itself. The program says it without a server.
#[tokio::test]
async fn the_manifest_is_what_the_program_says() {
    let manifest = include_str!("../plugin.toml");
    let value = |key: &str| -> String {
        manifest
            .lines()
            .find_map(|line| line.strip_prefix(&format!("{key} = ")))
            .unwrap_or_else(|| panic!("plugin.toml has no `{key}`"))
            .trim()
            .to_string()
    };
    let plugin = spelaion_plugin_host::Plugin::new(PROGRAM);
    let info = plugin.describe().await.expect("the program starts");

    assert_eq!(value("protocol"), format!("\"{}\"", info.protocol));
    let engines: Vec<String> = info.engines.iter().map(|e| format!("\"{e}\"")).collect();
    assert_eq!(value("engines"), format!("[{}]", engines.join(", ")));
    assert_eq!(info.plugin.name, env!("CARGO_PKG_NAME"));
    assert_eq!(info.plugin.version, env!("CARGO_PKG_VERSION"));
    // Three numbers, as SpelaionDB's `plugins.toml` wants.
    let min_app = value("min_app");
    let numbers = min_app.trim_matches('"').split('.').count();
    assert_eq!(numbers, 3, "min_app = {min_app}");
}
