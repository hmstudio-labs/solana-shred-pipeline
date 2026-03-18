use jito_protos::shredstream::{
    shredstream_proxy_client::ShredstreamProxyClient, SubscribeEntriesRequest,
};

/// Prepares endpoint string for tonic client connection.
/// - TCP endpoints: adds "http://" prefix if needed
/// - Unix socket endpoints: expects "unix:/path/to.sock" format
fn prepare_endpoint(endpoint: String) -> String {
    // Unix sockets don't need http:// prefix
    if endpoint.starts_with("unix:") {
        endpoint
    } else if endpoint.starts_with("http://") {
        endpoint
    } else {
        format!("http://{}", endpoint)
    }
}

#[tokio::main]
async fn main() -> Result<(), std::io::Error> {
    // Configure endpoint: use same format as server --grpc-service-endpoint
    let endpoint = std::env::var("GRPC_ENDPOINT")
        .unwrap_or_else(|_| "127.0.0.1:9999".to_string());

    let endpoint = prepare_endpoint(endpoint);

    let mut client = ShredstreamProxyClient::connect(endpoint)
        .await
        .unwrap();
    let mut stream = client
        .subscribe_entries(SubscribeEntriesRequest {})
        .await
        .unwrap()
        .into_inner();

    while let Some(slot_entry) = stream.message().await.unwrap() {
        let entries =
            match bincode::deserialize::<Vec<solana_entry::entry::Entry>>(&slot_entry.entries) {
                Ok(e) => e,
                Err(e) => {
                    println!("Deserialization failed with err: {e}");
                    continue;
                }
            };
        println!(
            "slot {}, entries: {}, transactions: {}",
            slot_entry.slot,
            entries.len(),
            entries.iter().map(|e| e.transactions.len()).sum::<usize>()
        );
    }
    Ok(())
}
