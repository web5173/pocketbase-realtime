//! Example: subscribe to the `posts` collection in PocketBase and print realtime messages.
//!
//! Prerequisites: download and run [PocketBase](https://pocketbase.io) (default port 8090),
//! and create a `posts` collection in the admin UI. Run: `cargo run --example demo`

use pocketbase_realtime::RealtimeClient;

#[tokio::main]
async fn main() {
    let client = RealtimeClient::new("http://localhost:8090");
    let mut messages = client.messages();

    // topic is a collection name (or a custom event name)
    client.subscribe("posts");
    println!("Subscribed to posts collection, waiting for messages... (Ctrl+C to exit)");

    while let Some(msg) = messages.recv().await {
        println!("[{}] {}", msg.topic, msg.data);
    }
}
