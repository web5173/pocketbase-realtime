# pocketbase-realtime

A lightweight anonymous SSE realtime subscription client for [PocketBase](https://pocketbase.io).

## Installation

```toml
[dependencies]
pocketbase-realtime = "0.1"
```

## Usage

```rust
use pocketbase_realtime::RealtimeClient;

#[tokio::main]
async fn main() {
    let client = RealtimeClient::new("http://localhost:8090");
    let mut messages = client.messages();

    // topic is a collection name (or a custom event name), e.g. the `posts` collection
    client.subscribe("posts");

    while let Some(msg) = messages.recv().await {
        println!("[{}] {}", msg.topic, msg.data);
    }
}
```

## API

| Member | Description |
| --- | --- |
| `RealtimeClient::new(base_url)` | Create a client and start the background task (requires a Tokio runtime) |
| `messages()` | Take the message receiver (`UnboundedReceiver<RealtimeMessage>`, callable once) |
| `subscribe(topic)` | Subscribe to a topic (idempotent) |
| `unsubscribe(Option<&str>)` | Unsubscribe from a topic; `None` removes all |
| `is_connected()` | Whether the connection is established |
| `close()` | Stop the background task |

`RealtimeMessage { topic, data }`: `topic` is the event name, `data` is the raw JSON string.

## Testing

1. Create a `posts` collection in the PocketBase admin UI
2. Run the example: `cargo run --example demo` (subscribes to the `posts` collection)
3. Create/update a `posts` record in the admin UI; the terminal shows `[posts] {...}`
   (if nothing arrives, set the collection's list/view rules to `""` to allow anonymous viewing — this client connects anonymously)

## Development tests

No real server needed, works offline.

```sh
cargo test
```

## License

MIT
