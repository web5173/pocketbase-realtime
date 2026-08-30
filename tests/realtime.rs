//! Integration tests: a local mock SSE server verifies the core flow
//! "connect → handshake → submit subscriptions → receive messages".

use std::time::Duration;

use pocketbase_realtime::RealtimeClient;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};

/// Start a mock server and return its base address.
///
/// - `GET /api/realtime` → SSE response: sends `PB_CONNECT` first, then a `demo/*` message
/// - `POST /api/realtime` → records subscriptions and replies 204
async fn start_mock_server() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else { break };
            tokio::spawn(handle_conn(stream));
        }
    });

    format!("http://{}", addr)
}

async fn handle_conn(mut stream: TcpStream) {
    // Read the request line and headers (until the blank line), and detect the method
    let mut reader = BufReader::new(&mut stream);
    let mut line = String::new();
    let mut method = String::new();
    let mut first = true;

    loop {
        line.clear();
        if reader.read_line(&mut line).await.unwrap_or(0) == 0 {
            return;
        }
        if line == "\r\n" || line == "\n" {
            break;
        }
        if first {
            method = line.split_whitespace().next().unwrap_or("").to_string();
            first = false;
        }
    }
    drop(reader);

    if method == "GET" {
        // SSE response
        let headers = "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nCache-Control: no-cache\r\n\r\n";
        stream.write_all(headers.as_bytes()).await.ok();
        stream
            .write_all(b"id:test-client-id\nevent:PB_CONNECT\ndata:{}\n\n")
            .await
            .ok();
        // Wait for the client to submit subscriptions before sending a business message
        tokio::time::sleep(Duration::from_millis(100)).await;
        stream
            .write_all(b"event:demo/*\ndata:{\"hello\":\"world\"}\n\n")
            .await
            .ok();
        // Keep the connection open briefly
        tokio::time::sleep(Duration::from_secs(1)).await;
    } else {
        // POST: ignore the body, reply 204
        let _ = stream
            .write_all(b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\n\r\n")
            .await;
    }
}

#[tokio::test]
async fn subscribes_and_receives_messages() {
    let addr = start_mock_server().await;
    let client = RealtimeClient::new(addr);
    let mut messages = client.messages();

    client.subscribe("demo/*");

    let msg = tokio::time::timeout(Duration::from_secs(3), messages.recv())
        .await
        .expect("timed out waiting for a message")
        .expect("message channel closed");

    assert_eq!(msg.topic, "demo/*");
    assert_eq!(msg.data, r#"{"hello":"world"}"#);
    client.close();
}

#[tokio::test]
async fn reconnect_after_empty_subscriptions() {
    let addr = start_mock_server().await;
    let client = RealtimeClient::new(addr);

    client.subscribe("a");
    tokio::time::sleep(Duration::from_millis(300)).await;
    client.unsubscribe(Some("a"));
    tokio::time::sleep(Duration::from_millis(300)).await;

    // After clearing subscriptions the client should disconnect; subscribing again
    // triggers a reconnect and messages are still received
    client.subscribe("b");
    let mut messages = client.messages();
    let msg = tokio::time::timeout(Duration::from_secs(3), messages.recv())
        .await
        .expect("timed out waiting for a message");

    assert!(msg.is_some());
    client.close();
}
