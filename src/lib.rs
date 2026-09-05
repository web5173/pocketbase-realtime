use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::StreamExt;
use reqwest::Client;
use reqwest_eventsource::{Event, EventSource};
use serde_json::json;
use tokio::sync::{mpsc, watch, Notify};
use tokio_util::sync::CancellationToken;

/// Max time to establish the connection and wait for `PB_CONNECT`.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(15);

/// Delay between reconnection attempts after a disconnect.
const RECONNECT_DELAY: Duration = Duration::from_secs(2);

/// A single realtime message: `topic` is the event name, `data` is the raw JSON string.
#[derive(Debug, Clone)]
pub struct RealtimeMessage {
    pub topic: String,
    pub data: String,
}

/// Lightweight anonymous SSE realtime subscription client.
///
/// Must be created inside a Tokio runtime (it spawns a background task for
/// connection, reconnection, and message dispatch). Subscriptions are
/// fire-and-forget: changes are re-submitted automatically in the background.
pub struct RealtimeClient {
    inner: Arc<Inner>,
}

struct Inner {
    realtime_url: String,
    client: Client,
    subs: Mutex<HashSet<String>>,
    /// Subscription version: bumped on every change; the background task uses it to decide whether to re-submit.
    version: AtomicU64,
    notify: Notify,
    tx: mpsc::UnboundedSender<RealtimeMessage>,
    rx: Mutex<Option<mpsc::UnboundedReceiver<RealtimeMessage>>>,
    cancel: CancellationToken,
    connected: watch::Sender<bool>,
}

impl RealtimeClient {
    /// Create a client and start the background connection task.
    ///
    /// `base_url` is the server base address, e.g. `http://localhost:8090`.
    pub fn new(base_url: impl Into<String>) -> Self {
        let base_url = base_url.into();
        let realtime_url = format!("{}/api/realtime", base_url.trim_end_matches('/'));
        let (tx, rx) = mpsc::unbounded_channel();
        let (connected, _) = watch::channel(false);

        let inner = Arc::new(Inner {
            realtime_url,
            client: Client::new(),
            subs: Mutex::new(HashSet::new()),
            version: AtomicU64::new(0),
            notify: Notify::new(),
            tx,
            rx: Mutex::new(Some(rx)),
            cancel: CancellationToken::new(),
            connected,
        });

        tokio::spawn(run(inner.clone()));

        Self { inner }
    }

    /// Take the message receiver (can only be called once).
    pub fn messages(&self) -> mpsc::UnboundedReceiver<RealtimeMessage> {
        self.inner
            .rx
            .lock()
            .unwrap()
            .take()
            .expect("messages() can only be called once")
    }

    /// Subscribe to messages for the given topic. Repeatedly subscribing to the same topic is idempotent.
    pub fn subscribe(&self, topic: impl Into<String>) {
        let topic = topic.into();
        if topic.is_empty() {
            return;
        }
        let mut subs = self.inner.subs.lock().unwrap();
        let is_new = subs.insert(topic);
        drop(subs);
        if is_new {
            self.inner.version.fetch_add(1, Ordering::Relaxed);
            // `notify_one` stores a permit, so no wakeup is lost even if the background task is not waiting yet
            self.inner.notify.notify_one();
        }
    }

    /// Unsubscribe from a topic. Passing `None` removes all subscriptions.
    pub fn unsubscribe(&self, topic: Option<&str>) {
        let mut subs = self.inner.subs.lock().unwrap();
        let changed = match topic {
            Some(topic) => subs.remove(topic),
            None => {
                let changed = !subs.is_empty();
                subs.clear();
                changed
            }
        };
        drop(subs);
        if changed {
            self.inner.version.fetch_add(1, Ordering::Relaxed);
            self.inner.notify.notify_one();
        }
    }

    /// Whether the connection is currently established.
    pub fn is_connected(&self) -> bool {
        *self.inner.connected.borrow()
    }

    /// Subscribe to connection-state changes (`true` = established, `false` = disconnected).
    /// The receiver yields the current value immediately and then on every change.
    pub fn connection_state(&self) -> watch::Receiver<bool> {
        self.inner.connected.subscribe()
    }

    /// Stop the background task.
    pub fn close(&self) {
        self.inner.cancel.cancel();
    }
}

impl Drop for RealtimeClient {
    fn drop(&mut self) {
        self.inner.cancel.cancel();
    }
}

/// Background main loop: waits while there are no subscriptions, otherwise
/// connects and keeps the connection alive, reconnecting after disconnects.
async fn run(inner: Arc<Inner>) {
    loop {
        // Do not hold a connection when there are no subscriptions
        if inner.subs.lock().unwrap().is_empty() {
            tokio::select! {
                _ = inner.cancel.cancelled() => break,
                _ = inner.notify.notified() => {}
            }
        }

        run_once(&inner).await;

        tokio::select! {
            _ = inner.cancel.cancelled() => break,
            _ = tokio::time::sleep(RECONNECT_DELAY) => {}
        }
    }
}

/// One connection cycle: open SSE → wait for `PB_CONNECT` → re-submit subscriptions →
/// keep reading until disconnect or cancel.
async fn run_once(inner: &Arc<Inner>) {
    // `EventSource::get` constructs synchronously; the connection is established on the
    // first poll, and connection errors surface via `next()`
    let mut es = EventSource::get(&inner.realtime_url);

    let client_id = match tokio::time::timeout(CONNECT_TIMEOUT, wait_pb_connect(&mut es)).await {
        Ok(Some(cid)) => cid,
        _ => return, // handshake failed or timed out → reconnect
    };

    inner.connected.send_replace(true);
    let mut last_version = inner.version.load(Ordering::Relaxed);
    send_subscriptions(inner, &client_id).await;

    loop {
        // If subscriptions changed → re-submit (or disconnect if empty)
        let cur_version = inner.version.load(Ordering::Relaxed);
        if cur_version != last_version {
            if inner.subs.lock().unwrap().is_empty() {
                break;
            }
            send_subscriptions(inner, &client_id).await;
            last_version = cur_version;
        }

        tokio::select! {
            ev = es.next() => {
                match ev {
                    Some(Ok(Event::Message(msg))) => {
                        if msg.event != "PB_CONNECT" {
                            let _ = inner.tx.send(RealtimeMessage {
                                topic: msg.event,
                                data: msg.data,
                            });
                        }
                    }
                    Some(Ok(Event::Open)) => {}
                    Some(Err(_)) | None => break, // disconnected → reconnect
                }
            }
            // Wake-up signal only; actual (re)submission is driven by the version check
            // at the top of the loop, so no notification is lost to select contention.
            _ = inner.notify.notified() => {}
            _ = inner.cancel.cancelled() => break,
        }
    }

    inner.connected.send_replace(false);
}

/// Wait for the server's `PB_CONNECT` event and return the `clientId`.
async fn wait_pb_connect(es: &mut EventSource) -> Option<String> {
    while let Some(ev) = es.next().await {
        match ev {
            Ok(Event::Message(msg)) if msg.event == "PB_CONNECT" => {
                // Prefer the SSE `id:` field; fall back to parsing clientId from the data payload
                if !msg.id.is_empty() {
                    return Some(msg.id);
                }
                if let Ok(value) = serde_json::from_str::<serde_json::Value>(&msg.data) {
                    if let Some(cid) = value.get("clientId").and_then(|c| c.as_str()) {
                        return Some(cid.to_string());
                    }
                }
            }
            Ok(_) => {}
            Err(_) => return None,
        }
    }
    None
}

/// Submit the current set of subscriptions to the server.
async fn send_subscriptions(inner: &Inner, client_id: &str) {
    let subs: Vec<String> = inner.subs.lock().unwrap().iter().cloned().collect();
    if subs.is_empty() {
        return;
    }
    let body = json!({ "clientId": client_id, "subscriptions": subs });
    let _ = inner.client.post(&inner.realtime_url).json(&body).send().await;
}
