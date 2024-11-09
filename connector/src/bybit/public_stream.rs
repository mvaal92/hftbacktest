use std::time::Duration;

use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use hftbacktest::prelude::{
    Event,
    LiveEvent,
    Side,
    LOCAL_ASK_DEPTH_BBO_EVENT,
    LOCAL_ASK_DEPTH_EVENT,
    LOCAL_BID_DEPTH_BBO_EVENT,
    LOCAL_BID_DEPTH_EVENT,
    LOCAL_BUY_TRADE_EVENT,
    LOCAL_SELL_TRADE_EVENT,
};
use tokio::{
    select,
    sync::{
        broadcast::{error::RecvError, Receiver},
        mpsc::UnboundedSender,
    },
    time,
};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{client::IntoClientRequest, Message},
};
use tracing::{error, info};

use crate::{
    bybit::{
        msg,
        msg::{Op, OrderBook, PublicStreamMsg},
        BybitError,
    },
    connector::PublishEvent,
    utils::parse_depth,
};

pub struct PublicStream {
    ev_tx: UnboundedSender<PublishEvent>,
    symbol_rx: Receiver<String>,
}

impl PublicStream {
    pub fn new(ev_tx: UnboundedSender<PublishEvent>, symbol_rx: Receiver<String>) -> Self {
        Self { ev_tx, symbol_rx }
    }

    async fn handle_public_stream(&self, text: &str) -> Result<(), BybitError> {
        let stream = serde_json::from_str::<PublicStreamMsg>(text)?;
        match stream {
            PublicStreamMsg::Op(resp) => {
                if let Some(true) = resp.success {
                    if !resp.success_topics.is_empty() {
                        info!("Successfully subscribed to topics: {:?}", resp.success_topics);
                    }
                }
                if !resp.fail_topics.is_empty() {
                    error!("Failed to subscribe to topics: {:?}", resp.fail_topics);
                    return Err(BybitError::ConnectionInterrupted);
                }
            }
            PublicStreamMsg::Topic(topic_msg) => {
                if topic_msg.topic.starts_with("orderbook.1") {
                    let data: OrderBook = serde_json::from_value(topic_msg.data)?;
                    let (bids, asks) = parse_depth(data.bids, data.asks)?;

                    for (px, qty) in bids {
                        self.ev_tx
                            .send(PublishEvent::LiveEvent(LiveEvent::Feed {
                                symbol: data.symbol.clone(),
                                event: Event {
                                    ev: LOCAL_BID_DEPTH_BBO_EVENT,
                                    exch_ts: topic_msg.cts.unwrap() * 1_000_000,
                                    local_ts: Utc::now().timestamp_nanos_opt().unwrap(),
                                    order_id: 0,
                                    px,
                                    qty,
                                    ival: 0,
                                    fval: 0.0,
                                },
                            }))
                            .unwrap();
                    }

                    for (px, qty) in asks {
                        self.ev_tx
                            .send(PublishEvent::LiveEvent(LiveEvent::Feed {
                                symbol: data.symbol.clone(),
                                event: Event {
                                    ev: LOCAL_ASK_DEPTH_BBO_EVENT,
                                    exch_ts: topic_msg.cts.unwrap() * 1_000_000,
                                    local_ts: Utc::now().timestamp_nanos_opt().unwrap(),
                                    order_id: 0,
                                    px,
                                    qty,
                                    ival: 0,
                                    fval: 0.0,
                                },
                            }))
                            .unwrap();
                    }
                } else if topic_msg.topic.starts_with("orderbook") {
                    let data: OrderBook = serde_json::from_value(topic_msg.data)?;
                    let (bids, asks) = parse_depth(data.bids, data.asks)?;

                    for (px, qty) in bids {
                        self.ev_tx
                            .send(PublishEvent::LiveEvent(LiveEvent::Feed {
                                symbol: data.symbol.clone(),
                                event: Event {
                                    ev: LOCAL_BID_DEPTH_EVENT,
                                    exch_ts: topic_msg.cts.unwrap() * 1_000_000,
                                    local_ts: Utc::now().timestamp_nanos_opt().unwrap(),
                                    order_id: 0,
                                    px,
                                    qty,
                                    ival: 0,
                                    fval: 0.0,
                                },
                            }))
                            .unwrap();
                    }

                    for (px, qty) in asks {
                        self.ev_tx
                            .send(PublishEvent::LiveEvent(LiveEvent::Feed {
                                symbol: data.symbol.clone(),
                                event: Event {
                                    ev: LOCAL_ASK_DEPTH_EVENT,
                                    exch_ts: topic_msg.cts.unwrap() * 1_000_000,
                                    local_ts: Utc::now().timestamp_nanos_opt().unwrap(),
                                    order_id: 0,
                                    px,
                                    qty,
                                    ival: 0,
                                    fval: 0.0,
                                },
                            }))
                            .unwrap();
                    }
                } else if topic_msg.topic.starts_with("publicTrade") {
                    let data: Vec<msg::Trade> = serde_json::from_value(topic_msg.data)?;
                    for item in data {
                        self.ev_tx
                            .send(PublishEvent::LiveEvent(LiveEvent::Feed {
                                symbol: item.symbol.clone(),
                                event: Event {
                                    ev: {
                                        if item.side == Side::Sell {
                                            LOCAL_SELL_TRADE_EVENT
                                        } else {
                                            LOCAL_BUY_TRADE_EVENT
                                        }
                                    },
                                    exch_ts: item.ts * 1_000_000,
                                    local_ts: Utc::now().timestamp_nanos_opt().unwrap(),
                                    order_id: 0,
                                    px: item.trade_price,
                                    qty: item.trade_size,
                                    ival: 0,
                                    fval: 0.0,
                                },
                            }))
                            .unwrap();
                    }
                }
            }
        }
        Ok(())
    }

    pub async fn connect(&mut self, url: &str) -> Result<(), BybitError> {
        info!("Connecting to public stream at {}", url);
        let mut request = url.into_client_request()?;
        let _ = request.headers_mut();

        let (ws_stream, _) = connect_async(request).await?;
        let (mut write, mut read) = ws_stream.split();
        let mut interval = time::interval(Duration::from_secs(15));

        loop {
            select! {
                _ = interval.tick() => {
                    info!("Sending ping message");
                    let op = Op {
                        req_id: "ping".to_string(),
                        op: "ping".to_string(),
                        args: vec![]
                    };
                    let s = serde_json::to_string(&op).unwrap();
                    if let Err(e) = write.send(Message::Text(s)).await {
                        error!("Failed to send ping: {:?}", e);
                        return Err(BybitError::ConnectionInterrupted);
                    }
                }
                msg = self.symbol_rx.recv() => {
                    info!("Received symbol subscription request");
                    match msg {
                        Ok(symbol) => {
                            info!("Processing subscription for symbol: {}", symbol);
                            let args = vec![
                                format!("orderbook.1.{symbol}"),
                                format!("orderbook.50.{symbol}"),
                                format!("orderbook.500.{symbol}"),
                                format!("publicTrade.{symbol}")
                            ];
                            info!("Generated subscription topics: {:?}", args);
                            let op = Op {
                                req_id: "subscribe".to_string(),
                                op: "subscribe".to_string(),
                                args,
                            };
                            let s = serde_json::to_string(&op).unwrap();
                            if let Err(e) = write.send(Message::Text(s)).await {
                                error!("Failed to send subscription: {:?}", e);
                                return Err(BybitError::ConnectionInterrupted);
                            }
                        }
                        Err(RecvError::Closed) => {
                            info!("Symbol receiver closed, exiting");
                            return Ok(());
                        }
                        Err(RecvError::Lagged(num)) => {
                            error!("Symbol receiver lagged by {} messages", num);
                        }
                    }
                },
                message = read.next() => {
                    match message {
                        Some(Ok(Message::Text(text))) => {
                            if let Err(error) = self.handle_public_stream(&text).await {
                                error!(?error, text, "Failed to handle public stream message");
                            }
                        }
                        Some(Ok(Message::Binary(data))) => {
                        }
                        Some(Ok(Message::Ping(_))) => {
                            info!("Received WebSocket ping");
                            if let Err(e) = write.send(Message::Pong(vec![])).await {
                                error!("Failed to send pong: {:?}", e);
                                return Err(BybitError::ConnectionInterrupted);
                            }
                        }
                        Some(Ok(Message::Pong(_))) => {
                            info!("Received WebSocket pong");
                        }
                        Some(Ok(Message::Close(_))) => {
                            error!("Received close frame");
                            return Err(BybitError::ConnectionInterrupted);
                        }
                        Some(Ok(Message::Frame(_))) => {
                            info!("Received raw WebSocket frame");
                        }
                        Some(Err(e)) => {
                            error!("WebSocket error: {:?}", e);
                            return Err(BybitError::ConnectionInterrupted);
                        }
                        None => {
                            error!("WebSocket stream ended");
                            return Err(BybitError::ConnectionInterrupted);
                        }
                    }
                }
            }
        }
    }
}
