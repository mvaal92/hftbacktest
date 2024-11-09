use std::{
    collections::{HashMap, HashSet},
    num::{ParseFloatError, ParseIntError},
    sync::{Arc, Mutex},
    time::Duration,
};

use hftbacktest::types::{ErrorKind, LiveError, LiveEvent, Order, Value};
use serde::Deserialize;
use thiserror::Error;
use tokio::sync::{broadcast, broadcast::Sender, mpsc::UnboundedSender};
use tracing::{error, info};

use crate::{
    bybit::{
        ordermanager::{OrderManager, SharedOrderManager},
        public_stream::PublicStream,
        rest::BybitClient,
        trade_stream::OrderOp,
    },
    connector::{Connector, ConnectorBuilder, GetOrders, PublishEvent},
    utils::{ExponentialBackoff, Retry},
};

#[allow(dead_code)]
mod msg;
mod ordermanager;
mod private_stream;
mod public_stream;
mod rest;
mod trade_stream;

#[derive(Error, Debug)]
pub enum BybitError {
    #[error("AssetNotFound")]
    AssetNotFound,
    #[error("AuthError: {code} - {msg}")]
    AuthError { code: i64, msg: String },
    #[error("OrderError: {code} - {msg}")]
    OrderError { code: i64, msg: String },
    #[error("InvalidPxQty: {0}")]
    InvalidPxQty(#[from] ParseFloatError),
    #[error("InvalidOrderId: {0}")]
    InvalidOrderId(ParseIntError),
    #[error("PrefixUnmatched")]
    PrefixUnmatched,
    #[error("OrderNotFound")]
    OrderNotFound,
    #[error("InvalidReqId")]
    InvalidReqId,
    #[error("InvalidArg: {0}")]
    InvalidArg(&'static str),
    #[error("OrderAlreadyExist")]
    OrderAlreadyExist,
    #[error("Serde: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("Reqwest: {0}")]
    Reqwest(#[from] reqwest::Error),
    #[error("Tungstenite: {0}")]
    Tungstenite(#[from] tokio_tungstenite::tungstenite::Error),
    #[error("ConnectionAbort: {0}")]
    ConnectionAbort(String),
    #[error("ConnectionInterrupted")]
    ConnectionInterrupted,
    #[error("OpError: {0}")]
    OpError(String),
    #[error("Config: {0:?}")]
    Config(#[from] toml::de::Error),
}

impl BybitError {
    pub fn to_value(&self) -> Value {
        match self {
            BybitError::AssetNotFound => Value::Empty,
            BybitError::AuthError { code, msg } => Value::Map({
                let mut map = HashMap::new();
                map.insert("code".to_string(), Value::Int(*code));
                map.insert("msg".to_string(), Value::String(msg.clone()));
                map
            }),
            BybitError::OrderError { code, msg } => Value::Map({
                let mut map = HashMap::new();
                map.insert("code".to_string(), Value::Int(*code));
                map.insert("msg".to_string(), Value::String(msg.clone()));
                map
            }),
            BybitError::InvalidPxQty(_) => Value::String(self.to_string()),
            BybitError::InvalidOrderId(_) => Value::String(self.to_string()),
            BybitError::PrefixUnmatched => Value::String(self.to_string()),
            BybitError::OrderNotFound => Value::String(self.to_string()),
            BybitError::InvalidReqId => Value::String(self.to_string()),
            BybitError::InvalidArg(_) => Value::String(self.to_string()),
            BybitError::OrderAlreadyExist => Value::String(self.to_string()),
            BybitError::Serde(_) => Value::String(self.to_string()),
            BybitError::Tungstenite(_) => Value::String(self.to_string()),
            BybitError::ConnectionAbort(_) => Value::String(self.to_string()),
            BybitError::ConnectionInterrupted => Value::String(self.to_string()),
            BybitError::OpError(_) => Value::String(self.to_string()),
            BybitError::Reqwest(_) => Value::String(self.to_string()),
            BybitError::Config(_) => Value::String(self.to_string()),
        }
    }
}

#[derive(Deserialize, Debug)]
pub struct Config {
    public_url: String,
    private_url: String,
    trade_url: String,
    rest_url: String,
    api_key: String,
    secret: String,
    category: String,
    order_prefix: String,
    symbols: Option<Vec<String>>,
}

type SharedSymbolSet = Arc<Mutex<HashSet<String>>>;

pub struct Bybit {
    config: Config,
    order_tx: Sender<OrderOp>,
    order_manager: SharedOrderManager,
    symbols: SharedSymbolSet,
    client: BybitClient,
    symbol_tx: Sender<String>,
}

impl Bybit {
    fn connect_public_stream(&self, ev_tx: UnboundedSender<PublishEvent>) {
        let public_url = self.config.public_url.clone();
        let symbol_tx = self.symbol_tx.clone();
        let symbols: Vec<String> = self.symbols.lock()
            .unwrap()
            .iter()
            .cloned()
            .collect();
        
        info!("Initializing public stream connection to {} with symbols {:?}", public_url, symbols);

        tokio::spawn(async move {
            let mut retry_count = 0;
            const MAX_RETRIES: u32 = 3;

            while retry_count < MAX_RETRIES {
                let result: Result<(), BybitError> = Retry::new(ExponentialBackoff::default())
                    .error_handler(|error: BybitError| {
                        error!(?error, "An error occurred in the public stream connection.");
                        if let Err(send_err) = ev_tx.send(PublishEvent::LiveEvent(LiveEvent::Error(LiveError::with(
                            ErrorKind::ConnectionInterrupted,
                            error.to_value(),
                        )))) {
                            error!(?send_err, "Failed to send error event");
                        }
                        Ok(())
                    })
                    .retry(|| async {
                        info!("Attempting to establish public stream connection (attempt {}/{})", retry_count + 1, MAX_RETRIES);
                        let mut stream = PublicStream::new(ev_tx.clone(), symbol_tx.subscribe());
                        
                        match stream.connect(&public_url).await {
                            Ok(_) => {
                                info!("Public stream connection established");
                                tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
                                
                                for symbol in &symbols {
                                    info!("Sending initial subscription for symbol: {}", symbol);
                                    if let Err(e) = symbol_tx.send(symbol.clone()) {
                                        error!("Failed to subscribe to symbol {}: {:?}", symbol, e);
                                        return Err(BybitError::ConnectionInterrupted);
                                    }
                                }
                                
                                loop {
                                    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                                }
                            }
                            Err(error) => {
                                error!(?error, "Failed to connect to public stream");
                                Err(error)
                            }
                        }
                    })
                    .await;

                if result.is_err() {
                    retry_count += 1;
                    if retry_count >= MAX_RETRIES {
                        error!("Maximum retries ({}) exceeded for public stream connection", MAX_RETRIES);
                        std::process::exit(1);
                    }
                    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                }
            }
        });
    }

    fn connect_private_stream(&self, ev_tx: UnboundedSender<PublishEvent>) {
        // Connects to the private stream for the position and order data.
        let private_url = self.config.private_url.clone();
        let api_key = self.config.api_key.clone();
        let secret = self.config.secret.clone();
        let category = self.config.category.clone();
        let order_manager = self.order_manager.clone();
        let instruments = self.symbols.clone();
        let client = self.client.clone();
        let symbol_tx = self.symbol_tx.clone();

        tokio::spawn(async move {
            let _ = Retry::new(ExponentialBackoff::default())
                .error_handler(|error: BybitError| {
                    error!(
                        ?error,
                        "An error occurred in the private stream connection."
                    );
                    ev_tx
                        .send(PublishEvent::LiveEvent(LiveEvent::Error(LiveError::with(
                            ErrorKind::ConnectionInterrupted,
                            error.to_value(),
                        ))))
                        .unwrap();
                    Ok(())
                })
                .retry(|| async {
                    let mut stream = private_stream::PrivateStream::new(
                        api_key.clone(),
                        secret.clone(),
                        ev_tx.clone(),
                        order_manager.clone(),
                        instruments.clone(),
                        category.clone(),
                        client.clone(),
                        symbol_tx.subscribe(),
                    );

                    // // todo: fix the operation order.
                    //
                    // // Cancel all orders before connecting to the stream in order to start with the
                    // // clean state.
                    // stream.cancel_all(&category).await?;
                    //
                    // // Fetches the initial states such as positions and open orders.
                    // stream.get_all_position(&category).await?;

                    stream.connect(&private_url).await?;

                    Ok(())
                })
                .await;
        });
    }

    fn connect_trade_stream(&self, ev_tx: UnboundedSender<PublishEvent>) {
        let trade_url = self.config.trade_url.clone();
        let api_key = self.config.api_key.clone();
        let secret = self.config.secret.clone();
        let order_manager = self.order_manager.clone();
        let order_tx = self.order_tx.clone();

        tokio::spawn(async move {
            let _ = Retry::new(ExponentialBackoff::default())
                .error_handler(|error: BybitError| {
                    error!(?error, "An error occurred in the trade stream connection.");
                    ev_tx
                        .send(PublishEvent::LiveEvent(LiveEvent::Error(LiveError::with(
                            ErrorKind::ConnectionInterrupted,
                            error.to_value(),
                        ))))
                        .unwrap();
                    Ok(())
                })
                .retry(|| async {
                    let mut stream = trade_stream::TradeStream::new(
                        api_key.clone(),
                        secret.clone(),
                        ev_tx.clone(),
                        order_manager.clone(),
                        order_tx.subscribe(),
                    );
                    stream.connect(&trade_url).await?;
                    Ok(())
                })
                .await;
        });
    }
}

impl ConnectorBuilder for Bybit {
    type Error = BybitError;

    fn build_from(config: &str) -> Result<Self, Self::Error> {
        let config: Config = toml::from_str(config)?;
        info!("Building Bybit connector with config: {:?}", config);
        
        let channel_capacity = 100;
        info!("Creating symbol channel with capacity: {}", channel_capacity);
        let (symbol_tx, _) = broadcast::channel(channel_capacity);
        
        // Store initial symbols but don't send yet
        let symbols = Arc::new(Mutex::new(HashSet::new()));
        if let Some(ref initial_symbols) = config.symbols {
            info!("Storing initial symbols: {:?}", initial_symbols);
            symbols.lock().unwrap().extend(initial_symbols.iter().cloned());
        }

        let order_tx = broadcast::channel(500).0;
        let order_manager = Arc::new(Mutex::new(OrderManager::new(&config.order_prefix)));
        let client = BybitClient::new(&config.rest_url, &config.api_key, &config.secret);
        
        Ok(Bybit {
            config,
            order_tx,
            order_manager,
            client,
            symbols,
            symbol_tx,
        })
    }
}

impl Connector for Bybit {
    fn run(&mut self, ev_tx: UnboundedSender<PublishEvent>) {
        info!("Starting Bybit connector");
        
        // Clone what we need before spawning
        let symbols = self.symbols.lock().unwrap().clone();
        let symbol_tx = self.symbol_tx.clone();
        
        // First set up the streams
        self.connect_public_stream(ev_tx.clone());
        self.connect_private_stream(ev_tx.clone());
        self.connect_trade_stream(ev_tx.clone());
        
        // Spawn task with cloned values
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(1)).await;
            
            info!("Sending initial symbols: {:?}", symbols);
            for symbol in symbols {
                if let Err(e) = symbol_tx.send(symbol.clone()) {
                    error!("Failed to send initial symbol {}: {:?}", symbol, e);
                } else {
                    info!("Successfully sent initial symbol: {}", symbol);
                }
            }
        });
    }

    fn register(&mut self, symbol: String) {
        info!("Registering new symbol: {}", symbol);
        let mut symbols = self.symbols.lock().unwrap();
        if !symbols.contains(&symbol) {
            info!("Adding new symbol to set: {}", symbol);
            symbols.insert(symbol.clone());
            info!("Sending symbol to subscription channel: {}", symbol);
            if let Err(e) = self.symbol_tx.send(symbol.clone()) {
                error!("Failed to send symbol to subscription channel: {:?}", e);
            }
        } else {
            info!("Symbol already registered: {}", symbol);
        }
    }

    fn order_manager(&self) -> Arc<Mutex<dyn GetOrders + Send + 'static>> {
        self.order_manager.clone()
    }

    fn submit(&self, asset: String, order: Order, ev_tx: UnboundedSender<PublishEvent>) {
        match self
            .order_manager
            .lock()
            .unwrap()
            .new_order(&asset, &self.config.category, order)
        {
            Ok(bybit_order) => {
                self.order_tx
                    .send(OrderOp {
                        op: "order.create",
                        bybit_order,
                    })
                    .unwrap();
            }
            Err(error) => {
                ev_tx
                    .send(PublishEvent::LiveEvent(LiveEvent::Error(LiveError::with(
                        ErrorKind::OrderError,
                        error.to_value(),
                    ))))
                    .unwrap();
            }
        }
    }

    fn cancel(&self, asset: String, order: Order, ev_tx: UnboundedSender<PublishEvent>) {
        match self.order_manager.lock().unwrap().cancel_order(
            &asset,
            &self.config.category,
            order.order_id,
        ) {
            Ok(bybit_order) => {
                self.order_tx
                    .send(OrderOp {
                        op: "order.cancel",
                        bybit_order,
                    })
                    .unwrap();
            }
            Err(error) => {
                ev_tx
                    .send(PublishEvent::LiveEvent(LiveEvent::Error(LiveError::with(
                        ErrorKind::OrderError,
                        error.to_value(),
                    ))))
                    .unwrap();
            }
        }
    }
}

