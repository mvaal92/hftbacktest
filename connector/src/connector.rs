use std::{
    fmt::Debug,
    sync::{Arc, Mutex},
};

use hftbacktest::types::{LiveEvent, Order};
use tokio::sync::mpsc::UnboundedSender;
use tracing::{info, debug, error};

/// A message will be received by the publisher thread and then published to the bots.
pub enum PublishEvent {
    BatchStart(u64),
    BatchEnd(u64),
    LiveEvent(LiveEvent),
    RegisterInstrument {
        id: u64,
        symbol: String,
        tick_size: f64,
    },
}

impl Debug for PublishEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PublishEvent::BatchStart(id) => write!(f, "BatchStart({})", id),
            PublishEvent::BatchEnd(id) => write!(f, "BatchEnd({})", id),
            PublishEvent::LiveEvent(event) => write!(f, "LiveEvent({:?})", event),
            PublishEvent::RegisterInstrument { id, symbol, tick_size } => 
                write!(f, "RegisterInstrument(id: {}, symbol: {}, tick_size: {})", id, symbol, tick_size),
        }
    }
}

/// Provides a build function for the Connector.
pub trait ConnectorBuilder {
    type Error: Debug;

    fn build_from(config: &str) -> Result<Self, Self::Error>
    where
        Self: Sized;
}

/// Provides an interface for connecting with an exchange or broker for a live bot.
pub trait Connector {
    /// Registers an instrument to be traded through this connector.
    fn register(&mut self, symbol: String) {
        info!("Registering new symbol: {}", symbol);
    }

    /// Returns an [`OrderManager`].
    fn order_manager(&self) -> Arc<Mutex<dyn GetOrders + Send + 'static>>;

    /// Runs the connector, establishing the connection and preparing to exchange information such
    /// as data feed and orders.
    fn run(&mut self, tx: UnboundedSender<PublishEvent>) {
        info!("Starting connector run");
        // Log when events are sent
        let tx_clone = tx.clone();
        tokio::spawn(async move {
            loop {
                if let Ok(()) = tx_clone.send(PublishEvent::BatchStart(0)) {
                    debug!("Successfully sent BatchStart event");
                } else {
                    error!("Failed to send BatchStart event");
                }
                tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            }
        });
    }

    /// Submits a new order.
    fn submit(&self, symbol: String, order: Order, tx: UnboundedSender<PublishEvent>) {
        info!("Submitting order for symbol: {}, order: {:?}", symbol, order);
    }

    /// Cancels an open order.
    fn cancel(&self, symbol: String, order: Order, tx: UnboundedSender<PublishEvent>) {
        info!("Cancelling order for symbol: {}, order: {:?}", symbol, order);
    }
}

/// Provides `orders` method to get the current working orders.
pub trait GetOrders {
    fn orders(&self, symbol: Option<String>) -> Vec<Order> {
        info!("Getting orders for symbol: {:?}", symbol);
        Vec::new()
    }
}
