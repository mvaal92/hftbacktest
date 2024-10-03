use std::sync::{Arc, Mutex};

use chrono::Utc;
use hashbrown::{hash_map::Entry, HashMap};
use hftbacktest::types::{Order, OrderId, Status};
use tracing::{debug, error};

use crate::{
    binancefutures::{
        msg::{rest::OrderResponse, stream::OrderTradeUpdate},
        BinanceFuturesError,
    },
    utils::{generate_rand_string, RefSymbolOrderId, SymbolOrderId},
};

#[derive(Debug)]
struct OrderExt {
    symbol: String,
    order: Order,
    removed_by_ws: bool,
    removed_by_rest: bool,
}

pub type SharedOrderManager = Arc<Mutex<OrderManager>>;

pub type ClientOrderId = String;

/// Binance has separated channels for REST APIs and Websocket. Order responses are delivered
/// through these channels, with no guaranteed order of transmission. To prevent duplicate handling
/// of order responses, such as order deletion due to cancellation or fill, OrderManager manages the
/// order states before transmitting the responses to a live bot.
///
/// Deletions must be confirmed by both channels. If not, differences in response times could result
/// in attempts to update an order that has already been deleted, potentially creating a ghost order
/// unintentionally.
///
/// To handle this, the `client_order_id` should include a random ID to differentiate it, even when
/// the order ID is the same(bot's order id). This is necessary because the order deletion is
/// immediately notified to the bot, but the Connector must still retain the `client_order_id` in
/// case an update arrives later from the other channel, which has not yet sent the deletion
/// message.
#[derive(Default, Debug)]
pub struct OrderManager {
    prefix: String,
    orders: HashMap<ClientOrderId, OrderExt>,
    order_id_map: HashMap<SymbolOrderId, ClientOrderId>,
}

impl OrderManager {
    pub fn new(prefix: &str) -> Self {
        Self {
            prefix: prefix.to_string(),
            orders: Default::default(),
            order_id_map: Default::default(),
        }
    }

    pub fn update_from_ws(
        &mut self,
        symbol: &str,
        client_order_id: &ClientOrderId,
        data: &OrderTradeUpdate,
    ) -> Result<Option<Order>, BinanceFuturesError> {
        if !client_order_id.starts_with(&self.prefix) {
            return Err(BinanceFuturesError::PrefixUnmatched);
        }
        let order_ext = self
            .orders
            .get_mut(client_order_id)
            .ok_or(BinanceFuturesError::OrderNotFound)?;

        let already_removed = order_ext.removed_by_ws || order_ext.removed_by_rest;
        if data.transaction_time * 1_000_000 >= order_ext.order.exch_timestamp {
            order_ext.order.qty = data.order.original_qty;
            order_ext.order.leaves_qty =
                data.order.original_qty - data.order.order_filled_accumulated_qty;
            order_ext.order.side = data.order.side;
            order_ext.order.time_in_force = data.order.time_in_force;
            order_ext.order.exch_timestamp = data.transaction_time * 1_000_000;
            order_ext.order.status = data.order.order_status;
            order_ext.order.exec_qty = data.order.order_last_filled_qty;
            order_ext.order.order_type = data.order.order_type;
        }

        let result = if already_removed {
            None
        } else {
            Some(order_ext.order.clone())
        };

        if order_ext.order.status != Status::New
            && order_ext.order.status != Status::PartiallyFilled
        {
            order_ext.removed_by_ws = true;
            if !already_removed {
                self.order_id_map
                    .remove(&RefSymbolOrderId::new(symbol, order_ext.order.order_id));
            }

            if order_ext.removed_by_ws && order_ext.removed_by_rest {
                self.orders.remove(client_order_id).unwrap();
            }
        }

        Ok(result)
    }

    pub fn update_submit_success(
        &mut self,
        symbol: String,
        order: Order,
        resp: OrderResponse,
    ) -> Option<Order> {
        let order = Order {
            qty: resp.orig_qty,
            leaves_qty: resp.orig_qty - resp.cum_qty,
            price_tick: (resp.price / order.tick_size).round() as i64,
            tick_size: order.tick_size,
            side: order.side,
            time_in_force: resp.time_in_force,
            exch_timestamp: resp.update_time * 1_000_000,
            status: Status::New,
            local_timestamp: 0,
            req: Status::None,
            exec_price_tick: 0,
            exec_qty: resp.executed_qty,
            order_id: order.order_id,
            order_type: resp.ty,
            // Invalid information
            q: Box::new(()),
            maker: false,
        };
        self.update_from_rest(symbol, resp.client_order_id, order)
    }

    pub fn update_submit_fail(
        &mut self,
        symbol: String,
        mut order: Order,
        error: &BinanceFuturesError,
        client_order_id: ClientOrderId,
    ) -> Option<Order> {
        match error {
            BinanceFuturesError::OrderError { code: -5022, .. } => {
                // GTX rejection.
            }
            BinanceFuturesError::OrderError { code: -1008, .. } => {
                // Server is currently overloaded with other requests. Please try again in a few minutes.
                error!("Server is currently overloaded with other requests. Please try again in a few minutes.");
            }
            BinanceFuturesError::OrderError { code: -2019, .. } => {
                // Margin is insufficient.
                error!("Margin is insufficient.");
            }
            BinanceFuturesError::OrderError { code: -1015, .. } => {
                // Too many new orders; current limit is 300 orders per TEN_SECONDS."
                error!("Too many new orders; current limit is 300 orders per TEN_SECONDS.");
            }
            error => {
                error!(?error, "submit error");
            }
        }

        order.req = Status::None;
        order.status = Status::Expired;
        self.update_from_rest(symbol, client_order_id, order)
    }

    pub fn update_cancel_success(
        &mut self,
        symbol: String,
        order: Order,
        resp: OrderResponse,
    ) -> Option<Order> {
        let order = Order {
            qty: resp.orig_qty,
            leaves_qty: resp.orig_qty - resp.cum_qty,
            price_tick: (resp.price / order.tick_size).round() as i64,
            tick_size: order.tick_size,
            side: resp.side,
            time_in_force: resp.time_in_force,
            exch_timestamp: resp.update_time * 1_000_000,
            status: Status::Canceled,
            local_timestamp: 0,
            req: Status::None,
            exec_price_tick: 0,
            exec_qty: resp.executed_qty,
            order_id: order.order_id,
            order_type: resp.ty,
            // Invalid information
            q: Box::new(()),
            maker: false,
        };
        self.update_from_rest(symbol, resp.client_order_id, order)
    }

    pub fn update_cancel_fail(
        &mut self,
        symbol: String,
        mut order: Order,
        error: &BinanceFuturesError,
        client_order_id: ClientOrderId,
    ) -> Option<Order> {
        match error {
            BinanceFuturesError::OrderError { code: -2011, .. } => {
                // The given order may no longer exist; it could have already been filled or
                // canceled. But, it cannot determine the order status because it lacks the
                // necessary information.
                order.leaves_qty = 0.0;
                order.status = Status::None;
            }
            error => {
                error!(?error, "cancel error");
            }
        }
        order.req = Status::None;
        self.update_from_rest(symbol, client_order_id, order)
    }

    fn update_from_rest(
        &mut self,
        symbol: String,
        client_order_id: ClientOrderId,
        order: Order,
    ) -> Option<Order> {
        match self.orders.entry(client_order_id.clone()) {
            Entry::Occupied(mut entry) => {
                let wrapper = entry.get_mut();
                let already_removed = wrapper.removed_by_ws || wrapper.removed_by_rest;
                if order.exch_timestamp >= wrapper.order.exch_timestamp {
                    wrapper.order.update(&order);
                }

                if order.status != Status::New && order.status != Status::PartiallyFilled {
                    wrapper.removed_by_rest = true;
                    if !already_removed {
                        self.order_id_map
                            .remove(&RefSymbolOrderId::new(&symbol, order.order_id));
                    }

                    if wrapper.removed_by_ws && wrapper.removed_by_rest {
                        entry.remove_entry();
                    }
                }

                if already_removed {
                    None
                } else {
                    Some(order)
                }
            }
            Entry::Vacant(entry) => {
                if !order.active() {
                    return None;
                }

                debug!(
                    %client_order_id,
                    ?order,
                    "BinanceFutures OrderManager received an unmanaged order from REST."
                );
                let order_ex = entry.insert(OrderExt {
                    symbol,
                    order: order.clone(),
                    removed_by_ws: false,
                    removed_by_rest: order.status != Status::New
                        && order.status != Status::PartiallyFilled,
                });
                if order_ex.removed_by_ws || order_ex.removed_by_rest {
                    self.order_id_map
                        .remove(&RefSymbolOrderId::new(&order_ex.symbol, order.order_id));
                }
                Some(order)
            }
        }
    }

    pub fn prepare_client_order_id(&mut self, symbol: String, order: Order) -> Option<String> {
        let symbol_order_id = SymbolOrderId::new(symbol.clone(), order.order_id);
        if self.order_id_map.contains_key(&symbol_order_id) {
            println!("symbol order id duplicate");
            return None;
        }

        let client_order_id = format!("{}{}", self.prefix, generate_rand_string(16));
        if self.orders.contains_key(&client_order_id) {
            return None;
        }

        self.order_id_map
            .insert(symbol_order_id, client_order_id.clone());
        self.orders.insert(
            client_order_id.clone(),
            OrderExt {
                symbol,
                order,
                removed_by_ws: false,
                removed_by_rest: false,
            },
        );
        Some(client_order_id)
    }

    pub fn get_client_order_id(&self, symbol: &str, order_id: OrderId) -> Option<String> {
        self.order_id_map
            .get(&RefSymbolOrderId::new(symbol, order_id))
            .cloned()
    }

    /// Due to API instability or network issues, discrepancies can occur where an order is deleted
    /// by one channel but remains active because its deletion wasn't confirmed by both channels.
    /// The gc method resolves this by removing orders that were deleted by one channel but not
    /// confirmed by the other, after a defined threshold period.
    pub fn gc(&mut self) {
        let now = Utc::now().timestamp_nanos_opt().unwrap();
        let stale_ts = now - 300_000_000_000;
        let stale_ids: Vec<(_, _)> = self
            .orders
            .iter()
            .filter(|&(_, wrapper)| {
                wrapper.order.status != Status::New
                    && wrapper.order.status != Status::PartiallyFilled
                    && wrapper.order.status != Status::Unsupported
                    && wrapper.order.exch_timestamp < stale_ts
            })
            .map(|(client_order_id, wrapper)| {
                (
                    client_order_id.clone(),
                    SymbolOrderId::new(wrapper.symbol.clone(), wrapper.order.order_id),
                )
            })
            .collect();
        for (client_order_id, order_id) in stale_ids.iter() {
            if self.order_id_map.contains_key(order_id) {
                // todo: something went wrong?
                self.order_id_map.remove(order_id).unwrap();
            }
            self.orders.remove(client_order_id);
        }
    }

    pub fn cancel_all_from_rest(&mut self, symbol: &str) -> Vec<Order> {
        let mut removed_orders = Vec::new();
        let mut removed_order_ids = Vec::new();
        for (client_order_id, order_ext) in &mut self.orders {
            if order_ext.symbol != symbol {
                continue;
            }
            let already_removed = order_ext.removed_by_ws || order_ext.removed_by_rest;

            order_ext.removed_by_rest = true;
            order_ext.order.status = Status::Canceled;
            // todo: check if the exchange timestamp exists in the REST response.
            order_ext.order.exch_timestamp = Utc::now().timestamp_nanos_opt().unwrap();
            if !already_removed {
                self.order_id_map
                    .remove(&RefSymbolOrderId::new(symbol, order_ext.order.order_id));
                removed_orders.push(order_ext.order.clone());
            }

            // Completely deletes the order if it is removed by both the REST response and the
            // WebSocket stream.
            if order_ext.removed_by_ws && order_ext.removed_by_rest {
                removed_order_ids.push(client_order_id.clone());
            }
        }

        for order_id in removed_order_ids {
            self.orders.remove(&order_id).unwrap();
        }
        removed_orders
    }

    pub fn get_orders(&self, symbol: &str) -> Vec<Order> {
        self.orders
            .iter()
            .filter(|(_, order)| order.symbol == symbol)
            .map(|(_, order)| &order.order)
            .cloned()
            .collect()
    }
}
