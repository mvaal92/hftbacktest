use std::{collections::HashMap, fmt::Debug};

use hftbacktest::prelude::*;
use std::f64::consts::E;

/// Calculates coefficients c1 and c2 based on the GLFT model.
fn compute_coeff(xi: f64, gamma: f64, delta: f64, a: f64, k: f64) -> (f64, f64) {
    let c1 = (1.0 / (xi * delta)) * ((1.0 + (xi * delta) / k).ln());
    let exponent = (k / (xi * delta)) + 1.0;
    let c2 = ((gamma / (2.0 * a * delta * k)) * (1.0 + (xi * delta) / k).powf(exponent)).sqrt();
    (c1, c2)
}

/// Measures trading intensity (lambda) using linear regression.
fn measure_trading_intensity(arrival_depth: &[f64]) -> Option<(f64, f64)> {
    let mut counts = HashMap::new();
    let mut max_tick = 0.0;

    for &depth in arrival_depth {
        if !depth.is_finite() {
            continue;
        }
        let tick = (depth / 0.5).round() - 1.0;
        if tick < 0.0 {
            continue;
        }
        *counts.entry(tick).or_insert(0.0) += 1.0;
        if tick > max_tick {
            max_tick = tick;
        }
    }

    let ticks: Vec<f64> = (0..=max_tick as usize).map(|i| i as f64 + 0.5).collect();
    let lambda: Vec<f64> = ticks
        .iter()
        .map(|&tick| *counts.get(&tick).unwrap_or(&0.0) / 600.0)
        .collect();

    // Perform linear regression on log(lambda) vs. ticks
    let x = ticks;
    let y: Vec<f64> = lambda.iter().map(|&l| l.ln()).collect();
    let n = x.len() as f64;
    let sx: f64 = x.iter().sum();
    let sy: f64 = y.iter().sum();
    let sxy: f64 = x.iter().zip(y.iter()).map(|(&xi, &yi)| xi * yi).sum();
    let sx2: f64 = x.iter().map(|&xi| xi * xi).sum();

    let denominator = n * sx2 - sx * sx;
    if denominator == 0.0 {
        return None;
    }

    let slope = (n * sxy - sx * sy) / denominator;
    let intercept = (sy - slope * sx) / n;

    let a = E.powf(intercept);
    let k = -slope;

    Some((a, k))
}

/// Measures market volatility.
fn measure_volatility(mid_price_changes: &[f64]) -> f64 {
    let mean: f64 = mid_price_changes.iter().copied().sum::<f64>() / mid_price_changes.len() as f64;
    let variance: f64 = mid_price_changes
        .iter()
        .map(|&x| (x - mean).powi(2))
        .sum::<f64>()
        / (mid_price_changes.len() as f64 - 1.0);
    variance.sqrt() * 10f64.sqrt() // Adjust for 100ms intervals
}

pub fn gridtrading<MD, I, R>(
    hbt: &mut I,
    recorder: &mut R,
    gamma: f64,
    delta: f64,
    adj1: f64,
    adj2: f64,
    grid_num: usize,
    max_position: f64,
    order_qty: f64,
) -> Result<(), i64>
where
    MD: MarketDepth,
    I: Bot<MD>,
    <I as Bot<MD>>::Error: Debug,
    R: Recorder,
    <R as Recorder>::Error: Debug,
{
    let tick_size = hbt.depth(0).tick_size() as f64;
    let mut arrival_depth: Vec<f64> = Vec::new();
    let mut mid_price_changes: Vec<f64> = Vec::new();
    let mut prev_mid_price = None;

    let mut int = 0;
    while hbt.elapse(100_000_000).unwrap() {
        int += 1;
        if int % 10 == 0 {
            recorder.record(hbt).unwrap();
        }

        let depth = hbt.depth(0);
        let position = hbt.position(0);
        let orders = hbt.orders(0);

        let best_bid_tick = depth.best_bid_tick() as f64;
        let best_ask_tick = depth.best_ask_tick() as f64;

        if best_bid_tick == INVALID_MIN as f64 || best_ask_tick == INVALID_MAX as f64 {
            continue;
        }

        let mid_price_tick = (best_bid_tick + best_ask_tick) / 2.0;
        if let Some(prev) = prev_mid_price {
            mid_price_changes.push(mid_price_tick - prev);
        }
        prev_mid_price = Some(mid_price_tick);

        // Record arrival depth
        for trade in hbt.last_trades(0) {
            let trade_price_tick = trade.px / tick_size;
            let depth = if trade.ev & BUY_EVENT == BUY_EVENT {
                trade_price_tick - mid_price_tick
            } else {
                mid_price_tick - trade_price_tick
            };
            arrival_depth.push(depth);
        }
        hbt.clear_last_trades(0);

        // Calibrate A and k every 5 seconds
        let (a, k) = if int % 50 == 0 && arrival_depth.len() >= 6_000 {
            if let Some((a, k)) = measure_trading_intensity(&arrival_depth[arrival_depth.len() - 6_000..]) {
                (a, k)
            } else {
                (1.0, 1.0) // Default values
            }
        } else {
            (1.0, 1.0) // Default values
        };

        // Measure volatility every 5 seconds
        let volatility = if int % 50 == 0 && mid_price_changes.len() >= 6_000 {
            measure_volatility(&mid_price_changes[mid_price_changes.len() - 6_000..])
        } else {
            0.0 // Default value
        };

        // Compute coefficients
        let (c1, c2) = compute_coeff(gamma, gamma, delta, a, k);

        let half_spread_tick = (c1 + delta / 2.0 * c2 * volatility) * adj1;
        let skew = c2 * volatility * adj2;

        let reservation_price_tick = mid_price_tick - skew * position;

        let bid_price_tick = (reservation_price_tick - half_spread_tick).min(best_bid_tick).floor();
        let ask_price_tick = (reservation_price_tick + half_spread_tick).max(best_ask_tick).ceil();

        let bid_price = bid_price_tick * tick_size;
        let ask_price = ask_price_tick * tick_size;

        let grid_interval = (half_spread_tick.round() * tick_size).max(tick_size);

        let mut bid_price = (bid_price / grid_interval).floor() * grid_interval;
        let mut ask_price = (ask_price / grid_interval).ceil() * grid_interval;

        // Update quotes
        hbt.clear_inactive_orders(Some(0));

        // Create new bid orders grid
        let mut new_bid_orders = HashMap::new();
        if position < max_position && bid_price.is_finite() {
            for _ in 0..grid_num {
                let bid_price_tick = (bid_price / tick_size).round() as u64;

                new_bid_orders.insert(bid_price_tick, bid_price);

                bid_price -= grid_interval;
            }
        }

        // Create new ask orders grid
        let mut new_ask_orders = HashMap::new();
        if position > -max_position && ask_price.is_finite() {
            for _ in 0..grid_num {
                let ask_price_tick = (ask_price / tick_size).round() as u64;

                new_ask_orders.insert(ask_price_tick, ask_price);

                ask_price += grid_interval;
            }
        }

        // Cancel outdated orders
        let cancel_order_ids: Vec<u64> = orders
            .values()
            .filter(|order| {
                order.cancellable()
                    && ((order.side == Side::Buy && !new_bid_orders.contains_key(&order.order_id))
                        || (order.side == Side::Sell && !new_ask_orders.contains_key(&order.order_id)))
            })
            .map(|order| order.order_id)
            .collect();
        for order_id in cancel_order_ids {
            hbt.cancel(0, order_id, false).unwrap();
        }

        // Submit new bid orders
        for (order_id, order_price) in new_bid_orders {
            if !orders.contains_key(&order_id) {
                hbt.submit_buy_order(
                    0,
                    order_id,
                    order_price,
                    order_qty,
                    TimeInForce::GTX,
                    OrdType::Limit,
                    false,
                )
                .unwrap();
            }
        }

        // Submit new ask orders
        for (order_id, order_price) in new_ask_orders {
            if !orders.contains_key(&order_id) {
                hbt.submit_sell_order(
                    0,
                    order_id,
                    order_price,
                    order_qty,
                    TimeInForce::GTX,
                    OrdType::Limit,
                    false,
                )
                .unwrap();
            }
        }
    }
    Ok(())
}


gridtrading(
    &mut hbt,
    &mut recorder,
    0.05,     // gamma
    1.0,      // delta
    1.0,      // adj1
    0.05,     // adj2
    20,       // grid_num
    20.0,     // max_position
    1.0,      // order_qty
)?;
