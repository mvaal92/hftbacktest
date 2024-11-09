use std::{thread, time::Duration};
use tracing::{info, error, debug};
use hftbacktest::{
    live::{
        ipc::iceoryx::IceoryxUnifiedChannel,
        Instrument,
        LiveBotBuilder,
    },
    prelude::{Bot, HashMapMarketDepth},
    depth::{MarketDepth, INVALID_MIN, INVALID_MAX},
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();
    info!("Starting market data monitor...");
    
    // First verify IceOryx is running
    if !std::path::Path::new("/tmp/iceoryx2/services").exists() {
        error!("IceOryx service directory not found. Please run restart_iceoryx.sh first");
        std::process::exit(1);
    }

    // Log IceOryx services
    info!("Found IceOryx services:");
    let services: Vec<String> = std::fs::read_dir("/tmp/iceoryx2/services")?
        .filter_map(Result::ok)
        .map(|e| e.file_name().to_string_lossy().to_string())
        .collect();

    if services.is_empty() {
        error!("No IceOryx services found!");
        return Err("No IceOryx services found".into());
    }

    for service in services {
        info!("Found service: {}", service);
        let service_name = service.trim_end_matches(".service")
            .trim_start_matches("iox2_");
        
        info!("Attempting to connect to service: {}", service_name);
        
        let mut bot = match LiveBotBuilder::new()
            .register(Instrument::new(
                "bybit",
                "ETHUSDT",
                0.01,     
                0.001,    
                HashMapMarketDepth::new(0.01, 10000.0),
                0,
            ))
            .error_handler(|e| {
                error!("Channel error: {:?}", e);
                Ok(())
            })
            .build::<IceoryxUnifiedChannel>() {
                Ok(bot) => {
                    info!("Successfully connected to service");
                    bot
                }
                Err(e) => {
                    error!("Failed to connect to service: {:?}", e);
                    continue;
                }
        };

        // Rest of your code remains the same
        thread::sleep(Duration::from_secs(2));
        let mut feed_count = 0;

        loop {
            debug!("Waiting for next feed...");
            match bot.wait_next_feed(true, 5_000_000_000) {
                Ok(_) => {
                    feed_count += 1;
                    let depth = bot.depth(0);
                    
                    info!("Received feed #{}", feed_count);
                    
                    // Print market depth state first
                    println!("\n=== Feed #{} ===", feed_count);
                    println!("Market Depth State:");
                    println!("  Best Bid Tick: {}", depth.best_bid_tick());
                    println!("  Best Ask Tick: {}", depth.best_ask_tick());
                    println!("  Tick Size: {}", depth.tick_size());

                    // Only print depth levels if they're valid
                    if depth.best_bid_tick() != INVALID_MIN {
                        println!("\nBid Prices:");
                        let start_tick = depth.best_bid_tick().saturating_sub(10);
                        for tick in start_tick..=depth.best_bid_tick() {
                            let qty = depth.bid_qty_at_tick(tick);
                            if qty > 0.0 {
                                let price = tick as f64 * depth.tick_size();
                                println!("  ${:.2} -> {:.3}", price, qty);
                            }
                        }
                    } else {
                        println!("\nNo valid bid prices");
                    }

                    if depth.best_ask_tick() != INVALID_MAX {
                        println!("\nAsk Prices:");
                        let end_tick = depth.best_ask_tick().saturating_add(10);
                        for tick in depth.best_ask_tick()..=end_tick {
                            let qty = depth.ask_qty_at_tick(tick);
                            if qty > 0.0 {
                                let price = tick as f64 * depth.tick_size();
                                println!("  ${:.2} -> {:.3}", price, qty);
                            }
                        }
                    } else {
                        println!("\nNo valid ask prices");
                    }
                    
                    // Print feed latency if available
                    if let Some((exch_ts, local_ts)) = bot.feed_latency(0) {
                        let latency = (local_ts - exch_ts) as f64 / 1_000_000.0;
                        println!("\nFeed Latency: {:.2}ms", latency);
                    }

                    thread::sleep(Duration::from_millis(100));
                }
                Err(e) => {
                    error!("Feed error: {:?}", e);
                    debug!("Waiting 1 second before retry...");
                    thread::sleep(Duration::from_secs(1));
                }
            }
        }
    }

    Ok(())
}