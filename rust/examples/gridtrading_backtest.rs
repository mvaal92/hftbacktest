use std::time::Instant;
use clap::Parser;
use algo::gridtrading;
use hftbacktest::{
    backtest::{
        assettype::LinearAsset,
        models::{IntpOrderLatency, PowerProbQueueFunc3, ProbQueueModel, QueuePos},
        recorder::BacktestRecorder,
        AssetBuilder,
        DataSource,
        MultiAssetMultiExchangeBacktest,
    },
    prelude::{HashMapMarketDepth, Interface},
};
use hftbacktest::backtest::ExchangeKind;

mod algo;

fn prepare_backtest() -> MultiAssetMultiExchangeBacktest<QueuePos, HashMapMarketDepth> {
    let latency_data = (20240501..20240532).map(
        |date| DataSource::File(format!("latency_{date}.npz"))
    ).collect();

    let latency_model = IntpOrderLatency::new(latency_data).unwrap();
    let asset_type = LinearAsset::new(1.0);
    let queue_model = ProbQueueModel::new(PowerProbQueueFunc3::new(3.0));

    let data = (20240501..20240532).map(
        |date| DataSource::File(format!("1000SHIBUSDT_{date}.npz"))
    ).collect();

    let hbt = MultiAssetMultiExchangeBacktest::builder()
        .add(
            AssetBuilder::new()
                .data(data)
                .latency_model(latency_model)
                .asset_type(asset_type)
                .maker_fee(-0.00005)
                .taker_fee(0.0007)
                .queue_model(queue_model)
                .depth(|| HashMapMarketDepth::new(0.000001, 1.0))
                .exchange(ExchangeKind::NoPartialFillExchange)
                .build()
                .unwrap(),
        )
        .build()
        .unwrap();
    hbt
}

fn main() {
    tracing_subscriber::fmt::init();

    let half_spread_bp = 0.0005;
    let grid_interval_bp = 0.0005;
    let grid_num = 10;
    let skew = half_spread_bp / grid_num as f64;
    let order_qty = 1.0;

    let mut start = Instant::now();
    let mut hbt = prepare_backtest();
    let mut recorder = BacktestRecorder::new(&hbt);
    gridtrading(
        &mut hbt,
        &mut recorder,
        half_spread_bp,
        grid_interval_bp,
        grid_num,
        skew,
        order_qty
    )
        .unwrap();
    hbt.close().unwrap();
    print!("{} seconds", start.elapsed().as_secs());
    recorder.to_csv(".").unwrap();
}
