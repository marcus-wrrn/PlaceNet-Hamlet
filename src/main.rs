use rustls;
use tracing::info;
use placenet_home::config::Config;
use placenet_home::app::App;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("Failed to install ring CryptoProvider");
    dotenvy::dotenv().ok();

    let config = Config::from_env();
    let ctx = App::initialize(config).await;

    let broadcast = ctx.broadcast_state();
    tokio::spawn(broadcast.run_broadcast_loop());
    tokio::spawn(ctx.run_beacon_message_loop());

    tokio::signal::ctrl_c().await.ok();
    info!("Shutting down");
}
