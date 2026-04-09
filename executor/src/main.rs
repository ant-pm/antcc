mod executor;

use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let colony_host = std::env::var("COLONY_HOST").expect("COLONY_HOST not set");
    colonyos::set_server_url(&format!("https://{colony_host}/api"));
    executor::run_executor().await
}
