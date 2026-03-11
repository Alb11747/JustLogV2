use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    justlog::run_cli().await
}
