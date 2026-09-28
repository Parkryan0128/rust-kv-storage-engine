#[path = "demo/mod.rs"]
mod web;
fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    web::run()
}
