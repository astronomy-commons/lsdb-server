

use axum::{
    routing::{any, get}, Router
};

use lsdb_server::routes::catch_all;

#[tokio::main]
async fn main() {
    println!("Starting Server");

    let app = Router::new()
        .route("/", get(|| async { "online" }))
        .fallback(any(catch_all));  // This will catch all other paths

    let listener = tokio::net::TcpListener::bind("0.0.0.0:5000").await.unwrap();
    axum::serve(listener, app).await.unwrap();
}