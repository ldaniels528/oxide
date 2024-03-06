////////////////////////////////////////////////////////////////////
//      TinyDB Server v0.1.0
////////////////////////////////////////////////////////////////////

use std::env;

use actix_web::{App, HttpResponse, HttpServer, Responder, web};
use serde::Serialize;

#[derive(Debug, Serialize)]
struct SystemInfo {
    title: String,
    version: String,
}

// Define a handler function for the index route
async fn index() -> impl Responder {
    HttpResponse::Ok().json(SystemInfo {
        title: "TinyDB".to_string(),
        version: "0.1.0".to_string(),
    })
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    println!("TinyDB server v0.1.0\n");

    // Get command-line arguments
    let port = get_port_number();
    println!("Starting to server on port {port}...");

    // Start the HTTP server
    HttpServer::new(|| {
        // Create an App
        App::new()
            // Define the index route
            .route("/", web::get().to(index))
    })
        .bind(format!("127.0.0.1:{port}"))? // Bind the server to the local address and port 8080
        .run() // Run the server
        .await // Await the result
}

fn get_port_number() -> String {
    let args: Vec<String> = env::args().collect();
    let port: String = if args.len() > 1 {
        args[1].trim().to_string()
    } else {
        "8080".into()
    };
    port
}