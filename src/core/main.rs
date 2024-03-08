////////////////////////////////////////////////////////////////////
//      TinyDB Server v0.1.0
////////////////////////////////////////////////////////////////////

use std::ops::Deref;

use actix_web::{HttpResponse, Responder};
use serde::Serialize;

mod codec;
mod columns;
mod dataframes;
mod dataframe_config;
mod data_types;
mod field_metadata;
mod fields;
mod namespaces;
mod row_collection;
mod row_metadata;
mod rows;
mod table_columns;
mod token_slice;
mod testdata;
mod tokenizer;
mod tokens;
mod typed_values;

type ServerError = Box<dyn std::error::Error>;

#[derive(Clone, Debug, PartialEq, Serialize)]
struct SystemInfo {
    title: String,
    version: String,
}

// Define a handler function for the index route
async fn index() -> impl Responder {
    HttpResponse::Ok().json(create_system_info())
}

#[actix_web::main]
async fn main() -> Result<(), ServerError> {
    println!("Welcome to TinyDB Server.\n");
    match get_port_number(std::env::args().collect()).await {
        Ok(port) => {
            println!("Starting server on port {}.", port);
            listen_to(&port).await
        }
        Err(err) => Err(err.into())
    }
}

fn create_system_info() -> SystemInfo {
    SystemInfo {
        title: "TinyDB".into(),
        version: "0.1.0".into(),
    }
}

fn get_address(port: &str) -> String {
    format!("127.0.0.1:{}", port)
}

async fn get_port_number(args: Vec<String>) -> Result<String, ServerError> {
    if args.len() > 1 {
        let re = regex::Regex::new(r"^\d+$").unwrap();
        let port: String = args[1].trim().into();
        if re.is_match(&port) {
            Ok(port)
        } else {
            Err(format!("Port '{}' is invalid", port).into())
        }
    } else {
        Ok("8080".into())
    }
}

async fn listen_to(port: &str) -> Result<(), ServerError> {
    let server = actix_web::HttpServer::new(|| {
        actix_web::App::new()
            .route("/", actix_web::web::get().to(index))
    })
        .bind(get_address(port))?
        .run();
    server.await?;
    Ok(())
}

// Unit tests
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_system_info() {
        let sys_info = create_system_info();
        assert_eq!(sys_info, SystemInfo {
            title: "TinyDB".into(),
            version: "0.1.0".into(),
        })
    }

    #[test]
    fn test_get_address() {
        let address = get_address("8888");
        assert_eq!(address, "127.0.0.1:8888")
    }

    #[tokio::test]
    async fn test_get_port_number_valid_input() -> Result<(), ServerError> {
        let args: Vec<String> = vec!["test_program".into(), "1234".into()];
        let expected: String = "1234".into();
        let actual: String = get_port_number(args).await?;
        assert_eq!(actual, expected);
        Ok(())
    }

    #[tokio::test]
    async fn test_get_port_number_no_input() -> Result<(), ServerError> {
        let args: Vec<String> = vec!["test_program".into()];
        let expected: String = "8080".into();
        let actual: String = get_port_number(args).await?;
        assert_eq!(actual, expected);
        Ok(())
    }
}