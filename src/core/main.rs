////////////////////////////////////////////////////////////////////
//      Oxide Server v0.1.0
////////////////////////////////////////////////////////////////////

use std::io;
use std::sync::Mutex;

use actix_web::{HttpRequest, HttpResponse, Responder, web};
use actix_web::web::Data;
use log::{error, LevelFilter};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::compiler::Compiler;
use crate::dataframe_config::DataFrameConfig;
use crate::dataframes::DataFrame;
use crate::error_mgmt::fail;
use crate::machine::MachineState;
use crate::namespaces::Namespace;
use crate::peers::{RemoteCallRequest, RemoteCallResponse};
use crate::rows::Row;
use crate::server::{RowJs, SystemInfoJs, to_row, to_row_json};
use crate::typed_values::TypedValue;

mod codec;
mod compiler;
mod dataframes;
mod dataframe_config;
mod data_types;
mod error_mgmt;
mod expression;
mod field_metadata;
mod fields;
mod machine;
mod namespaces;
mod opcode;
mod peers;
mod row_collection;
mod row_metadata;
mod rows;
mod server;
mod table_columns;
mod table_view;
mod template;
mod testdata;
mod token_slice;
mod tokenizer;
mod tokens;
mod typed_values;

#[derive(Debug, Serialize, Deserialize)]
pub struct AppState {
    machine: Mutex<MachineState>,
}

impl AppState {
    pub fn new() -> Self {
        Self {
            machine: Mutex::new(MachineState::new()),
        }
    }
}

#[actix_web::main]
async fn main() -> io::Result<()> {
    env_logger::builder()
        .filter_level(LevelFilter::Info)
        .init();

    println!("Welcome to Oxide Server.\n");
    let port = get_port_number(std::env::args().collect())?;

    println!("Starting server on port {}.", port);
    let app_state = web::Data::new(AppState::new());

    let server = actix_web::HttpServer::new(move || {
        actix_web::App::new()
            .app_data(app_state.clone())
            .route("/", web::get().to(index))
            .route("/rpc", web::post().to(post_rpc))
            .route("/{database}/{schema}/{name}/{id}", web::delete().to(delete_row))
            .route("/{database}/{schema}/{name}/{id}", web::get().to(get_row))
            .route("/{database}/{schema}/{name}/{id}", web::post().to(post_row))
            .route("/{database}/{schema}/{name}/{id}", web::put().to(put_row))
            .route("/{database}/{schema}/{name}", web::get().to(get_config))
            .route("/{database}/{schema}/{name}", web::post().to(post_config))
    })
        .bind(get_address(port.as_str()))?
        .run();
    server.await?;
    Ok(())
}

/// handler function for the index route
// ex: http://localhost:8080/
async fn index() -> impl Responder {
    HttpResponse::Ok().json(SystemInfoJs::new())
}

/// handler function for reading a configuration by namespace (database, schema, name)
// ex: http://localhost:8080/dataframes/create/quotes
async fn get_config(path: web::Path<(String, String, String)>) -> impl Responder {
    let (database, schema, name) = (&path.0, &path.1, &path.2);
    match DataFrameConfig::load(&Namespace::new(database, schema, name)) {
        Ok(cfg) => HttpResponse::Ok().json(cfg),
        Err(err) => {
            error!("error {}", err.to_string());
            HttpResponse::InternalServerError().finish()
        }
    }
}

/// handler function for creating a configuration by namespace (database, schema, name)
// ex: http://localhost:8080/dataframes/create/quotes
async fn post_config(data: web::Json<DataFrameConfig>, path: web::Path<(String, String, String)>) -> impl Responder {
    let (database, schema, name) = (&path.0, &path.1, &path.2);
    let cfg: DataFrameConfig = data.0;
    match DataFrame::create(Namespace::new(database, schema, name), cfg) {
        Ok(_) => HttpResponse::Ok().finish(),
        Err(err) => {
            error!("error {}", err.to_string());
            HttpResponse::InternalServerError().finish()
        }
    }
}

/// handler function for deleting an existing row by namespace (database, schema, name) and offset
// ex: http://localhost:8080/dataframes/create/quotes
async fn delete_row(path: web::Path<(String, String, String, usize)>) -> impl Responder {
    match delete_row_by_id(path) {
        Ok(outcome) => HttpResponse::Ok().json(outcome),
        Err(err) => {
            error!("error {}", err.to_string());
            HttpResponse::InternalServerError().finish()
        }
    }
}

/// handler function for row by namespace (database, schema, name) and row ID
// ex: http://localhost:8080/dataframes/create/quotes/0
async fn get_row(path: web::Path<(String, String, String, usize)>) -> impl Responder {
    match get_row_by_id(path) {
        Ok(Some(row)) => HttpResponse::Ok().json(to_row_json(row)),
        Ok(None) => HttpResponse::Ok().json(serde_json::json!({})),
        Err(err) => {
            error!("{}", err.to_string());
            HttpResponse::InternalServerError().finish()
        }
    }
}

/// handler function for creating/overwriting a new/existing row by namespace (database, schema, name) and offset
// ex: http://localhost:8080/dataframes/create/quotes
async fn post_row(data: web::Json<RowJs>, path: web::Path<(String, String, String, usize)>) -> impl Responder {
    match overwrite_row_by_id(data, path) {
        Ok(outcome) => HttpResponse::Ok().json(outcome),
        Err(err) => {
            error!("error {}", err.to_string());
            HttpResponse::InternalServerError().finish()
        }
    }
}

/// handler function for overwriting an existing row by namespace (database, schema, name) and offset
// ex: http://localhost:8080/dataframes/create/quotes
async fn put_row(data: web::Json<RowJs>, path: web::Path<(String, String, String, usize)>) -> impl Responder {
    match update_row_by_id(data, path) {
        Ok(outcome) => HttpResponse::Ok().json(outcome),
        Err(err) => {
            error!("error {}", err.to_string());
            HttpResponse::InternalServerError().finish()
        }
    }
}

async fn post_rpc(req: HttpRequest, data: web::Json<RemoteCallRequest>) -> impl Responder {
    fn process(req: HttpRequest, data: web::Json<RemoteCallRequest>) -> io::Result<Value> {
        let session_data = req.app_data::<Data<AppState>>().unwrap();
        let mut machine = session_data.machine.lock().unwrap();
        let opcodes = Compiler::compile(data.0.get_code())?;
        let (new_state, result) = machine.run(opcodes)?;
        machine.add_variables(new_state.get_variables().clone());
        Ok(result.to_json())
    }
    match process(req, data) {
        Ok(result) => HttpResponse::Ok().json(RemoteCallResponse::success(result)),
        Err(err) => HttpResponse::Ok().json(RemoteCallResponse::fail(err.to_string())),
    }
}

fn get_address(port: &str) -> String {
    format!("127.0.0.1:{}", port)
}

fn get_port_number(args: Vec<String>) -> io::Result<String> {
    if args.len() > 1 {
        let re = regex::Regex::new(r"^\d+$").unwrap();
        let port: String = args[1].trim().into();
        if re.is_match(&port) {
            Ok(port)
        } else {
            fail(format!("Port '{}' is invalid", port))
        }
    } else { Ok("8080".into()) }
}

fn delete_row_by_id(path: web::Path<(String, String, String, usize)>) -> io::Result<usize> {
    let (database, schema, name, id) = (&path.0, &path.1, &path.2, path.3);
    let mut df = DataFrame::load(Namespace::new(database, schema, name))?;
    df.delete(id)
}

fn get_row_by_id(path: web::Path<(String, String, String, usize)>) -> io::Result<Option<Row>> {
    let (database, schema, name, id) = (&path.0, &path.1, &path.2, path.3);
    let df = DataFrame::load(Namespace::new(database, schema, name))?;
    let (row, metadata) = df.read_row(id)?;
    Ok(if metadata.is_allocated { Some(row) } else { None })
}

fn overwrite_row_by_id(data: web::Json<RowJs>, path: web::Path<(String, String, String, usize)>) -> io::Result<usize> {
    let (database, schema, name, id) = (&path.0, &path.1, &path.2, path.3);
    let mut df = DataFrame::load(Namespace::new(database, schema, name))?;
    df.overwrite(to_row(&df.columns, data.0, id))
}

fn update_row_by_id(data: web::Json<RowJs>, path: web::Path<(String, String, String, usize)>) -> io::Result<usize> {
    let (database, schema, name, id) = (&path.0, &path.1, &path.2, path.3);
    let mut df = DataFrame::load(Namespace::new(database, schema, name))?;
    df.update(to_row(&df.columns, data.0, id))
}

// Unit tests
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_address() {
        assert_eq!(get_address("8888"), "127.0.0.1:8888")
    }

    #[test]
    fn test_get_port_number_valid_input() {
        let args: Vec<String> = vec!["test_program".into(), "1234".into()];
        let expected = "1234".to_string();
        let actual = get_port_number(args).unwrap();
        assert_eq!(actual, expected)
    }

    #[test]
    fn test_get_port_number_no_input() {
        let args: Vec<String> = vec!["test_program".into()];
        let expected: String = "8080".into();
        let actual: String = get_port_number(args).unwrap();
        assert_eq!(actual, expected)
    }
}