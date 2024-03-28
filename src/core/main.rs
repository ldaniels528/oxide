////////////////////////////////////////////////////////////////////
//      Oxide Server v0.1.0
////////////////////////////////////////////////////////////////////

use std::sync::Mutex;

use actix::{Actor, Addr};
use actix_web::{App, HttpRequest, HttpResponse, Responder, web};
use actix_web_actors::ws;
use log::{error, info, LevelFilter};
use serde_json::Value;

use crate::compiler::Compiler;
use crate::dataframe_actor::DataframeIO;
use crate::dataframe_config::DataFrameConfig;
use crate::dataframes::DataFrame;
use crate::error_mgmt::fail;
use crate::machine::MachineState;
use crate::namespaces::Namespace;
use crate::rows::Row;
use crate::rpc::{RemoteCallRequest, RemoteCallResponse};
use crate::server::{RowJs, SystemInfoJs, to_row, to_row_json};
use crate::table_columns::TableColumn;
use crate::websockets::OxideWebSocket;

mod codec;
mod compiler;
mod dataframe_actor;
mod dataframe_config;
mod dataframes;
mod data_types;
mod error_mgmt;
mod expression;
mod field_metadata;
mod fields;
mod machine;
mod namespaces;
mod opcode;
mod row_collection;
mod row_metadata;
mod rows;
mod rpc;
mod server;
mod table_columns;
mod table_view;
mod template;
mod testdata;
mod token_slice;
mod tokenizer;
mod tokens;
mod typed_values;
mod websockets;

#[derive(Debug)]
pub struct AppState {
    actor: Addr<DataframeIO>,
    machine: Mutex<MachineState>,
}

impl AppState {
    pub fn new() -> Self {
        AppState {
            actor: DataframeIO::new().start(),
            machine: Mutex::new(MachineState::new()),
        }
    }
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    env_logger::builder()
        .filter_level(LevelFilter::Info)
        .init();

    info!("Welcome to Oxide Server.\n");
    let port = get_port_number(std::env::args().collect())?;
    info!("Starting server on port {}.", port);
    let server = actix_web::HttpServer::new(move || {
        actix_web::App::new()
            .app_data(web::Data::new(AppState::new()))
            .route("/", web::get().to(handle_index))
            .route("/rpc", web::post().to(handle_post_rpc))
            .route("/{database}/{schema}/{name}/{id}", web::delete().to(handle_delete_row))
            .route("/{database}/{schema}/{name}/{id}", web::get().to(handle_get_row))
            .route("/{database}/{schema}/{name}/{id}", web::post().to(handle_post_row))
            .route("/{database}/{schema}/{name}/{id}", web::put().to(handle_put_row))
            .route("/{database}/{schema}/{name}", web::get().to(handle_get_config))
            .route("/{database}/{schema}/{name}", web::post().to(handle_post_config))
            .service(web::resource("/ws").to(handle_websockets))
    })
        .bind(get_address(port.as_str()))?
        .run();
    server.await?;
    Ok(())
}

/// handler function for the index route
// ex: http://localhost:8080/
async fn handle_index() -> impl Responder {
    HttpResponse::Ok().json(SystemInfoJs::new())
}

/// handler function for reading a configuration by namespace (database, schema, name)
// ex: http://localhost:8080/dataframes/create/quotes
async fn handle_get_config(path: web::Path<(String, String, String)>) -> impl Responder {
    match DataFrameConfig::load(&Namespace::new(&path.0, &path.1, &path.2)) {
        Ok(cfg) => HttpResponse::Ok().json(cfg),
        Err(err) => {
            error!("error {}", err.to_string());
            HttpResponse::InternalServerError().finish()
        }
    }
}

/// handler function for creating a configuration by namespace (database, schema, name)
// ex: http://localhost:8080/dataframes/create/quotes
async fn handle_post_config(req: HttpRequest,
                            data: web::Json<DataFrameConfig>,
                            path: web::Path<(String, String, String)>) -> impl Responder {
    match get_app_state(&req) {
        Ok(app_state) => {
            let ns = Namespace::new(&path.0, &path.1, &path.2);
            match create_table_from_config!(app_state.actor, ns, data.0) {
                Ok(_) => HttpResponse::Ok().finish(),
                Err(err) => {
                    error!("error {}", err.to_string());
                    HttpResponse::InternalServerError().finish()
                }
            }
        }
        Err(err) => {
            error!("error {}", err.to_string());
            HttpResponse::InternalServerError().finish()
        }
    }
}

/// handler function for deleting an existing row by namespace (database, schema, name) and offset
// ex: http://localhost:8080/dataframes/create/quotes
async fn handle_delete_row(req: HttpRequest,
                           path: web::Path<(String, String, String, usize)>) -> impl Responder {
    match delete_row_by_id(req, path).await {
        Ok(outcome) => HttpResponse::Ok().json(outcome),
        Err(err) => {
            error!("error {}", err.to_string());
            HttpResponse::InternalServerError().finish()
        }
    }
}

/// handler function for row by namespace (database, schema, name) and row ID
// ex: http://localhost:8080/dataframes/create/quotes/0
async fn handle_get_row(req: HttpRequest,
                        path: web::Path<(String, String, String, usize)>) -> impl Responder {
    match get_row_by_id(req, path).await {
        Ok(Some(row)) => HttpResponse::Ok().json(to_row_json(row)),
        Ok(None) => HttpResponse::Ok().json(serde_json::json!({})),
        Err(err) => {
            error!("error {}", err.to_string());
            HttpResponse::InternalServerError().finish()
        }
    }
}

/// handler function for creating/overwriting a new/existing row by namespace (database, schema, name) and offset
// ex: http://localhost:8080/dataframes/create/quotes
async fn handle_post_row(req: HttpRequest,
                         data: web::Json<RowJs>,
                         path: web::Path<(String, String, String, usize)>) -> impl Responder {
    match overwrite_row_by_id(req, data, path).await {
        Ok(outcome) => HttpResponse::Ok().json(outcome),
        Err(err) => {
            error!("error {}", err.to_string());
            HttpResponse::InternalServerError().finish()
        }
    }
}

/// handler function for overwriting an existing row by namespace (database, schema, name) and offset
// ex: http://localhost:8080/dataframes/create/quotes
async fn handle_put_row(req: HttpRequest,
                        data: web::Json<RowJs>,
                        path: web::Path<(String, String, String, usize)>) -> impl Responder {
    match update_row_by_id(req, data, path).await {
        Ok(outcome) => HttpResponse::Ok().json(outcome),
        Err(err) => {
            error!("error {}", err.to_string());
            HttpResponse::InternalServerError().finish()
        }
    }
}

/// handler function for executing remote procedure calls
async fn handle_post_rpc(req: HttpRequest, data: web::Json<RemoteCallRequest>) -> impl Responder {
    fn process(req: HttpRequest, data: web::Json<RemoteCallRequest>) -> std::io::Result<Value> {
        let session_data = req.app_data::<web::Data<AppState>>().unwrap();
        let mut machine = session_data.machine.lock().unwrap();
        let opcodes = Compiler::compile(data.0.get_code())?;
        let (new_state, result) = machine.evaluate_all(&opcodes)?;
        machine.add_variables(new_state.get_variables().clone());
        Ok(result.to_json())
    }
    match process(req, data) {
        Ok(result) => HttpResponse::Ok().json(RemoteCallResponse::success(result)),
        Err(err) => HttpResponse::Ok().json(RemoteCallResponse::fail(err.to_string())),
    }
}

async fn handle_websockets(req: HttpRequest, stream: web::Payload) -> impl Responder {
    info!("received ws <- {}", req.peer_addr().unwrap());
    ws::start(OxideWebSocket::new(), &req, stream)
}

async fn delete_row_by_id(req: HttpRequest,
                          path: web::Path<(String, String, String, usize)>) -> std::io::Result<usize> {
    let (ns, id) = (Namespace::new(&path.0, &path.1, &path.2), path.3);
    let mut df = get_app_state(&req)?.actor.clone();
    delete_row!(df, ns, id)
}

fn get_address(port: &str) -> String {
    format!("127.0.0.1:{}", port)
}

/// Retrieves the application state
fn get_app_state(req: &HttpRequest) -> std::io::Result<&web::Data<AppState>> {
    match req.app_data::<web::Data<AppState>>() {
        None => fail("No app state"),
        Some(app) => Ok(app)
    }
}

fn get_port_number(args: Vec<String>) -> std::io::Result<String> {
    if args.len() > 1 {
        let re = regex::Regex::new(r"^\d+$").unwrap();
        let port: String = args[1].trim().into();
        if re.is_match(&port) { Ok(port) } else {
            fail(format!("Port '{}' is invalid", port))
        }
    } else { Ok("8080".into()) }
}

async fn get_row_by_id(req: HttpRequest,
                       path: web::Path<(String, String, String, usize)>) -> std::io::Result<Option<Row>> {
    let (ns, id) = (Namespace::new(&path.0, &path.1, &path.2), path.3);
    let mut df = get_app_state(&req)?.actor.clone();
    read_row!(df, ns, id)
}

async fn overwrite_row_by_id(req: HttpRequest,
                             data: web::Json<RowJs>,
                             path: web::Path<(String, String, String, usize)>) -> std::io::Result<usize> {
    let (ns, id) = (Namespace::new(&path.0, &path.1, &path.2), path.3);
    let mut df = get_app_state(&req)?.actor.clone();
    let columns = get_columns!(df, ns)?;
    overwrite_row!(df, ns, to_row(&columns, data.0, id))
}

async fn update_row_by_id(req: HttpRequest,
                          data: web::Json<RowJs>,
                          path: web::Path<(String, String, String, usize)>) -> std::io::Result<usize> {
    let (ns, id) = (Namespace::new(&path.0, &path.1, &path.2), path.3);
    let mut df = get_app_state(&req)?.actor.clone();
    let columns = get_columns!(df, ns)?;
    update_row!(df, ns, to_row(&columns, data.0, id))
}

// Unit tests
#[cfg(test)]
mod tests {
    use std::fmt::format;

    use actix_web::{App, test};
    use serde_json::json;

    use super::*;

    #[actix::test]
    async fn test_get_address() {
        assert_eq!(get_address("8888"), "127.0.0.1:8888")
    }

    #[actix::test]
    async fn test_get_port_number_valid_input() {
        let args: Vec<String> = vec!["test_program".into(), "1234".into()];
        let expected = "1234".to_string();
        let actual = get_port_number(args).unwrap();
        assert_eq!(actual, expected)
    }

    #[actix::test]
    async fn test_get_port_number_no_input() {
        let args: Vec<String> = vec!["test_program".into()];
        let expected: String = "8080".into();
        let actual: String = get_port_number(args).unwrap();
        assert_eq!(actual, expected)
    }

    #[actix::test]
    async fn test_handle_post_config() {
        // create a test app
        let mut app = test::init_service(App::new()
            .app_data(web::Data::new(AppState::new()))
            .route("/{database}/{schema}/{name}", web::post().to(handle_post_config))
            .route("/{database}/{schema}/{name}", web::get().to(handle_get_config))
        ).await;

        // send a POST with the config
        let (database, schema, name) = ("web", "rest", "config");
        let req = test::TestRequest::post()
            .uri(&format!("/{}/{}/{}", database, schema, name))
            .set_json(&json!({"columns":[
                {"name":"symbol","column_type":"String(4)","default_value":null},
                {"name":"exchange","column_type":"String(4)","default_value":null},
                {"name":"lastSale","column_type":"Double","default_value":null}],
                "indices":[],"partitions":[]}))
            .to_request();
        let resp = test::call_service(&mut app, req).await;
        assert!(resp.status().is_success());

        // send a GET to retrieve the config
        let req = test::TestRequest::get()
            .uri(&format!("/{}/{}/{}", database, schema, name)).to_request();
        let resp = test::call_service(&mut app, req).await;
        assert!(resp.status().is_success());
        let body = String::from_utf8(test::read_body(resp).await.to_vec()).unwrap();
        assert_eq!(body, r#"{"columns":[{"name":"symbol","column_type":"String(4)","default_value":null},{"name":"exchange","column_type":"String(4)","default_value":null},{"name":"lastSale","column_type":"Double","default_value":null}],"indices":[],"partitions":[]}"#);
    }

    #[actix::test]
    async fn test_handle_rpc() {
        // create a test app
        let mut app = test::init_service(App::new()
            .app_data(web::Data::new(AppState::new()))
            .route("/rpc", web::post().to(handle_post_rpc))
        ).await;

        // send a POST with the RPC code
        let req = test::TestRequest::post()
            .uri("/rpc")
            .set_json(&json!({"code": "(5 * 7) + 1"}))
            .to_request();

        // process the request and verify the response
        let resp = test::call_service(&mut app, req).await;
        assert!(resp.status().is_success());
        let body = String::from_utf8(test::read_body(resp).await.to_vec()).unwrap();
        assert_eq!(body, "{\"result\":36,\"message\":null}");
    }

    #[actix::test]
    async fn test_handle_crud_workflow() {
        // create a test app
        let mut app = test::init_service(App::new()
            .app_data(web::Data::new(AppState::new()))
            .route("/{database}/{schema}/{name}", web::post().to(handle_post_config))
            .route("/{database}/{schema}/{name}", web::get().to(handle_get_config))
            .route("/{database}/{schema}/{name}/{id}", web::delete().to(handle_delete_row))
            .route("/{database}/{schema}/{name}/{id}", web::get().to(handle_get_row))
            .route("/{database}/{schema}/{name}/{id}", web::post().to(handle_post_row))
            .route("/{database}/{schema}/{name}/{id}", web::put().to(handle_put_row))
        ).await;

        // POST a new table config
        let (database, schema, name) = ("web", "dataframe", "crud");
        let req = test::TestRequest::post()
            .uri(&format!("/{}/{}/{}", database, schema, name))
            .set_json(&json!({"columns":[
                {"name":"symbol","column_type":"String(8)","default_value":null},
                {"name":"exchange","column_type":"String(8)","default_value":null},
                {"name":"lastSale","column_type":"Double","default_value":null}],
                "indices":[],"partitions":[]}))
            .to_request();
        let resp = test::call_service(&mut app, req).await;
        assert!(resp.status().is_success());

        // POST a new row
        let req = test::TestRequest::post()
            .uri(&format!("/{}/{}/{}/{}", database, schema, name, 0))
            .set_json(&json!({"id": 0, "fields": [
                {"name": "symbol", "value": "ATOM"},
                {"name": "exchange","value": "NYSE"},
                {"name": "lastSale","value": 24.17}]
            })).to_request();
        let resp = test::call_service(&mut app, req).await;
        assert!(resp.status().is_success());

        // GET the previously POSTed row
        let req = test::TestRequest::get()
            .uri(&format!("/{}/{}/{}/{}", database, schema, name, 0)).to_request();
        let resp = test::call_service(&mut app, req).await;
        assert!(resp.status().is_success());
        let body = String::from_utf8(test::read_body(resp).await.to_vec()).unwrap();
        assert_eq!(body, r#"{"id":0,"fields":[{"name":"symbol","value":"ATOM"},{"name":"exchange","value":"NYSE"},{"name":"lastSale","value":24.17}]}"#)
    }
}