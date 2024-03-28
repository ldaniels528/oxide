////////////////////////////////////////////////////////////////////
//      Oxide Server v0.1.0
////////////////////////////////////////////////////////////////////

use std::env;
use std::sync::Mutex;

use actix::{Actor, Addr};
use actix_web::{HttpRequest, HttpResponse, Responder, web};
use actix_web_actors::ws;
use log::{error, info, LevelFilter};
use serde_json::Value;

use shared_lib::{cnv_error, fail, get_host_and_port};
use shared_lib::{RemoteCallRequest, RemoteCallResponse};

use crate::compiler::Compiler;
use crate::dataframe_actor::DataframeIO;
use crate::dataframe_config::DataFrameConfig;
use crate::machine::MachineState;
use crate::namespaces::Namespace;
use crate::rows::Row;
use crate::server::{RowJs, SystemInfoJs, to_row, to_row_json};
use crate::table_columns::TableColumn;
use crate::websockets::OxideWebSocket;

mod codec;
mod compiler;
mod dataframe_actor;
mod dataframe_config;
mod dataframes;
mod data_types;
mod expression;
mod field_metadata;
mod fields;
mod machine;
mod namespaces;
mod opcode;
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

#[macro_export]
macro_rules! web_routes {
    () => {
        actix_web::App::new()
            .app_data(web::Data::new(crate::AppState::new()))
            .route("/", web::get().to(handle_index))
            .route("/rpc", web::post().to(handle_post_rpc))
            .route("/{database}/{schema}/{name}", web::delete().to(handle_delete_config))
            .route("/{database}/{schema}/{name}", web::get().to(handle_get_config))
            .route("/{database}/{schema}/{name}", web::post().to(handle_post_config))
            .route("/{database}/{schema}/{name}/{id}", web::delete().to(handle_delete_row))
            .route("/{database}/{schema}/{name}/{id}", web::get().to(handle_get_row))
            .route("/{database}/{schema}/{name}/{id}", web::patch().to(handle_patch_row))
            .route("/{database}/{schema}/{name}/{id}", web::post().to(handle_post_row))
            .route("/{database}/{schema}/{name}/{id}", web::put().to(handle_put_row))
            .service(web::resource("/ws").to(handle_websockets))
    }
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    // setup the logger
    env_logger::builder()
        .filter_level(LevelFilter::Info)
        .init();

    // get the commandline arguments
    let (host, port) = get_host_and_port(env::args().collect())?;

    // start the server
    info!("Welcome to Oxide Server.\n");
    info!("Starting server on port {}:{}.", host, port);
    let server = actix_web::HttpServer::new(move || web_routes!())
        .bind(format!("{}:{}", host, port))?
        .run();
    server.await?;
    Ok(())
}

/// handler function for the index route
// ex: http://localhost:8080/
async fn handle_index() -> impl Responder {
    HttpResponse::Ok().json(SystemInfoJs::new())
}

/// handler function for deleting a configuration by namespace (database, schema, name)
// ex: http://localhost:8080/dataframes/create/quotes
async fn handle_delete_config(path: web::Path<(String, String, String)>) -> impl Responder {
    match DataFrameConfig::delete(&Namespace::new(&path.0, &path.1, &path.2)) {
        Ok(_) => HttpResponse::Ok().finish(),
        Err(err) => {
            error!("error {}", err.to_string());
            HttpResponse::InternalServerError().finish()
        }
    }
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
    async fn intern(req: HttpRequest,
                    data: web::Json<DataFrameConfig>,
                    path: web::Path<(String, String, String)>) -> std::io::Result<usize> {
        let ns = Namespace::new(&path.0, &path.1, &path.2);
        Ok(create_table_from_config!(get_app_state(&req)?.actor, ns, data.0)?)
    }
    match intern(req, data, path).await {
        Ok(_) => HttpResponse::Ok().finish(),
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

/// handler function for appending a new/existing row by namespace (database, schema, name) and offset
// ex: http://localhost:8080/dataframes/create/quotes
async fn handle_post_row(req: HttpRequest,
                         data: web::Json<RowJs>,
                         path: web::Path<(String, String, String, usize)>) -> impl Responder {
    match append_row(req, data, path).await {
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
async fn handle_patch_row(req: HttpRequest,
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
    fn intern(req: HttpRequest, data: web::Json<RemoteCallRequest>) -> std::io::Result<Value> {
        let session_data = req.app_data::<web::Data<AppState>>().unwrap();
        let mut machine = session_data.machine.lock().unwrap();
        let opcodes = Compiler::compile(data.0.get_code())?;
        let (new_state, result) = machine.evaluate_all(&opcodes)?;
        machine.add_variables(new_state.get_variables().clone());
        Ok(result.to_json())
    }
    match intern(req, data) {
        Ok(result) => HttpResponse::Ok().json(RemoteCallResponse::success(result)),
        Err(err) => HttpResponse::Ok().json(RemoteCallResponse::fail(err.to_string())),
    }
}

async fn handle_websockets(req: HttpRequest, stream: web::Payload) -> impl Responder {
    info!("received ws <- {}", req.peer_addr().unwrap());
    ws::start(OxideWebSocket::new(), &req, stream)
}

async fn append_row(req: HttpRequest,
                    data: web::Json<RowJs>,
                    path: web::Path<(String, String, String, usize)>) -> std::io::Result<usize> {
    let ns = Namespace::new(&path.0, &path.1, &path.2);
    let df = get_app_state(&req)?.actor.clone();
    let columns = get_columns!(df, ns)?;
    append_row!(df, ns, to_row(&columns, data.0, 0))
}

async fn delete_row_by_id(req: HttpRequest,
                          path: web::Path<(String, String, String, usize)>) -> std::io::Result<usize> {
    let (ns, id) = (Namespace::new(&path.0, &path.1, &path.2), path.3);
    let df = get_app_state(&req)?.actor.clone();
    delete_row!(df, ns, id)
}

/// Retrieves the application state
fn get_app_state(req: &HttpRequest) -> std::io::Result<&web::Data<AppState>> {
    match req.app_data::<web::Data<AppState>>() {
        None => fail("No app state"),
        Some(app) => Ok(app)
    }
}

async fn get_row_by_id(req: HttpRequest,
                       path: web::Path<(String, String, String, usize)>) -> std::io::Result<Option<Row>> {
    let (ns, id) = (Namespace::new(&path.0, &path.1, &path.2), path.3);
    let df = get_app_state(&req)?.actor.clone();
    read_row!(df, ns, id)
}

async fn overwrite_row_by_id(req: HttpRequest,
                             data: web::Json<RowJs>,
                             path: web::Path<(String, String, String, usize)>) -> std::io::Result<usize> {
    let (ns, id) = (Namespace::new(&path.0, &path.1, &path.2), path.3);
    let df = get_app_state(&req)?.actor.clone();
    let columns = get_columns!(df, ns)?;
    overwrite_row!(df, ns, to_row(&columns, data.0, id))
}

async fn update_row_by_id(req: HttpRequest,
                          data: web::Json<RowJs>,
                          path: web::Path<(String, String, String, usize)>) -> std::io::Result<usize> {
    let (ns, id) = (Namespace::new(&path.0, &path.1, &path.2), path.3);
    let df = get_app_state(&req)?.actor.clone();
    let columns = get_columns!(df, ns)?;
    update_row!(df, ns, to_row(&columns, data.0, id))
}

// Unit tests
#[cfg(test)]
mod tests {
    use actix_web::test;
    use futures_util::SinkExt;
    use serde_json::json;
    use tokio_tungstenite::connect_async;
    use tokio_tungstenite::tungstenite::Message;

    use super::*;

    #[actix::test]
    async fn test_dataframe_config_lifecycle() {
        let mut app = test::init_service(web_routes!()).await;

        // send a POST with the config
        let (database, schema, name) = ("web", "rest", "config");
        let req = test::TestRequest::post().uri(&ns_uri(database, schema, name))
            .set_json(&json!({"columns":[
                {"name":"symbol","column_type":"String(4)","default_value":null},
                {"name":"exchange","column_type":"String(4)","default_value":null},
                {"name":"lastSale","column_type":"Double","default_value":null}],
                "indices":[],"partitions":[]}))
            .to_request();
        let resp = test::call_service(&mut app, req).await;
        assert!(resp.status().is_success());

        // send a GET to retrieve the config
        let req = test::TestRequest::get().uri(&ns_uri(database, schema, name)).to_request();
        let resp = test::call_service(&mut app, req).await;
        assert!(resp.status().is_success());
        let body = String::from_utf8(test::read_body(resp).await.to_vec()).unwrap();
        assert_eq!(body, r#"{"columns":[{"name":"symbol","column_type":"String(4)","default_value":null},{"name":"exchange","column_type":"String(4)","default_value":null},{"name":"lastSale","column_type":"Double","default_value":null}],"indices":[],"partitions":[]}"#);

        // DELETE the config
        let req = test::TestRequest::delete().uri(&ns_uri(database, schema, name)).to_request();
        let resp = test::call_service(&mut app, req).await;
        assert!(resp.status().is_success());
    }

    #[actix::test]
    async fn test_handle_rpc() {
        let mut app = test::init_service(web_routes!()).await;

        // send a POST with the RPC code
        let req = test::TestRequest::post().uri("/rpc")
            .set_json(&json!({"code": "(5 * 7) + 1"}))
            .to_request();

        // process the request and verify the response
        let resp = test::call_service(&mut app, req).await;
        assert!(resp.status().is_success());
        let body = String::from_utf8(test::read_body(resp).await.to_vec()).unwrap();
        assert_eq!(body, "{\"result\":36,\"message\":null}");
    }

    #[actix::test]
    async fn test_dataframe_lifecycle() {
        let mut app = test::init_service(web_routes!()).await;
        let (database, schema, name) = ("web", "dataframe", "lifecycle");

        // POST to create a new table
        let req = test::TestRequest::post()
            .uri(&ns_uri(database, schema, name))
            .set_json(&json!({"columns":[
                {"name":"symbol","column_type":"String(8)","default_value":null},
                {"name":"exchange","column_type":"String(8)","default_value":null},
                {"name":"lastSale","column_type":"Double","default_value":null}],
                "indices":[],"partitions":[]})).to_request();
        let resp = test::call_service(&mut app, req).await;
        assert!(resp.status().is_success());

        // POST to append a new stock quote
        let req = test::TestRequest::post().uri(&row_uri(database, schema, name, 0))
            .set_json(&json!({"fields": [
                {"name": "symbol", "value": "ATOM"},
                {"name": "exchange","value": "NYSE"},
                {"name": "lastSale","value": 24.17}]
            })).to_request();
        let resp = test::call_service(&mut app, req).await;
        assert!(resp.status().is_success());

        // POST to append a second stock quote
        let req = test::TestRequest::post().uri(&row_uri(database, schema, name, 0))
            .set_json(&json!({"fields": [
                {"name": "symbol", "value": "BABY"},
                {"name": "exchange","value": "NYSE"},
                {"name": "lastSale","value": 13.66}]
            })).to_request();
        let resp = test::call_service(&mut app, req).await;
        assert!(resp.status().is_success());

        // GET one of the previously POSTed stock quotes
        let req = test::TestRequest::get().uri(&row_uri(database, schema, name, 0)).to_request();
        let resp = test::call_service(&mut app, req).await;
        assert!(resp.status().is_success());
        let body = String::from_utf8(test::read_body(resp).await.to_vec()).unwrap();
        assert_eq!(body, r#"{"id":0,"fields":[{"name":"symbol","value":"ATOM"},{"name":"exchange","value":"NYSE"},{"name":"lastSale","value":24.17}]}"#);

        // PATCH one of the previously POSTed stock quotes with a new price
        let req = test::TestRequest::patch().uri(&row_uri(database, schema, name, 1))
            .set_json(&json!({"id": 1, "fields": [{"name": "lastSale","value": 13.33}]})).to_request();
        let resp = test::call_service(&mut app, req).await;
        assert!(resp.status().is_success());

        // GET the previously PATCHed stock quote
        let req = test::TestRequest::get().uri(&row_uri(database, schema, name, 1)).to_request();
        let resp = test::call_service(&mut app, req).await;
        assert!(resp.status().is_success());
        let body = String::from_utf8(test::read_body(resp).await.to_vec()).unwrap();
        assert_eq!(body, r#"{"id":1,"fields":[{"name":"symbol","value":"BABY"},{"name":"exchange","value":"NYSE"},{"name":"lastSale","value":13.33}]}"#);

        // PUT to overwrite the row
        let req = test::TestRequest::put().uri(&row_uri(database, schema, name, 0))
            .set_json(&json!({"fields": [
                {"name": "symbol", "value": "CRY"},
                {"name": "exchange","value": "NYSE"},
                {"name": "lastSale","value": 88.11}]
            })).to_request();
        let resp = test::call_service(&mut app, req).await;
        assert!(resp.status().is_success());

        // GET the stock quote from the previous PUT
        let req = test::TestRequest::get().uri(&row_uri(database, schema, name, 0)).to_request();
        let resp = test::call_service(&mut app, req).await;
        assert!(resp.status().is_success());
        let body = String::from_utf8(test::read_body(resp).await.to_vec()).unwrap();
        assert_eq!(body, r#"{"id":0,"fields":[{"name":"symbol","value":"CRY"},{"name":"exchange","value":"NYSE"},{"name":"lastSale","value":88.11}]}"#);

        // DELETE the first row
        let req = test::TestRequest::delete().uri(&row_uri(database, schema, name, 0)).to_request();
        let resp = test::call_service(&mut app, req).await;
        assert!(resp.status().is_success());

        // GET the stock quote from the previous DELETE
        let req = test::TestRequest::get().uri(&row_uri(database, schema, name, 0)).to_request();
        let resp = test::call_service(&mut app, req).await;
        assert!(resp.status().is_success());
        let body = String::from_utf8(test::read_body(resp).await.to_vec()).unwrap();
        assert_eq!(body, "{}");
    }

    #[actix::test]
    async fn test_websocket() {
        let app = test::init_service(web_routes!()).await;
        match send_message("Hello, WebSocket Server!").await {
            Ok(_) => {
                match send_message("I need some info!").await {
                    Ok(_) => {}
                    Err(e) => eprintln!("Error: {}", e)
                }
            }
            Err(e) => eprintln!("Error: {}", e)
        }
    }

    async fn send_message(message: &str) -> Result<(), Box<dyn std::error::Error>> {
        // Connect to the WebSocket server
        let (mut ws_stream, response) =
            connect_async("ws://localhost:8080/ws").await?;
        println!("response: {:?}", response);

        // Send a message
        ws_stream.send(Message::Text(message.to_string())).await?;
        Ok(())
    }

    fn ns_uri(database: &str, schema: &str, name: &str) -> String {
        format!("/{}/{}/{}", database, schema, name)
    }

    fn row_uri(database: &str, schema: &str, name: &str, id: usize) -> String {
        format!("/{}/{}/{}/{}", database, schema, name, id)
    }
}