#![warn(dead_code)]
////////////////////////////////////////////////////////////////////
// Oxide Server module
////////////////////////////////////////////////////////////////////

use crate::byte_code_compiler::ByteCodeCompiler;
use crate::columns::Column;
use crate::compiler::Compiler;
use crate::dataframe_actor::DataframeActor;
use crate::errors::throw;
use crate::errors::Errors::Exact;
use crate::expression::Expression;
use crate::expression::Expression::Literal;
use crate::interpreter::Interpreter;
use crate::namespaces::Namespace;
use crate::object_config::ObjectConfig;
use crate::packages::VERSION;
use crate::parameter::Parameter;
use crate::row_metadata::RowMetadata;
use crate::structures::Structures::Soft;
use crate::structures::{Row, SoftStructure, Structure, Structures};
use crate::typed_values::TypedValue;
use crate::typed_values::TypedValue::{ErrorValue, StringValue, Structured, Undefined};
use crate::web_engine::{WebSocketSystemServer, WebSocketUserServer};
use crate::*;
use actix::{Actor, Addr, StreamHandler};
use actix_session::Session;
use actix_web::dev::Server;
use actix_web::http::Method;
use actix_web::{web, App, HttpRequest, HttpResponse, Responder};
use actix_web_actors::ws;
use actix_web_actors::ws::WebsocketContext;
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use log::{error, info};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::error::Error;
use std::io::{stdout, Write};
use std::ops::Deref;
use std::thread;
use std::thread::JoinHandle;
use tokio::net::TcpStream;
use tokio::runtime::Runtime;
use tokio_tungstenite::tungstenite::handshake::client::Response;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};

/// API Methods
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum APIMethods {
    DELETE,
    GET,
    HEAD,
    PATCH,
    POST,
    PUT,
    WS
}

impl APIMethods {
    pub fn from_string(method: &str) -> std::io::Result<Self> {
        Ok(match method {
            "DELETE" => Self::DELETE,
            "GET" => Self::GET,
            "HEAD" => Self::HEAD,
            "PATCH" => Self::PATCH,
            "POST" => Self::POST,
            "PUT" => Self::PUT,
            "WS" => Self::WS,
            other => return throw(Exact(format!("{other} is not a valid HTTP method")))
        })
    }

    pub fn from_method(method: &Method) -> Option<Self> {
        Some(match method.clone() {
            Method::DELETE => Self::DELETE,
            Method::GET => Self::GET,
            Method::HEAD => Self::HEAD,
            Method::PATCH => Self::PATCH,
            Method::POST => Self::POST,
            Method::PUT => Self::PUT,
            _ => return None
        })
    }
}

/// Remote Call Request
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct RemoteCallRequest {
    code: String,
}

impl RemoteCallRequest {
    pub fn new(code: String) -> Self {
        RemoteCallRequest { code }
    }

    pub fn get_code(&self) -> &String { &self.code }
}

/// Remote Call Response
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RemoteCallResponse {
    result: Value,
    message: Option<String>,
}

impl RemoteCallResponse {
    pub fn from_string(json_string: &str) -> std::io::Result<RemoteCallResponse> {
        serde_json::from_str(json_string).map_err(|e| cnv_error!(e))
    }

    pub fn fail(message: String) -> Self {
        RemoteCallResponse {
            result: Value::Null,
            message: Some(message),
        }
    }

    pub fn success(value: Value) -> Self {
        RemoteCallResponse {
            result: value,
            message: None,
        }
    }

    pub fn get_message(&self) -> Option<String> { self.message.to_owned() }

    pub fn get_result(&self) -> Value { self.result.to_owned() }
}

/// Represents all the shared state of the application
#[derive(Debug)]
pub struct SharedState {
    apis: Vec<UserAPI>,
    actor: Addr<DataframeActor>,
}

impl SharedState {
    pub fn new() -> Self {
        SharedState {
            apis: vec![],
            actor: DataframeActor::new().start(),
        }
    }

    pub fn with_user_apis(apis: Vec<UserAPI>) -> Self {
        SharedState {
            apis,
            actor: DataframeActor::new().start(),
        }
    }
}

// JSON representation of Oxide system information
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SystemInfoJs {
    title: String,
    version: String,
}

impl SystemInfoJs {
    pub fn new() -> SystemInfoJs {
        SystemInfoJs {
            title: "Oxide".into(),
            version: VERSION.into(),
        }
    }
}

/// User API
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct UserAPI {
    pub path: String,
    pub methods: Vec<UserAPIMethod>,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct UserAPIMethod {
    pub method: APIMethods,
    pub code: TypedValue
}

#[macro_export]
macro_rules! web_routes {
    ($shared_state: expr) => {
        actix_web::App::new()
            .app_data(web::Data::new($shared_state))
            .service(web::resource("/ws").to(handle_system_websockets))
            .route("/{database}/{schema}/{name}/{a}/{b}", web::get().to(handle_row_range_get))
            .route("/{database}/{schema}/{name}/{id}", web::delete().to(handle_row_delete))
            .route("/{database}/{schema}/{name}/{id}", web::get().to(handle_row_get))
            .route("/{database}/{schema}/{name}/{id}", web::head().to(handle_row_head))
            .route("/{database}/{schema}/{name}/{id}", web::patch().to(handle_row_patch))
            .route("/{database}/{schema}/{name}/{id}", web::post().to(handle_row_post))
            .route("/{database}/{schema}/{name}/{id}", web::put().to(handle_row_put))
            .route("/{database}/{schema}/{name}", web::delete().to(handle_config_delete))
            .route("/{database}/{schema}/{name}", web::get().to(handle_config_get))
            .route("/{database}/{schema}/{name}", web::post().to(handle_config_post))
            .route("/", web::get().to(handle_index))
            .route("/info", web::get().to(handle_sys_info_get))
            .route("/rpc", web::post().to(handle_rpc_post))
    }
}

/// Converts a graph of [Structures] into a vector of [UserAPI]s
pub fn convert_to_user_api_config(
    value: &TypedValue
) -> std::io::Result<Vec<UserAPI>> {

    /// Extracts a [Structures] from a [TypedValue]
    fn extract_structure(value: &TypedValue) -> std::io::Result<Structures> {
        match value {
            Structured(ss) => Ok(ss.clone()),
            other => throw(Exact(format!("Structure expected near {}", other.to_code())))
        }
    }

    // perform the transformation
    let mut user_apis = vec![];
    let ss0 = extract_structure(value)?;
    for (path, graph) in ss0.to_name_values() {
        let ss1 = extract_structure(&graph)?;
        let mut methods = vec![];
        for (method_str, code) in ss1.to_name_values() {
            let method = APIMethods::from_string(method_str.as_str())?;
            methods.push(UserAPIMethod  { method, code });
        }
        user_apis.push(UserAPI { path, methods })
    }
    Ok(user_apis)
}

fn get_session(session: &Session) -> Result<Interpreter, Box<dyn Error>> {
    if let Some(state) = session.get::<Interpreter>("state")? {
        info!("read session: {:?}", state);
        Ok(state)
    } else {
        let state = Interpreter::new();
        info!("create session: {:?}", state);
        session.insert("state", state.to_owned())?;
        Ok(state)
    }
}

/// handler function for the index route
// ex: http://localhost:8080/
pub async fn handle_index(_session: Session) -> impl Responder {
    HttpResponse::Ok().body(r#"
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>Welcome to Oxide Server</title>
        </head>
        <body>
            <h1>Welcome to Oxide Server</h1>
            <form action="/rpc" method="post">
                <div style="width: 100%">
                    <textarea name="code" rows="30" cols="80" placeholder="Enter text here..."></textarea>
                </div>
                <div style="width: 100%">
                    <input type="button" value="Sumbit">
                </div>
            </form>
            <div id="output" style="width: 100%"></div>
        </body>
        </html>
    "#)
}

/// handler function for deleting a configuration by namespace (database, schema, name)
// ex: http://localhost:8080/dataframes/create/quotes
pub async fn handle_config_delete(
    path: web::Path<(String, String, String)>
) -> impl Responder {
    match ObjectConfig::delete(&Namespace::new(&path.0, &path.1, &path.2)) {
        Ok(_) => HttpResponse::Ok().finish(),
        Err(err) => {
            error!("error {}", err.to_string());
            HttpResponse::InternalServerError().finish()
        }
    }
}

/// handler function for reading a configuration by namespace (database, schema, name)
// ex: http://localhost:8080/dataframes/create/quotes
pub async fn handle_config_get(
    path: web::Path<(String, String, String)>
) -> impl Responder {
    match ObjectConfig::load(&Namespace::new(&path.0, &path.1, &path.2)) {
        Ok(cfg) => HttpResponse::Ok().json(cfg),
        Err(err) => {
            error!("error {}", err.to_string());
            HttpResponse::InternalServerError().finish()
        }
    }
}

/// handler function for creating a configuration by namespace (database, schema, name)
/// ex: http://localhost:8080/dataframes/create/quotes
pub async fn handle_config_post(
    req: HttpRequest,
    data: web::Json<ObjectConfig>,
    path: web::Path<(String, String, String)>,
) -> impl Responder {
    async fn intern(req: HttpRequest,
                    data: web::Json<ObjectConfig>,
                    path: web::Path<(String, String, String)>) -> std::io::Result<usize> {
        let ns = Namespace::new(&path.0, &path.1, &path.2);
        Ok(create_table_from_config!(get_shared_state(&req)?.actor, ns, data.0)?)
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
pub async fn handle_row_delete(
    req: HttpRequest,
    path: web::Path<(String, String, String, usize)>,
) -> impl Responder {
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
pub async fn handle_row_get(
    req: HttpRequest,
    path: web::Path<(String, String, String, usize)>,
) -> impl Responder {
    match get_row_by_id(req, path).await {
        Ok((columns, Some(row))) => HttpResponse::Ok().json(row.to_hash_json_value(&Parameter::from_columns(&columns))),
        Ok((_, None)) => HttpResponse::Ok().json(serde_json::json!({})),
        Err(err) => {
            error!("error {}", err.to_string());
            HttpResponse::InternalServerError().finish()
        }
    }
}

/// handler function for row metadata by namespace (database, schema, name) and row ID
// ex: http://localhost:8080/dataframes/create/quotes/0
pub async fn handle_row_head(
    req: HttpRequest,
    path: web::Path<(String, String, String, usize)>,
) -> impl Responder {
    match get_row_metadata_by_id(req, path).await {
        Ok(meta) => HttpResponse::Ok().json(meta),
        Err(err) => {
            error!("error {}", err.to_string());
            HttpResponse::InternalServerError().finish()
        }
    }
}

/// handler function for appending a new row by namespace (database, schema, name) and offset
// ex: http://localhost:8080/dataframes/create/quotes
pub async fn handle_row_post(
    req: HttpRequest,
    data: web::Json<Value>,
    path: web::Path<(String, String, String, usize)>,
) -> impl Responder {
    match append_row(req, data, path).await {
        Ok(outcome) => HttpResponse::Ok().json(outcome),
        Err(err) => {
            error!("error {}", err.to_string());
            HttpResponse::InternalServerError().finish()
        }
    }
}

/// handler function for replacing an existing row by namespace (database, schema, name) and offset
// ex: http://localhost:8080/dataframes/create/quotes
pub async fn handle_row_put(
    req: HttpRequest,
    data: web::Json<Value>,
    path: web::Path<(String, String, String, usize)>,
) -> impl Responder {
    match overwrite_row_by_id(req, data, path).await {
        Ok(outcome) => HttpResponse::Ok().json(outcome),
        Err(err) => {
            error!("error {}", err.to_string());
            HttpResponse::InternalServerError().finish()
        }
    }
}

/// handler function for patching an existing row by namespace (database, schema, name) and offset
// ex: http://localhost:8080/dataframes/create/quotes
pub async fn handle_row_patch(
    req: HttpRequest,
    data: web::Json<Value>,
    path: web::Path<(String, String, String, usize)>,
) -> impl Responder {
    match update_row_by_id(req, data, path).await {
        Ok(outcome) => HttpResponse::Ok().json(outcome),
        Err(err) => {
            error!("error {}", err.to_string());
            HttpResponse::InternalServerError().finish()
        }
    }
}

/// handler function for row by namespace (database, schema, name) and row range
// ex: http://localhost:8080/dataframes/create/quotes/0/2
pub async fn handle_row_range_get(
    req: HttpRequest,
    path: web::Path<(String, String, String, usize, usize)>,
) -> impl Responder {
    match get_range_by_id(req, path).await {
        Ok((columns, rows)) =>
            HttpResponse::Ok().json(Row::rows_to_json(&Parameter::from_columns(&columns), &rows)),
        Err(err) => {
            error!("error {}", err.to_string());
            HttpResponse::InternalServerError().finish()
        }
    }
}

/// handler function for executing remote procedure calls
pub async fn handle_rpc_post(
    session: Session,
    data: web::Json<RemoteCallRequest>,
) -> impl Responder {
    let mut interpreter = match get_session(&session) {
        Ok(the_interpreter) => the_interpreter,
        Err(err) =>
            return HttpResponse::Ok().json(RemoteCallResponse::fail(err.to_string())),
    };
    match interpreter.evaluate(data.0.get_code()) {
        Ok(result) =>
            HttpResponse::Ok().json(RemoteCallResponse::success(result.to_json())),
        Err(err) =>
            HttpResponse::Ok().json(RemoteCallResponse::fail(err.to_string())),
    }
}

/// handler function for the system information route
// ex: http://localhost:8080/info
pub async fn handle_sys_info_get(_session: Session) -> impl Responder {
    HttpResponse::Ok().json(SystemInfoJs::new())
}

/// handler function for executing remote procedure calls
pub async fn handle_user_api(
    req: HttpRequest,
    body: web::Bytes,
    session: Session,
) -> impl Responder {
    // find the user-defined API method for this request
    let user_api_method = match find_user_api_method(&req) {
        Ok(api_method) => api_method,
        Err(e) => return HttpResponse::Ok().json(RemoteCallResponse::fail(e.to_string())),
    };

    // get the interpreter from the session
    let mut interpreter = match get_session(&session) {
        Ok(the_interpreter) => the_interpreter,
        Err(e) => return HttpResponse::Ok().json(RemoteCallResponse::fail(e.to_string())),
    };
    
    // parse and populate the query parameters
    for (name, value) in parse_query_string(&req) {
        interpreter.with_variable(name.as_str(), value)
    }
    
    // gather the request headers
    let headers_map = req.headers().iter()
        .map(|(k, v)| (k.to_string(), v.to_str().unwrap_or("").to_string()))
        .map(|(k, v)| (k, TypedValue::wrap_value(v.as_str())
            .unwrap_or_else(|_| StringValue(v))))
        .collect::<Vec<_>>();
    
    // populate the request headers
    interpreter.with_variable("http_headers", Structured(Soft(SoftStructure::from_tuples(headers_map))));

    // determine the user code to execute
    let user_code = match user_api_method.code.clone() {
        TypedValue::Function { params, body: code, .. } => {
            match user_api_method.method {
                APIMethods::POST => {
                    for param in params {
                        let value = match serde_json::from_slice::<Value>(&body) {
                            Ok(jvalue) => TypedValue::from_json(&jvalue),
                            Err(e) => return HttpResponse::Ok().json(RemoteCallResponse::fail(e.to_string())),
                        };
                        interpreter.with_variable(param.get_name(), value)
                    }
                }
                _ => {}
            }
            code.deref().clone()
        },
        other => Literal(other)
    };

    // execute the user code
    //println!("handle_user_api: user_code {user_code}");
    match interpreter.invoke(&user_code) {
        Ok(result) => {
            println!("handle_user_api: result {result}");
            HttpResponse::Ok().json(result.to_json())
        },
        Err(err) => HttpResponse::Ok().json(RemoteCallResponse::fail(err.to_string())),
    }
}

fn find_user_api_method(
    req: &HttpRequest,
) -> std::io::Result<UserAPIMethod> {
    // lookup the user-defined APIs
    let shared_data = get_shared_state(req)?;
    let user_apis = shared_data.apis.to_owned();

    // find the user-defined API for this request
    let user_api = match user_apis.iter().find(|api| api.path == req.path()) {
        Some(api) => api,
        None => return throw(Exact(format!("No user-defined API found for path: {}", req.path()))),
    };

    // find the user-defined API method for this request
    let method = req.method();
    let api_method = match user_api.methods.iter()
        .find(|api_meth| api_meth.method == APIMethods::WS 
            || Some(api_meth.method.clone()) == APIMethods::from_method(method)) {
        Some(user_api_meth) => user_api_meth,
        None => return throw(Exact(format!("No user-defined API found for path: {}", req.path()))),
    };
    
    Ok(api_method.clone())
}

pub async fn handle_system_websockets(
    req: HttpRequest, stream: web::Payload,
) -> impl Responder {
    let query_map = parse_query_string(&req);
    ws::start(WebSocketSystemServer::new(query_map), &req, stream)
}

pub async fn handle_user_websockets(
    req: HttpRequest, stream: web::Payload,
) -> impl Responder {
    // find the user-defined API method for this request
    match find_user_api_method(&req) {
        Ok(api_method) => {
            ws::start(WebSocketUserServer::new(api_method), &req, stream)
        }
        Err(e) => {
            println!("Failed to find user-defined API method: {}", e);
            Err(actix_web::error::ErrorBadRequest(e.to_string()))
        }
    }
}

pub fn start_http_server(port: u16) -> JoinHandle<()> {
    start_http_server_with_user_apis(port, vec![])
}

pub fn start_http_server_with_user_apis(
    port: u16,
    apis: Vec<UserAPI>,
) -> JoinHandle<()> {
    thread::spawn(move || {
        let server = actix_web::HttpServer::new(move || { 
            let mut web_cfg = web_routes!(SharedState::with_user_apis(apis.clone()));
            for api in apis.clone() {
                for api_method in api.methods {
                    let path = api.path.as_str();
                    web_cfg = match api_method.method {
                        APIMethods::DELETE => web_cfg.route(path, web::delete().to(handle_user_api)),
                        APIMethods::GET => web_cfg.route(path, web::get().to(handle_user_api)),
                        APIMethods::HEAD => web_cfg.route(path, web::head().to(handle_user_api)),
                        APIMethods::PATCH => web_cfg.route(path, web::patch().to(handle_user_api)),
                        APIMethods::POST => web_cfg.route(path, web::post().to(handle_user_api)),
                        APIMethods::PUT => web_cfg.route(path, web::put().to(handle_user_api)),
                        APIMethods::WS => web_cfg.service(web::resource(path).to(handle_user_websockets)),
                    };
                }
            }
            web_cfg
        })
            .bind(format!("{}:{}", "0.0.0.0", port))
            .expect(format!("Can't bind to port {port}").as_str())
            .run();
        Runtime::new()
            .expect("Failed to create a Runtime instance")
            .block_on(server)
            .expect(format!("Failed while blocking on port {port}").as_str());
    })
}

pub async fn start_async_http_server(port: u16) -> Server {
       actix_web::HttpServer::new(move || web_routes!(SharedState::new()))
            .bind(format!("{}:{}", "0.0.0.0", port))
            .expect(format!("Can't bind to port {port}").as_str())
            .run()
}

async fn append_row(
    req: HttpRequest,
    data: web::Json<Value>,
    path: web::Path<(String, String, String, usize)>,
) -> std::io::Result<usize> {
    let ns = Namespace::new(&path.0, &path.1, &path.2);
    let actor = get_shared_state(&req)?.actor.to_owned();
    let columns = get_columns!(actor, ns)?;
    append_row!(actor, ns, Row::from_json(&columns, &data.0))
}

async fn delete_row_by_id(
    req: HttpRequest,
    path: web::Path<(String, String, String, usize)>,
) -> std::io::Result<usize> {
    let (ns, id) = (Namespace::new(&path.0, &path.1, &path.2), path.3);
    let actor = get_shared_state(&req)?.actor.to_owned();
    delete_row!(actor, ns, id)
}

/// Retrieves the shared application state
fn get_shared_state(
    req: &HttpRequest
) -> std::io::Result<&web::Data<SharedState>> {
    match req.app_data::<web::Data<SharedState>>() {
        None => throw(Exact("No shared application state".into())),
        Some(shared_state) => Ok(shared_state)
    }
}

async fn get_range_by_id(
    req: HttpRequest,
    path: web::Path<(String, String, String, usize, usize)>,
) -> std::io::Result<(Vec<Column>, Vec<Row>)> {
    let (ns, a, b) = (Namespace::new(&path.0, &path.1, &path.2), path.3, path.4);
    let actor = get_shared_state(&req)?.actor.to_owned();
    read_range!(actor, ns, a, b)
}

async fn get_row_by_id(
    req: HttpRequest,
    path: web::Path<(String, String, String, usize)>,
) -> std::io::Result<(Vec<Column>, Option<Row>)> {
    let (ns, id) = (Namespace::new(&path.0, &path.1, &path.2), path.3);
    let actor = get_shared_state(&req)?.actor.to_owned();
    read_row!(actor, ns, id)
}

async fn get_row_metadata_by_id(
    req: HttpRequest,
    path: web::Path<(String, String, String, usize)>,
) -> std::io::Result<RowMetadata> {
    let (ns, id) = (Namespace::new(&path.0, &path.1, &path.2), path.3);
    let actor = get_shared_state(&req)?.actor.to_owned();
    read_row_metadata!(actor, ns, id)
}

async fn overwrite_row_by_id(
    req: HttpRequest,
    data: web::Json<Value>,
    path: web::Path<(String, String, String, usize)>,
) -> std::io::Result<usize> {
    let (ns, id) = (Namespace::new(&path.0, &path.1, &path.2), path.3);
    let actor = get_shared_state(&req)?.actor.to_owned();
    let columns = get_columns!(actor, ns)?;
    overwrite_row!(actor, ns, Row::from_json(&columns, &data.0).with_row_id(id))
}

async fn update_row_by_id(
    req: HttpRequest,
    data: web::Json<Value>,
    path: web::Path<(String, String, String, usize)>,
) -> std::io::Result<usize> {
    let (ns, id) = (Namespace::new(&path.0, &path.1, &path.2), path.3);
    let actor = get_shared_state(&req)?.actor.to_owned();
    let columns = get_columns!(actor, ns)?;
    update_row!(actor, ns, Row::from_json(&columns, &data.0).with_row_id(id))
}

fn ns_uri(database: &str, schema: &str, name: &str) -> String {
    format!("/{}/{}/{}", database, schema, name)
}

fn range_uri(database: &str, schema: &str, name: &str, a: usize, b: usize) -> String {
    format!("/{}/{}/{}/{}/{}", database, schema, name, a, b)
}

fn parse_query_string(req: &HttpRequest) -> Vec<(String, TypedValue)> {
    req.query_string().split('&')
        .filter_map(|pair| {
            let mut kv = pair.splitn(2, '=');
            let name = kv.next()?.to_string();
            let raw_value = urlencoding::decode(kv.next()?).ok()?.to_string();
            let value = TypedValue::wrap_value(raw_value.as_str())
                .unwrap_or_else(|_| StringValue(raw_value));
            Some((name, value))
        })
        .collect::<Vec<(String, TypedValue)>>()
}

fn row_uri(database: &str, schema: &str, name: &str, id: usize) -> String {
    format!("/{}/{}/{}/{}", database, schema, name, id)
}

/// Unit tests
#[cfg(test)]
mod tests {
    use super::*;
    use crate::data_types::DataType::{NumberType, RuntimeResolvedType, StringType};
    use crate::expression::Expression::{Identifier, TypeOf};
    use crate::number_kind::NumberKind::F64Kind;
    use crate::packages::VERSION;
    use crate::testdata::make_quote_parameters;
    use crate::typed_values::TypedValue;
    use crate::typed_values::TypedValue::{Function, StringValue};
    use actix_web::test;
    use futures_util::stream::SplitSink;
    use futures_util::{SinkExt, StreamExt};
    use serde_json::json;
    use std::thread;
    use tokio::net::TcpStream;
    use tokio::runtime::Runtime;
    use tokio_tungstenite::tungstenite::Message;
    use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};

    #[test]
    async fn test_convert_to_user_api_config() {
        let mut interpreter = Interpreter::new();
        let config = interpreter.evaluate(r#"
                {
                    "/api/orders" : {
                        "GET" : (order_id -> {
                           type_of(order_id)
                        })
                    }
                     "/api/contests" : {
                        "GET" : (order_id -> {
                           type_of(order_id)
                        })
                        "POST" : ((symbol: String, exchange: String, lastSale: f64) -> {
                            type_of(symbol)
                        })
                        "PUT" : (form -> {
                            type_of(form)
                        })
                    }
                }
            "#).unwrap();
        let cfg = convert_to_user_api_config(&config).unwrap();
        assert_eq!(cfg, vec![
            UserAPI {
                path: "/api/orders".into(),
                methods: vec![
                    UserAPIMethod {
                        method: APIMethods::GET,
                        code: Function {
                            params: vec![
                                Parameter::new("order_id", RuntimeResolvedType),
                            ],
                            body: TypeOf(Identifier("order_id".into()).into()).into(),
                            returns: RuntimeResolvedType
                        }
                    }
                ]
            },
            UserAPI {
                path: "/api/contests".into(),
                methods: vec![
                    UserAPIMethod {
                        method: APIMethods::GET,
                        code: Function {
                            params: vec![
                                Parameter::new("order_id", RuntimeResolvedType),
                            ],
                            body: TypeOf(Identifier("order_id".into()).into()).into(),
                            returns: RuntimeResolvedType
                        }
                    }, UserAPIMethod {
                        method: APIMethods::POST,
                        code: Function {
                            params: vec![
                                Parameter::new("symbol", StringType),
                                Parameter::new("exchange", StringType),
                                Parameter::new("lastSale", NumberType(F64Kind)),
                            ],
                            body: TypeOf(Identifier("symbol".into()).into()).into(),
                            returns: RuntimeResolvedType
                        }
                    },
                    UserAPIMethod {
                        method: APIMethods::PUT,
                        code: Function {
                            params: vec![
                                Parameter::new("form", RuntimeResolvedType),
                            ],
                            body: TypeOf(Identifier("form".into()).into()).into(),
                            returns: RuntimeResolvedType
                        }
                    }
                ]
            }
        ])
    }

    #[actix::test]
    async fn test_create_system_info() {
        assert_eq!(SystemInfoJs::new(), SystemInfoJs {
            title: "Oxide".into(),
            version: VERSION.into(),
        })
    }

    #[actix::test]
    async fn test_dataframe_config_lifecycle() {
        // set up the sessions
        let mut app = test::init_service(web_routes!(SharedState::new())).await;

        // send a POST with the config
        let (database, schema, name) = ("web", "rest", "config");
        let config = ObjectConfig::build_table(make_quote_parameters());
        let req = test::TestRequest::post().uri(&ns_uri(database, schema, name))
            .set_json(&json!(config))
            .to_request();
        let resp = test::call_service(&mut app, req).await;
        assert!(resp.status().is_success());

        // send a GET to retrieve the config
        let req = test::TestRequest::get().uri(&ns_uri(database, schema, name)).to_request();
        let resp = test::call_service(&mut app, req).await;
        assert!(resp.status().is_success());
        let body = String::from_utf8(test::read_body(resp).await.to_vec()).unwrap();
        assert_eq!(body, r#"{"TableConfig":{"columns":[{"name":"symbol","data_type":{"FixedSizeType":["StringType",8]},"default_value":"Null"},{"name":"exchange","data_type":{"FixedSizeType":["StringType",8]},"default_value":"Null"},{"name":"last_sale","data_type":{"NumberType":"F64Kind"},"default_value":"Null"}],"indices":[],"partitions":[]}}"#);

        // DELETE the config
        let req = test::TestRequest::delete().uri(&ns_uri(database, schema, name)).to_request();
        let resp = test::call_service(&mut app, req).await;
        assert!(resp.status().is_success());
    }

    #[actix::test]
    async fn test_handle_rpc() {
        // set up the sessions
        let mut app = test::init_service(web_routes!(SharedState::new())).await;

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
        // set up the sessions
        let mut app = test::init_service(web_routes!(SharedState::new())).await;
        let (database, schema, name) = ("web", "dataframe", "lifecycle");
        let config = ObjectConfig::build_table(make_quote_parameters());

        // POST to create a new table
        let req = test::TestRequest::post()
            .uri(&ns_uri(database, schema, name))
            .set_json(&json!(config)).to_request();
        let resp = test::call_service(&mut app, req).await;
        assert!(resp.status().is_success());

        // POST to append a new stock quote
        let req = test::TestRequest::post().uri(&row_uri(database, schema, name, 0))
            .set_json(&json!({"symbol":"ATOM","exchange":"NYSE","last_sale":24.17}))
            .to_request();
        let resp = test::call_service(&mut app, req).await;
        assert!(resp.status().is_success());

        // POST to append a second stock quote
        let req = test::TestRequest::post().uri(&row_uri(database, schema, name, 0))
            .set_json(&json!({"symbol":"BABY","exchange":"NYSE","last_sale":13.66}))
            .to_request();
        let resp = test::call_service(&mut app, req).await;
        assert!(resp.status().is_success());

        // GET one of the previously POSTed stock quotes
        let req = test::TestRequest::get().uri(&row_uri(database, schema, name, 0)).to_request();
        let resp = test::call_service(&mut app, req).await;
        assert!(resp.status().is_success());
        let body = String::from_utf8(test::read_body(resp).await.to_vec()).unwrap();
        let json_value: Value = serde_json::from_str(body.as_str()).unwrap();
        assert_eq!(json_value, json!({"exchange":"NYSE","last_sale":24.17,"symbol":"ATOM"}));

        // PATCH one of the previously POSTed stock quotes with a new price
        let req = test::TestRequest::patch().uri(&row_uri(database, schema, name, 1))
            .set_json(&json!({"id":1,"last_sale":13.33})).to_request();
        let resp = test::call_service(&mut app, req).await;
        assert!(resp.status().is_success());

        // GET all stock quotes within a range
        let req = test::TestRequest::get().uri(&range_uri(database, schema, name, 0, 2)).to_request();
        let resp = test::call_service(&mut app, req).await;
        assert!(resp.status().is_success());
        let body = String::from_utf8(test::read_body(resp).await.to_vec()).unwrap();
        let json_value: Value = serde_json::from_str(body.as_str()).unwrap();
        assert_eq!(json_value, json!([
            {"symbol":"ATOM","exchange":"NYSE","last_sale":24.17},
            {"symbol":"BABY","exchange":"NYSE","last_sale":13.33}
        ]));

        // PUT to overwrite the row
        let req = test::TestRequest::put().uri(&row_uri(database, schema, name, 0))
            .set_json(&json!({"symbol":"CRY","exchange":"NYSE","last_sale":88.11}))
            .to_request();
        let resp = test::call_service(&mut app, req).await;
        assert!(resp.status().is_success());

        // GET the stock quote from the previous PUT
        let req = test::TestRequest::get().uri(&row_uri(database, schema, name, 0)).to_request();
        let resp = test::call_service(&mut app, req).await;
        assert!(resp.status().is_success());
        let body = String::from_utf8(test::read_body(resp).await.to_vec()).unwrap();
        let json_value: Value = serde_json::from_str(body.as_str()).unwrap();
        assert_eq!(json_value, json!({"last_sale":88.11,"symbol":"CRY","exchange":"NYSE"}));

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
    async fn test_handle_system_info() {
        // set up the sessions
        let mut app = test::init_service(web_routes!(SharedState::new())).await;

        // send a GET request for system info
        let req = test::TestRequest::get().uri("/info").to_request();

        // process the request and verify the response
        let resp = test::call_service(&mut app, req).await;
        assert!(resp.status().is_success());
        let body = String::from_utf8(test::read_body(resp).await.to_vec()).unwrap();
        assert_eq!(body, format!( r#"{{"title":"Oxide","version":"{}"}}"#, VERSION));
    }
}