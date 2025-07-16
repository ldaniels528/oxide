#![warn(dead_code)]
////////////////////////////////////////////////////////////////////
// WebEngine - HTTP/Websocket services
////////////////////////////////////////////////////////////////////

use crate::byte_code_compiler::ByteCodeCompiler;
use crate::compiler::Compiler;
use crate::data_types::DataType::*;
use crate::errors::Errors::*;
use crate::errors::TypeMismatchErrors::*;
use crate::errors::{throw, SyntaxErrors};
use crate::expression::Expression::*;
use crate::expression::{Expression, HttpMethodCalls};
use crate::interpreter::Interpreter;
use crate::machine::Machine;
use crate::numbers::Numbers::*;
use crate::packages::Package;
use crate::parameter::Parameter;
use crate::row_collection::RowCollection;
use crate::sequences::Sequence;
use crate::server_engine::UserAPIMethod;
use crate::structures::Structures::Soft;
use crate::structures::*;
use crate::typed_values::TypedValue;
use crate::typed_values::TypedValue::*;
use actix::{Actor, StreamHandler};
use actix_web_actors::ws;
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use isahc::{Body, ReadResponseExt, Request, RequestExt, Response};
use itertools::Itertools;
use log::{error, warn};
use once_cell::sync::Lazy;
use shared_lib::cnv_error;
use std::collections::HashMap;
use std::convert::Into;
use std::ops::Deref;
use std::sync::{Arc, RwLock};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};
use uuid::Uuid;

/// Manages and executes HTTP/Websocket connections
pub struct WebEngine;

impl WebEngine {

    /// Evaluates a query language expression
    pub fn evaluate(
        ms: &Machine,
        expression: &Expression,
    ) -> std::io::Result<(Machine, TypedValue)> {
        match expression {
            HTTP(method_call) => Self::exec_http(ms, method_call),
            _ => throw(SyntaxError(SyntaxErrors::IllegalExpression(expression.to_code()))),
        }
    }

    fn exec_http(
        ms: &Machine,
        call: &HttpMethodCalls
    ) -> std::io::Result<(Machine, TypedValue)> {
        // fn create_form(structure: Box<dyn Structure>) -> Form {
        //     structure.to_name_values().iter().fold(Form::new(), |form, (name, value)| {
        //         form.part(name.to_owned(), Part::text(value.unwrap_value()))
        //     })
        // }

        fn extract_string_tuples(value: TypedValue) -> std::io::Result<Vec<(String, String)>> {
            extract_value_tuples(value)
                .map(|values| values.iter()
                    .map(|(k, v)| (k.to_string(), v.unwrap_value()))
                    .collect())
        }

        fn extract_value_tuples(value: TypedValue) -> std::io::Result<Vec<(String, TypedValue)>> {
            match value {
                Structured(structure) => Ok(structure.to_name_values()),
                z => throw(TypeMismatch(UnsupportedType(StructureType(vec![]), z.get_type()))),
            }
        }

        // evaluate the URL or configuration object
        match ms.evaluate(&call.get_url_or_config())? {
            // GET http://localhost:9000/quotes/AAPL/NYSE
            (ms, StringValue(url)) =>
                Self::exec_http_request(&ms, call, url.to_string(), None, Vec::new()),
            // POST {
            //     url: http://localhost:8080/machine/append/stocks
            //     body: stocks
            //     headers: { "Content-Type": "application/json" }
            // }
            (ms, Structured(config)) => {
                let url = config.get("url");
                let maybe_body = config.get_opt("body")
                    .map(|body| body.unwrap_value());
                let headers = match config.get_opt("headers") {
                    None => Vec::new(),
                    Some(headers) => extract_string_tuples(headers)?
                };
                Self::exec_http_request(&ms, call, url.unwrap_value(), maybe_body, headers)
            }
            // unsupported expression
            (_ms, other) =>
                throw(TypeMismatch(StructExpected(other.to_code())))
        }
    }

    fn exec_http_request(
        ms: &Machine,
        method_call: &HttpMethodCalls,
        url: String,
        body: Option<String>,
        headers: Vec<(String, String)>,
    ) -> std::io::Result<(Machine, TypedValue)> {
        let mut builder = match method_call {
            HttpMethodCalls::CONNECT(..) => Request::connect(url),
            HttpMethodCalls::DELETE(..) => Request::delete(url),
            HttpMethodCalls::GET(..) => Request::get(url),
            HttpMethodCalls::HEAD(..) => Request::head(url),
            HttpMethodCalls::OPTIONS(..) => Request::options(url),
            HttpMethodCalls::PATCH(..) => Request::patch(url),
            HttpMethodCalls::POST(..) => Request::post(url),
            HttpMethodCalls::PUT(..) => Request::put(url),
            HttpMethodCalls::TRACE(..) => Request::trace(url),
        };

        // enrich and submit the request
        for (key, value) in headers {
            builder = builder.header(&key, &value);
        }
        let response = if let Some(body) = body {
            builder = builder.header("Content-Type", "application/json");
            builder
                .body(body)
                .map_err(|e| cnv_error!(e))?
                .send()
                .map_err(|e| cnv_error!(e))?
        } else {
            builder = builder.header("Content-Type", "application/json");
            builder
                .body(())
                .map_err(|e| cnv_error!(e))?
                .send()
                .map_err(|e| cnv_error!(e))?
        };
        Self::exec_http_response(ms, response, method_call.is_header_only())
            .map(|result| (ms.clone(), result))
    }

    /// Converts a [Response] to a [TypedValue]
    fn exec_http_response(
        ms: &Machine,
        mut response: Response<Body>,
        is_header_only: bool,
    ) -> std::io::Result<TypedValue> {
        if response.status().is_success() {
            if is_header_only {
                let mut key_values = vec![];
                for (h_key, h_val) in response.headers().iter() {
                    let value = match h_val.to_str() {
                        Ok(s) => StringValue(s.into()),
                        Err(e) => {
                            eprintln!("exec_http_response: {}", e.to_string());
                            ErrorValue(Exact(e.to_string()))
                        }
                    };
                    key_values.push((h_key.to_string(), value))
                }
                Ok(Structured(Soft(SoftStructure::ordered(key_values))))
            } else {
                let body = response.text().map_err(|e| cnv_error!(e))?;
                match Compiler::build(body.as_str()) {
                    Ok(expr) => {
                        Ok(match ms.evaluate(&expr) {
                            Ok((_, Undefined)) => Structured(Soft(SoftStructure::empty())),
                            Ok((_, value)) => value,
                            Err(_) => StringValue(body)
                        })
                    }
                    _ => Ok(StringValue(body))
                }
            }
        } else {
            throw(Exact(format!("Request failed with status: {}", response.status())))
        }
    }

}

/// Oxide WebSocket Client
pub struct WebSocketClient {
    read: SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
    write: SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
}

impl WebSocketClient {
    /// Starts the websocket client
    pub async fn connect(host: &str, port: u16, path: &str) -> std::io::Result<WebSocketClient> {
        let (mut ws_stream, _response) =
            connect_async(format!("ws://{host}:{port}{path}")).await
                .map_err(|e| cnv_error!(e))?;
        let (write, read) = ws_stream.split();
        Ok(Self { read, write })
    }

    async fn close(&mut self) -> std::io::Result<()> {
        self.write.close().await.map_err(|e| cnv_error!(e))
    }

    pub async fn evaluate(&mut self, script: &str) -> std::io::Result<TypedValue> {
        self.send_text_message(script).await?;
        self.read_next().await
    }

    pub async fn invoke(&mut self, expr: &Expression) -> std::io::Result<TypedValue> {
        self.send_binary_message(ByteCodeCompiler::encode(expr)?).await?;
        self.read_next().await
    }

    pub async fn with_variable(&mut self, name: &str, value: TypedValue) -> std::io::Result<TypedValue> {
        self.send_text_message(format!("{name} := {}", value.to_code()).as_str()).await?;
        self.read_next().await
    }

    async fn read_next(&mut self) -> std::io::Result<TypedValue> {
        match self.read.next().await {
            None => Ok(Undefined),
            Some(Ok(message)) =>
                Ok(match message {
                    Message::Binary(bytes) => ByteCodeCompiler::decode_value(&bytes),
                    Message::Text(text) => StringValue(text.to_string()),
                    msg => ErrorValue(TypeMismatch(UnexpectedResult(msg.to_string())))
                }),
            Some(Err(err)) => throw(Exact(err.to_string()))
        }
    }

    async fn send_binary_message(&mut self, message: Vec<u8>) -> std::io::Result<()> {
        self.write.send(Message::Binary(message)).await
            .map_err(|e| cnv_error!(e))
    }

    async fn send_text_message(&mut self, message: &str) -> std::io::Result<()> {
        self.write.send(Message::Text(message.to_string())).await
            .map_err(|e| cnv_error!(e))
    }
}

/// System WebSocket Server
pub struct WebSocketSystemServer {
    interpreter: Interpreter,
}

impl WebSocketSystemServer {
    pub fn new(query_map: Vec<(String, TypedValue)>) -> Self {
        let mut interpreter = Interpreter::new();
        for (name, value) in query_map {
            interpreter.with_variable(name.as_str(), value);
        }
        Self { interpreter }
    }
}

impl Actor for WebSocketSystemServer {
    type Context = ws::WebsocketContext<Self>;
}

impl StreamHandler<Result<ws::Message, ws::ProtocolError>> for WebSocketSystemServer {
    fn handle(&mut self, msg: Result<ws::Message, ws::ProtocolError>, ctx: &mut Self::Context) {
        match msg {
            Err(err) => transmit(ctx, &ErrorValue(Exact(err.to_string()))),
            Ok(ws::Message::Binary(bytes)) => {
                let model = ByteCodeCompiler::decode(&bytes.into());
                let value = self.interpreter.invoke(&model)
                    .unwrap_or_else(|err| ErrorValue(Exact(err.to_string())));
                transmit(ctx, &value)
            }
            Ok(ws::Message::Close(reason)) => {
                let message = reason.and_then(|r| r.description).unwrap_or_default();
                let value = if message.is_empty() { Boolean(true) } else { ErrorValue(Exact(message)) };
                transmit(ctx, &value)
            }
            Ok(ws::Message::Ping(msg)) => ctx.pong(&msg),
            Ok(ws::Message::Pong(msg)) => ctx.ping(&msg),
            Ok(ws::Message::Text(text)) => {
                let value = self.interpreter.evaluate(text.trim())
                    .unwrap_or_else(|err| ErrorValue(Exact(err.to_string())));
                transmit(ctx, &value)
            }
            Ok(other) => {
                warn!("Unhandled WebSocket message type ({:?}).", other);
            }
        }
    }
}

/// User WebSocket Server
pub struct WebSocketUserServer {
    interpreter: Interpreter,
    user_api_method: UserAPIMethod,
}

impl WebSocketUserServer {
    pub fn new(
        user_api_method: UserAPIMethod,
    ) -> Self {
        let mut interpreter = Interpreter::new();
        Self { interpreter, user_api_method }
    }

    fn get_handler_function(
        user_api_method: &UserAPIMethod,
        fx_name: &str
    ) -> std::io::Result<Expression> {
        match &user_api_method.code {
            Function { body, .. } => 
                match body.deref() {
                    StructureExpression(functions) =>
                        match functions.iter().find(|(name, fx)| name == fx_name) {
                            None => throw(FunctionNotFound(fx_name.to_string())),
                            Some((_name, fx_expr)) => Ok(fx_expr.clone())
                        }
                    z => throw(TypeMismatch(FunctionExpected(z.to_code())))
                }
            z => throw(TypeMismatch(StructExpected(z.to_code())))
        }
    }

    fn handle_event(
        &mut self,
        fx_name: &str,
        message: TypedValue,
        fail_on_error: bool,
    ) -> std::io::Result<TypedValue> {
        match Self::get_handler_function(&self.user_api_method, fx_name) {
            Ok(fx_expr) =>
                Ok(match self.interpreter.invoke(&fx_expr)? {
                    Function { params, body, .. } => {
                        for (n, param) in params.iter().enumerate() {
                            let arg = match n {
                                // connection object
                                0 => Structured(Soft(SoftStructure::new(&vec![
                                    ("id", Number(I64Value(0)))
                                ]))),
                                // message
                                1 => message.clone(),
                                _ => Undefined,
                            };
                            self.interpreter.with_variable(param.get_name(), arg)
                        }
                        self.interpreter.invoke(body.deref())?
                    }
                    other => other,
                }),
            Err(err) => if fail_on_error { throw(Exact(err.to_string())) } else { Ok(Undefined) }
        }
    }

    fn on_close(&mut self, message: TypedValue) -> std::io::Result<TypedValue> {
        self.handle_event("on_close", message, false)
    }

    fn on_message(&mut self, message: TypedValue) -> std::io::Result<TypedValue> {
        self.handle_event("on_message", message, true)
    }

    fn on_open(&mut self, message: TypedValue) -> std::io::Result<TypedValue> {
        self.handle_event("on_open", message, false)
    }
}

impl Actor for WebSocketUserServer {
    type Context = ws::WebsocketContext<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        self.on_open(StringValue("Connected".into())).ok();
    }
}

impl StreamHandler<Result<ws::Message, ws::ProtocolError>> for WebSocketUserServer {
    fn handle(&mut self, msg: Result<ws::Message, ws::ProtocolError>, ctx: &mut Self::Context) {
        match msg {
            Err(err) => transmit(ctx, &ErrorValue(Exact(err.to_string()))),
            Ok(ws::Message::Binary(bytes)) => {
                let value = self.on_message(ByteStringValue(bytes.into()))
                    .unwrap_or_else(|err| ErrorValue(Exact(err.to_string())));
                transmit(ctx, &value)
            }
            Ok(ws::Message::Close(reason)) => {
                let message = reason.and_then(|r| r.description).unwrap_or_default();
                let value = self.on_close(StringValue(message.into()))
                    .unwrap_or_else(|err| ErrorValue(Exact(err.to_string())));
                transmit(ctx, &value)
            }
            Ok(ws::Message::Ping(msg)) => ctx.pong(&msg),
            Ok(ws::Message::Pong(msg)) => ctx.ping(&msg),
            Ok(ws::Message::Text(message)) => {
                let value = self.on_message(StringValue(message.into()))
                    .unwrap_or_else(|err| ErrorValue(Exact(err.to_string())));
                transmit(ctx, &value)
            }
            Ok(other) => {
                warn!("Unhandled WebSocket message type ({:?}).", other);
            }
        }
    }
}

/// transmits the [TypedValue] to the client
fn transmit<A>(ctx: &mut ws::WebsocketContext<A>, value: &TypedValue) 
    where A: Actor<Context = ws::WebsocketContext<A>> + StreamHandler<Result<ws::Message, ws::ProtocolError>>,
{
    let bytes = ByteCodeCompiler::encode_value(&value)
        .unwrap_or_else(|err| {
            error!("ERROR: {}", err);
            vec![]
        });
    ctx.binary(bytes);
}

/// Websocket client
pub mod ws_commander {
    use super::*;
    use crate::data_types::DataType::*;
    use crate::errors::Errors::*;
    use crate::errors::TypeMismatchErrors::*;
    use crate::expression::Expression::*;
    use crate::numbers::Numbers::*;
    use crate::typed_values::TypedValue;
    use crate::typed_values::TypedValue::*;
    use once_cell::sync::Lazy;
    use std::collections::HashMap;
    use std::sync::{Arc, RwLock};
    use uuid::Uuid;

    pub static WEBSOCKET_REGISTRY: Lazy<Arc<RwLock<HashMap<u128, WebSocketClient>>>> =
        Lazy::new(|| Arc::new(RwLock::new(HashMap::new())));

    pub async fn close(id: u128) -> std::io::Result<TypedValue> {
        let mut map = WEBSOCKET_REGISTRY.write().unwrap();//.map_err(|e| cnv_error!(e))?;
        if let Some(client) = map.get_mut(&id) {
            client.close().await?;
            client.read_next().await
        } else {
            Ok(Undefined)
        }
    }
    
    pub async fn connect_ws(host: &str, port: u16, path: &str) -> std::io::Result<TypedValue> {
        let client = WebSocketClient::connect(host, port, path).await?;
        let id = Uuid::new_v4().as_u128();
        WEBSOCKET_REGISTRY.write().unwrap().insert(id, client);
        Ok(UUIDValue(id))
    }

    pub async fn send_binary_command(id: u128, msg: Vec<u8>) -> std::io::Result<TypedValue> {
        let mut map = WEBSOCKET_REGISTRY.write().unwrap();//.map_err(|e| cnv_error!(e))?;
        if let Some(client) = map.get_mut(&id) {
            client.send_binary_message(msg).await?;
            client.read_next().await
        } else {
            Ok(Undefined)
        }
    }

    pub async fn send_text_command(id: u128, msg: &str) -> std::io::Result<TypedValue> {
        let mut map = WEBSOCKET_REGISTRY.write().unwrap();//.map_err(|e| cnv_error!(e))?;
        if let Some(client) = map.get_mut(&id) {
            client.send_text_message(msg).await?;
            client.read_next().await
        } else {
            Ok(Undefined)
        }
    }
}

/// Unit tests
#[cfg(test)]
mod tests {
    /// HTTP tests
    #[cfg(test)]
    mod http_tests {
        use crate::data_types::DataType::{NumberType, RuntimeResolvedType, StringType};
        use crate::expression::Expression::{Identifier, TypeOf};
        use crate::interpreter::Interpreter;
        use crate::number_kind::NumberKind::F64Kind;
        use crate::numbers::Numbers::I64Value;
        use crate::packages::WwwPkg;
        use crate::parameter::Parameter;
        use crate::server_engine::{APIMethods, UserAPI, UserAPIMethod};
        use crate::structures::SoftStructure;
        use crate::structures::Structures::Soft;
        use crate::testdata::{verify_exact_code_with, verify_exact_table_with};
        use crate::typed_values::TypedValue::{Boolean, Function, Number, Structured};
        use serde_json::json;

        #[test]
        fn test_http_serve_and_query() {
            let mut interpreter = Interpreter::new();
            interpreter = verify_exact_code_with(interpreter, r#"
                http::serve(8282)
                stocks = nsd::save(
                    "web_engine.www_sql.stocks",
                    |--------------------------------|
                    | symbol | exchange  | last_sale |
                    |--------------------------------|
                    | GIF    | NYSE      | 11.75     |
                    | TRX    | NASDAQ    | 32.96     |
                    | RLP    | NYSE      | 23.66     |
                    | GTO    | NASDAQ    | 51.23     |
                    | BST    | NASDAQ    | 214.88    |
                    | SHMN   | OTCBB     | 5.02      |
                    | XCD    | OTCBB     | 1.37      |
                    | DRMQ   | OTHER_OTC | 0.02      |
                    | JTRQ   | OTHER_OTC | 0.0001    |
                    |--------------------------------|
                )
            "#, "true");

            let mut interpreter = Interpreter::new();
            interpreter = verify_exact_table_with(interpreter, r#"
                (GET http://localhost:8282/web_engine/www_sql/stocks/0/9)
                    where exchange is "NYSE"
            "#, vec![
                "|------------------------------------|",
                "| id | exchange | last_sale | symbol |",
                "|------------------------------------|",
                "| 0  | NYSE     | 11.75     | GIF    |",
                "| 2  | NYSE     | 23.66     | RLP    |",
                "|------------------------------------|"]);

            let mut interpreter = Interpreter::new();
            interpreter = verify_exact_table_with(interpreter, r#"
                use agg
                select
                    exchange,
                    min_sale: min(last_sale),
                    max_sale: max(last_sale),
                    avg_sale: avg(last_sale),
                    total_sale: sum(last_sale),
                    qty: count(last_sale)
                from
                    (GET http://localhost:8282/web_engine/www_sql/stocks/0/9)
                group_by exchange
                having total_sale > 1.0
                order_by total_sale::asc
            "#, vec![
                "|-------------------------------------------------------------------|",
                "| id | exchange | min_sale | max_sale | avg_sale | total_sale | qty |",
                "|-------------------------------------------------------------------|",
                "| 0  | OTCBB    | 1.37     | 5.02     | 3.195    | 6.39       | 2   |",
                "| 1  | NYSE     | 11.75    | 23.66    | 17.705   | 35.41      | 2   |",
                "| 2  | NASDAQ   | 32.96    | 214.88   | 99.69    | 299.07     | 3   |",
                "|-------------------------------------------------------------------|"]);
        }

        #[test]
        fn test_http_serve_workflow() {
            let mut interpreter = Interpreter::new();
            let result = interpreter.evaluate(r#"
                stocks = nsd::save(
                    "web_engine.www.stocks",
                    Table(symbol: String(8), exchange: String(8), last_sale: f64)::new
                )
            "#).unwrap();
            assert_eq!(result, Boolean(true));

            // set up a listener on port 8838
            let result = interpreter.evaluate(r#"
                http::serve(8838)
            "#).unwrap();
            assert_eq!(result, Boolean(true));

            // append a new row
            let row_id = interpreter.evaluate(r#"
                POST {
                    url: http://localhost:8838/web_engine/www/stocks/0
                    body: { symbol: "ABC", exchange: "AMEX", last_sale: 11.77 }
                }
            "#).unwrap();
            assert!(matches!(row_id, Number(I64Value(..))));

            // fetch the previously created row
            let row = interpreter.evaluate(format!(r#"
                GET http://localhost:8838/web_engine/www/stocks/{row_id}
            "#).as_str()).unwrap();
            assert_eq!(
                row.to_json(),
                json!({"exchange":"AMEX","symbol":"ABC","last_sale":11.77})
            );

            // replace the previously created row
            let result = interpreter.evaluate(format!(r#"
                PUT {{
                    url: http://localhost:8838/web_engine/www/stocks/{row_id}
                    body: {{ symbol: "ABC", exchange: "AMEX", last_sale: 11.79 }}
                }}
            "#).as_str()).unwrap();
            assert_eq!(result, Number(I64Value(1)));

            // re-fetch the previously updated row
            let row = interpreter.evaluate(format!(r#"
                GET http://localhost:8838/web_engine/www/stocks/{row_id}
            "#).as_str()).unwrap();
            assert_eq!(
                row.to_json(),
                json!({"symbol":"ABC","exchange":"AMEX","last_sale":11.79})
            );

            // update the previously created row
            let result = interpreter.evaluate(format!(r#"
                PATCH {{
                    url: http://localhost:8838/web_engine/www/stocks/{row_id}
                    body: {{ last_sale: 11.81 }}
                }}
            "#).as_str()).unwrap();
            assert_eq!(result, Number(I64Value(1)));

            // re-fetch the previously updated row
            let row = interpreter.evaluate(format!(r#"
                GET http://localhost:8838/web_engine/www/stocks/{row_id}
            "#).as_str()).unwrap();
            assert_eq!(
                row.to_json(),
                json!({"last_sale":11.81,"symbol":"ABC","exchange":"AMEX"})
            );

            // fetch the headers for the previously updated row
            let result = interpreter.evaluate(format!(r#"
                HEAD http://localhost:8838/web_engine/www/stocks/{row_id}
            "#).as_str()).unwrap();
            println!("HEAD: {}", result.to_string());
            assert!(matches!(result, Structured(Soft(..))));

            // delete the previously updated row
            let result = interpreter.evaluate(format!(r#"
                DELETE http://localhost:8838/web_engine/www/stocks/{row_id}
            "#).as_str()).unwrap();
            assert_eq!(result, Number(I64Value(1)));

            // verify the deleted row is empty
            let row = interpreter.evaluate(format!(r#"
                GET http://localhost:8838/web_engine/www/stocks/{row_id}
            "#).as_str()).unwrap();
            assert_eq!(row, Structured(Soft(SoftStructure::empty())));
        }

        #[test]
        fn test_http_serve_workflow_script() {
            let mut interpreter = Interpreter::new();
            let result = interpreter.evaluate(r#"
                // setup a listener on port 8848
                http::serve(8848)

                // create the table
                nsd::save(
                    "web_engine.http_workflow.stocks",
                    Table(symbol: String(8), exchange: String(8), last_sale: f64)::new
                )
                row_id = POST {
                    url: http://localhost:8848/web_engine/http_workflow/stocks/0
                    body: { symbol: "ABC", exchange: "AMEX", last_sale: 11.77 }
                }
                assert(row_id matches 0)
                GET http://localhost:8848/web_engine/http_workflow/stocks/0
            "#).unwrap();
            assert_eq!(
                result.to_json(),
                json!({"exchange": "AMEX", "last_sale": 11.77, "symbol": "ABC"})
            );
        }

        #[test]
        fn test_http_serve_user_api_get() {
            let mut interpreter = Interpreter::new();
            interpreter = verify_exact_code_with(interpreter, r#"
                http::serve(8289, {
                    "/api/stocks" : {
                        "GET" : (ticker -> {
                            let stocks = nsd::load("packages.http_serve_api_get.stocks")
                            stocks where symbol is ticker
                        })
                    }
                })
            "#, "true");

            let mut interpreter = Interpreter::new();
            interpreter = verify_exact_code_with(interpreter, r#"
                let stocks = nsd::save("packages.http_serve_api_get.stocks",
                    |--------------------------------|
                    | symbol | exchange  | last_sale |
                    |--------------------------------|
                    | GIF    | NYSE      | 11.75     |
                    | TRX    | NASDAQ    | 32.96     |
                    | SHMN   | OTCBB     | 5.02      |
                    | XCD    | OTCBB     | 1.37      |
                    | DRMQ   | OTHER_OTC | 0.02      |
                    | JTRQ   | OTHER_OTC | 0.0001    |
                    |--------------------------------|
                )
            "#, "true");

            let mut interpreter = Interpreter::new();
            interpreter = verify_exact_code_with(interpreter, r#"
                GET http://localhost:8289/api/stocks?ticker=SHMN
            "#, r#"[{exchange: "OTCBB", last_sale: 5.02, symbol: "SHMN"}]"#);
        }

        #[test]
        fn test_http_serve_user_api_post() {
            let mut interpreter = Interpreter::new();
            interpreter = verify_exact_code_with(interpreter, r#"
                http::serve(8219, {
                    "/api/stocks" : {
                        "GET" : (ticker -> {
                            let stocks = nsd::load("packages.http_serve_api_post.stocks")
                            stocks where symbol is ticker
                        })
                        "POST" : (quote -> {
                            let stocks = nsd::load("packages.http_serve_api_post.stocks")
                            quote ~> stocks
                        })
                    }
                })
            "#, "true");

            let mut interpreter = Interpreter::new();
            interpreter = verify_exact_code_with(interpreter, r#"
                let stocks = nsd::save("packages.http_serve_api_post.stocks",
                    |--------------------------------|
                    | symbol | exchange  | last_sale |
                    |--------------------------------|
                    | GIF    | NYSE      | 11.75     |
                    |--------------------------------|
                )
            "#, "true");

            let mut interpreter = Interpreter::new();
            interpreter = verify_exact_code_with(interpreter, r#"
                POST {
                    url: http://localhost:8219/api/stocks
                    body: { symbol: "ABC", exchange: "AMEX", last_sale: 11.79 }
                }
            "#, "1");

            let mut interpreter = Interpreter::new();
            interpreter = verify_exact_code_with(interpreter, r#"
                GET http://localhost:8219/api/stocks?ticker=ABC
            "#, r#"[{exchange: "AMEX", last_sale: 11.79, symbol: "ABC"}]"#);
        }
    }

    /// Unit tests
    #[cfg(test)]
    mod ws_tests {
        use crate::interpreter::Interpreter;
        use crate::numbers::Numbers::I64Value;
        use crate::testdata::{make_lines_from_table, start_test_server, verify_exact_code_with, verify_exact_table_with};
        use crate::typed_values::TypedValue::{Number, StringValue, UUIDValue};
        use crate::web_engine::{ws_commander, WebSocketClient};

        #[actix::test]
        async fn test_system_websockets_conversational() {
            let port = 8010;
            start_test_server(port);

            let mut wsc = WebSocketClient::connect("0.0.0.0", port, "/ws").await.unwrap();
            wsc.evaluate("let a = [0, 1, 3, 5]").await.unwrap();
            let value = wsc.evaluate("a[2]").await.unwrap();
            assert_eq!(value, Number(I64Value(3)))
        }

        #[actix::test]
        async fn test_system_websockets_script() {
            let port = 8011;
            start_test_server(port);

            let mut wsc = WebSocketClient::connect("0.0.0.0", port, "/ws").await.unwrap();
            let value = wsc.evaluate(r#"
                let stocks = nsd::save(
                    "ws.script.stocks",
                    Table(symbol: String(8), exchange: String(8), last_sale: f64)::new
                )
                [{ symbol: "ABC", exchange: "AMEX", last_sale: 11.77 },
                 { symbol: "UNO", exchange: "OTC", last_sale: 0.2456 },
                 { symbol: "BIZ", exchange: "NYSE", last_sale: 23.66 },
                 { symbol: "GOTO", exchange: "OTC", last_sale: 0.1428 },
                 { symbol: "BOOM", exchange: "NASDAQ", last_sale: 0.0872 }] ~> stocks
                stocks
        "#).await.unwrap();
            assert_eq!(make_lines_from_table(value), vec![
                "|-------------------------------|",
                "| symbol | exchange | last_sale |",
                "|-------------------------------|",
                "| ABC    | AMEX     | 11.77     |",
                "| UNO    | OTC      | 0.2456    |",
                "| BIZ    | NYSE     | 23.66     |",
                "| GOTO   | OTC      | 0.1428    |",
                "| BOOM   | NASDAQ   | 0.0872    |",
                "|-------------------------------|"])
        }

        #[actix::test]
        async fn test_user_websockets_script() {
            let port: u16 = 8012;
            let mut interpreter = Interpreter::new();
            interpreter.with_variable("port", Number(I64Value(port as i64)));
            interpreter = verify_exact_code_with(interpreter, r#"
                http::serve(port, {
                    "/api/ws" : {
                        "WS" : (() -> {
                            "on_open" : ((conn, message) -> message)
                            "on_message" : ((conn, message) -> {
                                let stocks = nsd::load("web_engine.user_websockets_script.stocks")
                                stocks where symbol is message::to_string
                            })
                            "on_close" : ((conn, message) -> message)
                        })
                    }
                })
            "#, "true");

            let mut interpreter = Interpreter::new();
            interpreter = verify_exact_code_with(interpreter, r#"
                let stocks = nsd::save("web_engine.user_websockets_script.stocks",
                    |--------------------------------|
                    | symbol | exchange  | last_sale |
                    |--------------------------------|
                    | GIF    | NYSE      | 11.75     |
                    | TRX    | NASDAQ    | 32.96     |
                    | SHMN   | OTCBB     | 5.02      |
                    | XCD    | OTCBB     | 1.37      |
                    | DRMQ   | OTHER_OTC | 0.02      |
                    | JTRQ   | OTHER_OTC | 0.0001    |
                    |--------------------------------|
                )
            "#, "true");

            // connect the web socket
            let mut wsc = WebSocketClient::connect("0.0.0.0", port, "/api/ws").await.unwrap();

            // send a text message
            wsc.send_text_message("JTRQ").await.unwrap();

            let value = wsc.read_next().await.unwrap();
            assert_eq!(make_lines_from_table(value), vec![
                "|--------------------------------|",
                "| symbol | exchange  | last_sale |",
                "|--------------------------------|",
                "| JTRQ   | OTHER_OTC | 0.0001    |",
                "|--------------------------------|"]);

            // send a binary message
            wsc.send_binary_message(b"TRX".to_vec()).await.unwrap();

            let value = wsc.read_next().await.unwrap();
            assert_eq!(make_lines_from_table(value), vec![
                "|-------------------------------|", 
                "| symbol | exchange | last_sale |", 
                "|-------------------------------|", 
                "| TRX    | NASDAQ   | 32.96     |", 
                "|-------------------------------|"]);

            // close the connection
            let outcome = wsc.close().await.unwrap();
            assert_eq!(wsc.read_next().await.unwrap(), StringValue(String::new()));
        }

        #[actix::test]
        async fn test_websockets_commander_script() {
            let port: u16 = 8014;
            let mut interpreter = Interpreter::new();
            interpreter.with_variable("port", Number(I64Value(port as i64)));
            interpreter = verify_exact_code_with(interpreter, r#"
                http::serve(port, {
                    "/api/ws" : {
                        "WS" : (() -> {
                            "on_open" : ((conn, message) -> message)
                            "on_message" : ((conn, message) -> {
                                let stocks = nsd::load("web_engine.websockets_commander.stocks")
                                stocks where symbol is message::to_string
                            })
                            "on_close" : ((conn, message) -> message)
                        })
                    }
                })
            "#, "true");

            let mut interpreter = Interpreter::new();
            interpreter = verify_exact_code_with(interpreter, r#"
                let stocks = nsd::save("web_engine.websockets_commander.stocks",
                    |--------------------------------|
                    | symbol | exchange  | last_sale |
                    |--------------------------------|
                    | GIF    | NYSE      | 11.75     |
                    | TRX    | NASDAQ    | 32.96     |
                    | SHMN   | OTCBB     | 5.02      |
                    | XCD    | OTCBB     | 1.37      |
                    | DRMQ   | OTHER_OTC | 0.02      |
                    | JTRQ   | OTHER_OTC | 0.0001    |
                    |--------------------------------|
                )
            "#, "true");

            // connect the web socket
            if let UUIDValue(client_id) = ws_commander::connect_ws("0.0.0.0", port, "/api/ws").await.unwrap() {
                println!("client_id {client_id:0x}");
                
                // send a text command
                let value = ws_commander::send_text_command(client_id, "SHMN").await.unwrap();
                assert_eq!(make_lines_from_table(value), vec![
                    "|-------------------------------|", 
                    "| symbol | exchange | last_sale |", 
                    "|-------------------------------|", 
                    "| SHMN   | OTCBB    | 5.02      |", 
                    "|-------------------------------|"]);

                // send a binary command
                let value = ws_commander::send_binary_command(client_id, b"DRMQ".to_vec()).await.unwrap();
                assert_eq!(make_lines_from_table(value), vec![
                    "|--------------------------------|", 
                    "| symbol | exchange  | last_sale |", 
                    "|--------------------------------|", 
                    "| DRMQ   | OTHER_OTC | 0.02      |", 
                    "|--------------------------------|"]);

                // close the connection
                let outcome = ws_commander::close(client_id).await.unwrap();
                assert_eq!(outcome, StringValue(String::new()));
            } else { 
                assert!(false);
            }
        }

        #[ignore]
        #[actix::test]
        async fn test_websockets_builtins_script() {
            let port: u16 = 8015;
            let mut interpreter = Interpreter::new();
            interpreter.with_variable("port", Number(I64Value(port as i64)));
            interpreter.with_variable("path", StringValue("/api/ws".into()));
            interpreter = verify_exact_code_with(interpreter, r#"
                http::serve(port, {
                    "/api/ws" : {
                        "WS" : (() -> {
                            "on_open" : ((conn, message) -> message)
                            "on_message" : ((conn, message) -> {
                                let stocks = nsd::load("web_engine.websocket_builtins.stocks")
                                stocks where symbol is message::to_string
                            })
                            "on_close" : ((conn, message) -> message)
                        })
                    }
                })
            "#, "true");

            let mut interpreter = Interpreter::new();
            interpreter = verify_exact_code_with(interpreter, r#"
                let stocks = nsd::save("web_engine.websocket_builtins.stocks",
                    |--------------------------------|
                    | symbol | exchange  | last_sale |
                    |--------------------------------|
                    | GIF    | NYSE      | 11.75     |
                    | TRX    | NASDAQ    | 32.96     |
                    | SHMN   | OTCBB     | 5.02      |
                    | XCD    | OTCBB     | 1.37      |
                    | DRMQ   | OTHER_OTC | 0.02      |
                    | JTRQ   | OTHER_OTC | 0.0001    |
                    |--------------------------------|
                )
            "#, "true");

            let mut interpreter = Interpreter::new();
            interpreter = verify_exact_code_with(interpreter, r#"
                let conn = ws::connect("0.0.0.0", 8015, "/api/ws")
            "#, "true");

            let mut interpreter = Interpreter::new();
            interpreter = verify_exact_table_with(interpreter, r#"
                ws::send_text(conn, "SHMN")
            "#, vec![
                "|-------------------------------|",
                "| symbol | exchange | last_sale |",
                "|-------------------------------|",
                "| SHMN   | OTCBB    | 5.02      |",
                "|-------------------------------|"]);

            let mut interpreter = Interpreter::new();
            interpreter = verify_exact_table_with(interpreter, r#"
                ws::send_bytes(conn, "DRMQ")
            "#, vec![
                "|--------------------------------|",
                "| symbol | exchange  | last_sale |",
                "|--------------------------------|",
                "| DRMQ   | OTHER_OTC | 0.02      |",
                "|--------------------------------|"]);
        }
    }

}