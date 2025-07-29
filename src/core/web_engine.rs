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
use itertools::Itertools;
use log::{error, warn};
use once_cell::sync::Lazy;
use shared_lib::cnv_error;
use std::collections::HashMap;
use std::convert::Into;
use std::ops::Deref;
use std::str::FromStr;
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

    /// Evaluates a query language expression
    pub async fn evaluate_async(
        ms: &Machine,
        expression: &Expression,
    ) -> std::io::Result<(Machine, TypedValue)> {
        match expression {
            HTTP(method_call) => Self::exec_http_async(ms, method_call).await,
            _ => throw(SyntaxError(SyntaxErrors::IllegalExpression(expression.to_code()))),
        }
    }

    fn exec_http(
        ms: &Machine,
        call: &HttpMethodCalls
    ) -> std::io::Result<(Machine, TypedValue)> {
        use isahc::{ReadResponseExt, RequestExt};

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

    async fn exec_http_async(
        ms: &Machine,
        call: &HttpMethodCalls
    ) -> std::io::Result<(Machine, TypedValue)> {
        use reqwest::multipart::{Form, Part};

        fn create_form(structure: Structures) -> Form {
            structure.to_name_values().iter().fold(Form::new(), |form, (name, value)| {
                form.part(name.to_owned(), Part::text(value.unwrap_value()))
            })
        }

        // evaluate the URL or configuration object
        match ms.evaluate_async(&call.get_url_or_config()).await? {
            // GET http://localhost:9000/quotes/AAPL/NYSE
            (ms, StringValue(url)) => {
                Self::exec_http_request_async(&ms, call, url.to_string(), None, Vec::new()).await
            },
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
                Self::exec_http_request_async(&ms, call, url.unwrap_value(), maybe_body, headers).await
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
        use isahc::{ReadResponseExt, Request, RequestExt};
        let mut builder = match method_call {
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

    async fn exec_http_request_async(
        ms: &Machine,
        method_call: &HttpMethodCalls,
        url: String,
        body: Option<String>,
        headers: Vec<(String, String)>,
    ) -> std::io::Result<(Machine, TypedValue)> {
        use reqwest::{Client, Method};
        use reqwest::header::{HeaderMap, HeaderName, HeaderValue, CONTENT_TYPE};

        // create the request
        let url_str = url.as_str();
        let client = Client::new();
        let mut builder = match method_call {
            HttpMethodCalls::DELETE(..) => client.delete(url_str),
            HttpMethodCalls::GET(..) => client.get(url_str),
            HttpMethodCalls::HEAD(..) => client.head(url_str),
            HttpMethodCalls::OPTIONS(..) => client.request(Method::OPTIONS, url_str),
            HttpMethodCalls::PATCH(..) => client.patch(url_str),
            HttpMethodCalls::POST(..) => client.post(url_str),
            HttpMethodCalls::PUT(..) => client.put(url_str),
            HttpMethodCalls::TRACE(..) => client.request(Method::TRACE, url_str),
        };

        // build the request headers
        let mut req_headers = HeaderMap::new();
        for (key, value) in headers {
            req_headers.insert(
                HeaderName::from_str(key.as_str()).map_err(|e| cnv_error!(e))?,
                HeaderValue::from_str(value.as_str()).map_err(|e| cnv_error!(e))?
            );
        }
        if body.is_some() {
            req_headers.insert(
                CONTENT_TYPE,
                HeaderValue::from_str("application/json").map_err(|e| cnv_error!(e))?);
        } else {
            req_headers.insert(
                CONTENT_TYPE,
                HeaderValue::from_str("application/json").map_err(|e| cnv_error!(e))?);
        };

        // set the body
        if let Some(body) = body {
            builder = builder.body(body)
        }

        // submit the request
        let response = builder
            .headers(req_headers)
            .send().await.map_err(|e| cnv_error!(e))?;

        Self::exec_http_response_async(ms, response, method_call.is_header_only())
            .await
            .map(|result| (ms.clone(), result))
    }

    /// Converts a [Response] to a [TypedValue]
    fn exec_http_response(
        ms: &Machine,
        mut response: isahc::Response<isahc::Body>,
        is_header_only: bool,
    ) -> std::io::Result<TypedValue> {
        use isahc::{ReadResponseExt, RequestExt};
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

    /// Converts a [Response] to a [TypedValue]
    async fn exec_http_response_async(
        ms: &Machine,
        mut response: reqwest::Response,
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
                let body = response.text().await.map_err(|e| cnv_error!(e))?;
                match Compiler::build(body.as_str()) {
                    Ok(expr) => {
                        Ok(match ms.evaluate_async(&expr).await {
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

    pub async fn close(&mut self) -> std::io::Result<()> {
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

    pub async fn read_next(&mut self) -> std::io::Result<TypedValue> {
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

    pub async fn send_binary_message(&mut self, message: Vec<u8>) -> std::io::Result<()> {
        self.write.send(Message::Binary(message)).await
            .map_err(|e| cnv_error!(e))
    }

    pub async fn send_text_message(&mut self, message: &str) -> std::io::Result<()> {
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

/// Unit tests
#[cfg(test)]
mod tests {
    /// HTTP tests
    #[cfg(test)]
    mod http_tests {
        use crate::data_types::DataType::{NumberType, RuntimeResolvedType, StringType};
        use crate::expression::Expression::{Identifier, Literal, StructureExpression};
        use crate::expression::HttpMethodCalls;
        use crate::interpreter::Interpreter;
        use crate::number_kind::NumberKind::F64Kind;
        use crate::numbers::Numbers::{F64Value, I64Value};
        use crate::packages::{webservers, WwwPkg};
        use crate::parameter::Parameter;
        use crate::server_engine::{APIMethods, UserAPI, UserAPIMethod};
        use crate::structures::SoftStructure;
        use crate::structures::Structures::Soft;
        use crate::test_util::{make_lines_from_table, start_test_server_async, verify_exact_code_with, verify_exact_code_with_async, verify_exact_json_with, verify_exact_json_with_async, verify_exact_table_with, verify_exact_table_with_async};
        use crate::typed_values::TypedValue::{Boolean, Function, Number, StringValue, Structured};
        use crate::web_engine::WebEngine;
        use crate::{server_engine, test_util};
        use serde_json::json;

        #[actix::test]
        async fn test_http_serve() {
            let port = start_test_server_async().await.unwrap();
            let mut interpreter = Interpreter::new();
            interpreter = verify_exact_code_with_async(interpreter, r#"
                let stocks = nsd::save(
                    "web_engine.http_serve.stocks",
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
            "#, "true").await;

            let (_, result) = WebEngine::exec_http_async(
                interpreter.get_machine(),
                &HttpMethodCalls::GET(Literal(StringValue(
                    format!("http://0.0.0.0:{port}/web_engine/http_serve/stocks/0/3")
                )).into()),
            ).await.unwrap();

            assert_eq!(result.to_json(), json!([
                {"exchange":"NYSE","last_sale":11.75,"symbol":"GIF"},
                {"exchange":"NASDAQ","last_sale":32.96,"symbol":"TRX"},
                {"exchange":"NYSE","last_sale":23.66,"symbol":"RLP"}
            ]));

            webservers::stop_server(port).await.unwrap();
        }

        #[actix::test]
        async fn test_http_serve_async() {
            let port = start_test_server_async().await.unwrap();
            let mut interpreter = Interpreter::new();
            interpreter = verify_exact_code_with_async(interpreter, r#"
                let stocks = nsd::save(
                    "web_engine.http_serve_async.stocks",
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
            "#, "true").await;

            let (_, result) = WebEngine::exec_http_async(
                interpreter.get_machine(),
                &HttpMethodCalls::GET(Literal(StringValue(
                    format!("http://0.0.0.0:{port}/web_engine/http_serve_async/stocks/0/3")
                )).into()),
            ).await.unwrap();

            assert_eq!(result.to_json(), json!([
                {"exchange":"NYSE","last_sale":11.75,"symbol":"GIF"},
                {"exchange":"NASDAQ","last_sale":32.96,"symbol":"TRX"},
                {"exchange":"NYSE","last_sale":23.66,"symbol":"RLP"}
            ]));

            webservers::stop_server(port).await.unwrap();
        }

        #[actix::test]
        async fn test_http_serve_and_query() {
            let port = start_test_server_async().await.unwrap();
            let mut interpreter = Interpreter::new();
            interpreter = verify_exact_code_with_async(interpreter, r#"
                stocks = nsd::save(
                    "web_engine.http_serve_and_query.stocks",
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
            "#, "true").await;

            let mut interpreter = Interpreter::new();
            interpreter = verify_exact_table_with_async(interpreter, format!(r#"
                (GET http://localhost:{port}/web_engine/http_serve_and_query/stocks/0/9)
                    where exchange is "NYSE"
            "#).as_str(), vec![
                "|------------------------------------|",
                "| id | exchange | last_sale | symbol |",
                "|------------------------------------|",
                "| 0  | NYSE     | 11.75     | GIF    |",
                "| 2  | NYSE     | 23.66     | RLP    |",
                "|------------------------------------|"]).await;

            let mut interpreter = Interpreter::new();
            interpreter = verify_exact_table_with_async(interpreter, format!(r#"
                use agg
                select
                    exchange,
                    min_sale: min(last_sale),
                    max_sale: max(last_sale),
                    avg_sale: avg(last_sale),
                    total_sale: sum(last_sale),
                    qty: count(last_sale)
                from
                    (GET http://localhost:{port}/web_engine/http_serve_and_query/stocks/0/9)
                group_by exchange
                having total_sale > 1.0
                order_by total_sale::asc
            "#).as_str(), vec![
                "|-------------------------------------------------------------------|",
                "| id | exchange | min_sale | max_sale | avg_sale | total_sale | qty |",
                "|-------------------------------------------------------------------|",
                "| 0  | OTCBB    | 1.37     | 5.02     | 3.195    | 6.39       | 2   |",
                "| 1  | NYSE     | 11.75    | 23.66    | 17.705   | 35.41      | 2   |",
                "| 2  | NASDAQ   | 32.96    | 214.88   | 99.69    | 299.07     | 3   |",
                "|-------------------------------------------------------------------|"]).await;

            webservers::stop_server(port).await.unwrap();
        }

        #[actix::test]
        async fn test_http_serve_post() {
            let port = start_test_server_async().await.unwrap();

            // create the "stocks" table
            let mut interpreter = Interpreter::new();
            let result = interpreter.evaluate(r#"
                stocks = nsd::save(
                    "web_engine.http_post_sync.stocks",
                    Table(symbol: String(8), exchange: String(8), last_sale: f64)::new
                )
            "#).unwrap();
            assert_eq!(result, Boolean(true));

            // POST {
            //     url: http://0.0.0.0:8228/web_engine/http_post_sync/stocks/0
            //     body: { symbol: "ABC", exchange: "AMEX", last_sale: 11.77 }
            // }
            let mut interpreter = Interpreter::new();
            let (_, result) = WebEngine::exec_http(
                interpreter.get_machine(),
                &HttpMethodCalls::POST(StructureExpression(vec![
                    ("url".into(), Literal(StringValue(format!("http://0.0.0.0:{port}/web_engine/http_post_sync/stocks/0")))),
                    ("body".into(), StructureExpression(vec![
                        ("symbol".into(), Literal(StringValue("GIF".into()))),
                        ("exchange".into(), Literal(StringValue("NYSE".into()))),
                        ("last_sale".into(), Literal(Number(F64Value(11.33)))),
                    ]))
                ]).into()),
            ).unwrap();

            assert_eq!(result.to_json(), json!(0));

            // GET http://0.0.0.0:8228/web_engine/http_post_sync/stocks/0
            let (_, result) = WebEngine::exec_http(
                interpreter.get_machine(),
                &HttpMethodCalls::GET(Literal(StringValue(
                    format!("http://0.0.0.0:{port}/web_engine/http_post_sync/stocks/0")
                )).into()),
            ).unwrap();

            assert_eq!(result.to_json(), json!(
                {"symbol":"GIF", "exchange":"NYSE", "last_sale":11.33}
            ));

            webservers::stop_server(port).await.unwrap();
        }

        #[actix::test]
        async fn test_http_serve_post_async() {
            let port = start_test_server_async().await.unwrap();

            // create the "stocks" table
            let mut interpreter = Interpreter::new();
            let result = interpreter.evaluate_async(r#"
                stocks = nsd::save(
                    "web_engine.http_post_async.stocks",
                    Table(symbol: String(8), exchange: String(8), last_sale: f64)::new
                )
            "#).await.unwrap();
            assert_eq!(result, Boolean(true));

            // POST {
            //     url: http://0.0.0.0:8229/web_engine/http_post_async/stocks/0
            //     body: { symbol: "ABC", exchange: "AMEX", last_sale: 11.77 }
            // }
            let mut interpreter = Interpreter::new();
            let (_, result) = WebEngine::exec_http_async(
                interpreter.get_machine(),
                &HttpMethodCalls::POST(StructureExpression(vec![
                    ("url".into(), Literal(StringValue(format!("http://0.0.0.0:{port}/web_engine/http_post_async/stocks/0")))),
                    ("body".into(), StructureExpression(vec![
                        ("symbol".into(), Literal(StringValue("GIF".into()))),
                        ("exchange".into(), Literal(StringValue("NYSE".into()))),
                        ("last_sale".into(), Literal(Number(F64Value(11.33)))),
                    ]))
                ]).into()),
            ).await.unwrap();

            assert_eq!(result.to_json(), json!(0));

            // GET http://0.0.0.0:8229/web_engine/http_post_async/stocks/0
            let (_, result) = WebEngine::exec_http_async(
                interpreter.get_machine(),
                &HttpMethodCalls::GET(Literal(StringValue(
                    format!("http://0.0.0.0:{port}/web_engine/http_post_async/stocks/0")
                )).into()),
            ).await.unwrap();

            assert_eq!(result.to_json(), json!(
                {"symbol":"GIF", "exchange":"NYSE", "last_sale":11.33}
            ));

            webservers::stop_server(port).await.unwrap();
        }

        #[actix::test]
        async fn test_http_serve_workflow() {
            let port = start_test_server_async().await.unwrap();
            let mut interpreter = Interpreter::new();
            let result = interpreter.evaluate_async(r#"
                stocks = nsd::save(
                    "web_engine.http_serve_workflow.stocks",
                    Table(symbol: String(8), exchange: String(8), last_sale: f64)::new
                )
            "#).await.unwrap();
            assert_eq!(result, Boolean(true));

            // append a new row
            let row_id = interpreter.evaluate_async(format!(r#"
                POST {{
                    url: http://localhost:{port}/web_engine/http_serve_workflow/stocks/0
                    body: {{ symbol: "ABC", exchange: "AMEX", last_sale: 11.77 }}
                }}
            "#).as_str()).await.unwrap();
            assert!(matches!(row_id, Number(I64Value(..))));

            // fetch the previously created row
            let row = interpreter.evaluate_async(format!(r#"
                GET http://localhost:{port}/web_engine/http_serve_workflow/stocks/{row_id}
            "#).as_str()).await.unwrap();
            assert_eq!(
                row.to_json(),
                json!({"exchange":"AMEX","symbol":"ABC","last_sale":11.77})
            );

            // replace the previously created row
            let result = interpreter.evaluate_async(format!(r#"
                PUT {{
                    url: http://localhost:{port}/web_engine/http_serve_workflow/stocks/{row_id}
                    body: {{ symbol: "ABC", exchange: "AMEX", last_sale: 11.79 }}
                }}
            "#).as_str()).await.unwrap();
            assert_eq!(result, Number(I64Value(1)));

            // re-fetches the previously updated row
            let row = interpreter.evaluate_async(format!(r#"
                GET http://localhost:{port}/web_engine/http_serve_workflow/stocks/{row_id}
            "#).as_str()).await.unwrap();
            assert_eq!(
                row.to_json(),
                json!({"symbol":"ABC","exchange":"AMEX","last_sale":11.79})
            );

            // update the previously created row
            let result = interpreter.evaluate_async(format!(r#"
                PATCH {{
                    url: http://localhost:{port}/web_engine/http_serve_workflow/stocks/{row_id}
                    body: {{ last_sale: 11.81 }}
                }}
            "#).as_str()).await.unwrap();
            assert_eq!(result, Number(I64Value(1)));

            // re-fetches the previously updated row
            let row = interpreter.evaluate_async(format!(r#"
                GET http://localhost:{port}/web_engine/http_serve_workflow/stocks/{row_id}
            "#).as_str()).await.unwrap();
            assert_eq!(
                row.to_json(),
                json!({"last_sale":11.81,"symbol":"ABC","exchange":"AMEX"})
            );

            // fetch the headers for the previously updated row
            let result = interpreter.evaluate_async(format!(r#"
                HEAD http://localhost:{port}/web_engine/http_serve_workflow/stocks/{row_id}
            "#).as_str()).await.unwrap();
            println!("HEAD: {}", result.to_string());
            assert!(matches!(result, Structured(Soft(..))));

            // delete the previously updated row
            let result = interpreter.evaluate_async(format!(r#"
                DELETE http://localhost:{port}/web_engine/http_serve_workflow/stocks/{row_id}
            "#).as_str()).await.unwrap();
            assert_eq!(result, Number(I64Value(1)));

            // verify the deleted row is empty
            let row = interpreter.evaluate_async(format!(r#"
                GET http://localhost:{port}/web_engine/http_serve_workflow/stocks/{row_id}
            "#).as_str()).await.unwrap();
            assert_eq!(row, Structured(Soft(SoftStructure::empty())));

            webservers::stop_server(port).await.unwrap();
        }

        #[actix::test]
        async fn test_http_serve_workflow_script() {
            let port = start_test_server_async().await.unwrap();
            let mut interpreter = Interpreter::new();
            let result = interpreter.evaluate_async(format!(r#"
                // create the table
                nsd::save(
                    "web_engine.http_workflow.stocks",
                    Table(symbol: String(8), exchange: String(8), last_sale: f64)::new
                )
                row_id = POST {{
                    url: http://localhost:{port}/web_engine/http_workflow/stocks/0
                    body: {{ symbol: "ABC", exchange: "AMEX", last_sale: 11.77 }}
                }}
                assert(row_id matches 0)
                GET http://localhost:{port}/web_engine/http_workflow/stocks/0
            "#).as_str()).await.unwrap();
            
            assert_eq!(
                result.to_json(),
                json!({"exchange": "AMEX", "last_sale": 11.77, "symbol": "ABC"})
            );

            webservers::stop_server(port).await.unwrap();
        }

        #[actix::test]
        async fn test_http_serve_user_api_get() {
            let port = webservers::get_random_port();
            let mut interpreter = Interpreter::new();
            interpreter = verify_exact_code_with_async(interpreter, format!(r#"
                http::start({port}, {{
                    "/api/stocks" : {{
                        "GET" : (ticker -> {{
                            let stocks = nsd::load("web_engine.http_serve_api_get.stocks")
                            stocks where symbol is ticker
                        }})
                    }}
                }})
            "#).as_str(), "true").await;

            interpreter = verify_exact_code_with_async(interpreter, r#"
                let stocks = nsd::save("web_engine.http_serve_api_get.stocks",
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
            "#, "true").await;

            interpreter = verify_exact_json_with_async(interpreter, format!(r#"
                GET http://localhost:{port}/api/stocks?ticker=SHMN
            "#).as_str(), json!([{"symbol": "SHMN", "exchange": "OTCBB", "last_sale": 5.02}])).await;
        }

        #[actix::test]
        async fn test_http_serve_user_api_post() {
            let port = webservers::get_random_port();
            let mut interpreter = Interpreter::new();
            interpreter = verify_exact_code_with_async(interpreter, format!(r#"
                http::start({port}, {{
                    "/api/stocks" : {{
                        "GET" : (ticker -> {{
                            let stocks = nsd::load("web_engine.http_serve_api_post.stocks")
                            stocks where symbol is ticker
                        }})
                        "POST" : (quote -> {{
                            let stocks = nsd::load("web_engine.http_serve_api_post.stocks")
                            quote ~> stocks
                        }})
                    }}
                }})
            "#).as_str(), "true").await;

            interpreter = verify_exact_code_with_async(interpreter, r#"
                let stocks = nsd::save("web_engine.http_serve_api_post.stocks",
                    |--------------------------------|
                    | symbol | exchange  | last_sale |
                    |--------------------------------|
                    | GIF    | NYSE      | 11.75     |
                    |--------------------------------|
                )
            "#, "true").await;

            interpreter = verify_exact_code_with_async(interpreter, format!(r#"
                POST {{
                    url: http://localhost:{port}/api/stocks
                    body: {{ symbol: "ABC", exchange: "AMEX", last_sale: 11.79 }}
                }}
            "#).as_str(), "1").await;

            interpreter = verify_exact_code_with_async(interpreter, format!(r#"
                GET http://localhost:{port}/api/stocks?ticker=ABC
            "#).as_str(), r#"[{exchange: "AMEX", last_sale: 11.79, symbol: "ABC"}]"#).await;
        }
    }

    /// Unit tests
    #[cfg(test)]
    mod ws_tests {
        use crate::numbers::Numbers::I64Value;
        use crate::packages::{webservers, Package};
        use crate::test_util::{make_lines_from_table, start_test_server_async};
        use crate::typed_values::TypedValue::Number;
        use crate::web_engine::WebSocketClient;

        #[actix::test]
        async fn test_websocket_basic_conversation() {
            let port = start_test_server_async().await.unwrap();
            let mut wsc = WebSocketClient::connect("0.0.0.0", port, "/ws").await.unwrap();
            wsc.evaluate("let a = [0, 1, 3, 5]").await.unwrap();
            let value = wsc.evaluate("a[2]").await.unwrap();
            assert_eq!(value, Number(I64Value(3)));
            webservers::stop_server(port).await.unwrap();
        }

        #[actix::test]
        async fn test_websocket_remote_evaluation() {
            let port = start_test_server_async().await.unwrap();
            let mut wsc = WebSocketClient::connect("0.0.0.0", port, "/ws").await.unwrap();
            let value = wsc.evaluate(r#"
                let stocks = nsd::save(
                    "web_engine.ws_script.stocks",
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
                "|-------------------------------|"]);
            webservers::stop_server(port).await.unwrap();
        }
    }

}