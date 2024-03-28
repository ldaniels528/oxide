////////////////////////////////////////////////////////////////////
// Oxide WebSockets module
////////////////////////////////////////////////////////////////////

use actix::{Actor, StreamHandler};
use actix_web_actors::ws;
use log::info;

// Oxide WebSocket
pub struct OxideWebSocket {}

impl OxideWebSocket {
    pub fn new() -> Self {
        OxideWebSocket {}
    }
}

impl Actor for OxideWebSocket {
    type Context = ws::WebsocketContext<Self>;
}

impl StreamHandler<Result<ws::Message, ws::ProtocolError>> for OxideWebSocket {
    fn handle(&mut self, msg: Result<ws::Message, ws::ProtocolError>, ctx: &mut Self::Context) {
        match msg {
            Ok(ws::Message::Ping(msg)) => {
                info!("Ping! [{:?}]",msg);
                ctx.pong(&msg)
            }
            Ok(ws::Message::Pong(msg)) => {
                info!("Pong! [{:?}]",msg);
                ctx.ping(&msg)
            }
            Ok(ws::Message::Text(text)) => {
                info!("Text! [{:?}]", text);
                ctx.text(text)
            }
            Ok(ws::Message::Binary(bytes)) => {
                info!("Binary! [{:?}]", bytes);
                ctx.binary(bytes)
            }
            _ => (),
        }
    }
}
