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

// Unit tests
#[cfg(test)]
mod tests {
    use futures_util::SinkExt;
    use tokio_tungstenite::connect_async;
    use tokio_tungstenite::tungstenite::protocol::Message;

    #[ignore]
    #[tokio::test]
    async fn test_send_message() {
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
}