use tokio_tungstenite::tungstenite::{handshake::server::Callback, http::HeaderValue};

pub mod tcp_listener;
pub mod tcp_tls_listener;
pub mod websocket_tls_tunnel;
pub mod websocket_tunnel;
pub mod ws_listener;
pub mod wss_listener;

struct WsCallBack {}

impl Callback for WsCallBack {
    fn on_request(
        self,
        request: &tokio_tungstenite::tungstenite::handshake::server::Request,
        response: tokio_tungstenite::tungstenite::handshake::server::Response,
    ) -> std::prelude::v1::Result<
        tokio_tungstenite::tungstenite::handshake::server::Response,
        tokio_tungstenite::tungstenite::handshake::server::ErrorResponse,
    > {
        let protocol = request
            .headers()
            .get("Sec-WebSocket-Protocol")
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();
        let mut mut_response = response.clone();
        mut_response.headers_mut().append(
            "Sec-WebSocket-Protocol",
            HeaderValue::from_str(protocol.as_str()).unwrap(),
        );
        std::prelude::v1::Ok(mut_response)
    }
}
