use log::warn;
use tokio_tungstenite::tungstenite::{
    handshake::server::Callback,
    http::{HeaderValue, Response, StatusCode},
};

pub mod tcp_listener;
pub mod tcp_tls_listener;
pub mod websocket_tls_tunnel;
pub mod websocket_tunnel;
pub mod ws_listener;
pub mod wss_listener;

struct WsCallBack {}

fn bad_request_response(
    message: &str,
) -> tokio_tungstenite::tungstenite::handshake::server::ErrorResponse {
    match Response::builder()
        .status(StatusCode::BAD_REQUEST)
        .body(Some(message.to_string()))
    {
        Ok(resp) => resp,
        Err(e) => {
            warn!("failed to build error response: {}", e);
            let mut resp = Response::new(Some(message.to_string()));
            *resp.status_mut() = StatusCode::BAD_REQUEST;
            resp
        }
    }
}

impl Callback for WsCallBack {
    fn on_request(
        self,
        request: &tokio_tungstenite::tungstenite::handshake::server::Request,
        response: tokio_tungstenite::tungstenite::handshake::server::Response,
    ) -> std::prelude::v1::Result<
        tokio_tungstenite::tungstenite::handshake::server::Response,
        tokio_tungstenite::tungstenite::handshake::server::ErrorResponse,
    > {
        let protocol = match request.headers().get("Sec-WebSocket-Protocol") {
            Some(header) => match header.to_str() {
                Ok(v) => v,
                Err(e) => {
                    warn!("invalid Sec-WebSocket-Protocol header: {}", e);
                    return Err(bad_request_response(
                        "invalid Sec-WebSocket-Protocol header",
                    ));
                }
            },
            None => {
                return Err(bad_request_response(
                    "missing Sec-WebSocket-Protocol header",
                ))
            }
        };
        let mut mut_response = response.clone();
        match HeaderValue::from_str(protocol) {
            Ok(value) => {
                mut_response
                    .headers_mut()
                    .append("Sec-WebSocket-Protocol", value);
            }
            Err(e) => {
                warn!("failed to set Sec-WebSocket-Protocol header: {}", e);
                return Err(bad_request_response(
                    "invalid Sec-WebSocket-Protocol header value",
                ));
            }
        }
        std::prelude::v1::Ok(mut_response)
    }
}
