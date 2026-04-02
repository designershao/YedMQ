use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use log::warn;
use rustls::{
    pki_types::{pem::PemObject, CertificateDer, PrivateKeyDer},
    server::WebPkiClientVerifier,
    RootCertStore, ServerConfig,
};
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

pub(crate) fn build_server_tls_config(
    cert_file: &str,
    key_file: &str,
    verify_client_cert: bool,
    cacert_file: &str,
) -> Result<ServerConfig> {
    let certs = load_pem_certificates(cert_file, "TLS certificate chain")?;
    let key = PrivateKeyDer::from_pem_file(key_file)
        .with_context(|| format!("failed to read TLS private key file: {key_file}"))?;

    let config_builder = rustls::ServerConfig::builder();
    let config_builder = if verify_client_cert {
        if cacert_file.trim().is_empty() {
            return Err(anyhow!(
                "verify_client_cert is enabled but cacert_file is not configured"
            ));
        }

        let roots = load_root_cert_store(cacert_file)?;
        let verifier = WebPkiClientVerifier::builder(Arc::new(roots))
            .build()
            .map_err(|e| anyhow!("failed to build client certificate verifier: {e}"))?;
        config_builder.with_client_cert_verifier(verifier)
    } else {
        config_builder.with_no_client_auth()
    };

    config_builder
        .with_single_cert(certs, key)
        .context("failed to build TLS server config")
}

fn load_root_cert_store(cacert_file: &str) -> Result<RootCertStore> {
    let mut roots = RootCertStore::empty();
    let certs = load_pem_certificates(cacert_file, "trusted client CA certificate")?;

    for cert in certs {
        roots
            .add(cert)
            .map_err(|e| anyhow!("failed to add client CA from {cacert_file}: {e}"))?;
    }

    if roots.is_empty() {
        return Err(anyhow!(
            "no valid client CA certificates found in {cacert_file}"
        ));
    }

    Ok(roots)
}

fn load_pem_certificates(path: &str, description: &str) -> Result<Vec<CertificateDer<'static>>> {
    match CertificateDer::pem_file_iter(path) {
        Ok(iter) => iter
            .collect::<std::result::Result<Vec<_>, _>>()
            .with_context(|| format!("failed to load {description} from {path}")),
        Err(rustls::pki_types::pem::Error::Io(error)) => {
            Err(anyhow!("failed to read {description} file {path}: {error}"))
        }
        Err(error) => Err(anyhow!(
            "failed to parse {description} from {path}: {error}"
        )),
    }
}

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
