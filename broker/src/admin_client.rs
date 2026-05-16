use std::{env, time::Duration};

use reqwest::StatusCode;
use serde::de::DeserializeOwned;
use thiserror::Error;

use crate::cli::ApiArgs;

#[derive(Debug, Clone)]
pub struct AdminClient {
    client: reqwest::Client,
    base_url: String,
    username: String,
    password: String,
}

#[derive(Debug, Error)]
pub enum AdminClientError {
    #[error("missing Admin API username; pass --user or set YEDMQ_API_USER")]
    MissingUsername,
    #[error(
        "missing Admin API password; pass --password, --password-stdin, or set YEDMQ_API_PASSWORD"
    )]
    MissingPassword,
    #[error("invalid timeout `{value}`; use values like 3s, 500ms, or 2")]
    InvalidTimeout { value: String },
    #[error("failed to build Admin API client: {0}")]
    BuildClient(reqwest::Error),
    #[error("Admin API authentication failed: {url}")]
    Unauthorized { url: String },
    #[error("Admin API endpoint not found: {url}")]
    NotFound { url: String },
    #[error("Admin API request failed: {url}: {source}")]
    Request {
        url: String,
        #[source]
        source: reqwest::Error,
    },
    #[error("Admin API returned status {status}: {url}: {body}")]
    Status {
        url: String,
        status: StatusCode,
        body: String,
    },
    #[error("failed to parse Admin API response from {url}: {source}")]
    Decode {
        url: String,
        #[source]
        source: reqwest::Error,
    },
}

impl AdminClient {
    pub fn new(args: &ApiArgs, stdin_password: Option<String>) -> Result<Self, AdminClientError> {
        let timeout = parse_timeout(&args.timeout)?;
        let client = reqwest::Client::builder()
            .timeout(timeout)
            .build()
            .map_err(AdminClientError::BuildClient)?;

        let username = args
            .user
            .clone()
            .or_else(|| env::var("YEDMQ_API_USER").ok())
            .ok_or(AdminClientError::MissingUsername)?;

        let password = args
            .password
            .clone()
            .or(stdin_password)
            .or_else(|| env::var("YEDMQ_API_PASSWORD").ok())
            .ok_or(AdminClientError::MissingPassword)?;

        Ok(Self {
            client,
            base_url: args.admin.trim_end_matches('/').to_string(),
            username,
            password,
        })
    }

    pub async fn get_json<T: DeserializeOwned>(&self, path: &str) -> Result<T, AdminClientError> {
        let (_, body) = self.get_json_with_status(path, false).await?;
        Ok(body)
    }

    pub async fn get_json_with_status<T: DeserializeOwned>(
        &self,
        path: &str,
        accept_non_success: bool,
    ) -> Result<(StatusCode, T), AdminClientError> {
        let url = self.url(path);
        let response = self
            .client
            .get(&url)
            .basic_auth(self.username.clone(), Some(self.password.clone()))
            .send()
            .await
            .map_err(|source| AdminClientError::Request {
                url: url.clone(),
                source,
            })?;

        let status = response.status();
        if status == StatusCode::UNAUTHORIZED {
            return Err(AdminClientError::Unauthorized { url });
        }
        if status == StatusCode::NOT_FOUND {
            return Err(AdminClientError::NotFound { url });
        }
        if !status.is_success() && !accept_non_success {
            let body = response.text().await.unwrap_or_default();
            return Err(AdminClientError::Status { url, status, body });
        }

        let body = response
            .json::<T>()
            .await
            .map_err(|source| AdminClientError::Decode {
                url: url.clone(),
                source,
            })?;
        Ok((status, body))
    }

    fn url(&self, path: &str) -> String {
        format!("{}/{}", self.base_url, path.trim_start_matches('/'))
    }
}

pub fn parse_timeout(value: &str) -> Result<Duration, AdminClientError> {
    let trimmed = value.trim();
    if let Some(ms) = trimmed.strip_suffix("ms") {
        let ms = ms
            .parse::<u64>()
            .map_err(|_| AdminClientError::InvalidTimeout {
                value: value.to_string(),
            })?;
        return Ok(Duration::from_millis(ms));
    }
    if let Some(seconds) = trimmed.strip_suffix('s') {
        let seconds = seconds
            .parse::<u64>()
            .map_err(|_| AdminClientError::InvalidTimeout {
                value: value.to_string(),
            })?;
        return Ok(Duration::from_secs(seconds));
    }
    let seconds = trimmed
        .parse::<u64>()
        .map_err(|_| AdminClientError::InvalidTimeout {
            value: value.to_string(),
        })?;
    Ok(Duration::from_secs(seconds))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_timeout_suffixes() {
        assert_eq!(parse_timeout("500ms").unwrap(), Duration::from_millis(500));
        assert_eq!(parse_timeout("3s").unwrap(), Duration::from_secs(3));
        assert_eq!(parse_timeout("2").unwrap(), Duration::from_secs(2));
    }

    #[test]
    fn rejects_invalid_timeout() {
        assert!(matches!(
            parse_timeout("soon"),
            Err(AdminClientError::InvalidTimeout { .. })
        ));
    }
}
