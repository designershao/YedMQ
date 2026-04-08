use anyhow::{Context, Result};
use interprocess::local_socket::{GenericFilePath, Name, ToFsName};

pub fn resolve_local_socket_name(raw: &str) -> Result<Name<'static>> {
    #[cfg(windows)]
    {
        let normalized = normalize_local_socket_path(raw);
        let error_context = normalized.clone();
        return normalized
            .to_fs_name::<GenericFilePath>()
            .with_context(|| format!("invalid Windows named pipe path: {error_context}"));
    }

    #[cfg(not(windows))]
    {
        let name = raw.to_string();
        name.clone()
            .to_fs_name::<GenericFilePath>()
            .with_context(|| format!("invalid local socket path: {name}"))
    }
}

#[cfg(windows)]
pub fn default_local_socket_path() -> &'static str {
    "yedmq_plugin.sock"
}

#[cfg(not(windows))]
pub fn default_local_socket_path() -> &'static str {
    "/tmp/yedmq_plugin.sock"
}

pub fn normalize_local_socket_path(raw: &str) -> String {
    #[cfg(windows)]
    {
        normalize_windows_local_socket_path(raw)
    }

    #[cfg(not(windows))]
    {
        raw.to_string()
    }
}

#[cfg(windows)]
fn normalize_windows_local_socket_path(raw: &str) -> String {
    if is_windows_named_pipe_path(raw) {
        return raw.to_string();
    }

    let readable_name = std::path::Path::new(raw)
        .file_name()
        .and_then(|value| value.to_str())
        .map(sanitize_component)
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "yedmq_plugin".to_string());

    let hash = stable_hash(raw.as_bytes());
    format!(r"\\.\pipe\yedmq-{readable_name}-{hash:016x}")
}

#[cfg(windows)]
fn sanitize_component(raw: &str) -> String {
    raw.chars()
        .map(|ch| match ch {
            'a'..='z' | 'A'..='Z' | '0'..='9' | '.' | '-' | '_' => ch,
            _ => '_',
        })
        .collect()
}

#[cfg(windows)]
fn stable_hash(bytes: &[u8]) -> u64 {
    const FNV_OFFSET_BASIS: u64 = 0xcbf29ce484222325;
    const FNV_PRIME: u64 = 0x100000001b3;

    let mut hash = FNV_OFFSET_BASIS;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

#[cfg(windows)]
fn is_windows_named_pipe_path(raw: &str) -> bool {
    raw.starts_with(r"\\.\pipe\") || raw.starts_with(r"\\?\pipe\")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(windows)]
    use std::time::Duration;

    #[cfg(windows)]
    #[test]
    fn normalizes_windows_file_style_paths_to_namespaced_names() {
        let normalized = normalize_local_socket_path(r"C:\temp\yedmq_plugin.sock");
        assert!(normalized.starts_with(r"\\.\pipe\yedmq-"));
        assert!(normalized.contains("yedmq_plugin.sock"));
    }

    #[cfg(windows)]
    #[test]
    fn keeps_explicit_windows_named_pipe_paths() {
        let raw = r"\\.\pipe\yedmq_plugin.sock";
        assert_eq!(normalize_local_socket_path(raw), raw);
    }

    #[cfg(windows)]
    #[tokio::test]
    async fn resolved_names_support_tokio_connect_roundtrip() {
        use interprocess::local_socket::{
            tokio::{prelude::*, Stream},
            ListenerOptions,
        };
        use interprocess::os::windows::{
            local_socket::ListenerOptionsExt,
            security_descriptor::{AsSecurityDescriptorMutExt, SecurityDescriptor},
        };
        use std::ptr;

        let raw = format!(r"C:\temp\yedmq_plugin_test_{}.sock", std::process::id());
        let listener_name = resolve_local_socket_name(&raw).expect("listener name");
        let client_name = resolve_local_socket_name(&raw).expect("client name");
        let mut security_descriptor = SecurityDescriptor::new().expect("security descriptor");
        unsafe {
            security_descriptor
                .set_dacl(ptr::null_mut(), false)
                .expect("set null dacl");
        }

        let listener = ListenerOptions::new()
            .name(listener_name)
            .security_descriptor(security_descriptor)
            .create_tokio()
            .expect("listener should start");

        let accept_task = tokio::spawn(async move { listener.accept().await });
        let stream = tokio::time::timeout(Duration::from_secs(3), Stream::connect(client_name))
            .await
            .expect("connect should not time out")
            .expect("connect should succeed");

        drop(stream);

        accept_task
            .await
            .expect("accept task should finish")
            .expect("listener should accept client");
    }

    #[cfg(not(windows))]
    #[test]
    fn keeps_unix_paths_unchanged() {
        let raw = "/tmp/yedmq_plugin.sock";
        assert_eq!(normalize_local_socket_path(raw), raw);
    }
}
