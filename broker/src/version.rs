use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct VersionInfo {
    pub version: &'static str,
    pub git_commit: &'static str,
    pub build_time: &'static str,
    pub rust_version: &'static str,
    pub target: &'static str,
}

pub fn version_info() -> VersionInfo {
    VersionInfo {
        version: env!("CARGO_PKG_VERSION"),
        git_commit: option_env!("YEDMQ_GIT_COMMIT").unwrap_or("unknown"),
        build_time: option_env!("YEDMQ_BUILD_TIME").unwrap_or("unknown"),
        rust_version: option_env!("YEDMQ_RUST_VERSION").unwrap_or("unknown"),
        target: option_env!("YEDMQ_BUILD_TARGET").unwrap_or("unknown"),
    }
}

pub fn format_text(info: &VersionInfo) -> String {
    format!(
        "YedMQ {}\nGit Commit: {}\nBuild Time: {}\nRust Version: {}\nTarget: {}",
        info.version, info.git_commit, info.build_time, info.rust_version, info.target
    )
}
