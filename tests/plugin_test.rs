use std::path::PathBuf;
use samoye::{plugin_service::plugin::{plugin_context::{Authentication, Authorization, ClientInfo, ConnectInfo, TopicInfo, TopicOperation}, plugin_host::PluginHost, plugin_metadata::PluginMetadata}};

fn get_demo_plugin_path() -> PathBuf {
    let crate_root_path = env!("CARGO_MANIFEST_DIR");
    PathBuf::from(crate_root_path)
        .join("tests")
        .join("plugins")
        .join("demo_plugin")
}

#[test]
pub fn test_plugin_metadata_load() {

    let plugin_path = get_demo_plugin_path();

    let plugin_metadata = PluginMetadata::new(plugin_path).unwrap();

    assert!(plugin_metadata.name == "demo_plugin");
    assert!(plugin_metadata.author == "Samoye");
    assert!(plugin_metadata.description == "Just a demo plugin");
    assert!(plugin_metadata.version == "1.0.0");
    assert!(plugin_metadata.entry == "./src/plugin.rn");
    assert!(plugin_metadata.priority == 1000);

}

#[test]
pub fn test_plugin_host_load() {
    let plugin_path = get_demo_plugin_path();
    let mut _plugin_host = PluginHost::load(plugin_path.to_str().unwrap().to_string()).unwrap();
    assert!(true)
}

#[test]
pub fn test_plugin_on_connect_auth() {
    let plugin_path = get_demo_plugin_path();
    let mut plugin_host = PluginHost::load(plugin_path.to_str().unwrap().to_string()).unwrap();
    plugin_host.init().unwrap();

    let connect_info = ConnectInfo {
        username: Some("samoye".to_string()),
        password: Some("123456".to_string()),
        remote_addr: "127.0.0.1".to_string(),
    };

    let r = plugin_host.on_connect_auth(connect_info).unwrap();

    match r {
        Authentication::Allow(tenant_id) => {
            assert_eq!(tenant_id, "tenant_id".to_string());
        },
        _ => assert!(false),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
pub async fn test_plugin_on_topic_permission_check() {
    let plugin_path = get_demo_plugin_path();
    let mut plugin_host = PluginHost::load(plugin_path.to_str().unwrap().to_string()).unwrap();
    plugin_host.init().unwrap();
    let client_info = ClientInfo {
        tenant_id: "tenant_id".into(),
        client_identifier: "client_id".into(),
        username: "samoye".into(),
        remote_addr: "127.0.0.1".into(),
    };
    let topic_info = TopicInfo {
        topic_filter: "/a/b".into(),
        qos: 0,
        operation: TopicOperation::Publish,
    };

    let r = plugin_host.on_topic_permission_check(client_info, topic_info).await.unwrap();
    match r {
        Authorization::Allow => assert!(true),
        _ => assert!(false),
    }
}