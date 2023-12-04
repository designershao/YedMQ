use std::{sync::Arc, time::Duration};

use anyhow::Result;
use log::warn;
use tokio::{net::TcpListener, select, sync::RwLock};

use crate::{
    connection::Connection,
    inflight::Inflight,
    plugin::{self, plugin_manager::PluginManager, session_context::SessionContext},
    protocol::{
        v3::{
            connack::{ConnAckPacket, ConnAckPacketBuilder, VariableHeader},
            fixed_header::FixHeader,
        },
        PacketType,
    },
    router::RouterCmd,
    session::{Session, SessionHandle, SessionManager, SessionManagerError},
    settings::Settings,
    topic::TopicManager,
};

pub struct MqttTcpListener {
    pub plugin_manager: Arc<PluginManager>,
    pub session_manager: Arc<RwLock<SessionManager>>,
    pub topic_manager: Arc<RwLock<TopicManager>>,
    pub router_sender: tokio::sync::mpsc::Sender<RouterCmd>,
    pub settings: Arc<Settings>,
}

impl MqttTcpListener {
    pub async fn run(self) -> Result<()> {
        let listener = TcpListener::bind(self.settings.listener.tcp.external.clone()).await?;
        loop {
            let (stream, _) = listener.accept().await?;

            let peer_addr = stream.peer_addr().unwrap();
            let plugin_manager = self.plugin_manager.clone();
            let session_manager = self.session_manager.clone();
            let topic_manager = self.topic_manager.clone();
            let router_sender = self.router_sender.clone();
            let settings = self.settings.clone();

            tokio::spawn(async move {
                let mut connection = Connection::new(stream);
                // wait the first connect packet, if the packet is not correct, the connection will be closed.
                let first_packet = connection.read_packet().await.unwrap();

                match first_packet {
                    crate::protocol::MqttPacketV3::Connect(packet) => {
                        let r = plugin_manager
                            .clone()
                            .call_hook_on_connect_auth(&peer_addr.to_string(), &packet)
                            .await;
                        match r {
                            Ok(auth_result) => {
                                match auth_result {
                                    crate::plugin::plugin_manager::OnConnectAuthResult::Pass(tenant_id, user_id) => {

                                        let session_context = SessionContext {
                                            tenant_id: tenant_id.clone(),
                                            client_identifier: packet.payload.client_identifier.clone(),
                                            username: user_id,
                                            remote_addr: connection.get_stream().peer_addr().unwrap().to_string(),
                                        };

                                        let connack_packet = ConnAckPacketBuilder::new().set_return_code(crate::protocol::v3::connack::ConnackReturnCode::Accpet).build();
                                        if let Err(e) = connection.write_packet(&crate::protocol::MqttPacketV3::Connack(connack_packet)).await {
                                            warn!("write connack packet error: {}", e);
                                            connection.shutdown().await.unwrap();
                                            return
                                        }

                                        {
                                            let _ = session_manager.write().await.create_tenant(tenant_id.clone()).await;
                                        }

                                        let new_session = Session {
                                            will_message: None,
                                            client_identifier: packet.payload.client_identifier.clone(),
                                            tenant_identifier: tenant_id.clone(),
                                            subscription_topics: vec![],
                                            inflight: Inflight::new(Duration::from_secs(settings.session.qos_expired_secs)),
                                            clean_session: packet.variable_header.clean_session,
                                            session_state: crate::session::SessionState::Online,
                                        };

                                        let (quit_signal, quit_waiter) = tokio::sync::oneshot::channel();

                                        let mut session_handle = None;

                                        {
                                            let mut session_manager = session_manager.write().await;
                                            let session_handle_result = session_manager.get_session_handle(tenant_id.clone(), packet.payload.client_identifier.clone()).await;
                                            if session_handle_result.is_ok() {
                                                let session_handle_pre = session_handle_result.unwrap();
                                                session_handle = Some(session_handle_pre);
                                            }
                                        }

                                        let session_handle = match session_handle {
                                            Some(mut session_handle_pre) => {
                                                if session_handle_pre.is_online().await {
                                                    session_handle_pre.kick_off().await;
                                                }

                                                if !session_handle_pre.is_clean_session().await {
                                                    session_handle_pre.into_online(
                                                        connection, 
                                                        plugin_manager, 
                                                        topic_manager,
                                                        packet.variable_header.keep_alive.into(),
                                                        settings.session.packet_resend_interval_secs,
                                                        router_sender,
                                                        quit_signal,
                                                        session_context
                                                    ).await;
                                                    }
                                                session_handle_pre
                                            }
                                            None => {
                                                let session_handle_new = SessionHandle::new(
                                                    new_session,
                                                    connection,
                                                    plugin_manager,
                                                    topic_manager,
                                                    packet.variable_header.keep_alive.into(),
                                                    settings.session.packet_resend_interval_secs,
                                                    router_sender.clone(),
                                                    quit_signal,
                                                    session_context
                                                ).await;
                                                session_handle_new
                                            }
                                        };

                                        {
                                            let _ = session_manager.write().await.register(
                                                tenant_id.clone(),
                                             packet.payload.client_identifier.clone(), 
                                                session_handle).await;
                                        }

                                        let _ = quit_waiter.await; // quit session online state

                                        if packet.variable_header.clean_session {
                                            let mut session_manager = session_manager.write().await;
                                            let _ = session_manager.remove(tenant_id.clone(), packet.payload.client_identifier.clone()).await;
                                        }
                                    },
                                    crate::plugin::plugin_manager::OnConnectAuthResult::Forbidden => {
                                        let connack_packet = ConnAckPacketBuilder::new().set_return_code(crate::protocol::v3::connack::ConnackReturnCode::InvalidUsernameOrPassword).build();
                                        if let Err(e) = connection.write_packet(&crate::protocol::MqttPacketV3::Connack(connack_packet)).await {
                                            warn!("write connack packet error: {}", e);
                                        }
                                        if let Err(e) = connection.shutdown().await {
                                            warn!("shutdown connection error: {}", e);
                                        }
                                    },
                                    crate::plugin::plugin_manager::OnConnectAuthResult::Error(_) => {
                                        let connack_packet = ConnAckPacketBuilder::new().set_return_code(crate::protocol::v3::connack::ConnackReturnCode::ServerUnavailable).build();
                                        if let Err(e) = connection.write_packet(&crate::protocol::MqttPacketV3::Connack(connack_packet)).await {
                                            warn!("write connack packet error: {}", e);
                                        }
                                        if let Err(e) = connection.shutdown().await {
                                            warn!("shutdown connection error: {}", e);
                                        }
                                    },
                                }
                            }
                            Err(e) => {
                                warn!("auth on connect failed: {}", e);
                            }
                        }
                    }
                    _ => {
                        connection.shutdown().await.unwrap();
                    }
                }
            });
        }
    }
}


mod tests {
    use std::{path::PathBuf, collections::HashMap};

    use tokio::io::{AsyncWriteExt, AsyncReadExt};

    use crate::{settings::Plugin, protocol::MqttPacketV3};

    use super::*;

    async fn get_test_plugin_manager() -> Arc<PluginManager> {
        let crate_root_path = env!("CARGO_MANIFEST_DIR");
        let plugin_path = PathBuf::from(crate_root_path).join("tests");

        let plugin_manager = PluginManager::new(&plugin_path.to_str().unwrap().to_string())
            .await
            .unwrap();
        Arc::new(plugin_manager)
    }


    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    pub async fn test_tcp_listener_connect() {

        let plugin_manager =get_test_plugin_manager().await;

        let keep_live_duration_secs = 5;

        let resend_duration_secs = 10;

        let (router_sender, router_receiver) = tokio::sync::mpsc::channel(10);

        let settings = Settings {
            session: crate::settings::Session { qos_expired_secs: 2, packet_resend_interval_secs: resend_duration_secs },
            listener: crate::settings::Listener { tcp: crate::settings::Tcp { external: "127.0.0.1:18088".to_string() }},
            plugin: crate::settings::Plugin { dir: "test".to_string() }
        };

        let listener = MqttTcpListener {
            plugin_manager,
            session_manager: Arc::new(RwLock::new(SessionManager{ session_table:  HashMap::<String,RwLock<HashMap<String, SessionHandle>>>::new()})),
            topic_manager: Arc::new(RwLock::new(TopicManager::new())),
            router_sender: router_sender,
            settings: Arc::new(settings),
        };

        tokio::spawn(async move {
            listener.run().await.unwrap();
        });

        let mut writer = tokio::net::TcpStream::connect("127.0.0.1:18088").await.unwrap();
        let connect_packet = crate::protocol::v3::connect::ConnectPacketBuilder::new("test".to_string())
            .clean_session(true)
            .keep_alive(keep_live_duration_secs)
            .build();

        let connect_packet = crate::protocol::MqttPacketV3::Connect(connect_packet);
        writer.write(&connect_packet.to_bytes()).await.unwrap();
        writer.flush().await.unwrap();

        let mut buf = Vec::new();
        let read_bytes = writer.read_buf(&mut buf).await.unwrap();
        if read_bytes == 0 {
            assert!(false)
        } else {
            let packet = crate::protocol::parse(&buf).unwrap().1.1;
            match packet {
                MqttPacketV3::Connack(connack_packet) => {
                    assert_eq!(connack_packet.variable_header.connect_return_code, 0x00);
                }
                _ => assert!(false)
            }
        }

    }

}