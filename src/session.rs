use std::{sync::Arc, time::Duration, collections::HashMap};

use anyhow::{Context, Result, anyhow};
use log::{warn, info};
use thiserror::Error;
use tokio::{sync::{oneshot::Sender, RwLock, Mutex}, net::TcpStream, select};

use crate::{protocol::{MqttPacketV3, v3::{publish::PublishPacketBuilder, pingresp::PingrespPacket, suback::SubackPacket}}, inflight::Inflight, router::RouterCmd, plugin::{plugin_manager::PluginManager, session_context::SessionContext}, topic::TopicManager, connection::Connection};

pub struct WillMessage {
    will_topic: String,
    will_message: Vec<u8>,
    will_qos: u8,
    will_retain: bool,
}

pub enum SessionState {
    Online,
    Offline,
}
pub struct Session {
    // MQTT Will Message
    pub will_message: Option<WillMessage>,

    // MQTT Client Identifier, unique in the tenant
    pub client_identifier: String,

    // Tenant Identifier, unique in the system
    pub tenant_identifier: String,

    // The session subscribed topics
    pub subscription_topics: Vec<String>,

    // The inflight , track all in flights qos packet.
    pub inflight: Inflight,

    // Clean session
    pub clean_session: bool,

    // Session state
    pub session_state: SessionState,
}

impl Session {

    pub async fn handle_rx_inflight_packet(&mut self, packet: &MqttPacketV3) -> Option<MqttPacketV3> {
        match packet {
            MqttPacketV3::Publish(publish_packet) => {
                if publish_packet.fix_header.qos > Some(0) {
                    self.new_rx_qos_state_ctx(&packet).await;
                    let packet = self
                        .inflight
                        .get_current_packet(
                            publish_packet.variable_header.packet_identifier.unwrap(),
                        )
                        .await
                        .unwrap();
                    Some(packet)
                } else {
                    None
                }
            }
            MqttPacketV3::Puback(puback_packet) => {
                self.inflight
                    .next_state(puback_packet.variable_header.packet_identifier)
                    .await;
                if let Some(p) = self
                    .inflight
                    .get_current_packet(puback_packet.variable_header.packet_identifier)
                    .await
                {
                    Some(p)
                } else {
                    None
                }
            }
            MqttPacketV3::Pubrec(pubrec_packet) => {
                self.inflight
                    .next_state(pubrec_packet.variable_header.packet_identifier)
                    .await;
                if let Some(p) = self
                    .inflight
                    .get_current_packet(pubrec_packet.variable_header.packet_identifier)
                    .await
                {
                    Some(p)
                } else {
                    None
                }
            }
            MqttPacketV3::Pubrel(pubrel_packet) => {
                self.inflight
                    .next_state(pubrel_packet.variable_header.packet_identifier)
                    .await;
                if let Some(p) = self
                    .inflight
                    .get_current_packet(pubrel_packet.variable_header.packet_identifier)
                    .await
                {
                    Some(p)
                } else {
                    None
                }
            }
            MqttPacketV3::Pubcomp(pubcomp_packet) => {
                self.inflight
                    .next_state(pubcomp_packet.variable_header.packet_identifier)
                    .await;
                if let Some(p) = self
                    .inflight
                    .get_current_packet(pubcomp_packet.variable_header.packet_identifier)
                    .await
                {
                    Some(p)
                } else {
                    None
                }
            }
            _ => {
                warn!("unhandled packet: {:?}", packet);
                None
            }
        }
    }

    pub async fn handle_tx_inflight_packet(&mut self, packet: &MqttPacketV3) -> Option<MqttPacketV3> {
        match packet {
            MqttPacketV3::Publish(publish_packet) => {
                if publish_packet.fix_header.qos > Some(0) {
                    self.new_tx_qos_state_ctx(&packet).await;
                    let packet = self
                        .inflight
                        .get_current_packet(
                            publish_packet.variable_header.packet_identifier.unwrap(),
                        )
                        .await
                        .unwrap();
                    Some(packet)
                } else {
                    None
                }
            }
            MqttPacketV3::Puback(puback_packet) => {
                self.inflight
                    .next_state(puback_packet.variable_header.packet_identifier)
                    .await;
                if let Some(p) = self
                    .inflight
                    .get_current_packet(puback_packet.variable_header.packet_identifier)
                    .await
                {
                    Some(p)
                } else {
                    None
                }
            }
            MqttPacketV3::Pubrec(pubrec_packet) => {
                self.inflight
                    .next_state(pubrec_packet.variable_header.packet_identifier)
                    .await;
                if let Some(p) = self
                    .inflight
                    .get_current_packet(pubrec_packet.variable_header.packet_identifier)
                    .await
                {
                    Some(p)
                } else {
                    None
                }
            }
            MqttPacketV3::Pubrel(pubrel_packet) => {
                self.inflight
                    .next_state(pubrel_packet.variable_header.packet_identifier)
                    .await;
                if let Some(p) = self
                    .inflight
                    .get_current_packet(pubrel_packet.variable_header.packet_identifier)
                    .await
                {
                    Some(p)
                } else {
                    None
                }
            }
            MqttPacketV3::Pubcomp(pubcomp_packet) => {
                self.inflight
                    .next_state(pubcomp_packet.variable_header.packet_identifier)
                    .await;
                if let Some(p) = self
                    .inflight
                    .get_current_packet(pubcomp_packet.variable_header.packet_identifier)
                    .await
                {
                    Some(p)
                } else {
                    None
                }
            }
            _ => {
                warn!("unhandled packet: {:?}", packet);
                None
            }
        }

    }

    async fn new_tx_qos_state_ctx(&mut self, packet: &MqttPacketV3) {
        if let MqttPacketV3::Publish(_) = packet {
            self.inflight.register_with_tx_packet(packet).await;
        }
    }

    async fn new_rx_qos_state_ctx(&mut self, packet: &MqttPacketV3) {
        if let MqttPacketV3::Publish(_) = packet {
            self.inflight.register_with_rx_packet(packet).await;
        }
    }
}


#[derive(Clone)]
pub enum SessionMessage {
    ForwardFromRouter(MqttPacketV3), //Receive packet from router
    KickOff,                         // Kick off the session
    Stop,
}

pub struct SessionHandle {
    session: Arc<Mutex<Session>>,
    sender: tokio::sync::mpsc::Sender<SessionMessage>,
}

impl SessionHandle {

    pub async fn handle(&mut self, msg: SessionMessage) {
        // if receiver closed, the connection has been closed
        // then only receive the msg which is from router
        if let Err(_) = self.sender.send(msg.clone()).await {
            info!("session receiver has been closed, the connection has shutdown.");
            let sender = Self::run_in_offline(self.session.clone()).await;
            self.sender = sender;
            let _ = self.sender.send(msg).await;
        } 
    }

    pub async fn into_offline(&mut self) {
        let session = self.session.clone();
        let session = session.lock().await;
        if !session.clean_session {
            let _ = self.sender.send(SessionMessage::Stop).await;
            let sender = Self::run_in_offline(self.session.clone()).await;
            self.sender = sender;
        }
    }

    pub async fn into_online(
        &mut self, 
        connection: Connection<TcpStream>,
        plugin_manager: Arc<PluginManager>,
        topic_manager: Arc<RwLock<TopicManager>>,
        keep_alive: u64,
        resend_check: u64,
        router_sender: tokio::sync::mpsc::Sender<RouterCmd>,
        quit_signal: tokio::sync::oneshot::Sender<()>
    ) {
        let _ = self.sender.send(SessionMessage::Stop).await;
        let sender = Self::run_in_online(self.session.clone(), connection, plugin_manager, topic_manager, keep_alive, resend_check, router_sender, quit_signal).await;
        self.sender = sender;
    }

    async fn run_in_offline(session: Arc<Mutex<Session>>) -> tokio::sync::mpsc::Sender<SessionMessage> {
        let (sender, mut receiver) = tokio::sync::mpsc::channel(100);
        {
            let session = session.clone();
            let mut session = session.lock().await;
            session.session_state = SessionState::Offline;
        }
        tokio::spawn(async move {
            loop {
                if let Some(msg) = receiver.recv().await {
                    match msg {
                        SessionMessage::ForwardFromRouter(packet) => {
                            match &packet {
                                MqttPacketV3::Publish(publish_packet) => {
                                    if publish_packet.fix_header.qos > Some(0) {
                                        let mut session = session.lock().await;
                                        session.new_tx_qos_state_ctx(&packet).await;
                                    }
                                }
                                _ => {}
                            }
                        }
                        SessionMessage::Stop => {
                            info!("receive stop message, start close the session message receiver");
                            receiver.close(); // ensure the message in buffer has been processed
                        }
                        _ => {}
                    }
                } else {
                    break;
                }
            }
        });
        sender
    }

    async fn run_in_online(
        session: Arc<Mutex<Session>>,
        mut connection: Connection<TcpStream>,
        plugin_manager: Arc<PluginManager>,
        topic_manager: Arc<RwLock<TopicManager>>,
        keep_alive: u64,
        resend_check: u64,
        router_sender: tokio::sync::mpsc::Sender<RouterCmd>,
        quit_signal: tokio::sync::oneshot::Sender<()>
    ) -> tokio::sync::mpsc::Sender<SessionMessage> {

        {
            let session = session.clone();
            let mut session = session.lock().await;
            session.session_state = SessionState::Online;
        }

        let (sender, mut receiver) = tokio::sync::mpsc::channel(100);

        let session_inner = session.clone();

        let plugin_manager = plugin_manager.clone();
        let topic_manager = topic_manager.clone();
        let router_sender = router_sender.clone();

        tokio::spawn(async move {

            let mut resend_check_interval =
                tokio::time::interval(Duration::from_secs(resend_check));

            let mut keep_alive_interval = tokio::time::interval(Duration::from_secs(keep_alive));

            let mut keep_alive_timeout_flag = false;


            loop {
                select! {
                    session_msg = receiver.recv() => {
                        match session_msg {
                            Some(SessionMessage::ForwardFromRouter(packet)) => { // receive the message from the router
                                let mut session = session_inner.lock().await;
                                connection.write_packet(&packet).await.unwrap();
                                match &packet {
                                    MqttPacketV3::Publish(publish_packet) => {
                                        if publish_packet.fix_header.qos > Some(0) {
                                            session.new_tx_qos_state_ctx(&packet).await;
                                        }
                                    }
                                    _ => {}
                                }
                            }
                            Some(SessionMessage::KickOff) => { // the broker kickoff the client
                                let mut session = session_inner.lock().await;
                                info!("session {} kick off", session.client_identifier);
                                connection.shutdown().await.unwrap();
                                session.session_state = SessionState::Offline;
                                info!("start close the session message receiver");
                                receiver.close();
                            }
                            Some(SessionMessage::Stop) => {
                                receiver.close();
                            }
                            None => {
                                connection.shutdown().await.unwrap();
                                break;
                            }
                        }
                    }
                    read_packet_result = connection.read_packet() => {
                        match &read_packet_result {
                            Ok(packet) => {
                                keep_alive_timeout_flag = false;
                                match &packet {
                                    MqttPacketV3::Publish(publish_packet) => {

                                        let mut session = session_inner.lock().await;

                                        let session_ctx = SessionContext { 
                                            tenant_id: session.tenant_identifier.clone(), 
                                            client_identifier: session.client_identifier.clone(), 
                                            username: "a".to_string(), 
                                            remote_addr: connection.get_stream().peer_addr().unwrap().to_string()
                                        };

                                        if let Err(e) = plugin_manager.call_hook_on_publish(&session_ctx, &publish_packet).await {
                                            warn!("tenant {} session {} call hook on publish error, details: {}", session.tenant_identifier, session.client_identifier, e);
                                        }

                                        let cmd = RouterCmd::RoutePacket(
                                            session.tenant_identifier.clone(),
                                            MqttPacketV3::Publish(publish_packet.clone()),
                                        );

                                        let _ = router_sender.send(cmd).await;

                                        if publish_packet.fix_header.qos > Some(0) {
                                            session.new_rx_qos_state_ctx(packet).await;
                                        }
                                        
                                        let packet = session
                                            .inflight
                                            .get_current_packet(publish_packet.variable_header.packet_identifier.unwrap())
                                            .await
                                            .unwrap();

                                        if let Err(e) = connection.write_packet(&packet).await {
                                            warn!("write packet error: {:?}", e);
                                            break;
                                        }
                                    }
                                    MqttPacketV3::Disconnect(_) => {
                                        let mut session = session_inner.lock().await;
                                        connection.shutdown().await.unwrap();
                                        session.session_state = SessionState::Offline;
                                        info!("start close the session message receiver");
                                        receiver.close();
                                    }
                                    MqttPacketV3::Pingreq(_) => {
                                        connection.write_packet(&MqttPacketV3::Pingresp(PingrespPacket::new())).await.unwrap();
                                    }
                                    MqttPacketV3::Puback(puback_packet) => {
                                        let mut session = session_inner.lock().await;
                                        session.inflight
                                            .next_state(puback_packet.variable_header.packet_identifier)
                                            .await;

                                        if let Some(p) = session
                                            .inflight
                                            .get_current_packet(puback_packet.variable_header.packet_identifier)
                                            .await
                                        {
                                            connection.write_packet(&p).await.unwrap();
                                        }
                                    }
                                    MqttPacketV3::Pubrec(pubrec_packet) => {
                                        let mut session = session_inner.lock().await;
                                        session.inflight
                                            .next_state(pubrec_packet.variable_header.packet_identifier)
                                            .await;
                                        if let Some(p) = session
                                            .inflight
                                            .get_current_packet(pubrec_packet.variable_header.packet_identifier)
                                            .await
                                        {
                                            connection.write_packet(&p).await.unwrap();
                                        }
                                    }
                                    MqttPacketV3::Pubrel(pubrel_packet) => {
                                        let mut session = session_inner.lock().await;
                                        session.inflight
                                            .next_state(pubrel_packet.variable_header.packet_identifier)
                                            .await;
                                        if let Some(p) = session
                                            .inflight
                                            .get_current_packet(pubrel_packet.variable_header.packet_identifier)
                                            .await
                                        {
                                            connection.write_packet(&p).await.unwrap();
                                        }
                                    }
                                    MqttPacketV3::Pubcomp(pubcomp_packet) => {
                                        let mut session = session_inner.lock().await;
                                        session.inflight
                                            .next_state(pubcomp_packet.variable_header.packet_identifier)
                                            .await;
                                        if let Some(p) = session
                                            .inflight
                                            .get_current_packet(pubcomp_packet.variable_header.packet_identifier)
                                            .await
                                        {
                                            connection.write_packet(&p).await.unwrap();
                                        }
                                    }
                                    MqttPacketV3::Subscribe(subscribe_packet) => {
                                        let session = session_inner.lock().await;

                                        let subscriptions = &subscribe_packet.payload.topic_filters;

                                        let packet_identifier = &subscribe_packet.variable_header.packet_identifier;

                                        let mut retain_messages: Vec<Arc<MqttPacketV3>> = vec![];

                                        let session_ctx = SessionContext { 
                                            tenant_id: session.tenant_identifier.clone(), 
                                            client_identifier: session.client_identifier.clone(), 
                                            username: todo!(), 
                                            remote_addr: connection.get_stream().peer_addr().unwrap().to_string()
                                        };

                                        let mut return_code: Vec<crate::protocol::v3::suback::ReturnCode> = vec![];
                                        {
                                            let mut topic_manager = topic_manager.write().await;

                                            for topic in subscriptions.iter() {
                                                let acl_result = plugin_manager.call_hook_on_subscribe_acl_check(&session_ctx, &topic.topic_name, topic.qos.into()).await; 
                                                if let Ok(r) = acl_result {
                                                    if r {
                                                        let sub_result = topic_manager.subscription(
                                                            session.tenant_identifier.clone(),
                                                            session.client_identifier.clone(),
                                                            topic.topic_name.clone(),
                                                            topic.qos,
                                                        );
                                                        if let Ok(_) = sub_result {
                                                            if topic.qos == 0 {
                                                                return_code.push(crate::protocol::v3::suback::ReturnCode::MaxQos0);
                                                            }
                                                            if topic.qos == 1 {
                                                                return_code.push(crate::protocol::v3::suback::ReturnCode::MaxQos1);
                                                            }
                                                            if topic.qos == 2 {
                                                                return_code.push(crate::protocol::v3::suback::ReturnCode::MaxQos2);
                                                            }
                                                            session.subscription_topics.push(topic.topic_name.clone());
                                                            let packets = topic_manager.get_retain_publish_packet(
                                                                session.tenant_identifier.clone(),
                                                                session.client_identifier.clone(),
                                                                topic.topic_name.clone(),
                                                            );
                                                            if let Ok(packets) = packets {
                                                                for packet in packets {
                                                                    retain_messages.push(packet);
                                                                }
                                                            }
                                                        } else {
                                                            warn!("tenant {} session {} subscribe error, details: {}", session.tenant_identifier, session.client_identifier, sub_result.unwrap_err());
                                                            return_code.push(crate::protocol::v3::suback::ReturnCode::Failure);
                                                        }
                                                    } else {
                                                        return_code.push(crate::protocol::v3::suback::ReturnCode::Failure);
                                                    }
                                                } else {
                                                    return_code.push(crate::protocol::v3::suback::ReturnCode::Failure);
                                                    warn!("tenant {} session {} call hook on subscribe error, details: {}", session.tenant_identifier, session.client_identifier, acl_result.unwrap_err());
                                                }
                                            }
                                        }
                                        for packet in retain_messages {
                                            connection.write_packet(&packet).await.unwrap();
                                        }
                                        connection.write_packet(&MqttPacketV3::Suback(SubackPacket::new(*packet_identifier, return_code))).await.unwrap();
                                    }
                                    MqttPacketV3::Unsubscribe(unsubscribe_packet) => {
                                        let session = session_inner.lock().await;

                                        let unsub_topic_filters = &unsubscribe_packet.payload.topic_filters;
                                        {
                                            let mut topic_manager = topic_manager.write().await;
                                            for topic in unsub_topic_filters {
                                                let _ = topic_manager.unsubscription(
                                                    session.tenant_identifier.clone(),
                                                    session.client_identifier.clone(),
                                                    topic.topic_name.clone(),
                                                );
                                            }
                                        }
                                        let unsub_ack =
                                            MqttPacketV3::Unsuback(crate::protocol::v3::unsuback::UnSubackPacket::new(
                                                unsubscribe_packet.variable_header.packet_identifier,
                                            ));
                                        connection.write_packet(&unsub_ack).await.unwrap();
                                    }
                                    _ => {}
                                }
                            },
                            Err(e) => {
                                warn!("read packet error: {:?}", e);
                                connection.shutdown().await.unwrap();
                                info!("start close the session message receiver");
                                receiver.close();
                            },
                        }
                    }
                    _ = keep_alive_interval.tick() => {
                        if keep_alive_timeout_flag {
                            let session = session_inner.lock().await;
                            if session.will_message.is_some() {
                                let will_message = session.will_message.as_ref().unwrap();
                                let publish_packet =
                                    PublishPacketBuilder::new(will_message.will_topic.clone(), will_message.will_message.clone())
                                        .retain(will_message.will_retain)
                                        .qos(will_message.will_qos)
                                        .build();
                                let _ = router_sender.send(RouterCmd::RoutePacket(session.tenant_identifier.clone(),MqttPacketV3::Publish(publish_packet))).await;
                            }
                            if session.clean_session {
                                receiver.close(); // close receiver waiting to consumer buffered message
                            }

                        } else {
                            keep_alive_timeout_flag = true;
                        }
                    }
                    _ = resend_check_interval.tick() => {
                        let session = session_inner.lock().await;
                        let packets = session.inflight.get_all_expired_packets_and_refresh_expired_time().await;
                        for packet in packets {
                            connection.write_packet(&packet).await.unwrap();
                        }
                    }
                }
            }
            quit_signal.send(());
        });
        sender
    }

    pub async fn new(
        session: Session,
        connection: Connection<TcpStream>,
        plugin_manager: Arc<PluginManager>,
        topic_manager: Arc<RwLock<TopicManager>>,
        keep_alive: u64,
        resend_check: u64,
        router_sender: tokio::sync::mpsc::Sender<RouterCmd>,
        quit_signal: tokio::sync::oneshot::Sender<()>
    ) -> Self {
        let session = Arc::new(Mutex::new(session));
        let sender = Self::run_in_online(session.clone(), connection, plugin_manager, topic_manager, keep_alive, resend_check, router_sender, quit_signal).await;
        SessionHandle { session: session.clone(), sender }
    }

}

#[derive(Error, Debug)]
pub enum SessionManagerError {
    #[error("tenant {0} not found")]
    TenantNotExisted(String),

    #[error("tenant {0} has existed")]
    TenantHasExisted(String),

    #[error("session {0} not found")]
    SessionNotExisted(String),
}

pub struct SessionManager {
    session_table: HashMap<String,RwLock<HashMap<String, SessionHandle>>>,
}

impl SessionManager {

    pub async fn create_tenant(&mut self, tenant_identifier: String) -> Result<()> {
        if self.session_table.contains_key(&tenant_identifier) {
            return Err(anyhow!(SessionManagerError::TenantHasExisted(tenant_identifier)));
        }
        self.session_table
            .insert(tenant_identifier, RwLock::new(HashMap::new()));
        Ok(())
    }

    pub async fn remove(&mut self, tenant_identifier: String, client_identifier: String) -> Result<()> {
        if !self.session_table.contains_key(&tenant_identifier) {
            return Err(anyhow!(SessionManagerError::TenantNotExisted(tenant_identifier)));
        } else {
            let mut session_table = self.session_table.get(&tenant_identifier).unwrap().write().await;
            session_table.remove(&client_identifier);
            Ok(())
        }
    }

    pub async fn register(&mut self, tenant_identifier: String, client_identifier: String, session_handle: SessionHandle) -> Result<()> {
        if !self.session_table.contains_key(&tenant_identifier) {
            return Err(anyhow!(SessionManagerError::TenantNotExisted(tenant_identifier)));
        } else {
            let mut session_table = self.session_table.get(&tenant_identifier).unwrap().write().await;
            session_table.insert(client_identifier, session_handle);
            Ok(())
        }
    }

    pub async fn get_session_handle(&mut self, tenant_identifier: String, client_identifier: String) -> Result<SessionHandle> {
        if !self.session_table.contains_key(&tenant_identifier) {
            return Err(anyhow!(SessionManagerError::TenantNotExisted(tenant_identifier)));
        } else {
            let session_table = self.session_table.get(&tenant_identifier).unwrap().read().await;
            if let Some(session_handle) = session_table.get(&client_identifier) {
                Ok(SessionHandle {
                     sender: session_handle.sender.clone(), session: session_handle.session.clone()
                })
            } else {
                Err(anyhow!(SessionManagerError::SessionNotExisted(client_identifier)))
            }
        }
    }

    pub async fn send_packet(
        &self,
        tenant_identifier: String,
        client_identifier: String,
        packet: &MqttPacketV3,
    ) -> Result<()> {
        if !self.session_table.contains_key(&tenant_identifier) {
            return Err(anyhow!(SessionManagerError::TenantNotExisted(tenant_identifier)));
        } else {
            let mut session_table = self.session_table.get(&tenant_identifier).unwrap().write().await;
            if let Some(handle) = session_table.get_mut(&client_identifier) {
                handle.handle(SessionMessage::ForwardFromRouter(packet.clone())).await;
            }
        }
        Ok(())
    }
}

mod tests {

    use std::{sync::Arc, time::Duration, path::PathBuf, io::Write};

    use tokio::{sync::RwLock, io::{AsyncReadExt, AsyncWriteExt}};

    use tokio_test::io::Builder;

    use crate::{
        protocol::{
            v3::{
                publish::PublishPacketBuilder,
            },
            MqttPacketV3,
        },
        topic::TopicManager, plugin::{plugin_manager::PluginManager}, session::Session, inflight::Inflight, session::SessionHandle, connection::{Connection},
    };

    async fn get_test_plugin_manager() -> Arc<PluginManager> {
        let crate_root_path = env!("CARGO_MANIFEST_DIR");
        let plugin_path = PathBuf::from(crate_root_path).join("tests");

        let plugin_manager = PluginManager::new(&plugin_path.to_str().unwrap().to_string())
            .await
            .unwrap();
        Arc::new(plugin_manager)
    }


    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_keep_alive_timeout_without_will_message() {

        let plugin_manager =get_test_plugin_manager().await;

        let keep_live_duration_secs = 5;

        let resend_duration_secs = 10;

        let (router_sender, router_receiver) = tokio::sync::mpsc::channel(10);

        let mut session = Session {
            will_message: None,
            client_identifier: "clinet_a".to_string(),
            tenant_identifier: "tenant_a".to_string(),
            subscription_topics: vec![],
            clean_session: true,
            inflight: Inflight::new(Duration::from_secs(resend_duration_secs)),
            session_state: crate::session::SessionState::Online,
        };

        let listener = tokio::net::TcpListener::bind("127.0.0.1:18088").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let mut writer = tokio::net::TcpStream::connect(addr).await.unwrap();

        let (mut reader, _addr) = listener.accept().await.unwrap();

        let connection = Connection::new(reader);

        let (quit_sender, quit_receiver) = tokio::sync::oneshot::channel();

        let session_handle = SessionHandle::new(
            session,
            connection, 
            plugin_manager,
            Arc::new(RwLock::new(TopicManager::new())),
            keep_live_duration_secs,
            resend_duration_secs,
            router_sender,
            quit_sender
        ).await;

        tokio::time::sleep(Duration::from_secs(keep_live_duration_secs + 2)).await;

        let mut buf = Vec::new();
        let n = writer.read_buf(&mut buf).await.unwrap();
        assert_eq!(n , 0);
        if n == 0 {
            writer.shutdown().await.unwrap();
        }
        tokio::time::sleep(Duration::from_secs(keep_live_duration_secs + 12)).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_qos_1_receive_process() {

        let plugin_manager =get_test_plugin_manager().await;

        let keep_live_duration_secs = 5;

        let resend_duration_secs = 10;

        let (router_sender, router_receiver) = tokio::sync::mpsc::channel(10);

        let mut session = Session {
            will_message: None,
            client_identifier: "clinet_a".to_string(),
            tenant_identifier: "tenant_a".to_string(),
            subscription_topics: vec![],
            clean_session: true,
            inflight: Inflight::new(Duration::from_secs(resend_duration_secs)),
            session_state: crate::session::SessionState::Online,
        };

        let listener = tokio::net::TcpListener::bind("127.0.0.1:18088").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let mut writer = tokio::net::TcpStream::connect(addr).await.unwrap();

        let (mut reader, _addr) = listener.accept().await.unwrap();

        let connection = Connection::new(reader);

        let (quit_sender, quit_receiver) = tokio::sync::oneshot::channel();

        let session_handle = SessionHandle::new(
            session,
            connection, 
            plugin_manager,
            Arc::new(RwLock::new(TopicManager::new())),
            keep_live_duration_secs,
            resend_duration_secs,
            router_sender,
            quit_sender
        ).await;

        let qos_1_publish_packet = PublishPacketBuilder::new("a/b".to_string(), vec![0x01])
            .qos(1)
            .packet_identifier(0x01)
            .build();

        let packet = MqttPacketV3::Publish(qos_1_publish_packet);

        writer.write(&packet.to_bytes().to_vec()).await.unwrap();
        writer.flush().await.unwrap();

        let mut buf = Vec::new();
        let read_bytes = writer.read_buf(&mut buf).await.unwrap();
        if read_bytes == 0 {
            assert!(false)
        } else {
            let packet = crate::protocol::parse(&buf).unwrap().1.1;
            match packet {
                MqttPacketV3::Puback(puback_packet) => {
                    assert_eq!(puback_packet.variable_header.packet_identifier, 0x01);
                }
                _ => assert!(false)
            }
        }


    }
}