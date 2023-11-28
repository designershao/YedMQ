use std::{sync::Arc, time::Duration};

use log::warn;
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



pub struct SessionHandle {
    session: Arc<Mutex<Session>>
}

impl SessionHandle {
    pub fn new(
        session: Session,
        mut connection: Connection<TcpStream>,
        plugin_manager: Arc<PluginManager>,
        topic_manager: Arc<RwLock<TopicManager>>,
        keep_alive: u64,
        resend_check: u64,
        router_sender: tokio::sync::mpsc::Sender<RouterCmd>,
    ) -> Self {
        let session = Arc::new(Mutex::new(session));
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
                    read_packet_result = connection.read_packet() => {
                        match &read_packet_result {
                            Ok(packet) => {
                                keep_alive_timeout_flag = false;
                                match &packet {
                                    MqttPacketV3::Publish(publish_packet) => {

                                        let session = session_inner.lock().await;

                                        let session_ctx = SessionContext { 
                                            tenant_id: session.tenant_identifier.clone(), 
                                            client_identifier: session.client_identifier.clone(), 
                                            username: todo!(), 
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
                                        connection.shutdown().await.unwrap();
                                        break;
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
                                break;
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

                        } else {
                            keep_alive_timeout_flag = true;
                        }
                    }
                    _ = resend_check_interval.tick() => {
                    }
                }
            }
        });
        SessionHandle { session }
    }
}
