use crate::protobuf::raft_service_server::RaftService;
use crate::protobuf::{
    AppendEntriesRequest, AppendEntriesResponse, ErrorCode, ErrorDetail,
    ForceSessionDisconnectRequest, ForceSessionDisconnectResponse, InstallSnapshotRequest,
    InstallSnapshotResponse, RaftType, RoutePacketRequest, RoutePacketResponse, VoteRequest,
    VoteResponse,
};
use crate::raft::raft_manager::RaftManager;
use crate::raft::NodeId;
use crate::router::RouterCmd;
use actix::Recipient;
use log::info;
use std::sync::Arc;
use tokio::sync::mpsc::Sender;

pub struct RaftServiceImpl {
    pub current_node_id: NodeId,

    pub raft_manager: Arc<RaftManager>,

    pub router_sender: Sender<RouterCmd>,

    pub session_manager_actor_force_stop_recipient:
        Recipient<crate::session::session_manager_actor::ForceStop>,

    pub session_manager_actor_force_disconnect_recipient:
        Recipient<crate::session::session_manager_actor::ForceDisconnect>,
}

#[tonic::async_trait]
impl RaftService for RaftServiceImpl {
    async fn get_subscriptions(
        &self,
        request: tonic::Request<crate::protobuf::GetSubscriptionRequest>,
    ) -> Result<tonic::Response<crate::protobuf::GetSubscriptionResponse>, tonic::Status> {
        let request = request.into_inner();

        let res = self
            .raft_manager
            .topic_raft()
            .get_subscriptions_ensure_linearizable(request.tenant_id, request.topic)
            .await;

        match res {
            Ok(subscriptions) => {
                let subscriptions_result =
                    subscriptions
                        .iter()
                        .map(|sub| crate::protobuf::Subscription {
                            qos: sub.qos as u32,
                            node_id: sub.node_id,
                            client_id: sub.client_identifier.clone(),
                        });
                let res = crate::protobuf::GetSubscriptionResponse {
                    success: true,
                    error: None,
                    subscriptions: subscriptions_result.collect(),
                };
                Ok(tonic::Response::new(res))
            }
            Err(e) => {
                let err_res = match e {
                    crate::raft::raft_manager::RaftManagerError::Raft(raft_err) => {
                        let err = raft_err.api_error().unwrap();
                        crate::protobuf::GetSubscriptionResponse {
                            success: false,
                            error: Some(crate::protobuf::ErrorDetail {
                                code: crate::protobuf::ErrorCode::NotLeader as i32,
                                message: err.to_string(),
                                node: self.current_node_id.to_string(),
                            }),
                            subscriptions: vec![],
                        }
                    }
                    _ => crate::protobuf::GetSubscriptionResponse {
                        success: false,
                        error: Some(crate::protobuf::ErrorDetail {
                            code: crate::protobuf::ErrorCode::InternalError as i32,
                            message: e.to_string(),
                            node: self.current_node_id.to_string(),
                        }),
                        subscriptions: vec![],
                    },
                };
                Ok(tonic::Response::new(err_res))
            }
        }
    }

    async fn inflight_get_current_packet(
        &self,
        request: tonic::Request<crate::protobuf::InflightGetCurrentPacketRequest>,
    ) -> Result<tonic::Response<crate::protobuf::InflightGetCurrentPacketResponse>, tonic::Status>
    {
        let request = request.into_inner();

        let res = self
            .raft_manager
            .session_state_raft()
            .inflight_get_current_packet_ensure_linearizable(
                &request.tenant_id,
                &request.client_id,
                request.packet_id as u16,
            )
            .await;
        match res {
            Ok(packet_opt) => {
                let r = packet_opt.and_then(|packet| Some(serde_json::to_string(&packet).unwrap()));

                let res = crate::protobuf::InflightGetCurrentPacketResponse {
                    success: true,
                    packet: r,
                    error: None,
                };

                Ok(tonic::Response::new(res))
            }
            Err(e) => {
                let err_resp = match e {
                    crate::raft::raft_manager::RaftManagerError::Raft(e) => {
                        crate::protobuf::InflightGetCurrentPacketResponse {
                            success: false,
                            error: Some(crate::protobuf::ErrorDetail {
                                code: crate::protobuf::ErrorCode::NotLeader as i32,
                                message: e.api_error().unwrap().to_string(),
                                node: self.current_node_id.to_string(),
                            }),
                            packet: None,
                        }
                    }
                    _ => crate::protobuf::InflightGetCurrentPacketResponse {
                        success: false,
                        error: Some(crate::protobuf::ErrorDetail {
                            code: crate::protobuf::ErrorCode::InternalError as i32,
                            message: e.to_string(),
                            node: self.current_node_id.to_string(),
                        }),
                        packet: None,
                    },
                };

                Ok(tonic::Response::new(err_resp))
            }
        }
    }

    async fn session_state_existed(
        &self,
        request: tonic::Request<crate::protobuf::SessionExistedRequest>,
    ) -> Result<tonic::Response<crate::protobuf::SessionExistedResponse>, tonic::Status> {
        let request = request.into_inner();

        let res = self
            .raft_manager
            .session_state_raft()
            .session_state_exists_ensure_linearizable(&request.tenant_id, &request.client_id)
            .await;

        match res {
            Ok(r) => {
                let res = crate::protobuf::SessionExistedResponse {
                    success: true,
                    error: None,
                    session_existed: r,
                };
                Ok(tonic::Response::new(res))
            }
            Err(e) => {
                let err_resp = match e {
                    crate::raft::raft_manager::RaftManagerError::Raft(e) => {
                        crate::protobuf::SessionExistedResponse {
                            success: false,
                            error: Some(crate::protobuf::ErrorDetail {
                                code: crate::protobuf::ErrorCode::NotLeader as i32,
                                message: e.api_error().unwrap().to_string(),
                                node: self.current_node_id.to_string(),
                            }),
                            session_existed: false,
                        }
                    }
                    _ => crate::protobuf::SessionExistedResponse {
                        success: false,
                        error: Some(crate::protobuf::ErrorDetail {
                            code: crate::protobuf::ErrorCode::InternalError as i32,
                            message: e.to_string(),
                            node: self.current_node_id.to_string(),
                        }),
                        session_existed: false,
                    },
                };
                Ok(tonic::Response::new(err_resp))
            }
        }
    }

    // Consistent get the session state
    async fn get_session_state(
        &self,
        request: tonic::Request<crate::protobuf::GetSessionStateRequest>,
    ) -> Result<tonic::Response<crate::protobuf::GetSessionStateResponse>, tonic::Status> {
        let request = request.into_inner();

        let res = self
            .raft_manager
            .session_state_raft()
            .get_session_state_ensure_linearizable(&request.tenant_id, &request.client_id)
            .await;

        match res {
            Ok(r) => {
                if let Some(session_state) = r {
                    let res = crate::protobuf::GetSessionStateResponse {
                        success: true,
                        error: None,
                        session_state_data: Some(serde_json::to_string(&session_state).unwrap()),
                    };
                    Ok(tonic::Response::new(res))
                } else {
                    let res = crate::protobuf::GetSessionStateResponse {
                        success: true,
                        error: None,
                        session_state_data: None,
                    };
                    Ok(tonic::Response::new(res))
                }
            }
            Err(e) => {
                let err_resp = match e {
                    crate::raft::raft_manager::RaftManagerError::Raft(e) => {
                        crate::protobuf::GetSessionStateResponse {
                            success: false,
                            error: Some(crate::protobuf::ErrorDetail {
                                code: crate::protobuf::ErrorCode::NotLeader as i32,
                                message: e.api_error().unwrap().to_string(),
                                node: self.current_node_id.to_string(),
                            }),
                            session_state_data: None
                        }
                    },
                    _ => crate::protobuf::GetSessionStateResponse {
                        success: false,
                        error: Some(crate::protobuf::ErrorDetail {
                            code: crate::protobuf::ErrorCode::InternalError as i32,
                            message: e.to_string(),
                            node: self.current_node_id.to_string(),
                        }),
                        session_state_data: None
                    },
                };
                Ok(tonic::Response::new(err_resp))
            }
        }
    }

    // Consistent get the session actor map
    async fn get_session_actor_map(
        &self,
        request: tonic::Request<crate::protobuf::GetSessionActorMapRequest>,
    ) -> Result<tonic::Response<crate::protobuf::GetSessionActorMapResponse>, tonic::Status> {
        let ret = self
            .raft_manager
            .session_actor_map_raft()
            .raft()
            .ensure_linearizable()
            .await;
        match ret {
            Ok(_) => {
                let request = request.into_inner();
                let client_id = request.client_id;
                let tenant_id = request.tenant_id;
                let session_actor_map_entry = self
                    .raft_manager
                    .session_actor_map_raft()
                    .get_session_actor_map(&tenant_id, &client_id)
                    .await;

                if let Some(session_actor_map_entry) = session_actor_map_entry {
                    let res = crate::protobuf::GetSessionActorMapResponse {
                        success: true,
                        error: None,
                        node_id: Some(session_actor_map_entry.node_id),
                        version: Some(crate::protobuf::SessionVersion {
                            node_id: session_actor_map_entry.node_id,
                            counter: session_actor_map_entry.version.counter,
                        }),
                    };
                    Ok(tonic::Response::new(res))
                } else {
                    let res = crate::protobuf::GetSessionActorMapResponse {
                        success: true,
                        error: None,
                        node_id: None,
                        version: None,
                    };
                    Ok(tonic::Response::new(res))
                }
            }
            Err(e) => {
                let res = crate::protobuf::GetSessionActorMapResponse {
                    success: false,
                    error: Some(crate::protobuf::ErrorDetail {
                        code: 500,
                        message: e.to_string(),
                        node: self.current_node_id.to_string(),
                    }),
                    node_id: None,
                    version: None,
                };

                Ok(tonic::Response::new(res))
            }
        }
    }

    async fn session_actor_force_stop(
        &self,
        request: tonic::Request<crate::protobuf::SessionActorForceStopRequest>,
    ) -> Result<tonic::Response<crate::protobuf::SessionActorForceStopResponse>, tonic::Status>
    {
        let req = request.into_inner();
        let client_id = req.client_id;
        let tenant_id = req.tenant_id;

        let res = self
            .session_manager_actor_force_stop_recipient
            .send(crate::session::session_manager_actor::ForceStop {
                client_id,
                tenant_id,
            })
            .await;

        if let Err(e) = res {
            let res = crate::protobuf::SessionActorForceStopResponse {
                success: false,
                error: Some(ErrorDetail {
                    code: ErrorCode::InternalError.into(),
                    message: e.to_string(),
                    node: self.current_node_id.to_string(),
                }),
            };
            Ok(tonic::Response::new(res))
        } else {
            let res = crate::protobuf::SessionActorForceStopResponse {
                success: true,
                error: None,
            };
            Ok(tonic::Response::new(res))
        }
    }

    // Foce session disconnect
    async fn session_force_disconnect(
        &self,
        request: tonic::Request<ForceSessionDisconnectRequest>,
    ) -> Result<tonic::Response<ForceSessionDisconnectResponse>, tonic::Status> {
        let req = request.into_inner();
        let client_id = req.client_id;
        let tenant_id = req.tenant_id;

        let res = self
            .session_manager_actor_force_disconnect_recipient
            .send(crate::session::session_manager_actor::ForceDisconnect {
                client_id,
                tenant_id,
            })
            .await;

        if let Err(e) = res {
            let res = ForceSessionDisconnectResponse {
                success: false,
                error: Some(ErrorDetail {
                    code: ErrorCode::InternalError.into(),
                    message: e.to_string(),
                    node: self.current_node_id.to_string(),
                }),
            };
            Ok(tonic::Response::new(res))
        } else {
            let res = ForceSessionDisconnectResponse {
                success: true,
                error: None,
            };
            Ok(tonic::Response::new(res))
        }
    }

    // Receive route packet from other node
    async fn route_packet(
        &self,
        request: tonic::Request<RoutePacketRequest>,
    ) -> Result<tonic::Response<RoutePacketResponse>, tonic::Status> {
        let req = request.into_inner();

        info!("rpc service receive route packet: {}", req.data);

        let router_cmd: RouterCmd =
            serde_json::from_str(&req.data).map_err(|x| tonic::Status::internal(x.to_string()))?;
        info!("router cmd: {:?}", router_cmd);
        let res = self.router_sender.send(router_cmd).await;

        if let Err(e) = res {
            let res = RoutePacketResponse {
                success: false,
                data: "".to_string(),
                error: Some(ErrorDetail {
                    code: ErrorCode::InternalError.into(),
                    message: e.to_string(),
                    node: self.current_node_id.to_string(),
                }),
            };
            Ok(tonic::Response::new(res))
        } else {
            let res = RoutePacketResponse {
                success: true,
                data: "".to_string(),
                error: None,
            };
            Ok(tonic::Response::new(res))
        }
    }

    async fn append_entries(
        &self,
        request: tonic::Request<AppendEntriesRequest>,
    ) -> Result<tonic::Response<AppendEntriesResponse>, tonic::Status> {
        let req = request.into_inner();
        if req.data.contains("vote") {
            let response = match req.raft_type() {
                RaftType::Topic => {
                    let append_req = serde_json::from_str(&req.data)
                        .map_err(|x| tonic::Status::internal(x.to_string()))?;

                    let resp = self
                        .raft_manager
                        .topic_raft()
                        .raft()
                        .append_entries(append_req)
                        .await
                        .map_err(|x| tonic::Status::internal(x.to_string()))?;

                    let data = serde_json::to_string(&resp).expect("fail to serialize resp");
                    let mes = AppendEntriesResponse {
                        success: true,
                        data,
                        error: None,
                    };

                    Ok(tonic::Response::new(mes))
                }
                RaftType::SessionActorMap => {
                    let append_req = serde_json::from_str(&req.data)
                        .map_err(|x| tonic::Status::internal(x.to_string()))?;

                    let resp = self
                        .raft_manager
                        .session_actor_map_raft()
                        .raft()
                        .append_entries(append_req)
                        .await
                        .map_err(|x| tonic::Status::internal(x.to_string()))?;
                    let data = serde_json::to_string(&resp).expect("fail to serialize resp");
                    let mes = AppendEntriesResponse {
                        success: true,
                        data,
                        error: None,
                    };

                    Ok(tonic::Response::new(mes))
                }
                RaftType::SessionState => {
                    let append_req = serde_json::from_str(&req.data)
                        .map_err(|x| tonic::Status::internal(x.to_string()))?;

                    let resp = self
                        .raft_manager
                        .session_state_raft()
                        .raft()
                        .append_entries(append_req)
                        .await
                        .map_err(|x| tonic::Status::internal(x.to_string()))?;

                    let data = serde_json::to_string(&resp).expect("fail to serialize resp");
                    let mes = AppendEntriesResponse {
                        success: true,
                        data,
                        error: None,
                    };

                    Ok(tonic::Response::new(mes))
                }
            };
            response
        } else {
            let response = match req.raft_type() {
                RaftType::Topic => {
                    let append_req = serde_json::from_str(&req.data)
                        .map_err(|x| tonic::Status::internal(x.to_string()))?;

                    let resp = self
                        .raft_manager
                        .topic_raft()
                        .raft()
                        .client_write(append_req)
                        .await
                        .map_err(|x| tonic::Status::internal(x.to_string()))?;

                    let data = serde_json::to_string(&resp.data).expect("fail to serialize resp");
                    let mes = AppendEntriesResponse {
                        success: true,
                        data,
                        error: None,
                    };

                    Ok(tonic::Response::new(mes))
                }
                RaftType::SessionActorMap => {
                    let append_req = serde_json::from_str(&req.data)
                        .map_err(|x| tonic::Status::internal(x.to_string()))?;

                    let resp = self
                        .raft_manager
                        .session_actor_map_raft()
                        .raft()
                        .client_write(append_req)
                        .await
                        .map_err(|x| tonic::Status::internal(x.to_string()))?;
                    let data = serde_json::to_string(&resp.data).expect("fail to serialize resp");
                    let mes = AppendEntriesResponse {
                        success: true,
                        data,
                        error: None,
                    };

                    Ok(tonic::Response::new(mes))
                }
                RaftType::SessionState => {
                    let append_req = serde_json::from_str(&req.data)
                        .map_err(|x| tonic::Status::internal(x.to_string()))?;

                    let resp = self
                        .raft_manager
                        .session_state_raft()
                        .raft()
                        .client_write(append_req)
                        .await
                        .map_err(|x| tonic::Status::internal(x.to_string()))?;

                    let data = serde_json::to_string(&resp.data).expect("fail to serialize resp");
                    let mes = AppendEntriesResponse {
                        success: true,
                        data,
                        error: None,
                    };

                    Ok(tonic::Response::new(mes))
                }
            };
            response
        }
    }

    async fn install_snapshot(
        &self,
        request: tonic::Request<InstallSnapshotRequest>,
    ) -> Result<tonic::Response<InstallSnapshotResponse>, tonic::Status> {
        let req = request.into_inner();

        let resp = match req.raft_type() {
            RaftType::Topic => {
                let install_req = serde_json::from_str(&req.data)
                    .map_err(|x| tonic::Status::internal(x.to_string()))?;

                let resp = self
                    .raft_manager
                    .topic_raft()
                    .raft()
                    .install_snapshot(install_req)
                    .await
                    .map_err(|x| tonic::Status::internal(x.to_string()))?;
                resp
            }
            RaftType::SessionActorMap => {
                let install_req = serde_json::from_str(&req.data)
                    .map_err(|x| tonic::Status::internal(x.to_string()))?;

                let resp = self
                    .raft_manager
                    .session_actor_map_raft()
                    .raft()
                    .install_snapshot(install_req)
                    .await
                    .map_err(|x| tonic::Status::internal(x.to_string()))?;
                resp
            }
            RaftType::SessionState => {
                let install_req = serde_json::from_str(&req.data)
                    .map_err(|x| tonic::Status::internal(x.to_string()))?;

                let resp = self
                    .raft_manager
                    .session_state_raft()
                    .raft()
                    .install_snapshot(install_req)
                    .await
                    .map_err(|x| tonic::Status::internal(x.to_string()))?;
                resp
            }
        };

        let data = serde_json::to_string(&resp).expect("fail to serialize resp");
        let mes = InstallSnapshotResponse {
            success: true,
            data,
            error: None,
        };

        Ok(tonic::Response::new(mes))
    }

    async fn vote(
        &self,
        request: tonic::Request<VoteRequest>,
    ) -> Result<tonic::Response<VoteResponse>, tonic::Status> {
        let req = request.into_inner();

        let resp = match req.raft_type() {
            RaftType::Topic => {
                let vote_req = serde_json::from_str(&req.data)
                    .map_err(|x| tonic::Status::internal(x.to_string()))?;

                let resp = self
                    .raft_manager
                    .topic_raft()
                    .raft()
                    .vote(vote_req)
                    .await
                    .map_err(|x| tonic::Status::internal(x.to_string()))?;
                resp
            }
            RaftType::SessionActorMap => {
                let vote_req = serde_json::from_str(&req.data)
                    .map_err(|x| tonic::Status::internal(x.to_string()))?;

                let resp = self
                    .raft_manager
                    .session_actor_map_raft()
                    .raft()
                    .vote(vote_req)
                    .await
                    .map_err(|x| tonic::Status::internal(x.to_string()))?;
                resp
            }
            RaftType::SessionState => {
                let vote_req = serde_json::from_str(&req.data)
                    .map_err(|x| tonic::Status::internal(x.to_string()))?;

                let resp = self
                    .raft_manager
                    .session_state_raft()
                    .raft()
                    .vote(vote_req)
                    .await
                    .map_err(|x| tonic::Status::internal(x.to_string()))?;
                resp
            }
        };

        let data = serde_json::to_string(&resp).expect("fail to serialize resp");
        let mes = VoteResponse {
            success: true,
            data,
            error: None,
        };

        Ok(tonic::Response::new(mes))
    }
}
