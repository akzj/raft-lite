use crate::mutl_raft_driver::Network;

use super::RequestId;
use super::{
    AppendEntriesRequest, AppendEntriesResponse, Error, InstallSnapshotRequest,
    InstallSnapshotResponse, NodeId, RequestVoteRequest, RequestVoteResponse,
};
use async_trait::async_trait;
use rand::Rng;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::sync::RwLock;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};

#[derive(Debug)]
pub enum NetEnvelope {
    RequestVote {
        from: NodeId,
        to: NodeId,
        req: RequestVoteRequest,
        tx: oneshot::Sender<RequestVoteResponse>,
    },
    AppendEntries {
        from: NodeId,
        to: NodeId,
        req: AppendEntriesRequest,
        tx: oneshot::Sender<AppendEntriesResponse>,
    },
    InstallSnapshot {
        from: NodeId,
        to: NodeId,
        req: InstallSnapshotRequest,
        tx: oneshot::Sender<InstallSnapshotResponse>,
    },
}

impl NetEnvelope {
    pub fn to(&self) -> &NodeId {
        match self {
            NetEnvelope::RequestVote { to, .. } => to,
            NetEnvelope::AppendEntries { to, .. } => to,
            NetEnvelope::InstallSnapshot { to, .. } => to,
        }
    }
}

#[derive(Clone)]
pub struct MockNetwork {
    // 每个节点一个 consumer channel
    dispatch: Arc<Mutex<HashMap<NodeId, mpsc::UnboundedSender<NetEnvelope>>>>,

    // 全局发送端，内部做延迟/丢包后转发到 dispatch
    ingress: mpsc::UnboundedSender<NetEnvelope>,

    // 配置
    latency: Arc<Mutex<Duration>>,
    drop: Arc<Mutex<f64>>,
}

impl MockNetwork {
    pub fn new() -> Self {
        let (ingress_tx, mut ingress_rx) = mpsc::unbounded_channel::<NetEnvelope>();
        let dispatch = Arc::new(Mutex::new(HashMap::new()));
        let latency = Arc::new(Mutex::new(Duration::from_millis(0)));
        let drop = Arc::new(Mutex::new(0.0));
        let net = Self {
            dispatch: dispatch.clone(),
            ingress: ingress_tx,
            latency: latency.clone(),
            drop: drop.clone(),
        };

        // 后台任务：处理延迟/丢包并路由
        tokio::spawn({
            let dispatch = dispatch.clone();
            async move {
                while let Some(mut env) = ingress_rx.recv().await {
                    let latency = *latency.lock().unwrap();
                    let drop_rate = *drop.lock().unwrap();

                    if rand::random::<f64>() < drop_rate {
                        continue; // 丢包
                    }

                    tokio::spawn({
                        let dispatch = dispatch.clone();
                        async move {
                            if !latency.is_zero() {
                                tokio::time::sleep(latency).await;
                            }

                            let guard = dispatch.lock().unwrap();
                            if let Some(tx) = guard.get(env.to()) {
                                let _ = tx.send(env);
                            }
                        }
                    });
                }
            }
        });

        net
    }

    // 为节点注册接收端（返回 Receiver）
    pub fn attach(&self, id: NodeId) -> mpsc::UnboundedReceiver<NetEnvelope> {
        let (tx, rx) = mpsc::unbounded_channel();
        self.dispatch.lock().unwrap().insert(id, tx);
        rx
    }

    pub fn set_latency(&self, d: Duration) {
        *self.latency.lock().unwrap() = d;
    }
    pub fn set_drop_rate(&self, p: f64) {
        *self.drop.lock().unwrap() = p.clamp(0.0, 1.0);
    }
}

pub struct MockNodeHandle {
    id: NodeId,
    net: MockNetwork,
    rx: Arc<tokio::sync::Mutex<mpsc::UnboundedReceiver<NetEnvelope>>>,
}

impl MockNodeHandle {
    pub fn new(id: NodeId, net: &MockNetwork) -> Self {
        let rx = Arc::new(tokio::sync::Mutex::new(net.attach(id.clone())));
        Self {
            id,
            net: net.clone(),
            rx,
        }
    }

    // 启动后台循环，把收到的请求交给 Raft 回调
    pub fn spawn_handler<F, Fut>(&self, handler: F)
    where
        F: Fn(NetEnvelope) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let rx = self.rx.clone();
        tokio::spawn(async move {
            let mut rx = rx.lock().await;
            while let Some(env) = rx.recv().await {
                handler(env).await;
            }
        });
    }
}

// 实现 Network trait
#[async_trait]
impl Network for MockNodeHandle {
    async fn send_request_vote(
        &self,
        target: NodeId,
        req: RequestVoteRequest,
    ) -> RequestVoteResponse {
        let (tx, rx) = oneshot::channel();
        let request_id = req.request_id;
        let env = NetEnvelope::RequestVote {
            from: self.id.clone(),
            to: target,
            req,
            tx,
        };
        let _ = self.net.ingress.send(env);
        rx.await.unwrap_or_else(|_| RequestVoteResponse {
            term: 0,
            vote_granted: false,
            request_id,
        })
    }

    async fn send_append_entries(
        &self,
        target: NodeId,
        req: AppendEntriesRequest,
    ) -> AppendEntriesResponse {
        let (tx, rx) = oneshot::channel();
        let request_id = req.request_id;
        let env = NetEnvelope::AppendEntries {
            from: self.id.clone(),
            to: target,
            req,
            tx,
        };
        let _ = self.net.ingress.send(env);
        rx.await.unwrap_or_else(|_| AppendEntriesResponse {
            term: 0,
            success: false,
            conflict_index: None,
            conflict_term: None,
            request_id,
        })
    }

    async fn send_install_snapshot(
        &self,
        target: NodeId,
        req: InstallSnapshotRequest,
    ) -> InstallSnapshotResponse {
        let (tx, rx) = oneshot::channel();
        let request_id = req.request_id;
        let env = NetEnvelope::InstallSnapshot {
            from: self.id.clone(),
            to: target,
            req,
            tx,
        };
        let _ = self.net.ingress.send(env);
        rx.await.unwrap_or_else(|_| InstallSnapshotResponse {
            term: 0,
            request_id,
            state: crate::InstallSnapshotState::Success,
        })
    }
}
