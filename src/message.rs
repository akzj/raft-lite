use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

use crate::{Command, RaftId, RequestId, cluster_config::ClusterConfig};

// === 网络接口 ===
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstallSnapshotRequest {
    pub term: u64,
    pub leader_id: RaftId,
    pub last_included_index: u64,
    pub last_included_term: u64,
    pub data: Vec<u8>,
    pub config: ClusterConfig, // 快照包含的集群配置信息
    pub request_id: RequestId,
    // 空消息标记 - 用于探测安装状态
    pub is_probe: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum InstallSnapshotState {
    Failed(String), // 失败，附带原因
    Installing,     // 正在安装
    Success,        // 成功完成
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstallSnapshotResponse {
    pub term: u64,
    pub request_id: RequestId,
    pub state: InstallSnapshotState,
    pub error_message: String, // 错误信息，如果有的话
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Snapshot {
    pub index: u64,
    pub term: u64,
    pub data: Vec<u8>,
    pub config: ClusterConfig,
}

// 快照探测计划结构
#[derive(Debug, Clone)]
pub struct SnapshotProbeSchedule {
    pub peer: RaftId,
    pub next_probe_time: Instant,
    pub interval: Duration, // 探测间隔
    pub max_attempts: u32,  // 最大尝试次数
    pub attempts: u32,      // 当前尝试次数
}

// === 核心状态与逻辑 ===

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    pub term: u64,
    pub index: u64,
    pub command: Command,
    pub is_config: bool,                      // 标记是否为配置变更日志
    pub client_request_id: Option<RequestId>, // 关联的客户端请求ID，用于去重
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestVoteRequest {
    pub term: u64,
    pub candidate_id: RaftId,
    pub last_log_index: u64,
    pub last_log_term: u64,
    pub request_id: RequestId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestVoteResponse {
    pub term: u64,
    pub vote_granted: bool,
    pub request_id: RequestId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntriesRequest {
    pub term: u64,
    pub leader_id: RaftId,
    pub prev_log_index: u64,
    pub prev_log_term: u64,
    pub entries: Vec<LogEntry>,
    pub leader_commit: u64,
    pub request_id: RequestId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntriesResponse {
    pub term: u64,
    pub success: bool,
    pub conflict_index: Option<u64>,
    pub conflict_term: Option<u64>, // 用于更高效的日志冲突处理
    pub request_id: RequestId,
    pub matched_index: u64, // 用于快速同步
}
