use crate::rpc::proto::rpc_server::Rpc;
use crate::{StoreCommand, StoreServer, StoreTransport};
use async_mutex::Mutex;
use async_trait::async_trait;
use crossbeam::queue::ArrayQueue;
use derivative::Derivative;
use omnipaxos_core::messages::{
    AcceptDecide, AcceptStopSign, AcceptSync, Accepted, 
    AcceptedStopSign, Decide, DecideStopSign, FirstAccept, 
    Message, Prepare, Promise, Compaction, PaxosMsg
};
use omnipaxos_core::ballot_leader_election::messages::{
    BLEMessage, HeartbeatMsg, 
    HeartbeatRequest, HeartbeatReply
};
use omnipaxos_core::ballot_leader_election::Ballot;
use omnipaxos_core::util::SyncItem;
use omnipaxos_core::storage::{SnapshotType, StopSign};
use std::collections::HashMap;
use std::sync::Arc;
use std::marker::PhantomData;
use tonic::{Request, Response, Status};

#[allow(missing_docs)]
pub mod proto {
    tonic::include_proto!("proto");
}

use proto::rpc_client::RpcClient;
use crate::rpc::proto::*;  
type NodeAddrFn = dyn Fn(usize) -> String + Send + Sync;

#[derive(Debug)]
struct ConnectionPool {
    connections: ArrayQueue<RpcClient<tonic::transport::Channel>>,
}

struct Connection {
    conn: RpcClient<tonic::transport::Channel>,
    pool: Arc<ConnectionPool>,
}

impl Drop for Connection {
    fn drop(&mut self) {
        self.pool.replenish(self.conn.clone())
    }
}

impl ConnectionPool {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            connections: ArrayQueue::new(16),
        })
    }

    async fn connection<S: ToString>(&self, addr: S) -> RpcClient<tonic::transport::Channel> {
        let addr = addr.to_string();
        match self.connections.pop() {
            Some(x) => x,
            None => RpcClient::connect(addr).await.unwrap(),
        }
    }

    fn replenish(&self, conn: RpcClient<tonic::transport::Channel>) {
        let _ = self.connections.push(conn);
    }
}

#[derive(Debug, Clone)]
struct Connections(Arc<Mutex<HashMap<String, Arc<ConnectionPool>>>>);

impl Connections {
    fn new() -> Self {
        Self(Arc::new(Mutex::new(HashMap::new())))
    }

    async fn connection<S: ToString>(&self, addr: S) -> Connection {
        let mut conns = self.0.lock().await;
        let addr = addr.to_string();
        let pool = conns
            .entry(addr.clone())
            .or_insert_with(ConnectionPool::new);
        Connection {
            conn: pool.connection(addr).await,
            pool: pool.clone(),
        }
    }
}

#[derive(Derivative)]
#[derivative(Debug)]
pub struct RpcTransport {
    #[derivative(Debug = "ignore")]
    node_addr: Box<NodeAddrFn>,
    connections: Connections,
}

impl RpcTransport {
    pub fn new(node_addr: Box<NodeAddrFn>) -> Self {
        RpcTransport {
            node_addr,
            connections: Connections::new(),
        }
    }
}

#[async_trait]
impl StoreTransport for RpcTransport {
    fn send_seqpaxos(&self, to_id: u64, msg: Message<StoreCommand, ()>) {
        let message = sp_message_to_proto_definition(msg.clone());
        let peer = (self.node_addr)(to_id as usize);
        let pool = self.connections.clone();

        tokio::task::spawn(async move {
            let mut client = pool.connection(peer).await;
            let request = tonic::Request::new(message);
            client.conn.spmessage(request).await.unwrap();
        });
    }

    fn send_ble(&self, to_id: u64, msg: BLEMessage) {
        let message = ble_message_to_proto_definition(msg.clone());
        let peer = (self.node_addr)(to_id as usize);
        let pool = self.connections.clone();
        tokio::task::spawn(async move {
            let mut client = pool.connection(peer).await;
            let request = tonic::Request::new(message);
            client.conn.blemessage(request).await.unwrap();
        });
    }
    
}

#[derive(Debug)]
pub struct RpcService {
    pub server: Arc<StoreServer<RpcTransport>>,
}

impl RpcService {
    pub fn new(server: Arc<StoreServer<RpcTransport>>) -> Self {
        Self { server }
    }
}

#[tonic::async_trait]
impl Rpc for RpcService {
    async fn execute(
        &self,
        request: Request<Query>,
    ) -> Result<Response<QueryResults>, tonic::Status> {
        let query = request.into_inner();
        let server = self.server.clone();
        let results = match server.query(query.sql).await {
            Ok(results) => results,
            Err(e) => return Err(Status::internal(format!("{}", e))),
        };
        let mut rows = vec![];
        for row in results.rows {
            rows.push(QueryRow {
                values: row.values.clone(),
            })
        }
        Ok(Response::new(QueryResults { rows }))
    }

    async fn spmessage(&self, request: Request<PaxosMsgRpc>) -> Result<Response<Void>, tonic::Status> {
        let message = sp_message_from_proto(request.into_inner());
        let server = self.server.clone();
        server.handle_sp_msg(message);
        Ok(Response::new(Void {}))
    }

    async fn blemessage(&self, request: Request<BleMessageRpc>) -> Result<Response<Void>, tonic::Status> {
        let message = ble_message_from_proto(request.into_inner());
        let server = self.server.clone();
        server.handle_ble_msg(message);
        Ok(Response::new(Void {}))
    }

}

fn sp_message_to_proto_definition(message: Message<StoreCommand, ()>) -> PaxosMsgRpc {
    PaxosMsgRpc {
        from: message.from,
        to: message.to,
        msg: Some(match message.msg {
            PaxosMsg::PrepareReq => paxos_msg_rpc::Msg::PrepareReq(PrepareReqRpc {}),
            PaxosMsg::Prepare(prepare) => paxos_msg_rpc::Msg::Prepare(prepare_proto(prepare)),
            PaxosMsg::Promise(promise) => paxos_msg_rpc::Msg::Promise(promise_proto(promise)),
            PaxosMsg::AcceptSync(accept_sync) => paxos_msg_rpc::Msg::AcceptSync(accept_sync_proto(accept_sync)),
            PaxosMsg::FirstAccept(first_accept) => paxos_msg_rpc::Msg::FirstAccept(first_accept_proto(first_accept)),
            PaxosMsg::AcceptDecide(accept_decide) => paxos_msg_rpc::Msg::AcceptDecide(accept_decide_proto(accept_decide)),
            PaxosMsg::Accepted(accepted) => paxos_msg_rpc::Msg::Accepted(accepted_proto(accepted)),
            PaxosMsg::Decide(decide) => paxos_msg_rpc::Msg::Decide(decide_proto(decide)),
            PaxosMsg::ProposalForward(proposals) => paxos_msg_rpc::Msg::ProposalForward(proposal_forward_proto(proposals)),
            PaxosMsg::Compaction(compaction) => paxos_msg_rpc::Msg::Compaction(compaction_proto(compaction)),
            PaxosMsg::ForwardCompaction(compaction) => paxos_msg_rpc::Msg::ForwardCompaction(compaction_proto(compaction)),
            PaxosMsg::AcceptStopSign(accept_stop_sign) => paxos_msg_rpc::Msg::AcceptStopSign(accept_stop_sign_proto(accept_stop_sign)),
            PaxosMsg::AcceptedStopSign(accepted_stop_sign) => paxos_msg_rpc::Msg::AcceptedStopSign(accepted_stop_sign_proto(accepted_stop_sign)),
            PaxosMsg::DecideStopSign(decide_stop_sign) => paxos_msg_rpc::Msg::DecideStopSign(decide_stop_sign_proto(decide_stop_sign)),
        })
    }
}


fn ble_message_to_proto_definition(message: BLEMessage) -> BleMessageRpc {
    BleMessageRpc {
        from: message.from,
        to: message.to,
        msg: Some(match message.msg {
            HeartbeatMsg::Request(request) => ble_message_rpc::Msg::HeartbeatReq(heartbeat_request_proto(request)),
            HeartbeatMsg::Reply(reply) => ble_message_rpc::Msg::HeartbeatRep(heartbeat_reply_proto(reply)),
        })
    }
}

fn ballot_proto(ballot: Ballot) -> BallotRpc {
    BallotRpc {
        n: ballot.n,
        priority: ballot.priority,
        pid: ballot.pid,
    }
}


fn prepare_proto(prepare: Prepare) -> PrepareRpc {
    PrepareRpc {
        n: Some(ballot_proto(prepare.n)),
        ld: prepare.ld,
        n_accepted: Some(ballot_proto(prepare.n_accepted)),
        la: prepare.la,
    }
}

fn store_command_proto(cmd: StoreCommand) -> StoreCommandRpc {
    StoreCommandRpc {
        id: cmd.id as u64,
        sql: cmd.sql.clone()
    }
}

fn sync_item_proto(sync_item: SyncItem<StoreCommand, ()>) -> SyncItemRpc {
    SyncItemRpc {
        item: Some(match sync_item {
            SyncItem::Entries(vec) => sync_item_rpc::Item::Entries(sync_item_rpc::Entries { vec: vec.into_iter().map(|e| store_command_proto(e)).collect() }),
            SyncItem::Snapshot(ss) => match ss {
                SnapshotType::Complete(_) => sync_item_rpc::Item::Snapshot(sync_item_rpc::Snapshot::Complete as i32),
                SnapshotType::Delta(_) => sync_item_rpc::Item::Snapshot(sync_item_rpc::Snapshot::Delta as i32),
                SnapshotType::_Phantom(_) => sync_item_rpc::Item::Snapshot(sync_item_rpc::Snapshot::Phantom as i32),
            },
            SyncItem::None => sync_item_rpc::Item::None(sync_item_rpc::None {}),
        }),
    }
}

fn stop_sign_proto(stop_sign: StopSign) -> StopSignObject {
    StopSignObject {
        config_id: stop_sign.config_id,
        nodes: stop_sign.nodes,
        metadata: match stop_sign.metadata {
            Some(vec) => Some(stop_sign_object::Metadata { vec: vec.into_iter().map(|m| m as u32).collect() }),
            None => None,
        }
    }
}

fn promise_proto(promise: Promise<StoreCommand, ()>) -> PromiseRpc {
    PromiseRpc {
        n: Some(ballot_proto(promise.n)),
        n_accepted: Some(ballot_proto(promise.n_accepted)),
        sync_item: match promise.sync_item {
            Some(s) => Some(sync_item_proto(s)),
            None => None,
        },
        ld: promise.ld,
        la: promise.la,
        stopsign: match promise.stopsign {
            Some(ss) => Some(stop_sign_proto(ss)),
            None => None,
        },
    }
}

fn accept_sync_proto(accept_sync: AcceptSync<StoreCommand, ()>) -> AcceptSyncRpc {
    AcceptSyncRpc {
        n: Some(ballot_proto(accept_sync.n)),
        sync_item: Some(sync_item_proto(accept_sync.sync_item)),
        sync_idx: accept_sync.sync_idx,
        decide_idx: accept_sync.decide_idx,
        stopsign: match accept_sync.stopsign {
            Some(ss) => Some(stop_sign_proto(ss)),
            None => None,
        },
    }
}

fn first_accept_proto(first_accept: FirstAccept<StoreCommand>) -> FirstAcceptRpc {
    FirstAcceptRpc {
        n: Some(ballot_proto(first_accept.n)),
        entries: first_accept.entries.into_iter().map(|e| store_command_proto(e)).collect(),
    }
}

fn accept_decide_proto(accept_decide: AcceptDecide<StoreCommand>) -> AcceptDecideRpc {
    AcceptDecideRpc {
        n: Some(ballot_proto(accept_decide.n)),
        ld: accept_decide.ld,
        entries: accept_decide.entries.into_iter().map(|e| store_command_proto(e)).collect(),
    }
}

fn accepted_proto(accepted: Accepted) -> AcceptedRpc {
    AcceptedRpc {
        n: Some(ballot_proto(accepted.n)),
        la: accepted.la,
    }
}

fn decide_proto(decide: Decide) -> DecideRpc {
    DecideRpc {
        n: Some(ballot_proto(decide.n)),
        ld: decide.ld,
    }
}

fn proposal_forward_proto(proposals: Vec<StoreCommand>) -> ProposalForwardRpc {
    ProposalForwardRpc {
        entries: proposals.into_iter().map(|e| store_command_proto(e)).collect(),
    }
}

fn compaction_proto(compaction: Compaction) -> CompactionRpc {
    CompactionRpc {
        compaction: Some(match compaction {
            Compaction::Trim(trim) => compaction_rpc::Compaction::Trim(compaction_rpc::Trim { trim }),
            Compaction::Snapshot(ss) => compaction_rpc::Compaction::Snapshot(ss),
        }),
    }
}

fn accept_stop_sign_proto(accept_stop_sign: AcceptStopSign) -> AcceptStopSignRpc {
    AcceptStopSignRpc {
        n: Some(ballot_proto(accept_stop_sign.n)),
        ss: Some(stop_sign_proto(accept_stop_sign.ss)),
    }
}

fn accepted_stop_sign_proto(accepted_stop_sign: AcceptedStopSign) -> AcceptedStopSignRpc {
    AcceptedStopSignRpc {
        n: Some(ballot_proto(accepted_stop_sign.n)),
    }
}

fn decide_stop_sign_proto(decide_stop_sign: DecideStopSign) -> DecideStopSignRpc {
    DecideStopSignRpc {
        n: Some(ballot_proto(decide_stop_sign.n)),
    }
}


fn heartbeat_request_proto(heartbeat_request: HeartbeatRequest) -> HeartbeatRequestRpc {
    HeartbeatRequestRpc {
        round: heartbeat_request.round,
    }
}

fn heartbeat_reply_proto(heartbeat_reply: HeartbeatReply) -> HeartbeatReplyRpc {
    HeartbeatReplyRpc {
        round: heartbeat_reply.round,
        ballot: Some(ballot_proto(heartbeat_reply.ballot)),
        majority_connected: heartbeat_reply.majority_connected,
    }
}

fn sp_message_from_proto(obj: PaxosMsgRpc) -> Message<StoreCommand, ()> {
    Message {
        from: obj.from,
        to: obj.to,
        msg: match obj.msg.unwrap() {
            paxos_msg_rpc::Msg::PrepareReq(_) => PaxosMsg::PrepareReq,
            paxos_msg_rpc::Msg::Prepare(prepare) => PaxosMsg::Prepare(prepare_from_proto(prepare)),
            paxos_msg_rpc::Msg::Promise(promise) => PaxosMsg::Promise(promise_from_proto(promise)),
            paxos_msg_rpc::Msg::AcceptSync(accept_sync) => PaxosMsg::AcceptSync(accept_sync_from_proto(accept_sync)),
            paxos_msg_rpc::Msg::FirstAccept(first_accept) => PaxosMsg::FirstAccept(first_accept_from_proto(first_accept)),
            paxos_msg_rpc::Msg::AcceptDecide(accept_decide) => PaxosMsg::AcceptDecide(accept_decide_from_proto(accept_decide)),
            paxos_msg_rpc::Msg::Accepted(accepted) => PaxosMsg::Accepted(accepted_from_proto(accepted)),
            paxos_msg_rpc::Msg::Decide(decide) => PaxosMsg::Decide(decide_from_proto(decide)),
            paxos_msg_rpc::Msg::ProposalForward(proposals) => PaxosMsg::ProposalForward(proposal_forward_from_proto(proposals)),
            paxos_msg_rpc::Msg::Compaction(compaction) => PaxosMsg::Compaction(compaction_from_proto(compaction)),
            paxos_msg_rpc::Msg::ForwardCompaction(compaction) => PaxosMsg::ForwardCompaction(compaction_from_proto(compaction)),
            paxos_msg_rpc::Msg::AcceptStopSign(accept_stop_sign) => PaxosMsg::AcceptStopSign(accept_stop_sign_from_proto(accept_stop_sign)),
            paxos_msg_rpc::Msg::AcceptedStopSign(accepted_stop_sign) => PaxosMsg::AcceptedStopSign(accepted_stop_sign_from_proto(accepted_stop_sign)),
            paxos_msg_rpc::Msg::DecideStopSign(decide_stop_sign) => PaxosMsg::DecideStopSign(decide_stop_sign_from_proto(decide_stop_sign)),
        }
    }
}

fn ble_message_from_proto(obj: BleMessageRpc) -> BLEMessage {
    BLEMessage {
        from: obj.from,
        to: obj.to,
        msg: match obj.msg.unwrap() {
            ble_message_rpc::Msg::HeartbeatReq(request) => HeartbeatMsg::Request(heartbeat_request_from_proto(request)),
            ble_message_rpc::Msg::HeartbeatRep(reply) => HeartbeatMsg::Reply(heartbeat_reply_from_proto(reply)),
        }
    }
}

fn ballot_from_proto(obj: BallotRpc) -> Ballot {
    Ballot {
        n: obj.n,
        priority: obj.priority,
        pid: obj.pid,
    }
}

fn prepare_from_proto(obj: PrepareRpc) -> Prepare {
    Prepare {
        n: ballot_from_proto(obj.n.unwrap()),
        ld: obj.ld,
        n_accepted: ballot_from_proto(obj.n_accepted.unwrap()),
        la: obj.la,
    }
}

fn store_command_from_proto(obj: StoreCommandRpc) -> StoreCommand {
    StoreCommand {
        id: obj.id as usize,
        sql: obj.sql.clone()
    }
}

fn sync_item_from_proto(obj: SyncItemRpc) -> SyncItem<StoreCommand, ()> {
    match obj.item.unwrap() {
        sync_item_rpc::Item::Entries(entries) => SyncItem::Entries(entries.vec.into_iter().map(|e| store_command_from_proto(e)).collect()),
        sync_item_rpc::Item::Snapshot(ss) => match sync_item_rpc::Snapshot::from_i32(ss) {
            Some(sync_item_rpc::Snapshot::Complete) => SyncItem::Snapshot(SnapshotType::Complete(())),
            Some(sync_item_rpc::Snapshot::Delta) => SyncItem::Snapshot(SnapshotType::Delta(())),
            Some(sync_item_rpc::Snapshot::Phantom) => SyncItem::Snapshot(SnapshotType::_Phantom(PhantomData)),
            _ => unimplemented!() // todo: verify
        },
        sync_item_rpc::Item::None(_) => SyncItem::None,

    }
}

fn stop_sign_from_proto(obj: StopSignObject) -> StopSign {
    StopSign {
        config_id: obj.config_id,
        nodes: obj.nodes,
        metadata: match obj.metadata {
            Some(md) => Some(md.vec.into_iter().map(|m| m as u8).collect()),
            None => None,
        },
    }
}

fn promise_from_proto(obj: PromiseRpc) -> Promise<StoreCommand, ()> {
    Promise {
        n: ballot_from_proto(obj.n.unwrap()),
        n_accepted: ballot_from_proto(obj.n_accepted.unwrap()),
        sync_item: match obj.sync_item {
            Some(s) => Some(sync_item_from_proto(s)),
            None => None,
        },
        ld: obj.ld,
        la: obj.la,
        stopsign: match obj.stopsign {
            Some(ss) => Some(stop_sign_from_proto(ss)),
            None => None,
        },
    }
}

fn accept_sync_from_proto(obj: AcceptSyncRpc) -> AcceptSync<StoreCommand, ()> {
    AcceptSync {
        n: ballot_from_proto(obj.n.unwrap()),
        sync_item: sync_item_from_proto(obj.sync_item.unwrap()),
        sync_idx: obj.sync_idx,
        decide_idx: obj.decide_idx,
        stopsign: match obj.stopsign {
            Some(ss) => Some(stop_sign_from_proto(ss)),
            None => None,
        },
    }
}

fn first_accept_from_proto(obj: FirstAcceptRpc) -> FirstAccept<StoreCommand> {
    FirstAccept {
        n: ballot_from_proto(obj.n.unwrap()),
        entries: obj.entries.into_iter().map(|e| store_command_from_proto(e)).collect(),
    }
}

fn accept_decide_from_proto(obj: AcceptDecideRpc) -> AcceptDecide<StoreCommand> {
    AcceptDecide {
        n: ballot_from_proto(obj.n.unwrap()),
        ld: obj.ld,
        entries: obj.entries.into_iter().map(|e| store_command_from_proto(e)).collect(),
    }
}

fn accepted_from_proto(obj: AcceptedRpc) -> Accepted {
    Accepted {
        n: ballot_from_proto(obj.n.unwrap()),
        la: obj.la,
    }
}

fn decide_from_proto(obj: DecideRpc) -> Decide {
    Decide {
        n: ballot_from_proto(obj.n.unwrap()),
        ld: obj.ld,
    }
}

fn proposal_forward_from_proto(obj: ProposalForwardRpc) -> Vec<StoreCommand> {
    obj.entries.into_iter().map(|e| store_command_from_proto(e)).collect()
}

fn compaction_from_proto(obj: CompactionRpc) -> Compaction {
    match obj.compaction.unwrap() {
        compaction_rpc::Compaction::Trim(trim) => Compaction::Trim(trim.trim),
        compaction_rpc::Compaction::Snapshot(ss) => Compaction::Snapshot(ss),
    }
}

fn accept_stop_sign_from_proto(obj: AcceptStopSignRpc) -> AcceptStopSign {
    AcceptStopSign {
        n: ballot_from_proto(obj.n.unwrap()),
        ss: stop_sign_from_proto(obj.ss.unwrap()),
    }
}

fn accepted_stop_sign_from_proto(obj: AcceptedStopSignRpc) -> AcceptedStopSign {
    AcceptedStopSign {
        n: ballot_from_proto(obj.n.unwrap()),
    }
}

fn decide_stop_sign_from_proto(obj: DecideStopSignRpc) -> DecideStopSign {
    DecideStopSign {
        n: ballot_from_proto(obj.n.unwrap()),
    }
}

fn heartbeat_request_from_proto(obj: HeartbeatRequestRpc) -> HeartbeatRequest {
    HeartbeatRequest {
        round: obj.round,
    }
}

fn heartbeat_reply_from_proto(obj: HeartbeatReplyRpc) -> HeartbeatReply {
    HeartbeatReply {
        round: obj.round,
        ballot: ballot_from_proto(obj.ballot.unwrap()),
        majority_connected: obj.majority_connected,
    }
}