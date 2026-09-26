use std::net::IpAddr;
use tonic::{Request, Response, Status};

use crate::backend::Backend;
use crate::proto::etcdserverpb::cluster_server::Cluster;
use crate::proto::etcdserverpb::*;

use super::KvBridge;

/// Virtual interface prefixes to exclude when looking for a physical IP.
const VIRTUAL_IFACE_PREFIXES: &[&str] = &[
    "tap", "veth", "br-", "docker", "virbr", "vnet", "tun", "wg", "dummy", "bond",
];

/// Get a suitable local IP address for client connections.
///
/// If the listen address is not 0.0.0.0, returns that address.
/// Otherwise, finds the first physical (non-virtual) interface with an IPv4 address.
pub fn get_client_bind_address(listen_addr: &std::net::SocketAddr) -> Option<String> {
    // If listening on a specific address, use that
    if !listen_addr.ip().is_unspecified() {
        return Some(format!("{}:{}", listen_addr.ip(), listen_addr.port()));
    }

    // Listening on 0.0.0.0 - find a physical interface
    let interfaces = local_ip_address::list_afinet_netifas()
        .ok()
        .unwrap_or_default();

    for (name, ip) in interfaces {
        let name = name.to_lowercase();

        // Skip virtual interfaces
        if VIRTUAL_IFACE_PREFIXES.iter().any(|p| name.starts_with(p)) {
            continue;
        }

        // Skip loopback
        if ip.is_loopback() {
            continue;
        }

        // Prefer IPv4
        if let IpAddr::V4(ipv4) = ip {
            return Some(format!("{}:{}", ipv4, listen_addr.port()));
        }
    }

    // Fallback: try to get local IP
    local_ip_address::local_ip().ok().map(|ip| format!("{}:{}", ip, listen_addr.port()))
}


#[tonic::async_trait]
impl<B: Backend> Cluster for KvBridge<B> {
    async fn member_add(
        &self,
        _request: Request<MemberAddRequest>,
    ) -> Result<Response<MemberAddResponse>, Status> {
        Err(Status::unknown("member add is not supported"))
    }

    async fn member_remove(
        &self,
        _request: Request<MemberRemoveRequest>,
    ) -> Result<Response<MemberRemoveResponse>, Status> {
        Err(Status::unknown("member remove is not supported"))
    }

    async fn member_update(
        &self,
        _request: Request<MemberUpdateRequest>,
    ) -> Result<Response<MemberUpdateResponse>, Status> {
        Err(Status::unknown("member update is not supported"))
    }

    async fn member_list(
        &self,
        _request: Request<MemberListRequest>,
    ) -> Result<Response<MemberListResponse>, Status> {
        // Get the client bind address from the configured listen address.
        // If listening on 0.0.0.0, find a physical interface IP.
        let client_urls = if let Some(bind_ip) = &self.client_bind_address {
            vec![format!("http://{}", bind_ip)]
        } else {
            vec![]
        };

        Ok(Response::new(MemberListResponse {
            header: Some(ResponseHeader::default()),
            members: vec![Member {
                name: "kine".to_string(),
                peer_ur_ls: client_urls.clone(),
                client_ur_ls: client_urls,
                is_learner: false,
                ..Default::default()
            }],
        }))
    }

    async fn member_promote(
        &self,
        _request: Request<MemberPromoteRequest>,
    ) -> Result<Response<MemberPromoteResponse>, Status> {
        Err(Status::unknown("member promote is not supported"))
    }
}
