/*
 * Copyright 2019 Cargill Incorporated
 * Copyright 2019 Walmart Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * -----------------------------------------------------------------------------
 */

mod error;
pub use error::EventHandlerError;
pub mod sabre;
mod state_delta;

use std::fmt::Write;
use std::time::{SystemTime, Duration};

use splinter::{
    admin::messages::{
        AdminServiceEvent, CircuitProposal, CreateCircuit, SplinterNode, SplinterService,
    },
    events::{Igniter, WebSocketClient, WebSocketError, WsResponse},
};
use state_delta::SabreProcessor;

use crate::application_metadata::ApplicationMetadata;

use self::sabre::setup_tp;
use db_models::models::{NewConsortiumProposal, NewConsortiumMember, Consortium, NewConsortiumService, NewProposalVoteRecord};
use crate::config::EventListenerConfig;
use kafka::producer::{Producer, RequiredAcks, Record};
use crate::proto::pubsub::{Message, Message_MessageType, ProposalSubmit, ProposalVote, ProposalAccept, ProposalReject, ProposalReady};
use protobuf::Message as Msg;

/// default value if the client should attempt to reconnet if ws connection is lost
const RECONNECT: bool = true;

/// default limit for number of consecutives failed reconnection attempts
const RECONNECT_LIMIT: u64 = 10;

/// default timeout in seconds if no message is received from server
const CONNECTION_TIMEOUT: u64 = 60;

pub fn run(
    config: EventListenerConfig,
    node_id: String,
    private_key: String,
    igniter: Igniter,
) -> Result<(), EventHandlerError> {

    // TODO: Resubscribe to all the earlier circuits
    let mut ws = WebSocketClient::new(
        &format!("{}/ws/admin/register/consortium", config.splinterd_url()),
        move |ctx, event| {
            if let Err(err) = process_admin_event(
                event,
                &node_id,
                &private_key,
                config.clone(),
                ctx.igniter(),
            ) {
                error!("Failed to process admin event: {}", err);
            }
            WsResponse::Empty
        },
    );

    ws.set_reconnect(RECONNECT);
    ws.set_reconnect_limit(RECONNECT_LIMIT);
    ws.set_timeout(CONNECTION_TIMEOUT);

    ws.on_error(move |err, ctx| {
        error!("An error occured while listening for admin events {}", err);
        match err {
            WebSocketError::ParserError { .. } => {
                debug!("Protocol error, closing connection");
                Ok(())
            }
            WebSocketError::ReconnectError(_) => {
                debug!("Failed to reconnect. Closing WebSocket.");
                Ok(())
            }
            _ => {
                debug!("Attempting to restart connection");
                ctx.start_ws()
            }
        }
    });

    igniter.start_ws(&ws).map_err(EventHandlerError::from)
}

fn process_admin_event(
    admin_event: AdminServiceEvent,
    node_id: &str,
    private_key: &str,
    config: EventListenerConfig,
    igniter: Igniter,
) -> Result<(), EventHandlerError> {

    let mut producer =
        match Producer::from_hosts(vec!(config.deployment_config().kafka_url().to_string()))
            .with_ack_timeout(Duration::from_secs(5))
            .with_required_acks(RequiredAcks::One)
            .create() {
            Ok(created) => created,
            Err(err) => return Err(EventHandlerError::InvalidMessageError(err.to_string())),
    };
    let topic = config.deployment_config().kafka_topic().to_string();

    let url = config.splinterd_url();
    match admin_event {
        AdminServiceEvent::ProposalSubmitted(msg_proposal) => {
            let time = SystemTime::now();

            // convert requester public key to hex
            let requester = to_hex(&msg_proposal.requester);
            let proposal = parse_proposal(&msg_proposal, time, requester.clone());

            let consortium = parse_consortium(&msg_proposal.circuit, time)?;

            let services = parse_splinter_services(
                &msg_proposal.circuit_id,
                &msg_proposal.circuit.roster,
                time,
            );

            let nodes = parse_splinter_nodes(
                &msg_proposal.circuit_id,
                &msg_proposal.circuit.members,
                time,
            );
            let mut proposal_submit = ProposalSubmit::new();
            proposal_submit.set_requester(requester);
            proposal_submit.set_requester_node_id(proposal.requester_node_id.clone());
            proposal_submit.set_circuit_id(proposal.circuit_id.clone());
            let message_bytes = match proposal_submit.write_to_bytes() {
                Ok(bytes) => bytes,
                Err(err) => return Err(EventHandlerError::InvalidMessageError(err.to_string())),
            };
            let mut message = Message::new();
            message.set_field_type(Message_MessageType::PROPOSAL_SUBMIT);
            message.set_message(message_bytes);
            let to_send_bytes = match message.write_to_bytes() {
                Ok(bytes) => bytes,
                Err(err) => return Err(EventHandlerError::InvalidMessageError(err.to_string())),
            };
            match producer.send(&Record::from_value(&topic, to_send_bytes)) {
                Ok(_) => info!("Wrote to Kafka about Proposal Update"),
                Err(err) => return Err(EventHandlerError::InvalidMessageError(err.to_string())),
            }
            Ok(())
        }
        AdminServiceEvent::ProposalVote((msg_proposal, signer_public_key)) => {
//            let proposal = get_pending_proposal_with_circuit_id(&pool, &msg_proposal.circuit_id)?;
            let vote = msg_proposal
                .votes
                .iter()
                .find(|vote| vote.public_key == signer_public_key)
                .ok_or_else(|| {
                    EventHandlerError::InvalidMessageError("Missing vote from signer".to_string())
                })?;
            let proposal_id: i64 = 1234;
            let time = SystemTime::now();
            let vote = NewProposalVoteRecord {
                proposal_id,
                voter_public_key: to_hex(&signer_public_key),
                voter_node_id: vote.voter_node_id.to_string(),
                vote: "Accept".to_string(),
                created_time: time,
            };
            let mut proposal_vote = ProposalVote::new();
            proposal_vote.set_voter(vote.voter_public_key.clone());
            proposal_vote.set_voter_node_id(vote.voter_node_id.clone());
            proposal_vote.set_circuit_id(msg_proposal.circuit_id.clone());
            let message_bytes = match proposal_vote.write_to_bytes() {
                Ok(bytes) => bytes,
                Err(err) => return Err(EventHandlerError::InvalidMessageError(err.to_string())),
            };
            let mut message = Message::new();
            message.set_field_type(Message_MessageType::PROPOSAL_VOTE);
            message.set_message(message_bytes);
            let to_send_bytes = match message.write_to_bytes() {
                Ok(bytes) => bytes,
                Err(err) => return Err(EventHandlerError::InvalidMessageError(err.to_string())),
            };
            match producer.send(&Record::from_value(&topic, to_send_bytes)) {
                Ok(_) => info!("Wrote to Kafka about Proposal Update"),
                Err(err) => return Err(EventHandlerError::InvalidMessageError(err.to_string())),
            }
            Ok(())
        }
        AdminServiceEvent::ProposalAccepted((msg_proposal, signer_public_key)) => {
//            let proposal = get_pending_proposal_with_circuit_id(&pool, &msg_proposal.circuit_id)?;
            let time = SystemTime::now();
            let vote = msg_proposal
                .votes
                .iter()
                .find(|vote| vote.public_key == signer_public_key)
                .ok_or_else(|| {
                    EventHandlerError::InvalidMessageError("Missing vote from signer".to_string())
                })?;

            let proposal_id: i64 = 1234;
            let vote = NewProposalVoteRecord {
                proposal_id,
                voter_public_key: to_hex(&signer_public_key),
                voter_node_id: vote.voter_node_id.to_string(),
                vote: "Accept".to_string(),
                created_time: time,
            };
            let mut proposal_accept = ProposalAccept::new();
            proposal_accept.set_voter(vote.voter_public_key.clone());
            proposal_accept.set_voter_node_id(vote.voter_node_id.clone());
            proposal_accept.set_circuit_id(msg_proposal.circuit_id.clone());
            let message_bytes = match proposal_accept.write_to_bytes() {
                Ok(bytes) => bytes,
                Err(err) => return Err(EventHandlerError::InvalidMessageError(err.to_string())),
            };
            let mut message = Message::new();
            message.set_field_type(Message_MessageType::PROPOSAL_ACCEPT);
            message.set_message(message_bytes);
            let to_send_bytes = match message.write_to_bytes() {
                Ok(bytes) => bytes,
                Err(err) => return Err(EventHandlerError::InvalidMessageError(err.to_string())),
            };
            match producer.send(&Record::from_value(&topic, to_send_bytes)) {
                Ok(_) => info!("Wrote to Kafka about Proposal Update"),
                Err(err) => return Err(EventHandlerError::InvalidMessageError(err.to_string())),
            }
            Ok(())
        }
        AdminServiceEvent::ProposalRejected((msg_proposal, signer_public_key)) => {
//            let proposal = get_pending_proposal_with_circuit_id(&pool, &msg_proposal.circuit_id)?;
            let proposal_id: i64 = 1234;
            let time = SystemTime::now();
            let vote = msg_proposal
                .votes
                .iter()
                .find(|vote| vote.public_key == signer_public_key)
                .ok_or_else(|| {
                    EventHandlerError::InvalidMessageError("Missing vote from signer".to_string())
                })?;

            let vote = NewProposalVoteRecord {
                proposal_id,
                voter_public_key: to_hex(&signer_public_key),
                voter_node_id: vote.voter_node_id.to_string(),
                vote: "Reject".to_string(),
                created_time: time,
            };
            let mut proposal_reject = ProposalReject::new();
            proposal_reject.set_voter(vote.voter_public_key.clone());
            proposal_reject.set_voter_node_id(vote.voter_node_id.clone());
            proposal_reject.set_circuit_id(msg_proposal.circuit_id.clone());
            let message_bytes = match proposal_reject.write_to_bytes() {
                Ok(bytes) => bytes,
                Err(err) => return Err(EventHandlerError::InvalidMessageError(err.to_string())),
            };
            let mut message = Message::new();
            message.set_field_type(Message_MessageType::PROPOSAL_REJECT);
            message.set_message(message_bytes);
            let to_send_bytes = match message.write_to_bytes() {
                Ok(bytes) => bytes,
                Err(err) => return Err(EventHandlerError::InvalidMessageError(err.to_string())),
            };
            match producer.send(&Record::from_value(&topic, to_send_bytes)) {
                Ok(_) => info!("Wrote to Kafka about Proposal Update"),
                Err(err) => return Err(EventHandlerError::InvalidMessageError(err.to_string())),
            }
            Ok(())
        }
        AdminServiceEvent::CircuitReady(msg_proposal) => {

            // Now that the circuit is created, submit the Sabre transactions to run xo
            let service_id = match msg_proposal.circuit.roster.iter().find_map(|service| {
                if service.allowed_nodes.contains(&node_id.to_string()) {
                    Some(service.service_id.clone())
                } else {
                    None
                }
            }) {
                Some(id) => id,
                None => {
                    debug!(
                        "New consortium does not have any services for this node: {}",
                        node_id
                    );
                    return Ok(());
                }
            };
            let scabbard_admin_keys = match serde_json::from_slice::<ApplicationMetadata>(
                msg_proposal.circuit.application_metadata.as_slice(),
            ) {
                Ok(metadata) => metadata.scabbard_admin_keys().to_vec(),
                Err(err) => {
                    return Err(EventHandlerError::InvalidMessageError(format!(
                        "unable to parse application metadata: {}",
                        err
                    )))
                }
            };

            let time = SystemTime::now();
            let requester = to_hex(&msg_proposal.requester);
            let proposal = parse_proposal(&msg_proposal, time, requester.clone());
            let mut proposal_ready = ProposalReady::new();
            proposal_ready.set_requester(requester);
            proposal_ready.set_requester_node_id(proposal.requester_node_id.clone());
            proposal_ready.set_circuit_id(proposal.circuit_id.clone());
            let message_bytes = match proposal_ready.write_to_bytes() {
                Ok(bytes) => bytes,
                Err(err) => return Err(EventHandlerError::InvalidMessageError(err.to_string())),
            };
            let mut message = Message::new();
            message.set_field_type(Message_MessageType::PROPOSAL_READY);
            message.set_message(message_bytes);
            let to_send_bytes = match message.write_to_bytes() {
                Ok(bytes) => bytes,
                Err(err) => return Err(EventHandlerError::InvalidMessageError(err.to_string())),
            };
            match producer.send(&Record::from_value(&topic, to_send_bytes)) {
                Ok(_) => info!("Wrote to Kafka about Proposal Update"),
                Err(err) => return Err(EventHandlerError::InvalidMessageError(err.to_string())),
            }

            let processor = SabreProcessor::new(
                &msg_proposal.circuit_id,
                &proposal.requester_node_id,
                &proposal.requester,
                config.clone(),
            );

            let mut xo_ws = WebSocketClient::new(
                &format!(
                    "{}/scabbard/{}/{}/ws/subscribe",
                    url, msg_proposal.circuit_id, service_id
                ),
                move |_, changes| {
                    if let Err(err) = processor.handle_state_changes(changes) {
                        error!("An error occurred while handling state changes {:?}", err);
                    }
                    WsResponse::Empty
                },
            );

            let url_to_string = url.to_string();
            let private_key_to_string = private_key.to_string();
            xo_ws.on_open(move |ctx| {
                debug!("Starting State Delta Export");
                let future = match setup_tp(
                    &private_key_to_string,
                    scabbard_admin_keys.clone(),
                    &url_to_string,
                    &msg_proposal.circuit_id.clone(),
                    &service_id.clone(),
                    config.clone(),
                ) {
                    Ok(f) => f,
                    Err(err) => {
                        error!("{}", err);
                        return WsResponse::Close;
                    }
                };

                if let Err(err) = ctx.igniter().send(future) {
                    error!("Failed to setup scabbard: {}", err);
                    WsResponse::Close
                } else {
                    WsResponse::Empty
                }
            });
            xo_ws.set_reconnect(RECONNECT);
            xo_ws.set_reconnect_limit(RECONNECT_LIMIT);
            xo_ws.set_timeout(CONNECTION_TIMEOUT);

            xo_ws.on_error(move |err, ctx| {
                error!(
                    "An error occured while listening for scabbard events {}",
                    err
                );
                match err {
                    WebSocketError::ParserError { .. } => {
                        debug!("Protocol error, closing connection");
                        Ok(())
                    }
                    WebSocketError::ReconnectError(_) => {
                        debug!("Failed to reconnect. Closing WebSocket.");
                        Ok(())
                    }
                    _ => {
                        debug!("Attempting to restart connection");
                        ctx.start_ws()
                    }
                }
            });

            igniter.start_ws(&xo_ws).map_err(EventHandlerError::from)
        }
    }
}

fn parse_proposal(
    proposal: &CircuitProposal,
    timestamp: SystemTime,
    requester_public_key: String,
) -> NewConsortiumProposal {
    NewConsortiumProposal {
        proposal_type: format!("{:?}", proposal.proposal_type),
        circuit_id: proposal.circuit_id.clone(),
        circuit_hash: proposal.circuit_hash.to_string(),
        requester: requester_public_key,
        requester_node_id: proposal.requester_node_id.to_string(),
        status: "Pending".to_string(),
        created_time: timestamp,
        updated_time: timestamp,
    }
}

fn parse_consortium(
    circuit: &CreateCircuit,
    timestamp: SystemTime,
) -> Result<Consortium, EventHandlerError> {
    let application_metadata = ApplicationMetadata::from_bytes(&circuit.application_metadata)?;

    Ok(Consortium {
        circuit_id: circuit.circuit_id.clone(),
        authorization_type: format!("{:?}", circuit.authorization_type),
        persistence: format!("{:?}", circuit.persistence),
        durability: format!("{:?}", circuit.durability),
        routes: format!("{:?}", circuit.routes),
        circuit_management_type: circuit.circuit_management_type.clone(),
        alias: application_metadata.alias().to_string(),
        status: "Pending".to_string(),
        created_time: timestamp,
        updated_time: timestamp,
    })
}

fn parse_splinter_services(
    circuit_id: &str,
    splinter_services: &[SplinterService],
    timestamp: SystemTime,
) -> Vec<NewConsortiumService> {
    splinter_services
        .iter()
        .map(|service| NewConsortiumService {
            circuit_id: circuit_id.to_string(),
            service_id: service.service_id.to_string(),
            service_type: service.service_type.to_string(),
            allowed_nodes: service.allowed_nodes.clone(),
            arguments: service
                .arguments
                .clone()
                .iter()
                .map(|(key, value)| {
                    json!({
                        "key": key,
                        "value": value
                    })
                })
                .collect(),
            status: "Pending".to_string(),
            created_time: timestamp,
            updated_time: timestamp,
        })
        .collect()
}

fn parse_splinter_nodes(
    circuit_id: &str,
    splinter_nodes: &[SplinterNode],
    timestamp: SystemTime,
) -> Vec<NewConsortiumMember> {
    splinter_nodes
        .iter()
        .map(|node| NewConsortiumMember {
            circuit_id: circuit_id.to_string(),
            node_id: node.node_id.to_string(),
            endpoint: node.endpoint.to_string(),
            status: "Pending".to_string(),
            created_time: timestamp,
            updated_time: timestamp,
        })
        .collect()
}

pub fn to_hex(bytes: &[u8]) -> String {
    let mut buf = String::new();
    for b in bytes {
        write!(&mut buf, "{:02x}", b).expect("Unable to write to string");
    }

    buf
}
