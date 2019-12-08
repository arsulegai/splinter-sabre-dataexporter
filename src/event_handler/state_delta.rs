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

use std::{error::Error, fmt, time::SystemTime};
use splinter::service::scabbard::StateChangeEvent;
use crate::config::EventListenerConfig;
use kafka::producer::{Producer, RequiredAcks, Record};
use crate::proto::pubsub::{Message, Message_MessageType, CircuitCreated, CircuitPayload};
use protobuf::Message as Msg;
use std::time::Duration;

pub struct SabreProcessor {
    circuit_id: String,
    node_id: String,
    requester: String,
    contract_address: String,
    config: EventListenerConfig,
}

impl SabreProcessor {
    pub fn new(circuit_id: &str, node_id: &str, requester: &str, config: EventListenerConfig) -> Self {
        SabreProcessor {
            circuit_id: circuit_id.into(),
            node_id: node_id.to_string(),
            requester: requester.to_string(),
            contract_address: config.deployment_config().tp_prefix().to_string(),
            config,
        }
    }

    pub fn handle_state_changes(
        &self,
        changes: Vec<StateChangeEvent>,
    ) -> Result<(), StateDeltaError> {
        changes
            .iter()
            .try_for_each(|change| self.handle_state_change(change))
    }

    fn handle_state_change(&self, change: &StateChangeEvent) -> Result<(), StateDeltaError> {

        let mut producer =
            match Producer::from_hosts(vec!(self.config.deployment_config().kafka_url().to_string()))
                .with_ack_timeout(Duration::from_secs(5))
                .with_required_acks(RequiredAcks::One)
                .create() {
                Ok(created) => created,
                Err(err) => return Err(StateDeltaError::SDError(err.to_string())),
            };
        debug!("Received state change: {}", change);
        let topic = self.config.deployment_config().kafka_topic().to_string();
        match change {
            StateChangeEvent::Set { key, .. } if key == &self.contract_address => {
                debug!("TP contract created successfully");
                let time = SystemTime::now();
                let mut circuit_created = CircuitCreated::new();
                circuit_created.set_requester(self.requester.clone());
                circuit_created.set_requester_node_id(self.node_id.clone());
                circuit_created.set_circuit_id(self.circuit_id.clone());
                let message_bytes = match circuit_created.write_to_bytes() {
                    Ok(bytes) => bytes,
                    Err(err) => return Err(StateDeltaError::SDError(err.to_string())),
                };
                let mut message = Message::new();
                message.set_field_type(Message_MessageType::CIRCUIT_CREATED);
                message.set_message(message_bytes);
                let to_send_bytes = match message.write_to_bytes() {
                    Ok(bytes) => bytes,
                    Err(err) => return Err(StateDeltaError::SDError(err.to_string())),
                };
                match producer.send(&Record::from_value(&topic, to_send_bytes)) {
                    Ok(_) => info!("Wrote to Kafka about Circuit Created"),
                    Err(err) => return Err(StateDeltaError::SDError(err.to_string())),
                }
                Ok(())
            }
            StateChangeEvent::Set { key, value } if &key[..6] == self.config.deployment_config().tp_prefix() => {
                let time = SystemTime::now();
                let mut circuit_payload = CircuitPayload::new();
                circuit_payload.set_requester(self.requester.clone());
                circuit_payload.set_requester_node_id(self.node_id.clone());
                circuit_payload.set_circuit_id(self.circuit_id.clone());
                circuit_payload.set_data(value.to_vec());
                let message_bytes = match circuit_payload.write_to_bytes() {
                    Ok(bytes) => bytes,
                    Err(err) => return Err(StateDeltaError::SDError(err.to_string())),
                };
                let mut message = Message::new();
                message.set_field_type(Message_MessageType::CIRCUIT_PAYLOAD);
                message.set_message(message_bytes);
                let to_send_bytes = match message.write_to_bytes() {
                    Ok(bytes) => bytes,
                    Err(err) => return Err(StateDeltaError::SDError(err.to_string())),
                };
                match producer.send(&Record::from_value(&topic, to_send_bytes)) {
                    Ok(_) => info!("Wrote to Kafka about Circuit Payload"),
                    Err(err) => return Err(StateDeltaError::SDError(err.to_string())),
                }
                Ok(())
            }
            StateChangeEvent::Delete { .. } => {
                debug!("Delete state skipping...");
                Ok(())
            }
            _ => {
                debug!("Unrecognized state change skipping...");
                Ok(())
            }
        }
    }
}

#[derive(Debug)]
pub enum StateDeltaError {
    SDError(String),
}

impl Error for StateDeltaError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            StateDeltaError::SDError(_) => None,
        }
    }
}

impl fmt::Display for StateDeltaError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            StateDeltaError::SDError(err) => {
                write!(f, "Failed to parse tp payload: {}", err)
            }
        }
    }
}
