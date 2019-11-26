/*
 * Copyright 2019 Cargill Incorporated
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

pub struct SabreProcessor {
    circuit_id: String,
    node_id: String,
    requester: String,
    contract_address: String,
}

impl SabreProcessor {
    pub fn new(circuit_id: &str, node_id: &str, requester: &str) -> Self {
        SabreProcessor {
            circuit_id: circuit_id.into(),
            node_id: node_id.to_string(),
            requester: requester.to_string(),
            contract_address: get_xo_contract_address(),
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
        debug!("Received state change: {}", change);
        match change {
            StateChangeEvent::Set { key, .. } if key == &self.contract_address => {
                debug!("Xo contract created successfully");
                let time = SystemTime::now();
                // TODO: Circuit is created
                Ok(())
            }
            StateChangeEvent::Set { key, value } if &key[..6] == XO_PREFIX => {
                let time = SystemTime::now();
                // TODO: Change event from the deployed smart contract
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
                write!(f, "Failed to parse xo payload: {}", err)
            }
        }
    }
}
