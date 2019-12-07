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

use actix_web::Result;
use futures::{
    future::{self, Either},
    Future, Stream,
};
use hyper::{Client as HyperClient, StatusCode, Uri};
use serde_json::Value;
use splinter::node_registry::Node;
use tokio::runtime::Runtime;

use crate::error::{ConfigurationError, GetNodeError};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DeploymentConfig {
    tp_name: String,
    tp_version: String,
    tp_prefix: String,
    tp_path: String,
}

impl DeploymentConfig {
    fn from(config_file: Option<String>) -> Result<Self, ConfigurationError> {
        let file = match config_file {
            Some(file_present) => file_present,
            None => return Err(ConfigurationError::MissingValue("Deployment configuration file is missing".to_string())),
        };
        let f = match std::fs::File::open(file) {
            Ok(file) => file,
            Err(err) => return Err(ConfigurationError::MissingValue(format!("Invalid deployment configuration {}", err.to_string()))),
        };
        let resultant: Result<DeploymentConfig, serde_yaml::Error> = serde_yaml::from_reader(f);
        let parsed = match resultant {
            Ok(parsed) => parsed,
            Err(err) => return Err(ConfigurationError::MissingValue(format!("Invalid deployment configuration {}", err.to_string()))),
        };
        Ok(DeploymentConfig {
            tp_name: parsed.tp_name,
            tp_version: parsed.tp_version,
            tp_prefix: parsed.tp_prefix,
            tp_path: parsed.tp_path,
        })
    }

    pub fn tp_name(&self) -> &str {
        &self.tp_name
    }

    pub fn tp_version(&self) -> &str {
        &self.tp_version
    }

    pub fn tp_prefix(&self) -> &str {
        &self.tp_prefix
    }

    pub fn tp_path(&self) -> &str {
        &self.tp_path
    }
}

#[derive(Debug, Clone)]
pub struct EventListenerConfig {
    splinterd_url: String,
    deployment_config: DeploymentConfig,
}

impl EventListenerConfig {
    pub fn splinterd_url(&self) -> &str {
        &self.splinterd_url
    }

    pub fn deployment_config(&self) -> &DeploymentConfig {
        &self.deployment_config
    }
}

pub struct DataReaderConfigBuilder {
    splinterd_url: Option<String>,
    config_file: Option<String>,
}

impl Default for DataReaderConfigBuilder {
    fn default() -> Self {
        Self {
            splinterd_url: Some("http://127.0.0.1:8080".to_owned()),
            config_file: Some("deployment-config.yaml".to_owned()),
        }
    }
}

impl DataReaderConfigBuilder {
    pub fn with_cli_args(&mut self, matches: &clap::ArgMatches<'_>) -> Self {
        Self {
            splinterd_url: matches
                .value_of("splinterd_url")
                .map(ToOwned::to_owned)
                .or_else(|| self.splinterd_url.take()),
            config_file: matches
                .value_of("config")
                .map(ToOwned::to_owned)
                .or_else(|| self.config_file.take()),
        }
    }

    pub fn build(mut self) -> Result<EventListenerConfig, ConfigurationError> {
        Ok(EventListenerConfig {
            splinterd_url: self
                .splinterd_url
                .take()
                .ok_or_else(|| ConfigurationError::MissingValue("splinterd_url".to_owned()))?,
            deployment_config: DeploymentConfig::from(self.config_file.take())?,
        })
    }
}

pub fn get_node(splinterd_url: &str) -> Result<Node, GetNodeError> {
    let mut runtime = Runtime::new()
        .map_err(|err| GetNodeError(format!("Failed to get set up runtime: {}", err)))?;
    let client = HyperClient::new();
    let splinterd_url = splinterd_url.to_owned();
    let uri = format!("{}/status", splinterd_url)
        .parse::<Uri>()
        .map_err(|err| GetNodeError(format!("Failed to get set up request: {}", err)))?;

    runtime.block_on(
        client
            .get(uri)
            .map_err(|err| {
                GetNodeError(format!(
                    "Failed to get splinter node metadata: {}",
                    err
                ))
            })
            .and_then(|resp| {
                if resp.status() != StatusCode::OK {
                    return Err(GetNodeError(format!(
                        "Failed to get splinter node metadata. Splinterd responded with status {}",
                        resp.status()
                    )));
                }
                let body = resp
                    .into_body()
                    .concat2()
                    .wait()
                    .map_err(|err| {
                        GetNodeError(format!(
                            "Failed to get splinter node metadata: {}",
                            err
                        ))
                    })?
                    .to_vec();

                let node_status: Value = serde_json::from_slice(&body).map_err(|err| {
                    GetNodeError(format!(
                        "Failed to get splinter node metadata: {}",
                        err
                    ))
                })?;

                let node_id = match node_status.get("node_id") {
                    Some(node_id_val) => node_id_val.as_str().unwrap_or("").to_string(),
                    None => "".to_string(),
                };

                Ok(node_id)
            })
            .and_then(move |node_id| {
                let uri = match format!("{}/nodes/{}", splinterd_url, node_id).parse::<Uri>() {
                        Ok(uri) => uri,
                        Err(err) => return
                            Either::A(
                                future::err(GetNodeError(format!(
                                    "Failed to get set up request : {}",
                                    err
                                ))))
                };

                Either::B(client
                    .get(uri)
                    .map_err(|err| {
                        GetNodeError(format!(
                            "Failed to get splinter node: {}",
                            err
                        ))
                    })
                    .then(|resp| {
                        let response = resp?;
                        let status = response.status();
                        let body = response
                            .into_body()
                            .concat2()
                            .wait()
                            .map_err(|err| {
                                GetNodeError(format!(
                                    "Failed to get splinter node metadata: {}",
                                    err
                                ))
                            })?
                            .to_vec();

                        match status {
                            StatusCode::OK => {
                                let node: Node = serde_json::from_slice(&body).map_err(|err| {
                                    GetNodeError(format!(
                                        "Failed to get splinter node: {}",
                                        err
                                    ))
                                })?;

                                Ok(node)
                            }
                            _ => Err(GetNodeError(format!(
                                "Failed to get splinter node data. Splinterd responded with status {}",
                                status
                            ))),
                        }
                    }))
            }),
    )
}
