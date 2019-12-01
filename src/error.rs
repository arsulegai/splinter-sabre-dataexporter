// Copyright 2019 Walmart Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::error::Error;
use std::fmt;

use sawtooth_sdk::signing::Error as KeyGenError;

use crate::authorization_handler::AppAuthHandlerError;

#[derive(Debug)]
pub enum EventListenerError {
    LoggingInitializationError(flexi_logger::FlexiLoggerError),
    ConfigurationError(Box<ConfigurationError>),
    AppAuthHandlerError(AppAuthHandlerError),
    KeyGenError(KeyGenError),
    GetNodeError(GetNodeError),
}

impl Error for EventListenerError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            EventListenerError::LoggingInitializationError(err) => Some(err),
            EventListenerError::ConfigurationError(err) => Some(err),
            EventListenerError::AppAuthHandlerError(err) => Some(err),
            EventListenerError::KeyGenError(err) => Some(err),
            EventListenerError::GetNodeError(err) => Some(err),
        }
    }
}

impl fmt::Display for EventListenerError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            EventListenerError::LoggingInitializationError(e) => {
                write!(f, "Logging initialization error: {}", e)
            }
            EventListenerError::ConfigurationError(e) => write!(f, "Coniguration error: {}", e),
            EventListenerError::AppAuthHandlerError(e) => write!(
                f,
                "The application authorization handler returned an error: {}",
                e
            ),
            EventListenerError::KeyGenError(e) => write!(
                f,
                "an error occurred while generating a new key pair: {}",
                e
            ),
            EventListenerError::GetNodeError(e) => write!(
                f,
                "an error occurred while getting splinterd node information: {}",
                e
            ),
        }
    }
}

impl From<flexi_logger::FlexiLoggerError> for EventListenerError {
    fn from(err: flexi_logger::FlexiLoggerError) -> EventListenerError {
        EventListenerError::LoggingInitializationError(err)
    }
}

impl From<AppAuthHandlerError> for EventListenerError {
    fn from(err: AppAuthHandlerError) -> EventListenerError {
        EventListenerError::AppAuthHandlerError(err)
    }
}

impl From<KeyGenError> for EventListenerError {
    fn from(err: KeyGenError) -> EventListenerError {
        EventListenerError::KeyGenError(err)
    }
}

#[derive(Debug, PartialEq)]
pub enum ConfigurationError {
    MissingValue(String),
}

impl Error for ConfigurationError {}

impl fmt::Display for ConfigurationError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ConfigurationError::MissingValue(config_field_name) => {
                write!(f, "Missing configuration for {}", config_field_name)
            }
        }
    }
}

impl From<ConfigurationError> for EventListenerError {
    fn from(err: ConfigurationError) -> Self {
        EventListenerError::ConfigurationError(Box::new(err))
    }
}

#[derive(Debug, PartialEq)]
pub struct GetNodeError(pub String);

impl Error for GetNodeError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        None
    }
}

impl fmt::Display for GetNodeError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<GetNodeError> for EventListenerError {
    fn from(err: GetNodeError) -> Self {
        EventListenerError::GetNodeError(err)
    }
}
