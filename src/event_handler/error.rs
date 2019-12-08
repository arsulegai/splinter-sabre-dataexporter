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

use std::error::Error;
use std::fmt;

use futures::future;
use sabre_sdk::protocol::payload::{
    CreateContractActionBuildError, CreateContractRegistryActionBuildError,
    CreateNamespaceRegistryActionBuildError, CreateNamespaceRegistryPermissionActionBuildError,
    SabrePayloadBuildError,
};
use sabre_sdk::protos::ProtoConversionError as SabreProtoConversionError;
use sawtooth_sdk::signing::Error as SigningError;
use splinter::events;

use crate::application_metadata::ApplicationMetadataError;

#[derive(Debug)]
pub enum EventHandlerError {
    IOError(std::io::Error),
    InvalidMessageError(String),
    ReactorError(events::ReactorError),
    WebSocketError(events::WebSocketError),
    SabreError(String),
    SawtoothError(String),
    SigningError(String),
    BatchSubmitError(String),
}

impl Error for EventHandlerError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            EventHandlerError::IOError(err) => Some(err),
            EventHandlerError::InvalidMessageError(_) => None,
            EventHandlerError::ReactorError(err) => Some(err),
            EventHandlerError::SabreError(_) => None,
            EventHandlerError::SawtoothError(_) => None,
            EventHandlerError::SigningError(_) => None,
            EventHandlerError::BatchSubmitError(_) => None,
            EventHandlerError::WebSocketError(err) => Some(err),
        }
    }
}

impl fmt::Display for EventHandlerError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            EventHandlerError::IOError(msg) => write!(f, "An I/O error occurred: {}", msg),
            EventHandlerError::InvalidMessageError(msg) => {
                write!(f, "The client received an invalid message: {}", msg)
            }
            EventHandlerError::ReactorError(msg) => write!(f, "Reactor Error: {}", msg),
            EventHandlerError::SabreError(msg) => write!(
                f,
                "An error occurred while building a Sabre payload: {}",
                msg
            ),
            EventHandlerError::SawtoothError(msg) => write!(
                f,
                "An error occurred while building a transaction or batch: {}",
                msg
            ),
            EventHandlerError::SigningError(msg) => {
                write!(f, "A signing error occurred: {}", msg)
            }
            EventHandlerError::BatchSubmitError(msg) => write!(
                f,
                "An error occurred while submitting a batch to the scabbard service: {}",
                msg
            ),
            EventHandlerError::WebSocketError(msg) => write!(f, "WebsocketError {}", msg),
        }
    }
}

impl From<std::io::Error> for EventHandlerError {
    fn from(err: std::io::Error) -> EventHandlerError {
        EventHandlerError::IOError(err)
    }
}

impl From<serde_json::error::Error> for EventHandlerError {
    fn from(err: serde_json::error::Error) -> EventHandlerError {
        EventHandlerError::InvalidMessageError(format!("{}", err))
    }
}

impl From<std::string::FromUtf8Error> for EventHandlerError {
    fn from(err: std::string::FromUtf8Error) -> EventHandlerError {
        EventHandlerError::InvalidMessageError(format!("{}", err))
    }
}

impl From<ApplicationMetadataError> for EventHandlerError {
    fn from(err: ApplicationMetadataError) -> EventHandlerError {
        EventHandlerError::InvalidMessageError(format!("{}", err))
    }
}

impl From<events::ReactorError> for EventHandlerError {
    fn from(err: events::ReactorError) -> Self {
        EventHandlerError::ReactorError(err)
    }
}

impl From<events::WebSocketError> for EventHandlerError {
    fn from(err: events::WebSocketError) -> Self {
        EventHandlerError::WebSocketError(err)
    }
}

macro_rules! impl_from_sabre_errors {
    ($($x:ty),*) => {
        $(
            impl From<$x> for EventHandlerError {
                fn from(e: $x) -> Self {
                    EventHandlerError::SabreError(e.to_string())
                }
            }
        )*
    };
}

impl_from_sabre_errors!(
    CreateContractActionBuildError,
    CreateContractRegistryActionBuildError,
    CreateNamespaceRegistryActionBuildError,
    CreateNamespaceRegistryPermissionActionBuildError,
    SabreProtoConversionError,
    SabrePayloadBuildError
);

impl From<SigningError> for EventHandlerError {
    fn from(err: SigningError) -> Self {
        EventHandlerError::SigningError(err.to_string())
    }
}

impl<T> Into<future::FutureResult<T, EventHandlerError>> for EventHandlerError {
    fn into(self) -> future::FutureResult<T, EventHandlerError> {
        future::err(self)
    }
}
