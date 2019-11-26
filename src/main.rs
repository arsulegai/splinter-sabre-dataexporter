// Copyright 2019 Cargill Incorporated
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

#[macro_use]
extern crate clap;
#[macro_use]
extern crate log;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate serde_json;

mod application_metadata;
mod authorization_handler;
mod config;
mod error;

use std::thread;

use flexi_logger::{style, DeferredNow, LogSpecBuilder, Logger};
use gameroom_database::ConnectionPool;
use log::Record;
use sawtooth_sdk::signing::create_context;
use splinter::events::Reactor;

use crate::config::{get_node, DataReaderConfigBuilder};
use crate::error::EventListenerError;

const APP_NAME: &str = env!("CARGO_PKG_NAME");
const VERSION: &str = env!("CARGO_PKG_VERSION");

// format for logs
pub fn log_format(
    w: &mut dyn std::io::Write,
    now: &mut DeferredNow,
    record: &Record,
) -> Result<(), std::io::Error> {
    let level = record.level();
    write!(
        w,
        "[{}] T[{:?}] {} [{}] {}",
        now.now().format("%Y-%m-%d %H:%M:%S%.3f"),
        thread::current().name().unwrap_or("<unnamed>"),
        record.level(),
        record.module_path().unwrap_or("<unnamed>"),
        style(level, &record.args()),
    )
}

fn run() -> Result<(), EventListenerError> {
    let matches = clap_app!(myapp =>
        (name: APP_NAME)
        (version: VERSION)
        (author: "Cargill Incorporated, Walmart Inc.")
        (about: "Daemon Package for Listening to events on Splinter")
        (@arg verbose: -v +multiple "Log verbosely")
        (@arg splinterd_url: --("splinterd-url") +takes_value "connection endpoint to SplinterD rest API")
    )
    .get_matches();

    let log_level = match matches.occurrences_of("verbose") {
        0 => log::LevelFilter::Warn,
        1 => log::LevelFilter::Info,
        2 => log::LevelFilter::Debug,
        _ => log::LevelFilter::Trace,
    };

    let mut log_spec_builder = LogSpecBuilder::new();
    log_spec_builder.default(log_level);
    log_spec_builder.module("hyper", log::LevelFilter::Warn);
    log_spec_builder.module("tokio", log::LevelFilter::Warn);
    log_spec_builder.module("trust_dns", log::LevelFilter::Warn);

    Logger::with(log_spec_builder.build())
        .format(log_format)
        .start()?;

    let config = DataReaderConfigBuilder::default()
        .with_cli_args(&matches)
        .build()?;

    // Generate a public/private key pair
    let context = create_context("secp256k1")?;
    let private_key = context.new_random_private_key()?;
    let public_key = context.get_public_key(&*private_key)?;

    // Get splinterd node information
    let node = get_node(config.splinterd_url())?;

    let reactor = Reactor::new();

    authorization_handler::run(
        config.splinterd_url().into(),
        node.identity.clone(),
        private_key.as_hex(),
        reactor.igniter(),
    )?;

    if let Err(err) = reactor.shutdown() {
        error!(
            "Unable to cleanly shutdown application authorization handler reactor: {}",
            err
        );
    }

    Ok(())
}

fn main() {
    if let Err(e) = run() {
        error!("{}", e);
        std::process::exit(1);
    }
}
