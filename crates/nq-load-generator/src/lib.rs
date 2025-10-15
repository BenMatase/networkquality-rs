// Copyright (c) 2023-2024 Cloudflare, Inc.
// Licensed under the BSD-3-Clause license found in the LICENSE file or at https://opensource.org/licenses/BSD-3-Clause

use std::{collections::HashMap, sync::Arc};

use anyhow::Context;
use http::{HeaderMap, HeaderName, HeaderValue};
use nq_core::client::{Direction, ThroughputClient};
use nq_core::{
    oneshot_result, BodyEvent, ConnectionType, EstablishedConnection, Network, OneshotResult, Time,
    Timestamp,
};
use nq_stats::CounterSeries;
use rand::seq::SliceRandom;
use serde::Deserialize;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

#[derive(Debug, Deserialize)]
pub struct LoadConfig {
    pub headers: HashMap<String, String>,
    pub download_url: url::Url,
    pub upload_url: url::Url,
    pub upload_size: usize,
    pub no_tls: bool,
}

pub struct LoadGenerator {
    headers: HeaderMap<HeaderValue>,
    config: LoadConfig,
    loads: Vec<LoadedConnection>,
}

impl LoadGenerator {
    pub fn new(config: LoadConfig) -> anyhow::Result<Self> {
        let mut headers = HeaderMap::new();

        for (key, value) in config.headers.iter() {
            headers.insert(
                HeaderName::from_bytes(key.as_bytes())?,
                HeaderValue::from_bytes(value.as_bytes())?,
            );
        }

        Ok(Self {
            headers,
            config,
            loads: Vec::new(),
        })
    }

    #[tracing::instrument(skip(self, network, time, shutdown))]
    pub fn new_loaded_connection(
        &self,
        direction: Direction,
        conn_type: ConnectionType,
        network: Arc<dyn Network>,
        time: Arc<dyn Time>,
        shutdown: CancellationToken,
    ) -> anyhow::Result<OneshotResult<LoadedConnection>> {
        let (tx, rx) = oneshot_result();

        let client = match direction {
            Direction::Down => ThroughputClient::download().plain_http_mode(self.config.no_tls),
            Direction::Up(size) => ThroughputClient::upload(size).plain_http_mode(self.config.no_tls),
        };

        let response_fut = client
            .new_connection(conn_type)
            .headers(self.headers.clone())
            .send(
                match direction {
                    Direction::Up(_) => self.config.upload_url.as_str().parse()?,
                    Direction::Down => self.config.download_url.as_str().parse()?,
                },
                network,
                time,
                shutdown,
            )?;

        tracing::debug!("got loaded connection response future");

        tokio::spawn(
            async move {
                let inflight_body = response_fut
                    .await
                    .context("could not await response for loaded connection")?;

                tracing::debug!("sending loaded connection");

                let _ = tx.send(Ok(LoadedConnection {
                    connection: inflight_body.connection,
                    events_rx: inflight_body.events,
                    total_bytes_series: CounterSeries::new(),
                    finished_at: None,
                    direction,
                    bytes_transferred_total: 0,
                }));

                Ok::<_, anyhow::Error>(())
            }
            .in_current_span(),
        );

        Ok(rx)
    }

    pub fn connections(&self) -> impl Iterator<Item = &LoadedConnection> {
        self.loads.iter()
    }

    pub fn random_connection(&self) -> Option<Arc<RwLock<EstablishedConnection>>> {
        let loads: Vec<_> = self.ongoing_loads().collect();
        loads
            .choose(&mut rand::thread_rng())
            .map(|c| c.connection.clone())
    }

    /// Select a random finished (idle) connection that has completed its load but
    /// is still kept alive so it can be re-used for a self probe in non-multiplexed
    /// protocols (e.g. HTTP/1.1) to avoid incurring connection setup latency.
    pub fn random_finished_connection(&self) -> Option<Arc<RwLock<EstablishedConnection>>> {
        let finished: Vec<_> = self
            .loads
            .iter()
            .filter(|l| l.finished_at.is_some())
            .collect();
        finished
            .choose(&mut rand::thread_rng())
            .map(|c| c.connection.clone())
    }

    pub fn push(&mut self, loaded_connection: LoadedConnection) {
        self.loads.push(loaded_connection);
    }

    pub fn update(&mut self) {
        for load in &mut self.loads {
            load.update();
        }
    }

    pub fn ongoing_loads(&self) -> impl Iterator<Item = &LoadedConnection> {
        self.loads.iter().filter(|load| load.finished_at.is_none())
    }

    pub fn count_loads(&self) -> usize {
        self.ongoing_loads().count()
    }

    pub fn into_connections(self) -> Vec<LoadedConnection> {
        self.loads
    }

}

#[derive(Debug)]
pub struct LoadedConnection {
    connection: Arc<RwLock<EstablishedConnection>>,
    events_rx: UnboundedReceiver<BodyEvent>,
    total_bytes_series: CounterSeries,
    finished_at: Option<Timestamp>,
    direction: Direction,
    /// Cumulative bytes transferred across restarts (for uploads only).
    bytes_transferred_total: usize,
}

impl LoadedConnection {
    pub fn update(&mut self) {
        while let Ok(event) = self.events_rx.try_recv() {
            match event {
                BodyEvent::ByteCount { at, total } => {
                    self.total_bytes_series.add(at, total as f64);
                    // Track cumulative upload bytes across restarts so capacity calc remains accurate.
                    if matches!(self.direction, Direction::Up(_)) {
                        self.bytes_transferred_total = total; // last reported total for current run
                    }
                }
                BodyEvent::Finished { at } => {
                    self.finished_at = Some(at);
                }
            }
        }
    }

    pub fn total_bytes_series(&self) -> &CounterSeries {
        &self.total_bytes_series
    }

    pub fn stop(&mut self) {
        self.events_rx.close();
        self.update();
    }

    pub fn is_finished_upload(&self) -> bool {
        self.finished_at.is_some() && matches!(self.direction, Direction::Up(_))
    }

    pub fn restart_upload(
        &mut self,
        network: Arc<dyn Network>,
        time: Arc<dyn Time>,
        shutdown: CancellationToken,
        url: &url::Url,
        size: usize,
        no_tls: bool,
    ) -> anyhow::Result<impl std::future::Future<Output = anyhow::Result<()>> + '_> {
        self.finished_at = None;
        let uri = url.as_str().parse()?;
        let client = ThroughputClient::upload(size)
            .plain_http_mode(no_tls)
            .with_connection(self.connection.clone());
        let response_fut = client.send(uri, network, time, shutdown)?;
        Ok(async move {
            let inflight_body = response_fut.await?;
            self.events_rx = inflight_body.events;
            Ok(())
        })
    }
}

impl LoadGenerator {
    /// Mutable access to internal loads vector for management (e.g., restarts).
    pub fn loads_mut(&mut self) -> &mut Vec<LoadedConnection> { &mut self.loads }
    pub fn config(&self) -> &LoadConfig { &self.config }
}
