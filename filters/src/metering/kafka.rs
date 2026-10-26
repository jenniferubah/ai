// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Praxis Contributors

//! Kafka usage-event sink backed by `rdkafka` (feature `metering-kafka`).

use std::time::Duration;

use metrics::counter;
use praxis_ai_apis::credential_source::{self, CredentialSource, DiagnosticContext};
use praxis_filter::FilterError;
use rdkafka::{
    ClientConfig,
    producer::{FutureProducer, FutureRecord, Producer},
    util::Timeout,
};
use tracing::{trace, warn};
use zeroize::Zeroizing;

use super::{
    METRIC_REPORT_FAILURES,
    config::{KafkaConfig, join_kafka_brokers, kafka_security_protocol},
};

/// Bounded wait when flushing the producer during filter teardown.
const KAFKA_FLUSH_TIMEOUT: Duration = Duration::from_secs(5);

/// Fire-and-forget Kafka producer for metering `CloudEvents`.
pub(super) struct KafkaUsageEventSink {
    /// Idempotent `rdkafka` producer shared across fire-and-forget tasks.
    producer: FutureProducer,
    /// Bound on waiting for produce acknowledgement.
    timeout: Duration,
    /// Topic that receives `CloudEvent` JSON payloads.
    topic: String,
}

impl KafkaUsageEventSink {
    /// Build an idempotent producer from validated Kafka config.
    ///
    /// # Errors
    /// Returns [`FilterError`] when the SASL password cannot be read or the
    /// producer fails to be constructed.
    pub(super) fn new(cfg: &KafkaConfig, timeout: Duration) -> Result<Self, FilterError> {
        let password = read_sasl_password(&cfg.sasl.password)?;
        let brokers = join_kafka_brokers(&cfg.brokers);
        let security_protocol = kafka_security_protocol(cfg.tls.enabled);

        let mut client = ClientConfig::new();
        client
            .set("bootstrap.servers", &brokers)
            .set("acks", "all")
            .set("enable.idempotence", "true")
            .set("security.protocol", security_protocol)
            .set("sasl.mechanisms", &cfg.sasl.mechanism)
            .set("sasl.username", &cfg.sasl.username)
            .set("sasl.password", password.as_str());

        if cfg.tls.enabled {
            let ca_file = cfg
                .tls
                .ca_file
                .as_deref()
                .ok_or_else(|| FilterError::from("external_metering: kafka.tls.ca_file is required"))?;
            client.set("ssl.ca.location", ca_file);
        }

        let producer: FutureProducer = client.create().map_err(|error| {
            FilterError::from(format!("external_metering: failed to create Kafka producer: {error}"))
        })?;

        Ok(Self {
            producer,
            timeout,
            topic: cfg.topic.clone(),
        })
    }

    /// Spawn a fire-and-forget produce of `body`, keyed by the `CloudEvents` `id`.
    pub(super) fn deliver(&self, body: Vec<u8>, event_id: String) {
        let producer = self.producer.clone();
        let topic = self.topic.clone();
        let timeout = self.timeout;

        tokio::spawn(async move {
            let record = FutureRecord::to(&topic).payload(&body).key(&event_id);
            match producer.send(record, timeout).await {
                Ok(delivery) => {
                    trace!(
                        partition = delivery.partition,
                        offset = delivery.offset,
                        event_id = %event_id,
                        "metering usage report produced to Kafka"
                    );
                },
                Err((error, _)) => {
                    counter!(METRIC_REPORT_FAILURES).increment(1);
                    warn!(%error, event_id = %event_id, "metering Kafka usage report failed");
                },
            }
        });
    }
}

impl Drop for KafkaUsageEventSink {
    fn drop(&mut self) {
        if let Err(error) = self.producer.flush(Timeout::After(KAFKA_FLUSH_TIMEOUT)) {
            warn!(%error, "metering Kafka producer flush on teardown failed");
        }
    }
}

/// Read the SASL password from a file-backed credential source.
fn read_sasl_password(source: &CredentialSource) -> Result<Zeroizing<String>, FilterError> {
    let ctx = DiagnosticContext {
        owner: "external_metering",
        reference: Some("kafka.sasl.password".to_owned()),
    };
    let transform = |token: &str| -> Result<Zeroizing<String>, FilterError> { Ok(Zeroizing::new(token.to_owned())) };
    let (handle, _watch) = credential_source::build_one(ctx, source, transform)?;
    let snapshot = handle.current();
    snapshot
        .payload
        .as_ref()
        .cloned()
        .ok_or_else(|| FilterError::from("external_metering: kafka.sasl.password is unavailable after startup read"))
}
