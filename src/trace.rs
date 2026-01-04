use opentelemetry::propagation::{Extractor, Injector};
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{propagation::TraceContextPropagator, trace::Sampler};
use std::collections::HashMap;
use tracing_subscriber::{fmt::format::FmtSpan, prelude::*};

pub fn init_tracing(
    service_name: &'static str,
) -> anyhow::Result<opentelemetry_sdk::trace::SdkTracerProvider> {
    opentelemetry::global::set_text_map_propagator(
        TraceContextPropagator::new(),
    );

    let otlp_endpoint = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
        .ok()
        .filter(|s| !s.is_empty());

    let sampling_ratio = std::env::var("OTEL_TRACES_SAMPLING")
        .ok()
        .and_then(|s| s.parse::<f64>().ok())
        .unwrap_or(1.0);

    // ParentBased sampler: follows parent span's sampling decision if present,
    // otherwise uses TraceIdRatioBased sampling
    let sampler = Sampler::ParentBased(Box::new(Sampler::TraceIdRatioBased(
        sampling_ratio,
    )));

    let tracer_provider = if let Some(endpoint) = otlp_endpoint {
        let exporter = opentelemetry_otlp::SpanExporter::builder()
            .with_tonic()
            .with_endpoint(&endpoint)
            .build()?;

        opentelemetry_sdk::trace::SdkTracerProvider::builder()
            .with_batch_exporter(exporter)
            .with_sampler(sampler)
            .with_resource(
                opentelemetry_sdk::Resource::builder()
                    .with_service_name(service_name)
                    .build(),
            )
            .build()
    } else {
        opentelemetry_sdk::trace::SdkTracerProvider::builder()
            .with_sampler(sampler)
            .with_resource(
                opentelemetry_sdk::Resource::builder()
                    .with_service_name(service_name)
                    .build(),
            )
            .build()
    };

    opentelemetry::global::set_tracer_provider(tracer_provider.clone());
    let tracer = tracer_provider.tracer(service_name);

    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::from_default_env())
        .with(
            tracing_subscriber::fmt::layer()
                .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE),
        )
        .with(tracing_opentelemetry::layer().with_tracer(tracer))
        .try_init()?;

    Ok(tracer_provider)
}

/// Injector implementation for tarpc::context::Context
///
/// This collects trace context in W3C Trace Context format and provides
/// a method to create a tarpc::Context with the propagated trace information.
pub struct ContextInjector {
    inner: HashMap<String, String>,
}

impl Default for ContextInjector {
    fn default() -> Self {
        Self::new()
    }
}

impl ContextInjector {
    pub fn new() -> Self {
        Self {
            inner: HashMap::new(),
        }
    }

    /// Creates a tarpc::Context with trace context propagated from OpenTelemetry.
    ///
    /// This method extracts trace_id and span_id from the W3C Trace Context format
    /// and constructs a tarpc::trace::Context with that information.
    pub fn into_context(self) -> tarpc::context::Context {
        let mut ctx = tarpc::context::Context::current();

        if let Some(traceparent) = self.inner.get("traceparent")
            && let Some(trace_ctx) = Self::parse_traceparent(traceparent)
        {
            ctx.trace_context = trace_ctx;
        }

        ctx
    }

    /// Parses W3C Trace Context traceparent header format:
    /// version-trace_id-span_id-trace_flags
    /// Example: 00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01
    fn parse_traceparent(traceparent: &str) -> Option<tarpc::trace::Context> {
        let parts: Vec<&str> = traceparent.split('-').collect();
        if parts.len() != 4 {
            return None;
        }

        let trace_id_hex = parts[1];
        let span_id_hex = parts[2];
        let trace_flags_hex = parts[3];

        let trace_id_u128 = u128::from_str_radix(trace_id_hex, 16).ok()?;
        let span_id_u64 = u64::from_str_radix(span_id_hex, 16).ok()?;

        let sampled = trace_flags_hex == "01";
        let sampling_decision = if sampled {
            tarpc::trace::SamplingDecision::Sampled
        } else {
            tarpc::trace::SamplingDecision::Unsampled
        };

        Some(tarpc::trace::Context {
            trace_id: trace_id_u128.into(),
            span_id: span_id_u64.into(),
            sampling_decision,
        })
    }
}

impl Injector for ContextInjector {
    fn set(&mut self, key: &str, value: String) {
        self.inner.insert(key.to_string(), value);
    }
}

/// Extractor implementation for tarpc::context::Context
///
/// This extracts trace context from tarpc::Context and provides it in
/// W3C Trace Context format for OpenTelemetry propagation.
pub struct ContextExtractor {
    traceparent: String,
}

impl ContextExtractor {
    pub fn new(ctx: &tarpc::context::Context) -> Self {
        let trace_id: u128 = ctx.trace_context.trace_id.into();
        let span_id: u64 = ctx.trace_context.span_id.into();
        let trace_flags = match ctx.trace_context.sampling_decision {
            tarpc::trace::SamplingDecision::Sampled => "01",
            tarpc::trace::SamplingDecision::Unsampled => "00",
        };

        let traceparent =
            format!("00-{:032x}-{:016x}-{}", trace_id, span_id, trace_flags);
        Self { traceparent }
    }
}

impl Extractor for ContextExtractor {
    fn get(&self, key: &str) -> Option<&str> {
        if key == "traceparent" {
            Some(&self.traceparent)
        } else {
            None
        }
    }

    fn keys(&self) -> Vec<&str> {
        vec!["traceparent"]
    }
}

pub fn init_tracing_stderr(
    service_name: &'static str,
) -> anyhow::Result<opentelemetry_sdk::trace::SdkTracerProvider> {
    opentelemetry::global::set_text_map_propagator(
        TraceContextPropagator::new(),
    );

    let otlp_endpoint = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
        .ok()
        .filter(|s| !s.is_empty());

    let sampling_ratio = std::env::var("OTEL_TRACES_SAMPLER_RATE")
        .ok()
        .and_then(|s| s.parse::<f64>().ok())
        .unwrap_or(0.1);

    // ParentBased sampler: follows parent span's sampling decision if present,
    // otherwise uses TraceIdRatioBased sampling
    let sampler = Sampler::ParentBased(Box::new(Sampler::TraceIdRatioBased(
        sampling_ratio,
    )));

    let tracer_provider = if let Some(endpoint) = otlp_endpoint {
        let exporter = opentelemetry_otlp::SpanExporter::builder()
            .with_tonic()
            .with_endpoint(&endpoint)
            .build()?;

        opentelemetry_sdk::trace::SdkTracerProvider::builder()
            .with_batch_exporter(exporter)
            .with_sampler(sampler)
            .with_resource(
                opentelemetry_sdk::Resource::builder()
                    .with_service_name(service_name)
                    .build(),
            )
            .build()
    } else {
        opentelemetry_sdk::trace::SdkTracerProvider::builder()
            .with_sampler(sampler)
            .with_resource(
                opentelemetry_sdk::Resource::builder()
                    .with_service_name(service_name)
                    .build(),
            )
            .build()
    };

    opentelemetry::global::set_tracer_provider(tracer_provider.clone());
    let tracer = tracer_provider.tracer(service_name);

    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));

    tracing_subscriber::registry()
        .with(filter)
        .with(
            tracing_subscriber::fmt::layer()
                .with_writer(std::io::stderr)
                .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE),
        )
        .with(tracing_opentelemetry::layer().with_tracer(tracer))
        .try_init()?;

    Ok(tracer_provider)
}
