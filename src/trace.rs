use opentelemetry::trace::{TraceContextExt, TracerProvider as _};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{propagation::TraceContextPropagator, trace::Sampler};
use tracing_subscriber::{fmt::format::FmtSpan, prelude::*};

/// Trait for RPC context types that can be enriched with distributed tracing information.
///
/// This trait provides a type-safe way to propagate OpenTelemetry trace context
/// to various RPC frameworks without coupling the tracing logic to a specific framework.
pub trait TraceContextInjector: Sized {
    /// Injects the current OpenTelemetry span context into this RPC context.
    fn with_current_trace(self) -> Self;
}

/// Implementation for tarpc's Context type.
impl TraceContextInjector for tarpc::context::Context {
    fn with_current_trace(mut self) -> Self {
        // Get the current OpenTelemetry span context
        let otel_ctx = tracing_opentelemetry::OpenTelemetrySpanExt::context(
            &tracing::Span::current(),
        );
        let span_ref = otel_ctx.span();
        let span_ctx = span_ref.span_context();

        // Only propagate if the span context is valid (not a no-op span)
        if span_ctx.is_valid() {
            let trace_id = span_ctx.trace_id();
            let span_id = span_ctx.span_id();
            let is_sampled = span_ctx.trace_flags().is_sampled();

            self.trace_context = tarpc::trace::Context {
                trace_id: tarpc::trace::TraceId::from(u128::from_be_bytes(
                    trace_id.to_bytes(),
                )),
                span_id: tarpc::trace::SpanId::from(u64::from_be_bytes(
                    span_id.to_bytes(),
                )),
                sampling_decision: if is_sampled {
                    tarpc::trace::SamplingDecision::Sampled
                } else {
                    tarpc::trace::SamplingDecision::Unsampled
                },
            };
        }

        self
    }
}

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

/// Creates a tarpc Context with the current tracing span context propagated.
///
/// This function extracts the OpenTelemetry span context from the current tracing span
/// and converts it to tarpc's trace context format for distributed tracing across RPC calls.
///
/// This is a tarpc-specific helper function for propagating trace context across RPC boundaries.
pub fn tarpc_context_with_trace() -> tarpc::context::Context {
    let mut ctx = tarpc::context::Context::current();

    // Get the current OpenTelemetry span context
    let otel_ctx = tracing_opentelemetry::OpenTelemetrySpanExt::context(
        &tracing::Span::current(),
    );
    let span_ref = otel_ctx.span();
    let span_ctx = span_ref.span_context();

    // Only propagate if the span context is valid (not a no-op span)
    if span_ctx.is_valid() {
        let trace_id = span_ctx.trace_id();
        let span_id = span_ctx.span_id();
        let is_sampled = span_ctx.trace_flags().is_sampled();

        ctx.trace_context = tarpc::trace::Context {
            trace_id: tarpc::trace::TraceId::from(u128::from_be_bytes(
                trace_id.to_bytes(),
            )),
            span_id: tarpc::trace::SpanId::from(u64::from_be_bytes(
                span_id.to_bytes(),
            )),
            sampling_decision: if is_sampled {
                tarpc::trace::SamplingDecision::Sampled
            } else {
                tarpc::trace::SamplingDecision::Unsampled
            },
        };
    }

    ctx
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
